// ingestion-bench: sequential ClickHouse vs Druid ingestion benchmark.
//
// Usage:
//
//	go run ingestion-bench/main.go --config ingestion-bench/config.yaml --out inres.csv
//	go run ingestion-bench/main.go --config ingestion-bench/config.yaml --out inres.csv --only clickhouse
//
// Design:
//   - Medium-complexity schema (~20 cols, see Event + clickhouseDDL).
//   - Deterministic synthetic generator (seeded, no external data needed).
//   - ClickHouse path: HTTP INSERT ... FORMAT JSONEachRow, batched, concurrent.
//   - Druid path: single native-batch `index_parallel` task whose http
//     inputSource pulls NDJSON files from an ephemeral Go file server.
//   - Docker targets are brought up ONE AT A TIME (compose profiles) when
//     docker.manage=true, then torn down before the next target starts.
package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------- config ---

type Config struct {
	Dataset struct {
		TotalEvents int    `yaml:"total_events"`
		BatchSize   int    `yaml:"batch_size"`
		Seed        int64  `yaml:"seed"`
		StartTime   string `yaml:"start_time"`
		SpanDays    int    `yaml:"span_days"`
	} `yaml:"dataset"`
	Bench struct {
		Concurrency                   int      `yaml:"concurrency"`
		TimeoutSeconds                int      `yaml:"timeout_seconds"`
		DruidMaxNumConcurrentSubtasks int      `yaml:"druid_max_num_concurrent_subtasks"`
		DruidSegmentGranularity       string   `yaml:"druid_segment_granularity"`
		DruidRowsPerFile              int      `yaml:"druid_rows_per_file"`
		HelperHostCandidates          []string `yaml:"helper_host_candidates"`
		HelperPort                    int      `yaml:"helper_port"`
	} `yaml:"bench"`
	Targets []Target `yaml:"targets"`
	Docker  struct {
		Manage           bool   `yaml:"manage"`
		ComposeFile      string `yaml:"compose_file"`
		DownAfterEach    bool   `yaml:"down_after_each"`
		ReadyTimeoutSecs int    `yaml:"ready_timeout_seconds"`
	} `yaml:"docker"`
}

type Target struct {
	Name       string `yaml:"name"`
	Enabled    bool   `yaml:"enabled"`
	Endpoint   string `yaml:"endpoint"`
	Database   string `yaml:"database"`
	Table      string `yaml:"table"`
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
	Datasource string `yaml:"datasource"`
}

// ---------------------------------------------------------------- schema ---

// Event is the medium-complexity row (~20 columns):
// low-card dims, high-card ids, numeric metrics, geo, mixed types.
type Event struct {
	EventTime string  `json:"event_time"` // "2006-01-02 15:04:05" (CH DateTime + Druid auto both parse)
	EventID   string  `json:"event_id"`
	UserID    uint32  `json:"user_id"`
	SessionID string  `json:"session_id"`
	EventType string  `json:"event_type"`
	ProductID uint32  `json:"product_id"`
	Category  string  `json:"category"`
	Brand     string  `json:"brand"`
	Price     float64 `json:"price"`
	Quantity  uint16  `json:"quantity"`
	Discount  float32 `json:"discount"`
	Total     float64 `json:"total"`
	Country   string  `json:"country"`
	City      string  `json:"city"`
	Device    string  `json:"device_type"`
	OS        string  `json:"os"`
	Browser   string  `json:"browser"`
	IsBot     uint8   `json:"is_bot"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

var (
	eventTypes = []string{"view", "click", "add_to_cart", "purchase", "search", "login", "logout", "refund"}
	categories = []string{"electronics", "books", "clothing", "food", "toys", "sports", "beauty", "home", "auto", "health", "garden", "music"}
	brands     = []string{"acme", "globex", "initech", "umbrella", "hooli", "stark", "wayne", "wonka", "gizmo", "froboz"}
	countries  = []string{"US", "DE", "FR", "IN", "BR", "JP", "GB", "CA", "AU", "NL"}
	cities     = []string{"berlin", "paris", "nyc", "tokyo", "london", "sydney", "toronto", "mumbai", "saopaulo", "amsterdam", "austin", "seattle"}
	devices    = []string{"desktop", "mobile", "tablet", "tv"}
	oses       = []string{"linux", "windows", "macos", "android", "ios"}
	browsers   = []string{"chrome", "firefox", "safari", "edge", "opera"}
)

func genEvent(r *rand.Rand, base time.Time, span time.Duration, i int) Event {
	ts := base.Add(time.Duration(r.Int63n(int64(span))))
	qty := uint16(1 + r.Intn(5))
	price := math.Round((5+r.Float64()*495)*100) / 100
	disc := float32(math.Round(r.Float64()*30*100) / 100)
	total := math.Round(float64(qty)*price*(1-float64(disc)/100)*100) / 100
	bot := uint8(0)
	if r.Float64() < 0.01 {
		bot = 1
	}
	return Event{
		EventTime: ts.UTC().Format("2006-01-02 15:04:05"),
		EventID:   fmt.Sprintf("%08x-%04x-%d", r.Uint32(), r.Uint32()&0xffff, i),
		UserID:    r.Uint32() % 2000000,
		SessionID: fmt.Sprintf("s-%08x", r.Uint32()),
		EventType: eventTypes[r.Intn(len(eventTypes))],
		ProductID: r.Uint32() % 500000,
		Category:  categories[r.Intn(len(categories))],
		Brand:     brands[r.Intn(len(brands))],
		Price:     price,
		Quantity:  qty,
		Discount:  disc,
		Total:     total,
		Country:   countries[r.Intn(len(countries))],
		City:      cities[r.Intn(len(cities))],
		Device:    devices[r.Intn(len(devices))],
		OS:        oses[r.Intn(len(oses))],
		Browser:   browsers[r.Intn(len(browsers))],
		IsBot:     bot,
		Latitude:  math.Round((r.Float64()*180-90)*1e6) / 1e6,
		Longitude: math.Round((r.Float64()*360-180)*1e6) / 1e6,
	}
}

// genBatchNDJSON renders rows [start, start+n) deterministically from seed.
func genBatchNDJSON(seed int64, base time.Time, span time.Duration, start, n int) []byte {
	r := rand.New(rand.NewSource(seed + int64(start)))
	var buf bytes.Buffer
	for k := 0; k < n; k++ {
		e := genEvent(r, base, span, start+k)
		b, _ := json.Marshal(e)
		buf.Write(b)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// ---------------------------------------------------------------- docker ---

func compose(args ...string) (string, error) {
	cmd := exec.Command("docker", args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		return out.String() + errb.String(), fmt.Errorf("docker %v: %w\n%s", args, err, errb.String())
	}
	return out.String(), nil
}

func dockerUp(cfg Config, profile string) error {
	fmt.Printf(">> docker compose up (profile=%s) ...\n", profile)
	o, err := compose("compose", "-f", cfg.Docker.ComposeFile, "--profile", profile, "up", "-d")
	fmt.Println(o)
	return err
}

func dockerDown(cfg Config, profile string) error {
	if !cfg.Docker.DownAfterEach {
		return nil
	}
	fmt.Printf(">> docker compose down (profile=%s) to free resources ...\n", profile)
	o, err := compose("compose", "-f", cfg.Docker.ComposeFile, "--profile", profile, "down")
	fmt.Println(o)
	return err
}

// resolveComposeFile accepts compose_file relative to the cwd or to the
// config file's directory, so the same config works from the repo root
// and from inside ingestion-bench/.
func resolveComposeFile(cfgPath, f string) string {
	if f == "" {
		return f
	}
	if _, err := os.Stat(f); err == nil {
		return f
	}
	if c := filepath.Join(filepath.Dir(cfgPath), f); c != f {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	if b := filepath.Join(filepath.Dir(cfgPath), filepath.Base(f)); b != f {
		if _, err := os.Stat(b); err == nil {
			return b
		}
	}
	return f // not found anywhere; let docker report the error
}

func waitReady(name, endpoint string, timeoutSecs int) error {
	deadline := time.Now().Add(time.Duration(timeoutSecs) * time.Second)
	client := &http.Client{Timeout: 5 * time.Second}
	for time.Now().Before(deadline) {
		var u string
		if name == "clickhouse" {
			u = strings.TrimRight(endpoint, "/") + "/ping"
		} else {
			u = strings.TrimRight(endpoint, "/") + "/status"
		}
		if resp, err := client.Get(u); err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if name == "clickhouse" && resp.StatusCode == 200 && strings.Contains(string(body), "Ok") {
				return nil
			}
			if name == "druid" && resp.StatusCode == 200 {
				// /status is up early; also require coordinator live before ingesting.
				if okDruidCoordinator(endpoint, client) {
					return nil
				}
			}
		}
		fmt.Printf("   waiting for %s (%s)...\n", name, u)
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("%s not ready within %ds", name, timeoutSecs)
}

func okDruidCoordinator(endpoint string, client *http.Client) bool {
	u := strings.TrimRight(endpoint, "/") + "/druid/coordinator/v1/leader"
	resp, err := client.Get(u)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

// -------------------------------------------------------------- clickhouse ---

const clickhouseDDL = `CREATE TABLE IF NOT EXISTS %s.%s (
  event_time DateTime,
  event_id String,
  user_id UInt32,
  session_id String,
  event_type LowCardinality(String),
  product_id UInt32,
  category LowCardinality(String),
  brand String,
  price Float64,
  quantity UInt16,
  discount Float32,
  total Float64,
  country LowCardinality(String),
  city String,
  device_type LowCardinality(String),
  os String,
  browser String,
  is_bot UInt8,
  latitude Float64,
  longitude Float64
) ENGINE = MergeTree()
ORDER BY (event_time, event_type, city)`

func chQuery(t Target, query string) error {
	u := strings.TrimRight(t.Endpoint, "/") + "/?query=" + url.QueryEscape(query)
	req, _ := http.NewRequest("POST", u, nil)
	if t.User != "" {
		req.SetBasicAuth(t.User, t.Password)
	}
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("clickhouse query failed (%d): %s", resp.StatusCode, string(body))
	}
	return nil
}

func chInsert(t Target, ndjson []byte) error {
	q := fmt.Sprintf("INSERT INTO %s.%s FORMAT JSONEachRow", t.Database, t.Table)
	u := strings.TrimRight(t.Endpoint, "/") + "/?query=" + url.QueryEscape(q)
	req, _ := http.NewRequest("POST", u, bytes.NewReader(ndjson))
	if t.User != "" {
		req.SetBasicAuth(t.User, t.Password)
	}
	client := &http.Client{Timeout: 300 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("insert failed (%d): %.500s", resp.StatusCode, string(body))
	}
	if bytes.Contains(body, []byte("Code:")) && bytes.Contains(body, []byte("Exception")) {
		return fmt.Errorf("insert exception: %.500s", string(body))
	}
	return nil
}

type benchResult struct {
	Target       string
	TotalEvents  int
	BatchSize    int
	Concurrency  int
	ElapsedSec   float64
	EventsPerSec float64
	P50Ms        float64
	P95Ms        float64
	MeanMs       float64
	Errors       int
	Notes        string
}

func benchClickHouse(cfg Config, t Target, base time.Time, span time.Duration) (benchResult, error) {
	db := t.Database
	if db == "" {
		db = "default"
	}
	fmt.Printf(">> ClickHouse: recreate %s.%s\n", db, t.Table)
	if err := chQuery(t, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", db, t.Table)); err != nil {
		return benchResult{}, err
	}
	t2 := t
	t2.Database = db
	if err := chQuery(t2, fmt.Sprintf(clickhouseDDL, db, t.Table)); err != nil {
		return benchResult{}, err
	}

	total := cfg.Dataset.TotalEvents
	bs := cfg.Dataset.BatchSize
	conc := cfg.Bench.Concurrency
	if conc < 1 {
		conc = 1
	}
	nB := (total + bs - 1) / bs
	fmt.Printf(">> ClickHouse: ingesting %d rows in %d batches (batch=%d, concurrency=%d)\n", total, nB, bs, conc)

	lats := make([]float64, nB)
	errCount := 0
	var mu sync.Mutex
	jobs := make(chan int, nB)
	for i := 0; i < nB; i++ {
		jobs <- i
	}
	close(jobs)

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for bi := range jobs {
				startRow := bi * bs
				n := bs
				if startRow+n > total {
					n = total - startRow
				}
				payload := genBatchNDJSON(cfg.Dataset.Seed, base, span, startRow, n)
				t0 := time.Now()
				err := chInsert(t2, payload)
				ms := float64(time.Since(t0).Microseconds()) / 1000.0
				mu.Lock()
				lats[bi] = ms
				if err != nil {
					errCount++
					fmt.Printf("   batch %d error: %v\n", bi, err)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	el := time.Since(start).Seconds()
	if errCount > 0 {
		return benchResult{}, fmt.Errorf("clickhouse: %d/%d batches failed", errCount, nB)
	}
	sort.Float64s(lats)
	return benchResult{
		Target: "clickhouse", TotalEvents: total, BatchSize: bs, Concurrency: conc,
		ElapsedSec: el, EventsPerSec: float64(total) / el,
		P50Ms: pct(lats, 50), P95Ms: pct(lats, 95), MeanMs: mean(lats),
		Notes: "http INSERT FORMAT JSONEachRow",
	}, nil
}

// ------------------------------------------------------------------ druid ---

func druidDropDatasource(endpoint, ds string) {
	client := &http.Client{Timeout: 15 * time.Second}
	req, _ := http.NewRequest("DELETE", strings.TrimRight(endpoint, "/")+"/druid/coordinator/v1/datasources/"+ds, nil)
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
}

func druidTaskSpec(ds string, uris []string, maxSubtasks int, segGran, interval string) map[string]any {
	return map[string]any{
		"type": "index_parallel",
		"spec": map[string]any{
			"ioConfig": map[string]any{
				"type":        "index_parallel",
				"inputSource": map[string]any{"type": "http", "uris": uris},
				"inputFormat": map[string]any{
					"type": "json",
					"flattenSpec": map[string]any{
						"useFieldDiscovery": true,
					},
				},
			},
			"tuningConfig": map[string]any{
				"type":                     "index_parallel",
				"maxNumConcurrentSubTasks": maxSubtasks,
				"forceGuaranteedRollup":    false,
			},
			"dataSchema": map[string]any{
				"dataSource": ds,
				"timestampSpec": map[string]any{
					"column": "event_time",
					"format": "auto",
				},
				"dimensionsSpec": map[string]any{
					"useFieldDiscovery": true,
				},
				"granularitySpec": map[string]any{
					"type":               "uniform",
					"segmentGranularity": segGran,
					"queryGranularity":   "NONE",
					"rollup":             false,
					"intervals":          []string{interval},
				},
			},
		},
	}
}

func benchDruid(cfg Config, t Target, base time.Time, span time.Duration) (benchResult, error) {
	endpoint := strings.TrimRight(t.Endpoint, "/")
	ds := t.Datasource
	if ds == "" {
		ds = "events"
	}
	druidDropDatasource(endpoint, ds)

	total := cfg.Dataset.TotalEvents
	rowsPerFile := cfg.Bench.DruidRowsPerFile
	if rowsPerFile <= 0 {
		rowsPerFile = 50000
	}
	nFiles := (total + rowsPerFile - 1) / rowsPerFile

	// 1. materialize NDJSON input files into ./druid-input (bind-mounted
	// into the fileserver sidecar, so Druid fetches over the compose net).
	dir := filepath.Join(filepath.Dir(cfg.Docker.ComposeFile), "druid-input")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return benchResult{}, err
	}
	if stale, _ := filepath.Glob(filepath.Join(dir, "events-*.ndjson")); len(stale) > 0 {
		for _, s := range stale {
			os.Remove(s)
		}
	}
	fmt.Printf(">> Druid: writing %d input files (%d rows each) to %s\n", nFiles, rowsPerFile, dir)
	names := make([]string, 0, nFiles)
	for f := 0; f < nFiles; f++ {
		startRow := f * rowsPerFile
		n := rowsPerFile
		if startRow+n > total {
			n = total - startRow
		}
		name := fmt.Sprintf("events-%04d.ndjson", f)
		payload := genBatchNDJSON(cfg.Dataset.Seed, base, span, startRow, n)
		if err := os.WriteFile(filepath.Join(dir, name), payload, 0o644); err != nil {
			return benchResult{}, err
		}
		names = append(names, name)
	}
	defer func() {
		for _, n := range names {
			os.Remove(filepath.Join(dir, n))
		}
	}()

	// 2. helper HTTP server (Druid middleManager fetches `uris` from here)
	port := cfg.Bench.HelperPort
	if port == 0 {
		port = 18089
	}
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir(dir)))
	srv := &http.Server{Addr: fmt.Sprintf("0.0.0.0:%d", port), Handler: mux}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return benchResult{}, fmt.Errorf("helper file server: %w (is port %d busy?)", err, port)
	}
	go srv.Serve(ln)
	defer srv.Close()
	fmt.Printf(">> Druid: helper file server on :%d serving %s\n", port, dir)

	hosts := cfg.Bench.HelperHostCandidates
	if len(hosts) == 0 {
		hosts = []string{"host.docker.internal", "172.17.0.1"}
	}
	// Preferred base first: fileserver sidecar on the compose network
	// (deterministic on Docker Desktop); host-based fallbacks after it.
	bases := []string{"http://fileserver:8080"}
	for _, h := range hosts {
		bases = append(bases, fmt.Sprintf("http://%s:%d", h, port))
	}
	interval := fmt.Sprintf("%s/%s",
		base.UTC().Format("2006-01-02"),
		base.Add(span).UTC().Format("2006-01-02"))
	maxSub := cfg.Bench.DruidMaxNumConcurrentSubtasks
	if maxSub < 1 {
		maxSub = 2
	}
	segGran := cfg.Bench.DruidSegmentGranularity
	if segGran == "" {
		segGran = "DAY"
	}

	client := &http.Client{Timeout: 60 * time.Second}
	var lastErr error
	for _, h := range bases {
		uris := make([]string, 0, nFiles)
		for _, n := range names {
			uris = append(uris, fmt.Sprintf("%s/%s", h, n))
		}
		spec := druidTaskSpec(ds, uris, maxSub, segGran, interval)
		body, _ := json.Marshal(spec)
		fmt.Printf(">> Druid: submitting index_parallel task via %s ...\n", h)
		req, _ := http.NewRequest("POST", endpoint+"/druid/indexer/v1/task", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			fmt.Printf("   submit via %s failed: %v, trying next host\n", h, err)
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			lastErr = fmt.Errorf("task submit (%d): %.500s", resp.StatusCode, string(respBody))
			fmt.Printf("   %v, trying next host\n", lastErr)
			continue
		}
		var sub struct {
			Task string `json:"task"`
		}
		if err := json.Unmarshal(respBody, &sub); err != nil || sub.Task == "" {
			lastErr = fmt.Errorf("bad submit response: %.300s", string(respBody))
			continue
		}
		fmt.Printf("   task id: %s\n", sub.Task)
		t0 := time.Now()
		res, err := pollDruidTask(endpoint, sub.Task, cfg.Bench.TimeoutSeconds)
		el := time.Since(t0).Seconds()
		if err != nil {
			// input-fetch failures manifest as FAILED quickly; retry next base.
			fmt.Printf("   task %s: %v\n", sub.Task, err)
			lastErr = err
			if strings.Contains(err.Error(), "FAILED") && h != bases[len(bases)-1] {
				fmt.Printf("   retrying with next helper host...\n")
				continue
			}
			return benchResult{}, err
		}
		_ = res
		perFile := el * 1000 / float64(nFiles)
		return benchResult{
			Target: "druid", TotalEvents: total, BatchSize: rowsPerFile,
			Concurrency: maxSub, ElapsedSec: el,
			EventsPerSec: float64(total) / el,
			P50Ms:        perFile, P95Ms: perFile, MeanMs: perFile,
			Notes: fmt.Sprintf("native-batch index_parallel task=%s files=%d", sub.Task, nFiles),
		}, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no helper host worked")
	}
	return benchResult{}, fmt.Errorf("druid ingestion failed (tried %v): %w", bases, lastErr)
}

func pollDruidTask(endpoint, taskID string, timeoutSecs int) (string, error) {
	if timeoutSecs <= 0 {
		timeoutSecs = 1800
	}
	client := &http.Client{Timeout: 30 * time.Second}
	deadline := time.Now().Add(time.Duration(timeoutSecs) * time.Second)
	for time.Now().Before(deadline) {
		u := fmt.Sprintf("%s/druid/indexer/v1/task/%s/status", strings.TrimRight(endpoint, "/"), taskID)
		resp, err := client.Get(u)
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var st struct {
			Status struct {
				Status string `json:"status"`
			} `json:"status"`
		}
		_ = json.Unmarshal(b, &st)
		s := strings.ToUpper(st.Status.Status)
		if s == "" {
			// older versions nest differently; dump raw for debugging
			time.Sleep(5 * time.Second)
			continue
		}
		fmt.Printf("   task %s status: %s\n", taskID, s)
		switch s {
		case "SUCCESS":
			return s, nil
		case "FAILED":
			return s, fmt.Errorf("task %s FAILED: %.800s (helper host unreachable? check helper_port + docker connectivity)", taskID, string(b))
		}
		time.Sleep(5 * time.Second)
	}
	return "", fmt.Errorf("task %s timed out after %ds", taskID, timeoutSecs)
}

// ------------------------------------------------------------------ misc ---

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	k := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if k < 0 {
		k = 0
	}
	if k >= len(sorted) {
		k = len(sorted) - 1
	}
	return sorted[k]
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := 0.0
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

func main() {
	cfgPath := flag.String("config", "ingestion-bench/config.yaml", "path to config yaml")
	outPath := flag.String("out", "inres.csv", "path to output csv")
	only := flag.String("only", "", "run only this target (clickhouse|druid), default: all enabled")
	flag.Parse()

	raw, err := os.ReadFile(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read config: %v\n", err)
		os.Exit(1)
	}
	var cfg Config
	if err := parseConfig(raw, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "parse config: %v\n", err)
		os.Exit(1)
	}
	base, err := time.Parse(time.RFC3339, cfg.Dataset.StartTime)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad dataset.start_time: %v\n", err)
		os.Exit(1)
	}
	span := time.Duration(cfg.Dataset.SpanDays) * 24 * time.Hour
	if span <= 0 {
		span = 7 * 24 * time.Hour
	}
	if cfg.Docker.ReadyTimeoutSecs <= 0 {
		cfg.Docker.ReadyTimeoutSecs = 300
	}
	// Accept compose_file relative to either the cwd or the config file's
	// dir, so the same config works from the repo root and from inside
	// ingestion-bench/.
	cfg.Docker.ComposeFile = resolveComposeFile(*cfgPath, cfg.Docker.ComposeFile)
	fmt.Printf("config %s: events=%d batch=%d seed=%d span=%dd concurrency=%d manage_docker=%v compose=%s\n",
		*cfgPath, cfg.Dataset.TotalEvents, cfg.Dataset.BatchSize, cfg.Dataset.Seed,
		cfg.Dataset.SpanDays, cfg.Bench.Concurrency, cfg.Docker.Manage, cfg.Docker.ComposeFile)
	for _, t := range cfg.Targets {
		fmt.Printf("  target %-12s enabled=%-5v endpoint=%s\n", t.Name, t.Enabled, t.Endpoint)
	}

	// pick targets sequentially (never in parallel: limited resources)
	var queue []Target
	for _, t := range cfg.Targets {
		if !t.Enabled {
			continue
		}
		if *only != "" && t.Name != *only {
			continue
		}
		queue = append(queue, t)
	}
	if len(queue) == 0 {
		fmt.Fprintln(os.Stderr, "no enabled targets (check config / --only)")
		os.Exit(1)
	}

	var results []benchResult
	failed := false
	for _, t := range queue {
		fmt.Printf("\n===== target: %s =====\n", t.Name)
		if cfg.Docker.Manage {
			if err := dockerUp(cfg, t.Name); err != nil {
				fmt.Fprintf(os.Stderr, "docker up %s: %v\n", t.Name, err)
				os.Exit(1)
			}
			// ensure teardown happens before next target even on failure
			// (no os.Exit in here: it would skip the deferred down)
			func() {
				defer func() {
					if err := dockerDown(cfg, t.Name); err != nil {
						fmt.Fprintf(os.Stderr, "docker down %s: %v\n", t.Name, err)
					}
				}()
				if err := waitReady(t.Name, t.Endpoint, cfg.Docker.ReadyTimeoutSecs); err != nil {
					fmt.Fprintf(os.Stderr, "wait ready: %v\n", err)
					failed = true
					return
				}
				r, err := runOne(cfg, t, base, span)
				if err != nil {
					fmt.Fprintf(os.Stderr, "benchmark %s failed: %v\n", t.Name, err)
					failed = true
					return
				}
				results = append(results, r)
				fmt.Printf(">> %s: %.0f ev/s (%.1fs for %d events)\n", r.Target, r.EventsPerSec, r.ElapsedSec, r.TotalEvents)
			}()
			if failed {
				os.Exit(1)
			}
		} else {
			r, err := runOne(cfg, t, base, span)
			if err != nil {
				fmt.Fprintf(os.Stderr, "benchmark %s failed: %v\n", t.Name, err)
				os.Exit(1)
			}
			results = append(results, r)
			fmt.Printf(">> %s: %.0f ev/s (%.1fs for %d events)\n", r.Target, r.EventsPerSec, r.ElapsedSec, r.TotalEvents)
		}
	}

	f, err := os.Create(*outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create out csv: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"target", "total_events", "batch_size", "concurrency", "elapsed_sec", "events_per_sec", "p50_batch_ms", "p95_batch_ms", "mean_batch_ms", "errors", "notes"})
	for _, r := range results {
		_ = w.Write([]string{
			r.Target,
			fmt.Sprint(r.TotalEvents),
			fmt.Sprint(r.BatchSize),
			fmt.Sprint(r.Concurrency),
			fmt.Sprintf("%.2f", r.ElapsedSec),
			fmt.Sprintf("%.1f", r.EventsPerSec),
			fmt.Sprintf("%.1f", r.P50Ms),
			fmt.Sprintf("%.1f", r.P95Ms),
			fmt.Sprintf("%.1f", r.MeanMs),
			fmt.Sprint(r.Errors),
			r.Notes,
		})
	}
	w.Flush()
	fmt.Printf("\nwrote %s (%d rows)\n", *outPath, len(results))
}

func runOne(cfg Config, t Target, base time.Time, span time.Duration) (benchResult, error) {
	switch t.Name {
	case "clickhouse":
		return benchClickHouse(cfg, t, base, span)
	case "druid":
		return benchDruid(cfg, t, base, span)
	default:
		return benchResult{}, fmt.Errorf("unknown target %q (want clickhouse|druid)", t.Name)
	}
}

// ------------------------------------------------- stdlib yaml subset ---
//
// Minimal YAML parser for the config shape we ship (nested maps, lists of
// maps/scalars, quoted scalars, flow `{a: b, ...}` / `[a, b]`). Stdlib-only
// so `go run ingestion-bench/main.go` works with no go.mod / no downloads.

type yline struct {
	indent int
	text   string // left-trimmed, comments stripped
}

func stripComment(s string) string {
	var inS, inD bool
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\'' && !inD:
			inS = !inS
		case c == '"' && !inS:
			inD = !inD
		case c == '#' && !inS && !inD && (i == 0 || s[i-1] == ' ' || s[i-1] == '\t'):
			return strings.TrimRight(s[:i], " \t")
		}
	}
	return s
}

func isListItem(t string) bool { return t == "-" || strings.HasPrefix(t, "- ") }

func splitKeyValue(t string) (string, string, bool) {
	// split at first ':' that ends the key (colon + space/tab/EOL, quotes respected)
	var inS, inD bool
	for i := 0; i < len(t); i++ {
		c := t[i]
		switch {
		case c == '\'' && !inD:
			inS = !inS
		case c == '"' && !inS:
			inD = !inD
		case c == ':' && !inS && !inD && (i+1 == len(t) || t[i+1] == ' ' || t[i+1] == '\t'):
			k := strings.TrimSpace(t[:i])
			v := strings.TrimSpace(t[i+1:])
			return unquote(k), unquote(v), true
		}
	}
	return "", "", false
}

func unquote(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			inner := s[1 : len(s)-1]
			if s[0] == '"' {
				if u, err := strconv.Unquote(s); err == nil {
					return u
				}
			}
			return inner
		}
	}
	return s
}

func splitTopLevel(s string, sep byte) []string {
	var parts []string
	var inS, inD bool
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\'' && !inD:
			inS = !inS
		case c == '"' && !inS:
			inD = !inD
		case !inS && !inD && (c == '{' || c == '['):
			depth++
		case !inS && !inD && (c == '}' || c == ']'):
			depth--
		case !inS && !inD && depth == 0 && c == sep:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

func parseScalarOrFlow(s string) any {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}' {
		m := map[string]any{}
		for _, p := range splitTopLevel(s[1:len(s)-1], ',') {
			if k, v, ok := splitKeyValue(strings.TrimSpace(p)); ok {
				m[k] = parseScalarOrFlow(v)
			}
		}
		return m
	}
	if len(s) >= 2 && s[0] == '[' && s[len(s)-1] == ']' {
		var l []any
		for _, p := range splitTopLevel(s[1:len(s)-1], ',') {
			if strings.TrimSpace(p) == "" {
				continue
			}
			l = append(l, parseScalarOrFlow(p))
		}
		return l
	}
	return unquote(s)
}

// parseNode parses a block/list whose lines all have indent > parentIndent.
func parseNode(lines []yline, pos *int, parentIndent int) any {
	if *pos < len(lines) && lines[*pos].indent > parentIndent && isListItem(lines[*pos].text) {
		var list []any
		for *pos < len(lines) && lines[*pos].indent > parentIndent {
			if !isListItem(lines[*pos].text) {
				break
			}
			itemIndent := lines[*pos].indent
			rest := strings.TrimSpace(strings.TrimPrefix(lines[*pos].text, "-"))
			(*pos)++
			if rest == "" {
				// nested block on following lines
				if *pos < len(lines) && lines[*pos].indent > itemIndent {
					list = append(list, parseNode(lines, pos, itemIndent))
				} else {
					list = append(list, "")
				}
				continue
			}
			if k, v, ok := splitKeyValue(rest); ok {
				m := map[string]any{}
				if v != "" {
					m[k] = parseScalarOrFlow(v)
				} else if *pos < len(lines) && lines[*pos].indent > itemIndent {
					m[k] = parseNode(lines, pos, itemIndent)
				} else {
					m[k] = ""
				}
				// continuation keys of the same map item
				for *pos < len(lines) && lines[*pos].indent > itemIndent && !isListItem(lines[*pos].text) {
					if ck, cv, ok := splitKeyValue(lines[*pos].text); ok {
						(*pos)++
						if cv != "" {
							m[ck] = parseScalarOrFlow(cv)
						} else if *pos < len(lines) && lines[*pos].indent > lines[*pos-1].indent {
							m[ck] = parseNode(lines, pos, lines[*pos-1].indent)
						} else {
							m[ck] = ""
						}
					} else {
						break
					}
				}
				list = append(list, m)
				continue
			}
			list = append(list, parseScalarOrFlow(rest))
		}
		return list
	}
	m := map[string]any{}
	for *pos < len(lines) && lines[*pos].indent > parentIndent && !isListItem(lines[*pos].text) {
		indent := lines[*pos].indent
		k, v, ok := splitKeyValue(lines[*pos].text)
		if !ok {
			(*pos)++
			continue
		}
		(*pos)++
		if v != "" {
			m[k] = parseScalarOrFlow(v)
			continue
		}
		if *pos < len(lines) && lines[*pos].indent > indent {
			m[k] = parseNode(lines, pos, indent)
		} else {
			m[k] = ""
		}
	}
	return m
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func yStr(m map[string]any, key string) string {
	if s, ok := m[key].(string); ok {
		return s
	}
	if m[key] != nil {
		return fmt.Sprint(m[key])
	}
	return ""
}

func yInt(m map[string]any, key string, def int) int {
	switch v := m[key].(type) {
	case nil:
		return def
	case string:
		if v == "" {
			return def
		}
		if i, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return int(f)
		}
		return def
	case int:
		return v
	default:
		if i, err := strconv.Atoi(fmt.Sprint(v)); err == nil {
			return i
		}
		return def
	}
}

func yInt64(m map[string]any, key string, def int64) int64 {
	if m[key] == nil {
		return def
	}
	if i, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(m[key])), 10, 64); err == nil {
		return i
	}
	return def
}

func yBool(m map[string]any, key string, def bool) bool {
	if m[key] == nil {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(fmt.Sprint(m[key]))) {
	case "true", "yes", "y", "1", "on":
		return true
	case "false", "no", "n", "0", "off":
		return false
	}
	return def
}

func yStrList(v any) []string {
	var out []string
	if l, ok := v.([]any); ok {
		for _, e := range l {
			out = append(out, fmt.Sprint(e))
		}
	}
	return out
}

func parseConfig(raw []byte, cfg *Config) error {
	// JSON fallback (same snake_case keys).
	if t := bytes.TrimSpace(raw); len(t) > 0 && t[0] == '{' {
		var m map[string]any
		if err := json.Unmarshal(t, &m); err != nil {
			return err
		}
		return fillConfig(m, cfg)
	}
	var lines []yline
	for _, rl := range strings.Split(string(raw), "\n") {
		rl = strings.TrimRight(rl, "\r")
		if strings.TrimSpace(rl) == "" || strings.HasPrefix(strings.TrimSpace(rl), "#") {
			continue
		}
		trimmed := stripComment(rl)
		if strings.TrimSpace(trimmed) == "" {
			continue
		}
		indent := len(trimmed) - len(strings.TrimLeft(trimmed, " "))
		if strings.Contains(trimmed, "\t") {
			return fmt.Errorf("tabs not supported in config yaml (line %q)", rl)
		}
		lines = append(lines, yline{indent: indent, text: strings.TrimSpace(trimmed)})
	}
	pos := 0
	root, _ := parseNode(lines, &pos, -1).(map[string]any)
	if root == nil {
		return fmt.Errorf("empty config")
	}
	return fillConfig(root, cfg)
}

func fillConfig(root map[string]any, cfg *Config) error {
	d := asMap(root["dataset"])
	cfg.Dataset.TotalEvents = yInt(d, "total_events", 500000)
	cfg.Dataset.BatchSize = yInt(d, "batch_size", 10000)
	cfg.Dataset.Seed = yInt64(d, "seed", 42)
	cfg.Dataset.StartTime = yStr(d, "start_time")
	if cfg.Dataset.StartTime == "" {
		return fmt.Errorf("dataset.start_time missing")
	}
	cfg.Dataset.SpanDays = yInt(d, "span_days", 7)

	b := asMap(root["bench"])
	cfg.Bench.Concurrency = yInt(b, "concurrency", 4)
	cfg.Bench.TimeoutSeconds = yInt(b, "timeout_seconds", 1800)
	cfg.Bench.DruidMaxNumConcurrentSubtasks = yInt(b, "druid_max_num_concurrent_subtasks", 2)
	cfg.Bench.DruidSegmentGranularity = yStr(b, "druid_segment_granularity")
	cfg.Bench.DruidRowsPerFile = yInt(b, "druid_rows_per_file", 50000)
	cfg.Bench.HelperHostCandidates = yStrList(b["helper_host_candidates"])
	cfg.Bench.HelperPort = yInt(b, "helper_port", 18089)

	if tl, ok := root["targets"].([]any); ok {
		for _, e := range tl {
			m := asMap(e)
			cfg.Targets = append(cfg.Targets, Target{
				Name:       yStr(m, "name"),
				Enabled:    yBool(m, "enabled", true),
				Endpoint:   yStr(m, "endpoint"),
				Database:   yStr(m, "database"),
				Table:      yStr(m, "table"),
				User:       yStr(m, "user"),
				Password:   yStr(m, "password"),
				Datasource: yStr(m, "datasource"),
			})
		}
	}

	dk := asMap(root["docker"])
	cfg.Docker.Manage = yBool(dk, "manage", true)
	cfg.Docker.ComposeFile = yStr(dk, "compose_file")
	if cfg.Docker.ComposeFile == "" {
		cfg.Docker.ComposeFile = "ingestion-bench/docker-compose.yml"
	}
	cfg.Docker.DownAfterEach = yBool(dk, "down_after_each", true)
	cfg.Docker.ReadyTimeoutSecs = yInt(dk, "ready_timeout_seconds", 300)
	return nil
}
