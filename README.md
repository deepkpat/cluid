# ingestion-bench

Sequential ingestion benchmark for **ClickHouse** vs **Druid**: generates deterministic synthetic events (medium-complexity ~20-column e-commerce/telemetry schema) and measures bulk-insert throughput per engine. Engines run **one at a time** via Docker Compose profiles, so a small box is never asked to host both.

No Go dependencies — only a Go toolchain and Docker required.

## Layout

| File | Purpose |
|---|---|
| `main.go` | Benchmark runner (stdlib only) |
| `config.yaml` | Full run: 500k rows |
| `config.mini.yaml` | Smoke test: 20k rows |
| `docker-compose.yml` | Engine containers (profiles `clickhouse` / `druid`, CPU+memory capped) |
| `druid-conf/` | Druid config overrides (shared metadata DB, slim peon heaps) |
| `druid-input/` | Staging dir for Druid input files (created/populated/cleaned at runtime) |
| `inres-*.csv` | Results |
| `report-mini.md` | Result notes |

## Quickstart

From inside `ingestion-bench/`:

```bash
# smoke test, ClickHouse only (~1 min + container boot)
go run . --config config.mini.yaml --out inres-mini-clickhouse.csv --only clickhouse

# smoke test, Druid only (cluster boot takes a few minutes)
go run . --config config.mini.yaml --out inres-mini-druid.csv --only druid

# full runs (both engines, sequentially)
go run . --config config.yaml --out inres.csv
```

From the repo root (`/mnt/d/projects`) the same works in file mode:

```bash
go run ingestion-bench/main.go --config ingestion-bench/config.yaml --out inres.csv
```

Useful flags: `--only clickhouse|druid`, `--config`, `--out`.

## How it works

1. For each enabled target, in order: `docker compose --profile <name> up -d` → wait for `/ping` (ClickHouse) or `/status` + coordinator leader (Druid) → benchmark → `down` before the next engine starts.
2. **ClickHouse**: concurrent HTTP `INSERT ... FORMAT JSONEachRow` (batch size × workers from config). Reports per-batch p50/p95/mean.
3. **Druid**: one native-batch `index_parallel` task. Input NDJSON files are written to `druid-input/`, served by a `fileserver` sidecar on the compose network (reliable on Docker Desktop, where container→host networking is flaky), with the Go helper HTTP server as fallback. Reports task wall time.
4. Results appended as one CSV row per engine: `target,total_events,batch_size,concurrency,elapsed_sec,events_per_sec,p50/p95/mean_batch_ms,errors,notes`.

Data is deterministic (seeded generator), so reruns are comparable. Timings exclude container boot — they cover ingestion only.

## Services & resources

- **ClickHouse** (1 container, 4 CPU / 4 GB): `clickhouse/clickhouse-server:24.12-alpine`, user `bench`/`benchpass`, DB `bench` (the image blocks network access for the password-less default user).
- **Druid** (9 containers, ~6 GB caps total): `apache/druid:34.0.0` services (coordinator, overlord, broker, router `:8888`, historical, middleManager) + `zookeeper:3.5.10` + postgres metadata DB + `fileserver` sidecar. Single-container micro-quickstart is not possible (image has no perl for the supervise launcher), hence the multi-container layout with shared `/opt/druid/var` volume.

Tune sizes, batching, and credentials in `config.yaml` / `config.mini.yaml`.

## Caveats

- Druid native batch has ~40 s fixed task cost (peon fork, segment commit) that dominates small runs — compare at 500k+ rows.
- Single runs have no variance data; repeat ≥3× per engine before concluding.
- `druid-input/*.ndjson` and helper port `18089` are used at runtime; input files are deleted after each run.
