# Ingestion benchmark report

Sources: `inres-mini-clickhouse.csv`, `inres-mini-druid.csv` (20k rows), `inres-clickhouse.csv` (500k rows). Medium-complexity ~20-col schema, seed 42.

## Mini runs (20,000 events)

| target     | rows   | batch/file | workers | elapsed | throughput | p50 batch | p95 batch | errors |
|------------|--------|------------|---------|---------|------------|-----------|-----------|--------|
| clickhouse | 20,000 | 2,000      | 2       | 0.38 s  | 52,520 ev/s | 39.0 ms  | 48.3 ms   | 0      |
| druid      | 20,000 | 10,000     | 1       | 41.08 s | 487 ev/s    | ~20.5 s* | ~20.5 s*  | 0      |

\* Druid ran a single `index_parallel` task over 2 files; per-"batch" = elapsed/files. Not comparable to per-INSERT latency.

## What was measured

- **ClickHouse**: concurrent HTTP `INSERT ... FORMAT JSONEachRow` (10 batches × 2k rows, 2 workers). Elapsed = insert loop only (excludes container boot, table DDL).
- **Druid**: one native-batch `index_parallel` task, input pulled by peons from the fileserver sidecar. Elapsed = task submit → SUCCESS (excludes cluster boot ~35 s, input file generation).

## Read these numbers with care

1. **Different ingestion models.** ClickHouse streaming inserts have near-zero fixed cost; Druid native batch pays task startup + peon JVM fork + segment commit/handoff (~35–40 s) regardless of row count. At 20k rows the fixed cost dominates Druid's number (~2.05 ms/row vs ~19 µs/row).
2. **Single run, no repeats.** No variance data; treat throughput as indicative, not significant.
3. **Different footprints.** ClickHouse ran as 1 container (4 CPU / 4 GB cap); Druid as 9 containers (~6 GB caps total) on a 7.4 GB / 8 CPU Docker Desktop host.
4. **Batch sizes differ** (2k vs 10k/file) — chosen per engine, not normalized.

## Full run — ClickHouse (500,000 events)

Source: `inres-clickhouse.csv` (`config.yaml`: 50 batches × 10k rows, 4 workers).

| target     | rows    | batch  | workers | elapsed | throughput   | p50 batch | p95 batch | errors |
|------------|---------|--------|---------|---------|--------------|-----------|-----------|--------|
| clickhouse | 500,000 | 10,000 | 4       | 4.01 s  | 124,603 ev/s | 147.8 ms  | 201.1 ms  | 0      |

Throughput more than doubled vs the mini run (52.5k → 124.6k ev/s): the larger 10k batches amortize per-request HTTP overhead, and 4 workers keep the server fed. Per-batch p95 (201 ms) stays well-behaved — no long-tail batches.

Druid full run (500k rows, 10 × 50k files) still pending — that's the comparison that amortizes Druid's ~40 s fixed task cost.

## Takeaway & next step

Both engines ingested 20k rows error-free; ClickHouse is ~108× faster **at this size**, but the mini run measures Druid's task overhead more than its row throughput. ClickHouse holds up at 500k rows (124.6k ev/s). Still pending: the Druid 500k run, then repeat ≥3× per engine before drawing conclusions.
