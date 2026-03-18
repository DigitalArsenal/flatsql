# FlatSQL Performance & Cluster Guidance

## Benchmark Matrix & Gates

- `npm run bench:perf` (alias: `npm run bench`) compares the JS B-tree fallback (`FlatSQLDatabase` from `src`) against the WASM/SQLite native engine (`wasm/index.js`) using the deterministic workload inside `bench/flatsql-perf.mjs`.
- For each scenario the script performs three runs per path, validates correctness, and prints medians in a labeled table. It enforces merge gates by ensuring `js_median / wasm_median` stays above 2.0 for the 10k sorted/1-index run and above 1.5 for the 100k unsorted 3-index run. The `sorted-1idx-easy` stretch case targets ≥3.0×.
- All instrumentation is FlatBuffer-first. The WASM path now uses the native bulk ingest route rather than looping over `ingestOne(...)`.
- Use `npm run bench:perf:profile` to print the end-to-end gate table plus the WASM ingest phase breakdown (`pack`, `decode`, `append`, `index`, `verify`).
- Profiling mode is diagnostic and slower by design because it enables native phase counters. Treat `npm run bench:perf` as the merge-gate signal.

## Join Tables for JSON Schemas

- The JSON schema parser (`src/schema/parser.ts`) now emits join tables for `$ref` relationships. Each reference adds a dedicated `From_Target_join` table with `{FromRowId, TargetRowId}` columns so that downstream schemas can perform relational joins natively instead of embedding raw JSON blobs.

## Cluster Mode Notes

- FlatSQL cluster mode is strictly SQLite-style shared-storage concurrency:
  - WAL journal mode with `PRAGMA journal_mode=WAL` (enabled in the native engine configuration when moving off `:memory:`)
  - SQLite file locking is honored; each worker/thread uses its own connection handle
  - One writer at a time, short-lived transactions; readers may proceed concurrently
  - SQLITE_BUSY / SQLITE_LOCKED are retried with bounded backoff rather than custom global mutexes
- Browser cluster mode (same-origin Web Workers + OPFS) defers to the WASM/native path. The main thread stays off the hot path, a dedicated writer worker handles ingest, readers use SharedArrayBuffer + Atomics for coordination, and the runtime rejects execution if the environment lacks cross-origin isolation or SharedArrayBuffer.
- Use `src/cluster/index.ts` to detect runtime support; it checks SharedArrayBuffer, Atomics, cross-origin isolation, and OPFS so that cluster mode fails closed in environments that cannot guarantee SQLite-safe locking.

## Native Cluster Validation

- Run `npm run test:cluster` to build and execute the native contention harness.
- Run `npm run test:cluster:smoke` for the short preflight version.
- Current reference result for the implemented native validator:
  - base workload: `60s`, `1 writer + 8 readers + verifier`, `0` misses, `0` verifier failures, `0` stalls
  - stretch workload: `30s`, `1 writer + 12 readers + verifier`, `0` misses, `0` verifier failures, `0` stalls

## Next Steps

- Re-run `npm run bench:perf` after any ingest/index optimization. If a gate fails, the script exits non-zero and prints a detailed diagnostic (`GATE FAILED: ...`).
