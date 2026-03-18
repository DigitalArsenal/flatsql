# TODO

Last updated: 2026-03-17

## Current State

- Schema parser now guards JSON schema metadata with a typed `JSONSchemaProperty` interface, so `$ref` join tables are only produced when `parseJsonRef` returns a real target.
- `npm test` passed for the current validation set (`wasi`, `basic`, `schema-joins`) when this state was recorded; existing `ts-jest` warnings were unchanged.
- Benchmark harness (`bench/flatsql-perf.mjs`) now runs only FlatSQL JS vs WASM with deterministic scenarios.
- Benchmark merge gates are enforced as `js_median / wasm_median`:
  - `>= 2.0x` for the 10k sorted / 1-index reference scenario
  - `>= 1.5x` for the 100k unsorted / 3-index scenario
  - `>= 3.0x` stretch target for the easy sorted / 1-index case
- Benchmark correctness is asserted on every run.
- `JSON+SQLite` and `bench/baseline.json` were removed from the benchmark path.
- Documentation in `docs/performance.md` now reflects the updated gate matrix and cluster guidance, with FlatBuffers positioned as the backing format and browser cluster requirements called out explicitly.

## Execution Order

1. Cluster validation first.
2. Then start step 1 (real FlatSQL WASM ingest path and hotspot profiling).

## Status Update

- Cluster validation is now implemented in native C++ at `cpp/test/cluster_validation_test.cpp`.
- Native SQLite cluster support for validation now uses:
  - WAL-capable native SQLite build
  - thread-safe native SQLite build
  - file-backed SQLite connections with busy timeout + bounded retry/backoff
  - shared runtime storage guarded by a shared read/write lock across DB instances
- Long validation run completed successfully:
  - base: 60s, 1 writer + 8 readers + verifier
  - stretch: 30s, 1 writer + 12 readers + verifier
  - result: `0` reader misses, `0` verifier failures, `0` stall detections
  - base throughput:
    - writes: `9164/s`
    - reads: `36721/s`
    - verifies: `5149.95/s`
  - stretch throughput:
    - writes: `6900.1/s`
    - reads: `48497/s`
    - verifies: `4378.93/s`
- Step 1 has started:
  - `wasm/index.js` now exposes `ingestBuffers(buffers, source?)` to drive the native bulk ingest path through a size-prefixed stream.
  - Native ingest phase counters (`decode`, `append`, `index`) are now exposed through the WASM wrapper for diagnostic profiling.
  - `bench/flatsql-perf.mjs` now uses the bulk WASM ingest path instead of per-record `ingestOne()` loops.
  - `npm run bench:perf:profile` now prints the WASM ingest phase breakdown. It is diagnostic only and intentionally includes instrumentation overhead.
  - `npm run test:wasm` now validates the shipped WASM wrapper bulk-ingest surface directly.
  - Full benchmark matrix passed on the bulk path with these ratios:
    - `10k sorted (1-index)`: `3.49x`
    - `100k unsorted (3-index)`: `10.45x`
    - `sorted 1-index stretch`: `5.20x`
  - `npm test` passed after these changes.

## Next Steps Toward Merge Readiness

1. Re-run `bench/flatsql-perf.mjs` and the cluster workload after any further ingest-path changes, then refresh the recorded numbers.
2. Assemble the merge-ready summary/metrics from the final benchmark + cluster runs.

## Notes

- There is already a basic cluster runtime capability test at `test/cluster-mode.test.ts`.
- The native cluster validation harness is not yet wired into `ctest`; it is currently a dedicated executable (`flatsql_cluster_validation`) because the full workload is long-running by design.
