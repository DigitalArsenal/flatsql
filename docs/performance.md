# FlatSQL Performance & Concurrency

FlatSQL ships two wasm engines:

- **The SQLite-backed engine** (`wasm/flatsql.wasm` for browsers,
  `wasm/flatsql-wasi.wasm` and `flatsql-wasi-noeh.wasm` for WASI hosts): one
  database per instance, single-threaded (`SQLITE_THREADSAFE=0`).
- **The partition store** (`wasm/flatsql-ps-threads.wasm`, wasm32-wasip1-threads):
  one writer per (producer, SDS type) partition on a pinned pool of writer
  threads, append-only logs with durable acks, and reader lanes that read
  committed snapshots and never wait on writers. Design and measured numbers:
  [PARTITION-STORE.md](PARTITION-STORE.md) (engine, native) and
  [PARTITION-STORE-WASM.md](PARTITION-STORE-WASM.md) (artifact and hosts).

The TypeScript engine (`FlatSQLDatabase` from `src/`) was removed in 3.0.0.

## Benchmark

- `npm run bench:perf` (alias `npm run bench`) times bulk ingest into the
  SQLite-backed wasm engine (`wasm/index.js`) over the deterministic OMM
  workload in `bench/flatsql-perf.mjs`: three runs per scenario, the median
  and records per second. It has no pass/fail gate. The gates it used to
  enforce were ratios against the TypeScript engine, which no longer exists.
- `npm run bench:perf:profile` adds the native phase breakdown (`pack`,
  `decode`, `append`, `index`, `verify`). Profiling is diagnostic and slower
  by design.
- The partition store's benchmarks are native (`cpp/build/flatsql_ps_bench`,
  PARTITION-STORE.md §8). They are measured on the machine each acceptance row
  names, never on the Mac for durability numbers (`F_FULLFSYNC` dominates).

## Join Tables for JSON Schemas

- The JSON schema parser (`src/schema/parser.ts`, `flatsql/schema`) emits join
  tables for `$ref` relationships. Each reference adds a dedicated
  `From_Target_join` table with `{FromRowId, TargetRowId}` columns so that
  downstream schemas can perform relational joins natively instead of
  embedding raw JSON blobs.

## Concurrency

- **No SQLite WAL.** Every FlatSQL build compiles `SQLITE_OMIT_WAL`; earlier
  text here describing a WAL cluster mode was wrong. SQLite is an in-memory
  query layer, not the storage.
- **SQLite-backed engine:** one instance serves one caller at a time. Hosts
  that need concurrency run separate instances; there is no shared-file
  locking between them.
- **Partition store:** writers never share a lock across partitions; readers
  take no lock a writer takes (T2 #1 measures the lock sets disjoint); a
  record is acked only after its durable append. Browsers get a local store
  only when cross-origin isolated, with shared memory and shared OPFS sync
  handles (design §5.5, ruling 6); otherwise reads go to the network.
- `src/cluster/index.ts` (`flatsql/cluster`) detects SharedArrayBuffer,
  Atomics, cross-origin isolation and OPFS so that browser callers fail closed.

## Native Cluster Validation (native library)

- `npm run test:cluster` builds and runs the native contention harness;
  `npm run test:cluster:smoke` is the short preflight.
- Reference result: base workload `60s`, `1 writer + 8 readers + verifier`,
  `0` misses, `0` verifier failures, `0` stalls; stretch workload `30s`,
  `1 writer + 12 readers + verifier`, `0` misses, `0` verifier failures,
  `0` stalls.
