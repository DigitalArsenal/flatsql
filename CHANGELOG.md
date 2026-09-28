# Changelog

## 3.0.0

- The partition store ships as `wasm/flatsql-ps-threads.wasm` (`flatsql/ps-threads.wasm`):
  a `wasm32-wasip1-threads` reactor built with wasi-sdk 30, reproducible, whose imports are
  WASI preview1, `wasi.thread-spawn`, a shared `env.memory` (at most 32768 pages) and the
  seven `env.flatsql_io_*` (docs/PARTITION-STORE-WASM.md).
- `flatsql/ps/node-host`: the Node wasi-threads host (thread pool, WASI, the
  space-data-module-sdk sync-fs flatsql_io provider, the fault overlay `flatsql/ps/fault`).
- `wasm/integrity.json` (`flatsql/integrity.json`) lists the sha256, sha384 SRI and size of
  every shipped wasm.
- **Breaking.** The package root is the wasm bindings (`wasm/index.js` plus the partition store
  locator). Removed: the TypeScript engine (`FlatSQLDatabase`, `DirectAccessor`,
  `FlatcAccessor`, `TableStore` from `src/`), the `node:sqlite` artifact builder
  (`FlatSQLArtifactBuilder`), `flatsql/standalone` (`wasm/standalone.js`),
  `flatsql/artifacts/standalone`, and the `sql.js` and `flatbuffers` dependencies. The BTree,
  stacked store, schema parser and cluster detection moved to `flatsql/btree`,
  `flatsql/storage`, `flatsql/schema` and `flatsql/cluster`.
- `npm run bench` measures the WASM engine only (the ratio gates compared it with the removed
  TypeScript engine); docs/performance.md no longer claims a WAL cluster mode.
