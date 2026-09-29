# Changelog

## 3.4.0

- Format-1 arena compaction (docs/STORAGE-DURABILITY.md §6.4.2): `compactArena` /
  C API `flatsql_compact_arena(handle, maxStepBytes)` packs the record arena in place with
  only the rows a query can still see. Every surviving row keeps its sequence (sequence runs
  in `_flatsql_seq_runs`, `next_sequence` in `_flatsql_state`; a state with runs writes
  `format_version` 2, which older builds refuse with -2 instead of renumbering). Offsets are
  rewritten everywhere they live: index rows' `data_offset`, the partition map, per-table
  record lists; tombstones of dropped rows go with them. Nothing is allocated for the bytes
  (the peak is the arena already there). It runs in bounded I/O steps with reads, ingests,
  tombstones and flushes allowed between them; inside an open transaction it packs in memory
  and returns 2, and the persist runs at the next call outside a transaction or at the next
  flush. The persist is a redo log (`<db>.fsdata.compact`, one `synchronous=FULL` layout
  commit with `compact_pending`, in-place copy, cut at the mark) that `flatsql_open_state`
  finishes after a crash at any point. `flatsql_arena_stat(handle, which)` reports the arena
  (size, capacity, records, dead bytes) and the last compaction.
- WAL restored on the wasi engines (`flatsql-wasi.wasm`, `flatsql-wasi-noeh.wasm`): the VFS
  implements `xShm*` on the heap (the single-connection case SQLite's own unix VFS handles
  the same way) and `SQLITE_OMIT_WAL` is off those two targets (feat/wal-heap-shm, 51471e7,
  cherry-picked). SDN's embedded engine (2.0.3 plus that commit) opens its control database
  in `journal_mode=WAL`; every release since 2.0.3 had built these targets without WAL and
  could not have opened it.
- A partition's virtual table shows only its own rows: an index scan or a rowid lookup on
  `Table@source` used to return rows of every partition sharing the base table's index table
  (and, by rowid, rows of other tables).
- Index inserts are `INSERT OR REPLACE` on (key, sequence): the tail past the mark replayed at
  open after a crash between the stream's fsync and the mark's commit no longer throws (a
  trap on the no-exceptions artifact).
- Artifacts (sha256): `wasm/flatsql.wasm`
  `eb61e4d65b90e3f5c5651a820b74f85fa200e3e46a0cbd9417fb82ed2db85bf8`, `wasm/flatsql-wasi.wasm`
  `8e6a491141fa8334bcdb4fe2d3c848ea3fe7255b48fd6e9a1e012e873bca8c7a`, `wasm/flatsql-wasi-noeh.wasm`
  `8f11fd49ee2e6961b9c1c22dd891d5a6645d0faf481f815408c0f1f5d3a385ab` (emscripten/emsdk 4.0.23, FlatBuffers 8af3053e).
- Partition store: a reader's index state is bounded by bytes, not by the store
  (docs/PARTITION-STORE.md §37); `flatsql-ps-threads.wasm` rebuilt for it.

## 3.3.1

- flatsql_sdn_node links flatbuffers util.cpp again; no published artifact changes.
  (`idl_parser.cpp` needs its `AbsolutePath` and `ClassicLocale::instance_`; a9c0943 had
  dropped it from that target. `wasm/flatsql.wasm`, `flatsql-wasi.wasm`,
  `flatsql-wasi-noeh.wasm` and `flatsql-ps-threads.wasm` are 3.3.0's.)

## 3.3.0

- Database-key record encryption follows FlatBuffers field-encryption format 3: each record is
  encrypted under its own key (DeriveBufferKey(sequence), the record index being its rowid by
  stream order) with an IV per instance position. `ingestOneEncrypted`, `encryptRecord` and
  `decryptRecord` take the record index; a database holding records in tables with
  (encrypted) columns must declare stored format 3, and format 2 is refused (README
  "Database-key encryption" has the migration from 3.2.0). C API:
  `flatsql_set_encryption_key` (+storedFormat), `flatsql_encrypt_buffer` and
  `flatsql_decrypt_buffer` (+recordIndex), `flatsql_ingest_one_encrypted`.
- Every build derives keys with HKDF-SHA256: the wasm builds (`flatsql.wasm`,
  `flatsql-wasi.wasm`, `flatsql-wasi-noeh.wasm`) take FlatBuffers 8af3053e, whose fallback
  backend implements HKDF-SHA256, AES-256-CTR and HMAC-SHA256, so they accept a key for a
  database with (encrypted) columns; native builds without a real HKDF-SHA256 refuse one.
  `computeHMAC`/`verifyHMAC` use `flatbuffers::HMACSha256` in every build.
- Partition store (docs/PARTITION-STORE.md §32-§35): the hot-partition split with stage-1
  helpers and prep states, arrivals compaction (A15: sealed arrivals segments rewritten
  without the GONE and REHOME postings they cover), the L0 accelerator fix, prefix-key sort
  for random-key posting buckets, and `flatsql_ps_stats` entries 26-35.
- Artifacts (sha256): `wasm/flatsql.wasm`
  `45e291c9293fc0c2c27e87cd410c813cd54f97ba6c3459d328122c1c37909d1f`, `wasm/flatsql-wasi.wasm`
  `989ea9fc7be9539bda7878e5202bee46b4ba45fd59d8718b01a2a8a6c7cb1afd`,
  `wasm/flatsql-wasi-noeh.wasm` `7315eb0361513578c7a011db8b1e10a7d41625e8fae59c276b61e2d1529bb591`,
  `wasm/flatsql-ps-threads.wasm` `e61120642b300997f59ef064d7cc6375bfc937e7cc4d551b47695f6d23ef03e6`
  (2,325,490 bytes).

## 3.2.0

- store-migrate gseqs: `RecordAttr.migrated_gseq` (field 6) on a FIRST copy keeps that gseq
  (the legacy `sdn_record_index.rowid`) when it is above the type's committed `gseq_hi`; any
  other case allocates and counts a fallback. `flatsql_ps_stats` appends entries 24
  (migrated gseqs used) and 25 (fallbacks); a type commit's arrivals are sorted by gseq
  (docs/PARTITION-STORE.md §31).
- Writer init TLV 19: `maxEntryBytes` (u64, clamped to [64 KiB, 1 GiB]; default 1 MiB + 4 KiB).
- Tag conditions (`_provider`, `_batch`, `_peer_id`, `_source`, `_source_name`) match when one
  live tag instance (the PUT or a RETAG) satisfies all of them, the legacy ANY-row semantics
  (§31.1).
- `wasm/flatsql-ps-threads.wasm` sha256
  `a90d9488187e527bccd3f02ce3f68be6443690bc1a2ee6c6786c64c907376e36` (2,261,085 bytes).

## 3.1.0

- The partition store gains compaction, reclamation and quota (docs/PARTITION-STORE.md
  Part III): live-frame compaction of sealed segments behind an INTENT_COMPACT/SWAP pair,
  retired files unlinked behind the reader gate, meta-segment and type-log retirement,
  exact `disk_bytes` per partition and type, arrival-order quota eviction to 0.85 of the
  cap, and a space emergency with a ballast file on ENOSPC.
- Two new exports on `flatsql-ps-threads.wasm`: `flatsql_ps_reader_gate(oldestStartNs)`
  (the host passes the oldest running reader statement's start, so retired files outlive
  every statement that may read them) and `flatsql_ps_set_quota(bytes)`.
- `wasm/flatsql-ps-threads.wasm` sha256
  `075dd0a104694df39b2776c34dc6444be224442717967d9cea5dd9af15964379` (2,237,656 bytes).

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
