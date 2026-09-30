# Changelog

## 3.6.0

- Store format level 3 (TB03, docs/PARTITION-STORE.md §41): a partition no longer stops
  committing past 1,170 live lanes. A batch writes only the lanes it changed (56 B for a
  one-lane batch at 66 lanes or at 5,000, where level 2 wrote 3,760 B at 66 and never acked
  past 1,170). Past 32 live lanes the head names a paged lane checkpoint `lk-<gen>.fsl` and a
  replay offset; checkpoints are cut by cadence (writer TLV 32, default 128 batches with lane
  deltas, 512 in all), before a meta segment they replay from retires, and after the crossing,
  under a crash protocol (write and sync, a durable head names it, then RETIRE the one it
  replaced; no cut while that RETIRE waits). Replay, readers, open and checkpoints fold lane
  tables by one rule; a lane at count 0 starts afresh when it comes back.
- Format levels (`format_level.h`, kFormatMax 3): STORE.format is the store's level. An open
  raises it to writeFormat (writer TLV 31, default kFormatMax) once the registry is non-empty,
  through STORE.tmp and an in-place rewrite; a fresh store is created at it. Older engines
  refuse a raised store without creating a file (3.5.1: "STORE is corrupt"); a host pinned at
  2 keeps its level; a raise that finds no room for STORE.tmp waits for the next open; a torn
  STORE is finished from STORE.tmp. Manifest version 3 is written from level 3. Writer stats
  entries 36-40: the store's level, kFormatMax, the level this open raised STORE from, lane
  checkpoints cut and their bytes (41 entries).
- Live-only candidate caps (N2): supersede, licence, control, CID and tag-instance lookups no
  longer let dead postings crowd out the live row (a CAT key superseded 20 times kept 12 live
  versions). A supersede walks the key's sources newest first and stops at its newest live
  row: a PUT of a key with 800 dead versions costs 2.3x a new key's, not 12.8x.
- A batch that minted lanes and failed rolls `nextLaneId` back to its first minted id; a
  LaneRef head in a store below level 3 quarantines the partition instead of failing the open;
  readers apply the writer's STORE rules.
- `wasm/flatsql-ps-threads.wasm` is rebuilt from this source (Linux wasi-sdk 30), sha256
  `87ea0c727eb3c0b889a2d3fb41f8dac631ec2af07f521fe9526556d094d2e119`.

## 3.5.1

- A fresh partition store survives a crash at any I/O call of its creation (A5). `openStore`
  used to write STORE first and in place, then MIGRATED, then the registry files; a kill
  between STORE's create and its write, before MIGRATED, or before the registry left a store no
  open accepted ("STORE is corrupt", "store is not MIGRATED", "registry: open registry.fsl
  failed"). SDN's format-2 kill -9 test hit it whenever its first kill landed in the creation.
  The I/O ABI has no rename, so the order carries the atomicity: the registry files, then
  MIGRATED (replacing one left by an attempt that never wrote STORE), then STORE, the commit
  point. STORE's one torn state (created, not yet written, or its unsynced bytes lost with
  power) sits beside a whole MIGRATED and an empty registry, and open finishes it from
  MIGRATED; a torn STORE beside registry frames is still refused. `crash_fault_test`
  `crash_fresh_store_creation_every_io_call_A5` freezes a fresh open at each of its mutating
  calls under every crash mode and reopens as SDN's `format2.Open` decides.
- Per-partition bookkeeping is O(1)/O(log S) per commit (B4, docs/PARTITION-STORE.md §40): the
  segment ledger is a hashed table with a change feed, segments are found by binary search, and
  a compaction candidate index replaces the per-commit walks. At S = 5,188 commit rounds p99
  100.7 ms -> 0.12-0.66 ms and 465 -> 2,300-5,400 rec/s. The RETIRE set is held under what one
  batch carries (the valve), and manifest version 3 carries a u32 segment count past 65,535
  (M3).
- `wasm/flatsql-ps-threads.wasm` is rebuilt from this source (Linux wasi-sdk 30), sha256
  `3a215da45a53f3a7016257429c392720e728caf850d3a1213dc501bfb5844359`.

## 3.5.0

- Partition store query gaps (docs/PARTITION-STORE.md §39), found by T6's comparisons with
  format 1 on a host-02-sized store:
  - `ORDER BY _cid` reads the type's cid catalog in text order (A17); an `OFFSET` is counted
    from catalog entries without reading rows. OMM LIMIT 500 OFFSET 1000 through SDN: 3.6 ms
    (9.9 s before; format 1 26 ms).
  - Per-object point profiles: hidden input columns `_asof`, `_forward`, `_nearest` (epoch
    seconds) return each object's live rows at its best second from `OBJECT_EPOCH` keys, and
    `_object` projects the object key. OMM nearest over 32,015 objects: 2.3 s (56 s before;
    format 1 22.7 s).
  - Untrusted SQL reads every type-level shape but a CID or gseq lookup from the newest-N
    arrivals window (A18): `SELECT _data FROM "CAT@celestrak-satcat"` answers in 11 ms instead
    of exceeding the work budget.
  - Type-level tag conditions match a live instance of any live copy (FIRST or REPEAT, any
    partition), each record once; `<TYPE>@<source>` follows.
  - `ORDER BY _gseq` with tag conditions pages in arrivals order, or through the tags'
    collected postings when the lane counters say they are rare. IQC datasync by source: 5
    pages in 32 ms (about 3.8 s a page before).
  - Rows and frames read ahead in pseq order; tag checks from lane tuples and lane `max_pseq`;
    DEAD / TAG_DEAD lookups skipped for snapshots without such postings.
- Records stored with their own size prefix (a `FinishSizePrefixed` buffer kept as is: SDN's
  dataset-publication PNMs, the local EPM) are accepted: `frameRootOffset()` finds the
  FlatBuffer after both prefixes for the frame check, the verifier, extraction and column
  projection. Before, they were refused with -102 (`kRejFid`) and store-migrate could not
  activate a store holding them. Stored bytes and CIDs are unchanged.
- Memory safety and races (§38, 668f0e6): the ps-wasm trap was V8's shared-memory grow race
  (the Node host and the wasm test main grow the heap before threads start); TSAN-clean
  partition publication and sync pool; the reader gate waits for calls in flight; a space
  emergency spends the ballast only on eviction work.
- UBSan: `EntryIter::next` compares distances (no offset on a null pointer), no `memcpy` with
  a null pointer for 0 bytes, packed counters and head fields read by value.
- The terabyte harness (audit §4): unit gates, engine tiers and a metadata-only 10 TB tier as
  slow tests (`cpp/test/ps/tb_*`, `scripts/tb`).
- `wasm/flatsql-ps-threads.wasm` sha256
  `787adbcccf52a9e9fe2767d6679b6f2d94a4ead41985bf2a7bb0855f821abc72` (2,484,927 bytes); the other
  artifacts are 3.4.0's.

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
