# Changelog

## 3.11.0

Store format 5, item 5: FlatBuffer index segments answer two read paths
(`cpp/src/p4/FORMAT.md` §3.3, §6, §7, §12).
- Checkpoints write the row orders `ROWS_BY_SEQ` and (types with an object
  key) `ROWS_BY_OBJECT_EPOCH` as `$FSG` segments of sorted `FSGPair`s. The seq
  walk (explorer paging, the retention SCAN, the whole-lane scan) and the
  object-key path (`NORAD_CAT_ID =`) now read segments plus the in-memory tail
  since the newest checkpoint. The `w` range, delivery time, copy ts and CID
  paths stay on SQLite; latest-per-object stays on the `LATEST` segments.
- Basis, per path: the replay E2E, gate fixture (1 GB WXF sea feed, 3 seed
  days of OMM), 5 interleaved repetitions, against 3.10.1's SQLite paths, each
  build on its own fixture from the same input, native and WasmEdge AOT, equal
  answer hashes. Ratios SQLite / FSG (medians): explorer cold 0.91x native,
  0.95x AOT, warm 1.06x / 1.01x (a second run: cold 0.94x / 0.93x, warm 1.10x
  / 1.00x); sync_norad cold 1.05x / 1.03x, warm 1.00x / 1.02x; retention 1.03x
  / 1.02x; scan_all 0.99x / 0.99x; every other workload 0.96x to 1.03x. All
  within the 10% bar, so both paths switch. Load average 3.5 to 7.6 on a
  28-core Mac Studio.
- Integrity: each row-order segment is digest-checked (sha256, verifier,
  header, sorted pairs) once per process; the checked identity is kept, so
  later loads skip the hash. The check runs on the checkpoint worker; until it
  is done the SQLite index answers, so no reader and no writer waits for a
  hash. First load measured: 5.84 MB of OMM row orders in 18.5 ms native.
  Segments this process wrote are checked by construction.
- Lookups: the first pair of each 128-pair page stays in memory (fence keys),
  so a lookup reads one page; the pairs stay on disk.
- LSM and retention: a row-order merge stays at most 1/8 of the live rows
  (at least 4,096) and leaves out removed rows' pairs. A segment a quarter
  dead is rewritten without them, or dropped whole when none stay.
- A checkpoint written before 3.11.0 has no row orders: the SQLite index
  answers until the next checkpoint folds every live row.
- `DROPS` checks only its candidate CIDs against the DAG cache's live entries
  instead of rebuilding the feed's closure per call: 0.020 ms on the sea feed
  (51.1 ms in 3.10.1). `REPLAY_DAG_SCALE` now bounds it like `FEED_ROOT` and
  `RANGES`.
- The replay gains `scan_all`, the weather node's whole-lane scan (50,000-row
  hydrated seq pages, a tile writer beside it): about 160,000 records/s
  native; a concurrent PUT's ack p99 stays 8 to 12 ms during the scan.
- Cost: `$FSG` bytes on the gate fixture 22.9 MB for 298,986 rows (OMM 17.3%
  of its stream, WXF sea 0.74%); the checkpoint install step measured 96 ms on
  one round (73 ms in 3.10.1).

## 3.10.1

Store format 5 IPFS layout: per-call costs no longer grow with the feed
(`cpp/src/p4/FORMAT.md` §4.3, §5, §12).
- `RANGES` from a commit walks only the DAG nodes rewritten at or after it:
  the new units, the rewritten units and their spine. `FEED_ROOT` computes only
  the root. On the gate's WXF sea feed (110,915 blocks), a pull after one round
  takes 0.089 ms (14 blocks); the whole-closure walk took 68.6 ms.
- The derived DAG cache only moves forward. A commits root asked for an earlier
  commit point comes from the roots it recorded (4 µs); the feed is never
  replayed from genesis for it. A point the cache never stopped at returns
  `P4_E_BUSY`.
- The replay E2E (`REPLAY_DAG_SCALE`) times `FEED_ROOT`, `RANGES` from the
  prior commit and the earlier commits root on the sea feed and on a small feed
  of the same lane shape. Each must stay within 2x + 1 ms of the small feed's.
  The earlier root must equal a fresh cache's, and the pruned `RANGES` must
  hold every block of the full closure past its `from`.
- FORMAT.md §5 records the measured PUT ack latency while a checkpoint
  prepares: p50/p99 9.04/10.44 ms, against 8.80/13.95 ms with none, plus a
  73 ms install step on one round. Basis: the 3.10.0 replay gate, native, on
  the gate fixture (1 GB WXF sea feed and 3 seed days of OMM), one run on a
  28-core Mac Studio at a load average of 23.7.

## 3.10.0

Store format 5, part 3: the IPFS layout (`cpp/src/p4/FORMAT.md` §4, §5, §6,
§8, §10).
- IPFS layout ops: `RANGES` (per-record `(range, CID)` tuples), `FEED_ROOT`,
  `DROPS`, `EXPOSURE` and `RELEASE`.
- Records over 1 MiB get UnixFS DAGs whose root CIDs are byte-equal to Kubo's
  `ipfs add --cid-version=1 --raw-leaves --chunker=size-1048576`.
- The feed root DAG (checkpoint, commits tree, segments) is a pure function of
  the frames. Its resident state is 88.9 B per row (was 515.3).
- CARv2 export and restore. The export writes a checkpoint first, and
  `ipfs dag import` takes the CAR byte for byte.
- Release protocol: physical removal waits for the node's release. Naming a
  retired generation releases all of it; `through` applies only to the current
  generation.
- SDS 1.256.0 fields (`FSG.INSTANCE_URL_IDS`, `FCK.RECORD_BYTES`, `MIN_W`,
  `MAX_W`): a compacted feed with its index deleted rebuilds exactly, including
  urls that only dead rows wrote.
- Checkpoint preparation runs off the PUT path, on a background thread from one
  SQLite read snapshot. The appender installs it at the next round.

## 3.9.0

Store format 5, part 2: checkpoints and state segments (`cpp/src/p4/FORMAT.md`
§3.2, §3.3, §6, §9). Stores from 3.8.0 are refused; nodes rebuild.
- `$FCK` checkpoints are written on the §6 cadence. Each lists LSM-merged
  `$FSG` segments: dictionary, latest per object, instances, tokens and
  tombstones.
- Open reads the newest checkpoint's segments plus the tail `$FMB`s.
- With every index file deleted, open finds the newest `$FCK` by a tail search
  and rebuilds the row index from the `$FMB` chain. On the 15 GB replay fixture
  that is 2.6 s for the checkpoint open and 18.5 s for the row-index rebuild,
  and every answer is equal.
- Compaction runs again where 3.8.0 deferred it. It writes consolidated `$FMB`s
  and fresh segments.
- Segments are 4.8% of an OMM stream (328 B records) and 0.7% of a WXF stream.
  Checkpoints are under 0.01%.
- p4 replay gate, 3.7.2 -> this build, native median: wxf_lead 826 -> 431 ms,
  explorer warm 87.8 -> 9.4 ms, sync_window warm 142 -> 78 ms, latest warm
  112 -> 87 ms.

## 3.8.0

Store format 5 (`cpp/src/p4/FORMAT.md`, owner-approved 2026-10-18), first part.
Format-4 stores are refused with `P4_E_FORMAT`. Nodes rebuild by re-sync; there is
no migration.
- Each feed is still one linear `[u32 LE size][FlatBuffer]` stream with records
  verbatim. A stream starts with a genesis `$FCK` frame. Each commit round adds
  one sibling `$FMB` frame per feed (SDS 1.255.0) holding every row's seq,
  source ts, delivery, node, batch, url, signature, sealed columns and ingest
  identity. Removals and moves are appended `$FMB` frames. The epoch and object
  key are re-extracted from the record bytes; they are not stored.
- An ack costs one stream sync (format 4: two). The index commit is not synced,
  because the index is a cache of the stream.
- Open recovers from the stream tail with each row's own seq and metadata. A
  missing or damaged index is rebuilt from the `$FMB` chain.
- p4 replay gate, 3.7.2 -> this build, native median: wxf_lead 799 -> 398 ms,
  gp_ingest 531 -> 478 ms (1 sync per call). Kill -9 mid-lead (x3) and rebuilds
  with every `.db` deleted answer exactly. `$FMB` is 76 B per row.
- Not yet in this release: checkpoint `$FCK` and `$FSG` segments, IPFS ranges,
  large-record DAGs. Compaction is deferred where live history needs the
  segments.

## 3.7.6

Store format 4's warm reads build no per-row strings and copy no record bytes. Every answer is
3.7.5's; the storage format is unchanged.
- A limited SCAN resolves the candidates its limit still needs (3.7.5 resolved 512 per page
  whatever the limit: 5.1 rows read per row answered by a 100-row explorer page), doubling when
  rows are rejected; walk chunks likewise.
- Rows hold views of the scan's copy of the feed dictionary and a slice of the read buffer
  (the frame in place, extracted without a copy); flat vectors replace the page maps.
- The p4 replay hashes a read pass's answers after its clock stops (a 4-lane hash).
- p4 replay gate, 3.7.2 -> this build, warm median, native then WasmEdge AOT (stock SQLite):
  - Explorer paging: 77.6 -> 9.5 ms, 111 -> 13.8 ms (5.5 ms).
  - EPOCH-window sync: 126 -> 71 ms, 183 -> 107 ms (105 ms).
  - Nearest-epoch latest: 102 -> 87 ms, 138 -> 114 ms (110 ms).

## 3.7.5

The p4 replay E2E fits the gauntlet's budget; the engine and its wasm are 3.7.4's (byte for byte).
- The gate replays a slice: a 1 GB sea feed, three days of gp_history (182,508 OMMs), 32,768
  OMMs ingested, a 64-tile lead and 8 retention pages; 5 repetitions, native and WasmEdge AOT,
  the same Mann-Whitney slower test against 3.7.2.
- Each build builds its own fixture from the same input, so a build whose files 3.7.2 cannot read
  is still compared on the same data; one process per build, runtime and repetition runs every
  workload, each on its own fresh clone.
- The 15 GB replay is `cpp/tests/p4-replay.bench.mjs`, outside the gate.

## 3.7.4

Store format 4 reads a page's frames in fewer, larger reads. Every answer is 3.7.3's.
- Frames up to 64 KiB apart join into one read (runs up to 4 MiB); 3.7.3 joined only frames
  within 4 KiB. A cold random read costs about as much as 250 KB of sequential transfer, and a
  warm one about as much as copying 64 KB.
- p4_replay now also runs every workload the way SDN runs the engine: the wasm32 test command
  under WasmEdge AOT, on sdn-server's flatsqlrt with the node's native host I/O. It also runs
  stock SQLite 3.53.4 on the same index files as a yardstick.
- 3.7.2 -> this build, 5 repetitions, median, native and then WasmEdge AOT:
  - EPOCH-window sync, cold: 480 -> 355 ms native, 576 -> 436 ms AOT.
  - Nearest-epoch latest, cold: 1,066 -> 982 ms native, 1,116 -> 997 ms AOT.
  - Explorer paging, warm: 88 -> 52 ms native, 123 -> 70 ms AOT.
  - Every answer hashes the same on both builds and both runtimes.

## 3.7.3

Store format 4 reads a page of rows in one pass. Every answer is 3.7.2's.
- A dense page's rows load in one rid range of the index, and their frames in stream-offset
  order with neighboring frames joined into one read; the type text of a row is read once per
  page. 3.7.2 sought each row's index entry and read each frame separately.
- p4_replay (cpp/tests/p4, its E2E cpp/tests/p4-replay.e2e.mjs): the node's format-4 workloads
  on a 15 GB store (1.72M WXF records and 623k Space-Track OMMs), 3.7.2 and this build
  interleaved, 5 repetitions, native: explorer paging (100 pages of 100 newest rows) 110 ms ->
  65 ms cold and 96 ms -> 57 ms warm; EPOCH-window sync 292 ms -> 265 ms warm; nearest-epoch
  latest 143 ms -> 128 ms warm. Every answer hashes the same on both builds.

## 3.7.2

Store format 4: a write's cost on a feed no longer grows with the feed. Every answer is 3.7.1's.
- A feed's index transaction keeps only the instances it changes (each taken from the feed's
  committed table when first touched) and the dictionary entries it adds; 3.7.1 copied the
  feed's whole instance table and dictionary into every commit round and replaced them at the
  commit, destroying the old copies under the type's lock.
- A delete's seq bounds are the first and last rows by rowid (two seeks); 3.7.1's
  `SELECT min(seq), max(seq) FROM r` scanned every row of the seq index.
- p4_record_scaling (cpp/tests/p4, its E2E cpp/tests/p4-record-scaling.e2e.mjs), native, a
  15.0 GB WXF feed of 191,331 nine-record tile batches: a 512-tile GFS lead 40.1 s -> 5.3 s
  (each tile now its two durable syncs), a 500-record retention page (SCAN + DELETE) 0.134 s
  -> 0.020 s, and while a lead writes to each of two feeds 0.237 s -> 0.038 s (2,006 -> 11,857
  records/s). The wasm32 test build under the Node wasi-threads host: lead 43.1 s -> 9.9 s, page
  0.172 s -> 0.037 s. The query results hash the same on both engines; REBUILD 8 finds no
  mismatch.

## 3.7.1

Store format 4 follow-ups (CONTRACT C-46, C-47, C-48). Every answer is 3.7.0's except where
C-46 (1)-(3) make it format 1's and (6) makes a crash a refusal. C-47 accepts three epoch edge
cases where format 1's answer depends on the CPU (a clamped epoch's coverage day caps the year
at 9999; a NaN epoch is not indexed; a type whose epochs all sit at one clamped extreme reports
that SUMMARY bound as NULL) and moves SandboxedSelect `SELECT *` rendering (C-46 (4)) to SDN.
C-48 accepts three format-1 defects format 4 does not copy (SDN-side: a `TYPE@source` fast-path
select answers only that source; a NUL inside a text value keeps the whole string; a sealed type
with a binary schema registers CID-only). Equivalence with format 1, field by field: 237 shapes,
195 equal, 1 equal (C-12 copy), 41 accepted, 0 different.
- EPOCH counts and limited pages push the limit down again (a58f818/226872c's design on the
  stream engine): a count probes each entity until one feed has a pick within the max delta; a
  page whose offset + limit fits 4,096 probes only its first entities and stops a text-keyed
  object walk there; a pick deleted before its answer sends the page to the full pass. R16
  (nearest count + limit 200), native warm: 600 feeds 8.2 s -> 0.29 s merged, 8.0 s -> 0.36 s
  staged; 16 feeds 3.5 s -> 0.24 s. The equivalence driver, the 365-variant R16 matrices and
  the new count/page shapes answer as 3.7.0 byte for byte.
- The default soft heap is 576 MiB (was 512): idle readers give way past three quarters of it,
  and a 600-feed type's warm readers hold 404-414 MiB, so type-wide reads no longer reopen up to
  600 connections each. The hard heap stays 640 MiB.
- An index file an earlier format-4 build wrote (74b2f0d, the WIP staged layouts, the feed-table
  and partition eras) is refused with `P4_E_FORMAT` before anything reads or writes it, and a
  failed open cleans up without crashing (3.7.0 dereferenced freed feeds after an init error).
- Full text reads each frame with its size prefix, as format 1 does: records whose 8-byte fields
  align to a prefix the producer cut off are indexed (3.7.0 skipped them silently). A type with
  no records reports `ready` and a search answers no rows (was `building` / unsupported).
- typecfg: a double epoch (`f64floor`) outside the int64 range is clamped to it, as Go's
  conversion on arm64 (format 1); 9.2e18..2^63 is no longer dropped; the millisecond value is
  defined. Format 2 (`flatsql-ps-threads.wasm`) shares the extractor.
- Tests: `cpp/tests/p4/t_c46.cpp` and the fixture `cpp/tests/p4/fixtures/store-74b2f0d`.

## 3.7.0

- Store format 4 (docs/STORE-FORMAT-4.md), the stream engine: each standard's records are kept
  per source feed (provider x source; `local` for none) as two files. `<feed>.fsdata` is a pure
  FlatBuffer stream, `[u32 LE size][FlatBuffer]` frames back to back with no header, tag or
  trailer, that a stock FlatBuffers reader walks from byte 0; `<feed>.db` is SQLite 3.53.4,
  unmodified, holding only the feed's index rows, which point at their frames by `(off, len)`.
  No record bytes are in SQLite. Ships as `wasm/flatsql-p4-threads.wasm` (wasm32-wasip1-threads;
  package export `flatsql/p4-threads.wasm`) and the native `flatsql_p4` library. The ABI is
  `cpp/include/flatsql/p4/flatsql_p4.h`, the SQL surface's reader API `p4_reader.h`.
- Writes: writer threads own feeds and group-commit every queued call. An acknowledged write
  has its frames synced in the stream and its index rows committed staged (appended pages
  only); each writer's indexer thread merges a feed's staged rows into its random-keyed indexes
  in one transaction between rounds (a quiet feed after 30 s; past 8 x flushEntries staged
  rows it merges before acking more). Reads see staged
  rows through capped in-memory views. Open recovers and can rebuild an index from its stream.
  Measured native at 5M records: 1,200 bytes written per record (4,615 when acks wrote every
  index), 74.1k records/s; reads equal within noise; `t_kill` 100/100, `t_power_loss` 100/100,
  `t_kill_merge` 50/50.
- Off by default in SDN: format 1 stays the store unless `SDN_STORE_FORMAT` selects 4. Migrate
  mode loads format 1 keeping its rowids and tag instances.
- SQLite 3.53.4 (the official amalgamation, byte-identical; CMake checks its sha256) for every
  artifact.
- `flatsql_io` VFS: per-path nodes (a shared heap WAL index and in-memory locks for
  connections that opt in with `share=1`, reader readahead with `ra=1`); format 1 behaviour is
  unchanged.
- typecfg: moved from `src/ps/extract.*` to `src/typecfg/`, unchanged.
- CI runs the format-4 suites natively and under the Node wasi-threads host, with kill -9
  loops; `npm-publish.yml` refuses a release whose `flatsql-p4-threads.wasm` is not the
  committed build.

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
