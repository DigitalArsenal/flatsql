# FlatSQL store format 4: one SQLite file per partition

Store format 4 keeps each partition's records in one SQLite file. A partition
is one producer and one record type. SQLite 3.53.4 is unmodified: only the
VFS, WAL and configuration are FlatSQL's. The engine is `cpp/src/p4`; it ships
as `wasm/flatsql-p4-threads.wasm` (wasm32-wasip1-threads).

- **Design:** the stack's `docs/architecture/flatsql-sqlite-partitions.md`,
  with its owner revision of 2026-10-01.
- **Interfaces:** the build-out contract (`CONTRACT.md`, version 11):
  - the ABI in `cpp/include/flatsql/p4/flatsql_p4.h`;
  - the reader API for the SQL surface in `cpp/include/flatsql/p4/p4_reader.h`.

This file records what was built, how to run it, and what was measured.

## 1. Layout

```
<data>/fsql4/
  STORE                 64 B, magic FSQ4, format 4 (C-7); written last by activation
  MIGRATED              40 B, magic FSQM, format 4
  T/TYPES               the registered types (crc'd records)
  T/<TYPE>.spec         the registered spec TLV (C-5); a reopen serves before re-registration
  T/<TYPE>.idx          the type index (derived; rebuilt from the files)
  T/<TYPE>.jnl          the intent journal
  T/<TYPE>.fts          full text (FTS5, contentless-delete; background)
  P/<TYPE>/<pid>.db     the partition's one file, for its life
```

- **Partition:** (type, producer token). The token comes from the peer (C-13).
- **No months and no generations** (C-32). A partition never changes or
  drops its file. An emptied partition keeps its file, and SQLite reuses the
  free pages.
- **Lazy type files:** a type's `.idx`, `.jnl` and `.fts` are made by its
  first write. A registered type without data has only its `.spec`, so an
  open costs O(types with data).

## 2. Files

**Partition file.**
- Tables:
  - `meta`: format, type, producer, peer, pid, ix, and the counters;
  - `src` and `lane`: a lane is format 1's tag key (C-3), with its counters;
  - `r(seq PK, cid, e, k, ts, p, f, s, x, d, w)`, where `w` = `coalesce(e, ts)`;
  - tag instances `rl(sid, seq, lane, at, u)`, WITHOUT ROWID, keyed
    `(seq, sid, lane)`.

| Index | Purpose |
|---|---|
| the rowid and `r_s(seq)` | arrival: the datasync cursor; newest-N cuts and oldest-first quota read `r_s`, not the records' pages |
| `rl_sid(sid, seq)` | source, newest first: `<TYPE>@<source>`, source pages, supersede |
| `r_ke(k, e)` | object and epoch: EPOCH points, object predicates, CAT supersede |
| `r_w(w DESC)` | epoch windows (`w DESC`, ties in CID order) |
| `r_c(cid)` | CID order, merged over the partitions |

- Writers open with `synchronous=FULL`, WAL, and no autocheckpoint.

**Type index** (`.idx`, `synchronous=FULL`).
- Tables:
  - `c(cid, pid, seq)`: every copy of a CID (dedupe, GET, TAGS, DELETE, exact-CID reads);
  - `ident(src, h, seq, cid)`: IQC ingest identities;
  - `part`, `src`, `lanes`, `file(pid, …)`, `lanecnt(lane, pid, …)` and `meta(uniq, copies, next_seq)`.
- It is a cache of the files plus the journal. Every row except `ident` can
  be rebuilt from the files (REBUILD 2).

**Intent journal** (`.jnl`).
- Tables: `j(id AUTOINCREMENT, op, k, c, pid, seq, s, v)` and `jm(seq_reserved)`.
- Ops: partition, lane, source, file (J_FILE), entry (J_C), identity, and removal (J_DEL).
- A group journals its entries before its partition file commits. A flush
  merges committed entries into the type index and cuts the journal behind them.

## 3. Writes

- **Calls and writers.**
  - Calls arrive in mailbox slots, in two pools (C-6): 8 MiB write requests
    and 64 KiB read requests.
  - Writer threads own partitions: each is pinned to one writer, with a
    credit. A full backlog answers `P4_E_BUSY`.
- **A group.** A writer takes every queued call of a partition, up to
  `groupRecords`, and runs these steps:
  1. parse and extract;
  2. probe dedupe against the type index and the pending map;
  3. assign seqs, from durable seq blocks;
  4. journal;
  5. commit one transaction on the partition's file;
  6. publish;
  7. ack.
  The ack follows the commit and the visible-through publish (C-4).
- **Removals.** These are all journaled removals:
  - copies (a CID held by another producer) and retags;
  - CAT supersede-on-ingest (ingest mode only, C-20);
  - IQC ingest identities (IDENT_DUP tags the held record, C-26);
  - batch supersede, deletes and quota deletions.
- **Migrate mode** (PUT mode 1, create mode 2):
  - seqs and tag instances are the caller's (format 1's rowids and `created_at`);
  - a COPY keeps the holder's bytes and ts (C-22);
  - identities are registered (C-21);
  - secondary indexes wait for REBUILD 1, and full text waits for activation.

## 4. Crash safety

Every open replays the journal before it serves (M8). Replay opens each file
it checks, which recovers that file's WAL.

1. **Registry:** partitions, sources, lanes and files. A J_FILE whose file has
   no committed schema is not trusted: the writer completes it. That state is
   a kill between the empty file and its schema.
2. **Entries:** each J_C or J_DEL is checked against its file by seq and CID.
   Counters move only for entries the index lacks.
3. **Recount:** touched files are recounted from their own rows.

The type index never records a file whose schema has not committed. Nothing
unlinks or replaces a partition file.

The month layout's wasm-only kill anomaly cannot arise in this layout.
- **What it was.** About 1% of wasm kill rounds left a type-index row naming
  an empty file that no longer existed (`P/PNM/3/202609.163.db`). 562276b
  remade such a file at open.
- **Root cause.** Not reproduced; the exact interleaving is unknown. The
  state needed a month file that a supersede had emptied, retired and
  unlinked many times over: generation 163 of one month.
- **Why it is gone.** Nothing retires, unlinks or replaces a partition file
  in this layout, so the state cannot occur. The heal is deleted. The wasm
  kill loop passes 100 of 100 rounds without it.

## 5. Maintenance

One background thread does this work, never on a caller's path:
- type-index flushes;
- PASSIVE and RESTART checkpoints from the WAL hook, for partition files, the
  type index, the journal and full text;
- closing evicted writer connections;
- the configured quota, once a second;
- full text;
- a journal VACUUM when a flush empties it.

- **QUOTA_GC** (C-32) deletes the oldest records by arrival.
  - The victim is the type whose oldest record arrived first.
  - The cut is merged over that type's `r_s` indexes, and each partition's
    writer deletes its rows (per-partition writers).
  - Each pass deletes at most 32,768 records, sized to the excess at the
    store's mean bytes per record, until the bytes in use fit.
  - Bytes in use count each partition file's pages less its free pages
    (pages still in the WAL are counted by `page_count`), plus the type files
    and any rollback journal.
  - When nothing is left to delete and the store is still over quota, writes
    refuse with `P4_E_NOSPACE` while reads continue.
  - `files_dropped` is always 0.
- **REBUILD:**
  - **1** builds the partition secondary indexes, after a migration's bulk append.
  - **2** rebuilds the type index from the files: `c` is rewritten in CID
    order, from every file's `(cid, seq)` merged.
  - **4** rebuilds full text.
  - **8** verifies, changing nothing:
    - `c` against the files' CIDs, merged (one sequential walk of each side);
    - the counters and lanes against the rows;
    - `PRAGMA integrity_check` on every live file, the type index and the
      journal (C-27). A damaged file is a mismatch, named in the slot err.

## 6. Reads

**Orders.**
- **Seq** (datasync): per-file pages by rowid, merged.
- **w** (windows): `r_w`, with ties in CID order.
- **CID:** each file's `r_c`, merged by (CID, pid).

Copies collapse to one row per CID: the lowest pid that matches, also under
OFFSET.

**A18 bound (C-31).**
- With `lane.source` set, the bound is that source's newest N records: one
  newest-first walk of each partition's `rl_sid`, merged, stopping at N. The
  scan then reads through `rl_sid` above the cut.
- Without a source, the bound is the type's newest N (`r_s`).
- Every other filter applies above the cut.
- `<TYPE>@<source>` therefore returns that source's records. This is an
  intended difference from format 1, whose per-type in-memory window answered
  `CAT@celestrak-satcat-csv` with 0 frames.

**Candidates instead of a walk.**
- An exact CID (tag 8) is one type-index probe.
- An equality, IN or range predicate on the object rule's first column reads
  `r_ke`.

**EPOCH nearest / as_of / forward** (C-32) take one `r_ke` seek per object
per partition.
- The objects are walked in k order: a seek to the next k.
- Each object's run is read from the target, in rank order, until an epoch
  group has a row that passes the filters. The lowest CID of that group
  wins, which is format 1's ranking.
- A record without an object is its own entity, keyed by its CID.

**Caps** end a read with its status: rows examined, bytes read, result rows
and bytes, and cancel. RB1 streams always end with RB1E.

**SQL surface.** `src/p4sql` answers ops 30 and 31 through `p4_reader.h`, and
the engine writes their RB1E (C-28). Without the surface, ops 30 and 31
answer `P4_E_UNSUPPORTED`.

## 7. Memory

There is one engine-wide budget (design §9).
- SQLite is built without memory statistics (C-30). The SQL surface's
  allocator enforces the hard heap limit (config tag 27, 640 MiB) and serves
  stats 29 and 30.
- Reader connections are pooled (tag 23), with a 512 KiB cache each (tag 24).
- Writer connections are an LRU (tag 21).
- Pending type-index entries flush at `pendingBytes` (64 MiB).

## 8. The artifact

| | |
|---|---|
| Build | `bash scripts/build-wasm.sh --ps` builds it with `flatsql-ps-threads.wasm`. `--ps-tests` also builds `cpp/build-ps-wasm/flatsql-p4-test.wasm`. Released bytes: `--ps --linux` (Docker, Linux wasi-sdk 30). CMake: `cpp/cmake/flatsql_p4_wasm.cmake`, globbing `src/p4`, `src/p4sql`, `tests/p4` and `tests/p4sql`. |
| SQLite | The 3.53.4 amalgamation, byte-identical (CMake checks its sha256). Options: `THREADSAFE=2`, WAL, FTS5, `DEFAULT_MEMSTATUS=0`, `TEMP_STORE=3`, `SQLITE_OS_OTHER`. The only VFS is `flatsql_io`. |
| Imports | `wasi_snapshot_preview1`, `wasi.thread-spawn`, `env.memory` (shared, at most 32768 pages) and the seven `env.flatsql_io_*`. `scripts/check-wasm-imports.mjs` fails on any change. |
| Exports | `flatsql_p4_init start stop wake layout register_type activate set_quota stats alloc free`, `wasi_thread_start`, `_initialize` and `memory`. |
| Host | As for the partition store (docs/PARTITION-STORE-WASM.md): grow the shared heap before `flatsql_p4_start`. The Node host is `wasm/ps-node-host.mjs`. |

## 9. Tests

```
FLATBUFFERS_DIR=<flatbuffers> cmake -S cpp -B cpp/build && cmake --build cpp/build --target flatsql_p4_test flatsql_p4_fault_test -j 6
cpp/build/flatsql_p4_test                                     # the fast suite
cpp/build/flatsql_p4_test --test=t_kill --rounds=100          # kill -9 loop
cpp/build/flatsql_p4_fault_test --test=t_power_loss --slow=1 --rounds=100
cpp/build/flatsql_p4_test --test=g2_fixture --fixture=<format-1 control.flatsqldb> --bfbs=<search-schemas> [--store=<dir> --keep=1]
bash scripts/build-wasm.sh --ps-tests && node scripts/p4-wasm-suite.mjs [--kill-rounds 100]
```

- **`t_kill`.** A forked engine ingests from three producers, with copies, a
  batch supersede every 13th call and a quota of 4 MiB every 11th. It is
  killed with SIGKILL at a random point, then the check runs:
  - every file passes `integrity_check`;
  - every row on disk is found by CID;
  - the count equals the distinct CIDs on disk;
  - REBUILD 8 finds no mismatch;
  - new seqs are above every seq on disk;
  - no file is left open.
  The wasm loop kills the guest's workers in the Node host.
- **`t_power_loss`.** The engine runs over FaultFs, frozen at a random I/O
  call per round, with five crash modes. After the crash, every acknowledged
  record must be there, REBUILD 8 must be clean, and the count must equal a
  full scan's distinct CIDs.
- **`g2_fixture`.**
  - It loads the host-02-sized format-1 fixture as store-migrate does.
  - It compares GET, TAGS, SCAN, WINDOW and INDEX_PAGE with format 1 per type.
  - It compares EPOCH nearest, as_of and forward for every OMM and MPE
    object with format 1's `queryPointEpochRecords` ranking SQL, run on the
    fixture.
- **Benches** (`t_fixture.cpp`):
  - `g3_bench`, modes `producers`, `w01`, `w06` and `w10`;
  - `a18_probe`;
  - `g6_bench`;
  - `open_bench`.

## 10. Measured

**Conditions.**
- Mac Studio, 28 threads, shared with other sessions. The load is the
  1-minute average.
- Native unless marked wasm. The wasm runs used V8 under the Node host.
- The brief's gates are judged in WasmEdge AOT through SDN, back to back
  with formats 1 and 2, at low load. These numbers are the engine's side, at
  high load, with few samples per shape.

| | |
|---|---|
| Fixture load (G2) | 4,364,873 copies of 3,945,845 records (OMM, MPE, CAT, IQC) in 138 s (31.5k copies/s, load 31). REBUILD 1 took 19.2 s. **827.6 B per copy** (915.5 B per record), against format 2's 1,742 and format 1's 2,366. 0 differences from format 1 in GET bytes/seq/ts/peer, TAGS, SCAN order, WINDOW and INDEX_PAGE, and in the 18 EPOCH point queries below. REBUILD 8 is clean. |
| EPOCH, every object | Fixture, 3 epochs per profile, load 29-37. **OMM, 32,015 objects:** as_of and forward 133-136 ms, nearest 165-171 ms. **MPE, 32,486 objects:** as_of and forward 149-163 ms, nearest 195-418 ms. Format 1's ranking SQL on the same fixture (native SQLite, not the deployed engine): 0.35-8.6 s. |
| A18 (C-31), warm | `p4_cursor_open`, fixture, load 30-46. **OMM@celestrak-gp NORAD=25544** inside the 400,000 bound: 13 frames, 17-18 ms (format 1: 181 cold / 149 warm). **IQC@IQEngine:** 9-11 ms. **CAT@celestrak-satcat:** 7.8-8.2 ms. **CAT@celestrak-satcat-csv:** 10,000 records under C-31, 7.7-8.6 ms. **MPE@celestrak-gp:** 7-8.9 ms. An 8 MiB reader cache, instead of 512 KiB, takes CAT to 6.6 ms. |
| HEAD by exact CID | 0.67 ms first, 17-33 µs after, on the fixture (was a ~5 s type scan). |
| W01, OMM ingest | 32,015 records into the fixture's OMM partition (1.7M rows), 4,096-record calls. The first call after open is cold: 1,038 ms. After that: 20.6-21.2k rec/s, call p50 195-225 ms, p99 229-271 ms (load 38-45). **The same clone with `r_c` dropped:** 61-64k rec/s, p50 55-59 ms, p99 71-81 ms, first call 364 ms (load 23-29). The per-file CID index costs about 3× on ingest (§11). |
| Ingest growth | 640,300 OMM records: 20 GP-like batches over the fixture's 32,015 objects, OMM growing from 1.7M to 2.3M rows. 27.9k rec/s. Call p50: 154 ms in the first tenth, 115 ms in the last (load 26), so no slope. In a 10-batch run the cold first call took 1,688 ms; the other calls had a median of 125 ms with spikes of 250-430 ms (load 29). |
| REBUILD | On a fixture clone (load 22-28): REBUILD 2 (the type index from the files, `c` in CID order) 9.0 s; the month build took 4m04s. REBUILD 8 (verify, with `integrity_check` of every file) 19.7 s. |
| G3, fresh store | OMM, 4,096-record calls. One producer: 52.8k rec/s, call p50 65 / p99 87 ms (load 31). Eight producers: 140k rec/s, p99 894 ms (load 33). Format 2: 17.4k with one writer and 6.6k with four, call p99 306-494 ms. |
| W06 | Batch supersede of all 1,696,780 OMM records on a fixture clone: 31.4 s (load 26). The month layout took 42.7 s and the prototype 9.7 s. It is I/O-bound: the deletes rewrite pages of the random-key indexes (`r_c`, `r_ke`). |
| W10 | Quota to 90% of the fixture clone's bytes: 196,608 oldest records deleted in 6.4 s (load 42). REBUILD 8 is clean. |
| First open | 232 registered types, as the daemon registers them. **First open:** open 37 ms, registration 3.45 s (232 durable spec writes, load 34), close 41 ms, 235 files in T/. **Reopen:** 20 ms, re-registration 0.5 ms. The month build took 62-81 s and wrote 940 type files. |
| Crash | Native t_kill 100 of 100 and t_power_loss 100 of 100. Wasm kill 100 of 100. Load 23-39. |

## 11. Open: the per-file CID index

C-32 puts a CID index in every partition file, as `r_c`.
- **The cost.** A record's CID is a random key, so a 4,096-record commit
  dirties about one `r_c` leaf page per record. On the fixture's OMM file,
  W01 runs about 3× slower with `r_c` than without it (§10).
- **Prior evidence.** The design's prototype measured the same cost (v1:
  "A random 32-byte key per row made writes 10–25× worse").
- **What `r_c` serves.** It serves only CID-ordered windows and REBUILD 2.
  The type index already maps every CID to its copies.
- **Where the gate stands.** Warm ingest p99 stays under format 2's
  306-494 ms. The cold first call after an open does not.
- **Proposed.** Dropping `r_c`, and serving CID order from the type index, is
  a contract change for the coordinator.
