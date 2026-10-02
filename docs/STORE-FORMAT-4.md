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
  - `meta`: format, type, producer, peer, pid, ix, the counters, and `nobj` (the
    file's distinct objects, files with `r_ke`) and `wh` = 1 (the histogram is kept);
  - `wh(b, n)`: the rows with an epoch per hour (`b` = floor(e / 3600));
  - `src` and `lane`: a lane is format 1's tag key (C-3), with its counters;
  - `r(seq PK, cid, e, k, ts, p, f, s, x, d, w)`, where `w` = `coalesce(e, ts)`;
  - tag instances `rl(sid, seq, lane, at, u)`, WITHOUT ROWID, keyed
    `(seq, sid, lane)`.

| Index | Purpose |
|---|---|
| the rowid and `r_s(seq)` | arrival: the datasync cursor; newest-N cuts and oldest-first quota read `r_s`, not the records' pages |
| `rl_sid(sid, seq)` | source, newest first: `<TYPE>@<source>`, source pages, supersede |
| `r_ke(k, e)` | object and epoch: EPOCH points, object predicates, CAT supersede |
| `r_w(w DESC)` | epoch windows (`w DESC`, ties in CID order); epoch-day and epoch-range reads |

There is no per-file CID index (C-34): the type index's `c` is the one CID
index (lookup, dedupe, CID-ordered windows, REBUILD 2).

`nobj` and `wh` are exact when present: every writer changes them in the
transaction that changes the rows (an object's existence is probed next to
the `(k, e)` entry the insert or delete touches). REBUILD 1 and 2 count them
from the rows; a file an older engine wrote has neither, and its readers walk.

- Writers open with `synchronous=FULL`, WAL, and no autocheckpoint.

**Type index** (`.idx`, `synchronous=FULL`).
- Tables:
  - `c(cid, pid, seq)`: every copy of a CID (dedupe, GET, TAGS, DELETE, exact-CID reads);
  - `ident(src, h, seq, cid)`: IQC ingest identities;
  - `part(pid, producer, peer, …counters)` (counters NULL until the file has its schema), `src`, `lanes`,
    `lanecnt(lane, pid, …)` and `meta(uniq, copies, next_seq)`.
- It is a cache of the files plus the journal. Every row except `ident` can
  be rebuilt from the files (REBUILD 2).

**Intent journal** (`.jnl`).
- Tables: `j(id AUTOINCREMENT, op, k, c, pid, seq, s, v)` and `jm(seq_reserved)`.
- Ops: partition, lane, source, file (J_FILE), entry (J_C), identity, and removal (J_DEL).
  J_FILE marks a file a group changed: a retag-only group or a tag-only
  supersede chunk changes lane counters with no J_C or J_DEL row.
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
3. **Counters:** each touched file's counters are loaded from its `meta` and
   lane rows, which commit with its rows (writes and removals both keep them;
   `mints`/`maxts` are bounds after removals). No row is recounted, so the
   cost is the journal tail's, not the partition's size.
4. **Files against the disk:** a partition the index says has a file whose
   file is missing is made again by its next write when it had no rows, and
   quarantined otherwise (writes answer `P4_E_CORRUPT`, naming the file).

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
  in this layout, so under kill -9 the state needs a committed file to
  vanish. Step 4 above checks it at every open anyway, so it can never wedge
  a partition.

## 5. Maintenance

Two background threads do this work, never on a caller's path.
- **The maintenance thread:** type-index flushes (a type whose flush lock a
  REBUILD holds is skipped that tick, never waited for); checkpoints for
  partition files, the type index, the journal and full text; closing evicted
  writer connections (each checkpointed first); the T/ files' sizes for
  SUMMARY 4, once a second.
- **Checkpoints never wait for a writer.** PASSIVE passes copy a WAL into its
  file while the writer keeps committing; once a pass leaves little behind, a
  TRUNCATE with no busy handler takes the writer lock only if it is free that
  instant, copies the last frames and empties the WAL. A WAL is checkpointed
  when it passes the PASSIVE pages (config tag 30, at most 32 MiB), and while
  the instance's WALs are over `walTotal`, every committing WAL of 4 MiB or
  more and the largest idle ones (down to half the total). The WAL stat is
  each path's frames not yet checkpointed, kept with every commit, checkpoint
  and close.
- **The WAL file's bound.** Under steady commits no PASSIVE pass ends exactly
  when the writer starts its next transaction, so a WAL never starts over by
  itself. The file's one writer, right after a commit that leaves its WAL at
  a quarter of `walTotal` or more (or at 32 MiB while the instance's WALs
  pass three quarters of it), runs a TRUNCATE checkpoint itself: no frame is
  added meanwhile, the PASSIVE passes have copied most of it, and it waits
  (2 s at most) only for readers still inside the WAL.
- **The long-work thread:** REBUILD and QUOTA_GC calls, the configured quota
  once a second, and full text. Nothing it does delays a checkpoint or a
  flush.
- **Backpressure:** a PUT answers `P4_E_BUSY` when its partition's backlog is
  full, the type's pending index entries pass `pendingBytes`, or the WALs pass
  twice `walTotal`.

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
  - **2** recounts each partition on its own writer (counters from its rows,
    lane rows from `rl JOIN r`, url, url0, created and updated kept from the
    file's lane table), writes them back to the file in one transaction, then
    checks `c` against the files (point probes both ways) and repairs only
    what differs.
  - **4** rebuilds full text.
  - **8** verifies, changing nothing:
    - `c` against the files' rows (point probes both ways, no sort);
    - the counters and lanes against the rows;
    - `PRAGMA integrity_check` on every live file, the type index and the
      journal (C-27). A damaged file is a mismatch, named in the slot err;
    - each partition's `nobj` and `wh` against its rows.

## 6. Reads

**Orders.**
- **Seq** (datasync): per-file pages by rowid, merged. A single source's
  pages come from `rl_sid` (the filter's lanes checked inside the index),
  starting and ending at its lanes' seq bounds (`lane.minseq`, `maxseq`).
- **w** (windows): `r_w`, with ties in CID order. With a lane filter, a page's
  seqs are probed on `rl`'s key before any row is read.
- **CID:** the type index's `c` in (CID, pid) order, the unflushed entries
  merged over it; a page's rows are read by seq, one read transaction per file.

Copies collapse to one row per CID: the lowest pid that matches, also under
OFFSET.

**A18 bound (C-31).**
- With `lane.source` set, the bound is that source's newest N records: a
  newest-first walk of `rl_sid` in only the partitions holding the source,
  merged by their in-memory bounds (a partition is opened only once its
  newest seq could be above the cut; pages start at 64 seqs), stopping at N.
  The scan then reads through `rl_sid` above the cut.
- Without a source, the bound is the type's newest N (`r_s`).
- Every other filter applies above the cut.
- `<TYPE>@<source>` therefore returns that source's records. This is an
  intended difference from format 1, whose per-type in-memory window answered
  `CAT@celestrak-satcat-csv` with 0 frames.

**Candidates instead of a walk.**
- An exact CID (tag 8) is one type-index probe.
- An equality, IN, range or LIKE predicate on the object rule's first column
  reads `r_ke` (in CID order too: the candidates' CIDs are sorted).
- An epoch window (EPOCH, W, EPOCH_DAY bounds) of a seq-ordered read takes
  its seqs from `r_w` (up to 200,000).
- An epoch-ordered window of a small source (at most 20,000 rows and a
  quarter of each file) reads that source's seqs from `rl_sid`.

**Counters instead of a read.**
- HEAD with no filter answers from the type's counters; with only a lane
  filter, from the selected lanes' counters summed over the partitions (as
  format 2).
- SUMMARY 4 (disk usage) answers from maintained sizes: each file's pages
  after its writer's last commit, the WALs' uncheckpointed frames, the T/
  files' sizes refreshed once a second.
- EPOCH coverage and window counts with at most an epoch range read the
  epochs from `r_ke` (or `r_w`) alone when the type has no copies in several
  partitions. A window or day count (epoch or epoch-day range) sums the
  whole hours from `wh` and counts the two edge hours on `r_w`, when every
  row of the file is visible.
- An unfiltered W window's offset of 4,096 or more (and INDEX_PAGE's epoch
  phase when every record has an epoch) sums the hours above it from `wh`;
  `r_w` is counted from the top of the hour that holds it.
- A lane-filtered CID page whose lane is a large share of the type walks the
  type index's `c` and checks each entry's lane on `rl`'s key (no row): the
  offset is counted on it and only the page's rows are read.
- The reader pool keeps at least one connection per partition file (a
  type-wide read touches every file of its type).
- At start the long-work thread reads each type index once, start to end
  (the host's page cache): the first writes' dedupe probes and the first
  reads' CID probes do not read it a page at a time.

**Full text** is checked a page at a time against FTS5 (a rowid range or a
probe per seq), on a pooled reader connection: no set of every match.

**EPOCH nearest / as_of / forward** (C-32) take one `r_ke` seek per object
per partition. The count of nearest over every epoch of a one-file type is
the file's `nobj` (read in the same transaction).
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

- **`t_kill`** (`t_kill.cpp`). A forked engine ingests from three producers,
  with copies, the same new records from two producers at once every 7th call,
  a batch supersede every 13th call and a quota of 4 MiB every 11th. It is
  killed with SIGKILL at a random point, then the check runs:
  - every file passes `integrity_check`;
  - every row on disk is found by CID, and every copy of a CID has one seq;
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
  - `open_bench`;
  - `reads_bench` (the read gate's material shapes on a fixture store;
    `--open-only=1` times the open, e.g. after a kill).
- There are no per-package unit tests (C-33): the proof is these loops, the
  fixture equivalence and the SDN end-to-end harness. The SQL surface's two
  engine-backed tests (`tests/p4sql`) run in the suite when it is built in.

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

## 11. The per-file CID index is gone (C-34)

C-32 first put a CID index in every partition file (`r_c`). A record's CID
is a random key, so each commit dirtied about one `r_c` leaf page per record.
On a g2 clone, W01 (20 calls x 4,096 records, load 22-25): with `r_c` 12.2k
rec/s, call p50 177 ms, first call 1,194 ms; without it 28.0k rec/s, p50
129 ms, first call 576 ms. The type index already mapped every CID to its
copies, so `r_c` is dropped and CID order is served from `c`.
