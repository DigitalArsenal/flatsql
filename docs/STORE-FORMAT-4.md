# FlatSQL store format 4: one SQLite file per source feed x standard

Store format 4 keeps each standard's records in one SQLite file per source
feed. A feed is a record's (provider, source) tag pair, for example
`OMM/space-data-network-02@celestrak-gp.db`; a record with no source lives in
`<TYPE>/local.db`. Each feed file is the feed's own table: it holds the
feed's records with their own CID index, and nothing ties a record in one
feed file to a record in another. SQLite 3.53.4 is unmodified: only the VFS,
WAL and configuration are FlatSQL's. The engine is `cpp/src/p4`; it ships as
`wasm/flatsql-p4-threads.wasm` (wasm32-wasip1-threads).

- **Design:** the stack's `docs/architecture/flatsql-sqlite-partitions.md`,
  with its owner revisions of 2026-10-01 and 2026-10-02.
- **Interfaces:** the build-out contract (`CONTRACT.md`, version 16; C-37 is
  this layout, C-38 its CID index and type index):
  - the ABI in `cpp/include/flatsql/p4/flatsql_p4.h`;
  - the reader API for the SQL surface in `cpp/include/flatsql/p4/p4_reader.h`.
  Both are unchanged by the feed layout: a PUT's tag names its feed file, and
  the lane-style summaries are answered from each feed file's instance
  counters.

This file records what was built, how to run it, and what was measured.

## 1. Layout

```
<data>/fsql4/
  STORE                 64 B, magic FSQ4, format 4 (C-7); written last by activation
  MIGRATED              40 B, magic FSQM, format 4
  T/TYPES               the registered types (crc'd records)
  T/<TYPE>.spec         the registered spec TLV (C-5); a reopen serves before re-registration
  T/<TYPE>.idx          the type index: the feed and token registries, each feed file's counters, next seq
  T/<TYPE>.jnl          the intent journal
  T/<TYPE>.fts          full text (FTS5, contentless-delete; background)
  P/<TYPE>/<feed>.db    one file per source feed of the type; local.db for records with no source
```

- **A feed file is the feed.** Its provider and source are in its `meta` and
  in the type index's `feed` table; no row holds a provider or source
  string. The file name is the provider and source, URL-escaped outside
  `[A-Za-z0-9._-]` and joined by `@`; a name that would collide with another
  feed's on a case-insensitive file system, or is long, carries the feed id.
- **No cross-feed identity (C-38).** A record is identified inside its feed
  file by its CID (the file's one CID index) and its seq. A record that
  arrives through a second source feed is a new row set in that feed's file
  with its own seq (ingest). A migrated record keeps format 1's rowid as its
  seq in every feed file holding it. Type-wide reads merge the feed files,
  so a record held by N feeds answers once per feed (C-38 (5)).
- **A record is local only while no feed holds it.** An untagged write of a
  CID that feeds hold is a copy of the record in each of them (format 1's
  copy of a tagged record), not a local record; a tagged write of a local
  record takes it into its feed with its seq.
- **Lazy type files:** a type's `.idx`, `.jnl` and `.fts` are made by its
  first write. A registered type without data has only its `.spec`.

## 2. Files

**Feed file.**

`r(rid, seq, n, b, c, u, at, cid, e, k, ts, f, x, d, w)`, one row per
delivery of a record to the feed:

| Column | Meaning |
|---|---|
| `rid` | `seq << 16 \| j`: a record's rows are one rowid range, in arrival order |
| `seq` | the record's arrival number in the type's seq space (the datasync cursor); every row of the record has it |
| `n` | the publishing node: `node(id, producer, peer)`, the copy's producer token and peer |
| `b` | the batch: `batch(id, batch, ppeer, pkey)`, format 1's summary key inside the feed (0 on local rows) |
| `c`, `u` | the content key and the url: `ckey(id, ckey)`, `url(id, url)` (0 = "") |
| `at` | when this feed delivered it (format 1's tag `created_at`) |
| `cid`, `e`, `k`, `ts` | the CID, epoch, object key and source timestamp |
| `d`, `x`, `f` | the record bytes verbatim (sealed bytes when sealed), the signature, a sealed record's extracted COL values |
| `w` | `coalesce(e, ts)` (virtual) |

A record's rows are its copies x its instances: every copy (producer token)
of the record appears with every one of its instances (batch, content key,
producer peer and key) of the feed. A local record has one row per copy.

The CID is stored once per row (it cannot be derived from `d`: a migrated
copy keeps its own bytes, C-35, and SDN supplies the CID) and indexed once
per file:

| Index | Purpose |
|---|---|
| `r_s(seq)` | arrival: the merge by seq, the newest-N cut of `<TYPE>@<source>` (C-31), datasync, oldest-first quota, without the rows' pages |
| `r_c(cid)` | **the file's one CID index**: re-fetch dedupe in the feed, and SDN's by-CID lookups (GET, TAGS, DELETE, SCAN by CID), which probe each feed file of the type |
| `r_ke(k, e)` | object and epoch: EPOCH nearest / as_of / forward per object, object predicates, CAT supersede |
| `r_w(w DESC)` | epoch windows; a w group is put in CID order from its rows |
| `r_b(b)` | batch supersede and batch-filtered reads |

Also, committed with the rows in the same transaction:
- `inst(b, c, n, bytes, ...)`: each instance's records (each once), their
  bytes (each record's smallest copy) and format 1's summary times
  (`first`, `updated`, `maxat`, url);
- `tokc(producer, n, bytes, ...)`: each producer token's copies in the file;
- `ident(h, seq)`: the feed's IQC ingest identities (C-21, C-26);
- `meta`: the file's counters (rows, records, bytes, records without an
  epoch, the records' bytes, copies, the copies' bytes, and bounds).

Writers open with `synchronous=FULL`, WAL, and no autocheckpoint.

**Type index** (`.idx`, `synchronous=FULL`; C-38 (2)): no per-record entry.

| Table | Purpose |
|---|---|
| `feed(fid, provider, source, name, counters)` | feed id <-> (provider, source) and file; each file's counters, mirrored |
| `inst(fid, b, c, ...)` | each file's live instances and counters, mirrored (SUMMARY 3 without opening files) |
| `tok(id, token, peer)` | the type's producer tokens in order (the lowest id is the first copy, C-12) |
| `ftok(fid, tok, counters)` | each file's copies per token, mirrored (SUMMARY 2) |
| `meta` | next seq |

The type's totals (records, copies, bytes, bounds) are sums over the feed
files' counters; HEAD with no filter, SUMMARY 1 and 2 never open a feed file.

**Intent journal** (`.jnl`): `j(id AUTOINCREMENT, op, fid, seq, k, s, v)` and
`jm(seq_reserved)`. Ops: `J_FEED` and `J_TOK` (new feed and token ids),
`J_TOUCH` (a feed file a write changes), `J_MOVE` (a record whose rows leave
a file, with its seq, for another file of the same write).

## 3. Writes

- **One writer per file.** Every feed file of a type is written by the
  type's one writer thread (types are spread over the writer threads); a
  file has one writer connection.
- **Calls** arrive in mailbox slots, in two pools (C-6): 8 MiB write
  requests and 64 KiB read requests. A type's backlog has a record credit;
  a full backlog answers `P4_E_BUSY`.
- **A PUT group** (the queued PUT calls of a type, up to `groupRecords`):
  1. parse, check and extract;
  2. ingest mode: each record goes to every feed its call's tags name,
     found in that feed file by its CID (`r_c`). In the file it is a new
     record (NEW: a fresh seq), a new copy (COPY: the holder's bytes and ts
     with this write's signature and peer), a new instance (RETAG), a repeat
     (DUP: the url follows the latest write, C-3), or an ingest-identity
     repeat (IDENT_DUP, C-26: the feed's `ident` names the held record,
     which takes the repeat's tag). A tagged record that is new to its feed
     and has local rows takes them, with their seq (a move: the local copies
     join the feed's instances and the local rows go). An untagged record
     is a copy in every feed file holding its CID (each file's `r_c`
     probed), or, held by none, goes to local;
  3. migrate mode: format 1's rowid is the record's seq in every feed file
     holding it. The record goes to the feeds of its tag instances and to
     the feeds already holding its seq (a copy sent without instances joins
     the record's instances there); with none, to local. Local rows of the
     seq move into the feeds once the record has an instance. A seq held by
     another CID is `P4_REJ_SEQ`. A copy keeps its own bytes (C-35);
  4. CAT supersede-on-ingest (ingest mode only, C-20): the scope feed's
     records of the same object identity are retired from that feed;
  5. seqs for records new to a feed, in content-time order (durable seq
     blocks);
  6. the journal (`synchronous=FULL`: the touched feed files, the moves,
     new ids, the seq reservation);
  7. each feed file in one transaction (rows, its counters, instances,
     tokens and identities), feed files before local;
  8. the type index in one transaction (the touched files' mirrors, new
     tokens, next seq);
  9. publish (visible-through) and ack: the ack follows the commits (C-4).
- **SUPERSEDE** (`provider`, `source`, keep): one feed file; its rows whose
  batch is not the kept one go, a chunk of 32,768 rows per transaction; a
  record left with no row in the file leaves the feed.
- **DELETE**: every row of the CIDs in every feed file (each file's `r_c`).
  `deleted` counts each CID's distinct copies over the files.
- **QUOTA_GC**: the type's oldest record row sets by arrival (the feed files
  merged by seq), every row of each.

## 4. Crash safety

Each feed file's transaction carries its rows with its own counters,
instances, token counts and identities: the file is consistent on its own.
The one write that spans two files with one record is a move (a local
record taken into a feed), committed feed first. Every open replays the
journal before it serves (M8):

1. the feed and token ids it names are registered;
2. a move cut between its two files is finished: the source file's rows of
   the seq go once another file holds the seq (a destination that did not
   commit leaves the record where it was);
3. every touched feed file's counters, instances and token counts are read
   from the file;
4. they are mirrored into the type index with the token registry and the
   next seq, in one index transaction;
5. the journal's rows go.

The cost is the journal tail's (one write), not the store's. A write whose
type-index commit fails runs the same replay at once; if that fails too,
the type refuses writes (`P4_E_IO`) until a reopen. A feed file the index
says has records and that is missing is quarantined (`P4_E_CORRUPT`, named).
Nothing unlinks or replaces a feed file. A write cut between two feed files
leaves the files it committed: a retry of an unacknowledged call finds its
records there (DUP) and adds them to the rest.

Every connection to a format-4 file is opened with `share=1` through
FlatSQL's VFS (`openConn`): it attaches to the path's node, so the engine's
connections see each other's locks and one WAL index. A connection opened
beside a running engine without `share=1` gets a private node (every lock
granted, a WAL index of its own); at close it takes itself for the file's
last connection and, when the WAL is empty, deletes it under the engine's
connections. The engine and the SQL surface open no other connection (the
SQL surface's own database is `:memory:` with ATTACH refused).

## 5. Maintenance

- **The maintenance thread:** checkpoints for feed files, the type index,
  the journal and full text; closing evicted writer connections; the T/
  files' sizes for SUMMARY 4.
- **Checkpoints never wait for a writer.** PASSIVE passes copy a WAL while
  the writer keeps committing; a TRUNCATE takes the writer lock only if it
  is free that instant. A file's one writer truncates its own WAL after a
  commit that leaves it at a quarter of `walTotal` (or 32 MiB while the
  instance's WALs pass three quarters of it).
- **The long-work thread:** REBUILD and QUOTA_GC calls, the configured
  quota once a second, and full text; their per-type work runs on the
  type's writer thread.
- **Quota** measures every file by its pages in use (pages less free pages:
  feed files, the type index, the journal and full text alike) and deletes
  the oldest record row sets by arrival until the store fits.
- **Full text** follows the records: catching up adds the seqs past
  `through` (the feed files merged by seq; a seq once, from the first file
  holding it), and the rows of seqs that left a feed file since the last
  pass (supersede, delete, quota, CAT supersede-on-ingest) are deleted
  first; a migrated seq (below the seq floor) only once no feed file holds
  it, and a seq that moved to another file keeps its row. The gone list is in memory: a crash leaves those rows (a search drops
  them, as it drops any hit without a live record), and REBUILD 4 removes
  them.
- **REBUILD:** 1 adds the feed files' secondary indexes (after a migration's
  bulk append); 2 recounts every feed file's counters, instances and tokens
  from its rows and mirrors them into the type index; 4 rebuilds full text;
  8 makes the same comparisons (file, engine and type-index mirror) and
  changes nothing, and runs `PRAGMA integrity_check` on every feed file, the
  type index and the journal (C-27). A record whose rows are not its copies
  x its instances, or that holds two CIDs, is a mismatch.

## 6. Reads

A scan first picks its feed files: the lane filter's (provider and source
name a feed file; a batch, content key or producer peer narrows to the feeds
holding such an instance), else every feed of the type. Each file is walked
in the scan's order through its own indexes (`r_s` arrival, `r_w` epoch,
`r_c` CID), a chunk at a time, and the walks are merged (C-38 (4); ties by
feed id). A page of candidates is resolved by reading each record's rows
(one rid range of its file, one read transaction per file). A record answers
once per feed file (C-10 within the file, C-38 (5) across files). Its copy
is the lowest token's matching row (C-12); its tag the earliest instance of
its feed that matches the lane filter (§3.6), with provider and source from
the file and the rest from its ids: never blank when the record has a tag.

- **A18 (C-31):** `<TYPE>@<source>` is the newest N records of that type from
  that source (the source's feed files' `r_s`, newest first, merged); `<TYPE>`
  the type's newest N (every feed file's `r_s`, merged). Every other filter
  applies above the cut.
- **Candidates instead of a walk:** an exact CID (each file's `r_c`), an
  equality or IN on the object rule's first column (each file's `r_ke`).
- **EPOCH nearest / as_of / forward:** one seek per object in each file's
  `r_ke`, read from the target in rank order until an epoch group has a
  record that passes the filters; ties at the best epoch go to the lowest
  CID (format 1's ranking), then the lowest feed id. One answer per entity.
  A record without an object is its own entity.
- **By CID:** GET answers from the first feed file holding the CID (its
  copies, the lowest token first); TAGS lists the tag instances of every
  feed file holding it, each with that file's seq.
- **Counters:** HEAD with no filter, SUMMARY 1 (records, copies, bytes),
  SUMMARY 2 (one row per producer token: its copies and bytes), SUMMARY 3
  (one row per feed file and instance: provider, source, batch, content key,
  producer peer and key, records, bytes, first, updated) and SUMMARY 4 are
  answered from counters without reading rows.
- **Full text** is checked a page at a time against FTS5.

**Caps** end a read with its status: rows examined, bytes read, result rows
and bytes, and cancel. RB1 streams always end with RB1E.

**SQL surface.** `src/p4sql` answers ops 30 and 31 through `p4_reader.h`, and
the engine writes their RB1E (C-28).

## 7. Memory

There is one engine-wide budget (design §9), whatever the number of feed
files.
- SQLite is built without memory statistics (C-30). The SQL surface's
  allocator enforces the hard heap limit (config tag 27, 640 MiB) and serves
  stats 29 and 30.
- Reader connections are one pool (tag 23) inside a shared cache budget
  (tags 23 x 24): idle connections close, least recently used first, while
  the pool is over its count or its caches pass the budget, and while the
  heap is past three quarters of the soft limit. A `SQLITE_NOMEM` open
  closes the idle readers and tries once more.
- Writer connections are an LRU (tag 21); the LRU never closes a pinned one.
- The type index is written with every write: nothing is pending in memory.

## 8. The artifact

| | |
|---|---|
| Build | `bash scripts/build-wasm.sh --ps --linux` (Docker, Linux wasi-sdk 30) builds the released bytes with `flatsql-ps-threads.wasm`. CMake: `cpp/cmake/flatsql_p4_wasm.cmake`, globbing `src/p4`, `src/p4sql`, `tests/p4` and `tests/p4sql`. |
| SQLite | The 3.53.4 amalgamation, byte-identical (CMake checks its sha256). Options: `THREADSAFE=2`, WAL, FTS5, `DEFAULT_MEMSTATUS=0`, `TEMP_STORE=3`, `SQLITE_OS_OTHER`. The only VFS is `flatsql_io`. |
| Imports | `wasi_snapshot_preview1`, `wasi.thread-spawn`, `env.memory` (shared, at most 32768 pages) and the seven `env.flatsql_io_*`. `scripts/check-wasm-imports.mjs` fails on any change. |
| Exports | `flatsql_p4_init start stop wake layout register_type activate set_quota stats alloc free`, `wasi_thread_start`, `_initialize` and `memory`. |

## 9. Tests

```
FLATBUFFERS_DIR=<flatbuffers> cmake -S cpp -B cpp/build && cmake --build cpp/build --target flatsql_p4_test flatsql_p4_fault_test -j 6
cpp/build/flatsql_p4_test                                     # the SQL surface's engine tests
cpp/build/flatsql_p4_test --test=t_kill --slow=1 --rounds=30  # kill -9 loop
cpp/build/flatsql_p4_fault_test --test=t_power_loss --slow=1 --rounds=30
```

- **`t_kill`** (`t_kill.cpp`): a forked engine ingests from three producers
  into three feeds (`prov@src`, `prov@src2` and `local`; repeats of earlier
  records land in another feed, so records sit in two or three feed files,
  a row set in each; the same new records from three producers at once,
  each to its own feed; a batch supersede every 13th call; a 4 MiB quota
  every 11th), is killed with SIGKILL at a random point, and the store is
  checked: every file passes `integrity_check`; a CID has one seq in each
  feed file and a seq one CID; no CID is both in local and in a feed file;
  every CID on disk is found by GET; the count
  equals the (feed file, CID) pairs on disk; REBUILD 8 finds no mismatch;
  new seqs are above every seq on disk; no file is left open. The wasm test
  command runs the same loop under the Node wasi-threads host
  (`scripts/p4-wasm-suite.mjs --kill-rounds N`).
- **`t_power_loss`:** the engine over FaultFs, frozen at a random I/O call,
  five crash modes, the same three feeds; every acknowledged record is
  there and every acknowledged tag instance still lists its source,
  REBUILD 8 is clean, the count equals the records of a full scan, each
  with its own seq.
- The proof is end to end (C-33): the SDN harness on the real engine
  (`sdn-server/internal/storage/format4proof`): `store-migrate --to 4`, every
  benchset read and the coverage classes against format 1 field by field,
  W01-W10, kill -9 and LazyFS power loss, and the growth steps.

## 10. Measured

See the build-out report (`ENGINE-REPORT.md`, sections "Feed tables" and
"C-38").
