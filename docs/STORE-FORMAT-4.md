# FlatSQL store format 4: one SQLite file per source feed x standard

Store format 4 keeps each standard's records in one SQLite file per source
feed. A feed is a record's (provider, source) tag pair, for example
`OMM/space-data-network-02@celestrak-gp.db`; a record with no source lives in
`<TYPE>/local.db`. SQLite 3.53.4 is unmodified: only the VFS, WAL and
configuration are FlatSQL's. The engine is `cpp/src/p4`; it ships as
`wasm/flatsql-p4-threads.wasm` (wasm32-wasip1-threads).

- **Design:** the stack's `docs/architecture/flatsql-sqlite-partitions.md`,
  with its owner revisions of 2026-10-01 and 2026-10-02.
- **Interfaces:** the build-out contract (`CONTRACT.md`, version 15; C-37 is
  this layout):
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
  T/<TYPE>.idx          the type index (derived from the feed files)
  T/<TYPE>.jnl          the intent journal
  T/<TYPE>.fts          full text (FTS5, contentless-delete; background)
  P/<TYPE>/<feed>.db    one file per source feed of the type; local.db for records with no source
```

- **A feed file is the feed.** Its provider and source are in its `meta` and
  in the type index's `feed` table; no row holds a provider or source
  string. The file name is the provider and source, URL-escaped outside
  `[A-Za-z0-9._-]` and joined by `@`; a name that would collide with another
  feed's on a case-insensitive file system, or is long, carries the feed id.
- **The same record from two feeds is a row in each feed's file**, under one
  seq (dedupe is per type: one seq per CID).
- **Lazy type files:** a type's `.idx`, `.jnl` and `.fts` are made by its
  first write. A registered type without data has only its `.spec`.

## 2. Files

**Feed file.**

`r(rid, seq, n, b, c, u, at, cid, e, k, ts, f, x, d, w)`, one row per
delivery of a record to the feed:

| Column | Meaning |
|---|---|
| `rid` | `seq << 16 \| j`: a record's rows are one rowid range, in arrival order |
| `seq` | the type-wide arrival number (the datasync cursor); every row of a record has it |
| `n` | the publishing node: `node(id, producer, peer)`, the copy's producer token and peer |
| `b` | the batch: `batch(id, batch, ppeer, pkey)`, format 1's summary key inside the feed (0 on local rows) |
| `c`, `u` | the content key and the url: `ckey(id, ckey)`, `url(id, url)` (0 = "") |
| `at` | when this feed delivered it (format 1's tag `created_at`) |
| `cid`, `e`, `k`, `ts` | the CID, epoch, object key and source timestamp |
| `d`, `x`, `f` | the record bytes verbatim (sealed bytes when sealed), the signature, a sealed record's extracted COL values |
| `w` | `coalesce(e, ts)` (virtual) |

A record's rows are its copies x its instances: every copy (producer token)
of the record appears with every one of its instances (batch, content key,
producer peer and key) in each feed it has. A batch supersede of one feed
file therefore never loses a copy the record keeps through another feed, and
a record with no instance has one row per copy in `local.db`.

| Index | Purpose |
|---|---|
| `r_s(seq)` | arrival: the newest-N cut of `<TYPE>@<source>` (C-31) and the one-feed datasync walk, without the rows' pages |
| `r_c(cid)` | CID order inside a feed |
| `r_ke(k, e)` | object and epoch: EPOCH nearest / as_of / forward per object, object predicates, CAT supersede |
| `r_w(w DESC, cid)` | epoch windows, ties in CID order |
| `r_b(b)` | batch supersede and batch-filtered reads |

Also `inst(b, c, n, bytes, ...)`: each instance's records (each once), their
bytes (each record's smallest copy) and format 1's summary times (`first`,
`updated`, `maxat`, url), and `meta`: the file's counters (rows, records,
bytes, records without an epoch, and bounds). Both commit with the rows.

Writers open with `synchronous=FULL`, WAL, and no autocheckpoint.

**Type index** (`.idx`, `synchronous=FULL`), the per-standard index of
C-37 (5):

| Table | Purpose |
|---|---|
| `feed(fid, provider, source, name, counters)` | feed id <-> (provider, source) and file; each file's counters, mirrored |
| `inst(fid, b, c, ...)` | each file's live instances and counters, mirrored (SUMMARY 3 without opening files) |
| `tok(id, token, peer, counters)` | the type's producer tokens and their copies' counters (SUMMARY 2) |
| `x(seq, fid, cid, len, w, k, e, cp)` | one entry per (record, feed file holding it); `cp` = the copies in that file |
| `x_c(cid, seq, fid, e)` | CID -> (seq, feed): lookup, dedupe, CID-ordered windows |
| `x_w(w DESC, cid, seq, fid, e)` | type windows, index pages, epoch windows |
| `x_k(k, e, cid, seq, fid)` | EPOCH per object (types with an object rule) |
| `ident(src, h, seq, cid)` | IQC ingest identities |
| `meta` | records, bytes, copies, copy bytes, bounds, next seq, full text through |

A type-wide read walks `x` and reads only the feed files that hold the
page's records; it never opens every feed file of the type.

**Intent journal** (`.jnl`): `j(id AUTOINCREMENT, op, fid, seq, k, s, v)` and
`jm(seq_reserved)`. Ops: `J_FEED` and `J_TOK` (new feed and token ids),
`J_TOUCH` (a (feed file, record) a write changes), `J_IDENT` (an ingest
identity).

## 3. Writes

- **One writer per file.** Every feed file of a type is written by the
  type's one writer thread (types are spread over the writer threads); a
  file has one writer connection. A record's rows across its feed files
  (a copy into every feed of the record, an untagged record that gains a
  tag) commit in order on that thread.
- **Calls** arrive in mailbox slots, in two pools (C-6): 8 MiB write
  requests and 64 KiB read requests. A type's backlog has a record credit;
  a full backlog answers `P4_E_BUSY`.
- **A PUT group** (the queued PUT calls of a type, up to `groupRecords`):
  1. parse, check and extract;
  2. load each record from the type index (`x_c`) and its feed files (its
     rows: one rid range per file);
  3. plan its delivery: a new record (NEW), a new copy (COPY: the holder's
     bytes and ts with this write's signature and peer; in migrate mode its
     own bytes, C-35), a new instance (RETAG), a repeat (DUP: the url follows
     the latest write, C-3), an ingest-identity repeat (IDENT_DUP, C-26);
     every copy is then put with every instance;
  4. CAT supersede-on-ingest (ingest mode only, C-20): the scope feed's
     records of the same object identity are retired from that feed;
  5. seqs for new records in content-time order (durable seq blocks);
  6. the journal (`synchronous=FULL`);
  7. each feed file in one transaction (rows, its counters, its instances),
     feed files before local;
  8. the type index in one transaction (the records' entries, the type's
     and tokens' counters, the feeds' mirrors);
  9. publish (visible-through) and ack: the ack follows the commits (C-4).
- **SUPERSEDE** (`provider`, `source`, keep): one feed file; its rows whose
  batch is not the kept one go, a chunk of 32,768 rows per transaction; a
  record left with no row anywhere is gone.
- **DELETE**: every row of the CIDs in every feed file.
- **QUOTA_GC**: the type's oldest records by arrival (the type index's seq
  order), every row of each.
- **Migrate mode** (PUT mode 1, create mode 2): seqs and tag instances are
  the caller's (format 1's rowids and `created_at`; each format-1 tag
  instance becomes rows in its feed's file); a copy keeps its own bytes;
  identities are registered (C-21); a new feed file is made without its
  secondary indexes, which REBUILD 1 adds; full text waits for activation.

## 4. Crash safety

Every open replays the journal before it serves (M8):

1. the feed and token ids it names are registered;
2. every touched feed file's counters are reloaded from its `meta` and
   `inst` (they commit with its rows);
3. every touched record is read from its feed files and brought in line
   (every copy with every instance; local rows only without one): a write
   cut between two feed files leaves rows that only add, completed here;
4. its type-index entries, the type's and tokens' counters and the feeds'
   mirrors are set from its rows, in one index transaction;
5. the journal's rows go.

The cost is the journal tail's (one write), not the store's. A write whose
type-index commit fails runs the same replay at once; if that fails too,
the type refuses writes (`P4_E_IO`) until a reopen. A feed file the index
says has records and that is missing is quarantined (`P4_E_CORRUPT`, named).
Nothing unlinks or replaces a feed file.

Every connection to a format-4 file is opened with `share=1` through
FlatSQL's VFS (`openConn`): it attaches to the path's node, so the engine's
connections see each other's locks and one WAL index. A connection opened
beside a running engine without `share=1` gets a private node (every lock
granted, a WAL index of its own); at close it takes itself for the file's
last connection and, when the WAL is empty, deletes it under the engine's
connections, after which their WAL handles and the WAL index no longer
describe one file. The wasm kill loop found this in its own check (read-only
connections for `integrity_check`, opened the default way: on wasm the
default VFS is FlatSQL's), as a feed file whose page 1 was another page
after 28-153 rounds; the check now opens them with `share=1`. The engine and
the SQL surface open no other connection (the SQL surface's own database is
`:memory:` with ATTACH refused).

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
  feed files, the type index, the journal and full text alike), so the
  index entries a pass deletes count as freed, and deletes the oldest
  records by arrival until the store fits.
- **Full text** follows the records: catching up adds the records past
  `through` (one row per record, from one of its feed files), and the rows
  of records gone since the last pass (supersede, delete, quota, CAT
  supersede-on-ingest: no row left in any feed file) are deleted first.
  The gone list is in memory: a crash leaves those rows (a search drops
  them, as it drops any hit without a live record), and REBUILD 4 removes
  them.
- **REBUILD:** 1 adds the feed files' secondary indexes (after a migration's
  bulk append); 2 recounts every feed file's counters and instances from
  its rows, sets every type-index entry from the files (dangling entries go)
  and the type's counters from the entries; 4 rebuilds full text; 8 makes
  the same comparisons and changes nothing, and runs `PRAGMA
  integrity_check` on every feed file, the type index and the journal
  (C-27). A record whose rows are not its copies x its instances is a
  mismatch.

## 6. Reads

A scan first picks its feed files: the lane filter's (provider and source
name a feed file; a batch, content key or producer peer narrows to the feeds
holding such an instance), else every feed of the type. Then:

- **One feed file** (a `<TYPE>@<source>` read, or a type with one feed): that
  file's own indexes (`r_s` arrival, `r_w` epoch, `r_c` CID, `r_ke` object).
- **Several:** the type index (`x` seq, `x_w` epoch, `x_c` CID, `x_k`
  object), which names the feed files holding each record; only those are
  read.

A page of candidates is resolved by reading each record's rows (one rid
range per feed file, one read transaction per file). A record answers once
(C-10). Its copy is the lowest token's matching row (C-12); its tag the
earliest instance that matches the lane filter (§3.6), with provider and
source from the row's feed file and the rest from its ids: never blank when
the record has a tag.

- **A18 (C-31):** `<TYPE>@<source>` is the newest N records of that type from
  that source (the source's feed files' `r_s`, newest first, merged); `<TYPE>`
  the type's newest N (`x`). Every other filter applies above the cut.
- **Candidates instead of a walk:** an exact CID (`x_c` / `r_c`), an
  equality or IN on the object rule's first column (`x_k` / `r_ke`).
- **EPOCH nearest / as_of / forward:** one seek per object in `x_k` (or
  `r_ke`), read from the target in rank order until an epoch group has a
  record that passes the filters; ties at the best epoch go to the lowest
  CID (format 1's ranking). A record without an object is its own entity.
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
cpp/build/flatsql_p4_test --test=t_kill --rounds=30           # kill -9 loop
cpp/build/flatsql_p4_fault_test --test=t_power_loss --slow=1 --rounds=30
```

- **`t_kill`** (`t_kill.cpp`): a forked engine ingests from three producers
  into three feeds (`prov@src`, `prov@src2` and `local`; repeats of earlier
  records land in another feed, so records sit in two or three feed files;
  the same new records from three producers at once, each to its own feed;
  a batch supersede every 13th call; a 4 MiB quota every 11th), is killed
  with SIGKILL at a random point, and the store is checked: every file
  passes `integrity_check`; a CID has one seq in every feed file; every row
  on disk is found by CID with that seq; the count equals the distinct CIDs
  on disk; REBUILD 8 finds no mismatch; new seqs are above every seq on
  disk; no file is left open. The wasm test command runs the same loop
  under the Node wasi-threads host (`scripts/p4-wasm-suite.mjs
  --kill-rounds N`).
- **`t_power_loss`:** the engine over FaultFs, frozen at a random I/O call,
  five crash modes, the same three feeds; every acknowledged record is
  there and every acknowledged tag instance still lists its source,
  REBUILD 8 is clean, the count equals a full scan's distinct CIDs.
- The proof is end to end (C-33): the SDN harness on the real engine
  (`sdn-server/internal/storage/format4proof`): `store-migrate --to 4`, every
  benchset read and the coverage classes against format 1 field by field,
  W01-W10, kill -9 and LazyFS power loss, and the growth step.

## 10. Measured

See the build-out report (`ENGINE-REPORT.md`, section "Feed tables").
