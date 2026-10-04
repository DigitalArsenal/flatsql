# FlatSQL store format 4: a FlatBuffer stream and an index per source feed x standard

Store format 4 keeps each standard's records per source feed. A feed is a
record's (provider, source) tag pair, for example `OMM` from
`space-data-network-02@celestrak-gp`; a record with no source belongs to the
type's `local` feed. Each feed is two files:

- `<feed>.fsdata`, the feed's records exactly as they arrived, as a pure
  FlatBuffer stream: `[u32 LE size][FlatBuffer]` frames back to back, with no
  file header, magic, per-frame tag, CRC, padding or trailer. A stock
  FlatBuffers reader walks it from byte 0 to the end with no FlatSQL code
  (owner, 2026-10-03: "the flatbuffers written to disk should be readable by a
  flatbuffer reader directly"). This is format 1's `.fsdata` (flatsql commit
  645f86c, `docs/STORAGE-DURABILITY.md` §6.4).
- `<feed>.db`, SQLite 3.53.4 unmodified (only the VFS, WAL and configuration
  are FlatSQL's): the feed's index rows and per-row metadata. A row points at
  its record's frame by `(off, len)`; no record bytes are in SQLite.

The records are streamed and the indexes built beside them (owner,
2026-10-03: "stream them directly while indices and btrees (SQLite database
metadata) were created in a separate thread in the data being streamed in";
BRIEF4 ruling (B)): an acknowledged write has its frames synced in the stream
and its rows committed *staged*, which writes appended pages only; the
writer's indexer thread later merges a feed's staged rows into its
random-keyed indexes in one large transaction (§3).

Each feed is its own table: it holds the feed's records with their own CID
index, and nothing ties a record in one feed to a record in another. The
engine is `cpp/src/p4`; it ships as `wasm/flatsql-p4-threads.wasm`
(wasm32-wasip1-threads).

- **Design:** the stack's `docs/architecture/flatsql-sqlite-partitions.md`,
  with its owner revisions of 2026-10-01, 2026-10-02 and 2026-10-03
  (revision 3: the streams and the index, the original design).
- **Interfaces:** the build-out contract (`CONTRACT.md`, C-37 is this layout,
  C-38 its CID index and type index, BRIEF4 the streams):
  - the ABI in `cpp/include/flatsql/p4/flatsql_p4.h`;
  - the reader API for the SQL surface in `cpp/include/flatsql/p4/p4_reader.h`.
  Both are unchanged by the streams: a PUT's tag names its feed, a read
  answers the record bytes from the stream, and SUMMARY 4's `db` column
  counts each feed's index and stream.

This file records what was built, how to run it, and what was measured.

## 1. Layout

```
<data>/fsql4/
  STORE                    64 B, magic FSQ4, format 4 (C-7); written last by activation
  MIGRATED                 40 B, magic FSQM, format 4
  T/TYPES                  the registered types (crc'd records)
  T/<TYPE>.spec            the registered spec TLV (C-5); a reopen serves before re-registration
  T/<TYPE>.idx             the type index: the feed and token registries
  T/<TYPE>.fts             full text (FTS5, contentless-delete; background)
  P/<TYPE>/<feed>.fsdata   the feed's records: [u32 LE size][FlatBuffer] ... and nothing else
  P/<TYPE>/<feed>.db       the feed's index: rows (off, len, CID, seq, provenance), its counters
  P/<TYPE>/local.fsdata, local.db   records with no source
```

- **A feed is its stream and its index.** Its provider and source are in the
  index's `meta` and in the type index's `feed` table; no row holds a provider
  or source string, and no frame holds anything but the record. The file name
  is the provider and source, URL-escaped outside `[A-Za-z0-9._-]` and joined
  by `@`; a name that would collide with another feed's on a case-insensitive
  file system, or is long, carries the feed id. So a name never holds `/`, and
  the files are always flat in `P/<TYPE>` whatever the provider or source
  holds (a space, `/`, `/../`, `%`, `?`, `#`, non-ASCII bytes).
- **Stream generations.** A compaction (§5) writes the live frames into the
  next generation, `<feed>.<gen>.fsdata` (generation 0 is `<feed>.fsdata`), a
  pure stream again; the index's `meta` names the current generation.
- **No cross-feed identity (C-38).** A record is identified inside its feed by
  its CID (the index's one CID index) and its seq. A record that arrives
  through a second source feed is a new row set in that feed with its own seq
  (ingest). A migrated record keeps format 1's rowid as its seq in every feed
  holding it. Type-wide reads merge the feeds, so a record held by N feeds
  answers once per feed (C-38 (5)).
- **A record is local only while no feed holds it.** An untagged write of a
  CID that feeds hold is a copy of the record in each of them (format 1's copy
  of a tagged record), not a local record; a tagged write of a local record
  takes it into its feed with its seq.
- **Lazy type files:** a type's `.idx` and `.fts` are made by its first write.
  A registered type without data has only its `.spec`.

## 2. Files

**Stream** (`<feed>.fsdata`). Frames `[u32 LE size][bytes]`, back to back,
appended in arrival order. `bytes` is exactly the record as it arrived: the
FlatBuffer the CID was computed over (a sealed record: its sealed bytes, an
`SDF1`/`SDFN` envelope, which a reader cannot parse without the key). The
rows of a record whose bytes are the same (its copies, its instances) share
one frame. Frames no live row names (deleted, superseded or evicted records)
stay until a compaction.

**Index** (`<feed>.db`):

`r(rid, seq, n, b, c, u, at, cid, e, k, ts, f, x, off, len, kk, m, w, cp)`,
one row per delivery of a record to the feed:

| Column | Meaning |
|---|---|
| `rid` | `seq << 16 \| j`: a record's rows are one rowid range, in arrival order |
| `seq` | the record's arrival number in the type's seq space (the datasync cursor); every row of the record has it |
| `n` | the publishing node: `node(id, producer, peer)`, the copy's producer token and peer |
| `b` | the batch: `batch(id, batch, ppeer, pkey)`, format 1's summary key inside the feed (0 on local rows) |
| `c`, `u` | the content key and the url: `ckey(id, ckey)`, `url(id, url)` (0 = "") |
| `at` | when this feed delivered it (format 1's tag `created_at`) |
| `cid`, `e`, `k`, `ts` | the CID, epoch, object key and source timestamp (the copy's: a record's copies share the record's, except a COPY made by StoreRoutedByProducer, which keeps its own, C-39 E6) |
| `off`, `len` | the record's frame in the stream: `[u32 len][bytes]` at `off` |
| `x`, `f` | the signature, a sealed record's extracted COL values |
| `kk` | the object key's id in `okey(id, k)` (each key of the feed once): the object index holds a small integer, not the key |
| `m` | 0 while the row is staged, 1 once merged (§3) |
| `w` | `coalesce(e, ts)` (virtual) |
| `cp` | `substr(cid, 1, 8)` (virtual): the CID index's key |

A record's rows are its copies x its instances: every copy (producer token)
of the record appears with every one of its instances (batch, content key,
producer peer and key) of the feed. A local record has one row per copy.

The CID is stored once per row (SDN supplies it; a sealed record's bytes are
not what it names) and indexed once per file:

| Index | Rows | Purpose |
|---|---|---|
| `r_s(seq)` | all | arrival: the merge by seq, the newest-N cut of `<TYPE>@<source>` (C-31), datasync, oldest-first quota, without the rows' pages |
| `r_c(cp)` | merged (`WHERE m=1`) | **the index's one CID index**, on the CID key's first 8 bytes (a lookup matches `cp` and then the whole CID in the row; `cp` order is CID order, a walk orders a `cp` group by the CID): re-fetch dedupe in the feed, and SDN's by-CID lookups (GET, TAGS, DELETE, SCAN by CID), which probe each feed of the type |
| `r_ke(kk, e)` | merged | object and epoch: EPOCH nearest / as_of / forward per object, object predicates, CAT supersede |
| `r_w(w DESC)` | merged | epoch windows; a w group is put in CID order from its rows |
| `r_b(b)` | all | batch supersede and batch-filtered reads |
| `r_a(at DESC)` (feeds) / `r_t(ts DESC)` (local) | all | newest-first pages (C-39 E2): delivery time, and the untagged copies' time; neither holds a CID (a group is put in order from its rows) |
| `r_m(rid)` | staged (`WHERE m=0`) | the staged rows (open rebuilds the feed's view from it) |

`r_c`, `r_ke` and `r_w` take random keys (CIDs, objects, epochs): a commit
that inserted into them dirtied pages all over each (about two random pages
per record at 4,096-record commits, each written to the WAL and again by
the checkpoint: ~16 KB per record into a big feed). They are partial
indexes on the merged rows, so an ack writes none of their pages; `r_s`,
`r_b`, `r_a`/`r_t` and `r_m` take keys that arrive in order (appended pages).
Feed index files have 4 KiB pages whatever the spec's tag 8 says (a page
smaller than the VFS's 4 KiB sector drags its sector-mates into every write;
a larger one writes more per dirty page).

Also, committed with the rows in the same transaction:
- `inst(b, c, n, bytes, ...)`: each instance's records (each once), their
  bytes (each record's smallest copy) and format 1's summary times
  (`first`, `updated`, `maxat`, url);
- `tokc(producer, n, bytes, ...)`: each producer token's copies in the file;
- `ident(h, seq)`: the feed's IQC ingest identities (C-21, C-26), and
  `idst(h, seq)` the staged ones (appended; the merge moves them into
  `ident`; a staged identity is the latest);
- `okey(id, k)`: each object key of the feed once (its `kk`);
- `moved(seq)`: the local records this feed took in (a move's intent, §4);
- `meta`: the file's counters (rows, records, bytes, records without an
  epoch, the records' bytes, copies, the copies' bytes, the stream's live
  frame bytes `fbytes`, and bounds), the stream's committed end `mark` and
  its generation `gen`.

Writers open with `synchronous=FULL`, WAL, and no autocheckpoint. A new
index's tables, its meta rows and its indexes commit in one transaction (a
migration's index is made without its secondary indexes, `meta` ix = 0, until
REBUILD 1), so an index either has no schema or a complete one.

**Type index** (`.idx`, `synchronous=FULL`; C-38 (2)): only registries.

| Table | Purpose |
|---|---|
| `feed(fid, provider, source, name, gen)` | feed id <-> (provider, source), file name, and the stream generation (an index rebuilt from its stream reads it) |
| `tok(id, token, peer)` | the type's producer tokens in order (the lowest id is the first copy, C-12) |

A write registers a new feed or token here while its type's writer plans it,
before any file of a new feed exists (only that writer and the open use the
type index): every feed file on disk belongs to a registered feed, which
every open visits and recovers. Each feed's counters live in its own index (read at open); the
type's totals (records, copies, bytes, bounds) are sums over the feeds, so
HEAD with no filter, SUMMARY 1 and 2 never open a feed file. There is no
intent journal.

## 3. Writes

- **One writer per feed.** Every feed of a type is written by the type's one
  writer thread (types are spread over the writer threads); an index has one
  writer connection, which belongs to the writer's **indexer thread**.
- **Calls** arrive in mailbox slots, in two pools (C-6): 8 MiB write
  requests and 64 KiB read requests. A type's backlog has a record credit;
  a full backlog answers `P4_E_BUSY`.
- **A PUT group** (the queued PUT calls of a type, up to `groupRecords`) is
  planned on the writer and committed on its indexer:
  1. *writer:* parse, check (frame size, file identifier, BFBS, CID) and
     extract the keys (CID, epoch, object, COL rules) from the frame bytes;
  2. *writer:* ingest mode: each record goes to every feed its call's tags
     name, found in that feed by its CID (`r_c`). In the feed it is a new
     record (NEW: a fresh seq), a new copy (COPY: the holder's bytes and ts
     with this write's signature and peer; with PUT tag 55,
     StoreRoutedByProducer, this write's own ts, C-39 E6), a new instance
     (RETAG), a repeat (DUP: the url follows the latest write, C-3), or an
     ingest-identity repeat (IDENT_DUP, C-26: the feed's `ident` names the
     held record, which takes the repeat's tag). A tagged record that is new
     to its feed and has local rows takes them, with their seq (a move: the
     local copies join the feed's instances and the local rows go). An
     untagged record is a copy in every feed holding its CID, or, held by
     none, goes to local;
  3. *writer:* migrate mode: format 1's rowid is the record's seq in every
     feed holding it. The record goes to the feeds of its tag instances and
     to the feeds already holding its seq (a copy sent without instances
     joins the record's instances there); with none, to local. Local rows of
     the seq move into the feeds once the record has an instance. A seq held
     by another CID is `P4_REJ_SEQ`. A copy keeps its own bytes (C-35) and the
     ts it is sent;
  4. *writer:* CAT supersede-on-ingest (ingest mode only, C-20): the scope
     feed's records of the same object identity are retired from that feed;
  5. *writer:* seqs for records new to a feed, in input order (format 1's
     rowids, C-39 E1); they stay above visible-through until committed;
  6. *writer:* each new row's rid (after its record's live rows) and frame: a
     frame the record already has in that feed when the bytes are the same,
     else a new frame. New feeds and tokens go into the type index
     (`synchronous=FULL`), and a new feed's index file is made (created
     durably, its schema committed with `mark` 0) before its first frame. The
     frames are appended to the feed's stream at the writer's end offset (not
     synced). The writer then plans the next group;
  7. *indexer:* a **round** takes every unit queued for it (the writer keeps
     up to 16 planned units in flight);
  8. *indexer:* each feed file the round touches, feeds before local, in ONE
     transaction for the whole round: the units' rows in plan order, its
     counters, instances, tokens, identities and moves; **its stream synced
     first**, then the transaction commits with the stream's new `mark`. The
     new rows are **staged** (`m=0`: in `r_s`, `r_b`, `r_a`/`r_t` and `r_m`,
     not in `r_c`, `r_ke`, `r_w`) and new identities go to `idst`, so the
     transaction writes appended pages only (a migration's append into an
     index without its secondary indexes is not staged). After the commit
     the feed's **view** of its staged rows is replaced (below);
  9. *indexer:* publish (visible-through) and ack every call of the round, in
     plan order: the ack follows the commits (C-4).
- **The merge** (the indexer, between rounds, and every 250 ms while no
  round waits): one feed per transaction, its staged rows (at most twice
  `flushEntries`, CID order: `r_c`'s pages filled one after the other) set
  `m=1`, which puts them in `r_c`, `r_ke` and `r_w`, and `idst` moved into
  `ident`; then the feed's view without them. The transaction keeps its
  dirty pages in memory up to a quarter of the hard heap (at most 256 MiB),
  so each page is written once. A merge costs about every page of the feed's
  three random-keyed indexes whatever it carries, so it waits for many rows:
  - a feed is **due** when it holds `flushEntries` staged rows (config tag
    42, "index flush entries", 131,072 by default), or when it has had no
    new rows for 30 s (a quiet feed ends fully indexed; a delivery that
    follows within the window shares the merge);
  - the due feeds are merged one after the other until calls wait for their
    ack;
  - the views of every feed are **capped** (§7): past half the cap the feed
    holding the most staged rows is merged even when not due; past the cap
    the merges go on before any more acks (the acks wait).

  A failed merge changes nothing (tried again 2 s later). REBUILD 1 merges
  everything. Stats 14 and 15 ("index flushes", "index flush entries") count
  the merges and the rows merged.
- **The view** (`Staged`, per feed): what the reads that use `r_c`, `r_ke`
  or `r_w` need of each staged row (rid, CID, epoch, object key, w, delivery
  time, ts), in a few runs each sorted four ways (rid; CID; object, epoch;
  w), and the staged identities. It is immutable: each commit and each merge
  publishes a new one (a commit adds a run, runs merge as they grow; a run
  that loses rows, to a merge or a delete, is copied without them, so a view
  never keeps merged rows' memory). A read step pins the one current when it
  starts (§6).

  A group is planned on the state its type's pending units leave its records
  in (their seeds); planning reads go through the reader pool and see
  committed rows only; planning writes SQLite only to register a new feed or
  token and to make a new feed's index file. A failed round fails
  its calls, and every unit after it on that writer (poison) until the writer
  has cut the streams they appended to back to their marks; those calls are
  answered with the error, unstored.
- **SUPERSEDE** (`provider`, `source`, keep): one feed; its rows whose batch
  is not the kept one go, a chunk of 32,768 rows per transaction; a record
  left with no row in the feed leaves it.
- **DELETE**: every row of the CIDs in every feed (each index's `r_c`).
  `deleted` counts each CID's distinct copies over the feeds.
- SUPERSEDE, DELETE, QUOTA_GC, REBUILD and compactions wait for the indexer
  to be idle (no round, no merge; it starts none until they are done), then
  run on the writer thread. They remove index rows (a staged row leaves the
  view with its commit); frames stay in the stream until a compaction.
- **Lane times** (an instance's `first` and `updated`, SUMMARY 3): a write
  that delivers the instance by a tag stamps `updated` with the clock (a
  migration: format 1's times, C-36). A copy joining instances the record
  already has (an untagged write) leaves them alone (C-39 E4). The instances a
  DELETE or a CAT supersede-on-ingest leaves are restamped with the clock, as
  format 1's `decrementSourceSummary` (C-39 E5).
- **Bounds after deletes:** a delete recounts a file's seq and w bounds from the
  ends of `r_s` / `r_w` and the staged rows; a delete at the file's epoch
  edge also its min/max epoch (the first row with an epoch from each end of
  `r_w`, and the staged rows), so the type's epoch range follows deletes. The
  ts bounds stay bounds.
- **QUOTA_GC**: the type's oldest record row sets by arrival (the feeds
  merged by seq), every row of each.

## 4. Crash safety

The stream is the record journal. Every commit syncs the frames it appended
before the index transaction that names them and moves the stream's `mark`,
so an index never claims bytes its stream cannot back (format 1's invariant,
`docs/STORAGE-DURABILITY.md` §3.2, §6.4); the ack follows that commit. Each
index transaction carries its rows with its own counters, instances, token
counts, identities and moves: a feed is consistent on its own. Every open,
before it serves (M8), visits every feed the type index names:

1. its index's counters, instances and tokens are read (the type's totals
   and the next seq follow from them: seqs never go back, a file keeps its
   max seq after deletes);
2. its stream is cut back to the index's `mark` (bytes past it were never
   acknowledged); a stream shorter than its mark lost acknowledged frames and
   the feed is quarantined (`P4_E_CORRUPT`, named), never silently cut;
3. a compaction cut by a crash leaves the next generation (unlinked) or the
   replaced one not yet unlinked (unlinked);
4. an index that is missing (or damaged: `SQLITE_CORRUPT`/`NOTADB`) is rebuilt
   from its stream after the other feeds are open. A feed's index file exists
   before its first frame (step 6 of a PUT), so a stream without one lost its
   index; a feed whose first commit a crash cut has its index with `mark` 0,
   and step 2 cuts its frames (never acknowledged) instead of indexing them
   (GATES-stream-r1 B1). The rebuild: the damaged file goes, and
   every whole frame is indexed again with its keys taken from the bytes (the
   CID is the sha256 of the frame's FlatBuffer; epoch and object key are
   extracted). What only the index held is gone: each record gets a fresh
   seq, the open's clock as ts and delivery time, an empty publishing node and
   (a source feed) one instance with an empty batch. A frame whose CID the
   feed already indexed, a sealed frame (its CID is the plaintext's), or a
   frame that does not parse stays unindexed. The stream is cut after its
   last whole frame;
5. a move cut between its feed and local is finished: the feed's `moved`
   table names the seq (committed with the feed's rows), and the local rows of
   that seq go once the feed holds it. A feed's next commit drops `moved`
   rows whose local side is done;
6. its view of staged rows is rebuilt from `r_m` and `idst`. A merge is one
   transaction that only sets `m` (and moves `idst` into `ident`): cut by a
   crash, its rows are still staged; committed, they are merged. Either way
   every acknowledged row is there once, with its metadata (`t_kill_merge`).

The one write that spans two files with one record is that move, committed
feed first. A write cut between two feeds leaves the feeds it committed: a
retry of an unacknowledged call finds its records there (DUP) and adds them
to the rest. A feed index a crash cut between its creation and its schema's
commit exists but holds nothing: the first writer open makes the schema then.

Every connection to a format-4 index is opened with `share=1` through
FlatSQL's VFS (`openConn`): it attaches to the path's node, so the engine's
connections see each other's locks and one WAL index. The open is a `file:`
URI whose path is the file's path with `%`, `?` and `#` escaped (`%25`,
`%3F`, `%23`), and an empty authority for an absolute path: SQLite decodes
`%HH` in a URI path and ends it at `?` or `#`, so without the escapes a
feed's escaped name (`a%20b`, `x%2F..%2Fy`) opened the decoded path (a
different file, a directory, or a path outside `P/<TYPE>`) while the
engine's own I/O created, sized and measured the escaped one. A connection opened
beside a running engine without `share=1` gets a private node (every lock
granted, a WAL index of its own); at close it takes itself for the file's
last connection and, when the WAL is empty, deletes it under the engine's
connections. The engine and the SQL surface open no other connection (the
SQL surface's own database is `:memory:` with ATTACH refused). Streams are
opened through the same seven host calls (`flatsql_io_*`), one
offset-addressed handle per generation shared by the writer, the indexer and
the readers.

## 5. Maintenance

- **The maintenance thread:** checkpoints for the indexes, the type index and
  full text; closing evicted writer connections; the T/ files' sizes for
  SUMMARY 4.
- **Checkpoints never wait for a writer.** PASSIVE passes copy a WAL once
  it passes 128 MiB (a checkpoint writes each page once however many frames
  the WAL holds for it); a TRUNCATE takes the writer lock only if it is free
  that instant. An index's one writer truncates its own WAL after a commit
  that leaves it at a quarter of `walTotal` (4 GiB by default; or 32 MiB
  while the instance's WALs pass three quarters of it). WAL sizes are
  accounted in bytes. Writer connections spill a transaction's dirty pages
  only past 32 MiB (a merge: past its budget). Activation retries a TRUNCATE
  checkpoint the maintenance thread is running.
- **The long-work thread:** REBUILD and QUOTA_GC calls, the configured
  quota once a second, full text, and compactions; their per-type work runs
  on the type's writer thread.
- **Compaction:** a stream more than half dead (its end less the live frames'
  bytes `fbytes`), with at least 1 MiB dead, is rewritten: its live frames, in
  stream order, into the next generation (`<feed>.<gen>.fsdata`, created
  empty, synced); then one index transaction repoints every row's `off` and
  sets the new `gen` and `mark`. Readers resolve a row's frame in the
  generation their read transaction sees (`meta` gen): the replaced
  generation stays open for 30 s for readers on an older snapshot, then goes
  (unlinked when the last reader holding it lets go; a reader whose
  generation is gone starts its read transaction over). The registry's `gen`
  follows. There is no rename (`flatsql_io` has none).
- **Quota** measures every index by its pages in use (pages less free pages),
  the type files alike, and each stream by its live frames; it deletes the
  oldest record row sets by arrival until the store fits (the frames go at
  the next compaction).
- **Disk usage:** SUMMARY 4's `db` is each feed's index pages plus its stream
  (its committed end, and a replaced generation while it is kept).
- **Full text** follows the records: catching up adds the seqs past
  `through` (the feeds merged by seq; a seq once, from the first feed holding
  it; the bytes from its frame), and the rows of seqs that left a feed since
  the last pass (supersede, delete, quota, CAT supersede-on-ingest) are
  deleted first; a migrated seq (below the seq floor) only once no feed holds
  it, and a seq that moved to another file keeps its row. The gone list is in
  memory: a crash leaves those rows (a search drops them, as it drops any hit
  without a live record), and REBUILD 4 removes them.
- **REBUILD:** 1 adds the indexes' secondary indexes (after a migration's
  bulk append) and merges every staged row; 2 recounts every index's
  counters, instances and tokens from its rows; 4 rebuilds full text; 8 makes
  the same comparisons (file and engine) and changes nothing, checks every
  row's frame in its stream (below the mark, its size prefix equal to the
  row's `len`) and the stream's end against the mark, compares each feed's
  view with its file's staged rows and identities, and runs `PRAGMA
  integrity_check` on every index and the type index (C-27). A record whose
  rows are not its copies x its instances, or that holds two CIDs, is a
  mismatch.

## 6. Reads

A scan first picks its feed files: the lane filter's (provider and source
name a feed file; a batch, content key or producer peer narrows to the feeds
holding such an instance), else every feed of the type. Request tag 20 (ops
12-16; C-43 B2) narrows the files: 2 the type's local file only (a lane
filter then answers empty), 3 the local file plus the feed files the lane
filter selects: with lane source `local`, format 1's "local" partition (its
untagged records and any feed whose source is `local`). SCAN orders 5-7
refuse it (they pick their files themselves). Each file is walked
in the scan's order through its own indexes (`r_s` arrival, `r_w` epoch,
`r_c` CID), a chunk at a time, and the walks are merged (C-38 (4); ties by
feed id). A page of candidates is resolved by reading each record's rows
(one rid range of its file, one read transaction per file). A record answers
once per feed file (C-10 within the file, C-38 (5) across files). Its copy
is the lowest token's matching row (C-12); its tag the earliest instance of
its feed that matches the lane filter (§3.6), or in a newest-first page (SCAN
5/6) the newest, the delivery the page orders it by (C-41 N9), with provider
and source from the file and the rest from its ids: never blank when the
record has a tag.

- **Staged rows:** `r_c`, `r_ke` and `r_w` hold merged rows only. Every
  walk or probe through them also takes the file's view's rows in the same
  order and range (a CID or object probe, a CID or w walk chunk, an object's
  epochs). Each step (a walk's chunk, the predicates' probes, an EPOCH pass
  over a file, GET's probe of a file) pins the view before its SQL
  transaction starts and lets it go when it ends, as each step already has a
  read transaction of its own: a view outlives no step, and no step waits on
  the host. A row merged after the pin comes from both with the same key and
  seq and collapses (a record's entries are adjacent in every order); one
  merged before it is in the step's SQL snapshot, which starts after the
  pin; a staged one in the view only. Walks through `r_s`, `r_a`, `r_t` and
  the rows themselves (one rid range of table `r`) see staged rows directly.
  Reads never wait on a merge (WAL snapshots, an immutable view). The
  planner's lookups (dedupe by CID, CAT supersede by object, identities)
  take the view the same way. Staged rows cost reads nothing measurable:
  500k staged rows over 300 feeds, every read shape within noise of the
  same store all merged.
- **A18 (C-31):** `<TYPE>@<source>` is the newest N records of that type from
  that source (the source's feed files' `r_s`, newest first, merged); `<TYPE>`
  the type's newest N (every feed file's `r_s`, merged). Every other filter
  applies above the cut.
- **Candidates instead of a walk:** an exact CID (each file's `r_c`), an
  equality or IN on the object rule's first column (each file's `r_ke`).
- **Keyed walks** (w, delivery time, ts) meet a record once per value its rows
  carry; it answers at the value of the row it answers with (its copy's w or
  ts; the delivery time of the instance it projects, its newest matching
  one), so once.
- **SCAN orders 5-7** (C-39 E2, E3; mailbox only) are format 1's two-part
  pages: the tagged records (the feed files) in the order with the offset,
  then, without a lane filter, the untagged (local) records from the start,
  as many as the limit leaves. 5 NEWEST: delivery time desc, CID asc; local
  ts desc, CID asc (the raw default page). 6 RECENT: delivery time desc, seq
  desc; local seq desc (QueryRecentRecords). In 5 and 6 a tagged record
  projects the instance it is ordered by: its newest matching instance's
  batch, url, content key, producer peer and key, and delivery time (C-41 N9;
  ties at that time by the smallest identity). 7 W_ASC: w asc, CID asc (a raw
  page with a sync filter or a search).
- **EPOCH nearest / as_of / forward:** the file's objects are its `okey`
  keys in key order (merged with the staged rows' keys); one seek per object
  in `r_ke` (its `kk`) beside the object's staged rows, read from the target
  in rank order until an epoch group has a record that passes the filters;
  ties at the best epoch go to the lowest CID (format 1's ranking), then the
  lowest feed id. One answer per entity. A record without an object is its
  own entity.
- **By CID:** GET answers from the first feed file holding the CID (its
  copies, the lowest token first); TAGS lists the tag instances of every
  feed file holding it, each with that file's seq.
- **Counters:** HEAD with no filter, SUMMARY 1 (records, copies, bytes),
  SUMMARY 2 (one row per producer token: its copies and bytes), SUMMARY 3
  (one row per feed file and instance: provider, source, batch, content key,
  producer peer and key, records, bytes, first, updated) and SUMMARY 4 are
  answered from counters without reading rows.
- **Full text** is checked a page at a time against FTS5.
- **Record bytes** (hydrated rows, COL predicates on extracted fields) come
  from the stream: a page's read transaction reads its index's `meta` gen
  first, then each row's frame `(off, len)` from that generation (its size
  prefix must equal `len`, else `P4_E_CORRUPT`).

**Caps** end a read with its status: rows examined, bytes read, result rows
and bytes, and cancel. RB1 streams always end with RB1E.

**SQL surface.** `src/p4sql` answers ops 30 and 31 through `p4_reader.h`, and
the engine writes their RB1E (C-28). `<TYPE>@<source>` resolves as spelled
(C-39 S1, C-43 B2 and B3):
1. `local` is format 1's "local" partition: the type's local file plus any
   feed whose source is `local` (`P4ScanSpec.part = 3`, lane source `local`;
   format 1 files a record tagged with source `local` in the partition of its
   untagged records);
2. else a feed source of the type spelled exactly so;
3. else a case variant of `local`: as 1;
4. else the type's one feed source equal but for case;
5. else (no such source, or case twins and neither spelled exactly) empty
   when a type has a feed of that source (spelled exactly when the type has
   case twins of it, equal but for case otherwise), else "no such table".

A name never reads another feed. SQLite's catalog folds case, so one virtual
table serves every spelling of a name: what it reads follows the running
statement's spelling (the quoted names in its text), resolved once per
statement, so a relation made earlier also sees feeds added since. A statement
that names two spellings of one relation which read different sources is
refused with an SQL error.

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
- Nothing is held back beyond the pipeline: at most 16 planned units per
  writer wait for their round; a unit's seed is its type's pending units'
  touched records only.
- **The views of staged rows** are C++ heap beside SQLite's, in the 2 GiB
  wasm memory. A staged row is 128 bytes on wasm32 (the row 112, its four
  sorted pointers 16; native 120 + 32) plus its object key's text past the
  string's inline buffer (10 bytes on wasm32), and an identity 40 bytes. The
  cap over every feed's view is 8 x `flushEntries` rows (1,048,576 by
  default: 128 MiB at 128 bytes a row) or a quarter of the hard heap (160 MiB
  by default), whichever comes first; the merges keep the views under it
  (§3), and a round adds at most 16 units per writer before the next check
  (65,536 rows at 4,096-record calls: 8 MiB). So at most about 235 MB with
  8 writers (the most the defaults pick: cores - 1, at most 8); SQLite's 640 MiB, the mailbox (~140 MiB at the
  default slots) and the views stay well inside 2 GiB. Briefly beside them:
  a superseded view a running read step still holds (one step, never across
  a wait on the host) and a publish's new orders. Measured native: 152 bytes
  a row on the 5M bench (76 MB at 500k staged rows), 178 on W01 (35-character
  MPE keys, IQC identities).
- A merge transaction holds up to a quarter of the hard heap (at most 256
  MiB) of dirty pages (SQLite heap, inside the hard heap).
- Streams: one open handle per feed (and a replaced generation while kept).

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
cpp/build/flatsql_p4_test --test=t_kill_merge --slow=1 --rounds=30  # kill -9 inside merges
cpp/build/flatsql_p4_fault_test --test=t_power_loss --slow=1 --rounds=30
```

- **Both loops walk every stream** as a stock reader does (no FlatSQL
  code): back-to-back `[u32 LE size][FlatBuffer]` frames from byte 0, each a
  `$PNM` buffer with its root inside it, ending exactly at the end of the file;
  `t_kill` also checks that every index row names one of those frames and
  that GET's bytes hash to each record's CID.
- **`t_kill`** (`t_kill.cpp`): a forked engine ingests from three producers
  into three feeds (`prov@src`, `prov@src2` and `local`; repeats of earlier
  records land in another feed, so records sit in two or three feed files,
  a row set in each; the same new records from three producers at once,
  each to its own feed; a batch supersede every 13th call; a 4 MiB quota
  every 11th; every 4th step four calls at once, each to a feed new to the
  store, `prov@fresh-<id>`, so kills land between new feeds' first frames
  and their first commits: GATES-stream-r1 B1), is killed with SIGKILL at a
  random point, and the store is checked: a source feed's row never comes
  back without its batch; every index passes `integrity_check`; a CID has one seq in each
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
  with its own seq. Its index flush threshold (2,000) and `t_kill`'s (5,000)
  make the merges frequent (and the views' cap, 8 x the threshold, holds
  acks back often), so both loops crash inside merges too.
- **`t_kill_merge`:** the kill -9 loop timed into merges: a child ingests
  with a 2,000-row flush threshold and logs each merge's start and end
  (`P4_MERGE_DEBUG`, native test builds) and each acknowledged call; the
  parent kills it 0-4 ms after the k-th merge start, then runs `t_kill`'s
  checks and finds every acknowledged record with its feed's batch.
- The proof is end to end (C-33): the SDN harness on the real engine
  (`sdn-server/internal/storage/format4proof`): `store-migrate --to 4`, every
  benchset read and the coverage classes against format 1 field by field,
  W01-W10, kill -9 and LazyFS power loss, and the growth steps.

## 10. Measured

The build-out reports hold the runs: `ENGINE-MERGE-REPORT.md` (the staged
acks and the merges: equivalence with the engine before them, the crash
loops, bytes per record, reads), `ENGINE-STREAM-REPORT.md` (the streams) and
`ENGINE-REPORT.md` (the feed tables, C-38). On the shared 28-core build box
(load 7-11), native:

- **Bytes written per record** (every write through `flatsql_io`: stream,
  WAL, index and type files, through close, the merges still owed included),
  against the engine whose acks wrote the random-keyed indexes:

  | | acks write every index | staged acks + merges |
  |---|---|---|
  | write bench, 5M records (8 producers, 4,096-record calls, ~300 feeds) | 4,615 | 1,200 (stream 480, WAL 477, index 243) |
  | write bench, 20M records | 8,248 | 1,840 (480, 856, 503) |
  | W01 into the host-02-sized fixture's big feeds, a delivery merged alone (two passes) | 26,633, 16,280 | 5,663, 4,901 |
  | W01, two deliveries ~7 s apart sharing one merge (both passes) | 21,456 | 3,256 |

- **Ingest** 74.1k records/s at 5M (62.5k before), 70.8k at 20M (43.0k);
  acknowledgement p50 / p99 289 / 943 ms at 5M (542 / 777), 300 / 1,106 ms
  at 20M (762 / 1,414).
- **Reads** back to back on the same stores (GET by CID, EPOCH nearest /
  as_of / forward over every object, a 1-day EPOCH window, newest 100 of a
  source): equal or faster, but for the 1-day window, +3.5% (+0.06 ms) at
  both sizes, within the box's noise. With 500k rows staged over 300 feeds
  every shape is within noise of the same store all merged.
- **Equivalence:** a driver covering five types (CAT supersede, IQC
  identities, OMM local-to-feed moves, copies, 35-character keys), every PUT
  response, SUPERSEDE, DELETE and every read shape before and after REBUILD
  1, answers byte for byte as the engine before staged acks, with no merge
  during the run, ~80 merges during it, and merges plus two REBUILD 1s.
- **Crash:** `t_kill` 100/100, `t_power_loss` 100/100, `t_kill_merge` 50/50
  (every kill inside a merge transaction); the fixture's migration matches
  format 1 (sampled, 0 differences).

Readability script (no FlatSQL code): `fsdata-reader-check.py` walks every
`P/<TYPE>/*.fsdata` of a store root, parses every frame with its SDS root
type and checks every index row's CID against its frame's sha256. The stock
C++ Verifier passes on every frame verified as its own buffer; verified in
place against the whole file, the frames that start 4 bytes off an 8-byte
boundary fail only its alignment check (frames are back to back, as in the
original design).
