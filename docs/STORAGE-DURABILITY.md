# FlatSQL durable storage — root cause and target design

Status: **specification + root-cause record**. No engine code has landed for this
yet; this document is the contract the implementation and the host integration
(Hermes, Phase 2) build against.

Owner-directed, 2026-08-06. Filed by Janus (module-SDK / ABI oracle) under graph
task `flatsql-ltx-state-persistence`.

---

## 1. Owner rulings (verbatim)

> "The WASM SQLite storage was always supposed to save the flatbuffers and btree
> / SQLite table data to disk and read it from there using flatsql, through the
> WasmEdge or shim FS access. I have no idea how that got messed up."

> "We should be using flatsql only."

> "We should be streaming space data standards flatbuffers."

Together these define the storage architecture:

1. **FlatSQL only.** One engine, end to end. No second SQL engine anywhere in
   the path.
2. **FlatSQL's own structures on disk.** Its btree/table structures are durable,
   in FlatSQL's own format, written and read by FlatSQL.
3. **SDS FlatBuffers streamed and indexed in place.** The payload on disk is the
   append-only stream of size-prefixed SDS FlatBuffer records, byte-identical to
   their wire form. FlatSQL's durable structures are **indexes over** those
   streams — keys and stream offsets, never copies of payload bytes. Query
   execution reads fields directly out of the streamed records (zero-copy).

Boot is therefore: open streams + open index files + verify + short tail
re-index of records appended after the last flushed index state. Full re-index
from the streams stays the fallback whenever the index files are missing,
stale-beyond-tail, or fail verification. The record journal / stream remains the
source of truth; index files are an accelerator and may always be discarded.

---

## 2. Root cause — how it got messed up

The owner asked explicitly. Answer, with evidence.

### 2.1 It was never a deliberate "go in-memory" decision

The in-memory engine is the *residue of an ABI omission*, not a design choice.

**The C++ core has always been able to open a real database file.** It still
can, today, untouched:

- `cpp/include/flatsql/sqlite_engine.h:16` — `std::string path = ":memory:";`
  (a *default*, not a constant)
- `cpp/src/sqlite_engine.cpp:162` — `sqlite3_open_v2(options_.path.c_str(), ...)`
- `cpp/src/sqlite_engine.cpp:175` — `if (options_.enableWal && options_.path != ":memory:")`

**But the WASM C ABI never exposed the path.** The only constructor reachable
from any wasm host is:

```c
void* flatsql_create_db(const char* schema, const char* dbName);
```

`cpp/src/flatsql_capi.cpp:777` builds `new FlatSQLDatabase(parsedSchema)` with a
default-constructed `RuntimeOptions`, i.e. `path == ":memory:"`, always. There is
no path parameter and no second entry point. **Every wasm consumer is
structurally forced in-memory** — the browser, the WASI reactor, and the node
host alike. The ABI was shaped for the browser demo first, where a path was
meaningless, and the server lane later inherited that ABI wholesale.

### 2.2 The wasm builds then stripped the filesystem out entirely

- `cpp/CMakeLists.txt:215` — browser target links `-s FILESYSTEM=0`: no
  filesystem at all.
- `cpp/CMakeLists.txt:141, 239, ~347` — **all three** wasm targets define
  `SQLITE_OMIT_WAL=1`. The native build does not. WAL is compiled out of exactly
  the artifacts that would need durability.

The result is measurable on the shipped artifact. `wasm/flatsql-wasi.wasm`
imports **six** WASI functions, total:

```
clock_time_get, fd_write, fd_read, environ_sizes_get, environ_get, random_get
```

No `path_open`. No `fd_seek`. No `fd_pread` / `fd_pwrite`. No `fd_prestat_get`.
**There is no file I/O in the binary.** Nothing was disabled at runtime or
misconfigured by a host — the capability is absent from the module. Any on-disk
design must therefore build the FS access layer first; this is the foundational
missing piece, and it is missing under every candidate design.

The host side is *not* the blocker: `kubo/sdn/wasmrt/runtime.go:206`
(`WithWASIArgs(args, envs, preopens)`) already plumbs preopens; `flatsqlrt.go:175`
simply passes `WithWASI()` with none, because the module could not use them.

### 2.3 SDN then engineered around the gap — that is where the 5-minute boot came from

space-data-network commit **`7d4a6939`** (2026-07-03, TJKoury), *"storage: cut
FlatSQLStore over to the FlatSQL-WASM engine (loop B.2)"*, states it plainly:

> `sql.Open("sqlite3", sdn.db)` is gone: the store now runs its entire SQL
> surface through internal/flatsqldrv over an in-process AOT-compiled FlatSQL
> engine. Control-table durability = the statement journal
> (`<basePath>/control.sdnj`), replayed at boot; record payloads stay in the
> append-only stream files, unchanged. dbPath is preserved as a string.

That commit is the origin of replay-from-zero. It also retired the cross-process
file-locking tests ("no shared db file — the WAL thrash class is structurally
gone"). The migration to FlatSQL-WASM was correct and owner-directed; what was
lost in it was the *file*, because the engine's wasm ABI could not accept one.

The consequence is documented downstream, correctly, as a hard blocker:
`sdn-server/internal/storage/flatsql.go:266` — *"dbPath no longer names a real
SQLite file (the engine is in-memory)"* — and
`sdn-server/internal/storage/record_catalog_replay.go:36-47`, which rules a
cross-boot replay cursor UNSOUND *precisely because* the control tables it would
resume into are always empty.

**No evidence of a real technical blocker was found** — no WASI FS performance
finding, no wasi-threads safety issue, no WasmEdge limitation. The cause is an
ABI omission that hardened into an architecture.

### 2.4 A second engine did survive the migration

`sdn-server/internal/flatsqldrv/standalone.go:33` — `sql.Open("sqlite", dbPath)`.
The admin/auth store (`auth.db`) is a **Go-native SQLite**, not FlatSQL. It is a
real file on disk. This is the one place a real database file survived, and it
is a second engine — directly contrary to "We should be using flatsql only" and
to the WASM-not-Go host boundary law. Filed as a defect for Hermes; it must move
onto FlatSQL once FlatSQL is disk-backed.

### 2.5 The original design is still visible in the repo as dead code

`cpp/include/flatsql/btree.h` + `cpp/src/btree.cpp` define a FlatSQL-native
BTree whose entries are `(key, dataOffset, dataLength, sequence)` — indexes as
offsets into the FlatBuffer stream, exactly the owner's stated architecture.

**`btree.cpp` appears in no CMake target.** It is not compiled into anything. It
was written, never wired, and `SqliteIndex` took over — which stores the same
tuple shape as rows in a vendored-SQLite table (`cpp/src/sqlite_index.cpp:134`,
`CREATE TABLE ... (key, data_offset, data_length, sequence) WITHOUT ROWID`).

That substitution is the drift, in one line: the native, stream-addressing,
paged-capable index became a SQLite table living in an ephemeral `:memory:`
database. Restoring the owner's design means finishing what `btree.h` started.

---

## 3. Target design

### 3.1 On-disk world

| Artifact | Contents | Owner | Status |
|---|---|---|---|
| `<base>/flatsql-stream/*.sdns` | Append-only size-prefixed SDS FlatBuffer records, byte-identical to wire form | host writes, FlatSQL reads | **exists** |
| `<base>/flatsql-index/*.fsix` | FlatSQL-native paged index/btree files: keys + `(streamId, offset, length, sequence)` | FlatSQL | **to build** |

Payload bytes live in exactly one place: the stream. Index files never copy
them. `.fsix` size is dominated by index structure, so the page format is sized
for key/offset tuples, not records.

### 3.2 Invariants

1. **Stream offsets are stable across restarts.** A `(streamId, offset)` pair
   resolves to the same record forever. Streams are append-only; compaction
   writes a new stream generation and reindexes, never rewrites offsets in place.
2. **Index files are derived state.** Any verification failure = discard and
   full re-index from the streams. Worst case is exactly today's behaviour;
   corruption is never a possible outcome.
3. **Tail invariant.** Each `.fsix` records the `(streamId, flushedOffset)`
   high-water mark it is consistent with. Boot re-indexes only
   `[flushedOffset, streamEnd)`. If a stream is shorter than its recorded mark,
   the pair is torn → fall back to full re-index.
4. **Fail closed.** Export/query-selected surfaces keep refusing
   (`ErrRecordCatalogHydrating`, sdn `2a2ffea5`) until the index is open and
   verified.
5. **No runtime detection in engine code.** Filesystem differences are absorbed
   entirely in the host FS shim (§3.4). Identical ABI, identical results, both
   runtimes.

### 3.3 ABI surface (to build — Phase 2 targets this)

All of it goes through the existing generic export-buffer pattern: pointer +
separate `_size()` accessor, `int` status returns, `flatsql_get_error()` for the
message. **No new host capability class.** Errors are values, never traps — the
`-fignore-exceptions` WASI artifact turns any throw into a guest-poisoning
`unreachable` (commit `b26ed45`), so every entry point below is `noexcept` by
construction.

```c
/* Open disk-backed. streamDir/indexDir must be inside a host-preopened dir.
   Returns NULL + flatsql_get_error() on failure. NULL/"" dirs == today's
   ephemeral engine, byte-for-byte. */
void* flatsql_open_db(const char* schema, const char* dbName,
                      const char* streamDir, const char* indexDir);

int  flatsql_is_disk_backed(void* h);

/* Boot. Opens + verifies index files, re-indexes the tail, returns the record
   count now visible. Negative = error code below; the host then calls
   flatsql_reindex_all(). */
int  flatsql_open_state(void* h);

/* Explicit full re-derivation from the streams. Always available. */
int  flatsql_reindex_all(void* h);

/* Flush dirty index pages and advance the recorded high-water mark. The host
   decides when; the engine just does it. */
int  flatsql_flush_index(void* h);

/* High-water mark the host may resume its own journal from. */
uint64_t flatsql_flushed_offset(void* h, const char* streamId);
```

Error codes (stable, host maps to typed errors):

| Code | Meaning | Host action |
|---|---|---|
| `0` | OK | proceed |
| `-1` | index files absent | `flatsql_reindex_all` |
| `-2` | format version mismatch | `flatsql_reindex_all` |
| `-3` | checksum/verification failure | `flatsql_reindex_all` |
| `-4` | torn pair (stream shorter than recorded mark) | `flatsql_reindex_all` |
| `-5` | FS unreachable / not preopened | fail closed, surface to operator |

Every negative code is recoverable by full re-index. There is no code that means
"data lost".

### 3.4 Isomorphism

One artifact, two host shims — differences absorbed in the shim, never in module
code:

- **WasmEdge / native host:** WASI preopens. `wasmrt.WithWASIArgs(args, envs,
  preopens)` already exists (`kubo/sdn/wasmrt/runtime.go:206`); `flatsqlrt` must
  pass the store directory instead of `WithWASI()` with none.
- **Browser:** the existing `wasm/standalone.js` shim (stdio-only today,
  `wasm/standalone.js:136`) grows a real FS backed by OPFS —
  `path_open`, `fd_pread`, `fd_pwrite`, `fd_seek`, `fd_close`,
  `fd_prestat_get`/`fd_prestat_dir_name`.

Acceptance is parity evidence, not "works in X": identical inputs must produce
identical query results and an identical flushed high-water mark in both lanes.

### 3.5 Build changes required

- Drop `SQLITE_OMIT_WAL=1` from the wasm targets *if and only if* the vendored
  engine remains in the read path (see §4); it is irrelevant to `.fsix`.
- Browser target cannot keep `-s FILESYSTEM=0`.
- The WASI target must actually link a WASI-backed FS. Today it does not — §2.2.
  **Verify by asserting `path_open` is in the module's import list**; that check
  belongs in the toolchain guardrail tests, because its absence is exactly the
  silent failure that produced this whole class.

---

## 4. Open question for the owner

"Use FlatSQL only" is unambiguous about *not adding* a second engine. What it
implies for the **vendored** engine needs an explicit ruling, because FlatSQL is
today built *on* SQLite 3.45.1 (`cpp/vendor/sqlite/`): the SQL parser, planner,
virtual-table layer, `SqliteIndex` btrees, R-Tree and sqlean extensions are all
that library. It is internal to FlatSQL, not a second engine in the host — but
it is also not FlatSQL-native code.

Two readings, sharply different cost:

- **(A) Vendored SQLite stays the query engine; `.fsix` replaces `SqliteIndex`
  as the durable index layer.** Indexes become FlatSQL-native, paged, on-disk,
  stream-addressing — the owner's stated design — while SQL parsing/planning
  continues to come from the vendored engine. Contained, incremental.
- **(B) FlatSQL replaces the vendored engine outright**, including SQL parsing
  and planning. This is a ground-up database, and reimplementing a pager +
  crash-safe journal alongside a mature one already in the same binary is the
  highest-risk possible way to obtain durability.

This document specifies (A), because (A) satisfies all three rulings as written:
FlatSQL is the only engine in the path, its btree/table structures become
FlatSQL's own on disk, and SDS FlatBuffers are streamed and indexed in place.
**(B) is a STOP-for-owner decision** — it is a rewrite of the storage engine, not
a persistence feature — and no code should be written toward it without an
explicit ruling.

---

## 5. Descoped: LTX-style snapshot/delta streaming

The original brief for this task was an LTX (Litestream/LiteFS transaction file)
snapshot + page-delta chain. **Descoped by the owner, 2026-08-06**: durability
comes from FlatSQL's own on-disk structures, not from a replication log layered
over an in-memory engine. No new format.

LTX remains a candidate for a *future replication / IPFS-seeding* layer, where
its properties are genuinely wanted (chained pre/post checksums, page-level
deltas, last-writer-wins compaction). Format research done for this task, if
that layer is ever picked up — verified against `github.com/superfly/ltx` source
at `main`, not the README, which is ~3.5 years stale and describes a field layout
the code no longer matches:

- Magic `"LTX1"`, header **100 bytes, big-endian**, no on-disk version byte
  (validated from the magic).
- Header: `Flags@4`, `PageSize@8`, `Commit@12`, `MinTXID@16`, `MaxTXID@24`,
  `Timestamp@32` (epoch ms), `PreApplyChecksum@40`, `WALOffset@48`, `WALSize@56`,
  `WALSalt1@64`, `WALSalt2@68`, `NodeID@72`; bytes 80-99 reserved.
- Page block: `PageHeader{pgno u32, flags u16}` (6 bytes), then u32 size, then
  **LZ4 block**-compressed data; terminated by a zero page header; then a varint
  `(pgno, offset, size)` page index, a `varint(0)` terminator, and a u64 index
  length. Incremental files must be strictly increasing in pgno; snapshots must
  be contiguous from page 1. The SQLite lock page is never encoded.
- Trailer 16 bytes: `PostApplyChecksum`, `FileChecksum`.
- Checksum: **CRC-64/ISO** (not ECMA). Per page:
  `ChecksumFlag | CRC64(BE32(pgno) || raw uncompressed page)`. Whole-DB rolling
  checksum is a commutative **XOR-fold** of per-page checksums, re-OR'd with
  `ChecksumFlag = 1<<63` (a "never zero" marker; zero means unset).
- Chain contract: a file's `(MinTXID-1, PreApplyChecksum)` must equal the
  previous file's `(MaxTXID, PostApplyChecksum)`.
- A snapshot is exactly `MinTXID == 1`.
