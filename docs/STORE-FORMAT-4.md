# FlatSQL store format 4: one SQLite file per partition and month

Store format 4 keeps each partition's records in SQLite files, one file per
partition and UTC content month, with SQLite 3.53.4 unmodified (VFS, WAL and
configuration only). The engine is `cpp/src/p4`; it ships as
`wasm/flatsql-p4-threads.wasm` (wasm32-wasip1-threads). The design is the
stack's `docs/architecture/flatsql-sqlite-partitions.md` (gates G1-G7); the
interfaces are the build-out contract (`CONTRACT.md`, version 9: the ABI in
`cpp/include/flatsql/p4/flatsql_p4.h`, the reader API for the SQL surface in
`cpp/include/flatsql/p4/p4_reader.h`). This file records what was built, how
to run it, and what was measured.

## 1. Layout

```
<data>/fsql4/
  STORE                       64 B, magic FSQ4, format 4 (C-7); written last by activation
  MIGRATED                    40 B, magic FSQM, format 4
  T/TYPES                     the registered types (crc'd records)
  T/<TYPE>.spec               the registered spec TLV (C-5); a reopen serves before re-registration
  T/<TYPE>.idx                the type index (derived; rebuilt from the files)
  T/<TYPE>.jnl                the intent journal
  T/<TYPE>.fts                full text (FTS5, contentless-delete; background)
  P/<TYPE>/<pid>/<YYYYMM>.<gen>.db   a partition's month
```

- A partition is (type, producer token); the token comes from the peer
  (C-13). The month is the record's bucket time (`bucket` rule, else the
  `epoch` rule, C-1); a type without either has one month, `0`.
- Generations are never reused (B9): the type index's `gens` table keeps the
  highest generation per (partition, month) across drops and restarts, and a
  new file probes past any path on disk.

## 2. Files

**Partition file.** `meta` (format, type, producer, peer, pid, tb, gen, ix,
ncopy), `src` and `lane` (a lane is format 1's tag key, C-3, with its
counters), `r(seq PK, cid, e, k, ts, p, f, s, x, d, w, wd)` with `w` =
`coalesce(e, ts)`, tag instances `rl(sid, seq, lane, at, u)` WITHOUT ROWID,
and for object types `ent(k, n, fw, lw)`. Indexes: `r_w(w DESC)`, `r_s(seq)`
(the seqs alone: the A18 cut walks it, not the records' pages; 12 B a row),
`rl_seq(seq)`, `r_dk(wd, k, w)` (epoch and object), `r_k(k, w)` (object, no
epoch), `r_en(cid) WHERE e IS NULL` (epoch rule). There is no CID index in a
file: CID lookups go through the type index. Writers open with
`synchronous=FULL`, WAL, no autocheckpoint.

**Type index** (`.idx`, `synchronous=FULL`: a flush trims the journal after
its commit): `c(tb, cid, pid, seq)`,
`ident`, `obj`, `part`, `src`, `lanes`, `file`, `gens`, `lanecnt`, `meta`
(uniq, copies, next_seq). It is a cache of the files plus the journal: every
row can be rebuilt (REBUILD 2).

**Intent journal** (`.jnl`): `j(id AUTOINCREMENT, op, tb, k, c, pid, seq,
gen, s, v)` and `jm(seq_reserved)`. Ops: partition, lane, source, file
(J_FILE), entry (J_C), identity, removal (J_DEL), drop (J_DROP). A group
journals its entries before its partition files commit; flushes merge
committed entries into the type index and cut the journal behind them.

## 3. Writes

- Calls arrive in mailbox slots (two pools: 8 MiB write requests, 64 KiB
  read requests, C-6); writer threads own partitions (pinned, with a credit:
  `P4_E_BUSY` when a partition's backlog is full).
- A writer takes every queued call of a partition into one group (up to
  `groupRecords`): parse and extract, dedupe probe against the type index
  and the pending map, seq assignment (durable seq blocks), journal, one
  transaction per partition file, publish, ack. The ack follows the commit
  and the visible-through publish (C-4).
- Copies (a CID held by another producer), retags, CAT supersede-on-ingest
  (ingest mode only, C-20), IQC ingest identities (IDENT_DUP tags the held
  record, C-26), batch supersede and deletes are journaled removals.
- Migrate mode (PUT mode 1, create mode 2): seqs and tag instances are the
  caller's (format 1's rowids and `created_at`), a COPY keeps the holder's
  bytes and ts (C-22), identities are registered (C-21), secondary indexes
  wait for REBUILD 1, and full text waits for activation.

## 4. Crash safety

Every open replays the journal before serving (M8: replay opens each file it
checks, which recovers its WAL):

1. Registry: partitions, sources, lanes, files. A J_FILE whose file has no
   committed schema (killed between the empty file and its schema) is not
   trusted: the writer completes it. A J_DROP still in the journal had its
   index rows kept by the crash: replay removes them and the counters lose
   the dropped copies.
2. Entries: each J_C or J_DEL is checked against its file by seq and CID;
   counters move only for entries the index lacks.
3. Touched files are recounted from their own rows.
4. Retired generations left on disk are unlinked (paths are known from
   `gens`; the wasm host has no directory listing).

The type index never records a file whose schema has not committed. A quota
drop refuses new claims on the month's files and waits until claimed writes
have published before it retires them.

## 5. Maintenance

One background thread (never on a caller's path): type-index flushes,
PASSIVE and RESTART checkpoints from the WAL hook, closing evicted writer
connections, unlinking retired files once their readers close, the
configured quota, full text, journal VACUUM when a flush empties it, and the
file rebuild: a file whose free pages reach 15% and 64 MB (config tags 45,
46) is rewritten by `VACUUM INTO` its next generation and swapped in; its
writes answer `P4_E_BUSY` meanwhile.

- **QUOTA_GC** (mode 1, D1): drops whole content months, oldest first, never
  the current one; past the configured quota writes refuse with
  `P4_E_NOSPACE` while reads continue.
- **REBUILD**: 1 partition secondary indexes (after a migration's bulk
  append), 2 the type index from the files, 4 full text, 8 verify (the index
  against the files, and `PRAGMA integrity_check` on every live file, the
  type index and the journal, C-27; a damaged file is a mismatch named in
  the slot err). Verify returns errors as errors (M9), never as mismatches.

## 6. Reads

Orders: seq (datasync), w (windows: `w DESC`, ties in CID order), CID (the
type index's months, paged and merged with the pending layer). Copies
collapse to one row per CID (the lowest pid that matches), also under OFFSET.
The A18 bound is the type's newest N seqs before every other filter (C-17,
C-24). An equality, IN or range predicate on the object rule's first column
reads through `ent` and `r_dk`/`r_k` instead of walking the window, so a
NORAD equality inside OMM's 400,000 bound reads that object's rows. Caps
(rows examined, bytes read, result rows and bytes, cancel) end a read with
its status; RB1 streams always end with RB1E. The SQL surface (`src/p4sql`)
answers ops 30 and 31 through `p4_reader.h`; the engine writes their RB1E
(C-28). Without the surface ops 30/31 answer `P4_E_UNSUPPORTED`.

## 7. Memory

One engine-wide budget (design §9): SQLite is built without memory
statistics (C-30); the hard heap limit (config tag 27, 640 MiB) is enforced
by the SQL surface's allocator, which also serves stats 29/30. Reader
connections are pooled (tag `readerConns`, 512 KiB cache each), writer
connections are an LRU (`writerConns`), pending index entries flush at
`pendingBytes` (64 MiB).

## 8. The artifact

| | |
|---|---|
| Build | `bash scripts/build-wasm.sh --ps` (with flatsql-ps-threads.wasm) or `--ps-tests` (plus `cpp/build-ps-wasm/flatsql-p4-test.wasm`); released bytes: `--ps --linux` (Docker, Linux wasi-sdk 30). CMake: `cpp/cmake/flatsql_p4_wasm.cmake`, globbing `src/p4`, `src/p4sql`, `tests/p4`, `tests/p4sql`. |
| SQLite | 3.53.4 amalgamation, byte-identical (CMake checks its sha256): `THREADSAFE=2`, WAL, FTS5, `DEFAULT_MEMSTATUS=0`, `TEMP_STORE=3`, `SQLITE_OS_OTHER`; the only VFS is `flatsql_io` (per-path nodes: shared heap WAL index, in-memory locks, reader readahead), registered as the default by `src/p4/capi_wasm.cpp`, which also installs pthread mutexes. |
| Imports | `wasi_snapshot_preview1`: `clock_time_get environ_get environ_sizes_get fd_close fd_prestat_dir_name fd_prestat_get fd_seek fd_write proc_exit sched_yield` (environ from libc++'s locale in the full-text tokenizer); `wasi.thread-spawn`; `env.memory` (shared, at most 32768 pages); the seven `env.flatsql_io_*`. `scripts/check-wasm-imports.mjs` fails on any change. |
| Exports | `flatsql_p4_init start stop wake layout register_type activate set_quota stats alloc free`, `wasi_thread_start`, `_initialize`, `memory`. |
| Host | As for the partition store (docs/PARTITION-STORE-WASM.md): grow the shared heap before `flatsql_p4_start`; the Node host is `wasm/ps-node-host.mjs`. |

## 9. Tests

```
FLATBUFFERS_DIR=<flatbuffers> cmake -S cpp -B cpp/build && cmake --build cpp/build --target flatsql_p4_test -j 6
cpp/build/flatsql_p4_test                       # the fast suite
cpp/build/flatsql_p4_test --test=t_kill --rounds=1000
cpp/build/flatsql_p4_test --test=g2_fixture --fixture=<format-1 control.flatsqldb> --bfbs=<search-schemas> [--store=<dir>]
bash scripts/build-wasm.sh --ps-tests && node scripts/p4-wasm-suite.mjs [--kill-rounds 1000]
```

- `t_design.cpp`: t_close (B1), t_dropreuse (B9), t_gap (B2), t_crash and
  t_kill (B3, M8). The kill loop's workload has three producers, copies,
  batch supersede every 13th call and a quota drop to 4 MiB every 11th
  (content months rotate, generations advance); each round kills the engine
  with SIGKILL at a random point and checks: every file passes
  `integrity_check`, every row on disk is found by CID, the count equals the
  distinct CIDs on disk, REBUILD 8 finds no mismatch, new seqs are above
  every seq on disk, and no file is left open. The wasm loop kills the
  guest's workers in the Node host.
- `t_abi.cpp`: markers, golden vectors, every op of §3.5. `t_ops.cpp`:
  bucket and identity rules, supersede, quota, rebuild, budget, group
  commit. `t_contract.cpp`: C-21/C-26, C-22, C-25, C-27, the file rebuild, a
  torn file creation, object-key reads inside the bound.
- `p4_fault_io.cpp` (`flatsql_p4_fault_test --test=t_power_loss --slow=1`):
  the engine over FaultFs (in-memory files that keep what a power loss
  leaves: unsynced writes dropped, a random subset kept in order or
  reordered, the last write torn, or kill -9), frozen at a random I/O call
  per round; after the crash every acknowledged record is there, REBUILD 8
  (with `integrity_check`) is clean and the count equals a full scan's
  distinct CIDs. It found that engine-created SQLite files lacked durable
  directory entries and that the rollback journal of a new file's switch to
  WAL could come back as a hot journal; with `dsync=1` the VFS now creates
  database files with `CREATE_PARENTS` and deletes durably when SQLite asks
  (`synchronous=EXTRA` during the switch).
- `t_fixture.cpp` (`g2_fixture`): loads the host-02-sized format-1 fixture as
  store-migrate does and compares GET, TAGS, SCAN, WINDOW and INDEX_PAGE with
  format 1 per type.

## 10. Measured

Mac Studio (28 threads, shared; the load is the 1-minute average):

| | |
|---|---|
| G2 fixture | 4,364,873 copies of 3,945,845 records (OMM, MPE, CAT, IQC) loaded in 101 s (43k copies/s, load 15-23; 193 s at load 39-44); REBUILD 1 in 10 s; **795.5 B per copy** with `r_s` (783.2 without it; gate < 800; format 1 2,366, format 2 1,742); 0 differences against format 1 in GET bytes/seq/ts/peer, TAGS, SCAN order, WINDOW and INDEX_PAGE per type; REBUILD 8 clean. |
| Group commit | One-record calls with 1,024 in flight: 512-record groups, 392 WAL B/record against 315 for 4,096-record calls (gate 2x). |
| Kill loop | native 400 of 400 rounds at load 11-38 with drops and supersede; the 1,000-round runs are recorded in the release notes. |
