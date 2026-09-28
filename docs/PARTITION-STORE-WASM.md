# FlatSQL partition store: the wasm artifact and the Node host (T4)

The partition store engine (docs/PARTITION-STORE.md, T1 and T2) built as one
`wasm32-wasip1-threads` artifact, `wasm/flatsql-ps-threads.wasm`, and the
Node host that runs it and the engine's test suites with real threads. The
design is the stack's `docs/architecture/flatsql-partition-store.md`: §5.5,
§5.6, §18 T4 and the §22 amendments naming T4 (A1, A34, 22.3a-6/7). This file
records what was built, how to run it, the measured acceptance, and every
departure from the design.

## 1. The artifact

| | |
|---|---|
| Build | `bash scripts/build-wasm.sh --ps` (artifact) or `--ps-tests` (plus the test commands). CMake: `cpp/cmake/wasi-threads-toolchain.cmake` and `cpp/cmake/flatsql_ps_wasm.cmake`, included from `cpp/CMakeLists.txt` only under the WASI toolchain. Engine and test sources are globbed (`src/ps/*.cpp`, `test/ps/*_test.cpp`), so files other tasks add build here unchanged. |
| Toolchain | wasi-sdk 30.0 (clang 21.1.4), target `wasm32-wasip1-threads`, `-pthread -matomics -mbulk-memory -fno-exceptions`, Release (`-O3 -DNDEBUG`), `-ffile-prefix-map` for the source, flatbuffers and SDK paths, `--strip-debug`. The build script refuses any other wasi-sdk version. |
| Bytes | The released artifact is the Linux wasi-sdk 30 build: Linux arm64 (Docker) and the x86_64 CI runner produce the same sha256 from different checkout paths. The macOS wasi-sdk 30 tarball ships a different sysroot (libc, libc++), compiler-rt and clang build and links different (equivalent) bytes; on macOS `bash scripts/build-wasm.sh --ps --linux` builds the released bytes in Docker (the Linux tarball, sha256-pinned). `npm-publish.yml` rebuilds and refuses to publish unless the build equals the committed file; CI warns when it differs. `wasm/integrity.json` holds the sha256, sha384 SRI and size of every shipped wasm (`scripts/write-integrity.mjs`, `npm run check:integrity`). |
| Shape | A reactor (`-mexec-model=reactor`). A host instantiates one instance per writer or reader instance; every guest thread is a new instance of the same module over the same shared memory. |
| Imports | `wasi_snapshot_preview1`: `clock_time_get fd_close fd_prestat_dir_name fd_prestat_get fd_seek fd_write proc_exit random_get sched_yield` (all provided by the SDK's browser and Node pool hosts); `wasi.thread-spawn`; `env.memory` (shared, 512 initial, 32768 maximum pages); the seven `env.flatsql_io_*`. Nothing else: `scripts/check-wasm-imports.mjs` and `test/standalone-wasm-imports.test.ts` fail on any change. |
| Exports | `flatsql_ps_init start stop pump wake layout reader_layout stats register_type register_partition ring` (docs/PARTITION-STORE.md §6), `flatsql_ps_alloc`/`flatsql_ps_free` (host-owned buffers for config TLVs and paths), `wasi_thread_start`, `_initialize`, `memory`. |
| SQLite | The lanes' SQL layer (ruling 1): `SQLITE_THREADSAFE=2`, `DEFAULT_MEMSTATUS=0`, `OMIT_WAL`, `TEMP_STORE=3`, `SQLITE_OS_OTHER`. No unix VFS (it would import WASI file calls): `src/ps/capi_wasm.cpp` registers a default VFS that opens no file, and installs pthread mutexes, because `SQLITE_OS_OTHER` compiles only no-op ones. The lanes open `:memory:` on `flatsql_ps_null` as natively. |
| Threads | `std::thread` defaults to 1 MiB stacks (wasi-libc's default is 128 KiB and a shadow stack has no guard page); lanes set their own (`ReaderConfig::stackBytes`). `sleepNs` is a timed `memory.atomic.wait32`, so the artifact never imports `poll_oneoff`. |
| Guard | `assertPthreadArtifact` (space-data-module-sdk 0.8.24) passes: shared memory, 1,636 atomic instructions, the wasi-threads contract, no emscripten thread hooks. |

### Host obligations

- Create `env.memory` shared, with a maximum no larger than the import's
  (browser writer 256 MiB, reader 128 MiB, design §5.5).
- Grow the heap before `flatsql_ps_start` when threads will allocate (alloc
  then free a large block through `flatsql_ps_alloc`/`flatsql_ps_free`): V8
  (Node 20–25, and browsers) refreshes each thread's view of a shared memory's
  size lazily, so a thread touching memory another thread has just grown can
  trap. WasmEdge is not affected.
- Give every guest thread a distinct `tid` below 2^29 (wasi-libc keeps it in
  mutex words).

## 2. The Node host (design §5.6)

`wasm/ps-node-host.mjs` (types `ps-node-host.d.mts`), with
`ps-node-guest.mjs`, `ps-node-imports.mjs` and `ps-node-fault.mjs`. Node 24+,
optional peer dependency `space-data-module-sdk` ≥ 0.8.24.

- **Supervisor.** The main thread never runs guest code. The guest's own
  thread (`_start` for a command, the exec thread for a reactor instance) is a
  worker; so is every guest thread.
- **wasi.thread-spawn** claims a pre-started worker from a pool the supervisor
  keeps (16 warm, grown on demand to a cap of 512). The claim is a CAS on the
  pool's SharedArrayBuffer and needs no event loop, so any guest thread can
  spawn, and workers are reused: a crash harness that starts and stops engines
  12,000 times runs on 17 workers. Tids come from one shared counter.
- **flatsql_io** is the SDK's Node sync-fs provider (`createNodeSyncFsIo`: a
  virtual-handle table in a SharedArrayBuffer, lazily opened fds per worker,
  `fdatasync`/`F_FULLFSYNC`, `CREATE_PARENTS`, `UNLINK_IF_UNUSED`, A23
  revocation) over the host root.
- **WASI preview1** is the host's own: a descriptor table in a
  SharedArrayBuffer (a descriptor opened on one thread works on all, as in a
  process), preopens, process-wide clocks (`process.hrtime`; a worker's
  `performance.now()` has its own origin, which would break the engine's
  cross-thread time comparisons), seeded `random_get` in deterministic mode,
  `poll_oneoff` clock waits, `proc_exit` from any thread ending the guest.
- **File system.** `root` is the guest's "/" for WASI and for flatsql_io;
  `mounts` preopen more host directories (the CLI mounts the checkout at its
  own path so the test commands find their vectors). Default: a private
  temporary root, removed afterwards.
- **Faults.** A guest thread that traps writes the trap to stderr from its own
  worker and sets a shared fault word; the supervisor ends the run (the thread
  that would join the dead one may be blocked forever).
- **API.** `runPsCommand(wasm, options)` (commands: exit code, kill, timeout,
  thread report); `createPsNodeInstance(wasm, options)` (one artifact instance:
  `call`, `write`, `read`, `threads`, `close`); CLI `node wasm/ps-node-host.mjs
  <command.wasm> [args]`.

### Fault overlay (§19, 22.3a-7)

`wasm/ps-node-fault.mjs`: a SharedArrayBuffer page table every worker
consults, interposed under the SDK provider's syscalls. It records the
pre-image of every 4 KiB page written since the file's last sync (pages wholly
past the synced size need none), the synced size, and files created since
their parent directory's last sync. The supervisor freezes it (later mutating
calls fail with EIO), stops the guest, and applies a crash: `dropAll` (every
unsynced page and size change undone, unsynced directory entries gone),
`pageSubset` (each unsynced page survives or not, in any order; the size is
the synced or the current one), `keepAll` (kill -9). Overflowing its page
budget fails the crash instead of making it partial.

## 3. Test commands

| Command | Contents |
|---|---|
| `cpp/build-ps-wasm/flatsql-ps-test.wasm` | `flatsql_ps_test` (every test in `cpp/test/ps`) as a wasi-threads command; host I/O through the seven imports. `--list` prints the tests. |
| `cpp/build-ps-wasm/flatsql-ps-test-memio.wasm` | The same command with the seven functions defined in the guest (`test/ps/wasm_io_stub.cpp`, all ACCESS): imports only WASI, thread-spawn and memory, so a WASI-only host (the SDK WasmEdge C runner) runs every in-memory test. |

Added tests (`PS_SLOW_TEST`, run only when named): `parity_canonical_dump`,
`parity_deterministic_bytes` (`cpp/test/ps/parity_test.cpp`) and
`host_crash_ingest` / `host_crash_verify` (`host_crash_test.cpp`). The wasm
test main grows the heap by 1,536 MiB before any thread starts
(`PS_WASM_HEAP_MB`).

```
npm run build:wasm:ps-tests                        # artifact + test commands
node wasm/ps-node-host.mjs cpp/build-ps-wasm/flatsql-ps-test.wasm --test=lane_
npm run test:ps-wasm                               # scripts/ps-wasm-suite.mjs: every default test + 1,000 crashes
FLATSQL_PS_WASM_SUITE=1 npm test -- test/ps-node-threads.test.ts
npm test -- test/ps-host-fault.test.ts test/ps-parity-vectors.test.ts
FLATSQL_PS_WASMEDGE=1 npm test -- test/ps-parity-vectors.test.ts   # + the C runner lane
```

## 4. Acceptance (§18 T4 as amended)

Machine: the owner's Mac Studio (Darwin 25.3.0, arm64, 28 hardware threads,
APFS), Node 25.4.0, shared with other lanes: 1-minute load average 18–25
during these runs. Native builds RelWithDebInfo; wasm builds as §1.

| # | Acceptance | Result |
|---|---|---|
| 1 | Imports exactly WASI p1 + `wasi.thread-spawn` + shared `env.memory` (≤ 32768 pages) + the 7 `env.flatsql_io_*`; exports `wasi_thread_start`; 0 emscripten imports; passes `assertPthreadArtifact` | **Pass.** §1 table; `scripts/check-wasm-imports.mjs`, `test/standalone-wasm-imports.test.ts` (the SDK guard runs in a child process). |
| 2 | The T1 and T2 suites, including 1,000 fault crashes, pass under the Node host with distinct OS thread ids ≥ writers + lanes | **Pass.** `scripts/ps-wasm-suite.mjs`: all 53 default-suite tests, each in its own guest process, plus `crash_faults_T1_1 --crash-trials=1000` (1,000 trials over the five modes, 298,923 acked records verified, 41.8 s). `readers_under_saturating_writers_T2_1` (8 writers, 16 lanes) ran 54 guest threads at once on 54 distinct worker (OS) threads. `test/ps-node-threads.test.ts`: the shipped artifact as a writer instance (4 writers) and a reader instance (4 lanes), each running thread on its own worker. `writer_backpressure_flood_T1_5` runs with `--bp-samples=40` (deviation 3). `crash_faults_T1_1_full` (10,000 trials, 64 types, 256 partitions): §4.1. |
| 3 | Parity: canonical dumps (by (pid, pseq)) and query results identical native vs Node; deterministic mode byte-identical across native, Node and the SDK WasmEdge C runner (patched 0.16.4) | **Pass.** Canonical dump, 4 writers, 11,313 lines (rows, counters, lane counters and six queries, partition and type level): sha256 `f3b419a2…` natively and under Node, three runs each. Deterministic mode, 122 files: in memory `e7ab8592…` natively, under Node and under the C runner (18.7 s, interpreted); on the host's files `c7e34c55…` natively and under Node. `test/ps-parity-vectors.test.ts`. |
| 4 | A published npm version with provenance; the wasm sha256 in `integrity.json`; `npm ls sql.js` empty; the package root exports only the wasm bindings | See §6 and the release record in §4.2. |

Host-level crashes (the overlay, §2): `test/ps-host-fault.test.ts` kills the
guest mid-ingest at a random 0.4–2.0 s, crashes the store in the three modes
in turn and verifies every acked record (bytes by CID), gap-free pseqs,
counters and 0 `d-*` bytes read at open; 9 cycles, half of them with the A8
journal. A negative control (half a data segment cut off) fails the verifier.

### 4.1 Slow runs

- `crash_faults_T1_1_full` under the Node host (10,000 trials, 64 types, 256
  partitions, the five crash modes, an A4 second crash every other trial, half
  the stores journaled): pass in 377 s; 2,122,844 acked records verified,
  410,159 merges, 1,167,880 journal records replayed, 125,016 guest threads
  spawned on 10 pooled workers. Mac, load 24 rising to 35 during the run
  (natively 243 s).
- The whole default suite under the host, one guest process per test:
  155 s of test time; the most concurrent guest threads was 54
  (`readers_under_saturating_writers_T2_1`), each on its own worker thread;
  the largest guest memory 1,902 MiB (the 1,536 MiB pre-grown heap included).
- Deterministic vector under the C runner: 18.7 s interpreted (0.12 s natively).

### 4.2 Release

Filled in when the release is published.

## 5. Design deviations

1. **The Node host's thread spawn is its own pool, not the SDK's Node
   branch** (§5.6: "The SDK's Node branch (lazy worker_threads) runs the guest
   threads"). `createWasiThreadSpawn` (0.8.24) removes a finished worker only on
   its `exit` event, which runs on the spawning thread's event loop; that
   thread is inside the guest, blocked, for the whole command. After `poolSize`
   spawns every later spawn is declined, and without a pool the finished
   threads are never joined. The crash harness spawns more than 12,000 guest
   threads per run. The host keeps a pool whose idle/assign handshake is a
   SharedArrayBuffer CAS, so no event loop is involved and any thread can
   spawn. Reported to the module-sdk lane (SDK 0.8.25); the SDK's Node sync-fs
   flatsql_io provider is used as designed.
2. **WASI is the host's own**, not the SDK shim: the shim keeps descriptors
   and clocks per worker (a worker's `performance.now()` has its own origin),
   has no preopens and no `poll_oneoff`. The artifact itself imports only names
   the SDK shim provides (§1), so browser hosts are unaffected.
3. **The flood test runs 40 samples under wasm32** (`--bp-samples=40`, default
   150): its in-memory host keeps every flooded byte twice for as long as the
   timed measurement lasts (12 GB resident natively with 150 samples), past
   wasm32's 4 GiB. Its ring, credit, pool and zero-loss checks are unchanged;
   the ack-p99 ratio is reported (1.066) and, as on any box with fewer than 8
   hardware threads, not enforced: WASI reports one CPU. The same holds for the
   T2 latency bounds (`latencyBoxQuiet` is false under wasm): they are native
   Linux-8 acceptance numbers.
4. **The C runner lane runs the memio command.** The SDK's WasmEdge
   wasi-threads C runner provides WASI and thread-spawn only, so the byte
   vectors there use the in-memory host, which is also compared natively and
   under Node; the host-file vector is compared natively and under Node.
   Adding flatsql_io to the runner (as the SDN C host module does) is an SDK
   change.
5. **Parity frames use exact values.** A fixture expression such as
   `a + b * c` is contracted to a fused multiply-add on arm64 and not in wasm,
   which changes the input bytes (1 ULP), not the engine's output; the parity
   workload builds its frames from exact doubles. The canonical dump masks
   `kRowAttrInM` (where attributes are stored until a merge moves them, like
   offsets) and injects the engine's wall clock (TOMB arrival times).
6. **Deterministic mode injects both clocks in the guest** (`setTestClock`,
   `src/ps/platform.cpp`): the store UUID hashes the monotonic clock, and a
   frozen clock keeps every time-based trigger out of the byte stream. A host
   running the artifact deterministically injects `clock_time_get` and
   `random_get` instead (the Node host's `deterministic` option seeds
   `random_get`).
7. **A wasm32 defect fixed in the lane arena**: the 256 GiB request bound was
   `size_t(1) << 38`, undefined where `size_t` is 32 bits; every lane
   allocation failed and every lane died at `sqlite3_open`.

## 6. Package 3.0.0 (A1, A34)

Semver-major (A34). The package root is the wasm bindings: `wasm/index.js`
(the SQLite engine, as `flatsql/wasm`) plus the partition store locator
(`getFlatSQLPsThreadsURL`, `loadFlatSQLPsThreads`,
`FLATSQL_PS_THREADS_SHA256`). New subpaths: `flatsql/ps-threads.wasm`,
`flatsql/ps/node-host`, `flatsql/ps/fault`, `flatsql/integrity.json`.

Removed (A1 ledger row T4):

| Removed | Replaced by |
|---|---|
| `src/core/` (the TS engine) and the root exports `FlatSQLDatabase`, `DirectAccessor`, `FlatcAccessor`, `TableStore` | the wasm engines |
| the `node:sqlite` artifact builder (`FlatSQLArtifactBuilder`, its worker) and the `flatsql/artifacts/standalone` builder | — |
| `wasm/standalone.js` and `flatsql/standalone` | `flatsql/ps/node-host` for the partition store. The legacy WASI engine's own tests keep a test-only copy (`test/support/legacy-standalone.js`, not shipped) until T8 retires that engine. |
| `sql.js`, and the npm `flatbuffers` package (its only user was the deleted TS workload) | — |

The BTree, stacked store, schema parser and cluster detection stay, as
`flatsql/btree`, `flatsql/storage`, `flatsql/schema` and `flatsql/cluster`.

**A1 parity check (the receipt).** A1 allows the deletion when each consumer's
suite passes on the new bindings or the consumer pins the last pre-T4 major.
Every consumer pins an earlier major, so none resolves 3.0.0:

| Consumer (stack pin) | flatsql |
|---|---|
| space-data-module-sdk 0.8.24 (450aa78; `src/runtime-host/flatsqlRuntimeStore.js`) | `^2.0.0` |
| OrbPro (216083d024; `packages/wasm-engine`, root) | `^1.4.4` |
| OrbPro `packages/space-data-module-sdk` 0.8.23 | `^2.0.0` |
| spacedatastandards.org 1.228.0 (cd86467de5; `test-flatsql*.js`) | `^1.3.0` |
| spacedatastandards `packages/standards-explorer` | `^0.3.9` |
| sdn-js 3.1.0 (space-data-network f20dbc35d) | `2.0.3` |

Moving each consumer to 3.x (or recording that it stays on 2.x) is a ripple
task per repository (A1, A34).
