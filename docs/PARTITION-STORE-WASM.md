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
- **V8 hosts (Node, browsers): grow the heap to the memory's maximum before
  `flatsql_ps_start`, so it never grows while guest threads run.** Allocate
  through `flatsql_ps_alloc` in steps from 1 GiB down to 1 MiB until each
  fails, then free them all (the allocator keeps the blocks; wasm memory never
  shrinks; the guest allocator grows at most 2 GiB per call). V8 bounds-checks
  `memory.fill`/`memory.copy` and atomics against each thread's cached view of
  a shared memory's size, and another thread's `memory.grow` updates that view
  only at this thread's next stack check. A thread that fills a block carved
  from memory another thread has just grown then traps "memory access out of
  bounds" (§4.3). A partial pre-grow only moves the failure: the 1,536 MiB the
  test main used to grow was passed at 1,651 MiB in CI. Stop 16 MiB short of
  4 GiB with a 4 GiB maximum: a heap segment ending at 2^32 wraps the
  allocator's pointer arithmetic. `createPsNodeInstance` does this
  (`growHeap: false` opts out, `heapBytes` caps it), and the Node host reports
  `memoryGrownWhileThreadsRan` for every guest and warns when it is not 0.
- **WasmEdge hosts do not pre-grow** (and SDN recycles reader instances on
  their page count). WasmEdge keeps one page count shared by every thread's
  instance and grows in place in a fixed reservation, so a grow is visible to
  every thread at once. WasmEdge 0.16.4's AOT compiler must carry SDN's
  `04-atomic-memarg-offset` patch (§4.3).
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
test main grows the heap to the memory's maximum before any thread starts
(§1; `PS_WASM_HEAP_MB` caps it).

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
during these runs (37–41 for the release rerun, §4.2). Native builds
RelWithDebInfo; wasm builds as §1.

| # | Acceptance | Result |
|---|---|---|
| 1 | Imports exactly WASI p1 + `wasi.thread-spawn` + shared `env.memory` (≤ 32768 pages) + the 7 `env.flatsql_io_*`; exports `wasi_thread_start`; 0 emscripten imports; passes `assertPthreadArtifact` | **Pass.** §1 table; `scripts/check-wasm-imports.mjs`, `test/standalone-wasm-imports.test.ts` (the SDK guard runs in a child process). |
| 2 | The T1 and T2 suites, including 1,000 fault crashes, pass under the Node host with distinct OS thread ids ≥ writers + lanes | **Pass.** `scripts/ps-wasm-suite.mjs`: all 53 default-suite tests, each in its own guest process, plus `crash_faults_T1_1 --crash-trials=1000` (1,000 trials over the five modes, 298,923 acked records verified, 41.8 s). `readers_under_saturating_writers_T2_1` (8 writers, 16 lanes) ran 54 guest threads at once on 54 distinct worker (OS) threads. `test/ps-node-threads.test.ts`: the shipped artifact as a writer instance (4 writers) and a reader instance (4 lanes), each running thread on its own worker. `writer_backpressure_flood_T1_5` runs with `--bp-samples=40` (deviation 3). `crash_faults_T1_1_full` (10,000 trials, 64 types, 256 partitions): §4.1. |
| 3 | Parity: canonical dumps (by (pid, pseq)) and query results identical native vs Node; deterministic mode byte-identical across native, Node and the SDK WasmEdge C runner (patched 0.16.4) | **Pass.** Canonical dump, 4 writers, 11,313 lines (rows, counters, lane counters and six queries, partition and type level): sha256 `f3b419a2…` natively and under Node, three runs each. Deterministic mode, 122 files: in memory `e7ab8592…` natively, under Node and under the C runner (18.7 s, interpreted); on the host's files `c7e34c55…` natively and under Node. `test/ps-parity-vectors.test.ts`. |
| 4 | A published npm version with provenance; the wasm sha256 in `integrity.json`; `npm ls sql.js` empty; the package root exports only the wasm bindings | **Pass.** `flatsql@3.0.0`, §4.2 and §6. |

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

- `flatsql@3.0.0`, published by `npm-publish.yml` from tag `v3.0.0` =
  `79fd694` (run 36486543061) with a signed provenance statement
  (`npm audit signatures`: verified attestation); `gitHead` 79fd694, dist-tag
  `latest`, `dependencies` `{}`.
- `wasm/flatsql-ps-threads.wasm`: 2,074,707 bytes, sha256
  `572a4bc38345d07ff91c345fc8d81e28873d72efa0fb8864a2edc5242145f9f5`, the same
  in `wasm/integrity.json`, from the Linux arm64 Docker build, the x86_64
  publish runner and the installed tarball.
- A fresh `npm install flatsql@3.0.0`: `npm ls sql.js` is empty; the root's
  exports equal `flatsql/wasm`'s; `loadFlatSQLPsThreads()` checks the bytes
  against `integrity.json`.
- At the release sources (`dac8d89` engine), under the Node host: the 55
  default-suite tests and `crash_faults_T1_1 --crash-trials=1000` pass (1,000
  trials, 403,798 acked records verified, 47.3 s; load 37–41). Main CI green
  on `79fd694` (run 36482613908).
- Two engine defects the Node host found before the release, fixed in the
  sources the artifact builds: the open path dropped the lane checkpoint
  pointer of a head with more than 32 live lanes (`5c7e769`; the 1,000-trial
  crash run, trial 317), and the type owner never merged its L0 directory
  when `mergeL0Blocks` exceeded the directory cap, so labeling, partition
  merges and acks stopped (`dac8d89`).

### 4.3 The shared-memory grow race (V8) and AOT wait/notify (WasmEdge)

**V8.** CI ps-wasm run 36603876540 (attempt 1) trapped "memory access out of
bounds" in `writeTypeMergeOutputs` on the type-merge helper thread during
`readers_under_saturating_writers_T2_1`, with memory at 1,651 MiB, past the
test main's 1,536 MiB pre-grow. A Docker build with the Linux wasi-sdk 30 at
the CI checkout path reproduces the CI test command exactly (its function
indices match the trace, and its `flatsql-ps-threads.wasm` is the committed
artifact's sha256). The trap offset `0x18d31a` is a `memory.fill`: the
zero-fill of `blocks[i].resize(de.l0Len)` right after its `operator new`.
Each thread's instance caches the memory size, and V8 bounds-checks
`memory.fill`/`memory.copy` and atomics against that cache. Another thread's
`memory.grow` updates the cache only at this thread's next stack check, so a
block the allocator carves from memory another thread has just grown fails
that check. Plain loads and stores are checked by guard pages and never trap.
Measured on the Mac, Node 25.4.0, load 35–65:

| Heap before threads start | T2_1 runs | Traps |
|---|---|---|
| none (`PS_WASM_HEAP_MB=0`) | 31 | 8, every one a `memory.fill`/`memory.copy` right after an allocation (vector push_back, string copy, the resize above) |
| 2,000 MiB (past the test's peak) | 30 | 0 |
| the memory's maximum (§1, now the default) | 500 | 0, and 0 runs grew memory after a thread started (1 run ended in a Node 25.4.0 SIGILL inside V8's JIT code, a host crash) |

Natively the same test under ASan+UBSan (Docker linux/arm64, 4 CPUs, a CPU
hog) found no memory error in 488 runs: the engine never overran.

**WasmEdge, the SDN host.** Every thread's instance imports one shared
`MemoryInstance`. On 64-bit hosts it is reserved in full up front (12 GiB,
`lib/system/allocator.cpp`) and `memory.grow` maps pages in place, so the
base never moves. The page count is one field that `memory.fill`/`copy`/
`init` and atomic wait/notify check live, and plain loads and stores are
checked by guard pages. The SDN C host I/O resolves every guest pointer per
call (`WasmEdge_MemoryInstanceGetPointer`), and the Go accessor re-reads the
page count on a miss. A grow is visible to every thread when the growing
thread's `memory.grow` returns, and the guest allocator's lock orders it
before any other thread can allocate from it. Measured: the same T2_1 with
no pre-grow on the patched WasmEdge 0.16.4, AOT, had 0 traps in 26 completed
runs.

**WasmEdge AOT wait/notify.** Those WasmEdge runs also hung at teardown: 4 of
30 with no pre-grow, 5 of 30 with a 1,536 MiB pre-grow. WasmEdge 0.16.4's AOT
compiler passed `memory.atomic.notify` and `memory.atomic.wait32/64` the bare
address operand and dropped the instruction's memarg offset; the interpreter
adds it. wasi-libc addresses its thread-list lock as `i32.const 0;
memory.atomic.notify offset=<lock>`, so those wakeups went to address 0 and
thread exit and join hung. A plain spawn/join guest with no flatsql code, 50
rounds of 16 threads, hung 11 of 20 runs AOT, 0 of 30 interpreted and 0 of 20
under V8. `sleepNs` (a timed wait on a stack local whose frame offset the
compiler folds into the memarg) compared another stack word and returned at
once: 100 × 10 ms took 0.000 s AOT and 1.159 s interpreted. SDN's static
WasmEdge carries patch `04-atomic-memarg-offset` from SDN `74db37e14`, and
its substrate self-test checks it (tag `sdn3`, so threaded AOT artifacts
recompile). With the patch, the plain guest ran 40 of 40, and T2_1 AOT ran
140 times with no hang. Two of those runs failed. One ran out of the 4 GiB
address space: the memio command keeps every file in guest memory, and
passing runs end near 2.7 GiB. The other run's output was not kept.
`memory_safety_sleep_and_notify_at_folded_offsets` passes. Without it, that test fails (20 of 20 folded-offset wakeups lost).
`sleepNs` now passes its address through a volatile, so its wait has a zero
memarg offset and sleeps even on an unpatched runtime. The artifact still has
4 folded wait/notify sites, all in wasi-libc's thread code.

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
   Linux-8 acceptance numbers. `lane_needs_bulk_under_1ms_zero_rows_T2_4`
   enforced its 1 ms p99 on every machine; it now follows the same rule
   (enforced on a quiet box, reported elsewhere) after a Debug build on the
   loaded 3-vCPU macOS CI runner and the Node host on the Linux runner
   measured past it. Its status, rows and examined counts are enforced
   everywhere. `type_arrivals_segments_and_fence_A15` caps type commits at 40
   rows: a slow type owner labeled more than 40 rows in one commit and filled
   an empty arrivals segment past its 40-entry seal (a batch never splits
   across segments), which failed on the macOS runner (main 8aea463) and
   under the Node host.
4. **The C runner lane runs the memio command.** The SDK's WasmEdge
   wasi-threads C runner provides WASI and thread-spawn only, so the byte
   vectors there use the in-memory host, which is also compared natively and
   under Node; the host-file vector is compared natively and under Node.
   Adding flatsql_io to the runner (as the SDN C host module does) is an SDK
   change.
5. **Parity inputs are built deterministically.** A fixture expression such
   as `a + b * c` is contracted to a fused multiply-add on arm64 and not in
   wasm, which changes the input bytes (1 ULP), not the engine's output; the
   parity workload builds its frames from exact doubles. `buildRecordAttr`
   (`src/ps/writer_pool.cpp`) creates its strings inside one call's argument
   list, whose evaluation order C++ leaves unspecified: GCC on x86_64 builds
   them in the opposite order from clang and from GCC on arm64, so the same
   attributes serialize to different bytes (measured on the CI runner and under
   x86_64 emulation). The parity workload builds its attributes in a fixed
   order; the helper itself belongs to the writer files T3 holds (reported). The canonical dump masks
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
