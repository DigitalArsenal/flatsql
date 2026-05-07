# WasmEdge Standalone Artifact Builder Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a real standalone FlatSQL wasm runtime that loads in WasmEdge and the browser from the same artifact, with a tiny JavaScript shim for browser host differences and a WasmEdge-backed artifact builder that exercises the resident C++ query/result cache.

**Architecture:** Make C++ the source of truth for ingestion, indexing, query execution, query-template registration, and query result caching. The browser shim should only provide WASI/browser capability adapters and C ABI marshalling. The WasmEdge side should keep a single module instance resident behind a runner process so repeated `FILE_ID` and query-template requests hit the C++ cache instead of spawning a new process per query.

**Tech Stack:** C++17, vendored SQLite, Emscripten standalone WASI, WasmEdge 0.14+, WasmEdge C API runner, WebAssembly JS API, minimal ES module shim, Jest, Node test runner utilities

**Status:** Implemented and verified. The detailed task list below is retained as the execution record; current source of truth for deployers is `README.md`, `docs/index.html`, `package.json` exports, and the tests listed in the verification section.

## Implementation Notes

- `wasm/flatsql-wasi.wasm` is now a standalone reactor with only the supported `wasi_snapshot_preview1` imports used by `wasm/standalone.js`.
- `flatsql/standalone` loads the raw reactor directly in browser/Node WebAssembly and supplies the small WASI shim.
- `flatsql/artifacts/standalone` is the C++-backed artifact-builder facade. It does not keep a TypeScript result cache.
- `flatsql/standalone/wasmedge` builds and drives `wasm/native/flatsql_wasmedge_runner.cpp`, a persistent WasmEdge C API process client.
- The WasmEdge runner disables WasmEdge logging on stdout, duplicates stdout for framed protocol traffic, enables the exception-handling proposal programmatically, and keeps one module instance resident across requests.
- `flatsql_enable_demo_extractors` now avoids exception-driven probing for absent demo tables because that path faults under WasmEdge even though it works in browser WebAssembly.

---

## Findings From The SDK

The SDK pattern to copy is the browser/WasmEdge contract, not the exact module shape:

- `runtimeTargets: ["browser", "wasmedge"]` resolves to one portable single-thread artifact.
- The canonical artifact is a raw `.wasm` path such as `dist/isomorphic/module.wasm`; the browser wrapper is optional.
- Browser support is a host shim: WASI imports plus browser-native capability adapters for filesystem, fetch, WebSocket, crypto, timers, and host dispatch.
- WasmEdge support uses either raw command mode for `_start` artifacts or a persistent runner/runtime-host path for stateful modules.
- Stateful FlatSQL work needs the persistent path. A command-mode `_start` process is useful for smoke tests, but it loses query cache state between requests.

FlatSQL does not meet that contract yet:

Initial gaps that this plan closed:

- `wasm/flatsql-wasi.wasm` had 69 imports across `env` and `wasi_snapshot_preview1`; WasmEdge failed on `env.invoke_v`.
- `wasm/wasi.js` only loaded bytes. It did not instantiate the module, provide a WASI/browser shim, call `_initialize`, or wrap the C ABI.
- `test/wasi.test.ts` only checked the wasm magic number and package path.
- `src/artifacts/builder.ts` was a Node `node:sqlite` implementation with TypeScript result caching, not the standalone C++ artifact builder path.

## Task 1: Add Failing Standalone Import And WasmEdge Smoke Tests

**Files:**
- Create: `test/standalone-wasm-imports.test.ts`
- Create: `test/wasmedge-standalone.test.ts`
- Modify: `package.json`

- [ ] **Step 1: Assert the standalone import surface**

Add a test that compiles `wasm/flatsql-wasi.wasm` with `WebAssembly.compile` and fails if any import module other than `wasi_snapshot_preview1` is present.

Required exports:
- `memory`
- `_initialize`
- `malloc`
- `free`
- `flatsql_create_db`
- `flatsql_register_file_id`
- `flatsql_ingest`
- `flatsql_register_query_template`
- `flatsql_query_template`
- `flatsql_query_cache_hits`
- `flatsql_query_cache_misses`
- `flatsql_query_cache_generation`
- `flatsql_destroy_db`

- [ ] **Step 2: Add a WasmEdge smoke test**

Add `npm run test:wasmedge` and a test that:

- skips only when `wasmedge` is absent
- records `wasmedge --version`
- runs `wasmedge --reactor wasm/flatsql-wasi.wasm`
- fails on unknown imports

Expected current result: FAIL with `unknown import ... module: "env", function name: "invoke_v"`.

## Task 2: Produce A Real Standalone WASI Reactor

**Files:**
- Modify: `cpp/CMakeLists.txt`
- Modify: `scripts/build-wasm.sh`
- Optional create: `cpp/cmake/flatsql_wasm_exports.cmake`

- [ ] **Step 1: Split the standalone target from the Emscripten JS target**

Keep `flatsql.js` and `flatsql.wasm` for the existing Emscripten wrapper. Add or harden a dedicated standalone target for the shared browser/WasmEdge artifact.

The standalone target must:

- use `-s STANDALONE_WASM=1`
- use `--no-entry`
- export the C ABI used by `wasm/index.js`
- avoid JS exception/runtime imports such as `env.invoke_*`, `env.__cxa_*`, and `env.emscripten_*`
- preserve `_initialize` for reactor initialization
- keep `-s FILESYSTEM=0`

- [ ] **Step 2: Remove Emscripten JS exception imports from the standalone build**

First try native wasm exception handling on the standalone target:

```cmake
-fwasm-exceptions
-s DISABLE_EXCEPTION_CATCHING=0
```

If WasmEdge/browser support or Emscripten output still imports `env.*`, refactor the standalone C ABI to return status codes without catching C++ exceptions in the exported wrapper. The requirement is import cleanliness, not a specific flag.

- [ ] **Step 3: Generate integrity metadata for the standalone artifact**

Extend `scripts/build-wasm.sh` to write a separate integrity file for `wasm/flatsql-wasi.wasm`, for example `wasm/integrity-wasi.json`, using the same SHA-384 format as `wasm/integrity.json`.

- [ ] **Step 4: Re-run import and smoke tests**

Run:

```bash
npm run build:wasm
npm test -- --runInBand --runTestsByPath test/standalone-wasm-imports.test.ts
npm run test:wasmedge
```

Expected: no `env.*` imports and WasmEdge reactor instantiation succeeds.

## Task 3: Add The Small Browser/Node Standalone Shim

**Files:**
- Create: `wasm/standalone.js`
- Create: `wasm/standalone.d.ts`
- Optional create: `wasm/standalone-wasi-shim.js`
- Optional create: `wasm/marshal.js`
- Modify: `wasm/wasi.js`
- Modify: `package.json`

- [ ] **Step 1: Implement a minimal WASI/browser import object**

Cover the WASI calls FlatSQL actually imports, matching the SDK reference shim style:

- `clock_time_get`
- `fd_write`
- `fd_read`
- `fd_close`
- `fd_seek`
- `fd_fdstat_get`
- `fd_sync`
- `environ_sizes_get`
- `environ_get`
- `args_sizes_get`
- `args_get`
- `random_get` if the standalone artifact imports it later
- `proc_exit`

Use browser-native `performance.now`, `crypto.getRandomValues`, and buffered stdio. In Node, use the same shim unless a caller passes a custom import object.

- [ ] **Step 2: Instantiate the raw standalone artifact directly**

`wasm/standalone.js` should:

- resolve/load `flatsql-wasi.wasm`
- instantiate with the shim import object
- call `_initialize` once if exported
- expose memory-safe helpers for UTF-8 strings, byte arrays, `malloc`, and `free`
- wrap the C ABI directly without `cwrap`, `ccall`, or the generated Emscripten JS runtime

- [ ] **Step 3: Factor common marshalling**

Move query parameter encoding, query batch encoding, size-prefixed ingest stream building, and result decoding into a small reusable module if practical. The standalone shim and existing `wasm/index.js` should agree on the binary parameter format.

- [ ] **Step 4: Export a C++-backed runtime API**

Expose:

- `loadFlatSQLStandalone(options)`
- `getFlatSQLStandaloneWasmURL()`
- `FlatSQLStandalone`
- `FlatSQLStandaloneDatabase`

The database API should include the cache-centric methods:

- `registerFileId`
- `enableDemoExtractors`
- `ingest`
- `ingestBuffers`
- `registerQueryTemplate`
- `queryTemplate`
- `query`
- `queryMany`
- `buildQueryCacheKey`
- `clearQueryCache`
- `getQueryCacheStats`
- `getFlatBufferByIndex`
- `exportData`
- `loadAndRebuild`
- `destroy`

Keep `./wasi` backward compatible as a byte-loader export, but add `./standalone` for the runtime shim.

## Task 4: Build A Persistent WasmEdge Runner

**Files:**
- Create: `src/standalone/wasmedge-runner.ts`
- Create: `src/standalone/process-client.ts`
- Create: `src/standalone/native/flatsql_wasmedge_runner.c`
- Create: `src/standalone/index.ts`
- Create: `src/standalone/index.d.ts`
- Modify: `package.json`

- [ ] **Step 1: Add runner build planning**

Mirror the SDK builder shape:

- detect `WASMEDGE_INCLUDE_DIR`
- detect `WASMEDGE_LIB_DIR`
- support Homebrew defaults
- compile with `cc` or `xcrun clang`
- link `-lwasmedge`
- set rpath/install name on macOS

Export:

- `resolveFlatSQLWasmEdgeRunnerSourcePath`
- `resolveFlatSQLWasmEdgeRunnerBuildPlan`
- `buildFlatSQLWasmEdgeRunner`

- [ ] **Step 2: Implement a binary stdio protocol**

Use length-prefixed binary messages, not JSON/base64, so the runner can move FlatBuffer payloads without avoidable copies.

Core opcodes:

- `create_db`
- `destroy_db`
- `register_file_id`
- `enable_demo_extractors`
- `ingest_stream`
- `register_query_template`
- `query_template`
- `query`
- `query_many`
- `query_cache_stats`
- `clear_query_cache`
- `get_flatbuffer_by_index`
- `export_data`
- `load_and_rebuild`

- [ ] **Step 3: Keep the WasmEdge module instance resident**

The runner should instantiate the standalone module once, register WASI, call `_initialize`, and keep the module plus database handles alive across many protocol messages. This is required for high-volume identical query traffic to hit the C++ result cache.

- [ ] **Step 4: Wrap the runner in a JS process client**

The JS client should launch the runner, send binary protocol frames, expose a typed API, and cleanly terminate the child on `destroy()`.

## Task 5: Add The Standalone Artifact Builder API

**Files:**
- Create: `src/artifacts/standalone-builder.ts`
- Modify: `src/artifacts/index.ts`
- Modify: `src/index.ts`
- Modify: `package.json`

- [ ] **Step 1: Define the builder boundary**

The standalone artifact builder should be a facade over the same C++ C ABI in both environments:

- browser backend: `wasm/standalone.js`
- Node/WasmEdge backend: persistent WasmEdge runner process

It should not keep a separate TypeScript query-result cache. The C++ `FlatSQLDatabase` owns cache keys, invalidation, hit/miss counters, and result storage.

- [ ] **Step 2: Preserve source-level compatibility**

Expose a creation function with explicit runtime selection:

```ts
createStandaloneArtifactBuilder(schema, {
  runtime: "browser" | "wasmedge",
  wasmPath,
  wasmEdgeRunnerBinary,
  dbName,
});
```

The default in Node should be WasmEdge when the runner is available. The existing `FlatSQLArtifactBuilder` can remain as a legacy/direct Node implementation until parity is proven.

- [ ] **Step 3: Add cache-oriented tests**

Tests must prove:

- registering `FILE_ID` routes records to the correct table
- two identical `queryTemplate` calls produce one miss then one hit
- parameters are part of the cache key
- ingestion increments the cache generation and invalidates stale results
- raw FlatBuffer retrieval works after cached query execution
- browser shim and WasmEdge runner return the same result rows for the same artifact

## Task 6: Update Docs, Package Exports, And Examples

**Files:**
- Create: `docs/browser-wasmedge-standalone.md`
- Create: `examples/standalone-browser/`
- Create: `examples/standalone-wasmedge/`
- Modify: `README.md`
- Modify: `package.json`

- [ ] **Step 1: Document the runtime split**

Document:

- `./wasm`: legacy Emscripten wrapper
- `./wasi`: raw bytes and URL compatibility helper
- `./standalone`: browser/Node standalone runtime shim
- `./artifacts/standalone`: C++-backed artifact builder

- [ ] **Step 2: Document the shim boundary**

Be explicit that:

- cache and SQL execution live in C++
- the JS shim only supplies WASI/browser capability differences and marshals memory
- WasmEdge uses a resident runner for stateful cache behavior
- command-mode process-per-query execution is not the performance path

- [ ] **Step 3: Add examples**

Add one browser example and one WasmEdge example that use the same `wasm/flatsql-wasi.wasm` artifact and demonstrate two identical cached `queryTemplate` calls.

## Task 7: Verification

**Files:**
- Test: all changed tests and build scripts

- [ ] **Step 1: Run standalone-focused verification**

```bash
npm run build:wasm
npm test -- --runInBand --runTestsByPath test/standalone-wasm-imports.test.ts test/wasi.test.ts
npm run test:wasmedge
```

- [ ] **Step 2: Run existing runtime suites**

```bash
npm test -- --runInBand --runTestsByPath test/wasm-query-params.test.ts test/remote-artifact.test.ts
npm run test:wasm
```

- [ ] **Step 3: Run native C++ verification**

```bash
cmake --build cpp/build --target flatsql_test -j4
./cpp/build/flatsql_test
ctest --test-dir cpp/build --output-on-failure
```

- [ ] **Step 4: Run package build**

```bash
npm run build
git diff --check
```

Expected final state: the standalone wasm artifact has no `env.*` imports, WasmEdge can instantiate it as a reactor, browser and WasmEdge paths both use the C++ cache, and the JavaScript layer is reduced to shimming host capabilities and moving bytes.
