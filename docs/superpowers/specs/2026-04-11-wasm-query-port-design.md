# WASM Query Port Design

## Goal

Port the parameterized-query and batched-query speedups into the portable FlatSQL WASM core so they run through the existing native SQLite engine rather than the Node-only artifact path.

This subproject is intentionally limited to the portable core runtime. The future WasmEdge file/socket/network harness will live in a separate artifact that imports this core once the core API and semantics are stable.

## Scope

This design covers subproject A only:

- expose parameterized queries through the WASM C API
- expose batched query execution through the WASM C API
- surface both capabilities in the JS/TS WASM wrapper
- preserve SQLite semantics by keeping execution inside the native engine
- add verification focused on statement-cache correctness and SQLite compatibility

This design does not cover:

- the Node artifact builder and `node:sqlite` runtime
- a WasmEdge host harness
- file/socket/network adapters
- remote page fetch or pager/VFS work
- artifact persistence in the WASM runtime

## Current State

The current branch already has the critical native pieces in the C++ core:

- `FlatSQLDatabase::query(const std::string&, const std::vector<Value>&)` in [cpp/include/flatsql/database.h](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/cpp/include/flatsql/database.h)
- `SQLiteEngine::execute(const std::string&, const std::vector<Value>&)` in [cpp/include/flatsql/sqlite_engine.h](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/cpp/include/flatsql/sqlite_engine.h)
- prepared-statement caching and parameter binding in [cpp/src/sqlite_engine.cpp](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/cpp/src/sqlite_engine.cpp)

The missing layer is exposure through the C/WASM boundary and the JS wrapper:

- [cpp/src/flatsql_capi.cpp](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/cpp/src/flatsql_capi.cpp) only exports `flatsql_query(void*, const char*)`
- [wasm/index.js](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/wasm/index.js) only surfaces `db.query(sql)`
- [wasm/index.d.ts](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/wasm/index.d.ts) only types `query(sql: string): QueryResult`

The Node artifact speedups on this branch are separate and not portable as implemented because they depend on:

- `node:sqlite`
- `node:worker_threads`
- direct file-backed `sqlitePath` access

## Architecture

### 1. Keep SQLite Semantics in Native Code

The portable query speedups should live in the existing C++ SQLite engine, not in JS glue.

Rules:

- SQL parsing, statement preparation, parameter binding, and execution stay in `SQLiteEngine`
- the C API only marshals parameters and exposes native results
- the JS wrapper only encodes parameters and decodes result rows
- no second JS-side SQL cache or query semantics layer is introduced for the WASM path

This keeps behavior anchored to SQLite itself and avoids a repeat of the current Node-only artifact path, where performance logic sits outside the portable core.

### 2. Thin C API Expansion

Add new C exports beside the current `flatsql_query`:

- parameterized single-query execution
- batched query execution
- batch result selection accessors if batching cannot safely reuse the current global result slot

The C API should remain intentionally simple:

- encode parameter values into a flat buffer or parallel arrays
- decode them into `std::vector<Value>`
- call the existing `FlatSQLDatabase::query(sql, params)`
- store results in native-owned buffers that JS can read back

The exported API should favor a stable ABI over elegance. We do not need an embind-heavy interface here.

### 3. JS/TS Wrapper Extension

Extend the WASM wrapper with:

- `db.query(sql, params?)`
- `db.queryMany([{ sql, params }...])`

Compatibility rules:

- existing `db.query(sql)` callers keep working unchanged
- result shape remains `{ columns, rows }`
- `queryMany` returns `QueryResult[]` in the same order as the input requests

The wrapper should not interpret SQL. It should only:

- marshal parameter values into native memory
- invoke the new exports
- materialize results from native accessors

### 4. Separate Future Harness Artifact

The WasmEdge harness is explicitly out of scope for this spec, but this design reserves for it:

- a portable WASM query API with parameter binding and batching
- no Node runtime assumptions in the public core API
- clean separation between core runtime and host-specific I/O

That harness should be a separate artifact that imports the original FlatSQL WASM core and provides host capabilities for file, socket, and network streaming.

## API Design

### WASM JS API

```ts
interface FlatSQLDatabase {
  query(sql: string, params?: QueryParam[]): QueryResult;
  queryMany(queries: readonly { sql: string; params?: QueryParam[] }[]): QueryResult[];
}
```

`QueryParam` in v1 supports the native `Value` subset that is already stably represented across the JS/WASM boundary:

- `null`
- `boolean`
- `number`
- `string`
- `Uint8Array`

`bigint` is explicitly out of scope for v1. It can be added later once the JS/WASM round-trip is tested end to end.

Named parameters are out of scope for this first pass. Positional parameters are sufficient and align with the current fast-path parsing and test targets.

### C API

Add exports equivalent to:

- `flatsql_query_params(handle, sql, param_buffer, param_count)`
- `flatsql_query_many(handle, request_buffer, request_count)`

Add support functions for:

- decoding parameter blobs
- selecting a batch result by index
- reading column/row counts from the selected result

The first implementation keeps batch results in a native vector of `QueryResult` objects with an explicit selected-result index for accessors. That is simpler and safer than overloading the existing single-result global slot.

## Execution Flow

### Single Query

1. JS receives `query(sql, params?)`
2. JS encodes `params` into a WASM-compatible payload
3. C API decodes payload into `std::vector<Value>`
4. `FlatSQLDatabase::query(sql, params)` calls `SQLiteEngine::execute(sql, params)`
5. `SQLiteEngine` reuses or prepares the statement, binds parameters, executes, and materializes rows
6. JS reads the result back through native accessors and returns `{ columns, rows }`

### Batched Query

1. JS receives `queryMany([...])`
2. JS encodes each `{ sql, params }` request into a batch payload
3. C API iterates requests and executes each through `FlatSQLDatabase::query(sql, params)`
4. Native code stores `QueryResult` objects in batch order
5. JS reads each result back and returns `QueryResult[]`

The first batch implementation does not need to guarantee shared-statement reuse across different SQL strings inside the same batch. Statement reuse already happens naturally for repeated SQL via the native `stmtCache_`.

## Critical Correctness Fixes

This subproject includes a critical audit of statement-cache reuse.

### Statement Bindings Must Be Cleared on Reuse

Current code in [cpp/src/sqlite_engine.cpp](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/cpp/src/sqlite_engine.cpp) resets cached statements before reuse, but this design requires explicit verification that old parameter bindings cannot leak into later executions.

Required behavior:

- `sqlite3_reset(stmt)` before reuse
- `sqlite3_clear_bindings(stmt)` before rebinding a new parameter set
- correct behavior when a later call binds fewer parameters than an earlier call
- correct behavior after execution errors

This is the main upstream-SQLite compatibility risk in the current path.

### Result Buffer Isolation

Batch execution must not let one result overwrite another before JS has read it.

Required behavior:

- each batch entry owns an independent native `QueryResult`
- result accessors select a specific batch result
- clearing a batch invalidates all of its results explicitly

### Type Handling

Tests must explicitly verify behavior for:

- `NULL`
- integer values
- floating-point values
- text
- blobs

`bigint` remains deferred from the public wrapper for v1 and is documented as unsupported in this pass.

## Error Handling

The port preserves current error semantics:

- SQL preparation errors surface the native SQLite message
- parameter decoding errors surface a FlatSQL C API error
- unsupported JS parameter types are rejected before execution
- batch execution should fail the whole call on the first invalid request in v1

This is intentionally strict. Partial-success batch semantics are unnecessary for subproject A.

## Testing Strategy

SQLite compatibility is a deliverable, not an assumption.

### 1. Native Statement-Reuse Tests

Add tests that prove:

- repeated parameterized queries with changing values return the correct rows
- cached statements do not retain stale bindings
- mixed-type parameters across repeated executions behave correctly
- execution after prior errors still works with the same cached SQL

### 2. WASM API Tests

Add tests in the WASM suite for:

- `query(sql, params?)` equivalence with literal-SQL `query(sql)` for the same logical lookup
- `queryMany` result ordering
- `queryMany` result isolation
- repeated batched calls with different values
- blob and `NULL` parameter/result round-trips where supported

### 3. Reference Semantic Tests

For SQL shapes that map cleanly to both environments, compare:

- result columns
- row counts
- row values
- explicit `ORDER BY` output ordering

Reference comparison uses direct SQLite semantics as the oracle where feasible, rather than comparing against JS wrapper expectations alone.

### 4. Performance Benchmarks

Benchmark the actual WASM path for:

- varying-key point lookups using literal SQL
- varying-key point lookups using parameterized SQL
- varying-key point lookups using `queryMany`

The benchmark target is improvement in the portable runtime, not parity with the Node artifact worker.

## Implementation Boundaries

Files expected to change for subproject A:

- [cpp/src/flatsql_capi.cpp](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/cpp/src/flatsql_capi.cpp)
- [cpp/src/sqlite_engine.cpp](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/cpp/src/sqlite_engine.cpp)
- [cpp/include/flatsql/sqlite_engine.h](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/cpp/include/flatsql/sqlite_engine.h)
- [wasm/index.js](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/wasm/index.js)
- [wasm/index.d.ts](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/wasm/index.d.ts)
- WASM query/integration tests

Files explicitly out of scope for this subproject:

- [src/artifacts/builder.ts](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/src/artifacts/builder.ts)
- [src/artifacts/worker-client.ts](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/src/artifacts/worker-client.ts)
- [wasm/flatsql-artifact.worker.js](/Users/tj/.config/superpowers/worktrees/flatsql/codex-remote-artifact-worker/wasm/flatsql-artifact.worker.js)

## Follow-On Work

Once subproject A is complete and validated, subproject B should define a separate WasmEdge-oriented harness artifact that:

- imports the original FlatSQL WASM core
- provides file/socket/network streaming APIs through host capabilities
- exposes a deployable Docker+WasmEdge example
- keeps host I/O policy outside the FlatSQL core package

That harness should consume the new `query(sql, params?)` and `queryMany(...)` surface rather than changing core query semantics again.
