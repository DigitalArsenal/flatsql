# Remote Artifact Worker Design

## Goal

Add a generic, worker-backed artifact build path to FlatSQL that can stream FlatBuffers through a dedicated worker and produce file-backed SQLite index artifacts without hardcoding Space Data Network, IPFS, or schema-specific policy into the core project.

## Scope

This v1 is intentionally narrow:

- Keep current FlatSQL default behavior unchanged.
- Add an explicit artifact-build path that writes SQLite index artifacts to a caller-provided path.
- Run artifact build work in a dedicated worker thread when the runtime supports workers.
- Prefer `SharedArrayBuffer` transport when available, with structured-clone fallback.
- Prove the path with tests using the existing demo schemas and extractors.

## Non-Goals

This branch does not implement:

- IPFS access, manifests, shard policies, or Space Data Network conventions
- Remote query federation across many artifacts
- A custom SQLite pager, VFS, or `sql.js-httpvfs`-style remote page fetch layer
- Full elimination of transient in-process FlatBuffer bytes during artifact build

## Design

### 1. File-Backed SQLite Runtime Option

Expose a new runtime option from the JS/WASM API down to the C API so callers can create a FlatSQL database with a file-backed SQLite index database instead of the default `:memory:` connection.

Requirements:

- The option must be optional and preserve current behavior when omitted.
- Existing query/index behavior must continue to work for in-memory mode.
- Artifact builders will use this option to persist SQLite index pages to disk.

### 2. Artifact Builder API

Add a new high-level API beside the existing FlatSQL database API:

- `FlatSQL.createArtifactBuilder(...)`
- `FlatSQLArtifactBuilder`

Responsibilities:

- create a database with a file-backed SQLite connection
- register file IDs and demo extractors
- ingest FlatBuffers in batches
- expose build/query helpers for artifact validation
- avoid exposing FlatSQL storage export as part of the artifact workflow

This keeps the artifact mode generic and opt-in rather than changing normal database usage.

### 3. Worker-Backed Execution

Add a dedicated artifact worker protocol and client:

- `FlatSQLArtifactWorkerClient`
- `wasm/flatsql-artifact.worker.js`

Responsibilities:

- create and own artifact-builder handles in the worker
- accept batch ingest requests
- return transport mode and build results
- keep SQLite work off the main thread

Transport:

- Prefer `SharedArrayBuffer` when `SharedArrayBuffer` and `Atomics` are available.
- Fallback to structured-clone transfer in unsupported runtimes.
- v1 may use a single shared batch buffer per request rather than a lock-free ring buffer.

### 4. Artifact Contract

The generated artifact is the SQLite file at the provided `sqlitePath`.

v1 guarantees:

- FlatSQL index tables such as `_idx_<table>_<column>` are persisted.
- Those tables can be reopened in a later process using the same schema and `sqlitePath`.
- Indexed lookups remain queryable through SQLite SQL against the artifact file.

v1 does not yet guarantee that the artifact alone is sufficient for full virtual-table queries without the source FlatBuffers.

## Data Flow

1. Caller creates an artifact builder with schema and `sqlitePath`.
2. Client chooses worker transport:
   - `shared-array-buffer` when supported
   - `clone` otherwise
3. Caller streams FlatBuffer batches to the worker.
4. Worker ingests batches into a file-backed FlatSQL database.
5. SQLite persists index pages to the artifact file while FlatSQL keeps its existing in-process behavior unchanged.
6. Tests reopen the artifact file and validate index contents.

## Error Handling

- Missing worker support falls back to an inline or clone-based worker transport error path rather than silently changing semantics.
- Invalid `sqlitePath` surfaces the native SQLite open error.
- Worker failures are propagated back to the caller with method context.
- Shared-buffer writes validate payload size before dispatch.

## Testing

Add coverage for:

- creating a file-backed SQLite FlatSQL database from the WASM API
- persisting index tables across destroy/reopen cycles
- worker artifact build using structured-clone transport
- worker artifact build using `SharedArrayBuffer` transport when available in Node

## Follow-On Work

- external locator-aware artifact rows for remote source reconstruction
- remote fetch abstractions for local file, HTTP range, and IPFS readers
- query federation across multiple artifacts
- optional WASM pthread builds where the runtime and build pipeline support them
