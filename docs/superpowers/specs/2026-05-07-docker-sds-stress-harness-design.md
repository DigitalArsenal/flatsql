# Docker SDS Stress Harness Design

## Goal

Add a Docker-based FlatSQL stress harness that exercises Space Data Standards schemas at operational scale and exposes where FlatSQL is stronger or weaker than a centralized data API model such as Unified Data Library.

The harness must measure storage, query latency, query-cache behavior, and transport bandwidth for:

- streaming FlatBuffer ingest
- raw FlatBuffer retrieval
- raw SQL queries
- parameterized SQL queries
- cacheable query-template calls
- repeated `FILE_ID` lookup traffic through PNM and DPM-style contracts

## Scale Target

The first full target is:

- 100 FlatSQL nodes
- 1 GB storage budget per node
- millions of records across the fleet
- all discoverable Space Data Standards FlatBuffer schemas from `../spacedatastandards.org/schema/*/main.fbs`

The harness also needs a local smoke mode that proves the same code path with a much smaller node count, record count, and storage budget.

## Scope

This project adds a stress-testing system, not a production distributed query fabric.

In scope:

- Docker image and orchestration scripts for repeatable stress runs.
- Workload generation from local SDS FlatBuffer schemas.
- Per-node data directories and storage budget checks.
- Per-use-case metrics for records, bytes, latency, cache hits, cache misses, and failures.
- Transport accounting for request bytes, response bytes, ingest bytes, and raw FlatBuffer bytes.
- Smoke-mode tests that run without requiring 100 GB of local free space.
- Full-run configuration for 100 nodes at 1 GB/node.

Out of scope for v1:

- Real UDL API integration.
- Kubernetes manifests.
- Cross-host service discovery.
- Production auth, entitlement, billing, or policy enforcement.
- A custom network protocol beyond the test harness transport.
- Real-world SDS data acquisition.

## Current Context

FlatSQL already has several pieces the harness should reuse:

- C++ WAL-backed concurrency validation in `cpp/test/cluster_validation_test.cpp`.
- Native C++ query result caching through `registerQueryTemplate`, `queryTemplate`, and `getQueryCacheStats`.
- Standalone browser and WasmEdge entry points over the same C++ runtime.
- SDS schema coverage available locally under `../spacedatastandards.org/schema`.
- PNM and DPM schemas that define stable `FILE_ID` semantics suitable for hot query-cache testing.

Current gaps:

- No Docker stress harness exists in FlatSQL.
- The existing C++ cluster validation only uses the local `User` test schema.
- Most SDS schemas do not mark fields with FlatSQL `indexed`, `key`, or `id` metadata, so v1 needs a generic workload plus explicit PNM/DPM `FILE_ID` workloads.
- The native result cache has fixed limits of 1024 entries and 1000 rows per cached result. The stress harness should measure when those limits help and when they become a bottleneck.

## Architecture

### 1. Harness Layout

Add a dedicated stress area:

- `stress/docker/Dockerfile`
- `stress/docker/docker-compose.yml`
- `stress/sds/`
- `stress/use-cases/`
- `scripts/stress-docker.sh`

The shell script is the main entry point. It chooses smoke or full mode, validates paths and storage budget, builds the Docker image, starts the run, and collects output artifacts.

### 2. Controller

The controller discovers schemas, creates a workload manifest, partitions work across nodes, and aggregates results.

Responsibilities:

- discover SDS schemas from `SDS_SCHEMA_ROOT`
- parse root type and `file_identifier`
- record schema parse/build failures
- generate node assignments
- produce a run manifest with mode, node count, storage budget, seed, and use-case mix
- aggregate worker NDJSON metrics into a final summary

The controller should not contain FlatSQL query semantics. It coordinates work and metrics only.

### 3. Worker

Each worker represents one FlatSQL node in full mode. In smoke mode, a worker may run multiple logical nodes to keep local testing cheap.

Responsibilities:

- create one node data directory
- enforce or report the configured storage budget
- initialize FlatSQL with the assigned schema set
- ingest generated FlatBuffers
- execute use-case workloads
- emit structured metrics
- exit non-zero on correctness failures

The worker should prefer the standalone C++ runtime path so the browser shim and WasmEdge path remain small deployment details instead of performance logic.

### 4. Metrics

Every use case emits NDJSON events with at least:

- `run_id`
- `node_id`
- `schema`
- `use_case`
- `operation`
- `records`
- `request_bytes`
- `response_bytes`
- `flatbuffer_bytes`
- `storage_bytes`
- `duration_ms`
- `latency_p50_ms`
- `latency_p95_ms`
- `latency_p99_ms`
- `cache_hits`
- `cache_misses`
- `cache_size`
- `errors`

The reducer computes fleet totals and per-use-case percentiles.

### 5. Transport Accounting

Transport accounting is a first-class requirement.

For each operation, the harness should track:

- bytes sent to stream FlatBuffers into a node
- bytes read back as raw FlatBuffers
- bytes sent as SQL or query-template requests
- bytes returned as query result rows
- cache hit ratio for repeated query templates
- response compression ratio when a run explicitly enables compression

This lets us separate compute wins from transport wins. A node-local cache hit avoids SQLite work but still returns response bytes to the caller. Raw FlatBuffer retrieval, projection queries, batching, compression, and controller-side aggregation are the levers that can reduce bytes moved over the harness transport.

## Use Cases

### 1. SDS Schema Sweep

Load every SDS `main.fbs`, parse root type and `file_identifier`, and attempt a minimal generated-record ingest.

Purpose:

- find schema parser gaps
- identify schemas that need generation adapters
- measure broad compatibility instead of cherry-picked formats

### 2. Bulk Streaming Ingest

Stream millions of generated FlatBuffers into nodes in size-prefixed batches.

Metrics:

- ingest bytes/sec
- records/sec
- per-batch latency
- WAL growth
- final storage bytes
- ingest failures by schema

### 3. Hot `FILE_ID` Query Cache

Repeatedly issue the same cacheable query-template call for one PNM/DPM-style `FILE_ID`.

Purpose:

- model 2M clients requesting the same query
- validate that identical query ID and params hit the C++ result cache
- measure cache-driven compute reduction while still accounting for repeated response bytes

### 4. Cold `FILE_ID` Fanout

Issue mostly unique `FILE_ID` lookups across nodes.

Purpose:

- measure cache miss cost
- expose cache-entry churn
- find when the 1024-entry cache cap becomes too small

### 5. Mixed Hot/Cold Query Distribution

Run a Zipf-like workload where a small number of `FILE_ID`s are very hot and the rest are long-tail.

Purpose:

- approximate public API traffic
- measure whether cache hit rate stays high under realistic skew
- compare raw query latency against cache-template latency

### 6. Raw FlatBuffer Retrieval

Query an index, retrieve `_data`, and return the original FlatBuffer bytes.

Purpose:

- measure raw binary transport
- verify FlatBuffer validity after retrieval
- compare raw byte retrieval with row-shaped SQL results

### 7. SQL Projection Query

Run parameterized SQL that returns a small projection rather than the whole FlatBuffer.

Purpose:

- measure when SQL projection saves bandwidth
- compare projected rows against raw FlatBuffer transport

### 8. Raw SQL Query

Run untemplated SQL calls directly through the raw query path.

Purpose:

- compare raw SQL overhead against registered query-template calls
- measure request bytes for full SQL strings versus compact query IDs
- expose cases where prepared/template paths are required for high-volume traffic

### 9. Large Result Query

Run queries that intentionally return more than 1000 rows.

Purpose:

- verify large results are not incorrectly cached
- measure bandwidth and latency for bulk result transfer
- expose need for paging or streaming result cursors

### 10. Node Fanout Query

Query the same logical request across many nodes and aggregate results.

Purpose:

- approximate distributed data discovery
- measure fanout request bytes, response bytes, and tail latency
- identify slow-node amplification

### 11. Backfill Sync

Start a node empty, then stream records from another node or from generated artifacts until it catches up.

Purpose:

- measure node bootstrap bandwidth
- measure rebuild/index time
- identify storage and WAL behavior during catch-up

### 12. Restart and Rebuild

Stop a worker, restart it against the same data directory, and verify it can reload or rebuild indexes.

Purpose:

- test operational recovery
- measure cold-start time
- catch persistence gaps

### 13. Cache Invalidation Churn

Alternate repeated hot queries with ingest batches that invalidate the cache generation.

Purpose:

- measure how often writes destroy cache value
- validate no stale data is returned
- identify whether cache invalidation is too broad

### 14. Bandwidth-Constrained Streaming

Throttle the worker network or inject chunked transport limits.

Purpose:

- measure streaming FlatBuffer behavior under constrained links
- identify optimal batch sizes
- compare binary ingest with query-response transfer cost

### 15. SDS Domain Query Pack

Define named workloads around common space-data access patterns:

- latest object announcement by `FILE_ID`
- DPM manifest lookup by `FILE_ID`
- satellite catalog object lookup
- position or ephemeris retrieval
- conjunction or event message lookup
- sensor or communication channel lookup
- space weather slice query
- telemetry packet lookup

Purpose:

- move beyond synthetic CRUD
- show where FlatSQL can serve operational space data with lower transport and cache overhead

### 16. UDL-Style API Comparison Shape

Without calling UDL, run API-shaped workloads that centralized systems usually serve:

- single object by stable ID
- latest records for an object
- time-window query
- catalog-wide filtered scan
- binary payload fetch
- repeated public hot query
- multi-tenant node fanout

Purpose:

- produce metrics that can be compared against centralized request/response systems
- show whether FlatSQL's local binary storage, raw retrieval, and cacheable templates reduce latency or bandwidth

## Configuration

The script should support environment variables:

- `STRESS_MODE=smoke|full`
- `NODE_COUNT=100`
- `NODE_STORAGE_GB=1`
- `SDS_SCHEMA_ROOT=../spacedatastandards.org/schema`
- `RUN_DURATION_SECONDS`
- `RECORDS_PER_NODE`
- `BATCH_BYTES`
- `QUERY_CONCURRENCY`
- `HOT_QUERY_RATIO`
- `OUTPUT_DIR`
- `RUNTIME=standalone|wasmedge|native`

Defaults should be safe for smoke mode. Full mode must require explicit opt-in.

## Docker Model

Smoke mode:

- one controller container
- one worker container running a small number of logical nodes
- small storage budget
- short duration

Full mode:

- one controller container
- 100 worker containers
- one mounted data directory per node
- 1 GB storage budget per node
- explicit preflight for at least 100 GB available plus overhead

Docker Compose can drive the local smoke mode. Full 100-container mode can use Compose profiles or generated Compose configuration, but the script should fail clearly if the host cannot satisfy storage or Docker daemon requirements.

## Failure Detection

A run fails when:

- any worker exits non-zero
- any FlatBuffer verification fails
- a query returns the wrong record
- a hot query fails to produce cache hits after warm-up
- storage budget is exceeded
- schema discovery finds zero schemas
- metrics cannot be reduced into a summary

The final report should call out weaknesses rather than only pass/fail:

- slowest schema
- highest bandwidth use case
- lowest cache hit rate
- largest WAL growth
- worst p99 latency
- schemas skipped or unsupported
- bottlenecked node

## Testing

Add test coverage in layers:

1. Unit test schema discovery against the local SDS tree or a fixture tree.
2. Unit test use-case manifest generation.
3. Unit test metrics reducer with fixture NDJSON.
4. Smoke test the worker without Docker.
5. Docker smoke test with a tiny node count and small record count when Docker is available.

The full 100-node run is not a normal CI test. It is an explicit stress command.

## Expected First Weaknesses To Expose

The harness is expected to reveal at least these pressure points:

- cache capacity too small for broad cold `FILE_ID` traffic
- full-cache invalidation after ingest reducing value during high-churn updates
- schemas without indexed fields limiting realistic SQL lookup speed
- raw result transfer becoming bandwidth-heavy for large payloads
- large SQL result sets needing paging or streaming
- WAL contention or checkpoint behavior under concurrent ingest/query load
- schema generation gaps for nested SDS records

## Approval Criteria

The design is ready to implement when it is acceptable that v1:

- uses generated SDS records rather than real production SDS feeds
- treats UDL as a comparison workload shape, not an external dependency
- supports 100 actual Docker worker containers for full mode
- keeps smoke mode cheap and deterministic
- makes transport metrics part of every use case
