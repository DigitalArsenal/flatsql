# Agent Guide: Response Bandwidth

Use this guide when modifying FlatSQL response caching, SDS stress tests, or Space Data Network FILE_ID behavior.

## Rules

- Do not add FlatSQL-specific fields or annotations to canonical SDS schemas.
- Treat `FILE_ID` as a Space Data Network publish-event policy key.
- Use runtime indexes or external SDN index profiles for lookup speed.
- Measure response bytes separately from query latency and cache hits.
- Prefer raw FlatBuffer artifacts for repeated high-volume responses.
- Tune native query-template cache limits with `configureQueryCache({ maxEntries, maxRows })` when hot FILE_ID traffic needs more than the defaults.

## Library Responsibilities

FlatSQL response bandwidth work belongs in these primitives:

- native `buildResponseArtifactCacheKey(...)` bindings for deterministic query/result cache keys
- `createQueryResponseArtifact(...)` for immutable byte artifacts
- `getResponseArtifactChunk(...)` for range-addressable chunk reads
- `MemoryResponseArtifactCache.getOrCreateByKey(...)` for bounded process-local artifact reuse with native keys

Deployment code is responsible for CDN, browser Cache API, WasmEdge host filesystem cache, auth, quotas, routing, and regional replication.

## Required Tests

When changing response artifacts, run:

```bash
npm test -- --runInBand --runTestsByPath test/response-artifact.test.ts
npm test -- --runInBand --runTestsByPath test/wasm-query-params.test.ts test/standalone-shim.test.ts test/standalone-artifact-builder.test.ts test/wasmedge-runner.test.ts
npm test -- --runInBand --runTestsByPath test/stress-harness.test.ts
npm run build
```

Before claiming production readiness, also run:

```bash
npm test -- --runInBand
npm run stress:sds:full-config -- --run-id sds-full-config-check
docker compose -f stress/results/sds-full-config-check/docker-compose.full.yml config
git diff --check
```

## SDS Stress Interpretation

The SDS harness reports measured events. A cache hit is not enough. If `responseBytes` remains high, repeated clients are still consuming bandwidth unless the response artifact is served from edge/browser/WasmEdge host cache.

For hot publish events, look at:

- `hot-file-id-cache.responseBytes`
- `raw-flatbuffer-retrieval.responseBytes`
- `sql-projection-query.responseBytes`
- `bulk-streaming-ingest.requestBytes`
- aggregate `ingestRecordsPerSecond` and `minNodeIngestRecordsPerSecond`; every node must stay at or above 75 fps for production readiness
- `bandwidth-constrained-streaming.requestBytes`
- per-node `storageBytes` and storage fill percentage; every node must reach at least 95% of its configured storage target

## Common Mistakes

- Calling a run successful because query latency improved while response bytes remain unchanged.
- Requiring fields like `file_identifier`, `indexed`, `key`, or `id` in SDS schemas.
- Returning JSON for large repeated query results when raw FlatBuffer bytes are acceptable.
- Building a process-local cache and assuming it solves 2M-user egress.
- Omitting schema version, projection, format, or publish-event key from a cache key.
