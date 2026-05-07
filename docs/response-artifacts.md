# Response Artifacts

FlatSQL reduces response bandwidth by producing cacheable response artifacts. The library owns deterministic byte output, cache keys, hashes, ETags, and chunk metadata. Deployment infrastructure owns CDN placement, global invalidation, routing, authorization, and quotas.

## Boundary

FlatSQL should do:

- build deterministic cache keys in the C++/WASM core from query shape, schema version, format, projection, params, and publish-event key
- encode raw FlatBuffer query results as size-prefixed byte streams
- encode JSON results only when callers explicitly request JSON
- produce content hashes and ETags for immutable artifacts
- split artifacts into range-addressable chunks
- expose small process-local caches for repeated query artifacts

FlatSQL should not do:

- add database-specific annotations to canonical SDS schemas
- implement a CDN or global cache-invalidation service
- decide authorization or tenant-specific cache policy
- route users to regions or nodes
- hide response byte cost behind a query-cache hit counter

## Recommended FILE_ID Flow

For Space Data Network publish events, `FILE_ID` is a network policy key, not a schema requirement.

1. Ingest records for a publish event.
2. Apply `FILE_ID` indexes through runtime policy or SDN index profiles.
3. Query the publish event with a stable projection and format.
4. Create a response artifact.
5. Serve the artifact by `ETag` or content hash from edge, browser, WasmEdge host storage, or a CDN.
6. Recompute only when the publish event, schema version, projection, or format changes.

```ts
import {
  createQueryResponseArtifact,
  MemoryResponseArtifactCache,
} from 'flatsql/response';
import initFlatSQL from 'flatsql/wasm';

const flatsql = await initFlatSQL();
const sql = "SELECT _data FROM PublishEventRecord WHERE FILE_ID = ?";
const cacheKey = flatsql.buildResponseArtifactCacheKey('PublishEventRecord', '1', sql, {
  format: 'raw-flatbuffer-stream',
  publishEventKey: 'publish-1',
  projection: ['_data'],
  params: ['publish-1'],
});
const result = db.query(sql);
const cache = new MemoryResponseArtifactCache();

const artifact = cache.getOrCreateByKey(cacheKey, () => createQueryResponseArtifact(result, {
  schemaName: 'PublishEventRecord',
  schemaVersion: 1,
  sql,
  params: ['publish-1'],
  format: 'raw-flatbuffer-stream',
  publishEventKey: 'publish-1',
  projection: ['_data'],
  cacheKey,
  chunkBytes: 1024 * 1024,
}));

console.log(cacheKey);
console.log(artifact.metadata.etag);
console.log(artifact.metadata.chunks);
```

## Production Checks

Before relying on response artifacts in production:

- verify `cacheKey` includes every dimension that can change bytes
- verify browser and WasmEdge runtimes produce the same native response cache key
- prefer `raw-flatbuffer-stream` for high-volume SDS responses
- verify `responseBytes` and `flatbufferBytes`, not only query latency
- tune native `configureQueryCache({ maxEntries, maxRows })` limits for hot FILE_ID query templates
- set chunk sizes based on edge/cache/range-request behavior
- cap in-process caches with `maxEntries` and `maxBytes`
- keep authorization outside shared immutable artifacts unless all callers are allowed to read identical bytes
- test hot `FILE_ID` traffic separately from cold fanout

## Runtime Notes

Browser deployments should call the native `buildResponseArtifactCacheKey(...)` binding through `flatsql/wasm` or `flatsql/standalone`, then map artifact metadata to HTTP cache headers and the browser Cache API where appropriate. WasmEdge deployments should call the same binding through `flatsql/artifacts/standalone` and map artifacts to host filesystem or edge-local object cache. FlatSQL response artifacts are intentionally plain bytes plus metadata so either runtime can use the same cache key and ETag semantics.
