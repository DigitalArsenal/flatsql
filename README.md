# FlatSQL

[![CI](https://github.com/DigitalArsenal/flatsql/actions/workflows/ci.yml/badge.svg)](https://github.com/DigitalArsenal/flatsql/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/flatsql.svg)](https://www.npmjs.com/package/flatsql)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0)

**SQL queries over raw FlatBuffer storage** — A streaming query engine that keeps data in native FlatBuffer format while providing SQL access via SQLite virtual tables.

## Live Demo

Try FlatSQL in your browser: **[https://digitalarsenal.github.io/flatsql/](https://digitalarsenal.github.io/flatsql/)**

## Installation

```bash
npm install flatsql
```

## Quick Start

```javascript
import { initFlatSQL } from 'flatsql/wasm';

// Initialize
const flatsql = await initFlatSQL();

// Create database with schema
const db = flatsql.createDatabase(`
  table User {
    id: int (id);
    name: string;
    email: string (key);
    age: int;
  }
`, 'myapp');

// Register file identifier for routing
db.registerFileId('USER', 'User');

// Ingest FlatBuffer data (streaming)
db.ingest(flatbufferStream);

// Query with SQL
const result = db.query('SELECT * FROM User WHERE age > 25');
console.log(result.columns, result.rows);
```

## What is FlatSQL?

FlatSQL bridges two technologies:

- **[FlatBuffers](https://github.com/digitalarsenal/flatbuffers)** — Google's efficient cross-platform serialization library. Data is stored in binary format with zero-copy access (no parsing/unpacking needed).
- **SQLite** — The most widely deployed SQL database engine, used here only for SQL parsing and query execution.

The key insight: instead of converting FlatBuffers to SQLite rows (expensive), FlatSQL uses [SQLite virtual tables](https://sqlite.org/vtab.html) to query FlatBuffer data directly. Your data stays in portable FlatBuffer format, readable by any FlatBuffer tooling, while you get SQL query capabilities.

## Why FlatSQL?

Traditional approach:
```
FlatBuffer → Deserialize → SQLite rows → Query → Serialize → FlatBuffer
```

FlatSQL approach:
```
FlatBuffer → Query (via virtual table) → FlatBuffer
```

**Benefits:**
- **Zero conversion overhead** — Data stays in FlatBuffer format
- **Streaming ingestion** — Indexes built during data arrival, not after
- **Portable output** — Exported data is standard FlatBuffers, readable by any tooling
- **Multi-source federation** — Query across multiple FlatBuffer sources with automatic source tagging

## Runtime Choices & Performance Gates

- Production should prefer the SQLite-backed native/WASM path (`initFlatSQL` → `FlatSQL` → `createDatabase`). The pure TypeScript `FlatSQLDatabase` is preserved only as an explicit fallback/reference implementation for environments that cannot load the WASM module.
- Standalone deployments use the C++ WASI reactor at `flatsql/wasi.wasm`. Browser hosts load it through `flatsql/standalone`, while Node/WasmEdge hosts use a resident runner from `flatsql/standalone/wasmedge`.
- Artifact-builder cache behavior belongs to C++: query templates, result-cache keys, invalidation generation, hit/miss counters, SQL execution, and raw FlatBuffer lookup all live in the WASM database instance. JavaScript only marshals bytes and fills host capability gaps.
- Use `db.ingestBuffers([...])` on the WASM path when your input is already a set of raw FlatBuffers. It feeds the native bulk-stream ingest path instead of looping through `ingestOne(...)`.
- Run `npm run bench` (alias `npm run bench:perf`) as the single-command benchmark matrix for FlatSQL JS vs FlatSQL WASM. Use `npm run bench:perf:profile` to print the WASM ingest phase breakdown (`pack`, `decode`, `append`, `index`, `verify`) alongside the gate table.
  Profiling mode is diagnostic and includes instrumentation overhead; use the non-profile benchmark for merge-gate decisions.
- Run `npm run test:cluster` for the native WAL-backed cluster validation workload, or `npm run test:cluster:smoke` for the short preflight run.

## SDS Stress Harness

The Space Data Standards stress harness exercises every discoverable `main.fbs` under `../spacedatastandards.org/schema`, records bandwidth and storage-fill metrics, and keeps canonical schemas database-neutral. Results are written under `stress/results/<run-id>/`.

```bash
# Local smoke path, no Docker required.
npm run stress:sds:smoke -- --run-id sds-smoke

# Existing single-controller Docker smoke path.
npm run stress:sds:docker-smoke -- --run-id sds-docker-smoke

# Container-per-node fleet path for small probes, e.g. 10 isolated nodes.
npm run stress:sds:docker-fleet -- --run-id sds-10-node --nodes 10 --storage-gb 1 --records-per-node 100000

# Generate and validate the 100-node / 1 GB-per-node production compose file without running it.
npm run stress:sds:full-config -- --run-id sds-full
docker compose -f stress/results/sds-full/docker-compose.full.yml config

# Run the full production gate. Requires Docker and enough local storage.
CONFIRM_FULL_STRESS=1 npm run stress:sds:full -- --run-id sds-full
```

The production gate only passes when the aggregate report shows container isolation, Docker transport, at least 100 nodes, no metric errors, measured native/standalone/WasmEdge events, complete SDS schema-sweep coverage, at least 95% storage fill against the 1 GB/node target on every node, and bulk-ingest throughput at or above 75 fps on every node.

Full mode defaults to 20,000 records per node, or 2 million records across the 100-node fleet. In full mode the generated PublishEventRecord payload size scales from the configured storage budget so the run targets the requested per-node storage fill without requiring hundreds of millions of tiny records.

## Agent Runtime Notes

Use these entrypoints when wiring agents or deployments:

| Import | Runtime | Use for |
|--------|---------|---------|
| `flatsql/wasm` | Emscripten JS wrapper + C++ WASM | Existing browser/Node API |
| `flatsql/standalone` | Raw `flatsql-wasi.wasm` + small JS WASI shim | Browser or Node direct WebAssembly API |
| `flatsql/artifacts/standalone` | C++ artifact builder facade | Cache-centric artifact workflows |
| `flatsql/response` | Response artifact bytes | ETags, chunks, and raw byte artifacts keyed by native runtime cache keys |
| `flatsql/standalone/wasmedge` | Native WasmEdge runner builder/client | Persistent standalone runtime in Node |
| `flatsql` `FlatSQLDatabase` | Pure TypeScript fallback | Reference or no-WASM environments only |

For repeated FILE_ID-style traffic, register a cacheable query template once and call `queryTemplate(...)` with positional params. Identical query IDs and params hit the C++ result cache; ingest, load, template changes, and explicit `clearQueryCache()` invalidate stale entries by generation.

```javascript
import { createStandaloneArtifactBuilder } from 'flatsql/artifacts/standalone';

const builder = await createStandaloneArtifactBuilder(schema, { runtime: 'standalone' });
await builder.registerFileId('USER', 'User');
await builder.enableDemoExtractors();
await builder.ingestBuffers(buffers);
await builder.configureQueryCache({ maxEntries: 4096, maxRows: 5000 });
await builder.registerQueryTemplate('userByEmail', 'SELECT id FROM User WHERE email = ?', true);

await builder.queryTemplate('userByEmail', ['alice@example.com']); // miss
await builder.queryTemplate('userByEmail', ['alice@example.com']); // hit
console.log(await builder.getQueryCacheStats());
```

The native cache defaults to 1024 entries and 1000 rows per cached result. Tune `maxEntries` and `maxRows` for production FILE_ID traffic instead of recompiling the C++ core.

For WasmEdge, build the small host runner and pass it to the same builder. The runner keeps one WASI module instance resident so cache state survives across requests.

```javascript
import { createStandaloneArtifactBuilder } from 'flatsql/artifacts/standalone';
import { buildFlatSQLWasmEdgeRunner } from 'flatsql/standalone/wasmedge';

const runner = await buildFlatSQLWasmEdgeRunner({
  outputPath: './build/flatsql-wasmedge-runner',
});

const builder = await createStandaloneArtifactBuilder(schema, {
  runtime: 'wasmedge',
  wasmEdgeRunnerBinary: runner.outputPath,
});
```

WasmEdge must support WebAssembly exception handling for this artifact. The test runner uses `wasmedge --enable-exception-handling --reactor wasm/flatsql-wasi.wasm _initialize` for the smoke check and the native C API runner enables the same proposal programmatically.

For high-volume repeated FILE_ID responses, build the cache key through the C++ runtime, wrap query results as response artifacts, and let edge/browser/WasmEdge host caches serve the immutable bytes by ETag or content hash. FlatSQL creates deterministic native cache keys and chunk metadata; deployment code owns CDN placement, routing, authorization, and global invalidation.

```javascript
import { createQueryResponseArtifact } from 'flatsql/response';

const sql = 'SELECT _data FROM PublishEventRecord WHERE FILE_ID = ?';
const cacheKey = await builder.buildResponseArtifactCacheKey('PublishEventRecord', '1', sql, {
  format: 'raw-flatbuffer-stream',
  publishEventKey: 'publish-1',
  projection: ['_data'],
  params: ['publish-1'],
});
const result = await builder.query(sql, ['publish-1']);
const artifact = createQueryResponseArtifact(result, {
  schemaName: 'PublishEventRecord',
  schemaVersion: 1,
  sql,
  params: ['publish-1'],
  format: 'raw-flatbuffer-stream',
  publishEventKey: 'publish-1',
  projection: ['_data'],
  cacheKey,
});

console.log(artifact.metadata.cacheKey, artifact.metadata.etag);
```

See `docs/response-artifacts.md` and `docs/agents/response-artifacts.md` for production and agent guidance.


## Source Code

| Repository | Description |
|------------|-------------|
| [digitalarsenal/flatsql](https://github.com/digitalarsenal/flatsql) | This project — FlatSQL query engine |
| [digitalarsenal/flatbuffers](https://github.com/digitalarsenal/flatbuffers) | Fork of Google FlatBuffers with WASM support |
| [flatc-wasm](https://digitalarsenal.github.io/flatbuffers/) | FlatBuffer compiler running in WebAssembly |

## Usage

### WASM (Browser/Node.js)

The C++ engine compiles to WebAssembly for cross-platform deployment:

```javascript
import { initFlatSQL } from 'flatsql/wasm';

const flatsql = await initFlatSQL();

// Create database with schema
const db = flatsql.createDatabase(`
  table User {
    id: int (id);
    name: string;
    email: string (key);
    age: int;
  }
`, 'myapp');

// Register file identifier routing
db.registerFileId('USER', 'User');

// Enable demo field extractors (for testing)
db.enableDemoExtractors();

// Ingest FlatBuffer stream
// Format: [4-byte size LE][FlatBuffer][4-byte size LE][FlatBuffer]...
db.ingest(streamData);

// Or ingest pre-built FlatBuffers directly through the bulk path.
db.ingestBuffers([
  flatsql.createTestUser(1, 'Alice', 'alice@example.com', 30),
  flatsql.createTestUser(2, 'Bob', 'bob@example.com', 25),
]);

// Query with SQL
const result = db.query('SELECT id, name, email FROM User WHERE age > 25');
console.log(result.columns); // ['id', 'name', 'email']
console.log(result.rows);    // [[1, 'Alice', 'alice@example.com'], ...]

// Export database
const exported = db.exportData();

// Cleanup
db.destroy();
```

### TypeScript (Pure JavaScript)

A TypeScript implementation for environments where WASM isn't available:

```typescript
import { FlatSQLDatabase, FlatcAccessor } from 'flatsql';
import { FlatcRunner } from 'flatc-wasm';

const flatc = await FlatcRunner.init();

const schema = `
  namespace App;

  table User {
    id: int (key);
    name: string (required);
    email: string;
    age: int;
  }
`;

const accessor = new FlatcAccessor(flatc, schema);
const db = FlatSQLDatabase.fromSchema(schema, accessor, 'myapp');

// Insert records
db.insert('User', { id: 1, name: 'Alice', email: 'alice@example.com', age: 30 });
db.insert('User', { id: 2, name: 'Bob', email: 'bob@example.com', age: 25 });

// Query
const result = db.query('SELECT name, email FROM User WHERE age > 20');
console.log(result.rows);

// Export as standard FlatBuffers
const exported = db.exportData();
```

### Cluster Runtime Detection

Use the runtime guard helpers before attempting browser cluster mode:

```typescript
import { detectClusterEnvironment, isClusterModeSupported } from 'flatsql';

const env = detectClusterEnvironment();
if (!isClusterModeSupported(env)) {
  throw new Error('Cluster mode unsupported on this runtime');
}
```

### Native C++ (Embedded)

For performance-critical applications, link the C++ library directly:

```cpp
#include <flatsql/database.h>

auto db = flatsql::FlatSQLDatabase::fromSchema(schema, "mydb");

// Register file ID routing
db.registerFileId("USER", "User");

// Set field extractor
db.setFieldExtractor("User", extractUserField);

// Ingest streaming data
size_t recordsIngested = 0;
db.ingest(data, length, &recordsIngested);

// Query
auto result = db.query("SELECT * FROM User WHERE id = 5");
for (size_t i = 0; i < result.rowCount(); i++) {
    std::cout << result.getString(i, "name") << std::endl;
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     FlatSQLDatabase                          │
├─────────────────────────────────────────────────────────────┤
│   SchemaParser        │      SQLiteEngine                    │
│   (FlatBuffers IDL)   │      (Virtual Tables)                │
├─────────────────────────────────────────────────────────────┤
│                    TableStore (per table)                    │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  SQLite Indexes            Field Extractors             │ │
│  │  (id, email, timestamp)    (getField callbacks)         │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│              StackedFlatBufferStore (append-only)            │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ [Header][FB₁][FB₂][FB₃]...                           │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Schema Joins & References

- JSON schemas that reference other definitions now produce explicit ``From_Target_join`` tables with `{FromRowId, TargetRowId}` columns.
- This keeps referenced schemas in the same normalized storage layer so queries can `JOIN` the virtual tables instead of embedding opaque JSON blobs.

### Stream Format

FlatSQL ingests size-prefixed FlatBuffer streams:

```
[4-byte size LE][FlatBuffer with file_id][4-byte size LE][FlatBuffer]...
```

The 4-byte file identifier in each FlatBuffer determines which table receives the record.

## Spatial Extensions

FlatSQL includes a full spatial computation engine with SpatiaLite-compatible functions, polygon boolean operations, Voronoi/Delaunay tessellation, and coordinate transforms.

### Spatial SQL Functions

| Category | Functions |
|----------|-----------|
| **Point ops** | `geo_distance`, `geo_bearing`, `geo_destination`, `geo_midpoint`, `geo_area_bbox`, `geo_within_radius`, `geo_bbox_contains` |
| **Geohash** | `geo_geohash_encode`, `geo_geohash_decode_lat`, `geo_geohash_decode_lon` |
| **Geometry I/O** | `geo_from_text` (WKT→blob), `geo_as_text` (blob→WKT), `geo_from_geojson`, `geo_as_geojson` |
| **Predicates** | `geo_contains` (point-in-polygon, ray casting with hole support) |
| **Boolean ops** | `geo_intersection`, `geo_union`, `geo_difference`, `geo_sym_difference`, `geo_buffer` |
| **Analysis** | `geo_area_geom`, `geo_centroid`, `geo_length_geom`, `geo_envelope`, `geo_convex_hull` |
| **Voronoi/Delaunay** | `geo_voronoi`, `geo_delaunay` |
| **Coord transforms** | `geo_to_ecef` (WGS84→ECEF), `geo_from_ecef` (ECEF→WGS84) |

### Examples

```sql
-- Distance between NYC and DC (~328 km)
SELECT geo_distance(40.7128, -74.0060, 38.9072, -77.0369);

-- Bearing from NYC to LA
SELECT geo_bearing(40.7128, -74.0060, 34.0522, -118.2437);

-- Geohash encode/decode
SELECT geo_geohash_encode(42.6, -5.6, 5);  -- 'ezs42'

-- Point-in-polygon
SELECT geo_contains('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 5, 5);  -- 1

-- Polygon intersection (accepts WKT or blob)
SELECT geo_as_text(geo_intersection(
  'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))',
  'POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))'
));

-- WGS84 to ECEF coordinate transform
SELECT geo_to_ecef(48.8566, 2.3522, 35);  -- '4200952.xxx,172458.xxx,4780111.xxx'

-- Voronoi diagram
SELECT geo_as_text(geo_voronoi(
  'MULTIPOINT((0 0), (10 0), (10 10), (0 10))',
  'POLYGON((-5 -5, 15 -5, 15 15, -5 15, -5 -5))'
));
```

### R-Tree Spatial Indexing

FlatSQL automatically creates R-Tree shadow tables for spatial columns during ingest. Spatial columns are detected by:
- **Convention**: columns named `latitude`/`lat` + `longitude`/`lon`/`lng`
- **Schema attribute**: fields marked with `(spatial)` in FlatBuffers IDL

```sql
-- R-Tree pre-filter + precise radius check (72x faster than full scan)
SELECT p.* FROM points p
INNER JOIN _rtree_points r ON p.rowid = r.id
WHERE r.minLat >= 36.2 AND r.maxLat <= 45.2
  AND r.minLon >= -80 AND r.maxLon <= -68
  AND geo_within_radius(40.7, -74.0, p.lat, p.lon, 500) = 1;
```

### Spatial Performance

| Operation | Throughput | Latency |
|-----------|-----------|---------|
| Point operations (distance, bearing) | 733K–1.07M ops/sec | ~1 µs |
| Geohash encode/decode | 631K–746K ops/sec | ~1.4 µs |
| Point-in-polygon | 127K–255K ops/sec | ~4 µs |
| Polygon intersection | 81K ops/sec | ~12 µs |
| Voronoi (4 sites) | 34K ops/sec | ~29 µs |
| Coordinate transforms | 556K–609K ops/sec | ~1.7 µs |
| R-Tree bbox query (10K records) | 77K ops/sec | ~13 µs |
| R-Tree vs full scan speedup | **58–72x** | |

### Space Data Module (SDM)

FlatSQL Spatial is also available as a standalone WASM module compliant with the [Space Data Module SDK](https://github.com/DigitalArsenal/space-data-module-sdk):

```javascript
import { initSpatialSDM } from 'flatsql/sdm';

const sdm = await initSpatialSDM();

// Read the embedded manifest
console.log(sdm.getManifest().pluginId);
// → 'com.digitalarsenal.flatsql.spatial'

// Compute distance
const km = sdm.computeDistance(40.7128, -74.006, 38.9072, -77.0369);

// Point-in-polygon
const inside = sdm.pointInPolygon('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 5, 5);

// Coordinate transform
const ecef = sdm.toECEF(48.8566, 2.3522, 35);
const back = sdm.fromECEF(ecef.x, ecef.y, ecef.z);
```

## SQL Support

### Supported

- `SELECT` with column selection
- `WHERE` with `=`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `LIKE`, `AND`, `OR`
- `ORDER BY` (ASC/DESC)
- `LIMIT` and `OFFSET`
- `COUNT(*)` aggregate
- Index-accelerated queries on `(id)` and `(key)` columns

### Not Supported

- `JOIN` (query one table at a time)
- `GROUP BY`, `HAVING`, most aggregates
- `INSERT`, `UPDATE`, `DELETE` (use API methods instead)
- Subqueries, CTEs, window functions

## Performance

FlatSQL outperforms traditional SQLite on query operations:

| Operation | FlatSQL | SQLite | Speedup |
|-----------|---------|--------|---------|
| Point query (by id) | 3.50 ms | 3.93 ms | 1.1x |
| Point query (by key) | 5.23 ms | 6.94 ms | 1.3x |
| Direct index lookup | 1.56 ms | 3.93 ms | 2.5x |
| Full scan | 0.84 ms | 1.25 ms | 1.5x |
| Direct iteration | 0.05 ms | 1.25 ms | 25x |

*Benchmarks: 10,000 records, 10,000 query iterations, Apple M3 Ultra*

## Performance Trade-offs

FlatSQL uses SQLite's virtual table (VTable) API to expose FlatBuffer data as queryable tables. This architecture enables SQL queries over raw binary data, but comes with fundamental trade-offs that affect performance characteristics.

### Access Paths

FlatSQL provides three access paths with different performance profiles:

| Access Path | Latency | Throughput | Use Case |
|-------------|---------|------------|----------|
| **Zero-Copy API** | 1.7 µs | 580K ops/sec | Direct index lookup, returns raw FlatBuffer pointer |
| **VTable SQL** | 12.9 µs | 78K ops/sec | Full SQL queries via SQLite |
| **Pure SQLite** | 2.5 µs | 400K ops/sec | Baseline comparison |

### Why VTable Queries Are Slower Than Pure SQLite

SQLite's VTable API has fundamental limitations that prevent FlatSQL from matching pure SQLite query performance:

1. **Per-Column Extraction** — The `xColumn()` callback is invoked once per column per row. There is no batch API to extract multiple fields at once, meaning each field access has function call overhead.

2. **Mandatory Value Conversion** — All values must be converted to SQLite's internal format via `sqlite3_result_*()` functions. Even though FlatBuffers already store data in an efficient binary format, we must convert strings to SQLite strings, integers to SQLite integers, etc.

3. **Row-by-Row Processing** — The `xNext()` callback advances one row at a time with no vectorized or batch iteration. This prevents SIMD optimizations or processing multiple records per call.

4. **No Direct Memory Access** — SQLite cannot read FlatBuffer memory directly; all data must flow through the VTable callback interface, adding overhead for every field access.

### When to Use Each Access Path

**Use the Zero-Copy API when:**
- You need maximum throughput for point lookups
- You're building hot paths that query by indexed keys
- You can work directly with FlatBuffer data structures

```cpp
// Zero-copy: returns pointer to raw FlatBuffer (1.7 µs)
const uint8_t* data = db.findRawByIndex("User", "email", email, &len);
auto user = GetUser(data);  // Direct FlatBuffer access
```

**Use VTable SQL when:**
- You need complex queries (filtering, sorting, aggregation)
- Query flexibility is more important than raw speed
- You're doing ad-hoc exploration of the data

```cpp
// VTable SQL: full query capability (12.9 µs)
auto result = db.query("SELECT * FROM User WHERE age > 25 ORDER BY name");
```

### Architecture: SQLite-Backed Indexes

FlatSQL uses SQLite's highly optimized B-tree for indexing (not a custom implementation). This provides:

- **Battle-tested performance** — SQLite's B-tree is used by billions of devices
- **Consistent behavior** — Same indexing code path as pure SQLite
- **Fast path optimization** — Type-specific lookups bypass `std::variant` overhead

The index stores `(key, sequence) → (offset, length)` mappings, allowing O(log n) lookups that return pointers directly into the FlatBuffer storage.

### Trade-off Summary

| Aspect | FlatSQL | Pure SQLite |
|--------|---------|-------------|
| Point lookup (indexed) | **1.4x faster** (zero-copy API) | Baseline |
| SQL queries | 5-7x slower (VTable overhead) | Baseline |
| Storage format | FlatBuffers (portable, zero-copy) | SQLite pages |
| Data conversion | None (zero-copy) or on-demand | Always required |
| Streaming ingest | Append-only, real-time indexing | Row-by-row inserts |

**Bottom line:** FlatSQL excels when you need streaming ingestion of FlatBuffer data with SQL query capability. Use the zero-copy API for performance-critical lookups; use SQL for complex queries where flexibility matters more than speed.

## Building from Source

### Prerequisites

- Node.js 24+
- CMake 3.20+ (for WASM builds)
- Emscripten (for WASM builds)

### TypeScript Build

```bash
npm install
npm run build
npm test
```

### WASM Build

```bash
# Clone DA-FlatBuffers (required dependency)
git clone https://github.com/DigitalArsenal/flatbuffers.git ../flatbuffers

# Build browser, standalone WASI, and SDM artifacts
npm run build:wasm

# Verify standalone and package surfaces
npm run test:wasi
npm run test:wasmedge
npm test -- --runInBand --runTestsByPath test/package-exports.test.ts
```

Output: `wasm/flatsql.js`, `wasm/flatsql.wasm`, `wasm/flatsql-wasi.wasm`, `sdm/flatsql-spatial.wasm`, and docs copies for GitHub Pages.

### Run Demo Locally

```bash
npm run serve
# Open http://localhost:8081
```

## API Reference

### initFlatSQL()

```typescript
import { initFlatSQL } from 'flatsql/wasm';

const flatsql = await initFlatSQL();
```

### createDatabase(schema, name)

```typescript
const db = flatsql.createDatabase(schemaString, 'dbname');
```

### db.registerFileId(fileId, tableName)

```typescript
db.registerFileId('USER', 'User');  // Route "USER" FlatBuffers to User table
```

### db.ingest(data, source?)

```typescript
const bytesConsumed = db.ingest(uint8ArrayStream);
// Or with source tagging (requires registerSource first):
const bytesConsumed = db.ingest(uint8ArrayStream, 'satellite-1');
```

### db.query(sql)

```typescript
const result = db.query('SELECT * FROM User WHERE age > 25');
// result.columns: string[]
// result.rows: any[][]
```

### db.exportData()

```typescript
const data = db.exportData();  // Returns Uint8Array
```

### db.registerSource(sourceName)

```typescript
db.registerSource('satellite-1');  // Creates User@satellite-1, Post@satellite-1, etc.
```

### db.createUnifiedViews()

```typescript
db.createUnifiedViews();  // Creates unified views with _source column
```

### db.listSources()

```typescript
const sources = db.listSources();  // ['satellite-1', 'satellite-2', ...]
```

## Multi-Source Queries

FlatSQL supports federating multiple data sources with the same schema. Each source gets its own set of tables, and you can query them individually or across all sources.

### Use Case

Imagine you have multiple satellites streaming telemetry data with the same schema:

```javascript
// Register sources
db.registerSource('satellite-1');
db.registerSource('satellite-2');
db.registerSource('ground-station');

// Register file IDs and extractors (must be done after registerSource)
db.registerFileId('TELE', 'Telemetry');
db.enableDemoExtractors();

// Create unified views (call once after all sources registered)
db.createUnifiedViews();

// Ingest from different sources
db.ingest(satellite1Stream, 'satellite-1');
db.ingest(satellite2Stream, 'satellite-2');
db.ingest(groundStream, 'ground-station');

// Query a specific source
db.query('SELECT * FROM "Telemetry@satellite-1" WHERE signal > 50');

// Query across all sources (unified view)
db.query('SELECT * FROM Telemetry WHERE timestamp > 1000');
```

### Table Naming Convention

- **Source-specific tables**: `TableName@sourceName` (e.g., `User@siteA`, `Telemetry@satellite-1`)
- **Unified views**: `TableName` (e.g., `User`, `Telemetry`) - combines all source tables with a `_source` column

### The _source Column

Unified views include a `_source` column that identifies which source each row came from:

```sql
-- See source for each record
SELECT _source, id, name FROM User LIMIT 10;

-- Count records by source
SELECT _source, COUNT(*) as count FROM User GROUP BY _source;

-- Filter by source in unified view
SELECT * FROM User WHERE _source = 'satellite-1';
```

## License

Apache 2.0

## Contributing

Contributions welcome. Please open an issue first to discuss significant changes.

## Contact

For questions, licensing inquiries, or commercial support: [tj@digitalarsenal.io](mailto:tj@digitalarsenal.io)

---

Built on [DA-FlatBuffers](https://digitalarsenal.github.io/flatbuffers/) and [SQLite](https://sqlite.org/).
