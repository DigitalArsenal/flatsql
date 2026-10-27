# FlatSQL 3.12: one FlatBuffer API

Status: implementation contract for the work branch; this is not the shipped 3.11 API.

## Release boundary

3.11.1 repairs CI and publishes the existing engine. 3.12.0 unifies the API and removes the legacy JSON/GeoJSON text surface. The unified release replaces the old entry points; it does not retain aliases, compatibility modes, or runtime version switches.

## Public contract

- One database API backed by the optimized stream store. Applications do not select `p4`, `ps`, or a separate SQL engine.
- One native ABI and one JavaScript package entry point for data operations. Runtime adapters supply memory, threads, files, and transport; they do not implement query or geometry semantics.
- Register binary reflection schemas (`.bfbs`). Store and ingest FlatBuffer records. Use a documented, bounded size-prefixed stream for piped input and output.
- Return whole records as unchanged FlatBuffer frames. Return projections and typed values through the existing SDS QRP schema; preserve null, bool, integer, float, text, and byte values without JSON conversion.
- Preserve cancellation, request budgets, backpressure, quotas, source/feed metadata, durable reopen, compaction, and the optimized index read paths behind this API.
- No engine JSON or GeoJSON text parser, serializer, result mode, or SQLite JSON extension. Package/build manifests are tooling metadata, not a database data format.
- Conversion belongs outside the library: a separate converter such as flatc-wasm creates schema-conformant FlatBuffers, pipes them into FlatSQL, and optionally renders returned FlatBuffers. FlatSQL does not bundle that converter.

## Geospatial contract

Use canonical SDS GJN (`$GJN`, root `GJNFeatureCollection`). Do not introduce a second geometry wire format or preserve the existing custom geometry blob as a public format.

GJN data must remain valid under the canonical schema, including point, multipoint, line, multiline, polygon, multipolygon, and geometry collection. Preserve polygon holes, optional altitude, feature IDs, properties, null geometry, and bounding-box metadata when an operation preserves the original feature. Opaque property fields are not an invitation to parse JSON inside the engine.

Register the geospatial SQL functions on the optimized SQL lanes. Preserve the established scalar distance, bounding-box, and radius query behavior. Geometry-valued arguments and results use GJN FlatBuffers. Remove WKT/GeoJSON conversion functions alongside the legacy parsers so the binary boundary is unambiguous.

Define collection and altitude behavior explicitly for operations that compute new geometry. Reject unsupported input or operation combinations with a typed error; do not silently drop members or dimensions.

## Source work

1. Define the unified entry point and exported ABI. Replace public PS/P4 exports, artifact names, declarations, and documentation together. Keep implementation details private.
2. Reuse SDS QRP for parameters/projection values and GJN for geometry; generate bindings from the pinned SDS package. If a required control contract is missing, change and publish canonical SDS first. Do not repurpose an existing field or create a local schema copy.
3. Connect the optimized SQL lanes to binary geospatial functions. Replace legacy geometry serialization and remove JSON/GeoJSON/WKT conversion code.
4. Replace legacy JSON query response modes and public RB1 decoding with FlatBuffer result frames. Preserve the whole-record fast path and bounded streaming behavior.
5. Replace the legacy schema-column compatibility rules with the unified schema contract where required for GJN; do not add a GJN-specific host query implementation.
6. Update Node/browser/WASI hosts and downstream SDN consumers in coordinated tasks. Relevant consumers include `sdn-js/src/local-flatsql.ts`, `sdn-js/src/flatsql-io-store.ts`, `sdn-server/internal/flatsqlrt`, and `sdn-server/internal/storage/format4`.
7. Rebuild and publish only the unified artifacts as 3.12.0 after the acceptance evidence is complete.

## Required evidence

- External flatc-wasm conversion into canonical GJN, binary ingest, SQL geo filtering/projection, and binary output that verifies against SDS.
- Original scalar geo query cases, null behavior, malformed-buffer rejection, polygon holes, collections, altitude, and feature property preservation.
- Field-for-field query equivalence on the storage fixture, including full records and projections, on native and WASM runtimes.
- Durable reopen, crash/power-loss recovery, compaction during reads, cancellation, quotas, and bounded result memory.
- Real browser interaction for every changed website demo and affected downstream feature; verify isolation/thread requirements and the exact shipped artifact.
- Native and WASM artifact sizes and the package contents compared with 3.11.1. Confirm that removed parser code and legacy engine artifacts are absent.
- Performance evidence on a pinned, unsaturated runtime using the same fixture and measurement conditions. No regressions hidden by a second engine or compatibility route.

The work branch must not be presented as a completed release until these checks pass and all public consumers use the unified API.
