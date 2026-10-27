# FlatSQL 3.12 binary API

FlatSQL has one public JavaScript entry point, `flatsql`, exporting `FlatSQL`
and `FlatSQLError`, and one native ABI, `cpp/include/flatsql/flatsql.h`.
`flatsql/flatsql.wasm` identifies the same WASI threads artifact on every host.
There are no PS/P4 selectors, old engine aliases, or compatibility modes.

## Schemas, records, and queries

- `FlatSQL.open(options)` opens persistent host storage. Node `root` is a
  filesystem directory; browser `root` is an OPFS directory. Node without a
  root creates a temporary store. `close()` drains/cancels operations and stops
  the engine. Close an instance before opening the same directory again.
- `registerSchema({name, fileIdentifier, bfbs, ...})` registers a binary
  reflection schema. Type names are SQL identifiers, up to 64 characters; the
  file identifier is four bytes. Indexed extraction rules and full-text
  indexing are schema configuration, executed inside the guest.
- `ingest(type, records, metadata)` accepts concatenated frames:
  `[u32 little-endian payload length][FlatBuffer payload]`. The guest validates
  records, computes identities, persists streams and updates indexes. Provider,
  source, producer, batch and timestamp metadata are optional. `signatures`
  supplies one opaque byte array per record, retained in `_signature`.
- `remove(type, records)` deletes those original binary payloads and returns a
  QRP receipt. Record hashing and deletion stay inside the engine.
- `query(request, options)` returns an async stream of byte chunks; `execute`
  collects it within a byte budget. Requests are canonical SDS QRP buffers,
  optionally size-prefixed, with `KIND=Request` and `REPRESENTATION=FlatBuffer`
  (or their schema defaults). `SQL` and `PARAMETERS` drive the query. Express
  filters, sort, selection, and paging in SQL; unrelated QRP selection fields
  are rejected instead of silently ignored.
- Projection output is a stream of size-prefixed SDS QRP pages: column names,
  typed rows, and a final summary containing counts, status and truncation.
  Chunk boundaries need not coincide with frame boundaries. QRP null, integer,
  float, text and bytes preserve SQLite values; boolean parameters bind as
  SQLite integers 0/1. Integers remain signed 64-bit values.
- `SELECT _data` with `{records:true}` streams original record frames unchanged.
  IDs, properties, altitude, null geometry and bounding boxes are preserved
  byte for byte. Derived geometry has the separate semantics below.
- With `registerSchema({..., fullText:true})`, SQL `_search MATCH ?1` uses the
  engine's FTS5 index over the record's text fields, including nested text.
  The engine returns `BUSY` while its background index catches up; retry the
  query, and never treat that condition as zero matches.
- Hidden metadata columns `_cid`, `_seq`, `_ts`, `_epoch`, `_producer`, `_peer`
  and `_signature` are available alongside `_source`, `_rowid` and `_data`.
- Root scalar/string/byte-vector fields are exposed by BFBS reflection.
  Complex/nested fields remain in `_data`. Unsigned integers above the SQLite
  signed 64-bit range produce an error; retrieve `_data` to preserve them.
- `surface()` returns the SQL catalog in QRP pages. `setQuota(bytes)` changes the
  disk budget; zero removes that budget. Low-level mailbox maintenance and
  storage controls are documented in the C header, behind the same engine.

`QueryOptions` provides cancellation (`AbortSignal`), elapsed timeout,
rows/result bytes, rows examined, bytes read, and bulk/sandbox lane selection.
QRP `MAX_ROWS`, `LIMIT`, `MAX_BYTES`, and `MAX_ELAPSED_MS` are enforced by the
engine. Breaking a stream cancels and drains its lane before its slot is reused.
`execute` defaults to a 16 MiB collected result budget, with bounded room for
terminal framing. A budget failure throws `FlatSQLError`; already streamed
chunks are partial, never a successful complete answer.

The data boundary accepts `Uint8Array`, not JavaScript rows or SQL strings.
The package does not parse or render JSON, GeoJSON or WKT, compile schema text,
or provide the SQLite JSON extension. Manifest JSON is tooling metadata.
Convert separately with flatc-wasm or generated SDS bindings. Private packed
control messages configure memory and files; they do not carry a second public
record or SQL result format.

## Geometry

Geometry arguments are canonical SDS GJN (`$GJN`, root `GJN`) FlatBuffers,
provided as a payload or one size-prefixed frame. Results are GJN payloads in
QRP `Bytes` values. Every result verifies against the same SDS schema. No
custom geometry blob or JSON parser exists inside the engine.

All seven GJN geometry kinds are decoded, including multi-geometries and
collections. Multiple features are evaluated as a collection; null feature
geometries contribute no points. SQL NULL propagates. Malformed buffers,
non-finite positions, open polygon rings, and unsupported combinations produce
SQL errors. Properties remain opaque, including SDS `JSON_VALUE` strings.

| Function | Behavior |
| --- | --- |
| `geo_distance`, `geo_within_radius`, `geo_bearing` | Spherical calculations; distances/radii in kilometers, coordinates in latitude/longitude order. |
| `geo_bbox_contains`, `geo_area_bbox` | Scalar bounding-box predicates and spherical area. |
| `geo_contains(g,lat,lon)` | Point/polygon membership, including polygon holes and collection members. |
| `geo_area_geom(g)` | Sum of planar polygon areas minus holes, in square degrees. Non-area members contribute zero. |
| `geo_length_geom(g)` | Haversine line length in kilometers; collection members contribute their lengths. |
| `geo_envelope(g)`, `geo_convex_hull(g)` | Two-dimensional envelope/hull over every member's coordinates. |
| `geo_centroid(g)` | Planar centroid, weighted by polygon area minus holes, then line length, then point count. Highest-dimensional members contribute; empty/degenerate input returns NULL. |
| `geo_intersection`, `geo_union`, `geo_difference`, `geo_sym_difference` | Convex, hole-free polygon operands, either winding. Exact clipped pieces; concave or holed operands are rejected. Results may contain multiple polygon pieces with touching boundaries. |
| `geo_buffer(g,d)` | Point buffer with positive distance in degrees, approximated by a 16-segment circle. Other geometry kinds are rejected. |
| `geo_voronoi`, `geo_delaunay` | Point/multipoint sites in the longitude/latitude plane. Voronoi bounds must be a convex, hole-free polygon. |
| `geo_destination`, `geo_midpoint` | GJN point results from spherical calculations. |
| `geo_to_ecef_x/y/z(lat,lon,alt)` | Separate numeric Cartesian components in meters; altitude input is meters. |
| `geo_from_ecef(x,y,z)` | GJN point with WGS84 longitude, latitude and altitude in meters. |

Derived map geometry is explicitly 2D: it contains newly computed coordinates
and no feature IDs, properties, source bbox or altitude. `geo_from_ecef` computes
altitude. Scalar map measurements ignore altitude. Original record queries
preserve the complete original 3D record. Geometry computations are planar
unless explicitly described as spherical; they do not unwrap antimeridian
crossings or act as a global spherical topology engine.

## Integrity and hosts

Default loads verify the adjacent `integrity.json`: browser Fetch SRI and Node
SHA-256. Supplying `wasmBytes` requires a verification callback, allowing a host
to enforce its signed artifact policy before instantiation. Generic adapters
provide threads, memory, filesystem I/O and transport; query, geometry, indexing,
and record identity logic stay inside C++/WASM.

Browser hosting requires shared memory and cross-origin isolation. Worker URLs
can be supplied explicitly for bundlers. The distributed private runtime
adapters bundle only the necessary published SDK host code and its license;
they do not bring the SDK's legacy FlatSQL dependency into the package.
