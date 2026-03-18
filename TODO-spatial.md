# Spatial Extensions TODO

## Point Operations
- [x] `geo_bearing(lat1, lon1, lat2, lon2)` — initial bearing in degrees (0-360)
- [x] `geo_destination(lat, lon, bearing_deg, distance_km)` — destination point as "lat,lon"
- [x] `geo_midpoint(lat1, lon1, lat2, lon2)` — great circle midpoint as "lat,lon"
- [x] `geo_area_bbox(minLat, maxLat, minLon, maxLon)` — bounding box area in sq km

## Geohash
- [x] `geo_geohash_encode(lat, lon, precision)` — geohash string
- [x] `geo_geohash_decode_lat(geohash)` — latitude from geohash
- [x] `geo_geohash_decode_lon(geohash)` — longitude from geohash

## WKT / Geometry I/O
- [x] WKT parser (POINT, LINESTRING, POLYGON, MULTIPOINT, MULTIPOLYGON)
- [x] `geo_from_text(wkt)` — parse WKT to internal BLOB geometry
- [x] `geo_as_text(geom)` — serialize internal geometry to WKT
- [x] `geo_from_geojson(json)` — parse GeoJSON to internal BLOB
- [x] `geo_as_geojson(geom)` — serialize internal geometry to GeoJSON

## Point-in-Polygon
- [x] `geo_contains(geom_polygon, lat, lon)` — general point-in-polygon via ray casting

## Polygon Boolean Operations
- [x] `geo_intersection(geom1, geom2)` — polygon intersection (Sutherland-Hodgman)
- [x] `geo_union(geom1, geom2)` — polygon union
- [x] `geo_difference(geom1, geom2)` — polygon difference (A minus B)
- [x] `geo_sym_difference(geom1, geom2)` — symmetric difference
- [x] `geo_buffer(geom, distance_deg)` — polygon buffer/offset

## Geometry Analysis
- [x] `geo_area_geom(geom)` — polygon area in square degrees (Shoelace formula)
- [x] `geo_centroid(geom)` — centroid as internal geometry POINT
- [x] `geo_length_geom(geom)` — line/polygon perimeter length in km (haversine)
- [x] `geo_envelope(geom)` — bounding box as internal geometry POLYGON
- [x] `geo_convex_hull(geom)` — convex hull via Graham scan

## Voronoi / Delaunay
- [x] `geo_voronoi(geom_multipoint, geom_bounds)` — Voronoi diagram as MULTIPOLYGON
- [x] `geo_delaunay(geom_multipoint)` — Delaunay triangulation as MULTIPOLYGON (triangles)

## Coordinate Transforms
- [x] `geo_to_ecef(lat, lon, alt)` — WGS84 geodetic to ECEF, returns "x,y,z"
- [x] `geo_from_ecef(x, y, z)` — ECEF to WGS84 geodetic, returns "lat,lon,alt"

## R-Tree Integration
- [x] Automatic R-Tree shadow table creation for spatial columns
- [x] R-Tree population during ingest pipeline
- [x] Convention-based lat/lon column detection (latitude/lat + longitude/lon/lng)
- [x] Schema attribute `(spatial)` for explicit spatial column marking

## Tests
- [x] Point operation tests (bearing, destination, midpoint) vs SpatiaLite reference
- [x] Geohash encode/decode round-trip
- [x] WKT parse/serialize round-trip
- [x] Point-in-polygon tests (convex, concave, edge cases)
- [x] Polygon boolean ops vs SpatiaLite reference values
- [x] Voronoi/Delaunay correctness tests
- [x] Coordinate transform round-trip (WGS84 <-> ECEF)
- [x] Edge cases (antimeridian, poles, degenerate geometries)

## Benchmarks
- [x] Point operations benchmark (733K–1.07M ops/sec via SQL)
- [x] Geohash encode/decode benchmark (631K–746K ops/sec)
- [x] WKT/GeoJSON parse/serialize benchmark
- [x] Point-in-polygon benchmark (127K–255K ops/sec via SQL)
- [x] Polygon boolean operations benchmark (39K–81K ops/sec)
- [x] Voronoi/Delaunay benchmark (15K–58K ops/sec)
- [x] Coordinate transform benchmark (556K–609K ops/sec)
- [x] Direct geometry API benchmark (no SQL overhead)
- [x] R-Tree vs full scan comparison (58x speedup on bbox, 72x on radius)
