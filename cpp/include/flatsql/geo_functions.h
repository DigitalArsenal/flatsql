#ifndef FLATSQL_GEO_FUNCTIONS_H
#define FLATSQL_GEO_FUNCTIONS_H

#include <sqlite3.h>

namespace flatsql {

/**
 * Register geo/spatial SQL functions on a SQLite database.
 *
 * Point operations:
 *   geo_distance(lat1, lon1, lat2, lon2)                          -> km (Haversine)
 *   geo_bbox_contains(minLat, maxLat, minLon, maxLon, lat, lon)   -> 0/1
 *   geo_within_radius(centerLat, centerLon, lat, lon, radiusKm)   -> 0/1
 *   geo_bearing(lat1, lon1, lat2, lon2)                           -> degrees 0-360
 *   geo_destination(lat, lon, bearing_deg, distance_km)           -> text "lat,lon"
 *   geo_midpoint(lat1, lon1, lat2, lon2)                          -> text "lat,lon"
 *   geo_area_bbox(minLat, maxLat, minLon, maxLon)                 -> sq km
 *
 * Geohash:
 *   geo_geohash_encode(lat, lon [, precision])                    -> text
 *   geo_geohash_decode_lat(geohash)                               -> double
 *   geo_geohash_decode_lon(geohash)                               -> double
 *
 * Geometry (WKT / blob):
 *   geo_from_text(wkt)                                            -> blob
 *   geo_as_text(geom)                                             -> text (WKT)
 *   geo_from_geojson(json)                                        -> blob
 *   geo_as_geojson(geom)                                          -> text (GeoJSON)
 *   geo_contains(geom, lat, lon)                                  -> 0/1
 *   geo_intersection(geom1, geom2)                                -> blob
 *   geo_union(geom1, geom2)                                       -> blob
 *   geo_difference(geom1, geom2)                                  -> blob
 *   geo_sym_difference(geom1, geom2)                              -> blob
 *   geo_buffer(geom, distance_deg)                                -> blob
 *   geo_area_geom(geom)                                           -> double (sq degrees)
 *   geo_centroid(geom)                                            -> blob (POINT)
 *   geo_length_geom(geom)                                         -> double (km)
 *   geo_envelope(geom)                                            -> blob (POLYGON)
 *   geo_convex_hull(geom)                                         -> blob
 *   geo_voronoi(multipoint_geom, bounds_geom)                     -> blob (MULTIPOLYGON)
 *   geo_delaunay(multipoint_geom)                                 -> blob (MULTIPOLYGON)
 *
 * Coordinate transforms:
 *   geo_to_ecef(lat, lon, alt)                                    -> text "x,y,z"
 *   geo_from_ecef(x, y, z)                                       -> text "lat,lon,alt"
 */
void registerGeoFunctions(sqlite3* db);

}  // namespace flatsql

#endif  // FLATSQL_GEO_FUNCTIONS_H
