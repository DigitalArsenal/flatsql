#ifndef FLATSQL_GEO_FUNCTIONS_H
#define FLATSQL_GEO_FUNCTIONS_H

#include <sqlite3.h>

namespace flatsql {

/**
 * Register geo/spatial SQL functions on a SQLite database.
 *
 * Functions:
 *   geo_distance(lat1, lon1, lat2, lon2)        -> km (Haversine)
 *   geo_bbox_contains(minLat, maxLat, minLon, maxLon, lat, lon) -> 0/1
 *   geo_within_radius(centerLat, centerLon, lat, lon, radiusKm)  -> 0/1
 */
void registerGeoFunctions(sqlite3* db);

}  // namespace flatsql

#endif  // FLATSQL_GEO_FUNCTIONS_H
