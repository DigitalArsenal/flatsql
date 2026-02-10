#include "flatsql/geo_functions.h"
#include <cmath>

namespace flatsql {

static constexpr double EARTH_RADIUS_KM = 6371.0;
static constexpr double DEG_TO_RAD = M_PI / 180.0;

// Haversine distance between two lat/lon points (returns km)
static void geoDistanceFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 4) {
        sqlite3_result_error(ctx, "geo_distance requires 4 args: lat1, lon1, lat2, lon2", -1);
        return;
    }
    for (int i = 0; i < 4; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }

    double lat1 = sqlite3_value_double(argv[0]) * DEG_TO_RAD;
    double lon1 = sqlite3_value_double(argv[1]) * DEG_TO_RAD;
    double lat2 = sqlite3_value_double(argv[2]) * DEG_TO_RAD;
    double lon2 = sqlite3_value_double(argv[3]) * DEG_TO_RAD;

    double dlat = lat2 - lat1;
    double dlon = lon2 - lon1;
    double a = sin(dlat / 2) * sin(dlat / 2) +
               cos(lat1) * cos(lat2) * sin(dlon / 2) * sin(dlon / 2);
    double c = 2 * atan2(sqrt(a), sqrt(1 - a));

    sqlite3_result_double(ctx, EARTH_RADIUS_KM * c);
}

// Check if point is within bounding box
static void geoBboxContainsFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 6) {
        sqlite3_result_error(ctx, "geo_bbox_contains requires 6 args: minLat, maxLat, minLon, maxLon, lat, lon", -1);
        return;
    }
    for (int i = 0; i < 6; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }

    double minLat = sqlite3_value_double(argv[0]);
    double maxLat = sqlite3_value_double(argv[1]);
    double minLon = sqlite3_value_double(argv[2]);
    double maxLon = sqlite3_value_double(argv[3]);
    double lat = sqlite3_value_double(argv[4]);
    double lon = sqlite3_value_double(argv[5]);

    int contained = (lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon) ? 1 : 0;
    sqlite3_result_int(ctx, contained);
}

// Point in radius check (returns 1/0)
static void geoWithinRadiusFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 5) {
        sqlite3_result_error(ctx, "geo_within_radius requires 5 args: centerLat, centerLon, lat, lon, radiusKm", -1);
        return;
    }
    for (int i = 0; i < 5; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }

    double lat1 = sqlite3_value_double(argv[0]) * DEG_TO_RAD;
    double lon1 = sqlite3_value_double(argv[1]) * DEG_TO_RAD;
    double lat2 = sqlite3_value_double(argv[2]) * DEG_TO_RAD;
    double lon2 = sqlite3_value_double(argv[3]) * DEG_TO_RAD;
    double radiusKm = sqlite3_value_double(argv[4]);

    double dlat = lat2 - lat1;
    double dlon = lon2 - lon1;
    double a = sin(dlat / 2) * sin(dlat / 2) +
               cos(lat1) * cos(lat2) * sin(dlon / 2) * sin(dlon / 2);
    double c = 2 * atan2(sqrt(a), sqrt(1 - a));
    double distance = EARTH_RADIUS_KM * c;

    sqlite3_result_int(ctx, distance <= radiusKm ? 1 : 0);
}

void registerGeoFunctions(sqlite3* db) {
    int flags = SQLITE_UTF8 | SQLITE_DETERMINISTIC;
    sqlite3_create_function(db, "geo_distance", 4, flags, nullptr, geoDistanceFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_bbox_contains", 6, flags, nullptr, geoBboxContainsFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_within_radius", 5, flags, nullptr, geoWithinRadiusFunc, nullptr, nullptr);
}

}  // namespace flatsql
