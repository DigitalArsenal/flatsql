#include "flatsql/geo_functions.h"
#include "flatsql/geo_geometry.h"
#include <cmath>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace flatsql {

static constexpr double EARTH_RADIUS_KM = 6371.0;
static constexpr double DEG_TO_RAD = M_PI / 180.0;
static constexpr double RAD_TO_DEG = 180.0 / M_PI;

// ---------------------------------------------------------------------------
// Geometry helpers
// ---------------------------------------------------------------------------

static geo::Geometry extractGeometry(sqlite3_context* ctx, sqlite3_value* val) {
    if (sqlite3_value_type(val) == SQLITE_TEXT) {
        const char* text = reinterpret_cast<const char*>(sqlite3_value_text(val));
        return geo::parseWKT(text);
    } else if (sqlite3_value_type(val) == SQLITE_BLOB) {
        const uint8_t* data = static_cast<const uint8_t*>(sqlite3_value_blob(val));
        int len = sqlite3_value_bytes(val);
        return geo::blobToGeometry(data, static_cast<size_t>(len));
    }
    throw std::runtime_error("Expected WKT text or geometry blob");
}

static void resultGeometry(sqlite3_context* ctx, const geo::Geometry& geom) {
    auto blob = geo::geometryToBlob(geom);
    sqlite3_result_blob(ctx, blob.data(), static_cast<int>(blob.size()), SQLITE_TRANSIENT);
}

// ---------------------------------------------------------------------------
// Existing functions
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// New point operations
// ---------------------------------------------------------------------------

// Forward azimuth from point 1 to point 2 (degrees 0-360)
static void geoBearingFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 4) {
        sqlite3_result_error(ctx, "geo_bearing requires 4 args: lat1, lon1, lat2, lon2", -1);
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

    double dlon = lon2 - lon1;
    double y = sin(dlon) * cos(lat2);
    double x = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon);
    double bearing = atan2(y, x) * RAD_TO_DEG;

    // Normalize to 0-360
    bearing = fmod(bearing + 360.0, 360.0);
    sqlite3_result_double(ctx, bearing);
}

// Destination point given start, bearing, distance
static void geoDestinationFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 4) {
        sqlite3_result_error(ctx, "geo_destination requires 4 args: lat, lon, bearing_deg, distance_km", -1);
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
    double brng = sqlite3_value_double(argv[2]) * DEG_TO_RAD;
    double d = sqlite3_value_double(argv[3]);

    double dR = d / EARTH_RADIUS_KM;
    double lat2 = asin(sin(lat1) * cos(dR) + cos(lat1) * sin(dR) * cos(brng));
    double lon2 = lon1 + atan2(sin(brng) * sin(dR) * cos(lat1),
                               cos(dR) - sin(lat1) * sin(lat2));

    char buf[64];
    snprintf(buf, sizeof(buf), "%.8f,%.8f", lat2 * RAD_TO_DEG, lon2 * RAD_TO_DEG);
    sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
}

// Great circle midpoint
static void geoMidpointFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 4) {
        sqlite3_result_error(ctx, "geo_midpoint requires 4 args: lat1, lon1, lat2, lon2", -1);
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

    double dlon = lon2 - lon1;
    double Bx = cos(lat2) * cos(dlon);
    double By = cos(lat2) * sin(dlon);
    double lat3 = atan2(sin(lat1) + sin(lat2),
                        sqrt((cos(lat1) + Bx) * (cos(lat1) + Bx) + By * By));
    double lon3 = lon1 + atan2(By, cos(lat1) + Bx);

    char buf[64];
    snprintf(buf, sizeof(buf), "%.8f,%.8f", lat3 * RAD_TO_DEG, lon3 * RAD_TO_DEG);
    sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
}

// Spherical area of bounding box (sq km)
static void geoAreaBboxFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 4) {
        sqlite3_result_error(ctx, "geo_area_bbox requires 4 args: minLat, maxLat, minLon, maxLon", -1);
        return;
    }
    for (int i = 0; i < 4; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }

    double minLat = sqlite3_value_double(argv[0]) * DEG_TO_RAD;
    double maxLat = sqlite3_value_double(argv[1]) * DEG_TO_RAD;
    double minLon = sqlite3_value_double(argv[2]) * DEG_TO_RAD;
    double maxLon = sqlite3_value_double(argv[3]) * DEG_TO_RAD;

    double area = EARTH_RADIUS_KM * EARTH_RADIUS_KM *
                  fabs(sin(maxLat) - sin(minLat)) *
                  fabs(maxLon - minLon);
    sqlite3_result_double(ctx, area);
}

// ---------------------------------------------------------------------------
// Geohash functions
// ---------------------------------------------------------------------------

static const char GEOHASH_BASE32[] = "0123456789bcdefghjkmnpqrstuvwxyz";

static void geoGeohashEncodeFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 2 && argc != 3) {
        sqlite3_result_error(ctx, "geo_geohash_encode requires 2 or 3 args: lat, lon [, precision]", -1);
        return;
    }
    for (int i = 0; i < argc; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }

    double lat = sqlite3_value_double(argv[0]);
    double lon = sqlite3_value_double(argv[1]);
    int precision = (argc == 3) ? sqlite3_value_int(argv[2]) : 12;
    if (precision < 1) precision = 1;
    if (precision > 22) precision = 22;

    double latMin = -90.0, latMax = 90.0;
    double lonMin = -180.0, lonMax = 180.0;
    bool isLon = true;
    int bit = 0;
    int ch = 0;
    std::string hash;
    hash.reserve(precision);

    while (static_cast<int>(hash.size()) < precision) {
        double mid;
        if (isLon) {
            mid = (lonMin + lonMax) / 2.0;
            if (lon >= mid) {
                ch |= (1 << (4 - bit));
                lonMin = mid;
            } else {
                lonMax = mid;
            }
        } else {
            mid = (latMin + latMax) / 2.0;
            if (lat >= mid) {
                ch |= (1 << (4 - bit));
                latMin = mid;
            } else {
                latMax = mid;
            }
        }
        isLon = !isLon;
        bit++;
        if (bit == 5) {
            hash += GEOHASH_BASE32[ch];
            bit = 0;
            ch = 0;
        }
    }

    sqlite3_result_text(ctx, hash.c_str(), static_cast<int>(hash.size()), SQLITE_TRANSIENT);
}

static void geohashDecodeBounds(const char* hash, size_t len,
                                double& latMin, double& latMax,
                                double& lonMin, double& lonMax) {
    latMin = -90.0; latMax = 90.0;
    lonMin = -180.0; lonMax = 180.0;
    bool isLon = true;

    for (size_t i = 0; i < len; i++) {
        int idx = -1;
        char c = hash[i];
        for (int j = 0; j < 32; j++) {
            if (GEOHASH_BASE32[j] == c) { idx = j; break; }
        }
        if (idx < 0) break;

        for (int bit = 4; bit >= 0; bit--) {
            if (isLon) {
                double mid = (lonMin + lonMax) / 2.0;
                if (idx & (1 << bit)) lonMin = mid; else lonMax = mid;
            } else {
                double mid = (latMin + latMax) / 2.0;
                if (idx & (1 << bit)) latMin = mid; else latMax = mid;
            }
            isLon = !isLon;
        }
    }
}

static void geoGeohashDecodeLatFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_error(ctx, "geo_geohash_decode_lat requires 1 arg: geohash", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    const char* hash = reinterpret_cast<const char*>(sqlite3_value_text(argv[0]));
    size_t len = strlen(hash);
    double latMin, latMax, lonMin, lonMax;
    geohashDecodeBounds(hash, len, latMin, latMax, lonMin, lonMax);
    sqlite3_result_double(ctx, (latMin + latMax) / 2.0);
}

static void geoGeohashDecodeLonFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_error(ctx, "geo_geohash_decode_lon requires 1 arg: geohash", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    const char* hash = reinterpret_cast<const char*>(sqlite3_value_text(argv[0]));
    size_t len = strlen(hash);
    double latMin, latMax, lonMin, lonMax;
    geohashDecodeBounds(hash, len, latMin, latMax, lonMin, lonMax);
    sqlite3_result_double(ctx, (lonMin + lonMax) / 2.0);
}

// ---------------------------------------------------------------------------
// Geometry-based SQL functions
// ---------------------------------------------------------------------------

// geo_from_text(wkt) -> blob
static void geoFromTextFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_error(ctx, "geo_from_text requires 1 arg: wkt_text", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    try {
        const char* wkt = reinterpret_cast<const char*>(sqlite3_value_text(argv[0]));
        auto geom = geo::parseWKT(wkt);
        resultGeometry(ctx, geom);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_as_text(geom) -> WKT text
static void geoAsTextFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_error(ctx, "geo_as_text requires 1 arg: geom", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    try {
        // If already text, pass through
        if (sqlite3_value_type(argv[0]) == SQLITE_TEXT) {
            const char* text = reinterpret_cast<const char*>(sqlite3_value_text(argv[0]));
            sqlite3_result_text(ctx, text, -1, SQLITE_TRANSIENT);
            return;
        }
        auto geom = extractGeometry(ctx, argv[0]);
        std::string wkt = geo::toWKT(geom);
        sqlite3_result_text(ctx, wkt.c_str(), static_cast<int>(wkt.size()), SQLITE_TRANSIENT);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_from_geojson(json) -> blob
static void geoFromGeoJSONFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_error(ctx, "geo_from_geojson requires 1 arg: json_text", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    try {
        const char* json = reinterpret_cast<const char*>(sqlite3_value_text(argv[0]));
        auto geom = geo::parseGeoJSON(json);
        resultGeometry(ctx, geom);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_as_geojson(geom) -> GeoJSON text
static void geoAsGeoJSONFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_error(ctx, "geo_as_geojson requires 1 arg: geom", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    try {
        auto geom = extractGeometry(ctx, argv[0]);
        std::string json = geo::toGeoJSON(geom);
        sqlite3_result_text(ctx, json.c_str(), static_cast<int>(json.size()), SQLITE_TRANSIENT);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_contains(geom, lat, lon) -> 0/1
static void geoContainsFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 3) {
        sqlite3_result_error(ctx, "geo_contains requires 3 args: geom, lat, lon", -1);
        return;
    }
    for (int i = 0; i < 3; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }
    try {
        auto geom = extractGeometry(ctx, argv[0]);
        double lat = sqlite3_value_double(argv[1]);
        double lon = sqlite3_value_double(argv[2]);
        geo::Point p(lon, lat);  // Point is (x=lon, y=lat)
        sqlite3_result_int(ctx, geo::pointInGeometry(p, geom) ? 1 : 0);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_intersection(geom1, geom2) -> blob
static void geoIntersectionFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 2) {
        sqlite3_result_error(ctx, "geo_intersection requires 2 args: geom1, geom2", -1);
        return;
    }
    for (int i = 0; i < 2; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }
    try {
        auto g1 = extractGeometry(ctx, argv[0]);
        auto g2 = extractGeometry(ctx, argv[1]);
        auto result = geo::polygonIntersection(g1, g2);
        resultGeometry(ctx, result);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_union(geom1, geom2) -> blob
static void geoUnionFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 2) {
        sqlite3_result_error(ctx, "geo_union requires 2 args: geom1, geom2", -1);
        return;
    }
    for (int i = 0; i < 2; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }
    try {
        auto g1 = extractGeometry(ctx, argv[0]);
        auto g2 = extractGeometry(ctx, argv[1]);
        auto result = geo::polygonUnion(g1, g2);
        resultGeometry(ctx, result);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_difference(geom1, geom2) -> blob
static void geoDifferenceFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 2) {
        sqlite3_result_error(ctx, "geo_difference requires 2 args: geom1, geom2", -1);
        return;
    }
    for (int i = 0; i < 2; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }
    try {
        auto g1 = extractGeometry(ctx, argv[0]);
        auto g2 = extractGeometry(ctx, argv[1]);
        auto result = geo::polygonDifference(g1, g2);
        resultGeometry(ctx, result);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_sym_difference(geom1, geom2) -> blob
static void geoSymDifferenceFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 2) {
        sqlite3_result_error(ctx, "geo_sym_difference requires 2 args: geom1, geom2", -1);
        return;
    }
    for (int i = 0; i < 2; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }
    try {
        auto g1 = extractGeometry(ctx, argv[0]);
        auto g2 = extractGeometry(ctx, argv[1]);
        auto result = geo::polygonSymDifference(g1, g2);
        resultGeometry(ctx, result);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_buffer(geom, distance_deg) -> blob
static void geoBufferFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 2) {
        sqlite3_result_error(ctx, "geo_buffer requires 2 args: geom, distance_deg", -1);
        return;
    }
    for (int i = 0; i < 2; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }
    try {
        auto geom = extractGeometry(ctx, argv[0]);
        double dist = sqlite3_value_double(argv[1]);
        auto result = geo::polygonBuffer(geom, dist);
        resultGeometry(ctx, result);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_area_geom(geom) -> double (sq degrees)
static void geoAreaGeomFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_error(ctx, "geo_area_geom requires 1 arg: geom", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    try {
        auto geom = extractGeometry(ctx, argv[0]);
        sqlite3_result_double(ctx, geo::polygonArea(geom));
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_centroid(geom) -> blob (POINT)
static void geoCentroidFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_error(ctx, "geo_centroid requires 1 arg: geom", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    try {
        auto geom = extractGeometry(ctx, argv[0]);
        auto c = geo::centroid(geom);
        auto result = geo::Geometry::makePoint(c.x, c.y);
        resultGeometry(ctx, result);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_length_geom(geom) -> double (km)
static void geoLengthGeomFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_error(ctx, "geo_length_geom requires 1 arg: geom", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    try {
        auto geom = extractGeometry(ctx, argv[0]);
        sqlite3_result_double(ctx, geo::lineLength(geom));
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_envelope(geom) -> blob (POLYGON)
static void geoEnvelopeFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_error(ctx, "geo_envelope requires 1 arg: geom", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    try {
        auto geom = extractGeometry(ctx, argv[0]);
        auto result = geo::envelope(geom);
        resultGeometry(ctx, result);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_convex_hull(geom) -> blob
static void geoConvexHullFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_error(ctx, "geo_convex_hull requires 1 arg: geom", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    try {
        auto geom = extractGeometry(ctx, argv[0]);
        auto result = geo::convexHull(geom);
        resultGeometry(ctx, result);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_voronoi(multipoint_geom, bounds_geom) -> blob (MULTIPOLYGON)
static void geoVoronoiFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 2) {
        sqlite3_result_error(ctx, "geo_voronoi requires 2 args: multipoint_geom, bounds_geom", -1);
        return;
    }
    for (int i = 0; i < 2; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }
    try {
        auto points = extractGeometry(ctx, argv[0]);
        auto bounds = extractGeometry(ctx, argv[1]);

        // Collect points from the multipoint geometry
        std::vector<geo::Point> sites;
        if (points.type == geo::GeomType::MULTIPOINT) {
            sites = points.points;
        } else if (points.type == geo::GeomType::POINT) {
            sites.push_back(points.point);
        } else {
            sqlite3_result_error(ctx, "geo_voronoi: first argument must be a POINT or MULTIPOINT geometry", -1);
            return;
        }

        auto result = geo::voronoiDiagram(sites, bounds);
        resultGeometry(ctx, result);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// geo_delaunay(multipoint_geom) -> blob (MULTIPOLYGON)
static void geoDelaunayFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_error(ctx, "geo_delaunay requires 1 arg: multipoint_geom", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    try {
        auto points = extractGeometry(ctx, argv[0]);

        std::vector<geo::Point> pts;
        if (points.type == geo::GeomType::MULTIPOINT) {
            pts = points.points;
        } else if (points.type == geo::GeomType::POINT) {
            pts.push_back(points.point);
        } else {
            sqlite3_result_error(ctx, "geo_delaunay: argument must be a POINT or MULTIPOINT geometry", -1);
            return;
        }

        auto result = geo::delaunayTriangulation(pts);
        resultGeometry(ctx, result);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// ---------------------------------------------------------------------------
// Coordinate transforms
// ---------------------------------------------------------------------------

// geo_to_ecef(lat, lon, alt) -> text "x,y,z"
static void geoToEcefFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 3) {
        sqlite3_result_error(ctx, "geo_to_ecef requires 3 args: lat, lon, alt", -1);
        return;
    }
    for (int i = 0; i < 3; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }

    double lat = sqlite3_value_double(argv[0]);
    double lon = sqlite3_value_double(argv[1]);
    double alt = sqlite3_value_double(argv[2]);

    auto ecef = geo::geodedicToECEF(lat, lon, alt);
    char buf[128];
    snprintf(buf, sizeof(buf), "%.3f,%.3f,%.3f", ecef.x, ecef.y, ecef.z);
    sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
}

// geo_from_ecef(x, y, z) -> text "lat,lon,alt"
static void geoFromEcefFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 3) {
        sqlite3_result_error(ctx, "geo_from_ecef requires 3 args: x, y, z", -1);
        return;
    }
    for (int i = 0; i < 3; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            sqlite3_result_null(ctx);
            return;
        }
    }

    double x = sqlite3_value_double(argv[0]);
    double y = sqlite3_value_double(argv[1]);
    double z = sqlite3_value_double(argv[2]);

    double lat, lon, alt;
    geo::ecefToGeodetic(x, y, z, lat, lon, alt);
    char buf[128];
    snprintf(buf, sizeof(buf), "%.8f,%.8f,%.3f", lat, lon, alt);
    sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

void registerGeoFunctions(sqlite3* db) {
    int flags = SQLITE_UTF8 | SQLITE_DETERMINISTIC;

    // Existing point operations
    sqlite3_create_function(db, "geo_distance", 4, flags, nullptr, geoDistanceFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_bbox_contains", 6, flags, nullptr, geoBboxContainsFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_within_radius", 5, flags, nullptr, geoWithinRadiusFunc, nullptr, nullptr);

    // New point operations
    sqlite3_create_function(db, "geo_bearing", 4, flags, nullptr, geoBearingFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_destination", 4, flags, nullptr, geoDestinationFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_midpoint", 4, flags, nullptr, geoMidpointFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_area_bbox", 4, flags, nullptr, geoAreaBboxFunc, nullptr, nullptr);

    // Geohash functions
    sqlite3_create_function(db, "geo_geohash_encode", 3, flags, nullptr, geoGeohashEncodeFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_geohash_encode", 2, flags, nullptr, geoGeohashEncodeFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_geohash_decode_lat", 1, flags, nullptr, geoGeohashDecodeLatFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_geohash_decode_lon", 1, flags, nullptr, geoGeohashDecodeLonFunc, nullptr, nullptr);

    // Geometry WKT/GeoJSON
    sqlite3_create_function(db, "geo_from_text", 1, flags, nullptr, geoFromTextFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_as_text", 1, flags, nullptr, geoAsTextFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_from_geojson", 1, flags, nullptr, geoFromGeoJSONFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_as_geojson", 1, flags, nullptr, geoAsGeoJSONFunc, nullptr, nullptr);

    // Geometry operations
    sqlite3_create_function(db, "geo_contains", 3, flags, nullptr, geoContainsFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_intersection", 2, flags, nullptr, geoIntersectionFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_union", 2, flags, nullptr, geoUnionFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_difference", 2, flags, nullptr, geoDifferenceFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_sym_difference", 2, flags, nullptr, geoSymDifferenceFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_buffer", 2, flags, nullptr, geoBufferFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_area_geom", 1, flags, nullptr, geoAreaGeomFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_centroid", 1, flags, nullptr, geoCentroidFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_length_geom", 1, flags, nullptr, geoLengthGeomFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_envelope", 1, flags, nullptr, geoEnvelopeFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_convex_hull", 1, flags, nullptr, geoConvexHullFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_voronoi", 2, flags, nullptr, geoVoronoiFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_delaunay", 1, flags, nullptr, geoDelaunayFunc, nullptr, nullptr);

    // Coordinate transforms
    sqlite3_create_function(db, "geo_to_ecef", 3, flags, nullptr, geoToEcefFunc, nullptr, nullptr);
    sqlite3_create_function(db, "geo_from_ecef", 3, flags, nullptr, geoFromEcefFunc, nullptr, nullptr);
}

}  // namespace flatsql
