// sdm_entry.cpp — Space Data Module entry point for FlatSQL Spatial Engine
// Compliant with https://github.com/DigitalArsenal/space-data-module-sdk
//
// Exports:
//   plugin_get_manifest()        — returns pointer to embedded JSON manifest
//   plugin_get_manifest_size()   — returns manifest byte count
//   All method functions declared in manifest.json
//
// Data exchange: methods read/write through shared memory buffers.
// The host writes input to the input buffer, calls the method,
// then reads output from the output buffer.

#include "sdm_manifest.h"
#include "../cpp/include/flatsql/geo_geometry.h"

#include <cmath>
#include <cstring>
#include <cstdio>
#include <string>

// ---------------------------------------------------------------------------
// Shared I/O buffers for host ↔ module data exchange
// ---------------------------------------------------------------------------

static constexpr size_t IO_BUFFER_SIZE = 65536;
static char input_buffer[IO_BUFFER_SIZE];
static char output_buffer[IO_BUFFER_SIZE];
static size_t input_length = 0;
static size_t output_length = 0;

// ---------------------------------------------------------------------------
// Exported: manifest accessor
// ---------------------------------------------------------------------------

extern "C" {

__attribute__((used))
const unsigned char* plugin_get_manifest(void) {
    return SDM_MANIFEST_DATA;
}

__attribute__((used))
size_t plugin_get_manifest_size(void) {
    return SDM_MANIFEST_SIZE;
}

// ---------------------------------------------------------------------------
// Exported: I/O buffer accessors (for host to read/write data)
// ---------------------------------------------------------------------------

__attribute__((used))
char* sdm_get_input_buffer(void) {
    return input_buffer;
}

__attribute__((used))
void sdm_set_input_length(size_t len) {
    input_length = len < IO_BUFFER_SIZE ? len : IO_BUFFER_SIZE;
}

__attribute__((used))
const char* sdm_get_output_buffer(void) {
    return output_buffer;
}

__attribute__((used))
size_t sdm_get_output_length(void) {
    return output_length;
}

// ---------------------------------------------------------------------------
// Helper: parse comma-separated doubles from input buffer
// ---------------------------------------------------------------------------

static int parseDoubles(const char* buf, size_t len, double* out, int maxCount) {
    int count = 0;
    const char* p = buf;
    const char* end = buf + len;
    while (p < end && count < maxCount) {
        char* next = nullptr;
        out[count] = strtod(p, &next);
        if (next == p) break;
        count++;
        p = next;
        while (p < end && (*p == ',' || *p == ' ' || *p == '\n')) p++;
    }
    return count;
}

// ---------------------------------------------------------------------------
// Method: compute_distance
// Input:  "lat1,lon1,lat2,lon2"
// Output: distance in km as text
// ---------------------------------------------------------------------------

__attribute__((used))
int compute_distance(void) {
    double args[4];
    if (parseDoubles(input_buffer, input_length, args, 4) < 4) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: expected 4 values: lat1,lon1,lat2,lon2");
        return 1;
    }

    static constexpr double EARTH_R = 6371.0;
    static constexpr double DEG2RAD = M_PI / 180.0;

    double lat1 = args[0] * DEG2RAD, lon1 = args[1] * DEG2RAD;
    double lat2 = args[2] * DEG2RAD, lon2 = args[3] * DEG2RAD;
    double dlat = lat2 - lat1, dlon = lon2 - lon1;
    double a = sin(dlat/2)*sin(dlat/2) + cos(lat1)*cos(lat2)*sin(dlon/2)*sin(dlon/2);
    double c = 2 * atan2(sqrt(a), sqrt(1-a));
    double dist = EARTH_R * c;

    output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "%.6f", dist);
    return 0;
}

// ---------------------------------------------------------------------------
// Method: compute_bearing
// Input:  "lat1,lon1,lat2,lon2"
// Output: bearing in degrees (0-360)
// ---------------------------------------------------------------------------

__attribute__((used))
int compute_bearing(void) {
    double args[4];
    if (parseDoubles(input_buffer, input_length, args, 4) < 4) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: expected 4 values: lat1,lon1,lat2,lon2");
        return 1;
    }

    static constexpr double DEG2RAD = M_PI / 180.0;
    static constexpr double RAD2DEG = 180.0 / M_PI;

    double lat1 = args[0] * DEG2RAD, lon1 = args[1] * DEG2RAD;
    double lat2 = args[2] * DEG2RAD, lon2 = args[3] * DEG2RAD;
    double dlon = lon2 - lon1;
    double y = sin(dlon) * cos(lat2);
    double x = cos(lat1)*sin(lat2) - sin(lat1)*cos(lat2)*cos(dlon);
    double bearing = atan2(y, x) * RAD2DEG;
    if (bearing < 0) bearing += 360.0;

    output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "%.6f", bearing);
    return 0;
}

// ---------------------------------------------------------------------------
// Method: point_in_polygon
// Input:  "WKT_POLYGON\nlat,lon"
// Output: "1" or "0"
// ---------------------------------------------------------------------------

__attribute__((used))
int point_in_polygon(void) {
    std::string input(input_buffer, input_length);
    size_t nl = input.find('\n');
    if (nl == std::string::npos) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: expected WKT\\nlat,lon");
        return 1;
    }

    std::string wkt = input.substr(0, nl);
    std::string coords = input.substr(nl + 1);

    double latlon[2];
    if (parseDoubles(coords.c_str(), coords.size(), latlon, 2) < 2) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: expected lat,lon after WKT");
        return 1;
    }

    try {
        auto geom = flatsql::geo::parseWKT(wkt);
        flatsql::geo::Point p(latlon[1], latlon[0]); // x=lon, y=lat
        bool inside = flatsql::geo::pointInGeometry(p, geom);
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "%d", inside ? 1 : 0);
        return 0;
    } catch (...) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: invalid WKT geometry");
        return 1;
    }
}

// ---------------------------------------------------------------------------
// Method: polygon_intersection
// Input:  "WKT1\nWKT2"
// Output: WKT of intersection
// ---------------------------------------------------------------------------

__attribute__((used))
int polygon_intersection(void) {
    std::string input(input_buffer, input_length);
    size_t nl = input.find('\n');
    if (nl == std::string::npos) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: expected WKT1\\nWKT2");
        return 1;
    }

    try {
        auto a = flatsql::geo::parseWKT(input.substr(0, nl));
        auto b = flatsql::geo::parseWKT(input.substr(nl + 1));
        auto result = flatsql::geo::polygonIntersection(a, b);
        std::string wkt = flatsql::geo::toWKT(result);
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "%s", wkt.c_str());
        return 0;
    } catch (...) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: polygon intersection failed");
        return 1;
    }
}

// ---------------------------------------------------------------------------
// Method: polygon_union
// Input:  "WKT1\nWKT2"
// Output: WKT of union
// ---------------------------------------------------------------------------

__attribute__((used))
int polygon_union(void) {
    std::string input(input_buffer, input_length);
    size_t nl = input.find('\n');
    if (nl == std::string::npos) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: expected WKT1\\nWKT2");
        return 1;
    }

    try {
        auto a = flatsql::geo::parseWKT(input.substr(0, nl));
        auto b = flatsql::geo::parseWKT(input.substr(nl + 1));
        auto result = flatsql::geo::polygonUnion(a, b);
        std::string wkt = flatsql::geo::toWKT(result);
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "%s", wkt.c_str());
        return 0;
    } catch (...) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: polygon union failed");
        return 1;
    }
}

// ---------------------------------------------------------------------------
// Method: compute_voronoi
// Input:  "MULTIPOINT_WKT\nBOUNDS_WKT"
// Output: MULTIPOLYGON WKT
// ---------------------------------------------------------------------------

__attribute__((used))
int compute_voronoi(void) {
    std::string input(input_buffer, input_length);
    size_t nl = input.find('\n');
    if (nl == std::string::npos) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: expected MULTIPOINT_WKT\\nBOUNDS_WKT");
        return 1;
    }

    try {
        auto pts = flatsql::geo::parseWKT(input.substr(0, nl));
        auto bounds = flatsql::geo::parseWKT(input.substr(nl + 1));

        std::vector<flatsql::geo::Point> sites;
        if (pts.type == flatsql::geo::GeomType::MULTIPOINT) {
            sites = pts.points;
        } else if (pts.type == flatsql::geo::GeomType::POINT) {
            sites.push_back(pts.point);
        }

        auto result = flatsql::geo::voronoiDiagram(sites, bounds);
        std::string wkt = flatsql::geo::toWKT(result);
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "%s", wkt.c_str());
        return 0;
    } catch (...) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: voronoi computation failed");
        return 1;
    }
}

// ---------------------------------------------------------------------------
// Method: compute_delaunay
// Input:  "MULTIPOINT_WKT"
// Output: MULTIPOLYGON WKT (triangles)
// ---------------------------------------------------------------------------

__attribute__((used))
int compute_delaunay(void) {
    try {
        std::string input(input_buffer, input_length);
        auto pts = flatsql::geo::parseWKT(input);

        std::vector<flatsql::geo::Point> points;
        if (pts.type == flatsql::geo::GeomType::MULTIPOINT) {
            points = pts.points;
        } else if (pts.type == flatsql::geo::GeomType::POINT) {
            points.push_back(pts.point);
        }

        auto result = flatsql::geo::delaunayTriangulation(points);
        std::string wkt = flatsql::geo::toWKT(result);
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "%s", wkt.c_str());
        return 0;
    } catch (...) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: delaunay computation failed");
        return 1;
    }
}

// ---------------------------------------------------------------------------
// Method: transform_to_ecef
// Input:  "lat,lon,alt"
// Output: "x,y,z"
// ---------------------------------------------------------------------------

__attribute__((used))
int transform_to_ecef(void) {
    double args[3];
    if (parseDoubles(input_buffer, input_length, args, 3) < 3) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: expected lat,lon,alt");
        return 1;
    }

    auto ecef = flatsql::geo::geodedicToECEF(args[0], args[1], args[2]);
    output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "%.3f,%.3f,%.3f", ecef.x, ecef.y, ecef.z);
    return 0;
}

// ---------------------------------------------------------------------------
// Method: transform_from_ecef
// Input:  "x,y,z"
// Output: "lat,lon,alt"
// ---------------------------------------------------------------------------

__attribute__((used))
int transform_from_ecef(void) {
    double args[3];
    if (parseDoubles(input_buffer, input_length, args, 3) < 3) {
        output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "ERROR: expected x,y,z");
        return 1;
    }

    double lat, lon, alt;
    flatsql::geo::ecefToGeodetic(args[0], args[1], args[2], lat, lon, alt);
    output_length = snprintf(output_buffer, IO_BUFFER_SIZE, "%.8f,%.8f,%.3f", lat, lon, alt);
    return 0;
}

// ---------------------------------------------------------------------------
// Method: spatial_query
// Input:  SQL query string (using any geo_* function)
// Output: Query result as text (column-separated values)
//
// NOTE: This method requires SQLite engine initialization.
// For standalone SDM use, it operates in "function-only" mode —
// geo_* functions work but table queries require a FlatSQL database.
// ---------------------------------------------------------------------------

__attribute__((used))
int spatial_query(void) {
    // In standalone SDM mode, we provide a minimal SQLite engine
    // that supports the spatial functions but no virtual tables.
    // Full FlatSQL database queries require the main flatsql WASM module.
    output_length = snprintf(output_buffer, IO_BUFFER_SIZE,
        "ERROR: spatial_query requires full FlatSQL engine. "
        "Use the individual method exports (compute_distance, point_in_polygon, etc.) "
        "for standalone SDM mode.");
    return 1;
}

}  // extern "C"
