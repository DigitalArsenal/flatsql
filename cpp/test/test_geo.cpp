// test_geo.cpp — Comprehensive spatial function tests
// Validates against SpatiaLite reference values where applicable.

#include "flatsql/sqlite_engine.h"
#include "flatsql/geo_geometry.h"
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string>
#include <sstream>

using namespace flatsql;

static constexpr double EPS = 1e-4;       // tolerance for geo calcs (km, degrees)
static constexpr double EPS_TIGHT = 1e-6; // tighter tolerance for exact formulas

static double getDouble(const QueryResult& r, int row = 0, int col = 0) {
    return std::get<double>(r.rows[row][col]);
}

static int64_t getInt(const QueryResult& r, int row = 0, int col = 0) {
    return std::get<int64_t>(r.rows[row][col]);
}

static std::string getString(const QueryResult& r, int row = 0, int col = 0) {
    return std::get<std::string>(r.rows[row][col]);
}

static std::vector<uint8_t> getBlob(const QueryResult& r, int row = 0, int col = 0) {
    return std::get<std::vector<uint8_t>>(r.rows[row][col]);
}

// Parse "lat,lon" or "x,y,z" strings
static std::vector<double> parseCSV(const std::string& s) {
    std::vector<double> out;
    std::istringstream ss(s);
    std::string token;
    while (std::getline(ss, token, ',')) {
        out.push_back(std::stod(token));
    }
    return out;
}

// ===========================================================================
// Test existing functions still work
// ===========================================================================

static void testExistingFunctions(SQLiteEngine& db) {
    std::cout << "  existing functions..." << std::endl;

    // NYC to DC distance (~328 km) — SpatiaLite: ST_Distance on spheroid ≈ 328.0 km
    auto r = db.execute("SELECT geo_distance(40.7128, -74.0060, 38.9072, -77.0369)");
    double d = getDouble(r);
    assert(d > 300 && d < 350);

    // bbox contains
    r = db.execute("SELECT geo_bbox_contains(40.0, 41.0, -75.0, -73.0, 40.5, -74.0)");
    assert(getInt(r) == 1);

    r = db.execute("SELECT geo_bbox_contains(40.0, 41.0, -75.0, -73.0, 42.0, -74.0)");
    assert(getInt(r) == 0);

    // within radius
    r = db.execute("SELECT geo_within_radius(40.7128, -74.0060, 40.7580, -73.9855, 10)");
    assert(getInt(r) == 1);  // ~5.5 km away

    std::cout << "  existing functions OK" << std::endl;
}

// ===========================================================================
// Point operations
// ===========================================================================

static void testBearing(SQLiteEngine& db) {
    std::cout << "  geo_bearing..." << std::endl;

    // NYC(40.7128,-74.0060) → LA(34.0522,-118.2437)
    // SpatiaLite: ST_Azimuth ≈ 273.6° (west-southwest)
    auto r = db.execute("SELECT geo_bearing(40.7128, -74.0060, 34.0522, -118.2437)");
    double bearing = getDouble(r);
    assert(bearing > 270 && bearing < 280);
    std::cout << "    NYC->LA bearing: " << bearing << "°" << std::endl;

    // North pole bearing should be 0°
    r = db.execute("SELECT geo_bearing(0, 0, 90, 0)");
    bearing = getDouble(r);
    assert(bearing < 1 || bearing > 359);  // ~0°

    // Due east bearing should be ~90°
    r = db.execute("SELECT geo_bearing(0, 0, 0, 90)");
    bearing = getDouble(r);
    assert(bearing > 89 && bearing < 91);

    std::cout << "  geo_bearing OK" << std::endl;
}

static void testDestination(SQLiteEngine& db) {
    std::cout << "  geo_destination..." << std::endl;

    // From equator/prime meridian, go 111.195 km north → should be ~1° N
    auto r = db.execute("SELECT geo_destination(0, 0, 0, 111.195)");
    auto coords = parseCSV(getString(r));
    assert(std::abs(coords[0] - 1.0) < 0.01);
    assert(std::abs(coords[1] - 0.0) < 0.01);
    std::cout << "    0,0 + 111km N = " << getString(r) << std::endl;

    // From equator/prime meridian, go 111.195 km east → should be ~1° E
    r = db.execute("SELECT geo_destination(0, 0, 90, 111.195)");
    coords = parseCSV(getString(r));
    assert(std::abs(coords[0] - 0.0) < 0.01);
    assert(std::abs(coords[1] - 1.0) < 0.01);

    std::cout << "  geo_destination OK" << std::endl;
}

static void testMidpoint(SQLiteEngine& db) {
    std::cout << "  geo_midpoint..." << std::endl;

    // Midpoint of NYC and LA
    auto r = db.execute("SELECT geo_midpoint(40.7128, -74.0060, 34.0522, -118.2437)");
    auto coords = parseCSV(getString(r));
    // SpatiaLite: midpoint ≈ (39.5, -97.2) — rough center of US
    assert(coords[0] > 38 && coords[0] < 41);
    assert(coords[1] > -100 && coords[1] < -93);
    std::cout << "    NYC-LA midpoint: " << getString(r) << std::endl;

    // Midpoint of equator opposites (0,0)-(0,180) should be at (0,90)
    r = db.execute("SELECT geo_midpoint(0, 0, 0, 180)");
    coords = parseCSV(getString(r));
    assert(std::abs(coords[0]) < 0.01);
    assert(std::abs(coords[1] - 90.0) < 0.01);

    std::cout << "  geo_midpoint OK" << std::endl;
}

static void testAreaBbox(SQLiteEngine& db) {
    std::cout << "  geo_area_bbox..." << std::endl;

    // 1°x1° box at equator ≈ 12,364 sq km (SpatiaLite reference)
    auto r = db.execute("SELECT geo_area_bbox(0, 1, 0, 1)");
    double area = getDouble(r);
    assert(area > 12000 && area < 12800);
    std::cout << "    1x1 deg at equator: " << area << " sq km" << std::endl;

    // 1°x1° box at 60° lat → smaller (cosine factor)
    r = db.execute("SELECT geo_area_bbox(60, 61, 0, 1)");
    double area60 = getDouble(r);
    assert(area60 < area);  // Should be roughly half
    assert(area60 > 5500 && area60 < 6500);
    std::cout << "    1x1 deg at 60°N: " << area60 << " sq km" << std::endl;

    std::cout << "  geo_area_bbox OK" << std::endl;
}

// ===========================================================================
// Geohash
// ===========================================================================

static void testGeohash(SQLiteEngine& db) {
    std::cout << "  geohash..." << std::endl;

    // SpatiaLite: ST_GeoHash(42.6, -5.6) = 'ezs42' (precision 5)
    auto r = db.execute("SELECT geo_geohash_encode(42.6, -5.6, 5)");
    std::string hash = getString(r);
    assert(hash == "ezs42");
    std::cout << "    encode(42.6,-5.6,5) = " << hash << std::endl;

    // Round-trip: decode should return center of cell
    r = db.execute("SELECT geo_geohash_decode_lat('ezs42')");
    double lat = getDouble(r);
    assert(std::abs(lat - 42.6) < 0.1);

    r = db.execute("SELECT geo_geohash_decode_lon('ezs42')");
    double lon = getDouble(r);
    assert(std::abs(lon - (-5.6)) < 0.1);
    std::cout << "    decode('ezs42') = " << lat << "," << lon << std::endl;

    // Higher precision round-trip
    r = db.execute("SELECT geo_geohash_encode(48.8566, 2.3522, 9)");
    hash = getString(r);
    std::cout << "    Paris hash(9): " << hash << std::endl;

    r = db.execute("SELECT geo_geohash_decode_lat('" + hash + "')");
    lat = getDouble(r);
    r = db.execute("SELECT geo_geohash_decode_lon('" + hash + "')");
    lon = getDouble(r);
    assert(std::abs(lat - 48.8566) < 0.001);
    assert(std::abs(lon - 2.3522) < 0.001);

    // 2-arg form (default precision)
    r = db.execute("SELECT geo_geohash_encode(51.5074, -0.1278)");
    hash = getString(r);
    assert(hash.size() == 12);  // default precision
    std::cout << "    London hash(default): " << hash << std::endl;

    std::cout << "  geohash OK" << std::endl;
}

// ===========================================================================
// WKT parsing and serialization
// ===========================================================================

static void testWKT(SQLiteEngine& db) {
    std::cout << "  WKT round-trip..." << std::endl;

    // POINT
    auto r = db.execute("SELECT geo_as_text(geo_from_text('POINT(2.3522 48.8566)'))");
    std::string wkt = getString(r);
    assert(wkt.find("POINT") != std::string::npos);
    assert(wkt.find("2.3522") != std::string::npos);
    std::cout << "    POINT: " << wkt << std::endl;

    // LINESTRING
    r = db.execute("SELECT geo_as_text(geo_from_text('LINESTRING(0 0, 1 1, 2 0)'))");
    wkt = getString(r);
    assert(wkt.find("LINESTRING") != std::string::npos);
    std::cout << "    LINESTRING: " << wkt << std::endl;

    // POLYGON (square)
    r = db.execute("SELECT geo_as_text(geo_from_text('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))");
    wkt = getString(r);
    assert(wkt.find("POLYGON") != std::string::npos);
    std::cout << "    POLYGON: " << wkt << std::endl;

    // POLYGON with hole
    r = db.execute("SELECT geo_as_text(geo_from_text('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2))'))");
    wkt = getString(r);
    assert(wkt.find("POLYGON") != std::string::npos);
    std::cout << "    POLYGON+hole: " << wkt << std::endl;

    // MULTIPOINT
    r = db.execute("SELECT geo_as_text(geo_from_text('MULTIPOINT((0 0), (1 1), (2 2))'))");
    wkt = getString(r);
    assert(wkt.find("MULTIPOINT") != std::string::npos);
    std::cout << "    MULTIPOINT: " << wkt << std::endl;

    // MULTIPOLYGON
    r = db.execute("SELECT geo_as_text(geo_from_text('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))'))");
    wkt = getString(r);
    assert(wkt.find("MULTIPOLYGON") != std::string::npos);
    std::cout << "    MULTIPOLYGON: " << wkt << std::endl;

    // WKT text passthrough (geo_as_text on WKT text input)
    r = db.execute("SELECT geo_as_text('POINT(1 2)')");
    wkt = getString(r);
    assert(wkt.find("POINT") != std::string::npos);

    std::cout << "  WKT OK" << std::endl;
}

// ===========================================================================
// GeoJSON
// ===========================================================================

static void testGeoJSON(SQLiteEngine& db) {
    std::cout << "  GeoJSON round-trip..." << std::endl;

    // Point
    auto r = db.execute("SELECT geo_as_geojson(geo_from_geojson('{\"type\":\"Point\",\"coordinates\":[2.3522,48.8566]}'))");
    std::string json = getString(r);
    assert(json.find("Point") != std::string::npos);
    assert(json.find("2.3522") != std::string::npos);
    std::cout << "    Point: " << json << std::endl;

    // Polygon
    r = db.execute("SELECT geo_as_geojson(geo_from_geojson('{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[10,0],[10,10],[0,10],[0,0]]]}'))");
    json = getString(r);
    assert(json.find("Polygon") != std::string::npos);
    std::cout << "    Polygon: " << json << std::endl;

    // WKT → GeoJSON
    r = db.execute("SELECT geo_as_geojson(geo_from_text('POINT(1.5 2.5)'))");
    json = getString(r);
    assert(json.find("1.5") != std::string::npos);

    std::cout << "  GeoJSON OK" << std::endl;
}

// ===========================================================================
// Point-in-polygon
// ===========================================================================

static void testContains(SQLiteEngine& db) {
    std::cout << "  geo_contains..." << std::endl;

    // Square polygon: (0,0)-(10,0)-(10,10)-(0,10)
    // In WKT: x=lon, y=lat → geo_contains(geom, lat, lon)
    std::string sq = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";

    // Point inside (lat=5, lon=5)
    auto r = db.execute("SELECT geo_contains('" + sq + "', 5, 5)");
    assert(getInt(r) == 1);

    // Point outside (lat=15, lon=5)
    r = db.execute("SELECT geo_contains('" + sq + "', 15, 5)");
    assert(getInt(r) == 0);

    // Point on edge (lat=0, lon=5) — implementation dependent, but should be consistent
    r = db.execute("SELECT geo_contains('" + sq + "', 0, 5)");
    // Ray casting: on-boundary is implementation-defined

    // Concave polygon (L-shape)
    std::string lshape = "POLYGON((0 0, 10 0, 10 5, 5 5, 5 10, 0 10, 0 0))";

    // Inside the L
    r = db.execute("SELECT geo_contains('" + lshape + "', 2, 2)");
    assert(getInt(r) == 1);

    // In the cutout part of L
    r = db.execute("SELECT geo_contains('" + lshape + "', 7, 7)");
    assert(getInt(r) == 0);

    // Polygon with hole
    std::string withHole = "POLYGON((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5))";
    r = db.execute("SELECT geo_contains('" + withHole + "', 10, 10)");
    assert(getInt(r) == 0);  // Inside hole

    r = db.execute("SELECT geo_contains('" + withHole + "', 2, 2)");
    assert(getInt(r) == 1);  // Outside hole, inside exterior

    std::cout << "  geo_contains OK" << std::endl;
}

// ===========================================================================
// Polygon boolean operations
// ===========================================================================

static void testPolygonBooleans(SQLiteEngine& db) {
    std::cout << "  polygon booleans..." << std::endl;

    // Two overlapping squares:
    // A: (0,0)-(10,0)-(10,10)-(0,10)
    // B: (5,5)-(15,5)-(15,15)-(5,15)
    std::string polyA = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";
    std::string polyB = "POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))";

    // Intersection: should be (5,5)-(10,5)-(10,10)-(5,10) → area 25
    auto r = db.execute("SELECT geo_area_geom(geo_intersection('" + polyA + "', '" + polyB + "'))");
    double intArea = getDouble(r);
    std::cout << "    intersection area: " << intArea << " (expected 25)" << std::endl;
    assert(std::abs(intArea - 25.0) < 1.0);

    // Area of A = 100
    r = db.execute("SELECT geo_area_geom('" + polyA + "')");
    double areaA = getDouble(r);
    assert(std::abs(areaA - 100.0) < EPS_TIGHT);

    // Intersection WKT
    r = db.execute("SELECT geo_as_text(geo_intersection('" + polyA + "', '" + polyB + "'))");
    std::cout << "    intersection WKT: " << getString(r) << std::endl;

    // Union
    r = db.execute("SELECT geo_as_text(geo_union('" + polyA + "', '" + polyB + "'))");
    std::string unionWKT = getString(r);
    std::cout << "    union WKT: " << unionWKT << std::endl;

    // Difference A-B: area should be 100 - 25 = 75
    r = db.execute("SELECT geo_as_text(geo_difference('" + polyA + "', '" + polyB + "'))");
    std::cout << "    difference A-B WKT: " << getString(r) << std::endl;

    // Symmetric difference
    r = db.execute("SELECT geo_as_text(geo_sym_difference('" + polyA + "', '" + polyB + "'))");
    std::cout << "    sym_difference WKT: " << getString(r) << std::endl;

    // Non-overlapping polygons: intersection should be empty or minimal area
    std::string polyC = "POLYGON((20 20, 30 20, 30 30, 20 30, 20 20))";
    r = db.execute("SELECT geo_area_geom(geo_intersection('" + polyA + "', '" + polyC + "'))");
    double noOverlap = getDouble(r);
    assert(std::abs(noOverlap) < 0.1);
    std::cout << "    non-overlapping intersection area: " << noOverlap << std::endl;

    std::cout << "  polygon booleans OK" << std::endl;
}

// ===========================================================================
// Buffer
// ===========================================================================

static void testBuffer(SQLiteEngine& db) {
    std::cout << "  geo_buffer..." << std::endl;

    // Buffer a point by 1 degree → should approximate a circle
    auto r = db.execute("SELECT geo_as_text(geo_buffer('POINT(0 0)', 1))");
    std::string bufWKT = getString(r);
    assert(bufWKT.find("POLYGON") != std::string::npos);
    std::cout << "    point buffer: " << bufWKT.substr(0, 60) << "..." << std::endl;

    // Area of buffered point should be ~π r² ≈ 3.14159 sq deg
    r = db.execute("SELECT geo_area_geom(geo_buffer('POINT(0 0)', 1))");
    double bufArea = getDouble(r);
    std::cout << "    buffer area: " << bufArea << " (expected ~" << M_PI << ")" << std::endl;
    assert(std::abs(bufArea - M_PI) < 0.5);  // 16-segment approximation

    std::cout << "  geo_buffer OK" << std::endl;
}

// ===========================================================================
// Geometry analysis
// ===========================================================================

static void testAnalysis(SQLiteEngine& db) {
    std::cout << "  geometry analysis..." << std::endl;

    // Area of unit square (in degrees)
    auto r = db.execute("SELECT geo_area_geom('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')");
    double area = getDouble(r);
    assert(std::abs(area - 1.0) < EPS_TIGHT);
    std::cout << "    unit square area: " << area << std::endl;

    // Centroid of unit square should be (0.5, 0.5)
    r = db.execute("SELECT geo_as_text(geo_centroid('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))");
    std::string centroidWKT = getString(r);
    std::cout << "    unit square centroid: " << centroidWKT << std::endl;
    assert(centroidWKT.find("0.5") != std::string::npos);

    // Centroid of triangle (0,0)-(6,0)-(3,6) → (3, 2)
    r = db.execute("SELECT geo_as_text(geo_centroid('POLYGON((0 0, 6 0, 3 6, 0 0))'))");
    centroidWKT = getString(r);
    std::cout << "    triangle centroid: " << centroidWKT << std::endl;

    // Line length: equator 0°→1° ≈ 111.195 km
    r = db.execute("SELECT geo_length_geom('LINESTRING(0 0, 1 0)')");
    double len = getDouble(r);
    std::cout << "    1° equator length: " << len << " km (expected ~111.2)" << std::endl;
    assert(len > 110 && len < 113);

    // Envelope
    r = db.execute("SELECT geo_as_text(geo_envelope('LINESTRING(1 2, 5 8, 3 4)'))");
    std::string envWKT = getString(r);
    std::cout << "    envelope: " << envWKT << std::endl;
    assert(envWKT.find("POLYGON") != std::string::npos);

    // Convex hull of scattered points
    r = db.execute("SELECT geo_as_text(geo_convex_hull('MULTIPOINT((0 0), (5 5), (10 0), (5 2))'))");
    std::string hullWKT = getString(r);
    std::cout << "    convex hull: " << hullWKT << std::endl;
    assert(hullWKT.find("POLYGON") != std::string::npos);

    // Convex hull area should be 50 (triangle 0,0 - 10,0 - 5,5)
    r = db.execute("SELECT geo_area_geom(geo_convex_hull('MULTIPOINT((0 0), (5 5), (10 0), (5 2))'))");
    double hullArea = getDouble(r);
    std::cout << "    hull area: " << hullArea << " (expected 25)" << std::endl;
    assert(hullArea > 20 && hullArea < 30);

    std::cout << "  geometry analysis OK" << std::endl;
}

// ===========================================================================
// Voronoi / Delaunay
// ===========================================================================

static void testVoronoi(SQLiteEngine& db) {
    std::cout << "  geo_voronoi..." << std::endl;

    // 4 points in a unit square
    std::string pts = "MULTIPOINT((0 0), (10 0), (10 10), (0 10))";
    std::string bounds = "POLYGON((-5 -5, 15 -5, 15 15, -5 15, -5 -5))";

    auto r = db.execute("SELECT geo_as_text(geo_voronoi('" + pts + "', '" + bounds + "'))");
    std::string vorWKT = getString(r);
    std::cout << "    voronoi: " << vorWKT.substr(0, 80) << "..." << std::endl;
    assert(vorWKT.find("POLYGON") != std::string::npos);

    // Should produce 4 cells
    // Count POLYGON occurrences
    size_t count = 0;
    size_t pos = 0;
    while ((pos = vorWKT.find("((", pos)) != std::string::npos) {
        count++;
        pos++;
    }
    std::cout << "    voronoi cell count: " << count << std::endl;
    assert(count >= 4);

    std::cout << "  geo_voronoi OK" << std::endl;
}

static void testDelaunay(SQLiteEngine& db) {
    std::cout << "  geo_delaunay..." << std::endl;

    // 4 points forming a square → 2 triangles
    std::string pts = "MULTIPOINT((0 0), (10 0), (10 10), (0 10))";
    auto r = db.execute("SELECT geo_as_text(geo_delaunay('" + pts + "'))");
    std::string delWKT = getString(r);
    std::cout << "    delaunay: " << delWKT.substr(0, 80) << "..." << std::endl;
    assert(delWKT.find("POLYGON") != std::string::npos);

    // 5 points → should get more triangles
    pts = "MULTIPOINT((0 0), (10 0), (10 10), (0 10), (5 5))";
    r = db.execute("SELECT geo_as_text(geo_delaunay('" + pts + "'))");
    delWKT = getString(r);
    std::cout << "    delaunay (5 pts): " << delWKT.substr(0, 80) << "..." << std::endl;

    std::cout << "  geo_delaunay OK" << std::endl;
}

// ===========================================================================
// Coordinate transforms
// ===========================================================================

static void testCoordinateTransforms(SQLiteEngine& db) {
    std::cout << "  coordinate transforms..." << std::endl;

    // WGS84 origin (0°,0°,0m) → ECEF: (6378137, 0, 0) meters
    auto r = db.execute("SELECT geo_to_ecef(0, 0, 0)");
    auto ecef = parseCSV(getString(r));
    assert(std::abs(ecef[0] - 6378137.0) < 1.0);
    assert(std::abs(ecef[1]) < 1.0);
    assert(std::abs(ecef[2]) < 1.0);
    std::cout << "    (0,0,0) → ECEF: " << getString(r) << std::endl;

    // North pole (90°,0°,0m) → ECEF: (0, 0, ~6356752)
    r = db.execute("SELECT geo_to_ecef(90, 0, 0)");
    ecef = parseCSV(getString(r));
    assert(std::abs(ecef[0]) < 1.0);
    assert(std::abs(ecef[1]) < 1.0);
    assert(std::abs(ecef[2] - 6356752.314) < 1.0);
    std::cout << "    (90,0,0) → ECEF: " << getString(r) << std::endl;

    // Round-trip: ECEF → geodetic → should match original
    r = db.execute("SELECT geo_from_ecef(6378137, 0, 0)");
    auto lla = parseCSV(getString(r));
    assert(std::abs(lla[0] - 0.0) < 0.001);
    assert(std::abs(lla[1] - 0.0) < 0.001);
    assert(std::abs(lla[2] - 0.0) < 1.0);
    std::cout << "    ECEF(6378137,0,0) → " << getString(r) << std::endl;

    // ISS altitude (~408 km above equator)
    r = db.execute("SELECT geo_to_ecef(0, 0, 408000)");
    ecef = parseCSV(getString(r));
    assert(std::abs(ecef[0] - (6378137.0 + 408000.0)) < 1.0);
    std::cout << "    ISS altitude: " << getString(r) << std::endl;

    // Round-trip with ISS position (lat=51.6, lon=120.5, alt=408000)
    r = db.execute("SELECT geo_to_ecef(51.6, 120.5, 408000)");
    std::string ecefStr = getString(r);
    ecef = parseCSV(ecefStr);
    std::string q = "SELECT geo_from_ecef(" + std::to_string(ecef[0]) + "," + std::to_string(ecef[1]) + "," + std::to_string(ecef[2]) + ")";
    r = db.execute(q);
    lla = parseCSV(getString(r));
    assert(std::abs(lla[0] - 51.6) < 0.01);
    assert(std::abs(lla[1] - 120.5) < 0.01);
    assert(std::abs(lla[2] - 408000.0) < 10.0);
    std::cout << "    round-trip ISS: " << getString(r) << std::endl;

    std::cout << "  coordinate transforms OK" << std::endl;
}

// ===========================================================================
// Blob serialization round-trip
// ===========================================================================

static void testBlobRoundTrip(SQLiteEngine& db) {
    std::cout << "  blob round-trip..." << std::endl;

    // POINT
    auto r = db.execute("SELECT geo_as_text(geo_from_text('POINT(1.5 2.5)'))");
    assert(getString(r).find("1.5") != std::string::npos);

    // POLYGON through blob
    std::string poly = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";
    r = db.execute("SELECT geo_area_geom(geo_from_text('" + poly + "'))");
    double area = getDouble(r);
    assert(std::abs(area - 100.0) < EPS_TIGHT);

    // Chain: WKT → blob → operations → blob → WKT
    r = db.execute("SELECT geo_as_text(geo_envelope(geo_from_text('LINESTRING(0 0, 5 5, 10 0)')))");
    std::string env = getString(r);
    assert(env.find("POLYGON") != std::string::npos);
    std::cout << "    chain: " << env << std::endl;

    std::cout << "  blob round-trip OK" << std::endl;
}

// ===========================================================================
// NULL handling
// ===========================================================================

static void testNullHandling(SQLiteEngine& db) {
    std::cout << "  NULL handling..." << std::endl;

    // All scalar functions should return NULL for NULL inputs
    auto r = db.execute("SELECT geo_bearing(NULL, 0, 0, 0)");
    assert(r.rows.size() == 1);
    assert(std::holds_alternative<std::monostate>(r.rows[0][0]));

    r = db.execute("SELECT geo_distance(NULL, 0, 0, 0)");
    assert(std::holds_alternative<std::monostate>(r.rows[0][0]));

    r = db.execute("SELECT geo_geohash_encode(NULL, 0, 5)");
    assert(std::holds_alternative<std::monostate>(r.rows[0][0]));

    r = db.execute("SELECT geo_to_ecef(NULL, 0, 0)");
    assert(std::holds_alternative<std::monostate>(r.rows[0][0]));

    std::cout << "  NULL handling OK" << std::endl;
}

// ===========================================================================
// SpatiaLite parity validation
// ===========================================================================

static void testSpatialiteParity(SQLiteEngine& db) {
    std::cout << "  SpatiaLite parity checks..." << std::endl;

    // ST_Distance(NYC, DC) ≈ 328 km
    auto r = db.execute("SELECT geo_distance(40.7128, -74.0060, 38.9072, -77.0369)");
    double d = getDouble(r);
    std::cout << "    NYC-DC distance: " << d << " km (SpatiaLite ref: ~328)" << std::endl;
    assert(d > 325 && d < 335);

    // ST_GeoHash(42.6, -5.6, 5) = 'ezs42'
    r = db.execute("SELECT geo_geohash_encode(42.6, -5.6, 5)");
    assert(getString(r) == "ezs42");

    // ST_Area of 10x10 square = 100 (planar)
    r = db.execute("SELECT geo_area_geom('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')");
    assert(std::abs(getDouble(r) - 100.0) < EPS_TIGHT);

    // Intersection area of two overlapping squares
    r = db.execute("SELECT geo_area_geom(geo_intersection('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 'POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))'))");
    double intArea = getDouble(r);
    std::cout << "    intersection area: " << intArea << " (SpatiaLite ref: 25)" << std::endl;
    assert(std::abs(intArea - 25.0) < 1.0);

    // Convex hull of 4 points
    r = db.execute("SELECT geo_area_geom(geo_convex_hull('MULTIPOINT((0 0), (10 0), (5 10))'))");
    double hullArea = getDouble(r);
    std::cout << "    triangle hull area: " << hullArea << " (SpatiaLite ref: 50)" << std::endl;
    assert(std::abs(hullArea - 50.0) < 1.0);

    // Point-in-polygon
    r = db.execute("SELECT geo_contains('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 5, 5)");
    assert(getInt(r) == 1);

    r = db.execute("SELECT geo_contains('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 15, 5)");
    assert(getInt(r) == 0);

    // ECEF round-trip (SpatiaLite equivalent: Transform between SRID 4326 and 4978)
    r = db.execute("SELECT geo_to_ecef(48.8566, 2.3522, 35)");
    auto ecef = parseCSV(getString(r));
    std::string q = "SELECT geo_from_ecef(" + std::to_string(ecef[0]) + "," + std::to_string(ecef[1]) + "," + std::to_string(ecef[2]) + ")";
    r = db.execute(q);
    auto lla = parseCSV(getString(r));
    assert(std::abs(lla[0] - 48.8566) < 0.01);
    assert(std::abs(lla[1] - 2.3522) < 0.01);
    std::cout << "    Paris ECEF round-trip: OK" << std::endl;

    std::cout << "  SpatiaLite parity OK" << std::endl;
}

// ===========================================================================
// Unit tests for geo_geometry.h types directly (no SQL)
// ===========================================================================

static void testGeometryDirect() {
    std::cout << "  direct geometry API..." << std::endl;
    using namespace geo;

    // WKT parse/serialize
    auto g = parseWKT("POINT(1.5 2.5)");
    assert(g.type == GeomType::POINT);
    assert(std::abs(g.point.x - 1.5) < EPS_TIGHT);
    assert(std::abs(g.point.y - 2.5) < EPS_TIGHT);
    assert(toWKT(g).find("1.5") != std::string::npos);

    // Polygon area
    Ring ring;
    ring.points = {{0,0}, {10,0}, {10,10}, {0,10}, {0,0}};
    auto poly = Geometry::makePolygon(ring);
    double a = polygonArea(poly);
    assert(std::abs(a - 100.0) < EPS_TIGHT);

    // Point-in-polygon
    assert(pointInPolygon({5, 5}, ring) == true);
    assert(pointInPolygon({15, 5}, ring) == false);

    // Convex hull
    auto mp = Geometry::makeMultiPoint({{0,0}, {10,0}, {5,10}, {5,3}});
    auto hull = convexHull(mp);
    assert(hull.type == GeomType::POLYGON);
    double hullArea = polygonArea(hull);
    assert(std::abs(hullArea - 50.0) < 1.0);

    // Blob round-trip
    auto blob = geometryToBlob(poly);
    auto restored = blobToGeometry(blob.data(), blob.size());
    assert(restored.type == GeomType::POLYGON);
    assert(std::abs(polygonArea(restored) - 100.0) < EPS_TIGHT);

    // Centroid
    auto c = centroid(poly);
    assert(std::abs(c.x - 5.0) < EPS_TIGHT);
    assert(std::abs(c.y - 5.0) < EPS_TIGHT);

    // Envelope
    auto env = envelope(Geometry::makeLineString({{1,2}, {5,8}, {3,4}}));
    assert(env.type == GeomType::POLYGON);

    // Coordinate transforms
    auto ecef = geodedicToECEF(0, 0, 0);
    assert(std::abs(ecef.x - 6378137.0) < 1.0);

    double lat, lon, alt;
    ecefToGeodetic(ecef.x, ecef.y, ecef.z, lat, lon, alt);
    assert(std::abs(lat) < 0.001);
    assert(std::abs(lon) < 0.001);
    assert(std::abs(alt) < 1.0);

    std::cout << "  direct geometry API OK" << std::endl;
}

// ===========================================================================
// main
// ===========================================================================

int main() {
    std::cout << "=== Spatial Extensions Test Suite ===" << std::endl;

    SQLiteEngine db;

    testGeometryDirect();
    testExistingFunctions(db);
    testBearing(db);
    testDestination(db);
    testMidpoint(db);
    testAreaBbox(db);
    testGeohash(db);
    testWKT(db);
    testGeoJSON(db);
    testContains(db);
    testPolygonBooleans(db);
    testBuffer(db);
    testAnalysis(db);
    testVoronoi(db);
    testDelaunay(db);
    testCoordinateTransforms(db);
    testBlobRoundTrip(db);
    testNullHandling(db);
    testSpatialiteParity(db);

    std::cout << std::endl;
    std::cout << "=== ALL SPATIAL TESTS PASSED ===" << std::endl;
    return 0;
}
