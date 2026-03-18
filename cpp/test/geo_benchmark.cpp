// geo_benchmark.cpp — Performance benchmarks for FlatSQL spatial functions
// Measures throughput and latency across all spatial operations
// Compares R-Tree indexed vs full scan for spatial queries

#include "flatsql/sqlite_engine.h"
#include "flatsql/geo_geometry.h"
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

using namespace flatsql;
using namespace std::chrono;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
constexpr int WARMUP = 100;
constexpr int POINT_OPS = 100000;       // scalar point function iterations
constexpr int GEOHASH_OPS = 100000;
constexpr int WKT_OPS = 50000;         // WKT parse/serialize
constexpr int CONTAINS_OPS = 50000;    // point-in-polygon
constexpr int BOOLEAN_OPS = 10000;     // polygon intersect/union/diff
constexpr int ANALYSIS_OPS = 50000;    // area, centroid, length, envelope
constexpr int HULL_OPS = 10000;
constexpr int VORONOI_OPS = 1000;
constexpr int DELAUNAY_OPS = 1000;
constexpr int COORD_OPS = 100000;      // ECEF transforms
constexpr int BUFFER_OPS = 10000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

class Timer {
public:
    void start() { t0_ = high_resolution_clock::now(); }
    void stop()  { t1_ = high_resolution_clock::now(); }
    double us() const { return duration_cast<nanoseconds>(t1_ - t0_).count() / 1000.0; }
    double ms() const { return us() / 1000.0; }
private:
    high_resolution_clock::time_point t0_, t1_;
};

static void report(const char* name, int ops, double totalUs) {
    double perOp = totalUs / ops;
    double opsPerSec = 1e6 / perOp;
    std::cout << "  " << std::left << std::setw(35) << name
              << std::right << std::setw(10) << std::fixed << std::setprecision(2) << perOp << " us/op"
              << std::setw(14) << std::fixed << std::setprecision(0) << opsPerSec << " ops/sec"
              << std::endl;
}

// ---------------------------------------------------------------------------
// Benchmark: Point Operations (pure scalar, no geometry engine)
// ---------------------------------------------------------------------------

static void benchPointOps(SQLiteEngine& db) {
    std::cout << "\n--- Point Operations (" << POINT_OPS << " iterations) ---\n";
    Timer t;

    // Warmup
    for (int i = 0; i < WARMUP; i++) {
        db.execute("SELECT geo_distance(40.7128, -74.0060, 38.9072, -77.0369)");
    }

    // geo_distance
    t.start();
    for (int i = 0; i < POINT_OPS; i++) {
        db.execute("SELECT geo_distance(40.7128, -74.0060, 38.9072, -77.0369)");
    }
    t.stop();
    report("geo_distance", POINT_OPS, t.us());

    // geo_bearing
    t.start();
    for (int i = 0; i < POINT_OPS; i++) {
        db.execute("SELECT geo_bearing(40.7128, -74.0060, 34.0522, -118.2437)");
    }
    t.stop();
    report("geo_bearing", POINT_OPS, t.us());

    // geo_destination
    t.start();
    for (int i = 0; i < POINT_OPS; i++) {
        db.execute("SELECT geo_destination(40.7128, -74.0060, 45, 100)");
    }
    t.stop();
    report("geo_destination", POINT_OPS, t.us());

    // geo_midpoint
    t.start();
    for (int i = 0; i < POINT_OPS; i++) {
        db.execute("SELECT geo_midpoint(40.7128, -74.0060, 34.0522, -118.2437)");
    }
    t.stop();
    report("geo_midpoint", POINT_OPS, t.us());

    // geo_area_bbox
    t.start();
    for (int i = 0; i < POINT_OPS; i++) {
        db.execute("SELECT geo_area_bbox(40, 41, -75, -74)");
    }
    t.stop();
    report("geo_area_bbox", POINT_OPS, t.us());

    // geo_within_radius
    t.start();
    for (int i = 0; i < POINT_OPS; i++) {
        db.execute("SELECT geo_within_radius(40.7128, -74.0060, 40.7580, -73.9855, 10)");
    }
    t.stop();
    report("geo_within_radius", POINT_OPS, t.us());

    // geo_bbox_contains
    t.start();
    for (int i = 0; i < POINT_OPS; i++) {
        db.execute("SELECT geo_bbox_contains(40, 41, -75, -74, 40.5, -74.5)");
    }
    t.stop();
    report("geo_bbox_contains", POINT_OPS, t.us());
}

// ---------------------------------------------------------------------------
// Benchmark: Geohash
// ---------------------------------------------------------------------------

static void benchGeohash(SQLiteEngine& db) {
    std::cout << "\n--- Geohash (" << GEOHASH_OPS << " iterations) ---\n";
    Timer t;

    for (int i = 0; i < WARMUP; i++) {
        db.execute("SELECT geo_geohash_encode(42.6, -5.6, 9)");
    }

    t.start();
    for (int i = 0; i < GEOHASH_OPS; i++) {
        db.execute("SELECT geo_geohash_encode(42.6, -5.6, 9)");
    }
    t.stop();
    report("geo_geohash_encode", GEOHASH_OPS, t.us());

    t.start();
    for (int i = 0; i < GEOHASH_OPS; i++) {
        db.execute("SELECT geo_geohash_decode_lat('u09tvw0f6')");
    }
    t.stop();
    report("geo_geohash_decode_lat", GEOHASH_OPS, t.us());

    t.start();
    for (int i = 0; i < GEOHASH_OPS; i++) {
        db.execute("SELECT geo_geohash_decode_lon('u09tvw0f6')");
    }
    t.stop();
    report("geo_geohash_decode_lon", GEOHASH_OPS, t.us());
}

// ---------------------------------------------------------------------------
// Benchmark: WKT/GeoJSON parse and serialize
// ---------------------------------------------------------------------------

static void benchWKT(SQLiteEngine& db) {
    std::cout << "\n--- WKT / GeoJSON (" << WKT_OPS << " iterations) ---\n";
    Timer t;

    // POINT
    t.start();
    for (int i = 0; i < WKT_OPS; i++) {
        db.execute("SELECT geo_from_text('POINT(2.3522 48.8566)')");
    }
    t.stop();
    report("geo_from_text(POINT)", WKT_OPS, t.us());

    // POLYGON (5 vertices)
    t.start();
    for (int i = 0; i < WKT_OPS; i++) {
        db.execute("SELECT geo_from_text('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')");
    }
    t.stop();
    report("geo_from_text(POLYGON 5v)", WKT_OPS, t.us());

    // geo_as_text (blob → WKT)
    t.start();
    for (int i = 0; i < WKT_OPS; i++) {
        db.execute("SELECT geo_as_text(geo_from_text('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))");
    }
    t.stop();
    report("geo_as_text(POLYGON)", WKT_OPS, t.us());

    // GeoJSON round-trip
    t.start();
    for (int i = 0; i < WKT_OPS; i++) {
        db.execute("SELECT geo_as_geojson(geo_from_geojson('{\"type\":\"Point\",\"coordinates\":[2.3522,48.8566]}'))");
    }
    t.stop();
    report("GeoJSON round-trip(POINT)", WKT_OPS, t.us());
}

// ---------------------------------------------------------------------------
// Benchmark: Point-in-Polygon
// ---------------------------------------------------------------------------

static void benchContains(SQLiteEngine& db) {
    std::cout << "\n--- Point-in-Polygon (" << CONTAINS_OPS << " iterations) ---\n";
    Timer t;

    // Simple square
    t.start();
    for (int i = 0; i < CONTAINS_OPS; i++) {
        db.execute("SELECT geo_contains('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 5, 5)");
    }
    t.stop();
    report("geo_contains(square, inside)", CONTAINS_OPS, t.us());

    t.start();
    for (int i = 0; i < CONTAINS_OPS; i++) {
        db.execute("SELECT geo_contains('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 15, 5)");
    }
    t.stop();
    report("geo_contains(square, outside)", CONTAINS_OPS, t.us());

    // Complex polygon (more vertices)
    std::string complex = "POLYGON((0 0, 3 0, 6 0, 9 0, 10 0, 10 3, 10 6, 10 9, 10 10, 7 10, 4 10, 1 10, 0 10, 0 7, 0 4, 0 1, 0 0))";
    t.start();
    for (int i = 0; i < CONTAINS_OPS; i++) {
        db.execute("SELECT geo_contains('" + complex + "', 5, 5)");
    }
    t.stop();
    report("geo_contains(17v polygon)", CONTAINS_OPS, t.us());
}

// ---------------------------------------------------------------------------
// Benchmark: Polygon Boolean Operations
// ---------------------------------------------------------------------------

static void benchBooleans(SQLiteEngine& db) {
    std::cout << "\n--- Polygon Booleans (" << BOOLEAN_OPS << " iterations) ---\n";
    Timer t;

    std::string a = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";
    std::string b = "POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))";

    t.start();
    for (int i = 0; i < BOOLEAN_OPS; i++) {
        db.execute("SELECT geo_intersection('" + a + "', '" + b + "')");
    }
    t.stop();
    report("geo_intersection", BOOLEAN_OPS, t.us());

    t.start();
    for (int i = 0; i < BOOLEAN_OPS; i++) {
        db.execute("SELECT geo_union('" + a + "', '" + b + "')");
    }
    t.stop();
    report("geo_union", BOOLEAN_OPS, t.us());

    t.start();
    for (int i = 0; i < BOOLEAN_OPS; i++) {
        db.execute("SELECT geo_difference('" + a + "', '" + b + "')");
    }
    t.stop();
    report("geo_difference", BOOLEAN_OPS, t.us());

    t.start();
    for (int i = 0; i < BOOLEAN_OPS; i++) {
        db.execute("SELECT geo_sym_difference('" + a + "', '" + b + "')");
    }
    t.stop();
    report("geo_sym_difference", BOOLEAN_OPS, t.us());
}

// ---------------------------------------------------------------------------
// Benchmark: Buffer
// ---------------------------------------------------------------------------

static void benchBuffer(SQLiteEngine& db) {
    std::cout << "\n--- Buffer (" << BUFFER_OPS << " iterations) ---\n";
    Timer t;

    t.start();
    for (int i = 0; i < BUFFER_OPS; i++) {
        db.execute("SELECT geo_buffer('POINT(0 0)', 1)");
    }
    t.stop();
    report("geo_buffer(POINT, 16 seg)", BUFFER_OPS, t.us());

    t.start();
    for (int i = 0; i < BUFFER_OPS; i++) {
        db.execute("SELECT geo_buffer('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 0.5)");
    }
    t.stop();
    report("geo_buffer(POLYGON)", BUFFER_OPS, t.us());
}

// ---------------------------------------------------------------------------
// Benchmark: Geometry Analysis
// ---------------------------------------------------------------------------

static void benchAnalysis(SQLiteEngine& db) {
    std::cout << "\n--- Geometry Analysis (" << ANALYSIS_OPS << " iterations) ---\n";
    Timer t;

    std::string poly = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";

    t.start();
    for (int i = 0; i < ANALYSIS_OPS; i++) {
        db.execute("SELECT geo_area_geom('" + poly + "')");
    }
    t.stop();
    report("geo_area_geom(square)", ANALYSIS_OPS, t.us());

    t.start();
    for (int i = 0; i < ANALYSIS_OPS; i++) {
        db.execute("SELECT geo_centroid('" + poly + "')");
    }
    t.stop();
    report("geo_centroid(square)", ANALYSIS_OPS, t.us());

    t.start();
    for (int i = 0; i < ANALYSIS_OPS; i++) {
        db.execute("SELECT geo_length_geom('LINESTRING(0 0, 1 0, 1 1, 0 1)')");
    }
    t.stop();
    report("geo_length_geom(3-seg line)", ANALYSIS_OPS, t.us());

    t.start();
    for (int i = 0; i < ANALYSIS_OPS; i++) {
        db.execute("SELECT geo_envelope('LINESTRING(1 2, 5 8, 3 4)')");
    }
    t.stop();
    report("geo_envelope(3-pt line)", ANALYSIS_OPS, t.us());
}

// ---------------------------------------------------------------------------
// Benchmark: Convex Hull
// ---------------------------------------------------------------------------

static void benchHull(SQLiteEngine& db) {
    std::cout << "\n--- Convex Hull (" << HULL_OPS << " iterations) ---\n";
    Timer t;

    // 4 points
    t.start();
    for (int i = 0; i < HULL_OPS; i++) {
        db.execute("SELECT geo_convex_hull('MULTIPOINT((0 0), (10 0), (5 10), (5 3))')");
    }
    t.stop();
    report("geo_convex_hull(4 pts)", HULL_OPS, t.us());

    // 8 points
    t.start();
    for (int i = 0; i < HULL_OPS; i++) {
        db.execute("SELECT geo_convex_hull('MULTIPOINT((0 0), (10 0), (10 10), (0 10), (5 5), (3 7), (7 3), (5 12))')");
    }
    t.stop();
    report("geo_convex_hull(8 pts)", HULL_OPS, t.us());
}

// ---------------------------------------------------------------------------
// Benchmark: Voronoi & Delaunay
// ---------------------------------------------------------------------------

static void benchVoronoiDelaunay(SQLiteEngine& db) {
    std::cout << "\n--- Voronoi / Delaunay (" << VORONOI_OPS << " iterations) ---\n";
    Timer t;

    std::string pts4 = "MULTIPOINT((0 0), (10 0), (10 10), (0 10))";
    std::string bounds = "POLYGON((-5 -5, 15 -5, 15 15, -5 15, -5 -5))";

    t.start();
    for (int i = 0; i < VORONOI_OPS; i++) {
        db.execute("SELECT geo_voronoi('" + pts4 + "', '" + bounds + "')");
    }
    t.stop();
    report("geo_voronoi(4 sites)", VORONOI_OPS, t.us());

    t.start();
    for (int i = 0; i < DELAUNAY_OPS; i++) {
        db.execute("SELECT geo_delaunay('" + pts4 + "')");
    }
    t.stop();
    report("geo_delaunay(4 pts)", DELAUNAY_OPS, t.us());

    // Larger point sets
    std::string pts8 = "MULTIPOINT((0 0), (10 0), (10 10), (0 10), (5 5), (3 7), (7 3), (5 1))";
    t.start();
    for (int i = 0; i < VORONOI_OPS; i++) {
        db.execute("SELECT geo_voronoi('" + pts8 + "', '" + bounds + "')");
    }
    t.stop();
    report("geo_voronoi(8 sites)", VORONOI_OPS, t.us());

    t.start();
    for (int i = 0; i < DELAUNAY_OPS; i++) {
        db.execute("SELECT geo_delaunay('" + pts8 + "')");
    }
    t.stop();
    report("geo_delaunay(8 pts)", DELAUNAY_OPS, t.us());
}

// ---------------------------------------------------------------------------
// Benchmark: Coordinate Transforms
// ---------------------------------------------------------------------------

static void benchCoordTransforms(SQLiteEngine& db) {
    std::cout << "\n--- Coordinate Transforms (" << COORD_OPS << " iterations) ---\n";
    Timer t;

    t.start();
    for (int i = 0; i < COORD_OPS; i++) {
        db.execute("SELECT geo_to_ecef(48.8566, 2.3522, 35)");
    }
    t.stop();
    report("geo_to_ecef", COORD_OPS, t.us());

    t.start();
    for (int i = 0; i < COORD_OPS; i++) {
        db.execute("SELECT geo_from_ecef(4200952.0, 172458.0, 4780111.0)");
    }
    t.stop();
    report("geo_from_ecef", COORD_OPS, t.us());
}

// ---------------------------------------------------------------------------
// Benchmark: Direct geometry engine (no SQL overhead)
// ---------------------------------------------------------------------------

static void benchDirectGeometryAPI() {
    std::cout << "\n--- Direct Geometry API (no SQL, 1M iterations) ---\n";
    using namespace geo;
    constexpr int N = 1000000;
    Timer t;

    // Point-in-polygon direct
    Ring ring;
    ring.points = {{0,0}, {10,0}, {10,10}, {0,10}, {0,0}};
    Point inside(5, 5);
    Point outside(15, 5);

    t.start();
    for (int i = 0; i < N; i++) {
        pointInPolygon(inside, ring);
    }
    t.stop();
    report("pointInPolygon(inside)", N, t.us());

    t.start();
    for (int i = 0; i < N; i++) {
        pointInPolygon(outside, ring);
    }
    t.stop();
    report("pointInPolygon(outside)", N, t.us());

    // Polygon area direct
    auto poly = Geometry::makePolygon(ring);
    t.start();
    for (int i = 0; i < N; i++) {
        polygonArea(poly);
    }
    t.stop();
    report("polygonArea(square)", N, t.us());

    // Centroid direct
    t.start();
    for (int i = 0; i < N; i++) {
        centroid(poly);
    }
    t.stop();
    report("centroid(square)", N, t.us());

    // WKT parse direct
    t.start();
    for (int i = 0; i < N; i++) {
        parseWKT("POINT(2.3522 48.8566)");
    }
    t.stop();
    report("parseWKT(POINT)", N, t.us());

    // WKT parse polygon
    constexpr int N2 = 100000;
    t.start();
    for (int i = 0; i < N2; i++) {
        parseWKT("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
    }
    t.stop();
    report("parseWKT(POLYGON 5v)", N2, t.us());

    // Blob serialization
    t.start();
    for (int i = 0; i < N; i++) {
        geometryToBlob(poly);
    }
    t.stop();
    report("geometryToBlob(POLYGON)", N, t.us());

    // ECEF transform direct
    t.start();
    for (int i = 0; i < N; i++) {
        geodedicToECEF(48.8566, 2.3522, 35);
    }
    t.stop();
    report("geodedicToECEF", N, t.us());

    t.start();
    for (int i = 0; i < N; i++) {
        double lat, lon, alt;
        ecefToGeodetic(4200952.0, 172458.0, 4780111.0, lat, lon, alt);
    }
    t.stop();
    report("ecefToGeodetic", N, t.us());

    // Intersection direct
    Ring ringB;
    ringB.points = {{5,5}, {15,5}, {15,15}, {5,15}, {5,5}};
    auto polyA = Geometry::makePolygon(ring);
    auto polyB = Geometry::makePolygon(ringB);
    constexpr int N3 = 100000;
    t.start();
    for (int i = 0; i < N3; i++) {
        polygonIntersection(polyA, polyB);
    }
    t.stop();
    report("polygonIntersection", N3, t.us());
}

// ---------------------------------------------------------------------------
// Benchmark: R-Tree spatial query vs full scan
// ---------------------------------------------------------------------------

static void benchRTreeVsFullScan(SQLiteEngine& db) {
    std::cout << "\n--- R-Tree vs Full Scan ---\n";
    Timer t;

    // Create a table with spatial data points using R-Tree directly
    db.execute("CREATE VIRTUAL TABLE IF NOT EXISTS bench_rtree USING rtree(id, minLat, maxLat, minLon, maxLon)");
    db.execute("CREATE TABLE IF NOT EXISTS bench_points (id INTEGER PRIMARY KEY, lat REAL, lon REAL, name TEXT)");

    // Insert 10K random spatial points
    constexpr int SPATIAL_RECORDS = 10000;
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> latDist(-90, 90);
    std::uniform_real_distribution<double> lonDist(-180, 180);

    t.start();
    db.execute("BEGIN");
    for (int i = 0; i < SPATIAL_RECORDS; i++) {
        double lat = latDist(rng);
        double lon = lonDist(rng);
        std::string sql = "INSERT INTO bench_points VALUES(" + std::to_string(i) + "," +
                          std::to_string(lat) + "," + std::to_string(lon) + ",'point_" + std::to_string(i) + "')";
        db.execute(sql);
        sql = "INSERT INTO bench_rtree VALUES(" + std::to_string(i) + "," +
              std::to_string(lat) + "," + std::to_string(lat) + "," +
              std::to_string(lon) + "," + std::to_string(lon) + ")";
        db.execute(sql);
    }
    db.execute("COMMIT");
    t.stop();
    std::cout << "  Inserted " << SPATIAL_RECORDS << " spatial records in " << t.ms() << " ms\n";

    // Benchmark: R-Tree bbox query
    constexpr int QUERY_ITERS = 1000;
    t.start();
    for (int i = 0; i < QUERY_ITERS; i++) {
        db.execute("SELECT id FROM bench_rtree WHERE minLat >= 40 AND maxLat <= 50 AND minLon >= -80 AND maxLon <= -70");
    }
    t.stop();
    report("R-Tree bbox query", QUERY_ITERS, t.us());

    // Benchmark: Full scan bbox equivalent
    t.start();
    for (int i = 0; i < QUERY_ITERS; i++) {
        db.execute("SELECT id FROM bench_points WHERE lat >= 40 AND lat <= 50 AND lon >= -80 AND lon <= -70");
    }
    t.stop();
    report("Full scan bbox query", QUERY_ITERS, t.us());

    // Benchmark: R-Tree + join for full record
    t.start();
    for (int i = 0; i < QUERY_ITERS; i++) {
        db.execute("SELECT p.id, p.lat, p.lon, p.name FROM bench_points p INNER JOIN bench_rtree r ON p.id = r.id WHERE r.minLat >= 40 AND r.maxLat <= 50 AND r.minLon >= -80 AND r.maxLon <= -70");
    }
    t.stop();
    report("R-Tree + join query", QUERY_ITERS, t.us());

    // Benchmark: geo_within_radius vs R-Tree pre-filter
    t.start();
    for (int i = 0; i < QUERY_ITERS; i++) {
        db.execute("SELECT id FROM bench_points WHERE geo_within_radius(40.7, -74.0, lat, lon, 500) = 1");
    }
    t.stop();
    report("Full scan + geo_within_radius", QUERY_ITERS, t.us());

    // R-Tree pre-filter + radius check (approximate bbox first, then precise)
    t.start();
    for (int i = 0; i < QUERY_ITERS; i++) {
        db.execute("SELECT p.id FROM bench_points p INNER JOIN bench_rtree r ON p.id = r.id "
                   "WHERE r.minLat >= 36.2 AND r.maxLat <= 45.2 AND r.minLon >= -80 AND r.maxLon <= -68 "
                   "AND geo_within_radius(40.7, -74.0, p.lat, p.lon, 500) = 1");
    }
    t.stop();
    report("R-Tree pre-filter + radius", QUERY_ITERS, t.us());

    // Count results for comparison
    auto r1 = db.execute("SELECT COUNT(*) FROM bench_points WHERE geo_within_radius(40.7, -74.0, lat, lon, 500) = 1");
    auto r2 = db.execute("SELECT COUNT(*) FROM bench_points p INNER JOIN bench_rtree r ON p.id = r.id "
                          "WHERE r.minLat >= 36.2 AND r.maxLat <= 45.2 AND r.minLon >= -80 AND r.maxLon <= -68 "
                          "AND geo_within_radius(40.7, -74.0, p.lat, p.lon, 500) = 1");
    int64_t c1 = std::get<int64_t>(r1.rows[0][0]);
    int64_t c2 = std::get<int64_t>(r2.rows[0][0]);
    std::cout << "  Result counts: full_scan=" << c1 << " rtree_prefilter=" << c2 << "\n";
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    std::cout << "=== FlatSQL Spatial Benchmark ===" << std::endl;

    bool quick = (argc > 1 && std::string(argv[1]) == "--quick");
    if (quick) {
        std::cout << "(quick mode)\n";
    }

    SQLiteEngine db;

    benchPointOps(db);
    benchGeohash(db);
    benchWKT(db);
    benchContains(db);
    benchBooleans(db);
    benchBuffer(db);
    benchAnalysis(db);
    benchHull(db);
    benchVoronoiDelaunay(db);
    benchCoordTransforms(db);
    benchDirectGeometryAPI();
    benchRTreeVsFullScan(db);

    std::cout << "\n=== BENCHMARK COMPLETE ===" << std::endl;
    return 0;
}
