#ifndef FLATSQL_GEO_GEOMETRY_H
#define FLATSQL_GEO_GEOMETRY_H

#include <cstdint>
#include <string>
#include <vector>
#include <cmath>

namespace flatsql {
namespace geo {

// --- Core geometry types ---

struct Point {
    double x;  // longitude
    double y;  // latitude
    Point() : x(0), y(0) {}
    Point(double x_, double y_) : x(x_), y(y_) {}
    bool operator==(const Point& o) const { return x == o.x && y == o.y; }
    bool operator!=(const Point& o) const { return !(*this == o); }
    bool operator<(const Point& o) const { return y < o.y || (y == o.y && x < o.x); }
};

struct Ring {
    std::vector<Point> points;  // closed ring (first == last)
    bool isClosed() const { return points.size() >= 4 && points.front() == points.back(); }
    void close() { if (!points.empty() && points.front() != points.back()) points.push_back(points.front()); }
};

enum class GeomType : uint8_t {
    POINT = 1,
    LINESTRING = 2,
    POLYGON = 3,
    MULTIPOINT = 4,
    MULTILINESTRING = 5,
    MULTIPOLYGON = 6
};

struct Geometry {
    GeomType type;
    int32_t srid = 4326;  // default WGS84
    Point point;                    // for POINT
    std::vector<Point> points;     // for LINESTRING, MULTIPOINT
    Ring exterior;                  // for POLYGON
    std::vector<Ring> holes;       // for POLYGON (interior rings)
    std::vector<Geometry> parts;   // for MULTI* types

    Geometry() : type(GeomType::POINT) {}
    static Geometry makePoint(double x, double y);
    static Geometry makeLineString(const std::vector<Point>& pts);
    static Geometry makePolygon(const Ring& exterior, const std::vector<Ring>& holes = {});
    static Geometry makeMultiPoint(const std::vector<Point>& pts);
    static Geometry makeMultiPolygon(const std::vector<Geometry>& polygons);
};

// --- Binary serialization (BLOB) ---

std::vector<uint8_t> geometryToBlob(const Geometry& geom);
Geometry blobToGeometry(const uint8_t* data, size_t len);

// --- WKT parsing / serialization ---

Geometry parseWKT(const std::string& wkt);
std::string toWKT(const Geometry& geom);

// --- GeoJSON parsing / serialization ---

Geometry parseGeoJSON(const std::string& json);
std::string toGeoJSON(const Geometry& geom);

// --- Point-in-polygon (ray casting) ---

bool pointInPolygon(const Point& p, const Ring& ring);
bool pointInGeometry(const Point& p, const Geometry& geom);

// --- Polygon boolean operations ---

Geometry polygonIntersection(const Geometry& a, const Geometry& b);
Geometry polygonUnion(const Geometry& a, const Geometry& b);
Geometry polygonDifference(const Geometry& a, const Geometry& b);
Geometry polygonSymDifference(const Geometry& a, const Geometry& b);
Geometry polygonBuffer(const Geometry& geom, double distance, int segments = 16);

// --- Geometry analysis ---

double polygonArea(const Geometry& geom);      // Shoelace formula, sq degrees
Point centroid(const Geometry& geom);
double lineLength(const Geometry& geom);        // haversine km
Geometry envelope(const Geometry& geom);        // bounding box polygon
Geometry convexHull(const Geometry& geom);      // Graham scan

// --- Voronoi / Delaunay ---

Geometry voronoiDiagram(const std::vector<Point>& sites, const Geometry& bounds);
Geometry delaunayTriangulation(const std::vector<Point>& points);

// --- Coordinate transforms ---

struct ECEFPoint { double x, y, z; };

// WGS84 constants
constexpr double WGS84_A = 6378137.0;              // semi-major axis (m)
constexpr double WGS84_F = 1.0 / 298.257223563;    // flattening
constexpr double WGS84_B = WGS84_A * (1.0 - WGS84_F);  // semi-minor axis
constexpr double WGS84_E2 = 2 * WGS84_F - WGS84_F * WGS84_F;  // eccentricity squared

ECEFPoint geodedicToECEF(double lat_deg, double lon_deg, double alt_m);
void ecefToGeodetic(double x, double y, double z, double& lat_deg, double& lon_deg, double& alt_m);

}  // namespace geo
}  // namespace flatsql

#endif  // FLATSQL_GEO_GEOMETRY_H
