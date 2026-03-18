// geo_geometry.cpp — Computational geometry engine for FlatSQL
// Implements all functions declared in flatsql/geo_geometry.h

#include "flatsql/geo_geometry.h"
#include <cmath>
#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <queue>
#include <set>
#include <limits>
#include <numeric>
#include <cstring>

namespace flatsql {
namespace geo {

// ============================================================================
// Helper constants
// ============================================================================

static constexpr double EARTH_RADIUS_KM = 6371.0;

static double degToRad(double d) { return d * M_PI / 180.0; }
static double radToDeg(double r) { return r * 180.0 / M_PI; }

// ============================================================================
// Geometry factory methods
// ============================================================================

Geometry Geometry::makePoint(double x, double y) {
    Geometry g;
    g.type = GeomType::POINT;
    g.point = Point(x, y);
    return g;
}

Geometry Geometry::makeLineString(const std::vector<Point>& pts) {
    Geometry g;
    g.type = GeomType::LINESTRING;
    g.points = pts;
    return g;
}

Geometry Geometry::makePolygon(const Ring& exterior, const std::vector<Ring>& holes) {
    Geometry g;
    g.type = GeomType::POLYGON;
    g.exterior = exterior;
    g.exterior.close();
    g.holes = holes;
    for (auto& h : g.holes) h.close();
    return g;
}

Geometry Geometry::makeMultiPoint(const std::vector<Point>& pts) {
    Geometry g;
    g.type = GeomType::MULTIPOINT;
    g.points = pts;
    return g;
}

Geometry Geometry::makeMultiPolygon(const std::vector<Geometry>& polygons) {
    Geometry g;
    g.type = GeomType::MULTIPOLYGON;
    g.parts = polygons;
    return g;
}

// ============================================================================
// Binary serialization helpers
// ============================================================================

static void writeU8(std::vector<uint8_t>& buf, uint8_t v) {
    buf.push_back(v);
}

static void writeI32(std::vector<uint8_t>& buf, int32_t v) {
    uint8_t b[4];
    std::memcpy(b, &v, 4);  // native little-endian assumed
    buf.insert(buf.end(), b, b + 4);
}

static void writeU32(std::vector<uint8_t>& buf, uint32_t v) {
    uint8_t b[4];
    std::memcpy(b, &v, 4);
    buf.insert(buf.end(), b, b + 4);
}

static void writeF64(std::vector<uint8_t>& buf, double v) {
    uint8_t b[8];
    std::memcpy(b, &v, 8);
    buf.insert(buf.end(), b, b + 8);
}

static void writePoint(std::vector<uint8_t>& buf, const Point& p) {
    writeF64(buf, p.x);
    writeF64(buf, p.y);
}

static void writeRing(std::vector<uint8_t>& buf, const Ring& r) {
    writeU32(buf, static_cast<uint32_t>(r.points.size()));
    for (auto& p : r.points) writePoint(buf, p);
}

// Reader helpers
struct BlobReader {
    const uint8_t* data;
    size_t len;
    size_t pos = 0;

    void check(size_t n) const {
        if (pos + n > len) throw std::runtime_error("blob too short");
    }

    uint8_t readU8() { check(1); return data[pos++]; }

    int32_t readI32() {
        check(4);
        int32_t v;
        std::memcpy(&v, data + pos, 4);
        pos += 4;
        return v;
    }

    uint32_t readU32() {
        check(4);
        uint32_t v;
        std::memcpy(&v, data + pos, 4);
        pos += 4;
        return v;
    }

    double readF64() {
        check(8);
        double v;
        std::memcpy(&v, data + pos, 8);
        pos += 8;
        return v;
    }

    Point readPoint() {
        double x = readF64();
        double y = readF64();
        return Point(x, y);
    }

    Ring readRing() {
        Ring r;
        uint32_t n = readU32();
        r.points.reserve(n);
        for (uint32_t i = 0; i < n; i++) r.points.push_back(readPoint());
        return r;
    }
};

// Forward declarations for recursive blob operations
static void geometryToBlobInner(std::vector<uint8_t>& buf, const Geometry& geom);
static Geometry blobToGeometryInner(BlobReader& rd);

static void geometryToBlobInner(std::vector<uint8_t>& buf, const Geometry& geom) {
    switch (geom.type) {
        case GeomType::POINT:
            writePoint(buf, geom.point);
            break;
        case GeomType::LINESTRING:
            writeU32(buf, static_cast<uint32_t>(geom.points.size()));
            for (auto& p : geom.points) writePoint(buf, p);
            break;
        case GeomType::POLYGON: {
            uint32_t ringCount = 1 + static_cast<uint32_t>(geom.holes.size());
            writeU32(buf, ringCount);
            writeRing(buf, geom.exterior);
            for (auto& h : geom.holes) writeRing(buf, h);
            break;
        }
        case GeomType::MULTIPOINT:
            writeU32(buf, static_cast<uint32_t>(geom.points.size()));
            for (auto& p : geom.points) writePoint(buf, p);
            break;
        case GeomType::MULTILINESTRING:
        case GeomType::MULTIPOLYGON:
            writeU32(buf, static_cast<uint32_t>(geom.parts.size()));
            for (auto& part : geom.parts) {
                // Recurse: write type byte + sub-geometry data
                writeU8(buf, static_cast<uint8_t>(part.type));
                writeI32(buf, part.srid);
                geometryToBlobInner(buf, part);
            }
            break;
    }
}

std::vector<uint8_t> geometryToBlob(const Geometry& geom) {
    std::vector<uint8_t> buf;
    writeU8(buf, static_cast<uint8_t>(geom.type));
    writeI32(buf, geom.srid);
    geometryToBlobInner(buf, geom);
    return buf;
}

static Geometry blobToGeometryInner(BlobReader& rd, GeomType type, int32_t srid) {
    Geometry g;
    g.type = type;
    g.srid = srid;

    switch (type) {
        case GeomType::POINT:
            g.point = rd.readPoint();
            break;
        case GeomType::LINESTRING: {
            uint32_t n = rd.readU32();
            g.points.reserve(n);
            for (uint32_t i = 0; i < n; i++) g.points.push_back(rd.readPoint());
            break;
        }
        case GeomType::POLYGON: {
            uint32_t ringCount = rd.readU32();
            if (ringCount > 0) {
                g.exterior = rd.readRing();
                for (uint32_t i = 1; i < ringCount; i++) g.holes.push_back(rd.readRing());
            }
            break;
        }
        case GeomType::MULTIPOINT: {
            uint32_t n = rd.readU32();
            g.points.reserve(n);
            for (uint32_t i = 0; i < n; i++) g.points.push_back(rd.readPoint());
            break;
        }
        case GeomType::MULTILINESTRING:
        case GeomType::MULTIPOLYGON: {
            uint32_t n = rd.readU32();
            g.parts.reserve(n);
            for (uint32_t i = 0; i < n; i++) {
                GeomType st = static_cast<GeomType>(rd.readU8());
                int32_t ss = rd.readI32();
                g.parts.push_back(blobToGeometryInner(rd, st, ss));
            }
            break;
        }
    }
    return g;
}

Geometry blobToGeometry(const uint8_t* data, size_t len) {
    BlobReader rd{data, len, 0};
    GeomType type = static_cast<GeomType>(rd.readU8());
    int32_t srid = rd.readI32();
    return blobToGeometryInner(rd, type, srid);
}

// ============================================================================
// WKT parsing — recursive descent
// ============================================================================

struct WKTParser {
    const std::string& s;
    size_t pos = 0;

    WKTParser(const std::string& str) : s(str) {}

    void skipWS() {
        while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n' || s[pos] == '\r'))
            pos++;
    }

    bool match(char c) {
        skipWS();
        if (pos < s.size() && s[pos] == c) { pos++; return true; }
        return false;
    }

    void expect(char c) {
        skipWS();
        if (pos >= s.size() || s[pos] != c)
            throw std::runtime_error(std::string("WKT: expected '") + c + "' at pos " + std::to_string(pos));
        pos++;
    }

    double parseNumber() {
        skipWS();
        size_t start = pos;
        if (pos < s.size() && (s[pos] == '-' || s[pos] == '+')) pos++;
        while (pos < s.size() && (std::isdigit(s[pos]) || s[pos] == '.')) pos++;
        // Handle scientific notation
        if (pos < s.size() && (s[pos] == 'e' || s[pos] == 'E')) {
            pos++;
            if (pos < s.size() && (s[pos] == '-' || s[pos] == '+')) pos++;
            while (pos < s.size() && std::isdigit(s[pos])) pos++;
        }
        if (start == pos) throw std::runtime_error("WKT: expected number");
        return std::stod(s.substr(start, pos - start));
    }

    Point parsePoint() {
        double x = parseNumber();
        double y = parseNumber();
        return Point(x, y);
    }

    std::vector<Point> parsePointList() {
        std::vector<Point> pts;
        pts.push_back(parsePoint());
        while (match(',')) pts.push_back(parsePoint());
        return pts;
    }

    Ring parseRing() {
        expect('(');
        Ring r;
        r.points = parsePointList();
        expect(')');
        r.close();
        return r;
    }

    std::string parseType() {
        skipWS();
        size_t start = pos;
        while (pos < s.size() && std::isalpha(s[pos])) pos++;
        std::string t = s.substr(start, pos - start);
        // Convert to uppercase
        for (auto& c : t) c = std::toupper(c);
        return t;
    }

    Geometry parse() {
        std::string t = parseType();
        if (t == "POINT") {
            expect('(');
            Point p = parsePoint();
            expect(')');
            return Geometry::makePoint(p.x, p.y);
        } else if (t == "LINESTRING") {
            expect('(');
            auto pts = parsePointList();
            expect(')');
            return Geometry::makeLineString(pts);
        } else if (t == "POLYGON") {
            expect('(');
            Ring ext = parseRing();
            std::vector<Ring> holes;
            while (match(',')) holes.push_back(parseRing());
            expect(')');
            return Geometry::makePolygon(ext, holes);
        } else if (t == "MULTIPOINT") {
            expect('(');
            std::vector<Point> pts;
            // Handle both MULTIPOINT((x y), (x y)) and MULTIPOINT(x y, x y)
            skipWS();
            if (pos < s.size() && s[pos] == '(') {
                // Parenthesized form
                expect('(');
                pts.push_back(parsePoint());
                expect(')');
                while (match(',')) {
                    expect('(');
                    pts.push_back(parsePoint());
                    expect(')');
                }
            } else {
                // Bare form
                pts = parsePointList();
            }
            expect(')');
            return Geometry::makeMultiPoint(pts);
        } else if (t == "MULTIPOLYGON") {
            expect('(');
            std::vector<Geometry> polys;
            // Each polygon: ((ring), (hole), ...)
            auto parseSinglePoly = [&]() {
                expect('(');
                Ring ext = parseRing();
                std::vector<Ring> holes;
                while (match(',')) holes.push_back(parseRing());
                expect(')');
                return Geometry::makePolygon(ext, holes);
            };
            polys.push_back(parseSinglePoly());
            while (match(',')) polys.push_back(parseSinglePoly());
            expect(')');
            return Geometry::makeMultiPolygon(polys);
        } else {
            throw std::runtime_error("WKT: unsupported type: " + t);
        }
    }
};

Geometry parseWKT(const std::string& wkt) {
    WKTParser parser(wkt);
    return parser.parse();
}

// ============================================================================
// WKT serialization
// ============================================================================

static std::string fmtCoord(double v) {
    std::ostringstream oss;
    oss.precision(15);
    oss << v;
    return oss.str();
}

static std::string pointToWKT(const Point& p) {
    return fmtCoord(p.x) + " " + fmtCoord(p.y);
}

static std::string ringToWKT(const Ring& r) {
    std::string s = "(";
    for (size_t i = 0; i < r.points.size(); i++) {
        if (i) s += ", ";
        s += pointToWKT(r.points[i]);
    }
    s += ")";
    return s;
}

std::string toWKT(const Geometry& geom) {
    switch (geom.type) {
        case GeomType::POINT:
            return "POINT(" + pointToWKT(geom.point) + ")";

        case GeomType::LINESTRING: {
            std::string s = "LINESTRING(";
            for (size_t i = 0; i < geom.points.size(); i++) {
                if (i) s += ", ";
                s += pointToWKT(geom.points[i]);
            }
            return s + ")";
        }

        case GeomType::POLYGON: {
            std::string s = "POLYGON(" + ringToWKT(geom.exterior);
            for (auto& h : geom.holes) s += ", " + ringToWKT(h);
            return s + ")";
        }

        case GeomType::MULTIPOINT: {
            std::string s = "MULTIPOINT(";
            for (size_t i = 0; i < geom.points.size(); i++) {
                if (i) s += ", ";
                s += "(" + pointToWKT(geom.points[i]) + ")";
            }
            return s + ")";
        }

        case GeomType::MULTILINESTRING: {
            std::string s = "MULTILINESTRING(";
            for (size_t i = 0; i < geom.parts.size(); i++) {
                if (i) s += ", ";
                s += "(";
                for (size_t j = 0; j < geom.parts[i].points.size(); j++) {
                    if (j) s += ", ";
                    s += pointToWKT(geom.parts[i].points[j]);
                }
                s += ")";
            }
            return s + ")";
        }

        case GeomType::MULTIPOLYGON: {
            std::string s = "MULTIPOLYGON(";
            for (size_t i = 0; i < geom.parts.size(); i++) {
                if (i) s += ", ";
                s += "(" + ringToWKT(geom.parts[i].exterior);
                for (auto& h : geom.parts[i].holes) s += ", " + ringToWKT(h);
                s += ")";
            }
            return s + ")";
        }
    }
    return "";
}

// ============================================================================
// GeoJSON parsing — simple string parser (no library dependency)
// ============================================================================

struct JSONParser {
    const std::string& s;
    size_t pos = 0;

    JSONParser(const std::string& str) : s(str) {}

    void skipWS() {
        while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n' || s[pos] == '\r'))
            pos++;
    }

    bool match(char c) {
        skipWS();
        if (pos < s.size() && s[pos] == c) { pos++; return true; }
        return false;
    }

    void expect(char c) {
        skipWS();
        if (pos >= s.size() || s[pos] != c)
            throw std::runtime_error(std::string("GeoJSON: expected '") + c + "'");
        pos++;
    }

    // Parse a JSON string value (assumes we are at the opening quote)
    std::string parseString() {
        skipWS();
        expect('"');
        std::string result;
        while (pos < s.size() && s[pos] != '"') {
            if (s[pos] == '\\') {
                pos++;
                if (pos < s.size()) result += s[pos];
            } else {
                result += s[pos];
            }
            pos++;
        }
        expect('"');
        return result;
    }

    double parseNumber() {
        skipWS();
        size_t start = pos;
        if (pos < s.size() && (s[pos] == '-' || s[pos] == '+')) pos++;
        while (pos < s.size() && std::isdigit(s[pos])) pos++;
        if (pos < s.size() && s[pos] == '.') {
            pos++;
            while (pos < s.size() && std::isdigit(s[pos])) pos++;
        }
        if (pos < s.size() && (s[pos] == 'e' || s[pos] == 'E')) {
            pos++;
            if (pos < s.size() && (s[pos] == '-' || s[pos] == '+')) pos++;
            while (pos < s.size() && std::isdigit(s[pos])) pos++;
        }
        return std::stod(s.substr(start, pos - start));
    }

    // Skip any JSON value (used to skip unknown keys)
    void skipValue() {
        skipWS();
        if (pos >= s.size()) return;
        char c = s[pos];
        if (c == '"') {
            parseString();
        } else if (c == '{') {
            pos++;
            skipWS();
            if (pos < s.size() && s[pos] == '}') { pos++; return; }
            // Skip key-value pairs
            while (true) {
                parseString();  // key
                expect(':');
                skipValue();
                if (!match(',')) break;
            }
            expect('}');
        } else if (c == '[') {
            pos++;
            skipWS();
            if (pos < s.size() && s[pos] == ']') { pos++; return; }
            while (true) {
                skipValue();
                if (!match(',')) break;
            }
            expect(']');
        } else if (c == 't' || c == 'f' || c == 'n') {
            // true, false, null
            while (pos < s.size() && std::isalpha(s[pos])) pos++;
        } else {
            // number
            parseNumber();
        }
    }

    // Parse a coordinate [lon, lat]
    Point parseCoordinate() {
        expect('[');
        double x = parseNumber();
        expect(',');
        double y = parseNumber();
        // Optionally skip altitude
        if (match(',')) parseNumber();
        expect(']');
        return Point(x, y);
    }

    // Parse array of coordinates [[lon,lat], ...]
    std::vector<Point> parseCoordinateArray() {
        std::vector<Point> pts;
        expect('[');
        pts.push_back(parseCoordinate());
        while (match(',')) pts.push_back(parseCoordinate());
        expect(']');
        return pts;
    }

    // Parse a ring (array of coordinate arrays)
    Ring parseRingCoords() {
        Ring r;
        r.points = parseCoordinateArray();
        r.close();
        return r;
    }

    // Parse polygon coordinates [ring, ring, ...]
    void parsePolygonCoords(Ring& ext, std::vector<Ring>& holes) {
        expect('[');
        ext = parseRingCoords();
        while (match(',')) holes.push_back(parseRingCoords());
        expect(']');
    }

    // Parse a geometry object. Expects { "type": ..., "coordinates": ... }
    Geometry parseGeometry() {
        expect('{');

        std::string gtype;
        bool hasCoords = false;
        bool hasGeometry = false;

        // We need to find "type" and "coordinates"/"geometry"
        // Since JSON object keys can be in any order, we do two-pass or store.
        // Simpler: scan forward for the type first, then handle coordinates.

        // Save position and scan for type and coordinates
        size_t objStart = pos;

        // Collect key-value pairs we care about
        std::string typeStr;
        size_t coordsPos = 0;
        size_t geomPos = 0;
        // Also handle "geometries" for GeometryCollection if needed

        // First pass: find type
        while (true) {
            std::string key = parseString();
            expect(':');
            if (key == "type") {
                typeStr = parseString();
            } else if (key == "coordinates") {
                coordsPos = pos;  // save position
                skipValue();
                hasCoords = true;
            } else if (key == "geometry") {
                geomPos = pos;
                skipValue();
                hasGeometry = true;
            } else {
                skipValue();
            }
            if (!match(',')) break;
        }
        expect('}');

        // Handle Feature wrapper
        if (typeStr == "Feature" && hasGeometry) {
            size_t savedPos = pos;
            pos = geomPos;
            Geometry g = parseGeometry();
            pos = savedPos;
            return g;
        }

        if (!hasCoords)
            throw std::runtime_error("GeoJSON: no coordinates found");

        // Now reparse coordinates from the saved position
        size_t endPos = pos;
        pos = coordsPos;

        Geometry g;
        if (typeStr == "Point") {
            Point p = parseCoordinate();
            g = Geometry::makePoint(p.x, p.y);
        } else if (typeStr == "LineString") {
            auto pts = parseCoordinateArray();
            g = Geometry::makeLineString(pts);
        } else if (typeStr == "Polygon") {
            Ring ext;
            std::vector<Ring> holes;
            parsePolygonCoords(ext, holes);
            g = Geometry::makePolygon(ext, holes);
        } else if (typeStr == "MultiPoint") {
            expect('[');
            std::vector<Point> pts;
            pts.push_back(parseCoordinate());
            while (match(',')) pts.push_back(parseCoordinate());
            expect(']');
            g = Geometry::makeMultiPoint(pts);
        } else if (typeStr == "MultiPolygon") {
            expect('[');
            std::vector<Geometry> polys;
            {
                Ring ext;
                std::vector<Ring> holes;
                parsePolygonCoords(ext, holes);
                polys.push_back(Geometry::makePolygon(ext, holes));
            }
            while (match(',')) {
                Ring ext;
                std::vector<Ring> holes;
                parsePolygonCoords(ext, holes);
                polys.push_back(Geometry::makePolygon(ext, holes));
            }
            expect(']');
            g = Geometry::makeMultiPolygon(polys);
        } else {
            throw std::runtime_error("GeoJSON: unsupported type: " + typeStr);
        }

        pos = endPos;
        return g;
    }
};

Geometry parseGeoJSON(const std::string& json) {
    JSONParser parser(json);
    return parser.parseGeometry();
}

// ============================================================================
// GeoJSON serialization
// ============================================================================

static std::string coordJSON(const Point& p) {
    return "[" + fmtCoord(p.x) + "," + fmtCoord(p.y) + "]";
}

static std::string coordArrayJSON(const std::vector<Point>& pts) {
    std::string s = "[";
    for (size_t i = 0; i < pts.size(); i++) {
        if (i) s += ",";
        s += coordJSON(pts[i]);
    }
    return s + "]";
}

static std::string ringJSON(const Ring& r) {
    return coordArrayJSON(r.points);
}

std::string toGeoJSON(const Geometry& geom) {
    switch (geom.type) {
        case GeomType::POINT:
            return "{\"type\":\"Point\",\"coordinates\":" + coordJSON(geom.point) + "}";

        case GeomType::LINESTRING:
            return "{\"type\":\"LineString\",\"coordinates\":" + coordArrayJSON(geom.points) + "}";

        case GeomType::POLYGON: {
            std::string s = "{\"type\":\"Polygon\",\"coordinates\":[" + ringJSON(geom.exterior);
            for (auto& h : geom.holes) s += "," + ringJSON(h);
            return s + "]}";
        }

        case GeomType::MULTIPOINT: {
            std::string s = "{\"type\":\"MultiPoint\",\"coordinates\":[";
            for (size_t i = 0; i < geom.points.size(); i++) {
                if (i) s += ",";
                s += coordJSON(geom.points[i]);
            }
            return s + "]}";
        }

        case GeomType::MULTILINESTRING: {
            std::string s = "{\"type\":\"MultiLineString\",\"coordinates\":[";
            for (size_t i = 0; i < geom.parts.size(); i++) {
                if (i) s += ",";
                s += coordArrayJSON(geom.parts[i].points);
            }
            return s + "]}";
        }

        case GeomType::MULTIPOLYGON: {
            std::string s = "{\"type\":\"MultiPolygon\",\"coordinates\":[";
            for (size_t i = 0; i < geom.parts.size(); i++) {
                if (i) s += ",";
                s += "[" + ringJSON(geom.parts[i].exterior);
                for (auto& h : geom.parts[i].holes) s += "," + ringJSON(h);
                s += "]";
            }
            return s + "]}";
        }
    }
    return "";
}

// ============================================================================
// Point-in-polygon — ray casting algorithm
// ============================================================================

bool pointInPolygon(const Point& p, const Ring& ring) {
    const auto& pts = ring.points;
    int n = static_cast<int>(pts.size());
    if (n < 3) return false;

    bool inside = false;
    for (int i = 0, j = n - 1; i < n; j = i++) {
        double yi = pts[i].y, yj = pts[j].y;
        double xi = pts[i].x, xj = pts[j].x;
        // Check if ray from p to +x crosses edge (i,j)
        if (((yi > p.y) != (yj > p.y)) &&
            (p.x < (xj - xi) * (p.y - yi) / (yj - yi) + xi)) {
            inside = !inside;
        }
    }
    return inside;
}

bool pointInGeometry(const Point& p, const Geometry& geom) {
    if (geom.type == GeomType::POLYGON) {
        // Must be inside exterior ring
        if (!pointInPolygon(p, geom.exterior)) return false;
        // Must be outside all holes
        for (auto& h : geom.holes) {
            if (pointInPolygon(p, h)) return false;
        }
        return true;
    } else if (geom.type == GeomType::MULTIPOLYGON) {
        for (auto& part : geom.parts) {
            if (pointInGeometry(p, part)) return true;
        }
        return false;
    }
    return false;
}

// ============================================================================
// Polygon boolean operations — Sutherland-Hodgman clipping
// ============================================================================

// Sutherland-Hodgman: clip subject polygon by clip polygon
// Both represented as vectors of points (not closed — or closed, we handle both)
static std::vector<Point> sutherlandHodgmanClip(const std::vector<Point>& subject,
                                                 const std::vector<Point>& clip) {
    if (subject.empty() || clip.empty()) return {};

    // Remove closing duplicate if present
    auto stripClose = [](const std::vector<Point>& pts) -> std::vector<Point> {
        if (pts.size() >= 2 && pts.front() == pts.back())
            return std::vector<Point>(pts.begin(), pts.end() - 1);
        return pts;
    };

    std::vector<Point> output = stripClose(subject);
    std::vector<Point> clipPts = stripClose(clip);

    if (output.empty() || clipPts.empty()) return {};

    int clipN = static_cast<int>(clipPts.size());

    for (int i = 0; i < clipN; i++) {
        if (output.empty()) return {};

        std::vector<Point> input = output;
        output.clear();

        Point edgeA = clipPts[i];
        Point edgeB = clipPts[(i + 1) % clipN];

        // "Inside" test: point is on left side of edge A->B
        auto isInside = [&](const Point& p) -> bool {
            return (edgeB.x - edgeA.x) * (p.y - edgeA.y) -
                   (edgeB.y - edgeA.y) * (p.x - edgeA.x) >= 0;
        };

        // Line intersection of segment (p1,p2) with edge (edgeA, edgeB)
        auto intersect = [&](const Point& p1, const Point& p2) -> Point {
            double x1 = p1.x, y1 = p1.y, x2 = p2.x, y2 = p2.y;
            double x3 = edgeA.x, y3 = edgeA.y, x4 = edgeB.x, y4 = edgeB.y;
            double denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
            if (std::fabs(denom) < 1e-15) return p1;  // parallel
            double t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom;
            return Point(x1 + t * (x2 - x1), y1 + t * (y2 - y1));
        };

        int inputN = static_cast<int>(input.size());
        for (int j = 0; j < inputN; j++) {
            Point cur = input[j];
            Point prev = input[(j + inputN - 1) % inputN];
            bool curIn = isInside(cur);
            bool prevIn = isInside(prev);

            if (curIn) {
                if (!prevIn) output.push_back(intersect(prev, cur));
                output.push_back(cur);
            } else if (prevIn) {
                output.push_back(intersect(prev, cur));
            }
        }
    }

    return output;
}

// Ring area (signed) — positive for CCW
static double signedRingArea(const std::vector<Point>& pts) {
    double area = 0;
    int n = static_cast<int>(pts.size());
    for (int i = 0; i < n; i++) {
        int j = (i + 1) % n;
        area += pts[i].x * pts[j].y;
        area -= pts[j].x * pts[i].y;
    }
    return area / 2.0;
}

// Ensure ring is CCW (positive area)
static std::vector<Point> ensureCCW(const std::vector<Point>& pts) {
    auto stripped = pts;
    if (stripped.size() >= 2 && stripped.front() == stripped.back())
        stripped.pop_back();
    if (signedRingArea(stripped) < 0)
        std::reverse(stripped.begin(), stripped.end());
    return stripped;
}

// Build a polygon from clipped points
static Geometry makePolyFromPoints(const std::vector<Point>& pts) {
    if (pts.size() < 3) {
        // Degenerate — return empty polygon
        Ring r;
        return Geometry::makePolygon(r);
    }
    Ring r;
    r.points = pts;
    r.close();
    return Geometry::makePolygon(r);
}

// Check if two polygon bounding boxes overlap
static bool bboxOverlap(const Geometry& a, const Geometry& b) {
    auto bounds = [](const Ring& r, double& minx, double& miny, double& maxx, double& maxy) {
        minx = miny = std::numeric_limits<double>::max();
        maxx = maxy = std::numeric_limits<double>::lowest();
        for (auto& p : r.points) {
            minx = std::min(minx, p.x);
            miny = std::min(miny, p.y);
            maxx = std::max(maxx, p.x);
            maxy = std::max(maxy, p.y);
        }
    };
    double ax1, ay1, ax2, ay2, bx1, by1, bx2, by2;
    bounds(a.exterior, ax1, ay1, ax2, ay2);
    bounds(b.exterior, bx1, by1, bx2, by2);
    return !(ax2 < bx1 || bx2 < ax1 || ay2 < by1 || by2 < ay1);
}

Geometry polygonIntersection(const Geometry& a, const Geometry& b) {
    if (a.type != GeomType::POLYGON || b.type != GeomType::POLYGON) {
        throw std::runtime_error("polygonIntersection: both arguments must be POLYGON");
    }
    if (!bboxOverlap(a, b)) {
        Ring empty;
        return Geometry::makePolygon(empty);
    }

    auto result = sutherlandHodgmanClip(a.exterior.points, b.exterior.points);
    return makePolyFromPoints(result);
}

Geometry polygonUnion(const Geometry& a, const Geometry& b) {
    if (a.type != GeomType::POLYGON || b.type != GeomType::POLYGON) {
        throw std::runtime_error("polygonUnion: both arguments must be POLYGON");
    }

    // If no overlap, return multipolygon
    if (!bboxOverlap(a, b)) {
        return Geometry::makeMultiPolygon({a, b});
    }

    // Check if intersection is non-empty
    auto inter = sutherlandHodgmanClip(a.exterior.points, b.exterior.points);
    if (inter.size() < 3) {
        return Geometry::makeMultiPolygon({a, b});
    }

    // Compute merged boundary:
    // Collect all vertices from both polygons, compute convex hull of exteriors
    // as a practical approximation for simple convex or near-convex polygons.
    // For truly general polygons, this is an approximation.
    // A more correct approach: combine boundary segments outside the other polygon.

    // Practical approach: collect all points from A that are outside B,
    // all points from B that are outside A, and all intersection points.
    // Then order them and build the union boundary.

    // For a robust implementation we use the following approach:
    // For convex polygons, the union is the convex hull of all points.
    // For general polygons, we fall back to multipolygon if the convex hull
    // approach would lose too much.

    // Collect all unique points
    std::vector<Point> allPts;
    auto addRingPts = [&](const Ring& r) {
        for (auto& p : r.points) allPts.push_back(p);
    };
    addRingPts(a.exterior);
    addRingPts(b.exterior);

    // Use convex hull as approximation if both are convex-ish
    // Otherwise return multipolygon
    // Check: is convex hull area roughly equal to area(A) + area(B) - area(intersection)?
    double areaA = std::fabs(signedRingArea(a.exterior.points));
    double areaB = std::fabs(signedRingArea(b.exterior.points));
    double areaI = std::fabs(signedRingArea(inter));

    // Compute convex hull of all points
    Geometry tempMP = Geometry::makeMultiPoint(allPts);
    Geometry hull = convexHull(tempMP);

    double hullArea = std::fabs(signedRingArea(hull.exterior.points));
    double expectedArea = areaA + areaB - areaI;

    // If convex hull area is close to expected union area, use it
    if (expectedArea > 0 && hullArea / expectedArea < 1.1) {
        return hull;
    }

    // Otherwise return as multipolygon (safe fallback)
    return Geometry::makeMultiPolygon({a, b});
}

Geometry polygonDifference(const Geometry& a, const Geometry& b) {
    if (a.type != GeomType::POLYGON || b.type != GeomType::POLYGON) {
        throw std::runtime_error("polygonDifference: both arguments must be POLYGON");
    }

    if (!bboxOverlap(a, b)) {
        return a;  // No overlap, A unchanged
    }

    // Check if B fully contains A
    auto inter = sutherlandHodgmanClip(a.exterior.points, b.exterior.points);
    double areaA = std::fabs(signedRingArea(a.exterior.points));
    double areaI = inter.size() >= 3 ? std::fabs(signedRingArea(inter)) : 0.0;

    if (inter.size() < 3 || areaI < 1e-15) {
        return a;  // No intersection
    }

    // If intersection equals A (B fully contains A), return empty
    if (areaA > 0 && std::fabs(areaI - areaA) / areaA < 0.01) {
        Ring empty;
        return Geometry::makePolygon(empty);
    }

    // A minus B: return A with B's intersection as a hole
    Ring hole;
    hole.points = inter;
    hole.close();
    std::vector<Ring> holes = a.holes;
    holes.push_back(hole);
    return Geometry::makePolygon(a.exterior, holes);
}

Geometry polygonSymDifference(const Geometry& a, const Geometry& b) {
    // Symmetric difference = difference(A,B) + difference(B,A)
    Geometry dAB = polygonDifference(a, b);
    Geometry dBA = polygonDifference(b, a);

    // Combine results
    std::vector<Geometry> parts;
    if (dAB.type == GeomType::MULTIPOLYGON) {
        parts.insert(parts.end(), dAB.parts.begin(), dAB.parts.end());
    } else if (dAB.type == GeomType::POLYGON && dAB.exterior.points.size() >= 3) {
        parts.push_back(dAB);
    }
    if (dBA.type == GeomType::MULTIPOLYGON) {
        parts.insert(parts.end(), dBA.parts.begin(), dBA.parts.end());
    } else if (dBA.type == GeomType::POLYGON && dBA.exterior.points.size() >= 3) {
        parts.push_back(dBA);
    }

    if (parts.empty()) {
        Ring empty;
        return Geometry::makePolygon(empty);
    }
    if (parts.size() == 1) return parts[0];
    return Geometry::makeMultiPolygon(parts);
}

// ============================================================================
// Buffer
// ============================================================================

Geometry polygonBuffer(const Geometry& geom, double distance, int segments) {
    if (geom.type == GeomType::POINT) {
        // Create a circle approximation around the point
        Ring r;
        for (int i = 0; i < segments; i++) {
            double angle = 2.0 * M_PI * i / segments;
            r.points.push_back(Point(
                geom.point.x + distance * std::cos(angle),
                geom.point.y + distance * std::sin(angle)
            ));
        }
        r.close();
        return Geometry::makePolygon(r);
    }

    if (geom.type == GeomType::LINESTRING) {
        // Buffer a linestring: create offset curves on both sides and join
        // Simplified: create a polygon around the line
        const auto& pts = geom.points;
        if (pts.size() < 2) {
            Ring empty;
            return Geometry::makePolygon(empty);
        }

        std::vector<Point> left, right;
        for (size_t i = 0; i < pts.size(); i++) {
            double dx, dy;
            if (i == 0) {
                dx = pts[1].x - pts[0].x;
                dy = pts[1].y - pts[0].y;
            } else if (i == pts.size() - 1) {
                dx = pts[i].x - pts[i - 1].x;
                dy = pts[i].y - pts[i - 1].y;
            } else {
                dx = pts[i + 1].x - pts[i - 1].x;
                dy = pts[i + 1].y - pts[i - 1].y;
            }
            double len = std::sqrt(dx * dx + dy * dy);
            if (len < 1e-15) { len = 1.0; }
            double nx = -dy / len * distance;
            double ny = dx / len * distance;
            left.push_back(Point(pts[i].x + nx, pts[i].y + ny));
            right.push_back(Point(pts[i].x - nx, pts[i].y - ny));
        }

        // Add semicircle caps at start and end
        Ring r;
        // Left side forward
        for (auto& p : left) r.points.push_back(p);
        // End cap (semicircle around last point)
        {
            double dx = pts.back().x - pts[pts.size() - 2].x;
            double dy = pts.back().y - pts[pts.size() - 2].y;
            double baseAngle = std::atan2(dy, dx);
            for (int i = 0; i <= segments / 2; i++) {
                double angle = baseAngle - M_PI / 2.0 + M_PI * i / (segments / 2);
                r.points.push_back(Point(
                    pts.back().x + distance * std::cos(angle),
                    pts.back().y + distance * std::sin(angle)
                ));
            }
        }
        // Right side backward
        for (int i = static_cast<int>(right.size()) - 1; i >= 0; i--)
            r.points.push_back(right[i]);
        // Start cap (semicircle around first point)
        {
            double dx = pts[1].x - pts[0].x;
            double dy = pts[1].y - pts[0].y;
            double baseAngle = std::atan2(dy, dx);
            for (int i = 0; i <= segments / 2; i++) {
                double angle = baseAngle + M_PI / 2.0 + M_PI * i / (segments / 2);
                r.points.push_back(Point(
                    pts.front().x + distance * std::cos(angle),
                    pts.front().y + distance * std::sin(angle)
                ));
            }
        }
        r.close();
        return Geometry::makePolygon(r);
    }

    if (geom.type == GeomType::POLYGON) {
        // Buffer each vertex outward along vertex normal
        const auto& pts = geom.exterior.points;
        if (pts.size() < 4) {
            Ring empty;
            return Geometry::makePolygon(empty);
        }

        // Work with non-closed ring
        std::vector<Point> verts(pts.begin(), pts.end());
        if (verts.size() >= 2 && verts.front() == verts.back()) verts.pop_back();
        int n = static_cast<int>(verts.size());

        Ring buffered;
        for (int i = 0; i < n; i++) {
            int prev = (i + n - 1) % n;
            int next = (i + 1) % n;

            // Edge vectors
            double e1x = verts[i].x - verts[prev].x;
            double e1y = verts[i].y - verts[prev].y;
            double e2x = verts[next].x - verts[i].x;
            double e2y = verts[next].y - verts[i].y;

            // Outward normals (for CCW winding, outward = left of edge direction)
            double len1 = std::sqrt(e1x * e1x + e1y * e1y);
            double len2 = std::sqrt(e2x * e2x + e2y * e2y);
            if (len1 < 1e-15) len1 = 1.0;
            if (len2 < 1e-15) len2 = 1.0;

            double n1x = -e1y / len1, n1y = e1x / len1;
            double n2x = -e2y / len2, n2y = e2x / len2;

            // Check winding — if CW, flip normals
            double area = signedRingArea(verts);
            if (area < 0) {
                n1x = -n1x; n1y = -n1y;
                n2x = -n2x; n2y = -n2y;
            }

            // Bisector
            double bx = n1x + n2x, by = n1y + n2y;
            double blen = std::sqrt(bx * bx + by * by);
            if (blen < 1e-15) {
                bx = n1x; by = n1y; blen = 1.0;
            }
            bx /= blen;
            by /= blen;

            // Scale factor: distance / cos(half-angle)
            double cosHalf = n1x * bx + n1y * by;
            if (std::fabs(cosHalf) < 0.1) cosHalf = 0.1;  // clamp for very sharp angles
            double offset = distance / cosHalf;

            buffered.points.push_back(Point(verts[i].x + bx * offset, verts[i].y + by * offset));
        }
        buffered.close();

        // Holes: buffer inward (reduce hole size) — skip for simplicity, keep original holes
        return Geometry::makePolygon(buffered, geom.holes);
    }

    // Fallback for multi-types: buffer each part
    if (geom.type == GeomType::MULTIPOLYGON) {
        std::vector<Geometry> parts;
        for (auto& part : geom.parts) {
            parts.push_back(polygonBuffer(part, distance, segments));
        }
        return Geometry::makeMultiPolygon(parts);
    }

    // Unsupported type — return as-is
    return geom;
}

// ============================================================================
// Polygon area — Shoelace formula
// ============================================================================

static double ringArea(const Ring& r) {
    double area = 0;
    int n = static_cast<int>(r.points.size());
    for (int i = 0; i < n; i++) {
        int j = (i + 1) % n;
        area += r.points[i].x * r.points[j].y;
        area -= r.points[j].x * r.points[i].y;
    }
    return std::fabs(area) / 2.0;
}

double polygonArea(const Geometry& geom) {
    if (geom.type == GeomType::POLYGON) {
        double area = ringArea(geom.exterior);
        for (auto& h : geom.holes) area -= ringArea(h);
        return std::fabs(area);
    } else if (geom.type == GeomType::MULTIPOLYGON) {
        double total = 0;
        for (auto& p : geom.parts) total += polygonArea(p);
        return total;
    }
    return 0;
}

// ============================================================================
// Centroid — standard polygon centroid formula
// ============================================================================

Point centroid(const Geometry& geom) {
    if (geom.type == GeomType::POINT) return geom.point;

    if (geom.type == GeomType::LINESTRING) {
        if (geom.points.empty()) return Point(0, 0);
        double sx = 0, sy = 0;
        for (auto& p : geom.points) { sx += p.x; sy += p.y; }
        return Point(sx / geom.points.size(), sy / geom.points.size());
    }

    if (geom.type == GeomType::MULTIPOINT) {
        if (geom.points.empty()) return Point(0, 0);
        double sx = 0, sy = 0;
        for (auto& p : geom.points) { sx += p.x; sy += p.y; }
        return Point(sx / geom.points.size(), sy / geom.points.size());
    }

    if (geom.type == GeomType::POLYGON) {
        const auto& pts = geom.exterior.points;
        int n = static_cast<int>(pts.size());
        if (n < 3) return Point(0, 0);

        double cx = 0, cy = 0, A = 0;
        for (int i = 0; i < n; i++) {
            int j = (i + 1) % n;
            double cross = pts[i].x * pts[j].y - pts[j].x * pts[i].y;
            A += cross;
            cx += (pts[i].x + pts[j].x) * cross;
            cy += (pts[i].y + pts[j].y) * cross;
        }
        A /= 2.0;
        if (std::fabs(A) < 1e-15) {
            // Degenerate — return average
            double sx = 0, sy = 0;
            for (auto& p : pts) { sx += p.x; sy += p.y; }
            return Point(sx / n, sy / n);
        }
        cx /= (6.0 * A);
        cy /= (6.0 * A);
        return Point(cx, cy);
    }

    if (geom.type == GeomType::MULTIPOLYGON) {
        double totalArea = 0, cx = 0, cy = 0;
        for (auto& part : geom.parts) {
            double a = polygonArea(part);
            Point c = centroid(part);
            cx += c.x * a;
            cy += c.y * a;
            totalArea += a;
        }
        if (totalArea < 1e-15) return Point(0, 0);
        return Point(cx / totalArea, cy / totalArea);
    }

    return Point(0, 0);
}

// ============================================================================
// Line length — haversine distance
// ============================================================================

static double haversineDist(const Point& a, const Point& b) {
    double lat1 = degToRad(a.y), lat2 = degToRad(b.y);
    double dlat = lat2 - lat1;
    double dlon = degToRad(b.x - a.x);
    double h = std::sin(dlat / 2) * std::sin(dlat / 2) +
               std::cos(lat1) * std::cos(lat2) *
               std::sin(dlon / 2) * std::sin(dlon / 2);
    return 2.0 * EARTH_RADIUS_KM * std::asin(std::sqrt(h));
}

double lineLength(const Geometry& geom) {
    if (geom.type == GeomType::LINESTRING) {
        double total = 0;
        for (size_t i = 1; i < geom.points.size(); i++) {
            total += haversineDist(geom.points[i - 1], geom.points[i]);
        }
        return total;
    }
    if (geom.type == GeomType::MULTILINESTRING) {
        double total = 0;
        for (auto& part : geom.parts) total += lineLength(part);
        return total;
    }
    return 0;
}

// ============================================================================
// Envelope — bounding box as POLYGON
// ============================================================================

static void collectPoints(const Geometry& geom, std::vector<Point>& pts) {
    switch (geom.type) {
        case GeomType::POINT:
            pts.push_back(geom.point);
            break;
        case GeomType::LINESTRING:
        case GeomType::MULTIPOINT:
            pts.insert(pts.end(), geom.points.begin(), geom.points.end());
            break;
        case GeomType::POLYGON:
            pts.insert(pts.end(), geom.exterior.points.begin(), geom.exterior.points.end());
            break;
        case GeomType::MULTILINESTRING:
        case GeomType::MULTIPOLYGON:
            for (auto& part : geom.parts) collectPoints(part, pts);
            break;
    }
}

Geometry envelope(const Geometry& geom) {
    std::vector<Point> pts;
    collectPoints(geom, pts);

    if (pts.empty()) {
        Ring empty;
        return Geometry::makePolygon(empty);
    }

    double minx = pts[0].x, miny = pts[0].y;
    double maxx = pts[0].x, maxy = pts[0].y;
    for (auto& p : pts) {
        minx = std::min(minx, p.x);
        miny = std::min(miny, p.y);
        maxx = std::max(maxx, p.x);
        maxy = std::max(maxy, p.y);
    }

    Ring r;
    r.points = {
        Point(minx, miny), Point(maxx, miny),
        Point(maxx, maxy), Point(minx, maxy),
        Point(minx, miny)
    };
    return Geometry::makePolygon(r);
}

// ============================================================================
// Convex hull — Graham scan
// ============================================================================

Geometry convexHull(const Geometry& geom) {
    std::vector<Point> pts;
    collectPoints(geom, pts);

    if (pts.size() < 3) {
        if (pts.size() == 1) return Geometry::makePoint(pts[0].x, pts[0].y);
        if (pts.size() == 2) return Geometry::makeLineString(pts);
        Ring empty;
        return Geometry::makePolygon(empty);
    }

    // Sort by y (then x)
    std::sort(pts.begin(), pts.end());

    // Remove duplicates
    pts.erase(std::unique(pts.begin(), pts.end()), pts.end());

    if (pts.size() < 3) {
        if (pts.size() == 1) return Geometry::makePoint(pts[0].x, pts[0].y);
        if (pts.size() == 2) return Geometry::makeLineString(pts);
    }

    int n = static_cast<int>(pts.size());

    // Cross product of vectors OA and OB where O = o, A = a, B = b
    auto cross = [](const Point& o, const Point& a, const Point& b) -> double {
        return (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x);
    };

    // Build lower hull
    std::vector<Point> lower;
    for (int i = 0; i < n; i++) {
        while (lower.size() >= 2 && cross(lower[lower.size() - 2], lower[lower.size() - 1], pts[i]) <= 0)
            lower.pop_back();
        lower.push_back(pts[i]);
    }

    // Build upper hull
    std::vector<Point> upper;
    for (int i = n - 1; i >= 0; i--) {
        while (upper.size() >= 2 && cross(upper[upper.size() - 2], upper[upper.size() - 1], pts[i]) <= 0)
            upper.pop_back();
        upper.push_back(pts[i]);
    }

    // Concatenate — remove last point of each half (it's the first of the other)
    lower.pop_back();
    upper.pop_back();
    lower.insert(lower.end(), upper.begin(), upper.end());

    Ring r;
    r.points = lower;
    r.close();
    return Geometry::makePolygon(r);
}

// ============================================================================
// Voronoi diagram — half-plane intersection (brute-force) approach
// ============================================================================

// Clip a convex polygon by a half-plane: keep points on the left of line (a -> b)
static std::vector<Point> clipByHalfPlane(const std::vector<Point>& poly,
                                           const Point& a, const Point& b) {
    if (poly.empty()) return {};

    auto side = [&](const Point& p) -> double {
        return (b.x - a.x) * (p.y - a.y) - (b.y - a.y) * (p.x - a.x);
    };

    auto lineIntersect = [&](const Point& p1, const Point& p2) -> Point {
        double x1 = p1.x, y1 = p1.y, x2 = p2.x, y2 = p2.y;
        double x3 = a.x, y3 = a.y, x4 = b.x, y4 = b.y;
        double denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
        if (std::fabs(denom) < 1e-15) return p1;
        double t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom;
        return Point(x1 + t * (x2 - x1), y1 + t * (y2 - y1));
    };

    std::vector<Point> result;
    int n = static_cast<int>(poly.size());
    for (int i = 0; i < n; i++) {
        int j = (i + 1) % n;
        double si = side(poly[i]);
        double sj = side(poly[j]);
        if (si >= 0) {
            result.push_back(poly[i]);
            if (sj < 0) result.push_back(lineIntersect(poly[i], poly[j]));
        } else if (sj >= 0) {
            result.push_back(lineIntersect(poly[i], poly[j]));
        }
    }
    return result;
}

Geometry voronoiDiagram(const std::vector<Point>& sites, const Geometry& bounds) {
    if (sites.empty()) {
        return Geometry::makeMultiPolygon({});
    }

    // Extract bounding polygon points (strip closing point)
    std::vector<Point> boundPts;
    if (bounds.type == GeomType::POLYGON && !bounds.exterior.points.empty()) {
        boundPts = bounds.exterior.points;
        if (boundPts.size() >= 2 && boundPts.front() == boundPts.back())
            boundPts.pop_back();
    } else {
        throw std::runtime_error("voronoiDiagram: bounds must be a POLYGON");
    }

    std::vector<Geometry> cells;

    for (size_t i = 0; i < sites.size(); i++) {
        // Start with the bounding polygon
        std::vector<Point> cell = boundPts;

        for (size_t j = 0; j < sites.size(); j++) {
            if (i == j) continue;
            if (cell.size() < 3) break;

            // Perpendicular bisector between site[i] and site[j]
            // The half-plane on site[i]'s side
            Point mid((sites[i].x + sites[j].x) / 2.0,
                      (sites[i].y + sites[j].y) / 2.0);

            // Direction perpendicular to (sites[j] - sites[i])
            double dx = sites[j].x - sites[i].x;
            double dy = sites[j].y - sites[i].y;

            // Two points on the bisector line
            Point ba(mid.x - dy, mid.y + dx);
            Point bb(mid.x + dy, mid.y - dx);

            // Keep the side containing site[i]
            // Check which side site[i] is on
            double side = (bb.x - ba.x) * (sites[i].y - ba.y) -
                          (bb.y - ba.y) * (sites[i].x - ba.x);
            if (side < 0) std::swap(ba, bb);  // flip to keep site[i] on left

            cell = clipByHalfPlane(cell, ba, bb);
        }

        if (cell.size() >= 3) {
            Ring r;
            r.points = cell;
            r.close();
            cells.push_back(Geometry::makePolygon(r));
        }
    }

    return Geometry::makeMultiPolygon(cells);
}

// ============================================================================
// Delaunay triangulation — Bowyer-Watson incremental algorithm
// ============================================================================

Geometry delaunayTriangulation(const std::vector<Point>& points) {
    if (points.size() < 3) {
        return Geometry::makeMultiPolygon({});
    }

    struct Triangle {
        int a, b, c;  // indices into point array
    };

    // Extended point list: original points + super-triangle vertices
    std::vector<Point> pts = points;

    // Compute bounding box
    double minx = pts[0].x, miny = pts[0].y;
    double maxx = pts[0].x, maxy = pts[0].y;
    for (auto& p : pts) {
        minx = std::min(minx, p.x); miny = std::min(miny, p.y);
        maxx = std::max(maxx, p.x); maxy = std::max(maxy, p.y);
    }

    double dx = maxx - minx, dy = maxy - miny;
    double dmax = std::max(dx, dy);
    double midx = (minx + maxx) / 2.0, midy = (miny + maxy) / 2.0;

    // Super-triangle vertices (indices n, n+1, n+2)
    int n = static_cast<int>(pts.size());
    pts.push_back(Point(midx - 20 * dmax, midy - dmax));
    pts.push_back(Point(midx, midy + 20 * dmax));
    pts.push_back(Point(midx + 20 * dmax, midy - dmax));

    std::vector<Triangle> triangles;
    triangles.push_back({n, n + 1, n + 2});

    // Circumcircle test
    auto circumcircleContains = [&](const Triangle& t, const Point& p) -> bool {
        double ax = pts[t.a].x, ay = pts[t.a].y;
        double bx = pts[t.b].x, by = pts[t.b].y;
        double cx = pts[t.c].x, cy = pts[t.c].y;
        double d = 2.0 * (ax * (by - cy) + bx * (cy - ay) + cx * (ay - by));
        if (std::fabs(d) < 1e-15) return false;
        double ux = ((ax * ax + ay * ay) * (by - cy) +
                     (bx * bx + by * by) * (cy - ay) +
                     (cx * cx + cy * cy) * (ay - by)) / d;
        double uy = ((ax * ax + ay * ay) * (cx - bx) +
                     (bx * bx + by * by) * (ax - cx) +
                     (cx * cx + cy * cy) * (bx - ax)) / d;
        double r2 = (ax - ux) * (ax - ux) + (ay - uy) * (ay - uy);
        double dist2 = (p.x - ux) * (p.x - ux) + (p.y - uy) * (p.y - uy);
        return dist2 < r2;
    };

    // Insert each point
    for (int i = 0; i < n; i++) {
        // Find triangles whose circumcircle contains point i
        std::vector<Triangle> bad, good;
        for (auto& t : triangles) {
            if (circumcircleContains(t, pts[i])) bad.push_back(t);
            else good.push_back(t);
        }

        // Find boundary edges of the polygonal hole
        // An edge is on the boundary if it is not shared by two bad triangles
        struct Edge {
            int a, b;
            bool operator==(const Edge& o) const {
                return (a == o.a && b == o.b) || (a == o.b && b == o.a);
            }
        };

        std::vector<Edge> boundary;
        for (auto& t : bad) {
            Edge edges[3] = {{t.a, t.b}, {t.b, t.c}, {t.c, t.a}};
            for (auto& e : edges) {
                bool shared = false;
                for (auto& t2 : bad) {
                    if (&t == &t2) continue;
                    Edge e2[3] = {{t2.a, t2.b}, {t2.b, t2.c}, {t2.c, t2.a}};
                    for (auto& ee : e2) {
                        if (e == ee) { shared = true; break; }
                    }
                    if (shared) break;
                }
                if (!shared) boundary.push_back(e);
            }
        }

        // Re-triangulate: create new triangles from boundary edges to new point
        triangles = good;
        for (auto& e : boundary) {
            triangles.push_back({e.a, e.b, i});
        }
    }

    // Remove triangles that share vertices with super-triangle
    std::vector<Triangle> finalTris;
    for (auto& t : triangles) {
        if (t.a >= n || t.b >= n || t.c >= n) continue;
        finalTris.push_back(t);
    }

    // Convert to multipolygon
    std::vector<Geometry> polys;
    for (auto& t : finalTris) {
        Ring r;
        r.points = {pts[t.a], pts[t.b], pts[t.c]};
        r.close();
        polys.push_back(Geometry::makePolygon(r));
    }

    return Geometry::makeMultiPolygon(polys);
}

// ============================================================================
// Coordinate transforms — WGS84 geodetic <-> ECEF
// ============================================================================

ECEFPoint geodedicToECEF(double lat_deg, double lon_deg, double alt_m) {
    double lat = degToRad(lat_deg);
    double lon = degToRad(lon_deg);
    double sinLat = std::sin(lat);
    double cosLat = std::cos(lat);
    double sinLon = std::sin(lon);
    double cosLon = std::cos(lon);

    double N = WGS84_A / std::sqrt(1.0 - WGS84_E2 * sinLat * sinLat);

    ECEFPoint r;
    r.x = (N + alt_m) * cosLat * cosLon;
    r.y = (N + alt_m) * cosLat * sinLon;
    r.z = (N * (1.0 - WGS84_E2) + alt_m) * sinLat;
    return r;
}

void ecefToGeodetic(double x, double y, double z,
                    double& lat_deg, double& lon_deg, double& alt_m) {
    // Iterative (Bowring's) method
    lon_deg = radToDeg(std::atan2(y, x));

    double p = std::sqrt(x * x + y * y);

    // Initial estimate of latitude
    double lat = std::atan2(z, p * (1.0 - WGS84_E2));

    // Iterate until convergence
    for (int iter = 0; iter < 20; iter++) {
        double sinLat = std::sin(lat);
        double N = WGS84_A / std::sqrt(1.0 - WGS84_E2 * sinLat * sinLat);
        double latNew = std::atan2(z + WGS84_E2 * N * sinLat, p);
        if (std::fabs(latNew - lat) < 1e-12) {
            lat = latNew;
            break;
        }
        lat = latNew;
    }

    double sinLat = std::sin(lat);
    double N = WGS84_A / std::sqrt(1.0 - WGS84_E2 * sinLat * sinLat);

    // Altitude
    double cosLat = std::cos(lat);
    if (std::fabs(cosLat) > 1e-10) {
        alt_m = p / cosLat - N;
    } else {
        alt_m = std::fabs(z) / std::fabs(sinLat) - N * (1.0 - WGS84_E2);
    }

    lat_deg = radToDeg(lat);
}

}  // namespace geo
}  // namespace flatsql
