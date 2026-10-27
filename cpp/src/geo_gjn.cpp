// Geometry transport is the canonical SDS GJN schema, generated at build time.
// Text conversion belongs to a separate tool (for example flatc-wasm).
#include "flatsql/geo_geometry.h"
#include "GJN/main_generated.h"

#include <cstring>

namespace flatsql::geo {
namespace {
bool validPosition(const GJNPosition* p) {
    return p && std::isfinite(p->LONGITUDE()) && std::isfinite(p->LATITUDE()) &&
           (!p->HAS_ALTITUDE() || std::isfinite(p->ALTITUDE()));
}
bool validPositions(const flatbuffers::Vector<flatbuffers::Offset<GJNPosition>>* ps, bool ring = false) {
    if (!ps) return !ring;
    for (const auto* p : *ps) if (!validPosition(p)) return false;
    if (!ring) return true;
    if (ps->size() < 4) return false;
    const auto* a = ps->Get(0);
    const auto* b = ps->Get(ps->size() - 1);
    return a->LONGITUDE() == b->LONGITUDE() && a->LATITUDE() == b->LATITUDE() &&
           a->HAS_ALTITUDE() == b->HAS_ALTITUDE() && (!a->HAS_ALTITUDE() || a->ALTITUDE() == b->ALTITUDE());
}
bool validRings(const flatbuffers::Vector<flatbuffers::Offset<GJNLinearRing>>* rs, bool polygon) {
    if (rs) for (const auto* r : *rs)
        if (!r || !validPositions(r->POSITIONS(), polygon) ||
            (!polygon && r->POSITIONS() && r->POSITIONS()->size() == 1)) return false;
    return true;
}
bool validGeometry(const GJNGeometry* g, unsigned depth) {
    if (!g || depth > 48) return false;
    switch (g->TYPE()) {
        case GJNGeometryType_POINT: return validPosition(g->POINT());
        case GJNGeometryType_MULTI_POINT: return validPositions(g->POSITIONS());
        case GJNGeometryType_LINE_STRING:
            return validPositions(g->POSITIONS()) && (!g->POSITIONS() || g->POSITIONS()->size() != 1);
        case GJNGeometryType_MULTI_LINE_STRING: return validRings(g->RINGS(), false);
        case GJNGeometryType_POLYGON: return validRings(g->RINGS(), true);
        case GJNGeometryType_MULTI_POLYGON:
            if (g->POLYGON_RINGS()) for (const auto* p : *g->POLYGON_RINGS())
                if (!p || !validRings(p->RINGS(), true)) return false;
            return true;
        case GJNGeometryType_GEOMETRY_COLLECTION:
            if (g->GEOMETRIES()) for (const auto* c : *g->GEOMETRIES())
                if (!validGeometry(c, depth + 1)) return false;
            return true;
        default: return false;
    }
}

Point position(const GJNPosition* p) {
    Point out(p->LONGITUDE(), p->LATITUDE());
    out.z = p->ALTITUDE();
    out.hasAltitude = p->HAS_ALTITUDE();
    return out;
}

using Positions = flatbuffers::Vector<flatbuffers::Offset<GJNPosition>>;
std::vector<Point> positions(const Positions* ps) {
    std::vector<Point> out;
    if (ps) {
        out.reserve(ps->size());
        for (const auto* p : *ps) out.push_back(position(p));
    }
    return out;
}

using Rings = flatbuffers::Vector<flatbuffers::Offset<GJNLinearRing>>;
Geometry polygon(const Rings* rs) {
    Geometry out;
    out.type = GeomType::POLYGON;
    if (rs) for (const auto* r : *rs) {
        Ring ring;
        ring.points = positions(r->POSITIONS());
        if (out.exterior.points.empty()) out.exterior = std::move(ring);
        else out.holes.push_back(std::move(ring));
    }
    return out;
}

Geometry geometry(const GJNGeometry* g, unsigned depth) {
    Geometry out;
    switch (g->TYPE()) {
        case GJNGeometryType_POINT:
            out.type = GeomType::POINT;
            out.point = position(g->POINT());
            break;
        case GJNGeometryType_MULTI_POINT:
        case GJNGeometryType_LINE_STRING:
            out.type = g->TYPE() == GJNGeometryType_MULTI_POINT ? GeomType::MULTIPOINT : GeomType::LINESTRING;
            out.points = positions(g->POSITIONS());
            break;
        case GJNGeometryType_POLYGON:
            out = polygon(g->RINGS());
            break;
        case GJNGeometryType_MULTI_LINE_STRING:
            out.type = GeomType::MULTILINESTRING;
            if (g->RINGS()) for (const auto* r : *g->RINGS()) {
                        auto ps = positions(r->POSITIONS());
                out.parts.push_back(Geometry::makeLineString(ps));
            }
            break;
        case GJNGeometryType_MULTI_POLYGON:
            out.type = GeomType::MULTIPOLYGON;
            if (g->POLYGON_RINGS()) for (const auto* p : *g->POLYGON_RINGS()) {
                out.parts.push_back(polygon(p->RINGS()));
            }
            break;
        case GJNGeometryType_GEOMETRY_COLLECTION:
            out.type = GeomType::COLLECTION;
            if (g->GEOMETRIES()) for (const auto* child : *g->GEOMETRIES())
                out.parts.push_back(geometry(child, depth + 1));
            break;
        default: break;
    }
    return out;
}

using Builder = flatbuffers::FlatBufferBuilder;
auto encodePosition(Builder& b, const Point& p) {
    return CreateGJNPosition(b, p.x, p.y, p.z, p.hasAltitude);
}
auto encodePositions(Builder& b, const std::vector<Point>& ps) {
    std::vector<flatbuffers::Offset<GJNPosition>> out;
    out.reserve(ps.size());
    for (const auto& p : ps) out.push_back(encodePosition(b, p));
    return b.CreateVector(out);
}
auto encodeRings(Builder& b, const Geometry& g) {
    std::vector<flatbuffers::Offset<GJNLinearRing>> out;
    if (!g.exterior.points.empty()) out.push_back(CreateGJNLinearRing(b, encodePositions(b, g.exterior.points)));
    for (const auto& h : g.holes) out.push_back(CreateGJNLinearRing(b, encodePositions(b, h.points)));
    return b.CreateVector(out);
}

flatbuffers::Offset<GJNGeometry> encodeGeometry(Builder& b, const Geometry& g, unsigned depth) {
    if (depth > 48) return 0;
    switch (g.type) {
        case GeomType::POINT: return CreateGJNGeometry(b, GJNGeometryType_POINT, encodePosition(b, g.point));
        case GeomType::MULTIPOINT:
        case GeomType::LINESTRING:
            return CreateGJNGeometry(b, g.type == GeomType::MULTIPOINT ? GJNGeometryType_MULTI_POINT : GJNGeometryType_LINE_STRING,
                                     0, encodePositions(b, g.points));
        case GeomType::POLYGON:
            return CreateGJNGeometry(b, GJNGeometryType_POLYGON, 0, 0, encodeRings(b, g));
        case GeomType::MULTILINESTRING: {
            std::vector<flatbuffers::Offset<GJNLinearRing>> rs;
            for (const auto& part : g.parts) rs.push_back(CreateGJNLinearRing(b, encodePositions(b, part.points)));
            return CreateGJNGeometry(b, GJNGeometryType_MULTI_LINE_STRING, 0, 0, b.CreateVector(rs));
        }
        case GeomType::MULTIPOLYGON: {
            std::vector<flatbuffers::Offset<GJNPolygonRings>> ps;
            for (const auto& part : g.parts) ps.push_back(CreateGJNPolygonRings(b, encodeRings(b, part)));
            return CreateGJNGeometry(b, GJNGeometryType_MULTI_POLYGON, 0, 0, 0, b.CreateVector(ps));
        }
        case GeomType::COLLECTION: {
            std::vector<flatbuffers::Offset<GJNGeometry>> parts;
            for (const auto& part : g.parts) parts.push_back(encodeGeometry(b, part, depth + 1));
            return CreateGJNGeometry(b, GJNGeometryType_GEOMETRY_COLLECTION, 0, 0, 0, 0, b.CreateVector(parts));
        }
    }
    return 0;
}
} // namespace

bool geometryFromFlatBuffer(const uint8_t* data, size_t len, Geometry* result) {
    if (!data || len < 8) return false;
    // SQL _data is a payload. A parameter may also be a size-prefixed frame.
    if (len >= 12 && flatbuffers::ReadScalar<uint32_t>(data) == len - 4 &&
        !std::memcmp(data + 8, "$GJN", 4)) { data += 4; len -= 4; }
    // _data omits the stream's four-byte prefix. Its scalar offsets retain
    // that alignment origin; bounds and nesting checks still apply.
    flatbuffers::Verifier verifier(data, len, 64, 1000000, false);
    if (!VerifyGJNBuffer(verifier)) return false;
    const auto* root = GetGJN(data);
    Geometry out;
    out.type = GeomType::COLLECTION;
    if (root->FEATURES()) for (const auto* feature : *root->FEATURES()) {
        if (!feature) return false;
        if (feature->HAS_GEOMETRY()) {
            if (!validGeometry(feature->GEOMETRY(), 0)) return false;
            out.parts.push_back(geometry(feature->GEOMETRY(), 0));
        }
    }
    // A single feature has its geometry's natural type; multiple features
    // are evaluated as a collection, never discarded in favor of the first.
    if (out.parts.size() == 1) *result = std::move(out.parts.front());
    else *result = std::move(out);
    return true;
}

std::vector<uint8_t> geometryToFlatBuffer(const Geometry& geom) {
    Builder b;
    const auto g = encodeGeometry(b, geom, 0);
    if (g.IsNull()) return {};
    const auto feature = CreateGJNFeature(b, 0, g, 0, 0, false, true);
    const auto root = CreateGJN(b, b.CreateVector(&feature, 1));
    FinishGJNBuffer(b, root);
    return {b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize()};
}
} // namespace flatsql::geo
