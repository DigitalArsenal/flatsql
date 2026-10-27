// geo_geometry.cpp — Computational geometry engine for FlatSQL
// Implements all functions declared in flatsql/geo_geometry.h

#include "flatsql/geo_geometry.h"
#include <cmath>
#include <algorithm>
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
    if (geom.type == GeomType::POINT) return p == geom.point;
    if (geom.type == GeomType::MULTIPOINT)
        return std::find(geom.points.begin(), geom.points.end(), p) != geom.points.end();
    if (geom.type == GeomType::POLYGON) {
        // Must be inside exterior ring
        if (!pointInPolygon(p, geom.exterior)) return false;
        // Must be outside all holes
        for (auto& h : geom.holes) {
            if (pointInPolygon(p, h)) return false;
        }
        return true;
    } else if (geom.type == GeomType::MULTIPOLYGON || geom.type == GeomType::COLLECTION) {
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
        return Geometry::makePolygon(Ring{});
    }
    if (!bboxOverlap(a, b)) {
        Ring empty;
        return Geometry::makePolygon(empty);
    }

    auto result = sutherlandHodgmanClip(ensureCCW(a.exterior.points), ensureCCW(b.exterior.points));
    return makePolyFromPoints(result);
}

// Convex, hole-free operands are checked at the SQL boundary. Split A by
// each edge of B: the outside pieces are disjoint, and only the remaining
// inside piece advances. This is an exact set difference, not a hull estimate.
static std::vector<Point> clipEdge(const std::vector<Point>& input, Point a, Point b, bool inside) {
    std::vector<Point> out;
    if (input.empty()) return out;
    auto side = [&](Point p) { return (b.x-a.x)*(p.y-a.y)-(b.y-a.y)*(p.x-a.x); };
    Point prev=input.back(); double before=side(prev);
    for (Point cur:input) {
        double after=side(cur);
        bool prevIn=inside ? before>=0 : before<=0;
        bool curIn=inside ? after>=0 : after<=0;
        if (prevIn != curIn) {
            double t=before/(before-after);
            out.emplace_back(prev.x+t*(cur.x-prev.x),prev.y+t*(cur.y-prev.y));
        }
        if (curIn) out.emplace_back(cur.x,cur.y);
        prev=cur; before=after;
    }
    return out;
}
Geometry polygonDifference(const Geometry& a, const Geometry& b) {
    auto remaining=ensureCCW(a.exterior.points), clip=ensureCCW(b.exterior.points);
    std::vector<Geometry> parts;
    if (clip.empty()) return a;
    for (size_t i=0;i<clip.size() && !remaining.empty();++i) {
        Point from=clip[i],to=clip[(i+1)%clip.size()];
        auto outside=clipEdge(remaining,from,to,false);
        if (outside.size()>=3 && std::fabs(signedRingArea(outside))>1e-15)
            parts.push_back(makePolyFromPoints(outside));
        remaining=clipEdge(remaining,from,to,true);
    }
    if(parts.size()==1) return parts.front();
    return Geometry::makeMultiPolygon(parts);
}
Geometry polygonUnion(const Geometry& a, const Geometry& b) {
    std::vector<Geometry> parts;
    if(!a.exterior.points.empty())parts.push_back(a);
    auto extra=polygonDifference(b,a);
    if(extra.type==GeomType::MULTIPOLYGON)parts.insert(parts.end(),extra.parts.begin(),extra.parts.end());
    else if(!extra.exterior.points.empty())parts.push_back(extra);
    if(parts.size()==1)return parts.front();
    return Geometry::makeMultiPolygon(parts);
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

    return Geometry::makePolygon(Ring{});
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
    } else if (geom.type == GeomType::MULTIPOLYGON || geom.type == GeomType::COLLECTION) {
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
    // Highest-dimensional members contribute; holes subtract area regardless
    // of ring winding. SQL rejects collections whose total weight is zero.
    struct Sum { double x=0,y=0,w=0; int dimension=-1; } sum;
    auto add=[&](double x,double y,double w,int dimension) {
        if(dimension>sum.dimension)sum={0,0,0,dimension};
        if(dimension==sum.dimension){sum.x+=x*w;sum.y+=y*w;sum.w+=w;}
    };
    auto visit=[&](auto&& self,const Geometry& g)->void {
        if(g.type==GeomType::POINT) {add(g.point.x,g.point.y,1,0);return;}
        if(g.type==GeomType::MULTIPOINT) {for(auto p:g.points)add(p.x,p.y,1,0);return;}
        if(g.type==GeomType::LINESTRING) {
            for(size_t i=1;i<g.points.size();++i) {
                auto a=g.points[i-1],b=g.points[i];double w=std::hypot(a.x-b.x,a.y-b.y);
                if(w>0)add((a.x+b.x)/2,(a.y+b.y)/2,w,1);
            }
            return;
        }
        if(g.type==GeomType::POLYGON) {
            auto ring=[&](const Ring& r,double sign) {
                double area=0,x=0,y=0;
                for(size_t i=1;i<r.points.size();++i) {
                    auto a=r.points[i-1],b=r.points[i];double cross=a.x*b.y-b.x*a.y;
                    area+=cross;x+=(a.x+b.x)*cross;y+=(a.y+b.y)*cross;
                }
                if(std::fabs(area)>1e-15)add(x/(3*area),y/(3*area),sign*std::fabs(area)/2,2);
            };
            ring(g.exterior,1);for(const auto& h:g.holes)ring(h,-1);return;
        }
        for(const auto& part:g.parts)self(self,part);
    };
    visit(visit,geom);
    if(sum.w<=1e-15)return Point(NAN,NAN);
    return Point(sum.x/sum.w,sum.y/sum.w);
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
    if (geom.type == GeomType::MULTILINESTRING || geom.type == GeomType::COLLECTION) {
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
        case GeomType::COLLECTION:
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
        return Geometry::makePolygon(Ring{});
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
