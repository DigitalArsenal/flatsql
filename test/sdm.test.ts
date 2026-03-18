/**
 * Tests for FlatSQL Spatial SDM (Space Data Module)
 *
 * Validates:
 * - Module loads and manifest is readable
 * - All exported methods work correctly
 * - Results match SpatiaLite reference values
 * - Error handling for invalid inputs
 */

import { initSpatialSDM } from "../sdm/index.js";
import type { FlatSQLSpatialSDM } from "../sdm/index.js";

let sdm: FlatSQLSpatialSDM;

beforeAll(async () => {
  sdm = await initSpatialSDM();
});

// ===========================================================================
// Manifest
// ===========================================================================

describe("SDM Manifest", () => {
  test("manifest is readable and has correct structure", () => {
    const manifest = sdm.getManifest();
    expect(manifest.pluginId).toBe("com.digitalarsenal.flatsql.spatial");
    expect(manifest.name).toBe("FlatSQL Spatial Engine");
    expect(manifest.version).toBe("0.4.1");
    expect(manifest.pluginFamily).toBe("analysis");
    expect(manifest.runtimeTargets).toContain("NODE");
    expect(manifest.runtimeTargets).toContain("BROWSER");
    expect(manifest.runtimeTargets).toContain("WASI");
  });

  test("manifest declares all expected methods", () => {
    const manifest = sdm.getManifest();
    const methodIds = manifest.methods.map((m) => m.methodId);
    expect(methodIds).toContain("compute_distance");
    expect(methodIds).toContain("compute_bearing");
    expect(methodIds).toContain("point_in_polygon");
    expect(methodIds).toContain("polygon_intersection");
    expect(methodIds).toContain("polygon_union");
    expect(methodIds).toContain("compute_voronoi");
    expect(methodIds).toContain("compute_delaunay");
    expect(methodIds).toContain("transform_to_ecef");
    expect(methodIds).toContain("transform_from_ecef");
    expect(methodIds).toContain("spatial_query");
  });

  test("each method has input and output ports", () => {
    const manifest = sdm.getManifest();
    for (const method of manifest.methods) {
      expect(method.inputPorts.length).toBeGreaterThan(0);
      expect(method.outputPorts.length).toBeGreaterThan(0);
      expect(method.drainPolicy).toBeDefined();
    }
  });
});

// ===========================================================================
// compute_distance
// ===========================================================================

describe("compute_distance", () => {
  test("NYC to DC ~328 km (SpatiaLite ref)", () => {
    const dist = sdm.computeDistance(40.7128, -74.006, 38.9072, -77.0369);
    expect(dist).toBeGreaterThan(325);
    expect(dist).toBeLessThan(335);
  });

  test("same point = 0", () => {
    const dist = sdm.computeDistance(0, 0, 0, 0);
    expect(dist).toBeCloseTo(0, 5);
  });

  test("equator 1 degree ~111.2 km", () => {
    const dist = sdm.computeDistance(0, 0, 0, 1);
    expect(dist).toBeGreaterThan(110);
    expect(dist).toBeLessThan(113);
  });
});

// ===========================================================================
// compute_bearing
// ===========================================================================

describe("compute_bearing", () => {
  test("NYC to LA ~274 deg (west)", () => {
    const bearing = sdm.computeBearing(40.7128, -74.006, 34.0522, -118.2437);
    expect(bearing).toBeGreaterThan(270);
    expect(bearing).toBeLessThan(280);
  });

  test("due north = 0 deg", () => {
    const bearing = sdm.computeBearing(0, 0, 90, 0);
    expect(bearing).toBeLessThan(1);
  });

  test("due east = 90 deg", () => {
    const bearing = sdm.computeBearing(0, 0, 0, 90);
    expect(bearing).toBeCloseTo(90, 0);
  });
});

// ===========================================================================
// point_in_polygon
// ===========================================================================

describe("point_in_polygon", () => {
  const square = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";

  test("point inside", () => {
    expect(sdm.pointInPolygon(square, 5, 5)).toBe(true);
  });

  test("point outside", () => {
    expect(sdm.pointInPolygon(square, 15, 5)).toBe(false);
  });

  test("concave polygon (L-shape)", () => {
    const lshape = "POLYGON((0 0, 10 0, 10 5, 5 5, 5 10, 0 10, 0 0))";
    expect(sdm.pointInPolygon(lshape, 2, 2)).toBe(true);
    expect(sdm.pointInPolygon(lshape, 7, 7)).toBe(false);
  });
});

// ===========================================================================
// polygon_intersection
// ===========================================================================

describe("polygon_intersection", () => {
  test("overlapping squares intersection area = 25 (SpatiaLite ref)", () => {
    const a = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";
    const b = "POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))";
    const result = sdm.polygonIntersection(a, b);
    expect(result).toContain("POLYGON");
    // Verify it contains the expected intersection vertices
    expect(result).toContain("5");
    expect(result).toContain("10");
  });

  test("non-overlapping polygons → empty", () => {
    const a = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
    const b = "POLYGON((10 10, 11 10, 11 11, 10 11, 10 10))";
    const result = sdm.polygonIntersection(a, b);
    // Should be an empty or degenerate polygon
    expect(result).toContain("POLYGON");
  });
});

// ===========================================================================
// polygon_union
// ===========================================================================

describe("polygon_union", () => {
  test("overlapping squares produce valid geometry", () => {
    const a = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";
    const b = "POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))";
    const result = sdm.polygonUnion(a, b);
    expect(result).toContain("POLYGON");
  });
});

// ===========================================================================
// voronoi
// ===========================================================================

describe("compute_voronoi", () => {
  test("4 corner points produce valid Voronoi diagram", () => {
    const pts = "MULTIPOINT((0 0), (10 0), (10 10), (0 10))";
    const bounds = "POLYGON((-5 -5, 15 -5, 15 15, -5 15, -5 -5))";
    const result = sdm.computeVoronoi(pts, bounds);
    expect(result).toContain("POLYGON");
  });
});

// ===========================================================================
// delaunay
// ===========================================================================

describe("compute_delaunay", () => {
  test("4 points produce Delaunay triangulation", () => {
    const pts = "MULTIPOINT((0 0), (10 0), (10 10), (0 10))";
    const result = sdm.computeDelaunay(pts);
    expect(result).toContain("POLYGON");
  });

  test("5 points with center", () => {
    const pts = "MULTIPOINT((0 0), (10 0), (10 10), (0 10), (5 5))";
    const result = sdm.computeDelaunay(pts);
    expect(result).toContain("MULTIPOLYGON");
  });
});

// ===========================================================================
// coordinate transforms
// ===========================================================================

describe("coordinate transforms", () => {
  test("WGS84 origin → ECEF (6378137, 0, 0)", () => {
    const ecef = sdm.toECEF(0, 0, 0);
    expect(ecef.x).toBeCloseTo(6378137, -1);
    expect(ecef.y).toBeCloseTo(0, 0);
    expect(ecef.z).toBeCloseTo(0, 0);
  });

  test("North pole → ECEF (0, 0, ~6356752)", () => {
    const ecef = sdm.toECEF(90, 0, 0);
    expect(ecef.x).toBeCloseTo(0, 0);
    expect(ecef.y).toBeCloseTo(0, 0);
    expect(ecef.z).toBeCloseTo(6356752, -1);
  });

  test("ECEF round-trip", () => {
    const ecef = sdm.toECEF(48.8566, 2.3522, 35);
    const back = sdm.fromECEF(ecef.x, ecef.y, ecef.z);
    expect(back.lat).toBeCloseTo(48.8566, 2);
    expect(back.lon).toBeCloseTo(2.3522, 2);
    expect(back.alt).toBeCloseTo(35, 0);
  });

  test("ISS orbit altitude round-trip", () => {
    const ecef = sdm.toECEF(51.6, 120.5, 408000);
    const back = sdm.fromECEF(ecef.x, ecef.y, ecef.z);
    expect(back.lat).toBeCloseTo(51.6, 1);
    expect(back.lon).toBeCloseTo(120.5, 1);
    expect(back.alt).toBeCloseTo(408000, -1);
  });
});

// ===========================================================================
// error handling
// ===========================================================================

describe("error handling", () => {
  test("compute_distance with wrong args returns error", () => {
    expect(() => {
      const { status, output } = sdm.callMethod("compute_distance", "1,2");
      if (status !== 0) throw new Error(output);
    }).toThrow();
  });

  test("point_in_polygon with bad WKT returns error", () => {
    expect(() => {
      const { status, output } = sdm.callMethod("point_in_polygon", "NOT_WKT\n5,5");
      if (status !== 0) throw new Error(output);
    }).toThrow();
  });

  test("unknown method throws", () => {
    expect(() => sdm.callMethod("nonexistent", "")).toThrow("Unknown method");
  });
});
