/**
 * FlatSQL Spatial Engine — Space Data Module (SDM)
 * TypeScript type definitions
 *
 * @see https://github.com/DigitalArsenal/space-data-module-sdk
 */

export interface ECEFPoint {
  x: number;
  y: number;
  z: number;
}

export interface GeodeticPoint {
  lat: number;
  lon: number;
  alt: number;
}

export interface MethodResult {
  status: number;
  output: string;
}

export interface PluginManifest {
  pluginId: string;
  name: string;
  version: string;
  pluginFamily: string;
  description: string;
  capabilities: string[];
  externalInterfaces: string[];
  methods: Array<{
    methodId: string;
    description: string;
    inputPorts: Array<{
      portId: string;
      schemaNames: string[];
      maxItems: number;
    }>;
    outputPorts: Array<{
      portId: string;
      schemaNames: string[];
      maxItems: number;
    }>;
    maxBatch: number;
    drainPolicy: string;
  }>;
  runtimeTargets: string[];
}

export interface FlatSQLSpatialSDM {
  /** Get the embedded plugin manifest */
  getManifest(): PluginManifest;

  /** Compute haversine distance between two WGS84 points (km) */
  computeDistance(lat1: number, lon1: number, lat2: number, lon2: number): number;

  /** Compute forward azimuth between two WGS84 points (degrees 0-360) */
  computeBearing(lat1: number, lon1: number, lat2: number, lon2: number): number;

  /** Test if a WGS84 point is inside a WKT polygon */
  pointInPolygon(wkt: string, lat: number, lon: number): boolean;

  /** Compute intersection of two WKT polygons */
  polygonIntersection(wkt1: string, wkt2: string): string;

  /** Compute union of two WKT polygons */
  polygonUnion(wkt1: string, wkt2: string): string;

  /** Compute Voronoi diagram from multipoint WKT within bounds */
  computeVoronoi(multipointWkt: string, boundsWkt: string): string;

  /** Compute Delaunay triangulation from multipoint WKT */
  computeDelaunay(multipointWkt: string): string;

  /** Transform WGS84 geodetic to ECEF */
  toECEF(lat: number, lon: number, alt: number): ECEFPoint;

  /** Transform ECEF to WGS84 geodetic */
  fromECEF(x: number, y: number, z: number): GeodeticPoint;

  /** Low-level method call */
  callMethod(methodName: string, input: string): MethodResult;
}

/** Initialize the FlatSQL Spatial SDM module */
export function initSpatialSDM(): Promise<FlatSQLSpatialSDM>;
