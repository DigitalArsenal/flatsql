/**
 * FlatSQL Spatial Engine — Space Data Module (SDM)
 *
 * Standalone WASM module compliant with space-data-module-sdk.
 * Provides spatial computation functions without requiring full FlatSQL.
 *
 * @see https://github.com/DigitalArsenal/space-data-module-sdk
 */

import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));

/**
 * Initialize the FlatSQL Spatial SDM module.
 * @returns {Promise<FlatSQLSpatialSDM>}
 */
export async function initSpatialSDM() {
  const wasmPath = join(__dirname, "flatsql-spatial.wasm");
  const wasmBytes = await readFile(wasmPath);
  const { instance } = await WebAssembly.instantiate(wasmBytes, {
    env: {},
    wasi_snapshot_preview1: {
      proc_exit: () => {},
      fd_write: () => 0,
      fd_seek: () => 0,
      fd_close: () => 0,
    },
  });

  const exports = instance.exports;
  const memory = /** @type {WebAssembly.Memory} */ (exports.memory);

  /**
   * Write a string to the input buffer.
   * @param {string} str
   */
  function writeInput(str) {
    const inputPtr = /** @type {Function} */ (exports.sdm_get_input_buffer)();
    const encoder = new TextEncoder();
    const bytes = encoder.encode(str);
    const view = new Uint8Array(memory.buffer, inputPtr, bytes.length);
    view.set(bytes);
    /** @type {Function} */ (exports.sdm_set_input_length)(bytes.length);
  }

  /**
   * Read the output buffer as a string.
   * @returns {string}
   */
  function readOutput() {
    const outputLen = /** @type {Function} */ (exports.sdm_get_output_length)();
    const outputPtr = /** @type {Function} */ (exports.sdm_get_output_buffer)();
    const view = new Uint8Array(memory.buffer, outputPtr, outputLen);
    return new TextDecoder().decode(view);
  }

  /**
   * Call a method with string input, return string output.
   * @param {string} methodName
   * @param {string} input
   * @returns {{ status: number, output: string }}
   */
  function callMethod(methodName, input) {
    writeInput(input);
    const fn = exports[methodName];
    if (!fn) throw new Error(`Unknown method: ${methodName}`);
    const status = /** @type {Function} */ (fn)();
    return { status, output: readOutput() };
  }

  /**
   * Get the embedded plugin manifest.
   * @returns {object}
   */
  function getManifest() {
    const ptr = /** @type {Function} */ (exports.plugin_get_manifest)();
    const size = /** @type {Function} */ (exports.plugin_get_manifest_size)();
    const view = new Uint8Array(memory.buffer, ptr, size);
    const text = new TextDecoder().decode(view);
    return JSON.parse(text);
  }

  return {
    /** Get the plugin manifest */
    getManifest,

    /**
     * Compute haversine distance between two points.
     * @param {number} lat1 @param {number} lon1
     * @param {number} lat2 @param {number} lon2
     * @returns {number} distance in km
     */
    computeDistance(lat1, lon1, lat2, lon2) {
      const { status, output } = callMethod("compute_distance", `${lat1},${lon1},${lat2},${lon2}`);
      if (status !== 0) throw new Error(output);
      return parseFloat(output);
    },

    /**
     * Compute bearing from point 1 to point 2.
     * @param {number} lat1 @param {number} lon1
     * @param {number} lat2 @param {number} lon2
     * @returns {number} bearing in degrees (0-360)
     */
    computeBearing(lat1, lon1, lat2, lon2) {
      const { status, output } = callMethod("compute_bearing", `${lat1},${lon1},${lat2},${lon2}`);
      if (status !== 0) throw new Error(output);
      return parseFloat(output);
    },

    /**
     * Test if a point is inside a polygon.
     * @param {string} wkt - WKT polygon
     * @param {number} lat @param {number} lon
     * @returns {boolean}
     */
    pointInPolygon(wkt, lat, lon) {
      const { status, output } = callMethod("point_in_polygon", `${wkt}\n${lat},${lon}`);
      if (status !== 0) throw new Error(output);
      return output === "1";
    },

    /**
     * Compute polygon intersection.
     * @param {string} wkt1 @param {string} wkt2
     * @returns {string} WKT of intersection
     */
    polygonIntersection(wkt1, wkt2) {
      const { status, output } = callMethod("polygon_intersection", `${wkt1}\n${wkt2}`);
      if (status !== 0) throw new Error(output);
      return output;
    },

    /**
     * Compute polygon union.
     * @param {string} wkt1 @param {string} wkt2
     * @returns {string} WKT of union
     */
    polygonUnion(wkt1, wkt2) {
      const { status, output } = callMethod("polygon_union", `${wkt1}\n${wkt2}`);
      if (status !== 0) throw new Error(output);
      return output;
    },

    /**
     * Compute Voronoi diagram.
     * @param {string} multipointWkt @param {string} boundsWkt
     * @returns {string} MULTIPOLYGON WKT
     */
    computeVoronoi(multipointWkt, boundsWkt) {
      const { status, output } = callMethod("compute_voronoi", `${multipointWkt}\n${boundsWkt}`);
      if (status !== 0) throw new Error(output);
      return output;
    },

    /**
     * Compute Delaunay triangulation.
     * @param {string} multipointWkt
     * @returns {string} MULTIPOLYGON WKT
     */
    computeDelaunay(multipointWkt) {
      const { status, output } = callMethod("compute_delaunay", multipointWkt);
      if (status !== 0) throw new Error(output);
      return output;
    },

    /**
     * Transform WGS84 to ECEF.
     * @param {number} lat @param {number} lon @param {number} alt
     * @returns {{ x: number, y: number, z: number }}
     */
    toECEF(lat, lon, alt) {
      const { status, output } = callMethod("transform_to_ecef", `${lat},${lon},${alt}`);
      if (status !== 0) throw new Error(output);
      const [x, y, z] = output.split(",").map(Number);
      return { x, y, z };
    },

    /**
     * Transform ECEF to WGS84.
     * @param {number} x @param {number} y @param {number} z
     * @returns {{ lat: number, lon: number, alt: number }}
     */
    fromECEF(x, y, z) {
      const { status, output } = callMethod("transform_from_ecef", `${x},${y},${z}`);
      if (status !== 0) throw new Error(output);
      const [lat, lon, alt] = output.split(",").map(Number);
      return { lat, lon, alt };
    },

    /** Low-level method call */
    callMethod,
  };
}
