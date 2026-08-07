import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

import { getFlatSQLWASIURL } from '../wasm/wasi.js';

const REQUIRED_EXPORTS = [
  'memory',
  '_initialize',
  'malloc',
  'free',
  'flatsql_create_db',
  'flatsql_destroy_db',
  'flatsql_register_file_id',
  'flatsql_ingest',
  'flatsql_build_response_artifact_cache_key',
  'flatsql_register_query_template',
  'flatsql_query_template',
  'flatsql_query_cache_hits',
  'flatsql_query_cache_misses',
  'flatsql_query_cache_generation',
];

const SUPPORTED_WASI_IMPORTS = new Set([
  'clock_time_get',
  'fd_write',
  'fd_read',
  'environ_sizes_get',
  'environ_get',
  'random_get',
]);

/**
 * The host I/O boundary, and the WHOLE of it.
 *
 * The standalone artifact is NOT WASI-only, and must not be: the disk-backed
 * engine (owner law 2026-08-06 — btree + flatbuffers live ON DISK, an
 * in-memory engine is drift) reaches its store through the seven-call contract
 * declared in `cpp/include/flatsql/flatsql_io.h`. That boundary is satisfied
 * call-for-call by every host — the emscripten browser bundle via
 * `cpp/js/flatsql_io_library.js`, the standalone/WASI shim via
 * `wasm/flatsql-io.js` (`createFlatSqlIoImports`), the Go host, and
 * `cpp/src/flatsql_io_native.cpp` — so the same artifact runs everywhere with
 * ZERO runtime detection inside the module.
 *
 * This list is therefore a CLOSED SET, not an allowance. Asserting it exactly
 * is strictly stronger than the `expect(nonWasiImports).toEqual([])` it
 * replaces: that assertion could only ever be satisfied by deleting the disk
 * boundary, while this one fails the build the moment an EIGHTH host import
 * appears — which is the real risk, because an undeclared host import is a
 * function some runtime's shim will not provide.
 *
 * Adding an entry here means adding it to the header AND to all four hosts.
 */
const SUPPORTED_HOST_IO_IMPORTS = new Set([
  'flatsql_io_open',
  'flatsql_io_read',
  'flatsql_io_write',
  'flatsql_io_truncate',
  'flatsql_io_sync',
  'flatsql_io_size',
  'flatsql_io_close',
]);

describe('standalone WASI artifact surface', () => {
  test('imports only WASI plus the closed host I/O contract, and exports the FlatSQL C ABI', async () => {
    const bytes = await readFile(fileURLToPath(getFlatSQLWASIURL()));
    const wasmModule = await WebAssembly.compile(bytes);

    const imports = WebAssembly.Module.imports(wasmModule);

    const unknownModules = [
      ...new Set(
        imports
          .map((entry) => entry.module)
          .filter((module) => module !== 'wasi_snapshot_preview1' && module !== 'env'),
      ),
    ].sort();
    expect(unknownModules).toEqual([]);

    const wasiImports = imports.filter((entry) => entry.module === 'wasi_snapshot_preview1');
    expect(wasiImports.map((entry) => entry.name).sort()).toEqual(
      [...SUPPORTED_WASI_IMPORTS].sort(),
    );

    const hostImports = imports.filter((entry) => entry.module === 'env');
    expect(hostImports.every((entry) => entry.kind === 'function')).toBe(true);
    expect(hostImports.map((entry) => entry.name).sort()).toEqual(
      [...SUPPORTED_HOST_IO_IMPORTS].sort(),
    );

    const exports = new Set(WebAssembly.Module.exports(wasmModule).map((entry) => entry.name));
    for (const exportName of REQUIRED_EXPORTS) {
      expect(exports.has(exportName)).toBe(true);
    }
  });
});
