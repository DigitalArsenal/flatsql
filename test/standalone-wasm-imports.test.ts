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

describe('standalone WASI artifact surface', () => {
  test('uses only WASI imports and exports the FlatSQL C ABI', async () => {
    const bytes = await readFile(fileURLToPath(getFlatSQLWASIURL()));
    const wasmModule = await WebAssembly.compile(bytes);

    const imports = WebAssembly.Module.imports(wasmModule);
    const nonWasiImports = imports.filter((entry) => entry.module !== 'wasi_snapshot_preview1');
    expect(nonWasiImports).toEqual([]);
    expect(imports.map((entry) => entry.name).sort()).toEqual([...SUPPORTED_WASI_IMPORTS].sort());

    const exports = new Set(WebAssembly.Module.exports(wasmModule).map((entry) => entry.name));
    for (const exportName of REQUIRED_EXPORTS) {
      expect(exports.has(exportName)).toBe(true);
    }
  });
});
