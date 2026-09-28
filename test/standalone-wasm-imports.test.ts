import { spawnSync } from 'node:child_process';
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

/**
 * The partition store artifact (wasm32-wasip1-threads; design §18 T4 #1):
 * WASI preview1, wasi.thread-spawn, a shared env.memory of at most 32768
 * pages and the seven env.flatsql_io_*; wasi_thread_start exported; no
 * emscripten import; and it passes space-data-module-sdk's
 * assertPthreadArtifact (shared memory, real atomics, the wasi-threads
 * contract, no emscripten thread hooks). scripts/check-wasm-imports.mjs
 * freezes the exact WASI list.
 */
describe('flatsql-ps-threads.wasm surface', () => {
  const PS_ARTIFACT = fileURLToPath(new URL('../wasm/flatsql-ps-threads.wasm', import.meta.url));

  test('imports exactly WASI preview1, wasi.thread-spawn, the shared env.memory and the seven flatsql_io_*', async () => {
    const bytes = await readFile(PS_ARTIFACT);
    const wasmModule = await WebAssembly.compile(bytes);
    const imports = WebAssembly.Module.imports(wasmModule);

    expect([...new Set(imports.map((i) => i.module))].sort()).toEqual(['env', 'wasi', 'wasi_snapshot_preview1']);
    expect(imports.filter((i) => i.module === 'wasi').map((i) => `${i.kind}:${i.name}`)).toEqual(['function:thread-spawn']);
    const env = imports.filter((i) => i.module === 'env');
    expect(env.filter((i) => i.kind === 'memory').map((i) => i.name)).toEqual(['memory']);
    expect(env.filter((i) => i.kind === 'function').map((i) => i.name).sort()).toEqual([...SUPPORTED_HOST_IO_IMPORTS].sort());
    expect(env.length).toBe(8);
    expect(imports.filter((i) => /^(__syscall_|_emscripten|emscripten_|__pthread_create_js)/.test(i.name))).toEqual([]);

    const exports = WebAssembly.Module.exports(wasmModule).map((e) => e.name);
    expect(exports).toContain('wasi_thread_start');
    for (const name of ['flatsql_ps_init', 'flatsql_ps_start', 'flatsql_ps_stop', 'flatsql_ps_alloc', 'flatsql_ps_free']) {
      expect(exports).toContain(name);
    }

    // The SDK guard runs in its own process: its module graph is plain ESM
    // that this Jest environment does not load.
    const guard = spawnSync(
      process.execPath,
      [
        '--input-type=module',
        '-e',
        `import { readFileSync } from 'node:fs';
         import { analyzeWasmThreadFeatures, assertPthreadArtifact } from 'space-data-module-sdk/compiler';
         const bytes = readFileSync(process.argv[1]);
         assertPthreadArtifact(bytes);
         process.stdout.write(JSON.stringify(analyzeWasmThreadFeatures(bytes)));`,
        PS_ARTIFACT,
      ],
      { encoding: 'utf8' },
    );
    expect(guard.stderr).toBe('');
    expect(guard.status).toBe(0);
    const features = JSON.parse(guard.stdout);
    expect(features.sharedMemory).toMatchObject({ source: 'import', module: 'env', name: 'memory', shared: true });
    expect(features.sharedMemory.max).toBeLessThanOrEqual(32768);
    expect(features.isIsomorphicPthreads).toBe(true);
    expect(features.emscriptenThreadHooks).toEqual([]);
  });
});
