// The published entrypoints of flatsql 3.x (docs/PARTITION-STORE-WASM.md §6).
//
// Resolved the way a consumer resolves them: a Node process whose nearest
// package.json is this one, importing "flatsql/..." (package self-reference).
// Jest's own resolver would find the 2.x copy space-data-module-sdk installs.
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function exportsOf(specifiers: string[]): Record<string, string[]> {
  const code = `
    const out = {};
    for (const s of ${JSON.stringify(specifiers)}) {
      try { out[s] = Object.keys(await import(s)).sort(); } catch (e) { out[s] = ['ERROR ' + e.code]; }
    }
    process.stdout.write(JSON.stringify(out));`;
  const r = spawnSync(process.execPath, ['--input-type=module', '-e', code], { cwd: ROOT, encoding: 'utf8' });
  expect(r.stderr).toBe('');
  return JSON.parse(r.stdout);
}

describe('published package exports', () => {
  test('the root is the wasm bindings, and the removed surfaces are gone', () => {
    const e = exportsOf([
      'flatsql',
      'flatsql/wasm',
      'flatsql/ps',
      'flatsql/ps/node-host',
      'flatsql/ps/fault',
      'flatsql/artifacts',
      'flatsql/response',
      'flatsql/standalone/wasmedge',
      'flatsql/btree',
      'flatsql/storage',
      'flatsql/schema',
      'flatsql/cluster',
      'flatsql/standalone',
      'flatsql/artifacts/standalone',
    ]);

    // Root = the wasm bindings: the SQLite-backed engine and the partition
    // store artifact's locator. No TypeScript engine.
    expect(e['flatsql']).toEqual(e['flatsql/wasm']);
    expect(e['flatsql']).toEqual(expect.arrayContaining([
      'default', 'initFlatSQL', 'FlatSQL', 'FlatSQLDatabase',
      'getFlatSQLPsThreadsURL', 'loadFlatSQLPsThreads', 'flatsqlPsThreadsSha256',
    ]));
    for (const gone of ['DirectAccessor', 'FlatcAccessor', 'TableStore', 'BTree', 'StackedFlatBufferStore',
      'parseSchema', 'createQueryResponseArtifact', 'FlatSQLArtifactBuilder']) {
      expect(e['flatsql']).not.toContain(gone);
    }

    expect(e['flatsql/ps']).toEqual(expect.arrayContaining(['getFlatSQLPsThreadsURL', 'loadFlatSQLPsThreads']));
    expect(e['flatsql/ps/node-host']).toEqual(expect.arrayContaining(['runPsCommand', 'createPsNodeInstance']));
    expect(e['flatsql/ps/fault']).toEqual(expect.arrayContaining(['createFaultOverlay']));
    expect(e['flatsql/artifacts']).toEqual(expect.arrayContaining(['decodeSizePrefixedStream']));
    expect(e['flatsql/artifacts']).not.toContain('FlatSQLArtifactBuilder');
    expect(e['flatsql/response']).toEqual(expect.arrayContaining(['createQueryResponseArtifact', 'MemoryResponseArtifactCache']));
    expect(e['flatsql/standalone/wasmedge']).toEqual(expect.arrayContaining(['buildFlatSQLWasmEdgeRunner', 'createFlatSQLWasmEdgeProcessRuntime']));
    expect(e['flatsql/btree']).toContain('BTree');
    expect(e['flatsql/storage']).toContain('StackedFlatBufferStore');
    expect(e['flatsql/schema']).toContain('parseSchema');
    expect(e['flatsql/cluster']).toContain('detectClusterEnvironment');

    // Removed in 3.0.0 (A1): wasm/standalone.js and the standalone artifact builder.
    expect(e['flatsql/standalone']).toEqual(['ERROR ERR_PACKAGE_PATH_NOT_EXPORTED']);
    expect(e['flatsql/artifacts/standalone']).toEqual(['ERROR ERR_PACKAGE_PATH_NOT_EXPORTED']);
  });
});
