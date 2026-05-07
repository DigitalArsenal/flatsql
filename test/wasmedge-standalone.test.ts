import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { getFlatSQLWASIURL } from '../wasm/wasi.js';

function buildWasmEdgeEnv(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  if (!env.DYLD_LIBRARY_PATH && env.WASMEDGE_LIB_DIR) {
    env.DYLD_LIBRARY_PATH = env.WASMEDGE_LIB_DIR;
  }
  return env;
}

function hasWasmEdge(): boolean {
  const result = spawnSync('wasmedge', ['--version'], {
    encoding: 'utf8',
    env: buildWasmEdgeEnv(),
  });
  return result.status === 0;
}

describe('WasmEdge standalone artifact', () => {
  const maybeTest = hasWasmEdge() ? test : test.skip;

  maybeTest('instantiates the packaged FlatSQL WASI reactor', () => {
    const wasmPath = fileURLToPath(getFlatSQLWASIURL());
    const result = spawnSync(
      'wasmedge',
      ['--enable-exception-handling', '--reactor', wasmPath, '_initialize'],
      {
        encoding: 'utf8',
        env: buildWasmEdgeEnv(),
        maxBuffer: 4 * 1024 * 1024,
      }
    );

    expect({
      status: result.status,
      signal: result.signal,
      error: result.error ? String(result.error) : null,
      stderr: result.stderr,
      stdout: result.stdout,
    }).toEqual({
      status: 0,
      signal: null,
      error: null,
      stderr: '',
      stdout: '',
    });
  });
});
