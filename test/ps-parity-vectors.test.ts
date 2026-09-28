// Parity vectors (T4 #3, docs/PARTITION-STORE-WASM.md).
//
//   logical: the canonical dump (rows sorted by (pid, pseq), counters) and
//            query results of a threaded 4-writer workload are identical
//            natively and under the Node wasi-threads host;
//   bytes:   deterministic mode (one thread, the injected clock, explicit
//            commit and seal points) writes byte-identical files natively,
//            under the Node host (in memory and on the host's files) and,
//            with FLATSQL_PS_WASMEDGE=1, under the SDK WasmEdge C runner
//            (patched WasmEdge 0.16.4, built by space-data-module-sdk).
//
// Needs the native test binary (cpp/build/flatsql_ps_test) and the wasm test
// commands (npm run build:wasm:ps-tests).
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { runPsCommand } from '../wasm/ps-node-host.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const NATIVE = process.env.FLATSQL_PS_NATIVE_TEST ?? path.join(ROOT, 'cpp/build/flatsql_ps_test');
const TEST_WASM = path.join(ROOT, 'cpp/build-ps-wasm/flatsql-ps-test.wasm');
const MEMIO_WASM = path.join(ROOT, 'cpp/build-ps-wasm/flatsql-ps-test-memio.wasm');
const mounts = [{ guest: ROOT, host: ROOT }];

const ready = existsSync(NATIVE) && existsSync(TEST_WASM);
const run = ready ? test : test.skip;
const runWasmEdge = ready && existsSync(MEMIO_WASM) && process.env.FLATSQL_PS_WASMEDGE === '1' ? test : test.skip;

function parityLine(output: string, name: string): string {
  const line = output.split('\n').find((l) => l.startsWith(`PARITY ${name} `));
  if (!line) throw new Error(`no PARITY ${name} line in:\n${output.slice(-4000)}`);
  return line;
}

function native(args: string[]): string {
  const r = spawnSync(NATIVE, args, { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  expect(r.status).toBe(0);
  return r.stdout;
}

async function node(args: string[], root: string): Promise<void> {
  const r = await runPsCommand(TEST_WASM, { root, args, mounts, timeoutMs: 600000 });
  expect({ exit: r.exitCode, error: r.error }).toEqual({ exit: 0, error: null });
}

describe('partition store parity vectors', () => {
  run('canonical dumps and query results are identical natively and under the Node host', async () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), 'flatsql-ps-parity-'));
    try {
      native(['--test=parity_canonical_dump', `--out=${path.join(dir, 'native.txt')}`]);
      await node(['--test=parity_canonical_dump', '--out=/node.txt'], dir);
      const a = readFileSync(path.join(dir, 'native.txt'), 'utf8');
      const b = readFileSync(path.join(dir, 'node.txt'), 'utf8');
      expect(a.split('\n').length).toBeGreaterThan(10000);
      expect(b).toBe(a);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 600000);

  run('deterministic mode writes byte-identical files natively and under the Node host', async () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), 'flatsql-ps-parity-'));
    try {
      // In memory: the same store root string on both sides.
      const nMem = parityLine(native(['--test=parity_deterministic_bytes']), 'deterministic_bytes');
      const out = path.join(dir, 'mem.txt');
      await node(['--test=parity_deterministic_bytes', '--out=/mem.txt'], dir);
      const wMem = readFileSync(out, 'utf8');
      expect(nMem).toMatch(/lines=\d{3}/);
      // On the host's files: one path, native and as the guest sees it.
      const store = path.join(dir, 'files');
      const nFiles = parityLine(native(['--test=parity_deterministic_bytes', `--dir=${store}`]), 'deterministic_bytes');
      const guestRoot = path.join(dir, 'guest');
      await node(['--test=parity_deterministic_bytes', `--dir=${store}`, '--out=/files.txt'], guestRoot);
      const wFiles = readFileSync(path.join(guestRoot, 'files.txt'), 'utf8');
      const sha = (text: string) => createHash('sha256').update(text).digest('hex');
      expect(nMem).toContain(`sha256=${sha(wMem)}`);
      expect(nFiles).toContain(`sha256=${sha(wFiles)}`);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 600000);

  runWasmEdge('deterministic mode writes the same bytes under the SDK WasmEdge C runner', async () => {
    // The SDK's isomorphic loader runs a threaded command on its wasi-threads
    // C runner (it builds and caches the patched runtime on first use).
    const sdkLoader = 'space-data-module-sdk/host/isomorphic';
    const { loadModule } = await import(sdkLoader);
    const harness = await loadModule({
      wasmSource: MEMIO_WASM,
      args: ['--test=parity_deterministic_bytes'],
      runtimeKind: 'wasmedge',
    });
    try {
      const stdout = Buffer.from(await harness.invokeRaw(new Uint8Array())).toString('utf8');
      const nMem = parityLine(native(['--test=parity_deterministic_bytes']), 'deterministic_bytes');
      expect(parityLine(stdout, 'deterministic_bytes')).toBe(nMem);
    } finally {
      await harness.destroy();
    }
  }, 3600000);
});
