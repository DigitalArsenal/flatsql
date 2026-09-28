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
      // Every file (path, size, sha256) must match; on a mismatch the
      // differing lines are the evidence.
      const same = (a: string, b: string) => {
        if (a === b) return;
        const al = a.split('\n');
        const bl = new Set(b.split('\n'));
        console.log(`native lines not under Node:\n${al.filter((l) => !bl.has(l)).slice(0, 40).join('\n')}`);
        expect(b).toBe(a);
      };
      // In memory: the same store root string on both sides.
      const nMemOut = path.join(dir, 'native-mem.txt');
      const nMem = parityLine(native(['--test=parity_deterministic_bytes', `--out=${nMemOut}`]), 'deterministic_bytes');
      await node(['--test=parity_deterministic_bytes', '--out=/mem.txt'], dir);
      expect(nMem).toMatch(/lines=\d{3}/);
      same(readFileSync(nMemOut, 'utf8'), readFileSync(path.join(dir, 'mem.txt'), 'utf8'));
      // On the host's files: one path, native and as the guest sees it.
      const store = path.join(dir, 'files');
      const nFilesOut = path.join(dir, 'native-files.txt');
      native(['--test=parity_deterministic_bytes', `--dir=${store}`, `--out=${nFilesOut}`]);
      const guestRoot = path.join(dir, 'guest');
      await node(['--test=parity_deterministic_bytes', `--dir=${store}`, '--out=/files.txt'], guestRoot);
      same(readFileSync(nFilesOut, 'utf8'), readFileSync(path.join(guestRoot, 'files.txt'), 'utf8'));
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 600000);

  runWasmEdge('deterministic mode writes the same bytes under the SDK WasmEdge C runner', () => {
    // The SDK's isomorphic loader runs a threaded command on its wasi-threads
    // C runner (it builds and caches the patched runtime on first use). It
    // runs in its own process: its module graph is plain ESM that this Jest
    // environment does not load.
    const code = `
      import { loadModule } from 'space-data-module-sdk/host/isomorphic';
      const harness = await loadModule({ wasmSource: process.argv[1], args: ['--test=parity_deterministic_bytes'], runtimeKind: 'wasmedge' });
      try {
        process.stdout.write(Buffer.from(await harness.invokeRaw(new Uint8Array())));
      } finally {
        await harness.destroy();
      }`;
    const r = spawnSync(process.execPath, ['--input-type=module', '-e', code, MEMIO_WASM], {
      cwd: ROOT,
      encoding: 'utf8',
      maxBuffer: 64 * 1024 * 1024,
      timeout: 3600000,
    });
    if (r.status !== 0) console.log(r.stderr);
    expect(r.status).toBe(0);
    const nMem = parityLine(native(['--test=parity_deterministic_bytes']), 'deterministic_bytes');
    expect(parityLine(r.stdout, 'deterministic_bytes')).toBe(nMem);
  }, 3600000);
});
