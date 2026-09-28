// The partition store under the Node wasi-threads host (T4 #2,
// docs/PARTITION-STORE-WASM.md).
//
// Always: the shipped flatsql-ps-threads.wasm runs a writer instance and a
// reader instance with every engine thread on its own OS thread.
// With FLATSQL_PS_WASM_SUITE=1 and the wasm test commands built
// (npm run build:wasm:ps-tests): the native T1/T2 suites, including 1,000
// fault crashes, run under the host (scripts/ps-wasm-suite.mjs).
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readdirSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { createPsNodeInstance } from '../wasm/ps-node-host.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const ARTIFACT = path.join(ROOT, 'wasm/flatsql-ps-threads.wasm');
const TEST_WASM = path.join(ROOT, 'cpp/build-ps-wasm/flatsql-ps-test.wasm');

function tlv(entries: Array<[number, Uint8Array]>): Uint8Array {
  const parts: Buffer[] = [];
  for (const [tag, bytes] of entries) {
    const head = Buffer.alloc(6);
    head.writeUInt16LE(tag, 0);
    head.writeUInt32LE(bytes.length, 2);
    parts.push(head, Buffer.from(bytes));
  }
  return new Uint8Array(Buffer.concat(parts));
}
const u32 = (v: number) => {
  const b = Buffer.alloc(4);
  b.writeUInt32LE(v);
  return new Uint8Array(b);
};
const text = (s: string) => new Uint8Array(Buffer.from(s));

async function waitFor(check: () => boolean, ms: number) {
  const until = Date.now() + ms;
  while (!check() && Date.now() < until) await new Promise((r) => setTimeout(r, 20));
}

describe('flatsql-ps-threads.wasm under the Node host', () => {
  test('a writer and a reader instance run every engine thread on its own OS thread', async () => {
    const root = mkdtempSync(path.join(os.tmpdir(), 'flatsql-ps-threads-'));
    const writers = 4;
    const lanes = 4;
    const writer = await createPsNodeInstance(ARTIFACT, { root });
    const reader = await createPsNodeInstance(ARTIFACT, { root });
    try {
      const wcfg = tlv([[1, text('/store')], [2, u32(writers)], [3, u32(2)]]);
      expect(await writer.call('flatsql_ps_init', 1, await writer.write(wcfg), wcfg.length)).toBe(0);
      expect(await writer.call('flatsql_ps_start')).toBe(0);
      // Writers plus the sync pool, the merge helper and the other service threads.
      await waitFor(() => writer.threads().runningGuestThreads >= writers + 2, 10000);
      const w = writer.threads();
      expect(w.runningGuestThreads).toBeGreaterThanOrEqual(writers + 2);

      const rcfg = tlv([[1, text('/store')], [20, u32(lanes)]]);
      expect(await reader.call('flatsql_ps_init', 2, await reader.write(rcfg), rcfg.length)).toBe(0);
      expect(await reader.call('flatsql_ps_start')).toBe(0);
      await waitFor(() => reader.threads().runningGuestThreads >= lanes, 10000);
      const r = reader.threads();
      expect(r.runningGuestThreads).toBe(lanes);

      // One Node worker (an OS thread) per running guest thread, per instance.
      expect(w.distinctWorkerThreadIds).toBeGreaterThanOrEqual(w.maxConcurrentGuestThreads);
      expect(r.distinctWorkerThreadIds).toBeGreaterThanOrEqual(r.maxConcurrentGuestThreads);
      expect(w.distinctWorkerThreadIds + r.distinctWorkerThreadIds).toBeGreaterThanOrEqual(writers + lanes);
      expect(writer.faulted() || reader.faulted()).toBe(false);

      expect(await reader.call('flatsql_ps_stop', 5000)).toBe(0);
      expect(await writer.call('flatsql_ps_stop', 5000)).toBe(0);
      expect(readdirSync(path.join(root, 'store/fsql2')).sort()).toEqual(
        ['MIGRATED', 'STORE', 'registry.fsh', 'registry.fsl'],
      );
    } finally {
      await reader.close();
      await writer.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 60000);

  const suite = process.env.FLATSQL_PS_WASM_SUITE === '1' && existsSync(TEST_WASM) ? test : test.skip;
  suite('the native T1 and T2 suites and 1,000 fault crashes pass under the host', () => {
    const result = spawnSync(process.execPath, [path.join(ROOT, 'scripts/ps-wasm-suite.mjs'), '--crash-trials', '1000'], {
      encoding: 'utf8',
      maxBuffer: 64 * 1024 * 1024,
    });
    if (result.status !== 0) console.log(result.stdout, result.stderr);
    expect(result.status).toBe(0);
    expect(result.stdout).toMatch(/PASS crash_faults_T1_1 x1000/);
  }, 3600000);
});
