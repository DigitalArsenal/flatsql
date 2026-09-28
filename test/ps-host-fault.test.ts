// Host-level crash trials under the Node host's fault overlay (design §19,
// 22.3a-7; docs/PARTITION-STORE-WASM.md).
//
// The guest ingests into real files through the flatsql_io imports
// (host_crash_ingest) and prints each durable ack; the host kills it at a
// random point, freezes the overlay, and applies a crash: every unsynced page
// undone (dropAll), a random subset of them (pageSubset), or none (keepAll).
// host_crash_verify then reopens the store and checks every acked record,
// gap-free pseqs and counters. A negative control truncates acked data and
// must fail the verifier. Needs the wasm test commands
// (npm run build:wasm:ps-tests).
import { closeSync, existsSync, mkdtempSync, openSync, readdirSync, readFileSync, rmSync, statSync, truncateSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { runPsCommand } from '../wasm/ps-node-host.mjs';
import { createFaultOverlay } from '../wasm/ps-node-fault.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const TEST_WASM = path.join(ROOT, 'cpp/build-ps-wasm/flatsql-ps-test.wasm');
const TRIALS = Number(process.env.FLATSQL_PS_HOST_CRASH_TRIALS ?? 9);

const run = existsSync(TEST_WASM) ? test : test.skip;

describe('host-level crashes under the fault overlay', () => {
  run(`${TRIALS} kill + crash + verify cycles keep every acked record`, async () => {
    const root = mkdtempSync(path.join(os.tmpdir(), 'flatsql-ps-hostcrash-'));
    const overlay = createFaultOverlay({ maxPages: 1 << 17 });
    let seed = 20260928;
    const random = () => (seed = (Math.imul(seed, 1103515245) + 12345) >>> 0) / 2 ** 32;
    const modes = ['dropAll', 'pageSubset', 'keepAll'] as const;
    const acks = path.join(root, 'acks.txt');
    const undone = { dropAll: 0, pageSubset: 0, keepAll: 0 };
    try {
      for (let t = 0; t < TRIALS; t++) {
        const fd = openSync(acks, 'a');
        const ingest = await runPsCommand(TEST_WASM, {
          root,
          args: ['--test=host_crash_ingest', '--dir=/store', `--round=${t + 1}`, `--journal=${t % 2}`],
          fault: overlay,
          stdoutFd: fd,
          killAfterMs: 400 + Math.floor(random() * 1600),
          beforeKill: () => overlay.freeze(),
        });
        closeSync(fd);
        expect(ingest.fault).toBe(false);
        expect(ingest.killed).toBe(true);
        expect(overlay.stats().overflow).toBe(false);
        const mode = modes[t % 3];
        undone[mode] += overlay.crash(mode, random).pagesUndone;
        const verify = await runPsCommand(TEST_WASM, {
          root,
          args: ['--test=host_crash_verify', '--dir=/store', '--acks=/acks.txt', `--journal=${t % 2}`],
        });
        expect({ trial: t, mode, exit: verify.exitCode, error: verify.error }).toEqual({ trial: t, mode, exit: 0, error: null });
      }
      const acked = readFileSync(acks, 'utf8').split('\n').filter((l) => l.startsWith('ACK ')).length;
      expect(acked).toBeGreaterThan(1000);
      // The overlay really undid unsynced pages in the dropping modes.
      expect(undone.dropAll + undone.pageSubset).toBeGreaterThan(0);

      // Negative control: cut acked frames off a data segment.
      const part = path.join(root, 'store/fsql2/p/00000001');
      const segment = readdirSync(part)
        .filter((f) => f.startsWith('d-'))
        .map((f) => path.join(part, f))
        .sort((a, b) => statSync(b).size - statSync(a).size)[0];
      truncateSync(segment, Math.floor(statSync(segment).size / 2));
      const broken = await runPsCommand(TEST_WASM, {
        root,
        args: ['--test=host_crash_verify', '--dir=/store', '--acks=/acks.txt'],
      });
      expect(broken.exitCode).not.toBe(0);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 900000);
});
