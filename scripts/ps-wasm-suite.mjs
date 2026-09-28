#!/usr/bin/env node
/**
 * The partition store's native test suites (T1, T2 and whatever registers in
 * flatsql_ps_test) under the Node wasi-threads host (T4 #2,
 * docs/PARTITION-STORE-WASM.md).
 *
 *   node scripts/ps-wasm-suite.mjs [--wasm cpp/build-ps-wasm/flatsql-ps-test.wasm]
 *        [--crash-trials 1000] [--only <substring>] [--jobs 1] [--json out.json]
 *
 * Every default-suite test runs in its own guest process, then the crash
 * harness runs with --crash-trials. Tests whose in-memory fixture cannot fit a
 * wasm32 address space (4 GiB) run with the arguments in WASM_ARGS (reported
 * with the results). Exits non-zero when any test fails. Prints the machine
 * and its load average with the results.
 */
import { execFileSync, spawn } from 'node:child_process';
import { closeSync, mkdtempSync, openSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';


const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

// The flood test keeps every flooded byte twice in its in-memory host for as
// long as its timed measurement lasts: 150 samples need about 12 GB natively.
const WASM_ARGS = {
  writer_backpressure_flood_T1_5: ['--bp-samples=40'],
};

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : fallback;
}

const wasm = path.resolve(arg('wasm', path.join(ROOT, 'cpp/build-ps-wasm/flatsql-ps-test.wasm')));
const crashTrials = Number(arg('crash-trials', '1000'));
const only = arg('only', '');
const jobs = Math.max(1, Number(arg('jobs', '1')));
const jsonOut = arg('json', '');

const list = execFileSync(process.execPath, [path.join(ROOT, 'wasm/ps-node-host.mjs'), wasm, '--list'], {
  encoding: 'utf8',
  stdio: ['ignore', 'pipe', 'ignore'],
})
  .trim()
  .split('\n')
  .map((line) => line.split(' '))
  .filter(([name, kind]) => kind === 'fast' && name.includes(only));

const runs = list.map(([name]) => ({ name, args: [`--test=${name}`, ...(WASM_ARGS[name] ?? [])] }));
if (crashTrials > 0 && 'crash_faults_T1_1'.includes(only)) {
  runs.push({ name: `crash_faults_T1_1 x${crashTrials}`, args: ['--test=crash_faults_T1_1', `--crash-trials=${crashTrials}`] });
}

const machine = `${os.type()} ${os.release()} ${os.arch()}, ${os.cpus().length} hardware threads, node ${process.version}`;
console.log(`# ${machine}; load ${os.loadavg().map((l) => l.toFixed(1)).join(' ')}`);
const results = [];
let failed = 0;
const logDir = mkdtempSync(path.join(os.tmpdir(), 'flatsql-ps-suite-'));
// Each run is its own process (a guest reserves a 4 GiB shared memory; one
// process per guest keeps a long suite from accumulating them).
function runChild(r, logPath, reportPath) {
  return new Promise((resolve) => {
    const log = openSync(logPath, 'w');
    const child = spawn(process.execPath, [path.join(ROOT, 'wasm/ps-node-host.mjs'), wasm, ...r.args], {
      stdio: ['ignore', log, log],
      env: { ...process.env, FLATSQL_PS_REPORT: reportPath, FLATSQL_PS_TIMEOUT_MS: '1800000' },
    });
    child.on('exit', (code, signal) => {
      closeSync(log);
      resolve({ code, signal });
    });
  });
}
async function runOne(r) {
  const base = path.join(logDir, `${results.length}-${r.name.replace(/[^A-Za-z0-9_]/g, '_')}`);
  const started = Date.now();
  const child = await runChild(r, `${base}.log`, `${base}.json`);
  const output = readFileSync(`${base}.log`, 'utf8');
  let res;
  try {
    res = JSON.parse(readFileSync(`${base}.json`, 'utf8'));
  } catch {
    res = { exitCode: child.code ?? 128, error: `host process ended (${child.signal ?? child.code})`, threads: {} };
  }
  const ms = Date.now() - started;
  const ok = res.exitCode === 0;
  if (!ok) failed++;
  const t = res.threads;
  const row = {
    test: r.name,
    args: r.args.slice(1),
    ok,
    exitCode: res.exitCode,
    error: res.error,
    seconds: +(ms / 1000).toFixed(1),
    guestThreadsSpawned: t.guestThreadsSpawned,
    maxConcurrentGuestThreads: t.maxConcurrentGuestThreads,
    distinctWorkerThreadIds: t.distinctWorkerThreadIds,
    memoryMiB: Math.round((t.memoryBytes ?? 0) / 2 ** 20),
    load1: +os.loadavg()[0].toFixed(1),
    measured: output.split('\n').filter((l) => l.includes('MEASURED')).map((l) => l.trim()),
  };
  results.push(row);
  console.log(`${ok ? 'PASS' : 'FAIL'} ${r.name}${row.args.length ? ` ${row.args.join(' ')}` : ''}: ${row.seconds} s, ` +
    `${row.maxConcurrentGuestThreads} concurrent guest threads on ${row.distinctWorkerThreadIds} distinct worker threads, ` +
    `${row.memoryMiB} MiB${ok ? '' : ` (exit ${res.exitCode}${res.error ? `: ${res.error}` : ''})`}`);
  if (!ok) console.log(output.split('\n').slice(-40).map((l) => `    ${l}`).join('\n'));
}

const queue = [...runs];
await Promise.all(Array.from({ length: jobs }, async () => {
  while (queue.length) await runOne(queue.shift());
}));
rmSync(logDir, { recursive: true, force: true });
const summary = { machine, loadAfter: os.loadavg(), wasm, tests: results.length, failed, results };
if (jsonOut) writeFileSync(jsonOut, `${JSON.stringify(summary, null, 2)}\n`);
console.log(`# ${results.length} runs, ${failed} failed; load ${os.loadavg().map((l) => l.toFixed(1)).join(' ')}`);
process.exit(failed ? 1 : 0);
