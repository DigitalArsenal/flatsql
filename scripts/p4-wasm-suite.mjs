#!/usr/bin/env node
/**
 * Store format 4's test suite (cpp/tests/p4, and the SQL surface's engine
 * tests) as the wasm32-wasip1-threads command flatsql-p4-test.wasm under the
 * Node wasi-threads host (wasm/ps-node-host.mjs), and the kill -9 loop
 * (docs/STORE-FORMAT-4.md, design gate G2).
 *
 *   node scripts/p4-wasm-suite.mjs [--wasm cpp/build-ps-wasm/flatsql-p4-test.wasm]
 *        [--only <substring>] [--kill-rounds 1000] [--json out.json]
 *
 * Every fast test runs in its own host process. The kill loop then runs
 * t_kill_run in a guest that the host kills at a random point (its workers are
 * terminated: no further I/O, the page cache survives, as with kill -9), and
 * t_kill_check in a fresh guest on the same store. Prints the machine and its
 * load with the results; exits non-zero when anything fails.
 */
import { spawn } from 'node:child_process';
import { closeSync, mkdtempSync, openSync, readFileSync, rmSync, writeFileSync, mkdirSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const HOST = path.join(ROOT, 'wasm/ps-node-host.mjs');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : fallback;
}
const wasm = path.resolve(arg('wasm', path.join(ROOT, 'cpp/build-ps-wasm/flatsql-p4-test.wasm')));

// ---- one kill round, in this process (--round N --root DIR) --------------------------
if (arg('round', '')) {
  const { runPsCommand } = await import(HOST);
  const round = Number(arg('round', '1'));
  const root = arg('root', '');
  const killAfterMs = 300 + Math.floor(Math.random() * 1500);
  const run = await runPsCommand(wasm, {
    args: ['--test=t_kill_run', '--slow=1', '--store=/fsql4', `--base=${round}`],
    root,
    keepRoot: true,
    killAfterMs,
    timeoutMs: 120000,
  });
  if (!run.killed) {
    console.log(JSON.stringify({ round, ok: false, why: `run ended before the kill: exit ${run.exitCode} ${run.error ?? ''}` }));
    process.exit(1);
  }
  const out = path.join(root, 'check.log');
  const fd = openSync(out, 'w');
  const check = await runPsCommand(wasm, {
    args: ['--test=t_kill_check', '--slow=1', '--store=/fsql4'],
    root,
    keepRoot: true,
    timeoutMs: 300000,
    stdoutFd: fd,
  });
  closeSync(fd);
  const text = readFileSync(out, 'utf8');
  const line = text.split('\n').find((l) => l.includes('PASS') || l.includes('FAIL')) ?? text.slice(-300);
  console.log(JSON.stringify({ round, ok: check.exitCode === 0, killAfterMs, why: line.trim() }));
  process.exit(check.exitCode === 0 ? 0 : 1);
}

function runChild(args, logPath, env = {}) {
  return new Promise((resolve) => {
    const log = openSync(logPath, 'w');
    const child = spawn(process.execPath, args, { stdio: ['ignore', log, log], env: { ...process.env, ...env } });
    child.on('exit', (code, signal) => {
      closeSync(log);
      resolve({ code, signal });
    });
  });
}

const only = arg('only', '');
const killRounds = Number(arg('kill-rounds', '0'));
const jsonOut = arg('json', '');
const machine = `${os.type()} ${os.release()} ${os.arch()}, ${os.cpus().length} hardware threads, node ${process.version}`;
console.log(`# ${machine}; load ${os.loadavg().map((l) => l.toFixed(1)).join(' ')}`);
const logDir = mkdtempSync(path.join(os.tmpdir(), 'flatsql-p4-suite-'));
const results = [];
let failed = 0;

// The fast tests.
const listLog = path.join(logDir, 'list.log');
await runChild([HOST, wasm, '--list'], listLog);
const tests = readFileSync(listLog, 'utf8')
  .split('\n')
  .map((l) => l.trim().split(' '))
  .filter(([name, kind]) => kind === 'fast' && name.includes(only))
  .map(([name]) => name);
for (const name of tests) {
  const base = path.join(logDir, name);
  const started = Date.now();
  const r = await runChild([HOST, wasm, `--test=${name}`, '--dir=/p4test'], `${base}.log`, {
    FLATSQL_PS_REPORT: `${base}.json`,
    FLATSQL_PS_TIMEOUT_MS: '1800000',
  });
  let rep = { exitCode: r.code ?? 128 };
  try {
    rep = JSON.parse(readFileSync(`${base}.json`, 'utf8'));
  } catch {}
  const ok = rep.exitCode === 0;
  if (!ok) failed++;
  const output = readFileSync(`${base}.log`, 'utf8');
  const row = { test: name, ok, exitCode: rep.exitCode, seconds: +((Date.now() - started) / 1000).toFixed(1),
    load1: +os.loadavg()[0].toFixed(1), measured: output.split('\n').filter((l) => l.includes('MEASURE')).map((l) => l.trim()) };
  results.push(row);
  console.log(`${ok ? 'PASS' : 'FAIL'} ${name}: ${row.seconds} s`);
  if (!ok) console.log(output.split('\n').slice(-30).map((l) => `    ${l}`).join('\n'));
}

// The kill loop.
if (killRounds > 0) {
  const store = mkdtempSync(path.join(os.tmpdir(), 'flatsql-p4-kill-'));
  let pass = 0;
  for (let round = 1; round <= killRounds; round++) {
    const log = path.join(logDir, `kill-${round}.log`);
    const r = await runChild([fileURLToPath(import.meta.url), '--wasm', wasm, '--round', String(round), '--root', store], log);
    const text = readFileSync(log, 'utf8').trim().split('\n').pop() ?? '';
    if (r.code === 0) pass++;
    else {
      failed++;
      console.log(`  kill round ${round} FAIL: ${text}`);
    }
    if (round % 50 === 0) console.log(`  kill: ${round} rounds, ${pass} pass; load ${os.loadavg()[0].toFixed(1)}`);
  }
  results.push({ test: `t_kill x${killRounds}`, ok: pass === killRounds, pass, rounds: killRounds });
  console.log(`${pass === killRounds ? 'PASS' : 'FAIL'} t_kill: ${pass} of ${killRounds} rounds`);
  rmSync(store, { recursive: true, force: true });
}

rmSync(logDir, { recursive: true, force: true });
const summary = { machine, loadAfter: os.loadavg(), wasm, failed, results };
if (jsonOut) writeFileSync(jsonOut, `${JSON.stringify(summary, null, 2)}\n`);
console.log(`# ${results.length} runs, ${failed} failed; load ${os.loadavg().map((l) => l.toFixed(1)).join(' ')}`);
process.exit(failed ? 1 : 0);
