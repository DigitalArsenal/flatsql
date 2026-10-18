#!/usr/bin/env node
// E2E for flatsql-p4-replay-10x-20261018: the format-4 operations SDN nodes
// spend their FlatSQL time in, replayed on flatsql 3.7.2 and on this checkout,
// on the same data.
//
// Builds the native format-4 test client twice: this checkout, and v3.7.2's
// engine with this checkout's replay test (cpp/tests/p4/t_replay.cpp and
// wxf_fixture.h: the requests, not the engine). The fixture is built once, by
// 3.7.2: p4_record_scaling's 15 GB sea feed (P4_REPLAY_WXF_BYTES) and ten days
// of real Space-Track gp_history OMMs ingested as the node ingests them. Every
// measured run gets its own APFS clone of it (cp -c: no second copy of the
// data). For each repetition (P4_REPLAY_REPS, default 5) and workload the two
// builds run in alternating order, each in a fresh process on a fresh clone:
// the first pass is cold, the read workloads' later passes warm. It prints,
// per workload, each build's median and p95 wall and CPU time, the speedup,
// the bound (durable syncs x this disk's sync latency, or the bytes at memcpy
// rate) and stock SQLite 3.53.4 on the same index and frames, and fails when
// any answer hashes differently on the two builds (or across repetitions) or
// a workload is slower than 3.7.2 (median 5% above, runs apart).
//
// Environment: FLATBUFFERS_DIR (else the sibling flatbuffers checkout),
// P4_REPLAY_DIR (work directory; default the system temp directory),
// P4_REPLAY_FIXTURE (a fixture directory to build once and reuse),
// P4_REPLAY_GP (the gp_history by-creation root, default
// /opt/data/sdn-archive/spacetrack/gp_history/by-creation), P4_REPLAY_REPS,
// P4_REPLAY_WORKLOADS (comma list), P4_REPLAY_WXF_BYTES.
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream, existsSync, mkdirSync, readFileSync, rmSync, statfsSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createGunzip } from 'node:zlib';

const cpp = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repo = resolve(cpp, '..');
const BASE_TAG = 'v3.7.2';
const reps = Number(process.env.P4_REPLAY_REPS || 5);
const wxfBytes = Number(process.env.P4_REPLAY_WXF_BYTES || 15e9);
const gpRoot = process.env.P4_REPLAY_GP || '/opt/data/sdn-archive/spacetrack/gp_history/by-creation';
const SEED_DAYS = ['2026-09-16', '2026-09-17', '2026-09-18', '2026-09-19', '2026-09-20', '2026-09-21', '2026-09-22', '2026-09-23',
  '2026-09-24', '2026-09-25'];
const INGEST_DAY = '2026-09-26';
const WORKLOADS = (process.env.P4_REPLAY_WORKLOADS || 'gp_ingest,wxf_lead,sync_norad,sync_window,latest,retention,explorer,open')
  .split(',').filter(Boolean);
const READS = new Set(['sync_norad', 'sync_window', 'latest', 'explorer']);
const WARM_PASSES = 3;

function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

function run(cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, { stdio: 'inherit', ...opts });
  if (r.status !== 0) fail(`${cmd} ${args.join(' ')} exited ${r.status ?? r.signal}`);
}

function flatbuffers() {
  const dir = resolve(process.env.FLATBUFFERS_DIR || join(repo, '..', 'flatbuffers'));
  if (!existsSync(join(dir, 'include/flatbuffers/flatbuffers.h'))) fail(`FlatBuffers not found at ${dir} (set FLATBUFFERS_DIR)`);
  return dir;
}

const work = process.env.P4_REPLAY_DIR || join(tmpdir(), `flatsql-p4-replay-${createHash('sha256').update(repo).digest('hex').slice(0, 12)}`);
mkdirSync(work, { recursive: true });

function build(name, src) {
  const dir = join(work, `build-${name}`);
  run('cmake', ['-S', src, '-B', dir, '-DCMAKE_BUILD_TYPE=Release', `-DFLATBUFFERS_DIR=${flatbuffers()}`], {
    stdio: ['ignore', 'ignore', 'inherit'],
  });
  run('nice', ['cmake', '--build', dir, '--target', 'flatsql_p4_test', '-j', '8'], { stdio: ['ignore', 'ignore', 'inherit'] });
  return join(dir, 'flatsql_p4_test');
}

// 3.7.2's engine, this checkout's replay requests.
const baseSrc = join(work, 'base');
rmSync(baseSrc, { recursive: true, force: true });
mkdirSync(baseSrc, { recursive: true });
execFileSync('sh', ['-c', `git -C "${repo}" archive ${BASE_TAG} cpp | tar -x -C "${baseSrc}"`]);
for (const f of ['t_replay.cpp', 'wxf_fixture.h']) writeFileSync(join(baseSrc, 'cpp/tests/p4', f), readFileSync(join(cpp, 'tests/p4', f)));
const arms = { base: build('base', join(baseSrc, 'cpp')), new: build('new', cpp) };

// The gp_history days as the replay's TSV (NORAD, OBJECT_ID, OBJECT_NAME,
// EPOCH, CREATION_DATE, MEAN_MOTION, ECCENTRICITY, INCLINATION, TLE).
async function gpTsv(out, days) {
  if (existsSync(out)) return;
  const w = createWriteStream(`${out}.tmp`);
  const clean = (v) => String(v ?? '').replace(/[\t\n\r]/g, ' ');
  for (const day of days) {
    const chunks = [];
    for await (const c of createReadStream(join(gpRoot, day.slice(0, 4), `${day}.json.gz`)).pipe(createGunzip())) chunks.push(c);
    for (const r of JSON.parse(Buffer.concat(chunks).toString('utf8')))
      w.write([r.NORAD_CAT_ID, r.OBJECT_ID, r.OBJECT_NAME, r.EPOCH, r.CREATION_DATE, r.MEAN_MOTION, r.ECCENTRICITY, r.INCLINATION,
        `${r.TLE_LINE1} ${r.TLE_LINE2}`].map(clean).join('\t') + '\n');
  }
  await new Promise((res) => w.end(res));
  execFileSync('mv', [`${out}.tmp`, out]);
}
const seedTsv = join(work, 'gp-seed.tsv');
const dayTsv = join(work, 'gp-day.tsv');
await gpTsv(seedTsv, SEED_DAYS);
await gpTsv(dayTsv, [INGEST_DAY]);

function freeBytes(dir) {
  const s = statfsSync(dir);
  return Number(s.bavail) * Number(s.bsize);
}

// The fixture, once (3.7.2 builds it: the store a node on 3.7.2 holds).
const fixture = process.env.P4_REPLAY_FIXTURE || join(work, 'fixture');
const stamp = join(fixture, 'REPLAY-FIXTURE');
const want = `wxf=${wxfBytes} seed=${SEED_DAYS.join(',')}`;
if (!existsSync(stamp) || readFileSync(stamp, 'utf8') !== want) {
  if (freeBytes(work) < wxfBytes * 1.15 + 22 * 2 ** 30) fail(`${work}: not enough free disk for a ${(wxfBytes / 1e9).toFixed(1)} GB fixture`);
  rmSync(fixture, { recursive: true, force: true });
  mkdirSync(fixture, { recursive: true });
  run(arms.base, ['--test=p4_record_scaling', `--store=${fixture}`, `--bytes=${Math.round(wxfBytes)}`, '--build-only=1']);
  run(arms.base, ['--test=p4_replay', '--phase=build', `--store=${fixture}`, `--gp=${seedTsv}`]);
  writeFileSync(stamp, want);
}

// One process on a fresh clone of the fixture; its REPLAY lines.
let cloneN = 0;
function measure(arm, workload, extra = []) {
  const clone = join(work, `clone-${process.pid}-${cloneN++}`);
  run('cp', ['-c', '-R', fixture, clone]);
  try {
    const args = ['--test=p4_replay', '--phase=run', `--store=${clone}`, `--workload=${workload}`, `--gp=${seedTsv}`,
      `--gp-day=${dayTsv}`, ...extra];
    const r = spawnSync(arms[arm], args, { encoding: 'utf8', maxBuffer: 64 << 20 });
    const lines = (r.stdout || '').split('\n');
    const passes = lines.filter((l) => l.startsWith('REPLAY {')).map((l) => JSON.parse(l.slice(7)));
    const after = lines.filter((l) => l.startsWith('REPLAY_AFTER ')).map((l) => JSON.parse(l.slice(13)));
    if (r.status !== 0 || !passes.length) fail(`${arm} ${workload}: exit ${r.status ?? r.signal}\n${(r.stdout || '').slice(-2000)}${(r.stderr || '').slice(-2000)}`);
    return { passes, after: after[0]?.hash ?? '' };
  } finally {
    rmSync(clone, { recursive: true, force: true });
  }
}

function bounds() {
  const r = spawnSync(arms.new, ['--test=p4_replay', '--phase=bounds', `--store=${work}`], { encoding: 'utf8' });
  const l = (r.stdout || '').split('\n').find((x) => x.startsWith('REPLAY_BOUNDS '));
  if (!l) fail(`bounds: ${r.stdout}${r.stderr}`);
  return JSON.parse(l.slice(14));
}

const q = (v, p) => {
  const s = [...v].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(p * s.length))];
};
const med = (v) => q(v, 0.5);

const b0 = bounds();
console.log(`bounds: ${JSON.stringify(b0)}`);
const results = {}; // workload -> pass -> arm -> [{wall, cpu, syncs}]
const hashes = {}; // workload|pass -> Set
let failed = false;
for (let rep = 0; rep < reps; rep++) {
  for (const workload of WORKLOADS) {
    const order = rep % 2 ? ['new', 'base'] : ['base', 'new'];
    for (const arm of order) {
      const m = measure(arm, workload, READS.has(workload) ? [`--passes=${1 + WARM_PASSES}`] : []);
      for (const p of m.passes) {
        const key = `${workload}|${p.pass}`;
        ((results[workload] ??= {})[p.pass] ??= { base: [], new: [] })[arm].push(p);
        (hashes[key] ??= new Set()).add(p.hash);
      }
      if (m.after) (hashes[`${workload}|after`] ??= new Set()).add(m.after);
    }
  }
  console.log(`repetition ${rep + 1}/${reps} done (load ${b0.load})`);
}
const b1 = bounds();
console.log(`bounds after: ${JSON.stringify(b1)}`);
const syncMs = Math.min(b0.sync_ms_median, b1.sync_ms_median);
const memcpy = Math.max(b0.memcpy_GBps, b1.memcpy_GBps);

const rows = [];
for (const workload of WORKLOADS) {
  for (const [pass, byArm] of Object.entries(results[workload] || {})) {
    const key = `${workload}|${pass}`;
    if (hashes[key].size !== 1) {
      failed = true;
      console.error(`FAIL: ${key}: ${hashes[key].size} distinct answer hashes (${[...hashes[key]].join(', ')})`);
    }
    const a = byArm.base.map((p) => p.wall_ms), b = byArm.new.map((p) => p.wall_ms);
    const ac = byArm.base.map((p) => p.cpu_ms), bc = byArm.new.map((p) => p.cpu_ms);
    const syncs = med(byArm.new.map((p) => p.syncs));
    const bytes = med(byArm.new.map((p) => p.rows));
    const bound = syncs > 0 ? syncs * syncMs : 0;
    rows.push({ workload, pass, base_med: med(a), base_p95: q(a, 0.95), new_med: med(b), new_p95: q(b, 0.95), speedup: med(a) / med(b),
      base_cpu: med(ac), new_cpu: med(bc), syncs, bound_ms: bound, rows: bytes });
    // Slower: the median past 3.7.2's by 5% and the two builds' runs apart
    // (this build's lower quartile above 3.7.2's upper one), not one noisy
    // cold run.
    if (med(b) > med(a) * 1.05 && q(b, 0.25) > q(a, 0.75)) {
      failed = true;
      console.error(`FAIL: ${key}: this build ${med(b).toFixed(1)} ms is slower than ${BASE_TAG} ${med(a).toFixed(1)} ms`);
    }
  }
  if (hashes[`${workload}|after`] && hashes[`${workload}|after`].size !== 1) {
    failed = true;
    console.error(`FAIL: ${workload}: the store after the writes differs between builds`);
  }
}
const f = (x) => (x >= 100 ? x.toFixed(0) : x.toFixed(2));
console.log(`\nworkload pass | ${BASE_TAG} median/p95 ms | this build median/p95 ms | speedup | cpu ms ${BASE_TAG} -> this | syncs (bound ms at ${syncMs.toFixed(2)} ms/sync) | rows`);
for (const r of rows)
  console.log(`${r.workload} ${r.pass} | ${f(r.base_med)}/${f(r.base_p95)} | ${f(r.new_med)}/${f(r.new_p95)} | ${r.speedup.toFixed(2)}x | ${f(r.base_cpu)} -> ${f(r.new_cpu)} | ${r.syncs} (${f(r.bound_ms)}) | ${r.rows}`);
writeFileSync(join(work, 'replay-results.json'), JSON.stringify({ bounds: [b0, b1], memcpy, rows }, null, 1));
console.log(failed ? 'FAIL: p4 replay' : 'PASS: p4 replay (every answer the same on both builds)');
process.exit(failed ? 1 : 0);
