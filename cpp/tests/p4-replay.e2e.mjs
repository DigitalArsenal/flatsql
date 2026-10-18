#!/usr/bin/env node
// E2E for flatsql-p4-replay-10x-20261018: the format-4 operations SDN nodes
// spend their FlatSQL time in, replayed on flatsql 3.7.2 and on this checkout,
// on the same data.
//
// Uses the prebuilt native clients: this checkout, and the cached v3.7.2's
// engine with the matching replay requests. Offline runs never invoke git,
// network or Docker, and keep their writes in cpp/build/replay-e2e. Each build has its own
// fixture from the same input (a build's store format is its own: 3.7.2 cannot
// read this build's files, nor this build 3.7.2's), once per source tree:
// p4_record_scaling's sea feed (P4_REPLAY_WXF_BYTES) and days of real
// Space-Track gp_history OMMs ingested as the node ingests them. Every measured
// workload gets its own APFS clone of it (cp -c: no second copy of the data).
// For each repetition (P4_REPLAY_REPS, default 5) the two builds run in
// alternating order, each in one fresh process that runs every workload in
// turn, each on its fresh clone: a workload's first pass is cold, the read
// workloads' later passes warm.
//
// Two profiles (P4_REPLAY_PROFILE): "gate" (the default, the gauntlet's E2E,
// under 300 s with its build caches warm) replays a slice: a 1 GB sea feed,
// three seed days, 32,768 OMMs of the ingest day, a 64-tile lead and 8
// retention pages; "full" (p4-replay.bench.mjs, outside the gate) replays the
// 15 GB sea feed, ten seed days, the whole ingest day, a 512-tile lead and 20
// retention pages. It prints,
// per workload, each build's median and p95 wall and CPU time, the speedup,
// the bound (durable syncs x this disk's sync latency, or the bytes at memcpy
// rate) and stock SQLite 3.53.4 on the same index and frames, and fails when
// any answer hashes differently on the two builds (or across repetitions, or
// between the runtimes) or
// a workload is slower than 3.7.2 (exact one-sided Mann-Whitney U test,
// p <= 0.005: at 5 repetitions every run of this build slower than every run
// of 3.7.2).
//
// Two runtimes (P4_REPLAY_RUNTIMES, default "native,aot"): the native test
// client, and the same client and engine as the wasm32 test command
// flatsql-p4-test.wasm (built in Docker with the Linux wasi-sdk 30 that builds
// the shipped flatsql-p4-threads.wasm) run as SDN runs the format-4 engine:
// sdn-server's flatsqlrt compiles it with the node's threaded AOT
// configuration and runs it on wasmrt with the node's native host I/O
// (cpp/tests/p4/aot/replay_aot_test.go, overlaid into that package with go
// test -overlay; the SDN checkout is not changed). The aot runtime needs
// Docker, P4_REPLAY_SDN (else $GAUNTLET_STACK_ROOT/repos/main-packages/
// space-data-network, else the sibling space-data-network checkout) and
// P4_REPLAY_WASMEDGE (else ~/.sdn-wasmedge-static-macos-patched/prefix, the
// patched static WasmEdge the node links).
//
// Environment: FLATBUFFERS_DIR (else the sibling flatbuffers checkout),
// P4_REPLAY_DIR (read-only baseline/input cache), P4_REPLAY_BINARY (the
// prebuilt native client; default cpp/build/flatsql_p4_test),
// P4_REPLAY_GP (the gp_history by-creation root, default
// /opt/data/sdn-archive/spacetrack/gp_history/by-creation), P4_REPLAY_REPS,
// P4_REPLAY_WORKLOADS (comma list), P4_REPLAY_WXF_BYTES.
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream, existsSync, mkdirSync, readdirSync, readFileSync, realpathSync, rmSync, statfsSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createGunzip } from 'node:zlib';

const cpp = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repo = resolve(cpp, '..');
const BASE_TAG = 'v3.7.2';
const PROFILES = {
  gate: { wxfBytes: 1e9, seedDays: 3, dayRows: 32768, leadTiles: 64, retentionPages: 8, warm: 2, boundsMb: 512 },
  full: { wxfBytes: 15e9, seedDays: 10, dayRows: 0, leadTiles: 512, retentionPages: 20, warm: 3, boundsMb: 2048 },
};
const PROFILE = process.env.P4_REPLAY_PROFILE || 'gate';
const prof = PROFILES[PROFILE];
if (!prof) throw new Error(`P4_REPLAY_PROFILE: unknown profile ${PROFILE}`);
const reps = Number(process.env.P4_REPLAY_REPS || 5);
const wxfBytes = Number(process.env.P4_REPLAY_WXF_BYTES || prof.wxfBytes);
const gpRoot = process.env.P4_REPLAY_GP || '/opt/data/sdn-archive/spacetrack/gp_history/by-creation';
const SEED_DAYS = ['2026-09-16', '2026-09-17', '2026-09-18', '2026-09-19', '2026-09-20', '2026-09-21', '2026-09-22', '2026-09-23',
  '2026-09-24', '2026-09-25'].slice(0, prof.seedDays);
const INGEST_DAY = '2026-09-26';
const WORKLOADS = (process.env.P4_REPLAY_WORKLOADS || 'gp_ingest,wxf_lead,sync_norad,sync_window,latest,retention,explorer,open')
  .split(',').filter(Boolean);
const RUNTIMES = (process.env.P4_REPLAY_RUNTIMES || 'native,aot').split(',').filter(Boolean);
for (const r of RUNTIMES) if (r !== 'native' && r !== 'aot') throw new Error(`P4_REPLAY_RUNTIMES: unknown runtime ${r}`);
const t0 = Date.now();
const READS = new Set(['sync_norad', 'sync_window', 'latest', 'explorer']);
const WARM_PASSES = prof.warm;

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

// Offline execution keeps the baseline/input cache read-only and every build,
// fixture and test write inside this checkout. No git, network or Docker.
const offline = process.env.P4_REPLAY_OFFLINE !== '0';
const cache = resolve(process.env.P4_REPLAY_DIR || join(cpp, 'build/replay-e2e'));
const workDir = join(cpp, 'build/replay-e2e');
mkdirSync(workDir, { recursive: true });
// The host I/O confines the aot runtime's store to this root by its real path.
const work = realpathSync(workDir);
process.env.FLATSQL_P4_TEST_DIR = join(work, 'scratch');
process.env.TMPDIR = join(work, 'tmp');
mkdirSync(process.env.TMPDIR, { recursive: true });
if (RUNTIMES.includes('aot') && offline)
  fail('offline AOT needs a newly built wasm command; use P4_REPLAY_RUNTIMES=native (Docker is unavailable offline)');

// The previously built 3.7.2 engine is an immutable offline reference.
const baseSrc = join(cache, 'base');
const baseBinary = join(cache, 'build-base/flatsql_p4_test');
if (!existsSync(baseBinary)) fail(`offline baseline missing: ${baseBinary}`);
const newBinary = process.env.P4_REPLAY_BINARY || join(cpp, 'build/flatsql_p4_test');
if (!existsSync(newBinary)) fail(`build flatsql_p4_test first: ${newBinary}`);
const arms = { base: baseBinary, new: newBinary };

// The aot runtime: each build's wasm test command, and the SDN harness.
// The sources a wasm test command is built from, hashed (build directories
// left out): a rebuild of the same sources reuses the command.
function treeHash(dir) {
  const h = createHash('sha256');
  const walk = (d) => {
    for (const e of readdirSync(d, { withFileTypes: true }).sort((a, b) => (a.name < b.name ? -1 : 1))) {
      if (e.name.startsWith('build') || e.name === 'node_modules' || e.name.startsWith('.')) continue;
      const f = join(d, e.name);
      if (e.isDirectory()) walk(f);
      else if (e.isFile()) h.update(`${f.slice(dir.length)}\0`).update(readFileSync(f));
    }
  };
  walk(dir);
  h.update(flatbuffers());
  return h.digest('hex').slice(0, 16);
}
function wasmTestBuild(name, src) {
  const arch = process.arch === 'arm64' ? 'arm64' : 'x86_64';
  const tgz = join(process.env.XDG_CACHE_HOME || join(homedir(), '.cache'), 'flatsql', `wasi-sdk-30.0-${arch}-linux.tar.gz`);
  if (!existsSync(tgz)) fail(`${tgz} missing: scripts/build-wasm.sh --ps --linux fetches and checks it`);
  const out = join(work, `wasm-${name}-${treeHash(src)}`);
  if (existsSync(join(out, 'flatsql-p4-test.wasm'))) return join(out, 'flatsql-p4-test.wasm');
  rmSync(out, { recursive: true, force: true });
  mkdirSync(out, { recursive: true });
  const script = [
    'apt-get update -qq >/dev/null && apt-get install -y -qq cmake ninja-build g++ >/dev/null',
    'mkdir -p /opt/wasi-sdk && tar -xzf /w/wasi-sdk.tar.gz -C /opt/wasi-sdk --strip-components=1',
    'cmake -S /w/flatsql/cpp -B /tmp/b -G Ninja -DCMAKE_TOOLCHAIN_FILE=/w/flatsql/cpp/cmake/wasi-threads-toolchain.cmake' +
      ' -DWASI_SDK_PREFIX=/opt/wasi-sdk -DFLATBUFFERS_DIR=/w/flatbuffers -DSDS_SCHEMA_DIR=/w/sds -DCMAKE_BUILD_TYPE=Release >/dev/null',
    'cmake --build /tmp/b --target flatsql_p4_test_wasm -j 8 >/tmp/build.log 2>&1 || { tail -40 /tmp/build.log; exit 1; }',
    'cp /tmp/b/flatsql-p4-test.wasm /w/out/',
  ].join('\n');
  // The SDS schemas the format-5 frames are generated from (the 1.255.0 devDependency).
  const sds = join(src, '..', 'node_modules', 'spacedatastandards.org', 'schema');
  const sdsMount = existsSync(sds) ? ['-v', `${sds}:/w/sds:ro`] : [];
  run('docker', ['run', '--rm', ...sdsMount, '-v', `${src}:/w/flatsql/cpp:ro`, '-v', `${flatbuffers()}:/w/flatbuffers:ro`, '-v', `${tgz}:/w/wasi-sdk.tar.gz:ro`,
    '-v', `${out}:/w/out`, 'ubuntu:22.04', 'bash', '-euc', script], { stdio: ['ignore', 'inherit', 'inherit'] });
  return join(out, 'flatsql-p4-test.wasm');
}
function aotSetup() {
  const stack = process.env.GAUNTLET_STACK_ROOT;
  const sdn = resolve(process.env.P4_REPLAY_SDN || (stack ? join(stack, 'repos/main-packages/space-data-network') : join(repo, '..', 'space-data-network')));
  const pkg = join(sdn, 'sdn-server/internal/flatsqlrt');
  if (!existsSync(join(pkg, 'aot.go'))) fail(`no SDN checkout at ${sdn} (set P4_REPLAY_SDN)`);
  const wasmedge = resolve(process.env.P4_REPLAY_WASMEDGE || join(homedir(), '.sdn-wasmedge-static-macos-patched/prefix'));
  if (!existsSync(join(wasmedge, 'sdn-runtime-patches.txt'))) fail(`${wasmedge} is not the patched WasmEdge prefix (set P4_REPLAY_WASMEDGE)`);
  const wasm = { base: wasmTestBuild('base', join(baseSrc, 'cpp')), new: wasmTestBuild('new', cpp) };
  const overlay = join(work, 'aot-overlay.json');
  writeFileSync(overlay, JSON.stringify({ Replace: { [join(pkg, 'zz_p4_replay_aot_test.go')]: join(cpp, 'tests/p4/aot/replay_aot_test.go') } }));
  const bin = join(work, 'p4-replay-aot.test');
  const env = { ...process.env, WASMEDGE_DIR: wasmedge, GOFLAGS: '-p=6' };
  for (const k of Object.keys(env)) if (k.startsWith('CGO_') || k === 'WASMEDGE_LIB_DIR') delete env[k];
  run(join(sdn, 'scripts/go-with-wasmedge.sh'), ['test', '-c', '-overlay', overlay, '-o', bin, './internal/flatsqlrt'],
    { cwd: join(sdn, 'sdn-server'), env, stdio: ['ignore', 'ignore', 'inherit'] });
  return { bin, wasm, cache: join(work, 'aot-cache') };
}
const aot = RUNTIMES.includes('aot') ? aotSetup() : null;

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
const inputPath = (name) => existsSync(join(cache, name)) ? join(cache, name) : join(work, name);
const seedTsv = inputPath(prof.seedDays === 10 ? 'gp-seed.tsv' : `gp-seed-${prof.seedDays}d.tsv`);
await gpTsv(seedTsv, SEED_DAYS);
let dayTsv = inputPath('gp-day.tsv');
await gpTsv(dayTsv, [INGEST_DAY]);
if (prof.dayRows) {
  const slice = inputPath(`gp-day-${prof.dayRows}.tsv`);
  if (!existsSync(slice)) writeFileSync(slice, readFileSync(dayTsv, 'utf8').split('\n').slice(0, prof.dayRows).join('\n') + '\n');
  dayTsv = slice;
}

function freeBytes(dir) {
  const s = statfsSync(dir);
  return Number(s.bavail) * Number(s.bsize);
}

// The fixtures: each build's own, from the same input, once per source tree.
const fixtureRoot = work;
const want = `wxf=${wxfBytes} seed=${SEED_DAYS.join(',')}`;
function fixtureOf(arm, src) {
  const name = `fixture-${PROFILE}-${arm}-`;
  if (arm === 'base') {
    const cached = readdirSync(cache).filter((n) => n.startsWith(name)).map((n) => join(cache, n))
      .find((d) => existsSync(join(d, 'REPLAY-FIXTURE')) && readFileSync(join(d, 'REPLAY-FIXTURE'), 'utf8') === want);
    if (!cached) fail(`offline ${BASE_TAG} fixture missing for ${want}`);
    return cached;
  }
  const dir = join(fixtureRoot, name + treeHash(src));
  const stamp = join(dir, 'REPLAY-FIXTURE');
  if (existsSync(stamp) && readFileSync(stamp, 'utf8') === want) return dir;
  // A fixture of this profile and build from other sources is stale.
  for (const e of readdirSync(fixtureRoot)) if (e.startsWith(name)) rmSync(join(fixtureRoot, e), { recursive: true, force: true });
  if (freeBytes(fixtureRoot) < wxfBytes * 1.15 + 22 * 2 ** 30) fail(`${fixtureRoot}: not enough free disk for a ${(wxfBytes / 1e9).toFixed(1)} GB fixture`);
  mkdirSync(dir, { recursive: true });
  run(arms[arm], ['--test=p4_record_scaling', `--store=${dir}`, `--bytes=${Math.round(wxfBytes)}`, '--build-only=1'], { stdio: ['ignore', 'ignore', 'inherit'] });
  run(arms[arm], ['--test=p4_replay', '--phase=build', `--store=${dir}`, `--gp=${seedTsv}`], { stdio: ['ignore', 'ignore', 'inherit'] });
  writeFileSync(stamp, want);
  return dir;
}
mkdirSync(fixtureRoot, { recursive: true });
const fixtures = { base: fixtureOf('base', join(baseSrc, 'cpp')), new: fixtureOf('new', cpp) };
console.log(`profile ${PROFILE}: builds and fixtures ready at ${((Date.now() - t0) / 1000).toFixed(0)} s`);

// One process runs the workloads in turn, each on a fresh clone of the
// build's fixture; each workload's REPLAY lines.
let cloneN = 0;
function measure(runtime, arm, workloads) {
  const clones = workloads.map(() => join(work, `clone-${process.pid}-${cloneN++}`));
  if (freeBytes(work) < 20 * 2 ** 30) fail('less than 20 GiB free');
  for (const c of clones) run('cp', ['-c', '-R', fixtures[arm], c]);
  try {
    const args = ['--test=p4_replay', '--phase=run', `--store=${clones.join(',')}`, `--workload=${workloads.join(',')}`, `--gp=${seedTsv}`,
      `--gp-day=${dayTsv}`, `--passes=${1 + WARM_PASSES}`, `--lead-tiles=${prof.leadTiles}`, `--retention-pages=${prof.retentionPages}`];
    const r = runtime === 'native'
      ? spawnSync(arms[arm], args, { encoding: 'utf8', maxBuffer: 64 << 20 })
      : spawnSync(aot.bin, ['-test.run', '^TestP4ReplayAOT$', '-test.timeout', '2h'], {
        cwd: work, encoding: 'utf8', maxBuffer: 64 << 20,
        env: { ...process.env, P4_REPLAY_AOT_WASM: aot.wasm[arm], P4_REPLAY_AOT_CACHE: aot.cache, P4_REPLAY_AOT_ROOT: work,
          P4_REPLAY_AOT_ARGS: args.join('\x1f') },
      });
    const out = {};
    const of = (w) => (out[w] ??= { passes: [], after: '' });
    for (const l of (r.stdout || '').split('\n')) {
      if (l.startsWith('REPLAY {')) { const p = JSON.parse(l.slice(7)); of(p.workload).passes.push(p); }
      if (l.startsWith('REPLAY_AFTER ')) { const p = JSON.parse(l.slice(13)); of(p.workload).after = p.hash; }
    }
    if (r.status !== 0 || workloads.some((w) => !out[w]?.passes.length))
      fail(`${runtime} ${arm} ${workloads.join(',')}: exit ${r.status ?? r.signal}\n${(r.stdout || '').slice(-2000)}${(r.stderr || '').slice(-2000)}`);
    return out;
  } finally {
    for (const c of clones) rmSync(c, { recursive: true, force: true });
  }
}

function bounds() {
  const r = spawnSync(arms.new, ['--test=p4_replay', '--phase=bounds', `--store=${work}`, `--bounds-mb=${prof.boundsMb}`], { encoding: 'utf8' });
  const l = (r.stdout || '').split('\n').find((x) => x.startsWith('REPLAY_BOUNDS '));
  if (!l) fail(`bounds: ${r.stdout}${r.stderr}`);
  return JSON.parse(l.slice(14));
}

const q = (v, p) => {
  const s = [...v].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(p * s.length))];
};
const med = (v) => q(v, 0.5);

// Slower is a test, not a comparison of two noisy medians: a bare median
// comparison fails an unchanged (sync-bound) workload half the time. The exact
// one-sided Mann-Whitney U test: U counts the pairs (3.7.2 run, this build's
// run) where this build is faster (ties count a half); under the null (the
// two builds' runs exchangeable) p = P(U <= observed), from the exact
// distribution of U for these sample sizes. Each sample is one process (its
// warm passes count as their median). A workload fails at p <= ALPHA. At 5
// repetitions only complete separation qualifies (p = 1/252 = 0.004); over
// this E2E's 12 comparisons per runtime (24 with both) the false-alarm rate is
// at most 4.8% (9.5%) by Bonferroni, and a real slowdown of a few run-to-run
// deviations still separates every run.
const ALPHA = 0.005;
function slowerP(base, mine) {
  let u = 0;
  for (const x of mine) for (const y of base) u += x < y ? 1 : x === y ? 0.5 : 0;
  const m = mine.length, n = base.length;
  // c[i][j][k]: orderings of i of mine and j of base with U = k.
  const c = Array.from({ length: m + 1 }, () => Array.from({ length: n + 1 }, () => new Float64Array(m * n + 1)));
  for (let i = 0; i <= m; i++)
    for (let j = 0; j <= n; j++)
      for (let k = 0; k <= i * j; k++) {
        if (i === 0 || j === 0) { c[i][j][k] = k === 0 ? 1 : 0; continue; }
        // the largest value is mine (U unchanged) or base (U gains i).
        c[i][j][k] = c[i - 1][j][k] + (k >= i ? c[i][j - 1][k - i] : 0);
      }
  const all = c[m][n];
  const total = all.reduce((x, y) => x + y, 0);
  let at = 0;
  for (let k = 0; k <= Math.floor(u); k++) at += all[k];
  return at / total;
}

const b0 = bounds();
console.log(`bounds: ${JSON.stringify(b0)}`);
console.log('sync accounting: v3.7.2=stream+FULL-commit counters; native format5=actual flatsql_io_sync calls (per-call min/max asserted)');
const results = {}; // runtime -> workload -> pass -> arm -> [{wall, cpu, syncs}]
const hashes = {}; // workload|pass -> Set, over both builds and both runtimes
let failed = false;
for (let rep = 0; rep < reps; rep++) {
  for (const runtime of RUNTIMES) {
    const order = rep % 2 ? ['new', 'base'] : ['base', 'new'];
    for (const arm of order) {
      const all = measure(runtime, arm, WORKLOADS);
      for (const workload of WORKLOADS) {
        // One sample per process and pass kind: a process's warm passes are
        // not independent of each other, so they count as their median.
        const kinds = {};
        for (const p of all[workload].passes) {
          if (arm === 'new' && ['gp_ingest', 'wxf_lead'].includes(workload) &&
              (p.syncs !== p.ops || p.sync_calls !== p.ops || p.sync_min !== 1 || p.sync_max !== 1))
            fail(`${runtime} ${workload}: ${p.syncs} syncs / ${p.ops} calls, per-call range ${p.sync_min}..${p.sync_max}; expected exactly 1 for every call`);
          (kinds[p.pass] ??= []).push(p);
          (hashes[`${workload}|${p.pass}`] ??= new Set()).add(p.hash);
        }
        for (const [kind, ps] of Object.entries(kinds)) {
          const one = { ...ps[0], wall_ms: med(ps.map((p) => p.wall_ms)), cpu_ms: med(ps.map((p) => p.cpu_ms)) };
          (((results[runtime] ??= {})[workload] ??= {})[kind] ??= { base: [], new: [] })[arm].push(one);
        }
        if (all[workload].after) (hashes[`${workload}|after`] ??= new Set()).add(all[workload].after);
      }
    }
  }
  console.log(`repetition ${rep + 1}/${reps} done at ${((Date.now() - t0) / 1000).toFixed(0)} s (load ${b0.load})`);
}
// The independent reference: stock SQLite on the same index and frames, the
// read workloads only, on a fresh clone (cold, then warm): a yardstick, not
// a comparison, so one repetition (P4_REPLAY_STOCK_REPS).
const stockRuns = {}; // workload -> pass -> [{wall_ms, cpu_ms, rows}]
for (let rep = 0; rep < Number(process.env.P4_REPLAY_STOCK_REPS || 1); rep++) {
  for (const workload of WORKLOADS.filter((w) => READS.has(w))) {
    const clone = join(work, `clone-${process.pid}-${cloneN++}`);
    run('cp', ['-c', '-R', fixtures.new, clone]);
    try {
      const r = spawnSync(arms.new, ['--test=p4_replay', '--phase=stock', `--store=${clone}`, `--workload=${workload}`, `--gp=${seedTsv}`,
        `--passes=2`], { encoding: 'utf8', maxBuffer: 64 << 20 });
      const lines = (r.stdout || '').split('\n').filter((l) => l.startsWith('REPLAY_STOCK ')).map((l) => JSON.parse(l.slice(13)));
      if (r.status !== 0 || !lines.length) fail(`stock ${workload}: exit ${r.status ?? r.signal}\n${(r.stderr || '').slice(-2000)}`);
      const kinds = {};
      for (const l of lines) (kinds[l.pass] ??= []).push(l);
      for (const [kind, ls] of Object.entries(kinds))
        ((stockRuns[workload] ??= {})[kind] ??= []).push({ ...ls[0], wall_ms: med(ls.map((l) => l.wall_ms)), cpu_ms: med(ls.map((l) => l.cpu_ms)) });
    } finally {
      rmSync(clone, { recursive: true, force: true });
    }
  }
}
const b1 = bounds();
console.log(`bounds after: ${JSON.stringify(b1)}`);
const syncMs = Math.min(b0.sync_ms_median, b1.sync_ms_median);
const memcpy = Math.max(b0.memcpy_GBps, b1.memcpy_GBps);

const rows = [];
for (const workload of WORKLOADS) {
  for (const key of Object.keys(hashes).filter((k) => k.startsWith(`${workload}|`))) {
    if (hashes[key].size === 1) continue;
    failed = true;
    console.error(key.endsWith('|after')
      ? `FAIL: ${workload}: the store after the writes differs between builds or runtimes`
      : `FAIL: ${key}: ${hashes[key].size} distinct answer hashes (${[...hashes[key]].join(', ')})`);
  }
  for (const runtime of RUNTIMES) {
    for (const [pass, byArm] of Object.entries(results[runtime]?.[workload] || {})) {
      const a = byArm.base.map((p) => p.wall_ms), b = byArm.new.map((p) => p.wall_ms);
      const ac = byArm.base.map((p) => p.cpu_ms), bc = byArm.new.map((p) => p.cpu_ms);
      const syncs = med(byArm.new.map((p) => p.syncs));
      const bound = syncs > 0 ? syncs * syncMs : 0;
      const sk = stockRuns[workload]?.[pass];
      const stockRows = sk ? med(sk.map((x) => x.rows)) : null;
      const nrows = med(byArm.new.map((p) => p.rows));
      rows.push({ runtime, workload, pass, base_med: med(a), base_p95: q(a, 0.95), new_med: med(b), new_p95: q(b, 0.95),
        sync_min: Math.min(...byArm.new.map((p) => p.sync_min)), sync_max: Math.max(...byArm.new.map((p) => p.sync_max)),
        base_syncs: med(byArm.base.map((p) => p.syncs)), calls: med(byArm.new.map((p) => p.ops)), speedup: med(a) / med(b), base_cpu: med(ac), new_cpu: med(bc), syncs, bound_ms: bound, rows: nrows,
        stock_ms: sk ? med(sk.map((x) => x.wall_ms)) : null, stock_rows: stockRows });
      if (sk && stockRows !== nrows) console.error(`note: ${workload}|${pass}: stock SQLite answered ${stockRows} rows, the engine ${nrows}: not the same question`);
      const p = slowerP(a, b);
      if (p <= ALPHA) {
        failed = true;
        console.error(`FAIL: ${runtime} ${workload}|${pass}: this build ${med(b).toFixed(1)} ms is slower than ${BASE_TAG} ${med(a).toFixed(1)} ms (Mann-Whitney p = ${p.toFixed(4)})`);
      }
    }
  }
}
const f = (x) => (x < 0 ? 'n/a' : x >= 100 ? x.toFixed(0) : x.toFixed(2));
console.log(`\nruntime workload pass | ${BASE_TAG} median/p95 ms | this build median/p95 ms | speedup | cpu ms ${BASE_TAG} -> this | syncs old -> new; per call (bound ms at ${syncMs.toFixed(2)} ms/sync) | rows | stock SQLite ms`);
for (const r of rows)
  console.log(`${r.runtime} ${r.workload} ${r.pass} | ${f(r.base_med)}/${f(r.base_p95)} | ${f(r.new_med)}/${f(r.new_p95)} | ${r.speedup.toFixed(2)}x | ${f(r.base_cpu)} -> ${f(r.new_cpu)} | ${r.base_syncs} -> ${r.syncs}; ${f(r.base_syncs / r.calls)} -> ${f(r.syncs / r.calls)}/call${['gp_ingest', 'wxf_lead'].includes(r.workload) ? ` [${r.sync_min}..${r.sync_max}]` : ''} (${f(r.bound_ms)}) | ${r.rows} | ${r.stock_ms == null ? 'n/a' : f(r.stock_ms)}`);
// Exact same E2E: independently walk every stream, then kill a lead three
// times and compare the acknowledged prefix before/after cache deletion.
for (const phase of ['walk', 'kill']) {
  const args = ['--test=p4_replay', `--phase=${phase}`, `--store=${phase === 'walk' ? fixtures.new : join(work, 'kill')}`,
    `--gp=${seedTsv}`, `--seed=${process.env.P4_REPLAY_KILL_SEED || 20261018}`, '--rounds=3', `--lead-tiles=${prof.leadTiles}`];
  const r = spawnSync(arms.new, args, { encoding: 'utf8', maxBuffer: 64 << 20 });
  process.stdout.write(r.stdout || '');
  process.stderr.write(r.stderr || '');
  if (r.status !== 0) failed = true;
}
writeFileSync(join(work, `replay-results-${PROFILE}.json`), JSON.stringify({ profile: PROFILE, bounds: [b0, b1], memcpy, rows }, null, 1));
console.log(`${PROFILE} profile: ${((Date.now() - t0) / 1000).toFixed(0)} s`);
console.log(failed ? 'FAIL: p4 replay' : `PASS: p4 replay (every answer the same on both builds, ${RUNTIMES.join(' and ')})`);
process.exit(failed ? 1 : 0);
