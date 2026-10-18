#!/usr/bin/env node
// E2E for flatsql-p4-replay-10x-20261018: the format-4 operations SDN nodes
// spend their FlatSQL time in, replayed on flatsql 3.7.2 and on this checkout,
// on the same data.
//
// Builds the native test client twice: this checkout, and v3.7.2's
// engine with this checkout's replay test (cpp/tests/p4/t_replay.cpp and
// wxf_fixture.h: the requests, not the engine). Each build builds its own
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
// under about 450 s with its build caches warm) replays a slice: a 1 GB sea feed,
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
// P4_REPLAY_DIR (work directory; default the system temp directory),
// P4_REPLAY_FIXTURES (where the fixtures are built and kept; default the work
// directory), SDS_SCHEMA_DIR (else node_modules, else the stack's SDS),
// P4_REPLAY_GP (the gp_history by-creation root, default
// /opt/data/sdn-archive/spacetrack/gp_history/by-creation), P4_REPLAY_REPS,
// P4_REPLAY_WORKLOADS (comma list), P4_REPLAY_WXF_BYTES, P4_REPLAY_NEW_ONLY=1
// (explicitly skip the baseline), P4_REPLAY_BASE_SOURCE (local extracted v3.7.2
// tree; never changes the self-building default), P4_REPLAY_BASE_FIXTURE
// or P4_REPLAY_NEW_FIXTURE (matching locally built fixture; measurements use
// clones), P4_REPLAY_WASM_IMAGE (prebuilt cmake/ninja/g++ image; builds
// with Docker networking disabled), P4_REPLAY_BOUNDS_MB (local bounds scratch size), and
// P4_REPLAY_DISCARD_BASE_FIXTURE=1 (remove a work-local baseline after its runs).
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream, existsSync, mkdirSync, readdirSync, readFileSync, realpathSync, rmSync, statfsSync, writeFileSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
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
const NEW_ONLY = process.env.P4_REPLAY_NEW_ONLY === '1';
const ARM_NAMES = NEW_ONLY ? ['new'] : ['base', 'new'];
const reps = Number(process.env.P4_REPLAY_REPS || 5);
const wxfBytes = Number(process.env.P4_REPLAY_WXF_BYTES || prof.wxfBytes);
const boundsMb = Number(process.env.P4_REPLAY_BOUNDS_MB || prof.boundsMb);
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
  throw new Error(`FAIL: ${msg}`);
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

const workDir = process.env.P4_REPLAY_DIR || join(tmpdir(), `flatsql-p4-replay-${createHash('sha256').update(repo).digest('hex').slice(0, 12)}`);
mkdirSync(workDir, { recursive: true });
// The host I/O confines the aot runtime's store to this root by its real path.
const work = realpathSync(workDir);
// Reject an impossible full run before builds or input conversion consume space.
if (PROFILE === 'full') diskCheck(work, wxfBytes * 1.15);
process.env.FLATSQL_P4_TEST_DIR = join(work, 'scratch');
process.env.TMPDIR = join(work, 'tmp');
mkdirSync(process.env.TMPDIR, { recursive: true });

// The SDS schemas format 5's frames are generated from: this checkout's
// spacedatastandards.org devDependency (1.255.0), else the stack's pinned SDS.
function sdsSchemaDir() {
  const stack = process.env.GAUNTLET_STACK_ROOT;
  const candidates = [process.env.SDS_SCHEMA_DIR, join(repo, 'node_modules/spacedatastandards.org/schema'),
    stack && join(stack, 'repos/main-packages/spacedatastandards.org/schema')].filter(Boolean);
  const dir = candidates.find((d) => existsSync(join(d, 'FSG/main.fbs')));
  if (!dir) fail(`SDS 1.255.0 schemas not found (tried ${candidates.join(', ')}): npm ci, or set SDS_SCHEMA_DIR`);
  return dir;
}
const sdsDir = sdsSchemaDir();

function build(name, src) {
  const dir = join(work, `build-${name}`);
  run('cmake', ['-S', src, '-B', dir, '-DCMAKE_BUILD_TYPE=Release', `-DFLATBUFFERS_DIR=${flatbuffers()}`, `-DSDS_SCHEMA_DIR=${sdsDir}`], {
    stdio: ['ignore', 'ignore', 'inherit'],
  });
  run('nice', ['cmake', '--build', dir, '--target', 'flatsql_p4_test', ...(name === 'new' ? ['flatsql_p4_fault_test'] : []), '-j', '8'], { stdio: ['ignore', 'ignore', 'inherit'] });
  return join(dir, 'flatsql_p4_test');
}

// The default always archives and builds both arms. Local restricted runs may
// explicitly supply an already extracted 3.7.2 source; it is copied before the
// current replay client is overlaid, so the supplied source stays untouched.
const baseSrc = join(work, 'base');
if (!NEW_ONLY) {
  rmSync(baseSrc, { recursive: true, force: true });
  mkdirSync(baseSrc, { recursive: true });
  if (process.env.P4_REPLAY_BASE_SOURCE) {
    const local = resolve(process.env.P4_REPLAY_BASE_SOURCE);
    if (local === baseSrc) fail('P4_REPLAY_BASE_SOURCE must be outside the harness output base directory');
    run('cp', ['-p', '-R', join(local, 'cpp'), baseSrc]);
  } else {
    const archive = spawnSync('git', ['-C', repo, 'archive', BASE_TAG, 'cpp'], { maxBuffer: 256 << 20 });
    if (archive.status !== 0) fail(`git archive ${BASE_TAG}: ${archive.stderr}`);
    execFileSync('tar', ['-x', '-C', baseSrc], { input: archive.stdout });
  }
  for (const f of ['t_replay.cpp', 'wxf_fixture.h']) writeFileSync(join(baseSrc, 'cpp/tests/p4', f), readFileSync(join(cpp, 'tests/p4', f)));
}
const arms = Object.fromEntries(ARM_NAMES.map((name) => [name, build(name, name === 'new' ? cpp : join(baseSrc, 'cpp'))]));

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
  const sdk = join(work, `wasi-sdk-30-${arch}`);
  if (!existsSync(join(sdk, '.ready')) || !existsSync(join(sdk, 'bin/clang'))) {
    mkdirSync(sdk, { recursive: true });
    run('tar', ['-xzf', tgz, '-C', sdk, '--strip-components=1']);
    writeFileSync(join(sdk, '.ready'), 'wasi-sdk-30.0\n');
  }
  const buildImage = process.env.P4_REPLAY_WASM_IMAGE;
  const script = [
    ...(buildImage ? [] : ['apt-get update -qq >/dev/null && apt-get install -y -qq cmake ninja-build g++ >/dev/null']),
    'cmake -S /w/flatsql/cpp -B /w/out/build -G Ninja -DCMAKE_TOOLCHAIN_FILE=/w/flatsql/cpp/cmake/wasi-threads-toolchain.cmake' +
      ' -DWASI_SDK_PREFIX=/w/wasi-sdk -DFLATBUFFERS_DIR=/w/flatbuffers -DSDS_SCHEMA_DIR=/w/sds -DCMAKE_BUILD_TYPE=Release >/dev/null',
    'cmake --build /w/out/build --target flatsql_p4_test_wasm -j 8 >/w/out/build.log 2>&1 || { tail -40 /w/out/build.log; exit 1; }',
    'cp /w/out/build/flatsql-p4-test.wasm /w/out/',
  ].join('\n');
  // The SDS schemas the format-5 frames are generated from (the 1.255.0 devDependency).
  const sdsMount = ['-v', `${sdsDir}:/w/sds:ro`];
  run('docker', ['run', '--rm', '-w', '/w/out', ...(buildImage ? ['--network', 'none', '--entrypoint', '/bin/bash'] : []), ...sdsMount, '-v', `${src}:/w/flatsql/cpp:ro`, '-v', `${flatbuffers()}:/w/flatbuffers:ro`, '-v', `${sdk}:/w/wasi-sdk:ro`,
    '-v', `${out}:/w/out`, buildImage || 'ubuntu:22.04', ...(buildImage ? [] : ['bash']), '-euc', script], { stdio: ['ignore', 'inherit', 'inherit'] });
  return join(out, 'flatsql-p4-test.wasm');
}
function aotSetup() {
  const stack = process.env.GAUNTLET_STACK_ROOT;
  const sdn = resolve(process.env.P4_REPLAY_SDN || (stack ? join(stack, 'repos/main-packages/space-data-network') : join(repo, '..', 'space-data-network')));
  const pkg = join(sdn, 'sdn-server/internal/flatsqlrt');
  if (!existsSync(join(pkg, 'aot.go'))) fail(`no SDN checkout at ${sdn} (set P4_REPLAY_SDN)`);
  const wasmedge = resolve(process.env.P4_REPLAY_WASMEDGE || join(homedir(), '.sdn-wasmedge-static-macos-patched/prefix'));
  if (!existsSync(join(wasmedge, 'sdn-runtime-patches.txt'))) fail(`${wasmedge} is not the patched WasmEdge prefix (set P4_REPLAY_WASMEDGE)`);
  const wasm = Object.fromEntries(ARM_NAMES.map((name) => [name, wasmTestBuild(name, name === 'new' ? cpp : join(baseSrc, 'cpp'))]));
  const overlay = join(work, 'aot-overlay.json');
  writeFileSync(overlay, JSON.stringify({ Replace: { [join(pkg, 'zz_p4_replay_aot_test.go')]: join(cpp, 'tests/p4/aot/replay_aot_test.go') } }));
  const bin = join(work, 'p4-replay-aot.test');
  const env = { ...process.env, WASMEDGE_DIR: wasmedge, GOFLAGS: '-p=6 -buildvcs=false' };
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
const inputPath = (name) => join(work, name);
const seedTsv = inputPath(prof.seedDays === 10 ? 'gp-seed.tsv' : `gp-seed-${prof.seedDays}d.tsv`);
await gpTsv(seedTsv, SEED_DAYS);
let dayTsv = inputPath('gp-day.tsv');
await gpTsv(dayTsv, [INGEST_DAY]);
if (prof.dayRows) {
  const slice = inputPath(`gp-day-${prof.dayRows}.tsv`);
  if (!existsSync(slice)) writeFileSync(slice, readFileSync(dayTsv, 'utf8').split('\n').slice(0, prof.dayRows).join('\n') + '\n');
  dayTsv = slice;
}

function diskCheck(dir, needed = 0) {
  if (PROFILE === 'full') run('df', ['-k', dir]);
  if (freeBytes(dir) < 20 * 2 ** 30 + needed) fail(`${dir}: must keep at least 20 GiB free (need ${needed} additional bytes)`);
}
function freeBytes(dir) {
  const s = statfsSync(dir);
  return Number(s.bavail) * Number(s.bsize);
}

// The fixtures: each build's own, from the same input, once per source tree.
const fixtureRoot = process.env.P4_REPLAY_FIXTURES || work;
const want = `wxf=${wxfBytes} seed=${SEED_DAYS.join(',')}`;
const fullFixtures = new Set();
// Register before construction so even a failed full build is removed.
if (PROFILE === 'full') process.once('exit', () => { for (const f of fullFixtures) rmSync(f, { recursive: true, force: true }); });
function fixtureOf(arm, src) {
  const fixtureOverride = process.env[arm === 'base' ? 'P4_REPLAY_BASE_FIXTURE' : 'P4_REPLAY_NEW_FIXTURE'];
  if (fixtureOverride) {
    const cached = resolve(fixtureOverride);
    if (readFileSync(join(cached, 'REPLAY-FIXTURE'), 'utf8') !== want) fail(`P4_REPLAY_${arm.toUpperCase()}_FIXTURE does not match the requested profile`);
    console.log(`local ${arm} fixture: ${cached} (read only; measurements use clones)`);
    return cached;
  }
  const name = `fixture-${PROFILE}-${arm}-`;
  const dir = join(fixtureRoot, name + treeHash(src));
  if (PROFILE === 'full') fullFixtures.add(dir);
  const stamp = join(dir, 'REPLAY-FIXTURE');
  if (existsSync(stamp) && readFileSync(stamp, 'utf8') === want) return dir;
  // A fixture of this profile and build from other sources is stale.
  for (const e of readdirSync(fixtureRoot)) if (e.startsWith(name)) rmSync(join(fixtureRoot, e), { recursive: true, force: true });
  if (freeBytes(fixtureRoot) < wxfBytes * 1.15 + 20 * 2 ** 30) fail(`${fixtureRoot}: not enough free disk for a ${(wxfBytes / 1e9).toFixed(1)} GB fixture`);
  mkdirSync(dir, { recursive: true });
  diskCheck(fixtureRoot, wxfBytes * 1.15);
  run(arms[arm], ['--test=p4_record_scaling', `--store=${dir}`, `--bytes=${Math.round(wxfBytes)}`, '--build-only=1'], { stdio: ['ignore', 'ignore', 'inherit'] });
  diskCheck(fixtureRoot);
  run(arms[arm], ['--test=p4_replay', '--phase=build', `--store=${dir}`, `--gp=${seedTsv}`], { stdio: ['ignore', 'ignore', 'inherit'] });
  writeFileSync(stamp, want);
  return dir;
}
mkdirSync(fixtureRoot, { recursive: true });
const fixtures = Object.fromEntries(ARM_NAMES.map((name) => [name, fixtureOf(name, name === 'new' ? cpp : join(baseSrc, 'cpp'))]));
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
    const r = runtimeRun(runtime, arm, args);
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

function runtimeRun(runtime, arm, args) {
  diskCheck(work);
  return runtime === 'native'
    ? spawnSync(arms[arm], args, { encoding: 'utf8', maxBuffer: 64 << 20 })
    : spawnSync(aot.bin, ['-test.run', '^TestP4ReplayAOT$', '-test.timeout', '2h'], {
      cwd: work, encoding: 'utf8', maxBuffer: 64 << 20,
      env: { ...process.env, P4_REPLAY_AOT_WASM: aot.wasm[arm], P4_REPLAY_AOT_CACHE: aot.cache, P4_REPLAY_AOT_ROOT: work,
        P4_REPLAY_AOT_ARGS: args.join('\x1f') },
    });
}
// checkpoint-prepare: a PUT's ack while a checkpoint prepares off the writer
// (its preparer held after its snapshot) against PUTs with none preparing.
const PHASE_LINE = { 'index-deleted': 'REPLAY_INDEX_DELETED ', compaction: 'REPLAY_COMPACTION ', 'checkpoint-prepare': 'REPLAY_CHECKPOINT_PREPARE ' };
function checkpointPhases(runtime, phases = ['index-deleted', 'compaction', 'checkpoint-prepare']) {
  for (const phase of phases) {
    diskCheck(work, phase === 'compaction' ? wxfBytes * 1.15 : 0);
    const clone = join(work, `clone-${process.pid}-${cloneN++}`);
    const reference = join(work, `clone-${process.pid}-${cloneN++}`);
    run('cp', ['-c', '-R', fixtures.new, clone]);
    if (phase === 'index-deleted') run('cp', ['-c', '-R', fixtures.new, reference]);
    try {
      const args = ['--test=p4_replay', `--phase=${phase}`, `--store=${clone}`, `--reference=${reference}`,
        `--gp=${seedTsv}`, `--retention-pages=${prof.retentionPages}`];
      const r = runtimeRun(runtime, 'new', args);
      console.log(`checkpoint phase runtime=${runtime} profile=${PROFILE}`);
      process.stdout.write(r.stdout || ''); process.stderr.write(r.stderr || '');
      if (r.status !== 0 || !(r.stdout || '').includes(PHASE_LINE[phase]))
        fail(`${runtime} ${phase}: exit ${r.status ?? r.signal}`);
    } finally {
      rmSync(clone, { recursive: true, force: true }); rmSync(reference, { recursive: true, force: true });
    }
    diskCheck(work);
  }
}


// The IPFS protocol phase is part of the same replay gate, on a fresh clone.
// Its independent parsers and measurements run in C++, natively only.
function ipfsPhase() {
  diskCheck(work, wxfBytes * 3);
  const clone = join(work, `clone-${process.pid}-${cloneN++}`);
  run('cp', ['-c', '-R', fixtures.new, clone]);
  try {
    const r = runtimeRun('native', 'new', ['--test=p4_replay', '--phase=ipfs', `--store=${clone}`,
      `--gp=${seedTsv}`, `--retention-pages=${prof.retentionPages}`]);
    process.stdout.write(r.stdout || ''); process.stderr.write(r.stderr || '');
    if (r.status !== 0 || !(r.stdout || '').includes('REPLAY_IPFS wall_ms=')) fail(`native IPFS: exit ${r.status ?? r.signal}`);
  } finally { rmSync(clone, { recursive: true, force: true }); }
  diskCheck(work);
}
function crashPhases() {
  for (const test of ['t_kill', 't_power_loss']) {
    diskCheck(work);
    const bin = test === 't_kill' ? arms.new : join(work, 'build-new', 'flatsql_p4_fault_test');
    const r = spawnSync(bin, [`--test=${test}`, '--slow=1', '--rounds=30'], { encoding: 'utf8', maxBuffer: 64 << 20 });
    process.stdout.write(r.stdout || ''); process.stderr.write(r.stderr || '');
    if (r.status !== 0) fail(`${test}: exit ${r.status ?? r.signal}`);
  }
}

function bounds() {
  diskCheck(work, boundsMb * 2 ** 20);
  const r = spawnSync(arms.new, ['--test=p4_replay', '--phase=bounds', `--store=${work}`, `--bounds-mb=${boundsMb}`], { encoding: 'utf8' });
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

if (NEW_ONLY) {
  checkpointPhases('native', PROFILE === 'full' ? ['index-deleted'] : undefined);
  if (PROFILE === 'gate') { ipfsPhase(); crashPhases(); }
  console.log(`${PROFILE} profile: ${((Date.now() - t0) / 1000).toFixed(0)} s`);
  console.log(`PASS: p4 replay ${PROFILE} new-only (checkpoint open, row-index rebuild and every read answer equal)`);
  process.exit(0);
}

const b0 = bounds();
console.log(`bounds: ${JSON.stringify(b0)}`);
console.log('sync accounting: v3.7.2=stream+FULL-commit counters; native format5=actual flatsql_io_sync calls (per-call min/max asserted)');
const results = {}; // runtime -> workload -> pass -> arm -> [{wall, cpu, syncs}]
const hashes = {}; // workload|pass -> Set, over both builds and both runtimes
let failed = false;
const progressFile = join(work, `replay-progress-${PROFILE}.jsonl`);
writeFileSync(progressFile, '');
for (let rep = 0; rep < reps; rep++) {
  for (const runtime of RUNTIMES) {
    const order = rep % 2 ? ['new', 'base'] : ['base', 'new'];
    for (const arm of order) {
      const all = measure(runtime, arm, WORKLOADS);
      writeFileSync(progressFile, JSON.stringify({ rep, runtime, arm, all }) + '\n', { flag: 'a' });
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
if (process.env.P4_REPLAY_DISCARD_BASE_FIXTURE === '1') {
  const baseline = realpathSync(fixtures.base);
  if (!baseline.startsWith(work + '/')) fail('P4_REPLAY_DISCARD_BASE_FIXTURE only deletes a fixture inside the harness work directory');
  rmSync(baseline, { recursive: true, force: true });
  console.log('local baseline fixture removed after all measurements');
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
// The recovery phases run natively: the engine code is the same in both runtimes,
// and the read workloads above already hash equal across them.
checkpointPhases('native');
ipfsPhase();
crashPhases();
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
