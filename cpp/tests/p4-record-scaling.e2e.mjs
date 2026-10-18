#!/usr/bin/env node
// E2E for flatsql-record-whole-map-copy-20261017: a record's write cost on a
// 15 GB WXF feed is the cost on a small one.
//
// Builds the native format-4 engine with its test client (cpp/tests/p4), then
// runs p4_record_scaling on a fresh store: one process builds the large feed
// through the migration path (P4_SCALING_BYTES of stream, default 15 GB) and a
// small feed of the same lane shape; the next (--accept=1) times one GFS
// lead's 512 tile PUTs and retention's 500-record pages on each, then pages on
// the large feed while a lead writes to each feed, and checks every record
// written and deleted, the instance summary across a reopen and REBUILD 8. It
// passes when the large feed's median retention page (SCAN + DELETE) takes at
// most 0.2 s, alone and while both lanes write, its per-record PUT cost is
// within 2x of the small feed's and its DELETE page within 4x. The store is
// removed afterwards.
//
// Environment: FLATBUFFERS_DIR (else the checkout's sibling flatbuffers),
// P4_SCALING_BYTES, P4_SCALING_DIR (a directory for the store; default the
// system temp directory), P4_SCALING_BUILD (the CMake build directory).
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, mkdtempSync, rmSync, statfsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const cpp = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repo = resolve(cpp, '..');
const bytes = Number(process.env.P4_SCALING_BYTES || 15e9);

function run(cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, { stdio: 'inherit', ...opts });
  if (r.status !== 0) {
    console.error(`FAIL: ${cmd} ${args.join(' ')} exited ${r.status ?? r.signal}`);
    process.exit(1);
  }
}

// The repository's convention (scripts/build-wasm.sh): FLATBUFFERS_DIR, else
// the sibling checkout.
function flatbuffers() {
  const dir = resolve(process.env.FLATBUFFERS_DIR || join(repo, '..', 'flatbuffers'));
  if (!existsSync(join(dir, 'include/flatbuffers/flatbuffers.h'))) {
    console.error(`FAIL: FlatBuffers not found at ${dir} (set FLATBUFFERS_DIR)`);
    process.exit(1);
  }
  return dir;
}

const build =
  process.env.P4_SCALING_BUILD ||
  join(tmpdir(), `flatsql-p4-scaling-build-${createHash('sha256').update(repo).digest('hex').slice(0, 12)}`);
mkdirSync(build, { recursive: true });
run('cmake', ['-S', cpp, '-B', build, '-DCMAKE_BUILD_TYPE=Release', `-DFLATBUFFERS_DIR=${flatbuffers()}`], {
  stdio: ['ignore', 'ignore', 'inherit'],
});
run('cmake', ['--build', build, '--target', 'flatsql_p4_test', '-j', '8']);

const parent = process.env.P4_SCALING_DIR || tmpdir();
const fs = statfsSync(parent);
const free = Number(fs.bavail) * Number(fs.bsize);
if (free < bytes * 1.15) {
  console.error(`FAIL: ${parent} has ${(free / 1e9).toFixed(1)} GB free; the fixture needs ${((bytes * 1.15) / 1e9).toFixed(1)} GB`);
  process.exit(1);
}
const store = mkdtempSync(join(parent, 'flatsql-p4-scaling-'));
const test = join(build, 'flatsql_p4_test');
const common = ['--test=p4_record_scaling', `--store=${store}`, `--bytes=${Math.round(bytes)}`];
let status = 1;
try {
  // The fixture in one process, the measured work in the next: a node opens
  // a store that is there.
  const fixture = spawnSync(test, [...common, '--build-only=1'], { stdio: 'inherit' });
  status = fixture.status ?? 1;
  if (status === 0) status = spawnSync(test, [...common, '--reuse=1', '--accept=1'], { stdio: 'inherit' }).status ?? 1;
} finally {
  rmSync(store, { recursive: true, force: true });
}
console.log(status === 0 ? 'PASS: p4 record scaling' : 'FAIL: p4 record scaling');
process.exit(status);
