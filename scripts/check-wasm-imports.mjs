#!/usr/bin/env node
/**
 * TOOLCHAIN GUARDRAIL — assert the EXACT import surface of every wasm artifact.
 *
 * This exists because of a specific, expensive defect (docs/STORAGE-DURABILITY.md
 * §2.2 and §4): the wasm builds silently had NO file I/O at all, every consumer
 * was forced in-memory, and nothing failed. A missing import meant I/O had
 * quietly gone to RAM; an unexpected import would have meant the filesystem had
 * quietly been re-routed through emscripten's half-mapped standalone FS. Both
 * are invisible at runtime until a node reboots and its data is gone.
 *
 * So both must FAIL THE BUILD:
 *   - the WASI surface may only ever be the six functions below;
 *   - the seven flatsql_io_* imports must ALL be present on engine artifacts;
 *   - no emscripten __syscall_* import may appear anywhere (no host satisfies
 *     them; the module would fail to instantiate under WasmEdge);
 *   - MODULE artifacts (space-data-module-sdk plugins) must have NONE of the
 *     flatsql_io imports — a module keeps the generic hook set, and growing
 *     private imports is a new host capability, which is an owner decision.
 *
 *   node scripts/check-wasm-imports.mjs [--dir cpp/build-wasm]
 */

import { readFileSync, existsSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const WASI_SIX = [
  'clock_time_get',
  'environ_get',
  'environ_sizes_get',
  'fd_read',
  'fd_write',
  'random_get',
];

const FLATSQL_IO_SEVEN = [
  'flatsql_io_close',
  'flatsql_io_open',
  'flatsql_io_read',
  'flatsql_io_size',
  'flatsql_io_sync',
  'flatsql_io_truncate',
  'flatsql_io_write',
];

/**
 * `engine` artifacts own durable storage and therefore carry the seven I/O
 * imports. `module` artifacts are SDK plugins and must not.
 */
const TARGETS = [
  { file: 'flatsql-wasi-noeh.wasm', role: 'engine', required: true },
  { file: 'flatsql-wasi.wasm', role: 'engine', required: true },
  { file: 'flatsql-sdn-node.wasm', role: 'module', required: false },
  { file: 'flatsql-spatial.wasm', role: 'module', required: false },
];

const args = process.argv.slice(2);
const dirIndex = args.indexOf('--dir');
// Default to the SHIPPED artifacts, not a build directory. Checking
// cpp/build-wasm first was itself an instance of this defect's family: a stale
// July build directory happily passed the gate while wasm/ shipped something
// else entirely. What consumers receive is what must be asserted; pass --dir to
// verify a fresh build before it is staged.
const searchDirs = dirIndex >= 0
  ? [resolve(args[dirIndex + 1])]
  : [join(ROOT, 'wasm'), join(ROOT, 'cpp/build-wasm')];

let failures = 0;
const fail = (msg) => {
  console.error(`  FAIL ${msg}`);
  failures++;
};
const pass = (msg) => console.log(`  ok   ${msg}`);

function locate(file) {
  for (const dir of searchDirs) {
    const candidate = join(dir, file);
    if (existsSync(candidate)) return candidate;
  }
  return null;
}

function importsOf(path) {
  const module = new WebAssembly.Module(readFileSync(path));
  return WebAssembly.Module.imports(module);
}

for (const target of TARGETS) {
  const path = locate(target.file);
  if (!path) {
    if (target.required) fail(`${target.file}: not found in ${searchDirs.join(', ')}`);
    else console.log(`  --   ${target.file}: not built, skipped`);
    continue;
  }

  console.log(`\n${target.file} (${target.role})`);
  const imports = importsOf(path);
  const wasi = imports.filter((i) => i.module === 'wasi_snapshot_preview1').map((i) => i.name).sort();
  const io = imports.filter((i) => i.name.startsWith('flatsql_io_')).map((i) => i.name).sort();
  const syscalls = imports.filter((i) => i.name.startsWith('__syscall_'));
  const unexpected = imports.filter(
    (i) => i.module !== 'wasi_snapshot_preview1' && !i.name.startsWith('flatsql_io_'),
  );

  // 1. The WASI surface of an ENGINE artifact is frozen. path_open/fd_seek/
  //    fd_pread appearing here would mean someone re-enabled an emscripten
  //    filesystem. Module artifacts are a different contract with a different
  //    owner, so their surface is reported, not asserted, here.
  if (target.role === 'engine') {
    if (JSON.stringify(wasi) !== JSON.stringify([...WASI_SIX].sort())) {
      fail(`WASI import set changed: expected [${WASI_SIX.sort()}], got [${wasi}]`);
    } else {
      pass('WASI surface is exactly the six permitted functions');
    }
  } else {
    console.log(`  --   WASI surface (not asserted for modules): [${wasi}]`);
  }

  // 2. Nothing emscripten-private may leak in: no host satisfies __syscall_*.
  if (syscalls.length) {
    fail(`emscripten __syscall_* imports present (no host satisfies these): ` +
      syscalls.map((i) => i.name).join(', '));
  } else {
    pass('no emscripten __syscall_* imports');
  }

  // 3. The I/O contract, per role.
  if (target.role === 'engine') {
    if (JSON.stringify(io) !== JSON.stringify(FLATSQL_IO_SEVEN)) {
      fail(`flatsql_io import set is wrong. expected [${FLATSQL_IO_SEVEN}], got [${io}]. ` +
        `A MISSING import means I/O silently went to RAM.`);
    } else {
      pass('all seven flatsql_io_* imports present');
    }
    const wrongModule = imports.filter(
      (i) => i.name.startsWith('flatsql_io_') && i.module !== 'env',
    );
    if (wrongModule.length) {
      fail(`flatsql_io imports must live in module "env" (browser and WASI hosts ` +
        `both bind there): ${wrongModule.map((i) => `${i.module}.${i.name}`).join(', ')}`);
    } else {
      pass('flatsql_io imports are all in module "env"');
    }
  } else {
    if (io.length) {
      fail(`MODULE artifact grew private imports [${io}] — a module keeps the ` +
        `generic hook set; new host capabilities are an owner decision`);
    } else {
      pass('module artifact carries no private I/O imports');
    }
  }

  if (unexpected.length) {
    if (target.role === 'engine') {
      fail(`unexpected imports: ${unexpected.map((i) => `${i.module}.${i.name}`).join(', ')}`);
    } else {
      // Reported, not failed: these artifacts predate this contract and belong
      // to another task. Silence would be worse than either.
      console.log(`  --   ${unexpected.length} non-WASI imports (module contract, ` +
        `not this gate): ${unexpected.slice(0, 4).map((i) => i.name).join(', ')}...`);
    }
  }
}

// The browser bundle minifies import names, so its surface is checked through
// the generated glue instead: the seven bridge functions must be present and no
// emscripten syscall filesystem may have been linked in.
const glue = locate('flatsql.js');
if (glue) {
  console.log('\nflatsql.js (browser glue)');
  const source = readFileSync(glue, 'utf8');
  const missing = FLATSQL_IO_SEVEN.filter((name) => !source.includes(name));
  if (missing.length) {
    fail(`browser glue is missing I/O bridge functions: ${missing.join(', ')}`);
  } else {
    pass('all seven flatsql_io_* bridge functions present in the glue');
  }
  // FILESYSTEM=0 leaves EMPTY `___syscall_*` stubs behind — harmless, and
  // present long before this work. What must never appear is a REAL filesystem:
  // WasmFS routes I/O to an in-memory backend that looks durable and is not,
  // and FORCE_FILESYSTEM emits imports no host satisfies. Both were measured
  // and rejected (docs/STORAGE-DURABILITY.md §3.5); this is the tripwire.
  const realFs = ['FS_createPreloadedFile', 'wasmFS$', 'createPreloadedFile', 'FS.mkdirTree']
    .filter((marker) => source.includes(marker));
  if (realFs.length) {
    fail(`browser glue links a real emscripten filesystem (${realFs.join(', ')}); ` +
      `FILESYSTEM=0 must stay and I/O must go through the seven imports`);
  } else {
    pass('no emscripten filesystem linked (FILESYSTEM=0 intact)');
  }
}

if (failures) {
  console.error(`\n${failures} import-surface check(s) FAILED`);
  process.exit(1);
}
console.log('\nImport surfaces are exactly as contracted.');
