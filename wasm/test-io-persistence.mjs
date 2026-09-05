/**
 * The wasm half of the durability matrix: the REAL artifact, instantiated
 * through the standalone shim, driven over the seven-import host I/O contract.
 *
 * cpp/test/state_persistence_test.cpp runs these exact scenarios natively. This
 * file runs them in wasm against three different backends. If any lane
 * disagrees with any other, that is a defect in the shim — the engine is one
 * binary and cannot be "different in the browser".
 *
 *   node wasm/test-io-persistence.mjs
 */

import { readFileSync } from 'node:fs';
import * as nodeFs from 'node:fs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { loadFlatSQLStandalone } from './standalone.js';
import {
  createMemoryBackend,
  createNodeFsBackend,
  createChunkedStoreBackend,
} from './flatsql-io.js';

const WASM_PATH = fileURLToPath(new URL('./flatsql-wasi-noeh.wasm', import.meta.url));

let failures = 0;
const check = (cond, what) => {
  if (cond) {
    console.log(`    ok   ${what}`);
  } else {
    console.error(`    FAIL ${what}`);
    failures++;
  }
};

const SCHEMA = `
  table omm {
    NORAD_CAT_ID: int (key);
    OBJECT_NAME: string;
  }
`;

/** Size-prefixed SDS-shaped FlatBuffer frames: [u32 size][buffer], id at 4..7. */
function makeStream(firstId, count) {
  const frames = [];
  for (let i = 0; i < count; i++) {
    const name = new TextEncoder().encode(`SAT-${firstId + i}`);
    // Root at 16, vtable at 8, two fields, then a terminated string. Exercise
    // real field extraction and B-tree writes, not just opaque frame counts.
    const body = new Uint8Array((33 + name.length + 3) & ~3);
    const view = new DataView(body.buffer);
    view.setUint32(0, 16, true);
    body.set(new TextEncoder().encode('OMM '), 4);
    view.setUint16(8, 8, true);
    view.setUint16(10, 12, true);
    view.setUint16(12, 4, true);
    view.setUint16(14, 8, true);
    view.setInt32(16, 8, true);
    view.setInt32(20, firstId + i, true);
    view.setUint32(24, 4, true);
    view.setUint32(28, name.length, true);
    body.set(name, 32);
    const frame = new Uint8Array(4 + body.length);
    new DataView(frame.buffer).setUint32(0, body.length, true);
    frame.set(body, 4);
    frames.push(frame);
  }
  const total = frames.reduce((n, f) => n + f.length, 0);
  const stream = new Uint8Array(total);
  let at = 0;
  for (const f of frames) {
    stream.set(f, at);
    at += f.length;
  }
  return stream;
}

/** Minimal in-memory key->bytes store: the exact shape sdn-js stores expose. */
function makeKeyValueStore() {
  const map = new Map();
  return {
    map,
    available: true,
    async readBytes(key) {
      return map.has(key) ? new Uint8Array(map.get(key)) : null;
    },
    async writeBytes(key, bytes) {
      map.set(key, new Uint8Array(bytes));
    },
    async deleteKey(key) {
      map.delete(key);
    },
  };
}

// Two loaders, one contract. `engineFactory` is swapped so the ENTIRE matrix
// below runs against the standalone/WASI artifact and against the emscripten
// browser bundle without a single assertion changing. That is the only way a
// lane-specific defect gets caught: the browser bundle once spun forever here,
// because with FILESYSTEM=0 emscripten's open() stub returns fd 0 and sqlite's
// vendored unix VFS loops on any fd below 3. Nothing had ever reached that code
// while the builds had no file I/O at all.
let engineFactory = null;

async function openEngine(backend) {
  return engineFactory(backend);
}

async function standaloneEngine(backend) {
  return loadFlatSQLStandalone({
    bytes: readFileSync(WASM_PATH),
    io: backend,
  });
}

async function browserEngine(backend) {
  const { initFlatSQL } = await import('./index.js');
  const flatsql = await initFlatSQL({ skipIntegrityCheck: true, io: backend });
  // The browser bundle's FlatSQL class already exposes createDatabase /
  // openDatabase with the same names and the same semantics.
  return flatsql;
}

/**
 * One backend, the full matrix. Everything here is backend-agnostic on purpose:
 * a lane that needs special-casing has already diverged.
 */
async function runMatrix(label, backend, dbPath) {
  console.log(`\n--- backend: ${label} ---`);
  const stream = makeStream(25544, 25);
  const late = makeStream(40000, 5);

  // 1. ingest -> flush -> TEAR DOWN
  let markAfterFlush = 0;
  let countBefore = 0;
  {
    await backend.hydrate(dbPath);
    await backend.hydrate(`${dbPath}.fsdata`);
    const engine = await openEngine(backend);
    const db = engine.openDatabase(SCHEMA, 'sds', dbPath, 2);
    check(db.isDiskBacked(), 'reports disk-backed');

    db.registerFileId('OMM ', 'omm');
    db.ingest(stream);
    countBefore = db.query('SELECT COUNT(*) AS n FROM omm').rows[0][0];
    check(Number(countBefore) === 25, `25 records visible pre-teardown (got ${countBefore})`);

    check(db.flushIndex() === 0, 'flushIndex returns 0');
    markAfterFlush = db.flushedOffset();
    check(markAfterFlush === stream.length,
      `high-water mark equals stream length (${markAfterFlush} vs ${stream.length})`);

    db.destroy();
    await backend.flush(); // durability barrier resolves here for async stores
  }

  // 2. reopen from the backend -> byte-identical
  {
    await backend.hydrate(dbPath);
    await backend.hydrate(`${dbPath}.fsdata`);
    const engine = await openEngine(backend);
    const db = engine.openDatabase(SCHEMA, 'sds', dbPath, 2);
    db.registerFileId('OMM ', 'omm');

    const restored = db.openState();
    check(restored === 25, `openState replays 25 records (got ${restored})`);
    check(db.flushedOffset() === markAfterFlush, 'high-water mark survived teardown');

    const after = db.query('SELECT COUNT(*) AS n FROM omm').rows[0][0];
    check(String(after) === String(countBefore),
      `query result identical after reopen (${after} vs ${countBefore})`);

    // 3. late append past the mark -> tail re-index
    db.ingest(late);
    check(db.flushIndex() === 0, 'second flush');
    check(db.flushedOffset() === stream.length + late.length,
      'mark advanced by exactly the appended bytes');
    db.destroy();
    await backend.flush();
  }

  {
    await backend.hydrate(dbPath);
    await backend.hydrate(`${dbPath}.fsdata`);
    const engine = await openEngine(backend);
    const db = engine.openDatabase(SCHEMA, 'sds', dbPath, 2);
    db.registerFileId('OMM ', 'omm');
    const restored = db.openState();
    check(restored === 30, `tail re-index picks up late appends (got ${restored})`);
    db.destroy();
  }

  // 4. corrupt derived state -> full re-derivation, never data loss
  {
    await backend.hydrate(dbPath);
    const engine = await openEngine(backend);
    const db = engine.openDatabase(SCHEMA, 'sds', dbPath, 2);
    db.registerFileId('OMM ', 'omm');
    const rebuilt = db.reindexAll();
    check(rebuilt === 30, `reindexAll re-derives everything from the stream (got ${rebuilt})`);
    check(db.reindexStep(7) === 1, 'bounded rebuild yields after seven records');
    let readError;
    try { db.query('SELECT COUNT(*) FROM omm'); } catch (error) { readError = error; }
    check(readError?.message.includes('state: reindex incomplete'), 'partial index is unavailable without trapping WASM');
    let writeError;
    try { db.ingest(makeStream(90000, 1)); } catch (error) { writeError = error; }
    check(writeError?.message.includes('state: reindex incomplete'), 'ingest is rejected without trapping WASM');
    check(db.flushIndex() < 0, 'partial rebuild cannot publish a checkpoint');
    let steps = 1;
    let rc;
    do { rc = db.reindexStep(7); ++steps; } while (rc === 1 && steps < 10);
    check(rc === 0 && steps === 5, 'same WASM instance resumes and completes five bounded steps');
    check(Number(db.query('SELECT COUNT(*) FROM omm').rows[0][0]) === 30, 'complete index contains all records and no rejected write');
    db.destroy();
    await backend.flush();
  }
}

/**
 * SOURCE PARTITIONS across a teardown (upstream-flatsql-3).
 *
 * This is Hermes's measurement, verbatim: two sources, 60 and 20 records,
 * flush, tear down, reopen — and NOTHING is re-registered on the way back.
 * On 1.4.4 the partitions came back 0/0 and the persisted unified view
 * answered "no such module: __flatsql_module_omm_alpha", which is what kept
 * the browser lane on snapshot exports instead of disk-backed state.
 */
async function runSourceMatrix(label, backend, dbPath) {
  console.log(`\n--- source partitions, backend: ${label} ---`);
  const alpha = makeStream(25544, 60);
  const beta = makeStream(40000, 20);
  const count = (db, sql) => Number(db.query(sql).rows[0][0]);

  {
    await backend.hydrate(dbPath);
    await backend.hydrate(`${dbPath}.fsdata`);
    const engine = await openEngine(backend);
    const db = engine.openDatabase(SCHEMA, 'sds', dbPath, 2);
    db.registerFileId('OMM ', 'omm');
    db.registerSource('alpha');
    db.registerSource('beta');
    db.ingest(alpha, 'alpha');
    db.ingest(beta, 'beta');
    db.createUnifiedViews();

    check(count(db, 'SELECT COUNT(*) AS n FROM "omm@alpha"') === 60,
      'alpha holds 60 records pre-teardown');
    check(count(db, 'SELECT COUNT(*) AS n FROM "omm@beta"') === 20,
      'beta holds 20 records pre-teardown');
    check(count(db, 'SELECT COUNT(*) AS n FROM omm') === 80,
      'unified view sees 80 pre-teardown');

    check(db.flushIndex() === 0, 'flush with sources returns 0');
    db.destroy();
    await backend.flush();
  }

  {
    await backend.hydrate(dbPath);
    await backend.hydrate(`${dbPath}.fsdata`);
    const engine = await openEngine(backend);
    const db = engine.openDatabase(SCHEMA, 'sds', dbPath, 2);
    db.registerFileId('OMM ', 'omm');
    // No registerSource. No createUnifiedViews. That is the point.
    const restored = db.openState();
    check(restored === 80, `openState replays all 80 records (got ${restored})`);
    check(db.listSources().join(',') === 'alpha,beta',
      `sources come back in registration order (got ${db.listSources().join(',')})`);
    check(count(db, 'SELECT COUNT(*) AS n FROM "omm@alpha"') === 60,
      'alpha still holds 60 records after reopen');
    check(count(db, 'SELECT COUNT(*) AS n FROM "omm@beta"') === 20,
      'beta still holds 20 records after reopen');
    check(count(db, 'SELECT COUNT(*) AS n FROM omm') === 80,
      'the persisted unified view answers WITHOUT re-registration');
    check(count(db, "SELECT COUNT(*) AS n FROM omm WHERE _source='omm@alpha'") === 60,
      '_source filter still selects alpha alone');
    db.destroy();
    await backend.flush();
  }
}

/**
 * The ephemeral engine is NOT skipped. Its documented behaviour — no
 * filesystem, so state calls report -5 and the caller derives fresh — is
 * asserted, because that fallback is a real production path.
 */
async function runEphemeral() {
  console.log('\n--- backend: none (ephemeral :memory:) ---');
  const engine = await openEngine(createMemoryBackend());
  const db = engine.createDatabase(SCHEMA, 'sds');
  db.registerFileId('OMM ', 'omm');
  db.ingest(makeStream(25544, 3));

  check(!db.isDiskBacked(), 'ephemeral engine reports NOT disk-backed');
  check(db.openState() === -5, 'openState reports -5 (no filesystem)');
  check(db.flushState === undefined || db.flushIndex() === -5, 'flushIndex reports -5');
  const n = db.query('SELECT COUNT(*) AS n FROM omm').rows[0][0];
  check(Number(n) === 3, 'ephemeral ingest and query still work');
  db.destroy();
}

const tmp = mkdtempSync(join(tmpdir(), 'flatsql-io-'));
try {
  // ---- lane 1: the standalone / WASI artifact (the server's binary) --------
  engineFactory = standaloneEngine;
  console.log('\n================ artifact: flatsql-wasi-noeh.wasm ================');
  await runMatrix('memory', createMemoryBackend(), 'sds.db');
  await runMatrix('node-fs', createNodeFsBackend(nodeFs, { root: tmp }), 'sds.db');
  await runMatrix(
    'chunked key->bytes store (the sdn-js shape)',
    createChunkedStoreBackend(makeKeyValueStore(), { chunkBytes: 4096 }),
    'sds.db',
  );
  await runSourceMatrix('node-fs', createNodeFsBackend(nodeFs, { root: tmp }), 'sources.db');
  await runSourceMatrix(
    'chunked key->bytes store (the sdn-js shape)',
    createChunkedStoreBackend(makeKeyValueStore(), { chunkBytes: 4096 }),
    'sources.db',
  );
  await runEphemeral();

  // ---- lane 2: the emscripten browser bundle, SAME assertions -------------
  // initFlatSQL rebinds its module-level wasm instance on every call, so the
  // browser lane runs last and once.
  engineFactory = browserEngine;
  console.log('\n================ artifact: flatsql.js + flatsql.wasm ================');
  await runMatrix(
    'chunked key->bytes store (browser bundle)',
    createChunkedStoreBackend(makeKeyValueStore(), { chunkBytes: 4096 }),
    'browser.db',
  );
  await runSourceMatrix(
    'chunked key->bytes store (browser bundle)',
    createChunkedStoreBackend(makeKeyValueStore(), { chunkBytes: 4096 }),
    'browser-sources.db',
  );
} finally {
  rmSync(tmp, { recursive: true, force: true });
}

if (failures) {
  console.error(`\n${failures} check(s) FAILED`);
  process.exit(1);
}
console.log('\nAll wasm I/O persistence checks passed.');
