// End to end: an engine whose host cannot give it every thread does not abort.
//
// flatsql_start creates the engine's threads (cpp/src/p4/mailbox.cpp
// startThreads). The WASM build has no exceptions, so a thread that std::thread
// could not create used to end the instance: "RuntimeError: unreachable" at
// __throw_system_error <- flatsql::p4::startThreads <- flatsql_start. Now the
// threads are created first and run only when all exist; if one cannot be
// created, the others are sent home and flatsql_start returns FLATSQL_E_BUSY.
//
// The Node host's thread pool is capped below what the engine starts
// (maxThreads), and a spawn it cannot serve is declined after 100 ms. Expected:
// open() rejects with FlatSQLError status -6 and its message, the host has no
// fault, and the same store then opens with an uncapped host and answers a
// request.
// Usage: node scripts/ci-engine-thread-start-e2e.mjs
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

const { FlatSQL, FlatSQLError } = await import('../wasm/index.js');
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'flatsql-thread-start-'));
try {
  const began = performance.now();
  let failure;
  try {
    const db = await FlatSQL.open({ root, maxThreads: 4, spawnWaitMs: 100 });
    await db.close();
  } catch (error) { failure = error; }
  const ms = Math.round(performance.now() - began);
  assert.ok(failure, 'open succeeded with fewer threads than the engine starts');
  assert.ok(failure instanceof FlatSQLError, `expected FlatSQLError, got ${failure?.name}: ${failure?.message}`);
  assert.equal(failure.status, -6, failure.message);
  assert.match(failure.message, /could not create its engine threads/);
  assert.doesNotMatch(String(failure.stack), /unreachable|__throw_system_error/);
  console.log(`capped host: FlatSQLError ${failure.status} after ${ms} ms: ${failure.message}`);

  const db = await FlatSQL.open({ root });
  const surface = await db.surface();
  assert.ok(surface.length > 0, 'the engine did not answer');
  await db.close();
  console.log(`uncapped host, same store: started, SURFACE ${surface.length} bytes`);
  console.log('PASS: a host without enough threads gets FLATSQL_E_BUSY from flatsql_start, not an abort');
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
