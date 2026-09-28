// Arena capacity cap through the shipped artifacts (the browser bundle and the
// no-exceptions WASI engine the node runs): past the cap, flatsql_ingest_one*
// answer -1 with "arena capacity exhausted" and the instance keeps working
// (no trap in the arena's resize).
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import FlatSQLModule from './flatsql.js';
import { loadFlatSQLStandalone } from './standalone.js';

const schema = `
  table User {
    id: int (id);
    name: string;
    email: string (key);
    age: int;
  }
  root_type User;
`;

const Module = await FlatSQLModule();
const api = {
  create: Module.cwrap('flatsql_create_db', 'number', ['string', 'string']),
  destroy: Module.cwrap('flatsql_destroy_db', null, ['number']),
  registerFileId: Module.cwrap('flatsql_register_file_id', null, ['number', 'string', 'string']),
  setArenaLimit: Module.cwrap('flatsql_set_arena_limit', null, ['number', 'number']),
  ingestOne: Module.cwrap('flatsql_ingest_one', 'number', ['number', 'number', 'number']),
  ingestOneWithSource: Module.cwrap('flatsql_ingest_one_with_source', 'number', ['number', 'number', 'number', 'string']),
  getError: Module.cwrap('flatsql_get_error', 'string', []),
  createTestUser: Module.cwrap('flatsql_create_test_user', 'number', ['number', 'string', 'string', 'number']),
  testBufferSize: Module.cwrap('flatsql_test_buffer_size', 'number', []),
};

function ingestUser(db, id, source) {
  const ptr = api.createTestUser(id, `user-${id}`, `u${id}@example.invalid`, 30);
  const size = api.testBufferSize();
  return source === undefined ? api.ingestOne(db, ptr, size) : api.ingestOneWithSource(db, ptr, size, source);
}

const db = api.create(schema, 'arena-limit');
assert.ok(db, 'create database');
api.registerFileId(db, 'USER', 'User');
api.setArenaLimit(db, 8192);

let accepted = 0;
let id = 1;
for (; id <= 1000; id++) {
  if (ingestUser(db, id) < 0) break;
  accepted++;
}
assert.ok(accepted > 0, 'records under the cap are accepted');
assert.ok(id <= 1000, 'an ingest past the cap is refused');
assert.equal(api.getError(), 'arena capacity exhausted');
assert.equal(ingestUser(db, 5000), -1);
assert.equal(ingestUser(db, 5001, 'src'), -1);
assert.equal(api.getError(), 'arena capacity exhausted');

// The instance is intact: a higher cap accepts again.
api.setArenaLimit(db, 16 * 1024 * 1024);
assert.ok(ingestUser(db, 6000) >= 0, 'ingest resumes under a higher cap');
api.destroy(db);

console.log(`browser bundle: ${accepted} records under an 8 KiB cap; refusals answered -1 without a trap`);

// The WASI engine (flatsql-wasi-noeh.wasm), same records.
function userBytes(i) {
  const ptr = api.createTestUser(i, `user-${i}`, `u${i}@example.invalid`, 30);
  return new Uint8Array(Module.HEAPU8.subarray(ptr, ptr + api.testBufferSize()));
}
const wasmPath = fileURLToPath(new URL('./flatsql-wasi-noeh.wasm', import.meta.url));
const engine = await loadFlatSQLStandalone({ bytes: readFileSync(wasmPath) });
const wdb = engine.openDatabase(schema, 'arena-limit-wasi', '', 2);
wdb.registerFileId('USER', 'User');
const rt = wdb._runtime;
rt.exports.flatsql_set_arena_limit(wdb._handle, 8192);
let wasiAccepted = 0;
let refusal = null;
for (let i = 1; i <= 1000 && !refusal; i++) {
  try {
    wdb.ingestOne(userBytes(i));
    wasiAccepted++;
  } catch (err) {
    refusal = err;
  }
}
assert.ok(wasiAccepted > 0, 'WASI: records under the cap are accepted');
assert.ok(refusal, 'WASI: an ingest past the cap is refused');
assert.equal(refusal.message, 'arena capacity exhausted');
assert.throws(() => wdb.ingestOne(userBytes(7000), 'src'), /arena capacity exhausted/);
rt.exports.flatsql_set_arena_limit(wdb._handle, 16 * 1024 * 1024);
assert.ok(wdb.ingestOne(userBytes(8000)) >= 0, 'WASI: ingest resumes under a higher cap');
wdb.destroy();
console.log(`WASI engine: ${wasiAccepted} records under an 8 KiB cap; refusals answered -1 without a trap`);
console.log('All arena limit checks passed');
