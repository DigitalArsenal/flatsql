// Exercise both record envelopes through the public threaded WASM API.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { FlatcRunner } from 'flatc-wasm/runner';
const packageRoot = process.env.FLATSQL_TEST_PACKAGE;
const { FlatSQL } = await import(packageRoot ? pathToFileURL(path.join(packageRoot, 'wasm/index.js')) : '../wasm/index.js');
const converter = await FlatcRunner.init();
const schemas = {};
for (const code of ['GJN', 'QRP']) {
  const source = await fs.readFile(new URL(`../node_modules/spacedatastandards.org/schema/${code}/main.fbs`, import.meta.url), 'utf8');
  schemas[code] = { entry: `${code}.fbs`, files: { [`${code}.fbs`]: source } };
  converter.mountFile(`/schemas/${code}.fbs`, source);
}
converter.mountFile('/out/.keep', '');
const compiled = converter.runCommand(['--binary', '--schema', '-o', '/out', '/schemas/GJN.fbs']);
assert.equal(compiled.code, 0, compiled.stderr);
const bfbs = converter.readFile('/out/GJN.bfbs');
const encode = (code, value) => converter.generateBinary(schemas[code], JSON.stringify(value), { sizePrefix: true, strictJson: true });
const record = encode('GJN', { FEATURES: [{ ID: 'violet comet beacon', HAS_GEOMETRY: false }] });
const framed = new Uint8Array(record.length + 4);
new DataView(framed.buffer).setUint32(0, record.length, true);
framed.set(record, 4);
function rows(stream) {
  const out = [];
  for (let at = 0; at < stream.length;) {
    const size = new DataView(stream.buffer, stream.byteOffset + at, 4).getUint32(0, true);
    const page = JSON.parse(converter.generateJSON(schemas.QRP,
      { path: 'page.bin', data: stream.slice(at + 4, at + 4 + size) }, { defaultsJson: true }));
    out.push(...(page.ROWS ?? []).map(row => row.CELLS));
    at += size + 4;
  }
  return out;
}
const root = await fs.mkdtemp(path.join(os.tmpdir(), 'flatsql-fts-envelopes-'));
let db;
try {
  db = await FlatSQL.open({ root });
  await db.registerSchema({ name: 'GJN', fileIdentifier: '$GJN', bfbs, fullText: true });
  const cids = [];
  for (const frame of [record, framed]) {
    const receipt = rows(await db.ingest('GJN', frame));
    assert.equal(receipt.length, 1);
    // Compare search identities with the ingested records through SQL below.
    cids.push(rows(await db.execute(encode('QRP', { SQL: 'SELECT _cid FROM GJN ORDER BY _rowid DESC LIMIT 1' })))[0][0].TEXT_VALUE);
  }
  assert.notEqual(cids[0], cids[1]);
  const search = async term => {
    const deadline = Date.now() + 10000;
    for (;;) {
      try {
        return rows(await db.execute(encode('QRP', {
          SQL: 'SELECT _cid FROM GJN WHERE _search MATCH ?1',
          PARAMETERS: [{ KIND: 'Text', TEXT_VALUE: term }],
        }))).map(row => row[0].TEXT_VALUE);
      } catch (error) {
        if (![-6, -9].includes(error.status) || Date.now() >= deadline) throw error;
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    }
  };
  const found = await search('violet');
  for (let i = 0; i < cids.length; ++i)
    console.log(`envelope=${i ? 'prefixed' : 'bare'} matches=${found.filter(cid => cid === cids[i]).length}`);
  assert.deepEqual(found.sort(), [...cids].sort());
  assert.deepEqual((await search('"violet comet"')).sort(), [...cids].sort());
  assert.deepEqual(await search('absentword'), []);
  await db.close();
  db = await FlatSQL.open({ root });
  assert.deepEqual((await search('violet')).sort(), [...cids].sort());
  await db.remove('GJN', framed);
  assert.deepEqual(await search('violet'), [cids[0]]);
  await db.remove('GJN', record);
  assert.deepEqual(await search('violet'), []);
  console.log('PASS: full-text record envelopes, phrase, absence, reopen, and removal');
} finally {
  await db?.close();
  await fs.rm(root, { recursive: true, force: true });
}
