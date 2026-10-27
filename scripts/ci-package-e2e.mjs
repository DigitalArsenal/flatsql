import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import assert from 'node:assert/strict';
const root = process.argv[2] ? path.resolve(process.argv[2]) : fileURLToPath(new URL('../', import.meta.url));
const pkg = JSON.parse(fs.readFileSync(path.join(root, 'package.json')));
const wasmDir = path.join(root, 'wasm');
const integrity = JSON.parse(fs.readFileSync(path.join(wasmDir, 'integrity.json')));
const wasm = fs.readFileSync(path.join(wasmDir, 'flatsql.wasm'));
assert.equal(createHash('sha384').update(wasm).digest('base64'), integrity.hash);
for (const [name, spec] of Object.entries(integrity.files)) {
 const bytes = fs.readFileSync(path.join(wasmDir, name));
 assert.equal(bytes.length, spec.size, name);
 assert.equal(createHash('sha256').update(bytes).digest('hex'), spec.sha256, name);
}
const server = http.createServer((req, res) => {
 if (req.url === '/flatsql.wasm') { res.setHeader('content-type', 'application/wasm'); res.end(wasm); }
 else if (req.url === '/integrity.json') { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(integrity)); }
 else { res.statusCode = 404; res.end(); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
let db;
try {
 const { initFlatSQL } = await import(pathToFileURL(path.join(root, pkg.main)).href);
 const sql = await initFlatSQL({ wasmPath: `http://127.0.0.1:${server.address().port}`, requireIntegrity: true });
 db = sql.createDatabase('table Sample { value:int; } root_type Sample; file_identifier "SMPL";', 'published-release-smoke');
 db.registerFileId('SMPL', 'Sample');
 // One raw FlatBuffer: root at 16, identifier at 4, vtable at 8, int field at 20.
 const record = new Uint8Array(24);
 const view = new DataView(record.buffer);
 view.setUint32(0, 16, true);
 record.set(new TextEncoder().encode('SMPL'), 4);
 view.setUint16(8, 6, true); view.setUint16(10, 8, true); view.setUint16(12, 4, true);
 view.setInt32(16, 8, true); view.setInt32(20, 42, true);
 db.ingestBuffers([record]);
 assert.deepEqual(db.query('SELECT value FROM Sample').rows, [[42]]);
 const geo = db.query('SELECT geo_distance(0,0,0,1), geo_bbox_contains(-1,1,-1,1,0,0), geo_within_radius(0,0,0,1,112), geo_distance(NULL,0,0,1)').rows[0];
 assert.ok(Math.abs(geo[0] - 111.19492664455873) < 1e-6);
 assert.equal(Number(geo[1]), 1); assert.equal(Number(geo[2]), 1); assert.equal(geo[3], null);
 console.log(JSON.stringify({ version: pkg.version, integrity: 'PASS', artifactCount: new Set(['flatsql.wasm', ...Object.keys(integrity.files)]).size, binaryIngestAndQuery: 'PASS', geospatialQueries: 'PASS' }));
} finally {
 db?.destroy();
 await new Promise(resolve => server.close(resolve));
}
