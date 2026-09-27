// A19 golden vectors: BFBS from the published SDS schemas
// (spacedatastandards.org@1.226.0 npm) via the published flatc-wasm, records
// from a Space-Track GP archive day plus edge cases. Expected rows come from
// goref.go (extractIndexedFields, recordIndexArgs and recordSupersedeKey
// copied from sdn-server, published SDS Go bindings v1.226.0):
//   node genvec.mjs <sds>/schema <gp-day>.json.gz ..
//   go mod tidy && go build -o goref . && for t in OMM MPE CAT PNM RFB OEM; do
//     ./goref $t.fbs ../$t.frames > ../$t.expected.jsonl; done
import { FlatcRunner } from 'flatc-wasm/runner';
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';

const [,, schemaDir, gpFile, outDir] = process.argv;
const runner = await FlatcRunner.init();
const files = [];
for (const dir of fs.readdirSync(schemaDir)) {
  const f = path.join(schemaDir, dir, 'main.fbs');
  if (fs.existsSync(f)) files.push({ path: `/schema/${dir}/main.fbs`, data: fs.readFileSync(f, 'utf8') });
}
runner.mountFiles(files);
const fields = (t) => {
  const text = fs.readFileSync(path.join(schemaDir, t, 'main.fbs'), 'utf8');
  const m = text.match(new RegExp(`table ${t}\\s*{([\\s\\S]*?)\\n}`));
  const out = {};
  for (const line of m[1].split('\n')) {
    const mm = line.replace(/\/\/.*$/, '').match(/^\s*([A-Z_0-9]+)\s*:\s*([^;=\s]+)/);
    if (mm) out[mm[1]] = mm[2];
  }
  return out;
};
function run(args) {
  const r = runner.runCommand(args);
  if (r.code !== 0) throw new Error(args.join(' ') + ': ' + r.stderr);
}
const scalar = new Set(['double', 'float', 'uint32', 'uint', 'int', 'int32', 'uint64', 'int64', 'long', 'ulong', 'bool', 'byte', 'ubyte', 'short', 'ushort']);
function buildAll(t, records) {
  run(['-b', '--schema', '-o', `/out/${t}`, `/schema/${t}/main.fbs`]);
  fs.writeFileSync(path.join(outDir, `${t}.bfbs`), runner.readFile(`/out/${t}/main.bfbs`, { encoding: null }));
  const fl = fields(t);
  const frames = [];
  records.forEach((rec, i) => {
    const clean = t === 'OEM' ? rec : {};
    if (t !== 'OEM') for (const [k, v] of Object.entries(rec)) {
      if (v === null || !(k in fl)) continue;
      const ty = fl[k];
      if (ty === 'string') clean[k] = String(v);
      else if (scalar.has(ty)) clean[k] = Number(v);
      else if (typeof v === 'number') clean[k] = v;  // enums by value
    }
    runner.mountFile(`/in/${t}_${i}.json`, JSON.stringify(clean));
    run(['-b', '--size-prefixed', '-o', `/bin/${t}`, `/schema/${t}/main.fbs`, `/in/${t}_${i}.json`]);
    frames.push(Buffer.from(runner.readFile(`/bin/${t}/${t}_${i}.bin`, { encoding: null })));
  });
  fs.writeFileSync(path.join(outDir, `${t}.frames`), Buffer.concat(frames));
  console.log(t, frames.length, 'records');
}
const gp = JSON.parse(zlib.gunzipSync(fs.readFileSync(gpFile)).toString());
const omm = gp.filter((_, i) => i % Math.max(1, Math.floor(gp.length / 300)) === 0).slice(0, 300);
omm.push({ ...omm[0], EPOCH: undefined, CREATION_DATE: '2026-09-25T02:28:46' });
omm.push({ ...omm[1], EPOCH: '   ', CREATION_DATE: '2026-09-20' });
omm.push({ ...omm[2], EPOCH: 'not-a-date', CREATION_DATE: '2026-09-20' });
omm.push({ ...omm[3], EPOCH: '2026-09-24T08:00:53.989344Z' });
omm.push({ ...omm[4], EPOCH: '2026-09-24T08:00:53+02:00' });
omm.push({ ...omm[5], EPOCH: '2026-09-24 08:00:53' });
omm.push({ ...omm[6], EPOCH: '1714567890.5' });
omm.push({ ...omm[7], NORAD_CAT_ID: 0, OBJECT_ID: '  2026-001A \t' });
omm.push({ ...omm[8], EPOCH: '1969-12-31T23:59:59.5' });
omm.push({ ...omm[9], EPOCH: '2024-02-30' });
buildAll('OMM', omm.map((r) => Object.fromEntries(Object.entries(r).filter(([, v]) => v !== undefined))));
const mpe = [];
for (let i = 0; i < 40; i++) mpe.push({ ENTITY_ID: `E-${i}`, EPOCH: 1.7e9 + i * 1234.567, MEAN_MOTION: 15 + i / 100 });
mpe.push({ ENTITY_ID: 'NEG', EPOCH: -1.5 });
mpe.push({ ENTITY_ID: 'ZERO', EPOCH: 0 });
mpe.push({ ENTITY_ID: '  SPACED  ', EPOCH: 86399.999 });
mpe.push({ EPOCH: 1e9 });
buildAll('MPE', mpe);
const cat = [];
for (let i = 0; i < 40; i++) cat.push({ OBJECT_NAME: `OBJ ${i}`, OBJECT_ID: `2020-0${10 + i}A`, NORAD_CAT_ID: 40000 + i, OBJECT_TYPE: i % 4, OPS_STATUS_CODE: i % 8 });
cat.push({ OBJECT_ID: 'X1', OBJECT_TYPE: 9, OPS_STATUS_CODE: 12 });
cat.push({ OBJECT_ID: 'X2', OBJECT_TYPE: -3, OPS_STATUS_CODE: 3 });
cat.push({ OBJECT_NAME: 'NO NORAD', OBJECT_ID: ' 2026-900A ' });
cat.push({ OBJECT_ID: 'U1', NORAD_CAT_ID: 11, CATALOG_URI: 'https://example.invalid/cat', CATALOG_OBJECT_ID: ' 42 ' });
cat.push({ OBJECT_ID: 'U2', NORAD_CAT_ID: 12, CATALOG_URI: 'https://example.invalid/cat', CATALOG_OBJECT_ID: '  ' });
cat.push({ OBJECT_ID: 'U3', CATALOG_OBJECT_ID: '43' });
cat.push({ CATALOG_URI: ' urn:x ', CATALOG_OBJECT_ID: 'y' });
cat.push({ OBJECT_NAME: 'NOTHING' });
buildAll('CAT', cat);
buildAll('PNM', [{ FILE_ID: 'abc' }, { FILE_ID: '  padded  ' }, { FILE_ID: '' }, { FILE_NAME: 'x' }]);
buildAll('RFB', [{ NORAD_CAT_ID: 25544, ID_TRANSMITTER: 'T1', ID: 'I1' }, { NORAD_CAT_ID: 0, ID: 'ONLY-ID' }, { ID_TRANSMITTER: '  ', ID: 'FALLBACK' }, {}]);

const obj = (norad, id) => ({ OBJECT_NAME: 'SAT', ...(norad ? { NORAD_CAT_ID: norad } : {}), ...(id !== undefined ? { OBJECT_ID: id } : {}) });
const line = (e) => ({ EPOCH: e, X: 1, Y: 2, Z: 3, X_DOT: 0, Y_DOT: 0, Z_DOT: 0 });
buildAll('OEM', [
  { CREATION_DATE: '2026-01-01', EPHEMERIS_DATA_BLOCK: [{ OBJECT: obj(25544, '1998-067A'), START_TIME: '2026-09-24T00:00:00Z', EPHEMERIS_DATA_LINES: [line('2026-09-25T00:00:00')] }] },
  { EPHEMERIS_DATA_BLOCK: [{ OBJECT: obj(25544, ' 1998-067A '), EPHEMERIS_DATA_LINES: [line(' 2026-09-25T01:02:03.5 '), line('2026-09-26')] }] },
  { EPHEMERIS_DATA_BLOCK: [{ OBJECT: obj(0, '2026-500B'), START_TIME: '   ', EPHEMERIS_DATA_LINES: [line('2026-09-25 01:02:03')] }] },
  { EPHEMERIS_DATA_BLOCK: [{ OBJECT: obj(7, undefined), START_TIME: 'garbage', EPHEMERIS_DATA_LINES: [line('2026-09-25')] }] },
  { EPHEMERIS_DATA_BLOCK: [{ START_TIME: '2026-09-24T00:00:00Z', EPHEMERIS_DATA_LINES: [line('2026-09-25')] }] },
  { EPHEMERIS_DATA_BLOCK: [{ OBJECT: obj(99, 'A'), START_TIME: '2026-09-24T00:00:00Z' }, { OBJECT: obj(100, 'B'), START_TIME: '2027-01-01' }] },
  { EPHEMERIS_DATA_BLOCK: [] },
  { CREATION_DATE: '2026-01-01' },
  { EPHEMERIS_DATA_BLOCK: [{ OBJECT: obj(5, 'E'), EPHEMERIS_DATA_LINES: [] }] },
  { EPHEMERIS_DATA_BLOCK: [{ OBJECT: obj(6, 'F'), START_TIME: '1970-01-01T00:00:00Z', STEP_SIZE: 60, EPHEMERIS_DATA: [1, 2, 3, 4, 5, 6] }] },
]);
