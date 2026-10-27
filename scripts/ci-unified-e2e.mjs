// End to end: an external converter feeds the public API; the real threaded
// WASM engine persists and queries binary records, and SDS decodes the output.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { FlatcRunner } from 'flatc-wasm/runner';
import { spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';
const packageRoot=process.env.FLATSQL_TEST_PACKAGE;
const {FlatSQL,FlatSQLError}=await import(packageRoot ? pathToFileURL(path.join(packageRoot,'wasm/index.js')) : '../wasm/index.js');
const began = performance.now();
const converter = await FlatcRunner.init();
const schema = {};
for (const code of ['GJN','QRP']) {
  const source = await fs.readFile(new URL(`../node_modules/spacedatastandards.org/schema/${code}/main.fbs`, import.meta.url), 'utf8');
  schema[code] = { entry: `${code}.fbs`, files: { [`${code}.fbs`]: source } };
  converter.mountFile(`/schemas/${code}.fbs`, source);
}
converter.mountFile('/out/.keep', '');
const compile = converter.runCommand(['--binary','--schema','-o','/out','/schemas/GJN.fbs']);
assert.equal(compile.code, 0, compile.stderr);
const bfbs = converter.readFile('/out/GJN.bfbs');
const position = (LONGITUDE, LATITUDE, ALTITUDE) => ({ LONGITUDE, LATITUDE, ...(ALTITUDE === undefined ? {} : {ALTITUDE, HAS_ALTITUDE: true}) });
const ring = (a,b) => ({ POSITIONS: [[a,a],[b,a],[b,b],[a,b],[a,a]].map(([x,y]) => position(x,y)) });
const gjn = { FEATURES: [
  { ID: 'polygon-with-hole', HAS_GEOMETRY: true, GEOMETRY: { TYPE: 'POLYGON', RINGS: [ring(0,10),ring(4,6)] },
    PROPERTIES: [{KEY:'nested',JSON_VALUE:'{"kept":[1,true,null]}'}, {KEY:'height',IS_NUMERIC:true,NUM_VALUE:400}] },
  { NUM_ID: 42, ID_IS_NUMERIC:true, HAS_GEOMETRY:true, GEOMETRY: {TYPE:'POINT',POINT:position(20,20,420)} },
  { ID: 'null', HAS_GEOMETRY: false, PROPERTIES_IS_NULL: true },
], BBOX: {WEST:0,SOUTH:0,EAST:20,NORTH:20,MIN_ALTITUDE:0,MAX_ALTITUDE:420,HAS_ALTITUDE:true} };
const record = converter.generateBinary(schema.GJN, JSON.stringify(gjn), {sizePrefix:true,unknownJson:false,strictJson:true});
const request = (sql, extra = {}) => converter.generateBinary(schema.QRP, JSON.stringify({SQL:sql,REPRESENTATION:'FlatBuffer',...extra}), {sizePrefix:true,unknownJson:false,strictJson:true});
function pages(stream) {
  const out=[];
  for(let at=0;at<stream.length;) {
    assert.ok(stream.length-at >=4);
    const n=new DataView(stream.buffer,stream.byteOffset+at,4).getUint32(0,true);
    assert.ok(n>=8 && n<=stream.length-at-4);
    const data=stream.slice(at,at+n+4);
    out.push(JSON.parse(converter.generateJSON(schema.QRP,{path:'page.bin',data},{defaultsJson:true})));
    at+=n+4;
  }
  assert.ok(out.length);
  return out;
}
const rows = stream => pages(stream).flatMap(page=>page.ROWS??[]).map(row=>row.CELLS);
const root=await fs.mkdtemp(path.join(os.tmpdir(),'flatsql-unified-e2e-'));
let db;
try {
  db=await FlatSQL.open({root});
  await db.registerSchema({name:'GJN',fileIdentifier:'$GJN',bfbs,fullText:true});
  await assert.rejects(db.registerSchema({name:'FlatSQL_Provenance',fileIdentifier:'$GJN',bfbs}), e=>e.status<0);
  await db.close();db=await FlatSQL.open({root});
  const ingest=rows(await db.ingest('GJN',record,{source:'customer',provider:'external-flatc',batch:'first',producer:'peer-A',timestamp:1700000000,signatures:[new Uint8Array([1,2,3,4])]}));
  assert.equal(ingest.length,1);
  assert.deepEqual(rows(await db.execute(request('SELECT _signature FROM GJN')))[0][0].BYTES_VALUE,[1,2,3,4]);
  const searchUntil = Date.now() + 10000;
  for (;;) {
    try {
      const found=rows(await db.execute(request('SELECT _rowid FROM GJN WHERE _search MATCH ?1',{PARAMETERS:[{KIND:'Text',TEXT_VALUE:'"polygon" AND "hole"'}]})));
      assert.equal(found.length,1);break;
    } catch(error) { if(![-6,-9].includes(error.status) || Date.now()>searchUntil)throw error; await new Promise(resolve=>setTimeout(resolve,10)); }
  }

  const projected=rows(await db.execute(request('SELECT geo_contains(_data,2,2),geo_contains(_data,5,5),geo_area_geom(_data),geo_envelope(_data) FROM GJN')));
  assert.equal(projected.length,1);
  assert.equal(projected[0][0].INT_VALUE,1); assert.equal(projected[0][1].INT_VALUE,0);
  assert.equal(projected[0][2].FLOAT_VALUE,96);
  const envelope=Uint8Array.from(projected[0][3].BYTES_VALUE);
  const converted=JSON.parse(converter.generateJSON(schema.GJN,{path:'geometry.bin',data:envelope},{defaultsJson:true}));
  assert.equal(converted.FEATURES[0].GEOMETRY.TYPE,'POLYGON');
  const exact=await db.execute(request('SELECT _data FROM GJN'),{records:true});
  assert.deepEqual(exact,record);
  const scalar=rows(await db.execute(request('SELECT geo_distance(0,0,0,1),geo_bbox_contains(-1,1,-1,1,0,0),geo_within_radius(0,0,0,1,112),geo_distance(NULL,0,0,1)')))[0];
  assert.ok(Math.abs(scalar[0].FLOAT_VALUE-111.194926644559)<1e-6);
  assert.equal(scalar[1].INT_VALUE,1);assert.equal(scalar[2].INT_VALUE,1);assert.equal(scalar[3].KIND,'Null');
  const binaryGeom=GEOMETRY=>converter.generateBinary(schema.GJN,JSON.stringify({FEATURES:[{HAS_GEOMETRY:true,GEOMETRY}]}),{sizePrefix:true,strictJson:true});
  const value=record=>({KIND:'Bytes',BYTES_VALUE:Array.from(record)});
  const polygons=[binaryGeom({TYPE:'POLYGON',RINGS:[ring(0,2)]}),binaryGeom({TYPE:'POLYGON',RINGS:[{POSITIONS:ring(1,3).POSITIONS.reverse()}]})];
  const areas=rows(await db.execute(request('SELECT geo_area_geom(geo_union(?1,?2)),geo_area_geom(geo_intersection(?1,?2)),geo_area_geom(geo_difference(?1,?2)),geo_area_geom(geo_sym_difference(?1,?2))',{PARAMETERS:polygons.map(value)})))[0];
  assert.deepEqual(areas.map(c=>c.FLOAT_VALUE),[7,1,3,6]);
  await assert.rejects(db.execute(request('SELECT geo_union(?1,?1)',{PARAMETERS:[value(record)]})),e=>e.status===-13);
  const generated=rows(await db.execute(request('SELECT geo_destination(0,0,90,111.194926644559),geo_midpoint(0,0,0,2),geo_from_ecef(6378137,0,0),geo_to_ecef_x(0,0,0)')))[0];
  for(let i=0;i<3;i++) {
    const point=JSON.parse(converter.generateJSON(schema.GJN,{path:'generated.bin',data:Uint8Array.from(generated[i].BYTES_VALUE)},{defaultsJson:true})).FEATURES[0].GEOMETRY.POINT;
    assert.ok(Math.abs(point.LONGITUDE-(i===2?0:1))<1e-8);
    assert.ok(Math.abs(point.LATITUDE)<1e-8);assert.equal(point.HAS_ALTITUDE,i===2);
  }
  assert.equal(generated[3].FLOAT_VALUE,6378137);
  const bound=rows(await db.execute(request('SELECT geo_contains(?1,2,2),?2,?3',{PARAMETERS:[
    {KIND:'Bytes',BYTES_VALUE:Array.from(record)}, {KIND:'Text',TEXT_VALUE:'parameter'}, {KIND:'Bool',BOOL_VALUE:true}]})))[0];
  assert.equal(bound[0].INT_VALUE,1);assert.equal(bound[1].TEXT_VALUE,'parameter');assert.equal(bound[2].INT_VALUE,1);
  for(const sql of ["SELECT geo_contains('POINT(2 2)',2,2)","SELECT geo_from_geojson('{}')","SELECT json('{}')"])
    await assert.rejects(db.execute(request(sql)),e=>e instanceof FlatSQLError && e.status===-13);
  await assert.rejects(db.execute(request('SELECT geo_contains(?1,2,2)',{PARAMETERS:[{KIND:'Bytes',BYTES_VALUE:[1,2,3]}]})),e=>e.status===-13);
  assert.throws(()=>db.query({SQL:'SELECT 1'}),TypeError);
  await assert.rejects(db.execute(request('WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000) SELECT x FROM n',{MAX_ROWS:3})),e=>e.status===-8);
  await assert.rejects(db.execute(request('WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n) SELECT sum(x) FROM n',{MAX_ELAPSED_MS:5})),e=>e.status===-8);
  const controller=new AbortController();
  const pending=db.execute(request('WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n) SELECT sum(x) FROM n'),{signal:controller.signal});
  setTimeout(()=>controller.abort(),10);
  await assert.rejects(pending,e=>e.status===-7);
  // Breaking a result stream releases its lane and slot after cancellation.
  for await(const _ of db.query(request('WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000) SELECT x FROM n'))) break;
  await db.close(); db=null;
  db=await FlatSQL.open({root});
  assert.deepEqual(await db.execute(request('SELECT _data FROM GJN'),{records:true}),record);
  assert.equal(rows(await db.execute(request('SELECT COUNT(*) FROM "GJN@customer"')))[0][0].INT_VALUE,1);
  // One content record can have several independent copies and source tags.
  // Read all of them through the same SQL/QRP interface, including after reopen.
  await db.ingest('GJN',record,{source:'customer',provider:'second-provider',batch:'second',producer:'peer-B',timestamp:1700000001,signatures:[Uint8Array.of(8,9)]});
  await db.ingest('GJN',record,{source:'archive',provider:'second-provider',batch:'third',producer:'peer-B',timestamp:1700000001,signatures:[Uint8Array.of(8,9)]});
  const provenanceSql = "SELECT p.provider,p.source,p.batch,p.peer,p.ts,p.sig FROM (SELECT DISTINCT _cid FROM GJN) r JOIN flatsql_provenance('GJN',r._cid) p ORDER BY p.provider,p.source,p.batch";
  const provenance = async () => rows(await db.execute(request(provenanceSql),{sandbox:true})).map(row=>row.map(c=>c.KIND==='Text'?c.TEXT_VALUE:c.KIND==='Int'?c.INT_VALUE:c.KIND==='Bytes'?c.BYTES_VALUE:c.KIND));
  const expectedProvenance = [
    ['external-flatc','customer','first','peer-A',1700000000,[1,2,3,4]],
    ['second-provider','archive','third','peer-B',1700000001,[8,9]],
    ['second-provider','customer','second','peer-B',1700000001,[8,9]],
  ];
  assert.deepEqual(await provenance(),expectedProvenance);
  await assert.rejects(db.execute(request(provenanceSql),{maxRows:1}),e=>e.status<0);
  await assert.rejects(db.execute(request("SELECT * FROM flatsql_provenance('GJN','bad-cid')")),e=>e.status<0);
  await assert.rejects(db.execute(request('SELECT * FROM flatsql_provenance')),e=>e.status<0);
  assert.equal(rows(await db.execute(request("SELECT * FROM flatsql_provenance('GJN',NULL)"))).length,0);
  await db.close(); db=await FlatSQL.open({root});
  assert.deepEqual(await provenance(),expectedProvenance);
  assert.deepEqual(await db.execute(request('SELECT DISTINCT _data FROM GJN'),{records:true}),record);
  assert.equal(rows(await db.remove('GJN',record))[0][0].INT_VALUE,2);
  assert.equal(rows(await db.execute(request('SELECT COUNT(*) FROM GJN')))[0][0].INT_VALUE,0);
  await db.ingest('GJN',record,{producer:'untagged-peer',timestamp:1700000002});
  const untagged=rows(await db.execute(request("SELECT provider,source,peer,ts FROM flatsql_provenance('GJN',(SELECT _cid FROM GJN LIMIT 1))")));
  assert.deepEqual(untagged[0].map(c=>c.KIND==='Text'?c.TEXT_VALUE:c.KIND==='Int'?c.INT_VALUE:c.KIND),['Null','Null','untagged-peer',1700000002]);
  await db.remove('GJN',record);
  await db.close();db=await FlatSQL.open({root});
  assert.equal(rows(await db.execute(request('SELECT COUNT(*) FROM GJN')))[0][0].INT_VALUE,0);

  await db.close(); db=null;
  if(packageRoot) {
    const cli=path.join(packageRoot,'bin/flatsql.mjs');
    const cliRoot=path.join(root,'cli'),schemaFile=path.join(root,'GJN.bfbs');
    await fs.writeFile(schemaFile,bfbs);
    const run=(args,input)=>new Promise((resolve,reject)=>{
      const child=spawn(process.execPath,[cli,...args]);const chunks=[];let error='';
      child.stdout.on('data',c=>chunks.push(c));child.stderr.on('data',c=>error+=c);
      child.on('error',reject);child.on('close',code=>code===0?resolve(Buffer.concat(chunks)):reject(new Error(error)));
      child.stdin.on('error',reject);child.stdin.end(input);
    });
    assert.equal(rows(await run(['ingest','--root',cliRoot,'--type','GJN','--id','$GJN','--schema',schemaFile],record)).length,1);
    assert.deepEqual(new Uint8Array(await run(['query','--root',cliRoot,'--records'],request('SELECT _data FROM GJN'))),record);
    await assert.rejects(run(['ingest','--root',cliRoot,'--type','GJN','--id','$GJN','--schema',schemaFile],record.slice(0,-1)),/Truncated/);
  }
  console.log(`PASS: external flatc-wasm -> GJN ingest -> QRP/record queries -> durable reopen; geometry, typed parameters, errors, budgets and cancellation (${Math.round(performance.now()-began)} ms)`);
} finally {
  await db?.close();
  await fs.rm(root,{recursive:true,force:true});
}
