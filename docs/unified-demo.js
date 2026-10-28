// Product UI is a separate consumer of the binary library API. Generated SDS
// bindings construct requests and render values; FlatSQL never renders JSON.
import { FlatSQL } from './api.mjs';
import * as fb from 'flatbuffers';
import { QRP } from 'spacedatastandards.org/lib/js/QRP/QRP.js';
import { qrpRepresentation } from 'spacedatastandards.org/lib/js/QRP/qrpRepresentation.js';
import { GJN, GJNT, GJNFeatureT, GJNGeometryT, GJNGeometryType, GJNLinearRingT, GJNPositionT, GJNPropertyT } from 'spacedatastandards.org/lib/js/GJN/main.js';
const asset = name => new URL(name, import.meta.url);
const el = id => document.getElementById(id);
let db;
// Each browser session gets its own store. The id lives in sessionStorage, so a
// reload keeps the store and a new tab or browser session starts an empty one.
const sessionId = () => {
 try {
  let id = sessionStorage.getItem('flatsql-demo-session');
  if (!id) { id = crypto.randomUUID(); sessionStorage.setItem('flatsql-demo-session', id); }
  return id;
 } catch { return crypto.randomUUID(); }
};
const root = `flatsql-demo-${sessionId()}`;
const buttons = ['btnBinaryIngest','btnBinaryQuery','btnBinaryReopen','btnBinaryClear'];
const status = text => { el('binaryStatus').textContent = text; };
function request(sql) {
 const b = new fb.Builder(256); const text = b.createString(sql);
 QRP.startQRP(b); QRP.addSql(b,text); QRP.addRepresentation(b,qrpRepresentation.FlatBuffer); QRP.addMaxRows(b,100n);
 QRP.finishSizePrefixedQRPBuffer(b,QRP.endQRP(b)); return b.asUint8Array();
}
function render(stream) {
 const cols=[],rows=[];
 for(let at=0;at<stream.length;) {
  const n=new DataView(stream.buffer,stream.byteOffset+at,4).getUint32(0,true);
  const page=QRP.getSizePrefixedRootAsQRP(new fb.ByteBuffer(stream.subarray(at,at+n+4)));
  if(page.columnsLength() && !cols.length) for(let i=0;i<page.columnsLength();i++) cols.push(page.COLUMNS(i).NAME());
  for(let i=0;i<page.rowsLength();i++) {
   const row=page.ROWS(i),values=[];
   for(let j=0;j<row.cellsLength();j++) {
    const c=row.CELLS(j);
    values.push(c.KIND()===0?'NULL':c.KIND()===1?String(c.BOOL_VALUE()):c.KIND()===2?String(c.INT_VALUE()):c.KIND()===3?String(c.FLOAT_VALUE()):c.KIND()===4?c.TEXT_VALUE():`${c.bytesValueLength()} bytes`);
   }
   rows.push(values);
  }
  at+=n+4;
 }
 const table=document.createElement('table'),head=table.createTHead().insertRow();
 for(const name of cols) {const th=document.createElement('th');th.textContent=name;head.append(th);}
 const body=table.createTBody();
 for(const row of rows) {const tr=body.insertRow();for(const value of row)tr.insertCell().textContent=value;}
 el('binaryResults').replaceChildren(table);
 el('binaryCount').textContent=`${rows.length} result row${rows.length===1?'':'s'} · ${stream.length.toLocaleString()} FlatBuffer bytes`;
}
async function open() {
 db=await FlatSQL.open({root,wasmUrl:asset('flatsql.wasm'),workerUrl:asset('runtime/browser-guest.mjs'),threadWorkerUrl:asset('runtime/wasiThreadBrowserWorker.mjs'),ioWorkerUrl:asset('runtime/opfsIoWorker.mjs')});
 const bfbs=new Uint8Array(await (await fetch(asset('GJN.bfbs'))).arrayBuffer());
 await db.registerSchema({name:'GJN',fileIdentifier:'$GJN',bfbs});
}
// Standard SDS GJN records built in the browser with the generated SDS bindings.
const position = (LONGITUDE, LATITUDE, ALTITUDE) =>
 new GJNPositionT(LONGITUDE, LATITUDE, ALTITUDE ?? 0, ALTITUDE !== undefined);
const ring = (a, b) => new GJNLinearRingT([[a,a],[b,a],[b,b],[a,b],[a,a]].map(([x,y]) => position(x,y)));
const geometries = [
 new GJNGeometryT(GJNGeometryType.POLYGON, null, [], [ring(0,10), ring(4,6)]),
 new GJNGeometryT(GJNGeometryType.POINT, position(20,20,420)),
 new GJNGeometryT(GJNGeometryType.GEOMETRY_COLLECTION, null, [], [], [], [
  new GJNGeometryT(GJNGeometryType.POINT, position(-73.98,40.75)),
  new GJNGeometryT(GJNGeometryType.LINE_STRING, null, [position(0,0), position(1,0)]),
 ]),
];
function generateRecords() {
 const records = geometries.map((GEOMETRY, i) => {
  const b = new fb.Builder(512);
  const feature = new GJNFeatureT(`sample-${i+1}`, GEOMETRY, [new GJNPropertyT('source', 'generated in browser')], 0, false, true);
  GJN.finishSizePrefixedGJNBuffer(b, new GJNT([feature]).pack(b));
  return b.asUint8Array();
 });
 const frames = new Uint8Array(records.reduce((total, record) => total + record.length, 0));
 let at = 0;
 for (const record of records) { frames.set(record, at); at += record.length; }
 return frames;
}
async function query() {render(await db.execute(request(el('binarySql').value)));}
async function action(fn, message) {
 for(const id of buttons)el(id).disabled=true;
 try {await fn(); status(message);}
 catch(error){status(`Error: ${error.message}`);}
 finally{for(const id of buttons)el(id).disabled=!db;}
}
el('btnBinaryIngest').onclick=()=>action(async()=>{
 await db.ingest('GJN',generateRecords(),{provider:'demo',source:'spacedatastandards.org'});await query();
},'3 SDS GJN records generated and loaded · spatial SQL ready');
el('btnBinaryQuery').onclick=()=>action(query,'Query complete');
el('btnBinaryReopen').onclick=()=>action(async()=>{await db.close();db=null;await open();await query();},'Reopened persistent binary records');
el('btnBinaryClear').onclick=()=>action(async()=>{
 await db.close();db=null;
 const directory=await navigator.storage.getDirectory();
 await directory.removeEntry(root,{recursive:true});
 await open();await query();
},'Demo store cleared');
for(const button of document.querySelectorAll('[data-binary-sql]'))button.onclick=()=>{el('binarySql').value=button.dataset.binarySql;};
(async()=>{
 if(!crossOriginIsolated) {
  status('Preparing browser storage…');
  if(!('serviceWorker' in navigator))throw new Error('This browser cannot isolate shared WASM memory');
  await navigator.serviceWorker.register(new URL('../../coi-serviceworker.js',import.meta.url),{scope:new URL('../../',import.meta.url).pathname});
  await navigator.serviceWorker.ready;
  if(navigator.serviceWorker.controller)location.reload();
  else navigator.serviceWorker.addEventListener('controllerchange',()=>location.reload(),{once:true});
  return;
 }
 await action(async()=>{await open();await query();},'Ready · binary records, persistent storage');
})().catch(error=>status(`Error: ${error.message}`));
