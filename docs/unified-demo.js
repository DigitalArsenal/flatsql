// Product UI is a separate consumer of the binary library API. Generated SDS
// bindings construct requests and render values; FlatSQL never renders JSON.
import { FlatSQL } from './api.mjs';
import * as fb from 'flatbuffers';
import { QRP } from 'spacedatastandards.org/lib/js/QRP/QRP.js';
import { qrpRepresentation } from 'spacedatastandards.org/lib/js/QRP/qrpRepresentation.js';
const asset = name => new URL(name, import.meta.url);
const el = id => document.getElementById(id);
let db;
const root = 'flatsql-unified-demo';
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
async function query() {render(await db.execute(request(el('binarySql').value)));}
async function action(fn, message) {
 for(const id of buttons)el(id).disabled=true;
 try {await fn(); status(message);}
 catch(error){status(`Error: ${error.message}`);}
 finally{for(const id of buttons)el(id).disabled=!db;}
}
el('btnBinaryIngest').onclick=()=>action(async()=>{
 const frames=new Uint8Array(await (await fetch(asset('samples.fb'))).arrayBuffer());
 await db.ingest('GJN',frames,{provider:'demo',source:'flatc-wasm'});await query();
},'3 binary records loaded · spatial SQL ready');
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
