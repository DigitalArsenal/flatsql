// Demo controls consume the same binary API as every FlatSQL application.
import { FlatSQL } from './api.mjs';
import * as fb from 'flatbuffers';
import { QRP } from 'spacedatastandards.org/lib/js/QRP/QRP.js';
import { qrpRepresentation } from 'spacedatastandards.org/lib/js/QRP/qrpRepresentation.js';
import { RAW_TYPES } from './demo-raw-store.js';
import { createQueryView } from './demo-query.js';
const asset=name=>new URL(name,import.meta.url);
const el=id=>document.getElementById(id);
const status=text=>{el('binaryStatus').textContent=text;};
const saved=(key,value)=>{try{if(value!==undefined)sessionStorage.setItem(key,value);return sessionStorage.getItem(key);}catch{return null;}};
const sessionId=saved('flatsql-demo-session')||crypto.randomUUID();
saved('flatsql-demo-session',sessionId);
const root=`flatsql-demo-${sessionId}`;
const stores=new Map();
const internalTypes=new Set(RAW_TYPES);
let mode=saved('flatsql-demo-backend')||'memory',selected='GJN',catalog=[],busy=false,stopping=false,db;
const current=()=>stores.get(mode);
const queryView=createQueryView({results:el('binaryResults'),summary:el('binaryCount'),state:el('queryState'),elapsed:el('queryElapsed'),cancel:el('btnCancelQuery')});
function pages(stream) {
 const out=[];
 for(let at=0;at<stream.length;) {
  const n=new DataView(stream.buffer,stream.byteOffset+at,4).getUint32(0,true);
  out.push(QRP.getSizePrefixedRootAsQRP(new fb.ByteBuffer(stream.subarray(at,at+n+4))));at+=n+4;
 }
 return out;
}
function request(sql) {
 const b = new fb.Builder(256); const text = b.createString(sql);
 QRP.startQRP(b); QRP.addSql(b,text); QRP.addRepresentation(b,qrpRepresentation.FlatBuffer);
 QRP.finishSizePrefixedQRPBuffer(b,QRP.endQRP(b)); return b.asUint8Array();
}
const bytes=n=>{const units=['B','KiB','MiB','GiB','TiB'];let i=0;while(n>=1024&&i<units.length-1){n/=1024;i++;}return `${n.toLocaleString(undefined,{maximumFractionDigits:1})} ${units[i]}`;};
async function meters() {
 const memory=performance.memory;
 if(memory?.jsHeapSizeLimit>0 && Number.isFinite(memory.usedJSHeapSize)) {
  el('memoryMeter').hidden=false;el('memoryMeter').max=memory.jsHeapSizeLimit;el('memoryMeter').value=memory.usedJSHeapSize;
  el('memoryText').textContent=`~${bytes(Math.max(0,memory.jsHeapSizeLimit-memory.usedJSHeapSize))} JS heap budget left · ${bytes(memory.usedJSHeapSize)} used. Excludes worker/WASM memory; free system RAM is unavailable.`;
 } else {el('memoryText').textContent='This browser does not expose remaining RAM. Generated data size is shown below; engine and index memory are additional.';}
 el('generatedBytes').textContent=`${bytes(current()?.bytes||0)} generated since opening this store`;
 try {
  const estimate=await navigator.storage?.estimate?.();
  if(!estimate?.quota)throw new Error();
  el('diskMeter').hidden=false;el('diskMeter').max=estimate.quota;el('diskMeter').value=estimate.usage||0;
  el('diskText').textContent=`~${bytes(Math.max(0,estimate.quota-(estimate.usage||0)))} available · ${bytes(estimate.usage||0)} of ${bytes(estimate.quota)} origin quota used`;
 }catch{el('diskMeter').hidden=true;el('diskText').textContent='Storage quota is unavailable in this browser.';}
}
function controls() {
 for(const input of document.querySelectorAll('button,input,select'))input.disabled=busy||!db;
 el('btnStop').disabled=!busy||!generating;
 el('btnCancelQuery').disabled=!queryView.running;
 el('btnBinaryIngest').disabled=busy||!db||!el('datasetSelect').value;
 el('btnContinuous').disabled=el('btnBinaryIngest').disabled;
 for(const button of document.querySelectorAll('[data-count]'))button.disabled=el('btnBinaryIngest').disabled;
 for(const id of ['btnBinaryQuery','btnCount','btnPreview','btnSpatial'])el(id).disabled=busy||!db||internalTypes.has(selected);
 el('btnBinaryReopen').disabled=busy||!db||mode!=='opfs';
 el('btnPersist').disabled=busy||!db||mode!=='opfs'||!navigator.storage?.persist;
 el('storageMode').querySelector('[value=opfs]').disabled=!navigator.storage?.getDirectory;
 el('storageMode').disabled=busy;
 el('storageMode').value=mode;
}
function summary() {
 const counts=current()?.counts||new Map();
 const total=[...counts.values()].reduce((a,b)=>a+b,0);
 el('recordSummary').textContent=`${total.toLocaleString()} records in ${mode==='memory'?'RAM':'disk'} · ${(counts.get(selected)||0).toLocaleString()} ${selected} records`;
}
async function register() {
 if(internalTypes.has(selected))return;
 if(current().registered.has(selected))return;
 const type=catalog.find(t=>t.name===selected);
 const response=await fetch(asset(`sds/${selected}.bfbs`));
 if(!response.ok)throw new Error(`Schema ${selected}: HTTP ${response.status}`);
 await db.registerSchema({name:selected,fileIdentifier:type.fileIdentifier,bfbs:new Uint8Array(await response.arrayBuffer())});
 current().registered.add(selected);
}
async function open() {
 if(stores.has(mode)){db=current().db;return;}
 db=await FlatSQL.open({root,backend:mode,wasmUrl:asset('flatsql.wasm'),workerUrl:asset('runtime/browser-guest.mjs'),threadWorkerUrl:asset('runtime/wasiThreadBrowserWorker.mjs'),ioWorkerUrl:asset('runtime/opfsIoWorker.mjs')});
 stores.set(mode,{db,registered:new Set(),counts:new Map(),bytes:0});
 for(const page of pages(await db.surface()))for(let i=0;i<page.tablesLength();i++) {
  const table=page.TABLES(i),name=table.NAME();
  if(catalog.some(t=>t.name===name)){current().registered.add(name);await count(name);}
 }
 await register();
 for(const type of RAW_TYPES)await count(type);
}
async function count(name=selected) {
 if(internalTypes.has(name)) {
  const result=await callGenerator({action:'rawCount',backend:mode,root,type:name});
  current().counts.set(name,result.count);summary();return result.count;
 }
 const stream=await db.execute(request(`SELECT COUNT(*) FROM "${name}"`));
 const rowPage=pages(stream).find(page=>page.rowsLength()>0);
 if(!rowPage)throw new Error('The record count query returned no row');
 const value=Number(rowPage.ROWS(0).CELLS(0).INT_VALUE());
 current().counts.set(name,value);summary();return value;
}
async function query(){
 if(internalTypes.has(selected)) {
  queryView.reset();
  const notice=document.createElement('p');notice.style.padding='12px';
  notice.textContent=`${(current().counts.get(selected)||0).toLocaleString()} ${selected} records stored as size-prefixed FlatBuffers. These internal control schemas are kept outside FlatSQL feeds and cannot be queried with SQL.`;
  el('binaryResults').replaceChildren(notice);
  el('binaryCount').textContent='Raw SDS collection';
 }else return queryView.run(db,request(el('binarySql').value));
}
async function action(fn,message) {
 if(busy)return;
 busy=true;controls();
 try{const result=await fn();if(message)status(result?.cancelled?'Query cancelled':message);}
 catch(error){status(`Error: ${error.message}`);}
 finally{busy=false;controls();summary();await meters();}
}
function selectedUI() {
 const type=catalog.find(t=>t.name===selected);
 el('datasetHint').textContent=internalTypes.has(selected)?`${type.description} — Generated as raw binary records. SQL is unavailable because this type is reserved for FlatSQL's internal store format.`:type.description;
 el('schemaDownload').href=asset(`sds/${selected}.bfbs`);
 el('binarySql').value=`SELECT * FROM "${selected}" LIMIT 50`;
 el('btnSpatial').hidden=selected!=='GJN';summary();
 if(internalTypes.has(selected))el('binarySql').value='SQL is unavailable for this internal control schema.';
}
function filterTypes() {
 const term=el('typeSearch').value.trim().toLowerCase();
 const types=catalog.filter(t=>`${t.name} ${t.description}`.toLowerCase().includes(term));
 const containsSelected=types.some(t=>t.name===selected);
 el('datasetSelect').replaceChildren(...(containsSelected?[]:[new Option(types.length?'Choose a matching type':'No matching types','')]),...types.map(t=>new Option(`${t.name} — ${t.description.split('\n')[0]}`,t.name)));
 if(!containsSelected)el('datasetSelect').options[0].disabled=true;
 el('datasetSelect').value=containsSelected?selected:'';
 el('typeCount').textContent=`${types.length} / ${catalog.length}`;
 if(!types.length)el('datasetHint').textContent='No matching SDS types. Try another search.';
 else if(!containsSelected)el('datasetHint').textContent='Choose a matching type from the dropdown to generate records.';
 controls();
 if(containsSelected)selectedUI();
}
function selectType(name) {
 return action(async()=>{selected=name;selectedUI();await register();await count();await query();},'Ready · add more records to the selected type');
}
const generator=new Worker(asset('generator.mjs'),{type:'module'});
let generationId=0,generating=false;
function callGenerator(params,transfer=[]) {
 return new Promise((resolve,reject)=>{
  const id=++generationId;
  const timer=setTimeout(()=>{cleanup();reject(new Error('Record generation timed out'));},30000);
  const cleanup=()=>{clearTimeout(timer);generator.removeEventListener('message',message);generator.removeEventListener('error',error);};
  const message=({data})=>{if(data.id!==id)return;cleanup();data.error?reject(new Error(data.error)):resolve(data);};
  const error=event=>{cleanup();reject(new Error(event.message));};
  generator.addEventListener('message',message);generator.addEventListener('error',error);
  generator.postMessage({id,...params},transfer);
 });
}
function generate(continuous=false) {
 const requested=continuous?Infinity:Number(el('recordCount').value);
 if(!continuous&&(!Number.isSafeInteger(requested)||requested<1)){status('Enter a positive whole number of records.');return;}
 generating=true;stopping=false;
 return action(async()=>{
  const progress=el('generationProgress');progress.hidden=false;
  if(continuous)progress.removeAttribute('value');else{progress.max=requested;progress.value=0;}
  const start=await count();let added=0;
  try {
   while(added<requested&&!stopping) {
    const next=await callGenerator({type:selected,count:Math.min(1000,requested-added),start:start+added});
    const generatedBytes=next.frames.length;
    if(internalTypes.has(selected))await callGenerator({action:'rawAppend',type:selected,count:next.count,frames:next.frames,backend:mode,root},[next.frames.buffer]);
    else await db.ingest(selected,next.frames,{provider:'demo',source:'spacedatastandards.org'});
    added+=next.count;current().bytes+=generatedBytes;await count();
    if(!continuous)progress.value=added;
    status(`Generating · ${added.toLocaleString()} ${selected} records${continuous?'':` of ${requested.toLocaleString()}`}…`);
    await meters();
   }
   await count();await query();
   const stored=current().counts.get(selected)||0;
   status(`${stopping?'Stopped':'Generated'} · ${added.toLocaleString()} ${selected} records · ${stored.toLocaleString()} stored${stored<start+added?' (identical content deduplicated)':''}`);
  } catch(error) {
   // A failed batch may contain committed records. Recount the store instead
   // of displaying an invented all-or-nothing total.
   await count().catch(()=>{});
   throw new Error(`${error.message}. ${added.toLocaleString()} records acknowledged before this batch; see the store count. You can stop, clear, or choose disk storage.`);
  } finally {generating=false;progress.hidden=true;}
 });
}
el('typeSearch').oninput=filterTypes;
el('datasetSelect').onchange=()=>selectType(el('datasetSelect').value);
el('btnBinaryIngest').onclick=()=>generate();
el('btnContinuous').onclick=()=>generate(true);
el('btnStop').onclick=()=>{stopping=true;status('Stopping after the current batch…');};
for(const button of document.querySelectorAll('[data-count]'))button.onclick=()=>{el('recordCount').value=button.dataset.count;generate();};
el('btnBinaryQuery').onclick=()=>action(query,'Query complete');
el('btnCount').onclick=()=>{el('binarySql').value=`SELECT COUNT(*) AS records FROM "${selected}"`;action(query,'Query complete');};
el('btnPreview').onclick=()=>{el('binarySql').value=`SELECT * FROM "${selected}" LIMIT 50`;action(query,'Query complete');};
el('btnSpatial').onclick=()=>{el('binarySql').value='SELECT _rowid FROM GJN WHERE geo_contains(_data,2,2) LIMIT 50';action(query,'Query complete');};
el('storageMode').onchange=()=>{
 const nextMode=el('storageMode').value;
 return action(async()=>{
 const before=mode;mode=nextMode;
 try{await open();await register();}
 catch(error){mode=before;db=current()?.db;throw error;}
 saved('flatsql-demo-backend',mode);selectedUI();await count();await query();
},'Ready · storage selected');
};
el('btnBinaryReopen').onclick=()=>action(async()=>{await db.close();db=null;stores.delete(mode);await callGenerator({action:'rawReset',backend:mode,root});await open();await count();await query();},'Reopened disk store');
el('btnBinaryClear').onclick=()=>action(async()=>{
 await db.close();db=null;stores.delete(mode);
 if(mode==='opfs'){const directory=await navigator.storage.getDirectory();await directory.removeEntry(root,{recursive:true});}
 await callGenerator({action:'rawReset',backend:mode,root});
 await open();await count();await query();
},'Demo store cleared');
el('btnPersist').onclick=()=>action(async()=>{
 const granted=await navigator.storage.persist();
 el('persistenceStatus').textContent=granted?'Persistent storage granted by this browser.':'Persistence was not granted. Disk data remains subject to browser eviction.';
},'Storage persistence checked');
(async()=>{
 status('Preparing browser storage…');
 if(!await flatsqlIsolation)throw new Error('This browser cannot isolate shared WASM memory');
 const response=await fetch(asset('sds/catalog.json'));
 if(!response.ok)throw new Error(`SDS catalog: HTTP ${response.status}`);
 const manifest=await response.json();catalog=manifest.types;el('sdsVersion').textContent=`SDS ${manifest.version} · ${catalog.length} types`;
 filterTypes();
 await action(async()=>{await open();await count();await query();},'Ready · choose a type and generate records');
 setInterval(()=>{if(!busy)meters();},3000);
})().catch(error=>status(`Error: ${error.message}`));
