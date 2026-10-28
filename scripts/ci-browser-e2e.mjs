// Real browser interaction against the built website and shipped WASM bytes.
// Chromium headless shell only; no browser framework or production credentials.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { FlatcRunner } from 'flatc-wasm/runner';
const converter=await FlatcRunner.init();
const qrpSource=fs.readFileSync(new URL('../node_modules/spacedatastandards.org/schema/QRP/main.fbs',import.meta.url),'utf8');
const invalidQuery=converter.generateBinary({entry:'QRP.fbs',files:{'QRP.fbs':qrpSource}},JSON.stringify({SQL:"SELECT json('{}')",REPRESENTATION:'FlatBuffer'}),{sizePrefix:true,strictJson:true});
const root=fileURLToPath(new URL('../docs',import.meta.url));
const evidence=process.env.FLATSQL_BROWSER_RESULTS??fs.mkdtempSync(path.join(os.tmpdir(),'flatsql-browser-e2e-'));
fs.mkdirSync(evidence,{recursive:true});
const digest=b=>createHash('sha256').update(b).digest('hex');
assert.equal(digest(fs.readFileSync(path.join(root,'assets/unified/flatsql.wasm'))),digest(fs.readFileSync(new URL('../wasm/flatsql.wasm',import.meta.url))));
function shell() {
 if(process.env.GAUNTLET_HEADLESS_SHELL)return process.env.GAUNTLET_HEADLESS_SHELL;
 const cache=process.env.PLAYWRIGHT_BROWSERS_PATH??path.join(os.homedir(),process.platform==='darwin'?'Library/Caches/ms-playwright':'.cache/ms-playwright');
 const dirs=fs.readdirSync(cache).filter(d=>/^chromium_headless_shell-\d+$/.test(d)).sort((a,b)=>Number(b.split('-')[1])-Number(a.split('-')[1]));
 for(const dir of dirs)for(const sub of fs.readdirSync(path.join(cache,dir))) {
   const bin=path.join(cache,dir,sub,'chrome-headless-shell');if(fs.existsSync(bin))return bin;
 }
 throw new Error('Install chromium-headless-shell, or set GAUNTLET_HEADLESS_SHELL');
}
const server=http.createServer((req,res)=>{
 const pathname=new URL(req.url,'http://localhost').pathname;
 if(pathname.endsWith('/favicon.ico')){res.writeHead(204);res.end();return;}
 const relative=pathname.replace(/^\/flatsql\//,'/');
 const file=path.resolve(root,'.'+(relative.endsWith('/')?relative+'index.html':relative));
 if(!file.startsWith(root+'/')||!fs.existsSync(file)||!fs.statSync(file).isFile()){res.writeHead(404);res.end();return;}
 const mime={'.html':'text/html','.js':'text/javascript','.mjs':'text/javascript','.css':'text/css','.wasm':'application/wasm','.svg':'image/svg+xml','.json':'application/json','.png':'image/png','.mp4':'video/mp4','.webm':'video/webm'};
 // Deliberately no COOP/COEP headers: verify the actual Pages isolation flow.
 res.setHeader('content-type',mime[path.extname(file)]??'application/octet-stream');fs.createReadStream(file).pipe(res);
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const profile=fs.mkdtempSync(path.join(os.tmpdir(),'flatsql-headless-'));
const child=spawn(shell(),['--remote-debugging-port=0',`--user-data-dir=${profile}`,'--no-first-run','--no-default-browser-check','--use-angle=swiftshader','--enable-unsafe-swiftshader',...(process.platform==='linux'?['--no-sandbox']:[]),'about:blank'],{stdio:['ignore','ignore','pipe']});
let socket;
try {
 const port=await new Promise((resolve,reject)=>{
   let output='';const timer=setTimeout(()=>reject(new Error('Browser startup timed out')),20000);
   child.on('error',reject);child.stderr.on('data',b=>{output+=b;const m=output.match(/DevTools listening on ws:\/\/[^:]+:(\d+)\//);if(m){clearTimeout(timer);resolve(Number(m[1]));}});
   child.on('exit',code=>{clearTimeout(timer);reject(new Error(`Browser exited: ${code}`));});
 });
 const tab=await(await fetch(`http://127.0.0.1:${port}/json/new?about:blank`,{method:'PUT'})).json();
 socket=new WebSocket(tab.webSocketDebuggerUrl);await new Promise((resolve,reject)=>{socket.onopen=resolve;socket.onerror=reject;});
 let serial=0;const pending=new Map(),errors=[];
 socket.onmessage=({data})=>{
   const m=JSON.parse(data);
   if(m.id){const p=pending.get(m.id);pending.delete(m.id);if(m.error)p.reject(new Error(m.error.message));else p.resolve(m.result);}
   else if(m.method==='Runtime.exceptionThrown')errors.push(m.params.exceptionDetails?.exception?.description??m.params.exceptionDetails.text);
   else if(m.method==='Runtime.consoleAPICalled' && m.params.type==='error')errors.push(m.params.args.map(a=>a.value??a.description).join(' '));
 };
 const send=(method,params={})=>new Promise((resolve,reject)=>{const id=++serial;pending.set(id,{resolve,reject});socket.send(JSON.stringify({id,method,params}));});
 const evaluate=async expression=>{
   const r=await send('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});
   if(r.exceptionDetails)throw new Error(r.exceptionDetails.exception?.description??r.exceptionDetails.text);return r.result.value;
 };
 const wait=async(expression,label)=>{
   const deadline=Date.now()+30000;
   while(Date.now()<deadline){try{if(await evaluate(expression))return;}catch{}await new Promise(r=>setTimeout(r,20));}
   throw new Error(`Timed out: ${label}; ${await evaluate("JSON.stringify({status:document.getElementById('binaryStatus')?.textContent,text:document.body.innerText.slice(-1000),isolated:crossOriginIsolated,frame:document.getElementById('binaryDemo')?.contentDocument?.body.innerText,frameIsolation:document.getElementById('binaryDemo')?.contentWindow?.crossOriginIsolated})")}`);
 };
 const shot=async name=>{const s=await send('Page.captureScreenshot',{format:'png'});fs.writeFileSync(path.join(evidence,name+'.png'),Buffer.from(s.data,'base64'));};
 await send('Runtime.enable');await send('Page.enable');
 const base=`http://127.0.0.1:${server.address().port}/flatsql/`;
 await send('Emulation.setDeviceMetricsOverride',{width:1100,height:920,deviceScaleFactor:1,mobile:false});
 await send('Page.navigate',{url:base+'unified-demo.html'});
 const status="document.getElementById('binaryStatus')?.textContent??''";
 await wait(`(${status}).startsWith('Ready')`,'engine ready');
 assert.equal(await evaluate('crossOriginIsolated'),true);
 const diagnostic=await evaluate(`(async()=>{const {FlatSQL}=await import('/flatsql/assets/unified/api.mjs');const a=await FlatSQL.open({backend:'memory'});let b;try{b=await FlatSQL.open({backend:'memory'});try{await a.execute(new Uint8Array(${JSON.stringify(Array.from(invalidQuery))}));return null}catch(error){return {name:error.name,status:error.status,message:error.message}}}finally{await b?.close();await a.close()}})()`);
 assert.equal(diagnostic.name,'FlatSQLError');assert.equal(diagnostic.status,-13);assert.match(diagnostic.message,/json|function/i);
 const gjnSource=fs.readFileSync(new URL('../node_modules/spacedatastandards.org/schema/GJN/main.fbs',import.meta.url),'utf8');
const gjnFrame=converter.generateBinary({entry:'GJN.fbs',files:{'GJN.fbs':gjnSource}},JSON.stringify({FEATURES:[{ID:'sample-1',HAS_GEOMETRY:true,GEOMETRY:{TYPE:'POINT',POINT:{LONGITUDE:20,LATITUDE:20,ALTITUDE:420,HAS_ALTITUDE:true}},PROPERTIES:[{KEY:'source',VALUE:'e2e'}]}]}),{sizePrefix:true,unknownJson:false,strictJson:true});
const rawQuery=converter.generateBinary({entry:'QRP.fbs',files:{'QRP.fbs':qrpSource}},JSON.stringify({SQL:'SELECT DISTINCT _data FROM GJN',REPRESENTATION:'FlatBuffer'}),{sizePrefix:true,strictJson:true});
 const cleared=await evaluate(`(async()=>{
   const {FlatSQL}=await import('/flatsql/assets/unified/api.mjs');
   const bfbs=new Uint8Array(await (await fetch('/flatsql/assets/unified/GJN.bfbs')).arrayBuffer());
   const frame=new Uint8Array(${JSON.stringify(Array.from(gjnFrame))});
   const query=new Uint8Array(${JSON.stringify(Array.from(rawQuery))});
   let db=await FlatSQL.open({root:'multi-source-e2e'});
   try {
     await db.registerSchema({name:'GJN',fileIdentifier:'$GJN',bfbs});
     for(let i=0;i<12;i++)await db.ingest('GJN',frame,{source:'source-'+i,provider:'provider-'+i});
     await db.close();db=await FlatSQL.open({root:'multi-source-e2e'});
     const before=await db.execute(query,{records:true});
     if(before.length!==frame.length||!before.every((b,i)=>b===frame[i]))throw Error('Shared record changed');
     await db.remove('GJN',frame);
     await db.close();db=await FlatSQL.open({root:'multi-source-e2e'});
     return (await db.execute(query,{records:true})).length;
   }finally{await db.close();}
 })()`);
 assert.equal(cleared,0);
 await evaluate("document.getElementById('recordCount').value=3;document.getElementById('binarySql').value='SELECT _rowid, geo_contains(_data,2,2), geo_area_geom(_data), geo_length_geom(_data) FROM GJN';document.getElementById('btnBinaryIngest').click()");
 await wait(`(${status}).startsWith('Generated · 3 GJN')`,'binary ingest');
 assert.deepEqual(await evaluate("Array.from(document.querySelectorAll('tbody tr')).map(r=>Array.from(r.cells).map(c=>c.textContent))"),[['1','1','96','0'],['2','0','0','0'],['3','0','0','111.19492664455873']]);
 await shot('binary-desktop');
 const query=async(sql,expected)=>{
   await evaluate(`document.getElementById('binarySql').value=${JSON.stringify(sql)};document.getElementById('btnBinaryQuery').click()`);
   await wait("!document.getElementById('btnBinaryQuery').disabled",'query completion');
   assert.match(await evaluate(status),/^Query complete/);
   if(expected)assert.deepEqual(await evaluate("Array.from(document.querySelectorAll('tbody tr')).map(r=>Array.from(r.cells).map(c=>c.textContent))"),expected);
 };
 await query('SELECT _rowid FROM GJN WHERE geo_contains(_data,2,2)',[['1']]);
 await query("SELECT p.provider,p.source FROM GJN r JOIN flatsql_provenance('GJN',r._cid) p ORDER BY r._rowid",Array.from({length:3},()=>['demo','spacedatastandards.org']));
 await shot('binary-provenance');
 await query('SELECT _rowid,geo_envelope(_data) FROM GJN');
 assert.equal(await evaluate("document.querySelectorAll('tbody tr').length"),3);
 assert.match(await evaluate("document.querySelector('tbody tr').lastElementChild.textContent"),/^\d+ bytes$/);
 await query('SELECT COUNT(*) FROM GJN',[['3']]);
 // Repeat generation appends unique content; changing types preserves GJN.
 await evaluate("document.getElementById('btnBinaryIngest').click()");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'append');
 await query('SELECT COUNT(*) FROM GJN',[['6']]);
 await evaluate("document.getElementById('typeSearch').value='orbit mean';document.getElementById('typeSearch').dispatchEvent(new Event('input'))");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'filtered type');
 assert.equal(await evaluate("document.getElementById('datasetSelect').value"),'OMM');
 await evaluate("document.getElementById('recordCount').value=7;document.getElementById('btnBinaryIngest').click()");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'OMM batch');
 await query('SELECT COUNT(*) FROM OMM',[['7']]);
 await query('SELECT COUNT(*) FROM GJN',[['6']]);
 await evaluate("document.getElementById('typeSearch').value='no such sds';document.getElementById('typeSearch').dispatchEvent(new Event('input'))");
 assert.equal(await evaluate("document.getElementById('btnBinaryIngest').disabled"),true);
 await evaluate("document.getElementById('typeSearch').value='';document.getElementById('typeSearch').dispatchEvent(new Event('input'))");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'clear filter');
 assert.equal(await evaluate("document.getElementById('datasetSelect').options.length"),286);
 // Exercise every catalog entry through the real picker, generator worker,
 // binary ingest and SQL count, including nested/required fields and int64s.
 const allTypes=await evaluate("Array.from(document.getElementById('datasetSelect').options,o=>o.value)");
 for(const type of allTypes) {
   await evaluate(`document.getElementById('datasetSelect').value=${JSON.stringify(type)};document.getElementById('datasetSelect').dispatchEvent(new Event('change'))`);
   await wait("!document.getElementById('datasetSelect').disabled",`${type} selected`);
   assert.doesNotMatch(await evaluate(status),/^Error:/,type+' select');
   await evaluate("document.getElementById('recordCount').value=2;document.getElementById('btnBinaryIngest').click()");
   await wait("!document.getElementById('btnBinaryIngest').disabled",`${type} generation`);
   assert.match(await evaluate(status),/^Generated/,type+' generate');
   if(allTypes.indexOf(type)%50===0)console.log(`SDS browser progress: ${allTypes.indexOf(type)+1}/${allTypes.length}`);
   if(['FMB','FCK','FSG'].includes(type)){
     assert.equal(await evaluate("document.getElementById('btnBinaryQuery').disabled"),true);
     assert.match(await evaluate("document.getElementById('binaryResults').textContent"),new RegExp(`^2 ${type} records stored`));
     continue;
   }
   const total=type==='GJN'?8:type==='OMM'?9:2;
   await query(`SELECT COUNT(*) FROM "${type}"`,[[String(total)]]);
 }
 console.log(`Verified all ${allTypes.length} SDS generators: 283 SQL collections and 3 raw internal-schema collections`);
 await evaluate("document.getElementById('datasetSelect').value='GJN';document.getElementById('datasetSelect').dispatchEvent(new Event('change'))");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'GJN selected');
 await evaluate("document.getElementById('recordCount').value=1001;document.getElementById('btnBinaryIngest').click()");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'multi-batch generation');
 await query('SELECT COUNT(*) FROM GJN',[['1009']]);
 await evaluate("document.getElementById('btnContinuous').click()");
 await wait(`(${status}).startsWith('Generating · ')`,'continuous generation starts');
 await evaluate("document.getElementById('btnStop').click()");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'continuous stop');
 assert.match(await evaluate(status),/^Stopped/);
 assert.equal(await evaluate("document.getElementById('recordCount').hasAttribute('max')"),false);
 assert.match(await evaluate("document.getElementById('memoryText').textContent"),/heap|RAM/);
 assert.match(await evaluate("document.getElementById('diskText').textContent"),/available/);
 await query('SELECT COUNT(*) FROM GJN');
 const ramCount=await evaluate("document.querySelector('tbody').innerText");
 await shot('generator-desktop');
 await evaluate("document.getElementById('recordCount').value=0;document.getElementById('btnBinaryIngest').click()");
 assert.match(await evaluate(status),/positive whole number/);
 assert.equal(await evaluate("document.getElementById('btnBinaryIngest').disabled"),false);
 await evaluate("document.getElementById('binarySql').value='SELECT nonexistent FROM GJN';document.getElementById('btnBinaryQuery').click()");
 await wait("!document.getElementById('btnBinaryQuery').disabled",'query error recovery');
 assert.match(await evaluate(status),/^Error:/);

 // Disk is an actual OPFS-backed separate store and RAM survives switching.
 await evaluate("document.getElementById('storageMode').value='opfs';document.getElementById('storageMode').dispatchEvent(new Event('change'))");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'disk selection');
 console.log('Disk selection:', await evaluate(status), await evaluate("document.getElementById('storageMode').value"));
 assert.match(await evaluate(status),/^Ready/);
 await query('SELECT COUNT(*) FROM GJN',[['0']]);
 await evaluate("document.getElementById('recordCount').value=3;document.getElementById('btnBinaryIngest').click()");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'disk generation');
 await query('SELECT COUNT(*) FROM GJN',[['3']]);
 await evaluate("document.getElementById('storageMode').value='memory';document.getElementById('storageMode').dispatchEvent(new Event('change'))");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'return to RAM');
 await query('SELECT COUNT(*) FROM GJN',[[ramCount]]);
 await evaluate("document.getElementById('btnBinaryClear').click()");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'clear RAM only');
 await query('SELECT COUNT(*) FROM GJN',[['0']]);
 await evaluate("document.getElementById('storageMode').value='opfs';document.getElementById('storageMode').dispatchEvent(new Event('change'))");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'return to disk');
 await query('SELECT COUNT(*) FROM GJN',[['3']]);
 await evaluate("document.getElementById('btnPersist').click()");
 await wait("!document.getElementById('btnPersist').disabled",'storage persistence request');
 assert.match(await evaluate("document.getElementById('persistenceStatus').textContent"),/granted/);
 // Internal storage-control schemas remain standard binary records, stored
 // outside engine feeds. They append and survive OPFS reopen/reload too.
 await evaluate("document.getElementById('datasetSelect').value='FSG';document.getElementById('datasetSelect').dispatchEvent(new Event('change'))");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'FSG disk collection');
 await evaluate("document.getElementById('recordCount').value=5;document.getElementById('btnBinaryIngest').click()");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'FSG disk generation');
 await evaluate("document.getElementById('btnBinaryIngest').click()");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'FSG disk append');
 assert.match(await evaluate("document.getElementById('binaryResults').textContent"),/^10 FSG records stored/);
 const rawFrames=await evaluate(`(async()=>{
  let dir=await navigator.storage.getDirectory();
  for(const name of ['flatsql-demo-'+sessionStorage.getItem('flatsql-demo-session'),'demo-raw','FSG'])dir=await dir.getDirectoryHandle(name);
  let count=0;
  for await(const handle of dir.values()) {
   const bytes=new Uint8Array(await(await handle.getFile()).arrayBuffer());
   for(let at=0;at<bytes.length;) {
    const size=new DataView(bytes.buffer,at,4).getUint32(0,true);
    if(String.fromCharCode(...bytes.subarray(at+8,at+12))!=='$FSG'||at+4+size>bytes.length)throw Error('Invalid raw SDS frame');
    at+=size+4;count++;
   }
  }
  return count;
 })()`);
 assert.equal(rawFrames,10);
 await send('Emulation.setDeviceMetricsOverride',{width:1100,height:1200,deviceScaleFactor:1,mobile:false});
 await shot('raw-internal-sds');
 await send('Emulation.setDeviceMetricsOverride',{width:1100,height:920,deviceScaleFactor:1,mobile:false});
 await evaluate("document.getElementById('btnBinaryReopen').click()");
 await wait(`(${status}).startsWith('Reopened')`,'durable reopen');
 assert.match(await evaluate("document.getElementById('binaryResults').textContent"),/^10 FSG records stored/);
 await send('Page.reload');await wait(`(${status}).startsWith('Ready')`,'page reload');
 await query("SELECT count(*) FROM GJN r JOIN flatsql_provenance('GJN',r._cid) p WHERE p.provider='demo' AND p.source='spacedatastandards.org'",[['3']]);
 await query('SELECT COUNT(*) FROM GJN',[['3']]);
 await evaluate("document.getElementById('datasetSelect').value='FSG';document.getElementById('datasetSelect').dispatchEvent(new Event('change'))");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'FSG after reload');
 assert.match(await evaluate("document.getElementById('binaryResults').textContent"),/^10 FSG records stored/);
 await evaluate("document.getElementById('datasetSelect').value='GJN';document.getElementById('datasetSelect').dispatchEvent(new Event('change'))");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'GJN after reload');
 await query('SELECT COUNT(*) FROM GJN',[['3']]);
 await send('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true});
 await shot('binary-mobile');
 assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth'),true);
 await evaluate("document.getElementById('btnBinaryClear').click()");
 await wait(`(${status}).startsWith('Demo store cleared')`,'clear exact demo store');
 assert.equal(await evaluate("document.querySelector('tbody').innerText"),'0');
 await evaluate("document.getElementById('datasetSelect').value='FSG';document.getElementById('datasetSelect').dispatchEvent(new Event('change'))");
 await wait("!document.getElementById('btnBinaryIngest').disabled",'raw collection cleared');
 assert.match(await evaluate("document.getElementById('binaryResults').textContent"),/^0 FSG records stored/);

 await send('Emulation.setDeviceMetricsOverride',{width:1280,height:1000,deviceScaleFactor:1,mobile:false});
 await send('Page.navigate',{url:base});
 await wait("document.getElementById('binaryDemo')?.contentDocument?.getElementById('binaryStatus')?.textContent.startsWith('Ready')",'homepage demo');
 assert.equal(await evaluate("document.querySelectorAll('.hero-badges img').length"),2);
 await evaluate("document.getElementById('binaryDemo').scrollIntoView({behavior:'instant',block:'start'})");
 await wait("Math.abs(document.getElementById('binaryDemo').getBoundingClientRect().top-80)<5",'homepage demo visible');
 await evaluate('new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))');
 await shot('homepage-demo');
 assert.deepEqual(errors,[]);
 console.log(`PASS: identical WASM, Pages isolation, all SDS types, search, unique append, batched/continuous generation and stop, RAM/OPFS meters, binary queries, persistence/reload, clear, desktop/mobile and homepage. Screenshots: ${evidence}`);
} finally {
 socket?.close();child.kill('SIGTERM');
 await new Promise(resolve=>{if(child.exitCode!==null)return resolve();child.once('exit',resolve);setTimeout(resolve,3000);});
 if(child.exitCode===null)child.kill('SIGKILL');
 server.close();fs.rmSync(profile,{recursive:true,force:true});
}
