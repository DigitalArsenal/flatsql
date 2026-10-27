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
   while(Date.now()<deadline){try{if(await evaluate(expression))return;}catch{}await new Promise(r=>setTimeout(r,100));}
   throw new Error(`Timed out: ${label}; ${await evaluate("JSON.stringify({text:document.body.innerText.slice(-1000),isolated:crossOriginIsolated,frame:document.getElementById('binaryDemo')?.contentDocument?.body.innerText,frameIsolation:document.getElementById('binaryDemo')?.contentWindow?.crossOriginIsolated})")}`);
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
 await evaluate("document.getElementById('btnBinaryIngest').click()");
 await wait(`(${status}).startsWith('3 binary records loaded')`,'binary ingest');
 assert.deepEqual(await evaluate("Array.from(document.querySelectorAll('tbody tr')).map(r=>Array.from(r.cells).map(c=>c.textContent))"),[['1','1','96','0'],['2','0','0','0'],['3','0','0','111.19492664455873']]);
 await shot('binary-desktop');
 const query=async(sql,expected)=>{
   await evaluate(`document.getElementById('binarySql').value=${JSON.stringify(sql)};document.getElementById('btnBinaryQuery').click()`);
   await wait("!document.getElementById('btnBinaryQuery').disabled",'query completion');
   assert.match(await evaluate(status),/^Query complete/);
   if(expected)assert.deepEqual(await evaluate("Array.from(document.querySelectorAll('tbody tr')).map(r=>Array.from(r.cells).map(c=>c.textContent))"),expected);
 };
 await query('SELECT _rowid FROM GJN WHERE geo_contains(_data,2,2)',[['1']]);
 await query('SELECT _rowid,geo_envelope(_data) FROM GJN');
 assert.equal(await evaluate("document.querySelectorAll('tbody tr').length"),3);
 assert.match(await evaluate("document.querySelector('tbody tr').lastElementChild.textContent"),/^\d+ bytes$/);
 await query('SELECT COUNT(*) FROM GJN',[['3']]);
 await evaluate("document.getElementById('btnBinaryReopen').click()");
 await wait(`(${status}).startsWith('Reopened')`,'durable reopen');
 assert.equal(await evaluate("document.querySelector('tbody').innerText"),'3');
 await send('Page.reload');await wait(`(${status}).startsWith('Ready')`,'page reload');
 await query('SELECT COUNT(*) FROM GJN',[['3']]);
 await send('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true});
 await shot('binary-mobile');
 assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth'),true);
 await evaluate("document.getElementById('btnBinaryClear').click()");
 await wait(`(${status}).startsWith('Demo store cleared')`,'clear exact demo store');
 assert.equal(await evaluate("document.querySelector('tbody').innerText"),'0');
 await send('Emulation.setDeviceMetricsOverride',{width:1280,height:1000,deviceScaleFactor:1,mobile:false});
 await send('Page.navigate',{url:base});
 await wait("document.getElementById('binaryDemo')?.contentDocument?.getElementById('binaryStatus')?.textContent.startsWith('Ready')",'homepage demo');
 assert.equal(await evaluate("document.querySelectorAll('.hero-badges img').length"),2);
 await evaluate("document.getElementById('binaryDemo').scrollIntoView()");await shot('homepage-demo');
 assert.deepEqual(errors,[]);
 console.log(`PASS: identical WASM, Pages isolation, binary ingest/filter/projection, persistence/reload, clear, desktop/mobile and homepage. Screenshots: ${evidence}`);
} finally {
 socket?.close();child.kill('SIGTERM');
 await new Promise(resolve=>{if(child.exitCode!==null)return resolve();child.once('exit',resolve);setTimeout(resolve,3000);});
 if(child.exitCode===null)child.kill('SIGKILL');
 server.close();fs.rmSync(profile,{recursive:true,force:true});
}
