import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { FlatcRunner } from 'flatc-wasm/runner';
const root = fileURLToPath(new URL('../', import.meta.url));
const out = path.join(root,'docs/assets/unified');
await fs.mkdir(out,{recursive:true});
// Remove only obsolete files produced by the earlier demo build.
for (const name of ['browser-host.mjs','browser-guest.mjs','thread-worker.mjs','io-worker.mjs'])
  await fs.rm(path.join(out,name),{force:true});
const compiler = await FlatcRunner.init();
const source = await fs.readFile(path.join(root,'node_modules/spacedatastandards.org/schema/GJN/main.fbs'),'utf8');
compiler.mountFile('/schema/GJN.fbs',source); compiler.mountFile('/out/.keep','');
const result = compiler.runCommand(['--binary','--schema','-o','/out','/schema/GJN.fbs']);
if(result.code) throw new Error(result.stderr);
await fs.writeFile(path.join(out,'GJN.bfbs'),compiler.readFile('/out/GJN.bfbs'));
const p=(LONGITUDE,LATITUDE,ALTITUDE)=>({LONGITUDE,LATITUDE,...(ALTITUDE===undefined?{}:{ALTITUDE,HAS_ALTITUDE:true})});
const ring=(a,b)=>({POSITIONS:[[a,a],[b,a],[b,b],[a,b],[a,a]].map(([x,y])=>p(x,y))});
const input={entry:'GJN.fbs',files:{'GJN.fbs':source}};
const geometries=[
 {TYPE:'POLYGON',RINGS:[ring(0,10),ring(4,6)]},
 {TYPE:'POINT',POINT:p(20,20,420)},
 {TYPE:'GEOMETRY_COLLECTION',GEOMETRIES:[{TYPE:'POINT',POINT:p(-73.98,40.75)},{TYPE:'LINE_STRING',POSITIONS:[p(0,0),p(1,0)]}]},
];
const records=geometries.map((GEOMETRY,i)=>compiler.generateBinary(input,JSON.stringify({FEATURES:[{ID:`sample-${i+1}`,HAS_GEOMETRY:true,GEOMETRY,PROPERTIES:[{KEY:'source',VALUE:'external flatc-wasm'}]}]}),{sizePrefix:true,unknownJson:false,strictJson:true}));
await fs.writeFile(path.join(out,'samples.fb'),Buffer.concat(records));
await fs.copyFile(path.join(root,'wasm/flatsql.wasm'),path.join(out,'flatsql.wasm'));
await fs.copyFile(path.join(root,'wasm/integrity.json'),path.join(out,'integrity.json'));
await fs.cp(path.join(root,'wasm/runtime'),path.join(out,'runtime'),{recursive:true});
const entries={'api.mjs':'wasm/index.js','demo.mjs':'docs/unified-demo.js'};
for(const [name,entry] of Object.entries(entries)) await build({entryPoints:[path.join(root,entry)],outfile:path.join(out,name),bundle:true,format:'esm',platform:'browser',target:'es2022',external:['node:*','./runtime/*','./api.mjs'],minify:true,logLevel:'warning'});
console.log('Built browser demo: canonical SDS fixtures, generic host adapters and the identical unified WASM artifact');
