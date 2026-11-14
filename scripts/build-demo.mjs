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
const sds = path.join(root, 'node_modules/spacedatastandards.org');
const schemas = [];
// The published SDS package is the schema authority; never maintain a demo list.
for (const dir of await fs.readdir(path.join(sds, 'schema'), {withFileTypes:true})) {
  if (!dir.isDirectory()) continue;
  const source = await fs.readFile(path.join(sds, 'schema', dir.name, 'main.fbs'), 'utf8');
  compiler.mountFile(`/schema/${dir.name}/main.fbs`, source);
  if (!/root_type\s+\w+\s*;/.test(source)) continue;
  const json = JSON.parse(await fs.readFile(path.join(sds, 'lib/fbjson', dir.name, 'main.fb.schema.json'), 'utf8'));
  const name = json['x-flatbuffer-root-type'];
  schemas.push({name, fileIdentifier:json['x-flatbuffer-file-identifier'], description:json.definitions[name].description || name, json});
}
schemas.sort((a,b) => a.name.localeCompare(b.name));
const schemaOut = path.join(out, 'sds');
// This directory contains only this build's generated assets. Drop stale chunks.
await fs.rm(schemaOut, {recursive:true,force:true});
await fs.mkdir(schemaOut, {recursive:true});
compiler.mountFile('/out/.keep','');
for (const schema of schemas) {
  const result = compiler.runCommand(['--binary','--schema','-o','/out',`/schema/${schema.name}/main.fbs`]);
  if(result.code) throw new Error(`${schema.name}: ${result.stderr}`);
  await fs.writeFile(path.join(schemaOut,`${schema.name}.bfbs`),compiler.readFile('/out/main.bfbs'));
}
await fs.writeFile(path.join(schemaOut,'catalog.json'),JSON.stringify({version:JSON.parse(await fs.readFile(path.join(sds,'package.json'))).version,types:schemas.map(({json,...entry})=>entry)}));

// Generate constructors from SDS metadata, then let the official generated
// bindings pack each record. No handwritten schema, binary offsets, or compiler
// on the browser's main thread. Each type is loaded only when selected.
function objectCode(json, name, depth=0, ancestors=[]) {
  if (ancestors.includes(name)) return 'null';
  const definition=json.definitions[name];
  if (!definition?.properties) return 'null';
  const props=[];
  for (const [key,field] of Object.entries(definition.properties)) {
    function value(spec) {
      if (spec['x-flatbuffer-type']==='union' || spec.oneOf || spec.anyOf) return undefined;
      if (spec.$ref) {
        const ref=spec.$ref.split('/').pop(), target=json.definitions[ref];
        if (target['x-flatbuffer-type']==='union') return undefined;
        if (target.enum) {
          const values=Object.values(target['x-flatbuffer-enum-values']||{}).map(v=>v.value);
          return values.length?`${JSON.stringify(values)}[Number(sequence % ${values.length}n)]`:undefined;
        }
        if(depth>=2&&!spec['x-flatbuffer-required'])return 'null';
        return objectCode(json,ref,depth+1,[...ancestors,name]);
      }
      if (spec.enum) return undefined;
      if (spec.type==='array') {
        const item=value({...spec.items,'x-flatbuffer-type':spec['x-flatbuffer-type']?.slice(1,-1)});
        if (!item || item==='null') return '[]';
        return `[${item}]`;
      }
      if (spec.type==='string') return '`demo-${id}`';
      if (spec.type==='boolean') return 'true';
      if (spec.type==='number') return 'Number(sequence % 1000000000n) + 0.25';
      if (spec.type==='integer') {
        if (/^(u?long|u?int64)$/.test(spec['x-flatbuffer-type']||'') || spec.maximum>Number.MAX_SAFE_INTEGER) return 'sequence';
        return `Number(sequence % ${BigInt(Math.min(spec.maximum??1000000000,1000000000))}n)`;
      }
    }
    const expression=value(field);
    if(expression!==undefined)props.push(`${JSON.stringify(key)}:${expression}`);
  }
  return `Object.assign(new bindings.${name}T(),{${props.join(',')}})`;
}
await build({
  stdin:{contents:`export const generators={${schemas.map(s=>`${s.name}:()=>import('sds:${s.name}')`).join(',')}};`,resolveDir:root,sourcefile:'sds-generators.js'},
  outdir:schemaOut,entryNames:'generators',bundle:true,splitting:true,format:'esm',platform:'browser',target:'es2022',minify:true,
  outExtension:{'.js':'.mjs'},
  plugins:[{name:'sds-generators',setup(b){
    b.onResolve({filter:/^sds:/},args=>({path:args.path.slice(4),namespace:'sds'}));
    b.onLoad({filter:/.*/,namespace:'sds'},args=>{
      const s=schemas.find(s=>s.name===args.path);
      return {resolveDir:root,contents:`import * as fb from 'flatbuffers';import * as bindings from 'spacedatastandards.org/lib/js/${s.name}/main.js';
export function generate(id,sequence){const b=new fb.Builder(1024);const record=${objectCode(s.json,s.name)};bindings.${s.name}.finishSizePrefixed${s.name}Buffer(b,record.pack(b));return b.asUint8Array();}`};
    });
  }}],logLevel:'warning'
});
await fs.copyFile(path.join(root,'wasm/flatsql.wasm'),path.join(out,'flatsql.wasm'));
await fs.copyFile(path.join(root,'wasm/integrity.json'),path.join(out,'integrity.json'));
await fs.cp(path.join(root,'wasm/runtime'),path.join(out,'runtime'),{recursive:true});
const entries={'api.mjs':'wasm/index.js','demo.mjs':'docs/unified-demo.js','generator.mjs':'docs/demo-generator.js'};
for(const [name,entry] of Object.entries(entries)) await build({entryPoints:[path.join(root,entry)],outfile:path.join(out,name),bundle:true,format:'esm',platform:'browser',target:'es2022',external:['node:*','./runtime/*','./api.mjs','./sds/*'],minify:true,logLevel:'warning'});
await fs.rm(path.join(out,'samples.fb'),{force:true});
console.log(`Built browser demo: ${schemas.length} SDS types, lazy generators, binary schemas and the identical unified WASM artifact`);
