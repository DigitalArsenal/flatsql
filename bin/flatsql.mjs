#!/usr/bin/env node
// Binary streams only. Schema/text conversion is a separate program.
import fs from 'node:fs/promises';
import { once } from 'node:events';
import { FlatSQL } from '../wasm/index.js';

const help = `Usage:
  flatsql ingest --root DIR --schema FILE.bfbs --type NAME --id '$FID' < records.fb > receipts.fb
  flatsql query --root DIR [--records] [--request FILE.fb] > results.fb

Ingest reads concatenated size-prefixed FlatBuffers from stdin.
Query reads one SDS QRP FlatBuffer request from stdin or --request.
Results are size-prefixed SDS QRP pages, or unchanged record frames with --records.
Optional ingest metadata: --provider NAME --source NAME --producer NAME --batch NAME.
Use a separate converter such as flatc-wasm for text input and output.
`;
async function main() {
  const [command,...args]=process.argv.slice(2), options={};
  if (!command || command==='--help' || command==='-h') { process.stdout.write(help); return; }
  const allowed=new Set(['root','schema','type','id','request','provider','source','producer','batch']);
  for (let i=0;i<args.length;i++) {
    const key=args[i].slice(2);
    if (args[i]==='--records') { options.records=true; continue; }
    if (!args[i].startsWith('--') || !allowed.has(key) || !args[i+1] || args[i+1].startsWith('--'))
      throw new Error(`Unknown or incomplete option: ${args[i]}`);
    options[key]=args[++i];
  }
  if (!options.root || !['ingest','query'].includes(command)) throw new Error(help);
  if (command==='ingest' && (!options.schema || !options.type || !options.id)) throw new Error(help);
  const signal=new AbortController();
  const interrupt=()=>signal.abort();
  process.once('SIGINT',interrupt); process.once('SIGTERM',interrupt);
  const write=async data=>{ if (!process.stdout.write(data)) await once(process.stdout,'drain'); };
  const db=await FlatSQL.open({root:options.root});
  try {
    if (command==='query') {
      let request;
      if (options.request) {
        if ((await fs.stat(options.request)).size>8*1024*1024) throw new Error('QRP request exceeds 8 MiB');
        request=await fs.readFile(options.request);
      } else {
        const parts=[];let size=0;
        for await (const chunk of process.stdin) {
          size+=chunk.length;if(size>8*1024*1024)throw new Error('QRP request exceeds 8 MiB');parts.push(chunk);
        }
        request=Buffer.concat(parts);
      }
      for await (const chunk of db.query(request,{records:options.records,signal:signal.signal})) await write(chunk);
    } else {
      await db.registerSchema({name:options.type,fileIdentifier:options.id,bfbs:await fs.readFile(options.schema)});
      const limit=4*1024*1024;
      let pending=Buffer.alloc(0),batch=[],size=0;
      const flush=async()=>{
        if(!size)return;
        await write(await db.ingest(options.type,Buffer.concat(batch,size),{...options,signal:signal.signal}));
        batch=[];size=0;
      };
      for await(const chunk of process.stdin) {
        pending=Buffer.concat([pending,chunk]);let at=0;
        while(pending.length-at>=4) {
          const n=pending.readUInt32LE(at)+4;
          if(n<12 || n>limit)throw new Error('Record frame must be between 12 bytes and 4 MiB');
          if(pending.length-at<n)break;
          if(size+n>limit)await flush();
          batch.push(Buffer.from(pending.subarray(at,at+n)));size+=n;at+=n;
        }
        pending=Buffer.from(pending.subarray(at));
      }
      if(pending.length)throw new Error('Truncated FlatBuffer record frame');
      await flush();
    }
  } finally {
    await db.close();process.removeListener('SIGINT',interrupt);process.removeListener('SIGTERM',interrupt);
  }
}
main().catch(error=>{process.stderr.write(`flatsql: ${error.message}\n`);process.exitCode=1;});
