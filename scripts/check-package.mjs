// Inspect and exercise the tarball customers install, without development deps.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
const temp=await fs.mkdtemp(path.join(os.tmpdir(),'flatsql-package-e2e-'));
function run(command,args,options={}) {
 const r=spawnSync(command,args,{encoding:'utf8',...options});
 assert.equal(r.status,0,`${command}: ${r.error??r.stderr}\n${r.stdout}`);return r.stdout;
}
try {
 const [pack]=JSON.parse(run('npm',['pack','--ignore-scripts','--json','--pack-destination',temp]));
 const files=pack.files.map(f=>f.path);
 for(const name of files) assert.match(name,/^(?:package\.json|README\.md|LICENSE|docs\/API-3\.12\.md|bin\/flatsql\.mjs|cpp\/README\.md|cpp\/include\/flatsql\/flatsql(?:_io)?\.h|wasm\/(?:index\.(?:js|d\.ts)|flatsql\.wasm|integrity\.json|runtime\/[\w.-]+\.(?:mjs|txt)))$/,name);
 assert.deepEqual(files.filter(f=>f.endsWith('.wasm')),['wasm/flatsql.wasm']);
 assert.ok(pack.size<2_000_000,`Compressed package grew beyond 2 MB: ${pack.size}`);
 run('tar',['-xzf',path.join(temp,pack.filename),'-C',temp]);
 const pkg=path.join(temp,'package');
 const manifest=JSON.parse(await fs.readFile(path.join(pkg,'package.json'),'utf8'));
 assert.deepEqual(Object.keys(manifest.exports).sort(),['.','./flatsql.wasm','./integrity.json']);
 assert.equal(Object.keys(manifest.dependencies??{}).length,0);
 assert.equal(Object.keys(manifest.peerDependencies??{}).length,0);
 const api=await import(pathToFileURL(path.join(pkg,'wasm/index.js')));
 assert.deepEqual(Object.keys(api),['FlatSQL','FlatSQLError']);
 const output=run(process.execPath,['scripts/ci-unified-e2e.mjs'],{env:{...process.env,FLATSQL_TEST_PACKAGE:pkg},timeout:60000});
 process.stdout.write(output);
 console.log(`PASS: installed tarball, one API/artifact, no runtime dependencies; ${pack.size} compressed bytes, ${pack.unpackedSize} unpacked bytes`);
} finally { await fs.rm(temp,{recursive:true,force:true}); }
