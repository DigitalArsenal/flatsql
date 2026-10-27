// Bundle only the generic host adapters from the pinned, published SDK.
// The package has no SDK -> old FlatSQL dependency cycle at runtime.
import { build } from 'esbuild';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const root=fileURLToPath(new URL('../',import.meta.url));
const output=path.join(root,'wasm/runtime');
await fs.mkdir(output,{recursive:true});
const entries={
 'node-host.mjs':['wasm/node-host.mjs','node'],
 'node-guest.mjs':['wasm/node-guest.mjs','node'],
 'browser-host.mjs':['wasm/browser-host.mjs','browser'],
 'browser-guest.mjs':['wasm/browser-guest.mjs','browser'],
 'wasiThreadBrowserWorker.mjs':['node_modules/space-data-module-sdk/src/host/wasiThreadBrowserWorker.mjs','browser'],
 'opfsIoWorker.mjs':['node_modules/space-data-module-sdk/src/host/opfsIoWorker.mjs','browser'],
};
for(const [name,[entry,platform]] of Object.entries(entries)) await build({entryPoints:[path.join(root,entry)],outfile:path.join(output,name),bundle:true,format:'esm',platform,target:'es2022',external:['node:*'],minify:true,logLevel:'warning',legalComments:'eof'});
const sdk=JSON.parse(await fs.readFile(path.join(root,'node_modules/space-data-module-sdk/package.json'),'utf8'));
await fs.writeFile(path.join(output,'NOTICE.txt'),`Generic host adapters bundled from space-data-module-sdk ${sdk.version} (Apache-2.0).\nhttps://github.com/DigitalArsenal/space-data-module-sdk\n`);
await fs.copyFile(path.join(root,'node_modules/space-data-module-sdk/LICENSE'),path.join(output,'LICENSE.sdk.txt'));
console.log(`Bundled generic runtime adapters from SDK ${sdk.version}`);
