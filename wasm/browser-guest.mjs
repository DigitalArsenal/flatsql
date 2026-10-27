import { createBrowserWasiShim, createSharedWasiProcess } from 'space-data-module-sdk/host/wasi-shim';
import { createWasiThreadSpawn } from 'space-data-module-sdk/host/wasi-threads';
import { createFlatsqlIoWorker, resolveExtraImports, mergeImportFragments } from 'space-data-module-sdk/host/flatsql-io';
let memory, instance, io, threads, extras;
const fatal = error => postMessage({ fatal: String(error?.stack ?? error) });
async function init(bytes, options) {
  const module = await WebAssembly.compile(bytes);
  memory = new WebAssembly.Memory({ initial: 512, maximum: options.maximumPages ?? 8192, shared: true });
  const processState = createSharedWasiProcess({ env: {} });
  io = await createFlatsqlIoWorker({ backend: options.backend ?? 'opfs', rootDirectory: options.root ?? 'flatsql',
    workerUrl: options.ioWorkerUrl, onError: fatal, lock: { name: `flatsql/${options.root ?? 'flatsql'}`, ifAvailable: true } });
  await io.attachMemory(0, memory);
  const descriptors = [{ provider: 'flatsql-io', instanceId: 0, channels: [io.buffer] }];
  const wasi = createBrowserWasiShim({ processState });
  wasi.setMemory(memory);
  extras = resolveExtraImports(descriptors, { getMemory: () => memory, memory });
  const imports = mergeImportFragments({ ...wasi.imports, env: { memory }, wasi: {} }, extras.fragments);
  // Grow the heap before workers can touch it; V8 shared-memory bounds caches
  // can otherwise observe different lengths on simultaneous guest threads.
  let threadSpawn = () => -1;
  imports.wasi['thread-spawn'] = arg => threadSpawn(arg);
  instance = await WebAssembly.instantiate(module, imports);
  instance.exports._initialize();
  const allocations = [];
  const maxBytes = (options.maximumPages ?? 8192) * 65536;
  for (let step = 2 ** 30; step >= 2 ** 20 && allocations.length < 64;) {
    const p = memory.buffer.byteLength + step + 2 ** 20 <= maxBytes ? instance.exports.flatsql_alloc(step) : 0;
    if (p) allocations.push(p); else step /= 2;
  }
  for (const p of allocations) instance.exports.flatsql_free(p);
  threads = await createWasiThreadSpawn({ wasmModule: module, memory, processState,
    poolSize: options.poolSize, extraImports: descriptors, instanceId: 0,
    enableBrowserThreads: true, browserWorkerUrl: options.threadWorkerUrl, onGuestError: (_id, _tid, e) => fatal(e) });
  threadSpawn = threads.threadSpawn;
  return memory;
}
async function close() {
  await threads?.terminateAll();
  extras?.close();
  await io?.stop();
}
onmessage = async ({ data }) => {
  try {
    let value;
    if (data.op === 'init') {
      try { value = await init(data.bytes, data.options); }
      catch (error) { await close(); throw error; }
    } else if (data.op === 'call') value = instance.exports[data.fn](...data.args);
    else if (data.op === 'write') {
      value = instance.exports.flatsql_alloc(Math.max(1, data.bytes.length));
      if (!value) throw new Error('FlatSQL allocation failed');
      new Uint8Array(memory.buffer).set(data.bytes, value);
    } else if (data.op === 'read') value = new Uint8Array(memory.buffer).slice(data.ptr, data.ptr + data.len);
    else if (data.op === 'close') await close();
    else throw new Error('Unknown FlatSQL worker operation');
    postMessage({ id: data.id, value });
  } catch (error) { postMessage({ id: data.id, error: String(error?.stack ?? error) }); }
};
