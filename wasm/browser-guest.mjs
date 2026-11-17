import { createBrowserWasiShim, createSharedWasiProcess } from 'space-data-module-sdk/host/wasi-shim';
import { createWasiThreadSpawn } from 'space-data-module-sdk/host/wasi-threads';
import { createFlatsqlIoWorker, resolveExtraImports, mergeImportFragments } from 'space-data-module-sdk/host/flatsql-io';
let memory, instance, io, threads, extras, engine;
let threadSpawn = () => -1;
const fatal = error => postMessage({ fatal: String(error?.stack ?? error) });
// A browser engine thread is a pooled worker that must load its script and
// instantiate the module over the shared memory (arm) before the engine starts:
// a spawn the pool cannot serve is a std::thread failure, and the engine is
// built without exceptions. Each attempt gives the workers this long to arm.
const ARMING_MS = 8000;
const ATTEMPTS = 3;
// Threads flatsql_start creates (cpp/src/p4/mailbox.cpp startThreads): every
// service thread of the mailbox layout, one indexer per writer, the slow-work
// thread and the checkpoint thread. Layout offsets: cpp/include/flatsql/flatsql.h.
const LAYOUT_BYTES = 640, LAYOUT_THREADS = 120, LAYOUT_CLASSES = 128, CLASS_WRITE = 1, ENGINE_THREADS = 2;
function threadsToStart() {
  const p = instance.exports.flatsql_alloc(LAYOUT_BYTES);
  if (!p) throw new Error('FlatSQL allocation failed');
  try {
    if (instance.exports.flatsql_layout(p) !== LAYOUT_BYTES) throw new Error('Invalid FlatSQL mailbox layout');
    const layout = new DataView(memory.buffer, p, LAYOUT_BYTES);
    const service = layout.getUint32(LAYOUT_THREADS, true);
    let writers = 0;
    for (let i = 0; i < service; i++) if (layout.getUint32(LAYOUT_CLASSES + 4 * i, true) === CLASS_WRITE) writers++;
    return service + writers + ENGINE_THREADS;
  } finally { instance.exports.flatsql_free(p); }
}
class FlatSQLThreadPoolError extends Error {
  constructor(needed, armedPerAttempt) {
    const armed = Math.max(...armedPerAttempt);
    super(`FlatSQL starts ${needed} engine threads; the browser armed at most ${armed} of ${needed} thread workers ` +
      `(${armedPerAttempt.join(', ')} in ${armedPerAttempt.length} attempts of ${ARMING_MS} ms each), so the engine was not started`);
    this.name = 'FlatSQLThreadPoolError';
    this.detail = { name: this.name, message: this.message, needed, armed, armedPerAttempt };
  }
}
// Start the engine only on a pool that holds every thread it starts.
async function start() {
  const needed = threadsToStart();
  const armedPerAttempt = [];
  for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
    threads = await createWasiThreadSpawn({ wasmModule: engine.module, memory, processState: engine.processState,
      poolSize: needed, probeTimeoutMs: ARMING_MS, extraImports: engine.descriptors, instanceId: 0,
      enableBrowserThreads: true, browserWorkerUrl: engine.threadWorkerUrl, onGuestError: (_id, _tid, e) => fatal(e) });
    const report = threads.spawnReport();
    armedPerAttempt.push(report.armed);
    if (report.poolSize === needed && report.armed === needed) {
      threadSpawn = threads.threadSpawn;
      return instance.exports.flatsql_start();
    }
    await threads.terminateAll();
    threads = null;
  }
  throw new FlatSQLThreadPoolError(needed, armedPerAttempt);
}
async function init(bytes, options) {
  const module = await WebAssembly.compile(bytes);
  memory = new WebAssembly.Memory({ initial: 512, maximum: options.maximumPages ?? 8192, shared: true });
  const processState = createSharedWasiProcess({ env: {} });
  io = await createFlatsqlIoWorker({ backend: options.backend ?? 'opfs', rootDirectory: options.root ?? 'flatsql',
    workerUrl: options.ioWorkerUrl, onError: fatal,
    // Memory stores own disjoint backing maps. Only durable OPFS stores share
    // files and require an origin-wide lock on their root.
    lock: options.backend === 'memory' ? undefined : { name: `flatsql/${options.root ?? 'flatsql'}`, ifAvailable: true } });
  await io.attachMemory(0, memory);
  const descriptors = [{ provider: 'flatsql-io', instanceId: 0, channels: [io.buffer] }];
  const wasi = createBrowserWasiShim({ processState });
  wasi.setMemory(memory);
  extras = resolveExtraImports(descriptors, { getMemory: () => memory, memory });
  const imports = mergeImportFragments({ ...wasi.imports, env: { memory }, wasi: {} }, extras.fragments);
  // Grow the heap before workers can touch it; V8 shared-memory bounds caches
  // can otherwise observe different lengths on simultaneous guest threads.
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
  // The pool is made at start, sized from the configured engine's layout.
  engine = { module, processState, descriptors, threadWorkerUrl: options.threadWorkerUrl };
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
    } else if (data.op === 'start') value = await start();
    else if (data.op === 'call') {
      if (data.fn === 'flatsql_start') throw new Error('Start the FlatSQL engine with the start operation');
      value = instance.exports[data.fn](...data.args);
    } else if (data.op === 'write') {
      value = instance.exports.flatsql_alloc(Math.max(1, data.bytes.length));
      if (!value) throw new Error('FlatSQL allocation failed');
      new Uint8Array(memory.buffer).set(data.bytes, value);
    } else if (data.op === 'read') value = new Uint8Array(memory.buffer).slice(data.ptr, data.ptr + data.len);
    else if (data.op === 'close') await close();
    else throw new Error('Unknown FlatSQL worker operation');
    postMessage({ id: data.id, value });
  } catch (error) { postMessage({ id: data.id, error: String(error?.stack ?? error), detail: error?.detail }); }
};
