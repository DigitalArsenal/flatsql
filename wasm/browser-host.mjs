// Browser transport for the same wasi-threads artifact used by Node/WasmEdge.
export async function createBrowserInstance(bytes, options = {}) {
  if (!globalThis.crossOriginIsolated || typeof SharedArrayBuffer !== 'function')
    throw new Error('FlatSQL requires cross-origin isolation (COOP/COEP) for shared WASM memory');
  const worker = new Worker(options.workerUrl ?? new URL('./browser-guest.mjs', import.meta.url), { type: 'module' });
  let sequence = 0, failure = null;
  const pending = new Map();
  const fail = error => {
    failure = error;
    for (const { reject } of pending.values()) reject(error);
    pending.clear();
  };
  worker.onerror = event => fail(new Error(event.message || 'FlatSQL worker failed'));
  worker.onmessageerror = () => fail(new Error('FlatSQL worker message failed'));
  const rpc = message => new Promise((resolve, reject) => {
    if (failure) return reject(failure);
    const id = ++sequence;
    const timer=setTimeout(()=>{fail(new Error('FlatSQL worker request timed out'));worker.terminate();},30000);
    pending.set(id, { resolve: value=>{clearTimeout(timer);resolve(value);}, reject: error=>{clearTimeout(timer);reject(error);} });
    try { worker.postMessage({ ...message, id }); }
    catch(error){pending.get(id).reject(error);pending.delete(id);}
  });
  worker.onmessage = ({ data }) => {
    if (data.fatal) { fail(new Error(data.fatal)); return; }
    const request = pending.get(data.id);
    if (!request) return;
    pending.delete(data.id);
    if (data.error) request.reject(new Error(data.error)); else request.resolve(data.value);
  };
  try {
    const memory = await rpc({ op: 'init', bytes, options: {
      maximumPages: options.maximumPages, root: options.root, backend: options.backend,
      poolSize: (options.writers ?? 1) * 2 + (options.readers ?? 1) + 6,
      threadWorkerUrl: options.threadWorkerUrl ? String(options.threadWorkerUrl) : undefined,
      ioWorkerUrl: options.ioWorkerUrl ? String(options.ioWorkerUrl) : undefined,
    } });
    return {
      memory,
      call: (fn, ...args) => rpc({ op: 'call', fn, args }),
      write: bytes => rpc({ op: 'write', bytes }),
      read: (ptr, len) => rpc({ op: 'read', ptr, len }),
      faulted: () => !!failure,
      async close() {
        try { if (!failure) await rpc({ op: 'close' }); }
        finally { worker.terminate(); fail(new Error('FlatSQL closed')); }
      },
    };
  } catch (error) { worker.terminate(); throw error; }
}
