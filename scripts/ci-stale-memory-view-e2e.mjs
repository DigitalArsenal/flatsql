// End to end: a pool worker whose view of the shared memory is stale still
// starts its guest thread. Under FlatSQL 3.12.0-3.12.4 that thread trapped at
// open: "guest thread N trapped: RuntimeError: memory access out of bounds"
// in __wasi_thread_start_C.
//
// A worker that receives the shared WebAssembly.Memory gets a
// SharedArrayBuffer of the memory's size when the message starts to
// deserialize, and joins the memory's grow notifications only when the
// Memory object is attached, after everything ahead of it. A grow in between
// never reaches that worker. The Node host grows the heap once, before the
// first guest thread, and never again (node-host.mjs heapLimitBytes), so
// nothing refreshes the view. V8 bounds-checks atomics against it: a new
// thread's first atomic, its tid store at about 55 MiB, traps against 32 MiB.
//
// Natural runs hit that window rarely (about 0.5% of check:package runs on
// GitHub's Linux runners). Here it is certain: the host's first pool worker
// takes the memory from a message with a million objects ahead of it, the
// heap grows while they deserialize, and the guest's first thread lands on
// that worker. The runtime runs unchanged; only node:worker_threads' Worker
// is steered. The test fails if that thread traps, if the worker starts it
// with a stale view, or if open or close fails.
//
// A Node whose workers see the grow anyway cannot have this race. If no
// attempt leaves the worker with a stale view, the test prints a line that
// starts with "SKIP:" and exits 0 (ci-e2e.sh reports it as SKIP).
// Usage: node scripts/ci-stale-memory-view-e2e.mjs [opens]
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { syncBuiltinESMExports } from 'node:module';
import { pathToFileURL } from 'node:url';
import threads from 'node:worker_threads';
import { CONTROL_FAULT, POOL_HEADER, SLOT_IDLE, SLOT_RUNNING } from '../wasm/node-imports.mjs';

// The probe's handshake words, then the view its first guest thread got.
const STEP = 0, GO = 1, VIEW_BYTES = 2, GROWN = 3, THREAD_VIEW_BYTES = 4;
const ATTEMPTS = 3;
const READY = 1, RECEIVING = 2, RECEIVED = 3;
// The pool's first slot: its state, then the tid of its guest thread.
const SLOT0 = POOL_HEADER;

if (!threads.isMainThread) {
  // The probe: the host's first pool worker. It receives the memory while the
  // heap grows, then runs the unchanged guest (node-guest.mjs) over it.
  const { workerData } = threads;
  const sync = new Int32Array(workerData.probeSync);
  const step = value => { Atomics.store(sync, STEP, value); Atomics.notify(sync, STEP); };
  step(READY);
  Atomics.wait(sync, GO, 0);
  step(RECEIVING);
  const { memory } = threads.receiveMessageOnPort(workerData.probePort).message;
  Atomics.wait(sync, GROWN, 0);
  Atomics.store(sync, VIEW_BYTES, memory.buffer.byteLength);
  step(RECEIVED);
  workerData.memory = memory;
  // Record the view this worker has when it instantiates its first guest
  // thread.
  const { Instance } = WebAssembly;
  WebAssembly.Instance = function ProbeInstance(module, imports) {
    Atomics.compareExchange(sync, THREAD_VIEW_BYTES, 0, memory.buffer.byteLength);
    return new Instance(module, imports);
  };
  await import(workerData.probeGuest);
} else {
  const packageRoot = process.env.FLATSQL_TEST_PACKAGE;
  const opens = Number(process.argv[2] ?? 2);
  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
  const until = async test => {
    for (const deadline = Date.now() + 30000; !test();) {
      if (Date.now() > deadline) throw new Error('stale-view probe: handshake timed out');
      await sleep(1);
    }
  };
  let probe = null;
  // Hold the reactor's messages: growHeap until the probe deserializes, the
  // next one until the probe is the pool's lowest idle slot.
  const steerReactor = worker => {
    const post = worker.postMessage.bind(worker);
    let queue = Promise.resolve(), phase = 'before';
    worker.postMessage = (message, ...rest) => {
      queue = queue.then(async () => {
        try {
          if (message?.op === 'growHeap') {
            await until(() => Atomics.load(probe.sync, STEP) >= READY);
            Atomics.store(probe.sync, GO, 1); Atomics.notify(probe.sync, GO);
            await until(() => Atomics.load(probe.sync, STEP) >= RECEIVING);
            await sleep(20);
            phase = 'growing';
          } else if (phase === 'growing') {
            phase = 'grown';
            Atomics.store(probe.sync, GROWN, 1); Atomics.notify(probe.sync, GROWN);
            await until(() => Atomics.load(probe.sync, STEP) === RECEIVED && Atomics.load(probe.pool, SLOT0) === SLOT_IDLE);
          }
        } catch (error) { probe.error = error; }
        post(message, ...rest);
      });
    };
  };
  const BaseWorker = threads.Worker;
  threads.Worker = class SteeredWorker extends BaseWorker {
    constructor(url, options = {}) {
      const data = options.workerData;
      if (data?.mode === 'pool' && data.slot === 0) {
        const { memory, ...rest } = data;
        const channel = new threads.MessageChannel();
        channel.port1.postMessage({ pad: Array.from({ length: 1_000_000 }, (_, i) => ({ i })), memory });
        channel.port1.unref();
        const sync = new SharedArrayBuffer(32);
        super(new URL(import.meta.url), { ...options, workerData: { ...rest, probeGuest: String(url), probePort: channel.port2, probeSync: sync }, transferList: [channel.port2] });
        probe = { sync: new Int32Array(sync), pool: new Int32Array(data.cfg.pool), control: new Int32Array(data.cfg.control),
          memory, before: memory.buffer.byteLength, port: channel.port1, error: null };
      } else {
        super(url, options);
        if (data?.mode === 'reactor') steerReactor(this);
      }
    }
  };
  syncBuiltinESMExports();
  const { FlatSQL } = await import(packageRoot ? pathToFileURL(path.join(packageRoot, 'wasm/index.js')) : '../wasm/index.js');
  const began = performance.now();
  const MiB = bytes => (bytes / 2 ** 20).toFixed(1);
  let verified = 0, skipped = null;
  for (let open = 1; open <= opens && !skipped; open += 1) {
    const missed = [];
    for (let attempt = 1; ; attempt += 1) {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), 'flatsql-stale-view-'));
      let db = null, failure = null;
      try {
        db = await FlatSQL.open({ root });
        // Guest threads start after flatsql_start returns: wait for the
        // probe's, and long enough for a trap to be reported.
        await until(() => Atomics.load(probe.pool, SLOT0) >= SLOT_RUNNING);
        await until(() => Atomics.load(probe.sync, THREAD_VIEW_BYTES) !== 0);
        await sleep(200);
        await db.close();
      } catch (error) { failure = error; }
      finally {
        await db?.close().catch(() => {});
        probe.port.close();
        await fs.rm(root, { recursive: true, force: true });
      }
      const view = Atomics.load(probe.sync, VIEW_BYTES), size = probe.memory.buffer.byteLength;
      const threadView = Atomics.load(probe.sync, THREAD_VIEW_BYTES);
      const ranThread = Atomics.load(probe.pool, SLOT0 + 1) > 0;
      const stale = view === probe.before && size > view;
      // A trap or a failed open or close fails the test, stale view or not.
      assert.equal(Atomics.load(probe.control, CONTROL_FAULT), 0,
        stale ? 'a guest thread trapped on the pool worker with a stale memory view' : 'a guest thread trapped');
      assert.equal(failure, null, `open/close failed: ${failure?.message}`);
      // The precondition: the probe's view stayed at the size before the heap
      // grew, and the probe took the guest's first thread.
      if (!probe.error && stale && ranThread) {
        assert.equal(threadView, size,
          `the pool worker started its guest thread with a stale memory view (${MiB(threadView)} of ${MiB(size)} MiB)`);
        console.log(`open ${open}: pool worker view ${MiB(view)} MiB of ${MiB(size)} MiB ran the first guest thread (attempt ${attempt})`);
        verified += 1;
        break;
      }
      missed.push({ view, size, ranThread, error: probe.error,
        // The heap grew and the probe's view already showed it.
        refreshed: !probe.error && size > probe.before && view === size });
      if (attempt < ATTEMPTS) continue;
      if (verified === 0 && missed.every(m => m.refreshed)) {
        skipped = { attempts: attempt, size };
        break;
      }
      assert.fail(`stale-view probe: precondition not met in ${attempt} attempts (before ${probe.before}; `
        + missed.map(m => `view ${m.view}, size ${m.size}, thread ${m.ranThread}, ${m.error ?? 'no handshake error'}`).join('; ') + ')');
    }
  }
  const ms = Math.round(performance.now() - began);
  if (skipped) {
    console.log(`SKIP: no stale memory view could be produced on Node ${process.versions.node}; the race is absent here; skipped `
      + `(in ${skipped.attempts} attempts the pool worker's view already showed the grown heap, ${MiB(skipped.size)} MiB; V8 ${process.versions.v8}; ${ms} ms)`);
  } else {
    console.log(`PASS: ${opens} opens with a stale pool-worker memory view (${ms} ms)`);
  }
}
