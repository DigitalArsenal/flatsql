// FlatSQL partition store: a guest thread's worker under the Node host
// (ps-node-host.mjs). Three modes:
//
//   command  the guest's own thread: runs `_start` (the wasm test commands)
//            and reports its exit;
//   reactor  the exec thread of one flatsql-ps-threads.wasm instance: runs
//            `_initialize`, then serves the instance's C ABI calls (init,
//            start, stats, stop) posted by the supervisor;
//   pool     a pooled thread for wasi.thread-spawn: waits on its slot of the
//            pool's SharedArrayBuffer, instantiates the module over the shared
//            memory, runs wasi_thread_start(tid, arg), and returns to idle.
//
// Every mode imports the same host (ps-node-imports.mjs), including
// wasi.thread-spawn, so any guest thread may spawn threads. The supervisor
// on the main thread starts pool workers and is never blocked by the guest.

import { parentPort, threadId, workerData } from "node:worker_threads";
import { writeSync } from "node:fs";

import {
  CONTROL_EXITED,
  CONTROL_EXIT_CODE,
  CONTROL_FAULT,
  CONTROL_FAULT_TID,
  POOL_GEN,
  POOL_HEADER,
  POOL_MAX_RUNNING,
  POOL_RUNNING,
  POOL_SLOT_WORDS,
  SLOT_ASSIGNED,
  SLOT_DEAD,
  SLOT_IDLE,
  SLOT_RUNNING,
  WasiExitError,
  createThreadImports,
  poolThreadSpawn,
  registerThread,
} from "./ps-node-imports.mjs";

const { mode, wasmModule, memory, cfg } = workerData;
// A pool worker counts as a guest OS thread once it runs a guest thread.
let control = mode === "pool" ? new Int32Array(cfg.control) : registerThread(cfg.control);
const pool = new Int32Array(cfg.pool);

const built = createThreadImports(cfg, () => memory);
const imports = {
  ...built.imports,
  env: { ...built.imports.env, memory },
  wasi: { "thread-spawn": poolThreadSpawn(cfg.pool, cfg.spawnWaitMs) },
};

const isExit = (error) => error instanceof WasiExitError || error?.name === "WasiExitError";

function exitProcess(code) {
  if (Atomics.compareExchange(control, CONTROL_EXITED, 0, 1) === 0) {
    Atomics.store(control, CONTROL_EXIT_CODE, code);
  }
  Atomics.notify(control, CONTROL_EXITED);
}

function fault(tid, error) {
  // Written here, synchronously: the thread that would join this one may be
  // blocked in the guest forever.
  try {
    writeSync(2, `[ps-node-host] guest thread ${tid} trapped: ${error?.stack ?? error}\n`);
  } catch {
    // stderr gone
  }
  Atomics.store(control, CONTROL_FAULT_TID, tid);
  Atomics.store(control, CONTROL_FAULT, 1);
  Atomics.notify(control, CONTROL_FAULT);
}

if (mode === "pool") {
  const slot = POOL_HEADER + workerData.slot * POOL_SLOT_WORDS;
  Atomics.store(pool, slot + 3, threadId);
  let registered = false;
  for (;;) {
    Atomics.store(pool, slot, SLOT_IDLE);
    Atomics.add(pool, POOL_GEN, 1);
    Atomics.notify(pool, POOL_GEN);
    for (;;) {
      const state = Atomics.load(pool, slot);
      if (state === SLOT_ASSIGNED) break;
      Atomics.wait(pool, slot, state, 1000);
    }
    const tid = Atomics.load(pool, slot + 1);
    const arg = Atomics.load(pool, slot + 2);
    if (!registered) {
      registerThread(cfg.control);
      registered = true;
    }
    Atomics.store(pool, slot, SLOT_RUNNING);
    const running = Atomics.add(pool, POOL_RUNNING, 1) + 1;
    for (;;) {
      const max = Atomics.load(pool, POOL_MAX_RUNNING);
      if (running <= max || Atomics.compareExchange(pool, POOL_MAX_RUNNING, max, running) === max) break;
    }
    try {
      const instance = new WebAssembly.Instance(wasmModule, imports);
      instance.exports.wasi_thread_start(tid, arg);
    } catch (error) {
      Atomics.sub(pool, POOL_RUNNING, 1);
      Atomics.store(pool, slot, SLOT_DEAD);
      if (isExit(error)) exitProcess(error.code);
      else fault(tid, error);
      break;
    }
    Atomics.sub(pool, POOL_RUNNING, 1);
  }
  built.close();
} else if (mode === "command") {
  const instance = new WebAssembly.Instance(wasmModule, imports);
  let code = 0;
  let error = null;
  try {
    instance.exports._start();
  } catch (e) {
    if (isExit(e)) code = e.code;
    else {
      code = 134;
      error = String(e?.stack ?? e);
    }
  }
  exitProcess(code);
  parentPort.postMessage({ t: "done", code: Atomics.load(control, CONTROL_EXIT_CODE), error });
} else {
  const instance = new WebAssembly.Instance(wasmModule, imports);
  instance.exports._initialize?.();
  const mem = () => new Uint8Array(memory.buffer);
  parentPort.on("message", (msg) => {
    const reply = (value, err) =>
      parentPort.postMessage({ t: "reply", id: msg.id, value, error: err ? String(err?.stack ?? err) : null });
    try {
      if (msg.op === "call") {
        const fn = instance.exports[msg.fn];
        if (typeof fn !== "function") throw new Error(`no export ${msg.fn}`);
        reply(fn(...(msg.args ?? [])));
      } else if (msg.op === "write") {
        const n = msg.bytes.length;
        const ptr = instance.exports.flatsql_ps_alloc(Math.max(n, 1));
        if (!ptr) throw new Error("flatsql_ps_alloc failed");
        mem().set(msg.bytes, ptr);
        reply(ptr);
      } else if (msg.op === "read") {
        reply(mem().slice(msg.ptr, msg.ptr + msg.len));
      } else {
        throw new Error(`unknown op ${msg.op}`);
      }
    } catch (err) {
      if (isExit(err)) exitProcess(err.code);
      reply(undefined, err);
    }
  });
  parentPort.postMessage({ t: "ready" });
}
