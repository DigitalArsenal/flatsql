// FlatSQL partition store: the Node wasi-threads host (design §5.6, T4;
// docs/PARTITION-STORE-WASM.md).
//
// Runs flatsql-ps-threads.wasm (a reactor) and the wasm test commands on
// Node 24+ with real threads:
//   - the guest's own thread is a worker (ps-node-guest.mjs); this thread
//     supervises and is never blocked by the guest;
//   - wasi.thread-spawn claims a pre-started worker from a pool this thread
//     keeps (grown on demand up to a cap); workers are reused, so a guest that
//     starts and stops engines thousands of times never exhausts it, and any
//     guest thread may spawn;
//   - env.flatsql_io_* is space-data-module-sdk's Node sync-fs provider over a
//     shared virtual-handle table; the fault overlay (ps-node-fault.mjs) is
//     interposed on every syscall when `fault` is given;
//   - WASI preview1 is this host's own (ps-node-imports.mjs): shared
//     descriptors, process-wide clocks, preopens.
//
// Requires the optional peer dependency space-data-module-sdk >= 0.8.24.
//
//   node wasm/ps-node-host.mjs <command.wasm> [args...]
//     runs a wasm command in a private root (FLATSQL_PS_ROOT to choose it,
//     FLATSQL_PS_KEEP_ROOT=1 to keep it), with the checkout mounted at its own
//     path and the FLATSQL_PS_* / PS_* environment; FLATSQL_PS_TIMEOUT_MS
//     bounds the run. Prints the thread report to stderr (and writes it as
//     JSON to FLATSQL_PS_REPORT when set).

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";

import { createNodeSyncFsIoTable } from "space-data-module-sdk/host/flatsql-io/node";

import {
  CONTROL_EXITED,
  CONTROL_EXIT_CODE,
  CONTROL_FAULT,
  CONTROL_FAULT_TID,
  CONTROL_HEADER,
  CONTROL_SLOT,
  CONTROL_STARTED,
  CONTROL_TIDS,
  POOL_CREATED,
  POOL_DECLINED,
  POOL_MAX_RUNNING,
  POOL_RUNNING,
  POOL_SPAWNED,
  POOL_WANT,
  createControlBlock,
  createThreadPool,
  createWasiFdTable,
} from "./ps-node-imports.mjs";

const GUEST_URL = new URL("./ps-node-guest.mjs", import.meta.url);

/** The shared memory limits a module imports (env.memory), in pages. */
export function importedMemoryLimits(bytes) {
  const u8 = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  let at = 8;
  const leb = () => {
    let result = 0;
    let shift = 0;
    for (;;) {
      const b = u8[at++];
      result += (b & 0x7f) * 2 ** shift;
      shift += 7;
      if (!(b & 0x80)) return result;
    }
  };
  const name = () => {
    const n = leb();
    const s = Buffer.from(u8.subarray(at, at + n)).toString("utf8");
    at += n;
    return s;
  };
  while (at < u8.length) {
    const id = u8[at++];
    const size = leb();
    const end = at + size;
    if (id === 2) {
      const count = leb();
      for (let i = 0; i < count; i += 1) {
        const mod = name();
        const field = name();
        const kind = u8[at++];
        if (kind === 0) leb();
        else if (kind === 1) {
          at += 1;
          const flags = leb();
          leb();
          if (flags & 1) leb();
        } else if (kind === 2) {
          const flags = leb();
          const initial = leb();
          const maximum = flags & 1 ? leb() : undefined;
          return { module: mod, name: field, initial, maximum, shared: (flags & 2) !== 0 };
        } else if (kind === 3) {
          at += 2;
        } else {
          throw new Error(`unknown import kind ${kind}`);
        }
      }
      return null;
    }
    at = end;
  }
  return null;
}

// The guest's file system: `root` (a host directory) is the guest's "/" for
// WASI and the flatsql_io root; `mounts` preopen further host directories at
// guest paths. With no root a private temporary directory is created and
// removed afterwards.
function prepareRoot(options) {
  if (options.root) {
    fs.mkdirSync(options.root, { recursive: true });
    return { root: path.resolve(options.root), cleanup: () => {} };
  }
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "flatsql-ps-node-"));
  return {
    root,
    cleanup: () => {
      if (!options.keepRoot) fs.rmSync(root, { recursive: true, force: true });
    },
  };
}

function hostConfig(options, root) {
  fs.mkdirSync(path.join(root, "tmp"), { recursive: true });
  const preopens = [{ guest: "/", host: root }, ...(options.mounts ?? [])];
  let deterministic = null;
  if (options.deterministic) {
    const randomState = new SharedArrayBuffer(8);
    new BigUint64Array(randomState)[0] = BigInt(options.deterministic.randomSeed ?? 0x9e3779b97f4a7c15n) | 1n;
    deterministic = { randomSeed: String(options.deterministic.randomSeed ?? 0), randomState };
  }
  return {
    args: options.args ?? [],
    env: { TMPDIR: "/tmp", ...(options.env ?? {}) },
    fdTable: createWasiFdTable({ preopens }),
    control: createControlBlock(),
    pool: createThreadPool(options.maxThreads ?? 512),
    spawnWaitMs: options.spawnWaitMs ?? 60000,
    ioRoot: root,
    ioTable: createNodeSyncFsIoTable({ maxHandles: options.maxHandles ?? 16384 }),
    instanceId: options.instanceId ?? 0,
    fault: options.fault?.buffer ?? null,
    stdoutFd: options.stdoutFd,
    stderrFd: options.stderrFd,
    deterministic,
  };
}

async function loadModule(source) {
  if (source instanceof WebAssembly.Module) return { module: source, bytes: null };
  const bytes = typeof source === "string" ? fs.readFileSync(source) : new Uint8Array(source);
  return { module: await WebAssembly.compile(bytes), bytes };
}

function createMemory(bytes, options) {
  const limits = importedMemoryLimits(bytes);
  if (!limits || limits.module !== "env" || limits.name !== "memory" || !limits.shared) {
    throw new Error("the module must import a shared env.memory");
  }
  const initial = Math.max(limits.initial, options.initialPages ?? 0);
  const maximum = Math.min(limits.maximum ?? 65536, options.maximumPages ?? 65536);
  return new WebAssembly.Memory({ initial, maximum, shared: true });
}

/**
 * One guest process: its memory, its file system, its thread pool, and the
 * workers that run it. The supervisor starts pool workers when a spawn asks
 * for one and watches for a guest fault or exit.
 */
class Supervisor {
  constructor(module, memory, cfg, options) {
    this.module = module;
    this.memory = memory;
    this.cfg = cfg;
    this.control = new Int32Array(cfg.control);
    this.pool = new Int32Array(cfg.pool);
    this.cap = this.pool[0];
    this.workers = new Set();
    this.listeners = new Set();
    this.stopped = false;
    this.poolErrors = [];
    const warm = Math.min(options.warmThreads ?? 16, this.cap);
    for (let i = 0; i < warm; i += 1) this.startPoolWorker();
    this.timer = setInterval(() => this.tick(), 2);
  }

  startPoolWorker() {
    const slot = Atomics.add(this.pool, POOL_CREATED, 1);
    if (slot >= this.cap) {
      Atomics.sub(this.pool, POOL_CREATED, 1);
      return;
    }
    const worker = new Worker(GUEST_URL, {
      workerData: { mode: "pool", slot, wasmModule: this.module, memory: this.memory, cfg: this.cfg },
    });
    worker.on("error", (error) => this.poolErrors.push(String(error?.stack ?? error)));
    worker.once("exit", () => this.workers.delete(worker));
    this.workers.add(worker);
  }

  tick() {
    if (this.stopped) return;
    const want = Atomics.exchange(this.pool, POOL_WANT, 0);
    for (let i = 0; i < want; i += 1) this.startPoolWorker();
    for (const listener of this.listeners) listener();
  }

  startGuest(mode) {
    const worker = new Worker(GUEST_URL, {
      workerData: { mode, wasmModule: this.module, memory: this.memory, cfg: this.cfg },
    });
    this.workers.add(worker);
    worker.once("exit", () => this.workers.delete(worker));
    return worker;
  }

  exited() {
    return Atomics.load(this.control, CONTROL_EXITED) !== 0;
  }

  faulted() {
    return Atomics.load(this.control, CONTROL_FAULT) !== 0;
  }

  report() {
    const c = this.control;
    const slots = Math.min(Atomics.load(c, CONTROL_SLOT), CONTROL_TIDS);
    const tids = new Set();
    for (let i = 0; i < slots; i += 1) tids.add(Atomics.load(c, CONTROL_HEADER + i));
    return {
      guestThreadsSpawned: Atomics.load(this.pool, POOL_SPAWNED),
      // Pool threads running at once, plus the guest's own thread.
      maxConcurrentGuestThreads: Atomics.load(this.pool, POOL_MAX_RUNNING) + 1,
      runningGuestThreads: Atomics.load(this.pool, POOL_RUNNING),
      spawnsDeclined: Atomics.load(this.pool, POOL_DECLINED),
      workerThreads: Atomics.load(c, CONTROL_STARTED),
      distinctWorkerThreadIds: tids.size,
      memoryBytes: this.memory.buffer.byteLength,
    };
  }

  async stop() {
    this.stopped = true;
    clearInterval(this.timer);
    await Promise.all([...this.workers].map((w) => w.terminate().catch(() => {})));
    this.workers.clear();
  }
}

/**
 * Run a wasi-threads command (the ps test commands) to completion.
 *
 * @param {string|Uint8Array} wasm path or bytes
 * @param {object} [options]
 * @param {string[]} [options.args] argv after argv[0]
 * @param {Record<string,string>} [options.env] (TMPDIR defaults to "/tmp")
 * @param {string} [options.root] host directory the guest sees as "/" (WASI
 *   and flatsql_io); default a private temporary directory, removed after
 * @param {boolean} [options.keepRoot] keep that temporary directory
 * @param {Array<{guest:string,host:string}>} [options.mounts] more preopens
 * @param {number} [options.maxThreads] pool cap (default 512)
 * @param {number} [options.warmThreads] workers started up front (default 16)
 * @param {number} [options.timeoutMs] stop the guest after this long
 * @param {number} [options.killAfterMs] kill the guest after this long (a
 *   crash, not a failure: the result has killed: true)
 * @param {() => void} [options.beforeKill] runs just before a kill (the fault
 *   overlay freezes here)
 * @param {object} [options.fault] a fault overlay (createFaultOverlay)
 * @param {number} [options.stdoutFd] guest stdout goes to this fd (default 1)
 * @param {{ randomSeed?: bigint }} [options.deterministic] seeded random_get
 * @returns {Promise<{ exitCode: number, killed: boolean, timedOut: boolean,
 *   fault: boolean, error: string|null, elapsedMs: number, threads: object }>}
 */
export async function runPsCommand(wasm, options = {}) {
  const { module, bytes } = await loadModule(wasm);
  const memory = createMemory(bytes ?? options.bytes, options);
  const argv0 = typeof wasm === "string" ? path.basename(wasm) : "command.wasm";
  const fsRoot = prepareRoot(options);
  const cfg = hostConfig({ ...options, args: [argv0, ...(options.args ?? [])] }, fsRoot.root);
  const started = performance.now();
  const sup = new Supervisor(module, memory, cfg, options);
  const guest = sup.startGuest("command");
  const result = await new Promise((resolve) => {
    let settled = false;
    const timers = [];
    const settle = (value) => {
      if (settled) return;
      settled = true;
      for (const t of timers) clearTimeout(t);
      sup.listeners.clear();
      resolve(value);
    };
    const base = { exitCode: 0, killed: false, timedOut: false, fault: false, error: null };
    guest.on("message", (msg) => {
      if (msg.t === "done") settle({ ...base, exitCode: msg.code, error: msg.error, fault: msg.error !== null });
    });
    guest.on("error", (error) => settle({ ...base, exitCode: 134, error: String(error?.stack ?? error), fault: true }));
    guest.on("exit", (code) => settle({ ...base, exitCode: code || 134, error: `guest worker exited (${code})`, fault: true }));
    // A pool thread that exits the process or traps: the guest's own thread
    // may be blocked in a join forever, so decide from here.
    sup.listeners.add(() => {
      if (sup.faulted()) {
        settle({
          ...base,
          exitCode: 134,
          error: `guest thread ${Atomics.load(sup.control, CONTROL_FAULT_TID)} trapped`,
          fault: true,
        });
      } else if (sup.exited()) {
        timers.push(setTimeout(() => settle({ ...base, exitCode: Atomics.load(sup.control, CONTROL_EXIT_CODE) }), 100));
      }
    });
    if (options.timeoutMs) {
      timers.push(setTimeout(() => settle({ ...base, exitCode: 124, timedOut: true, error: `timed out after ${options.timeoutMs} ms` }), options.timeoutMs));
    }
    if (options.killAfterMs !== undefined) {
      timers.push(setTimeout(() => {
        options.beforeKill?.();
        settle({ ...base, exitCode: 137, killed: true });
      }, options.killAfterMs));
    }
  });
  const threads = sup.report();
  await sup.stop();
  fsRoot.cleanup();
  return { ...result, elapsedMs: performance.now() - started, threads, root: fsRoot.root, poolErrors: sup.poolErrors };
}

/**
 * Instantiate flatsql-ps-threads.wasm as one instance (writer or reader) with
 * an exec thread of its own. Returns an RPC handle for the C ABI:
 * `call(fn, ...args)`, `write(bytes) -> ptr`, `read(ptr, len)`, `threads()`,
 * `close()`.
 */
export async function createPsNodeInstance(wasm, options = {}) {
  const { module, bytes } = await loadModule(wasm);
  const memory = createMemory(bytes ?? options.bytes, options);
  const fsRoot = prepareRoot(options);
  const cfg = hostConfig(options, fsRoot.root);
  const sup = new Supervisor(module, memory, cfg, options);
  const worker = sup.startGuest("reactor");
  const pending = new Map();
  let nextId = 1;
  let failure = null;
  const fail = (error) => {
    failure = error;
    for (const p of pending.values()) p.reject(error);
    pending.clear();
  };
  sup.listeners.add(() => {
    if (!failure && sup.faulted()) fail(new Error(`guest thread ${Atomics.load(sup.control, CONTROL_FAULT_TID)} trapped`));
  });
  await new Promise((resolve, reject) => {
    worker.on("message", (msg) => {
      if (msg.t === "ready") resolve();
      else if (msg.t === "reply") {
        const p = pending.get(msg.id);
        pending.delete(msg.id);
        if (!p) return;
        if (msg.error) p.reject(new Error(msg.error));
        else p.resolve(msg.value);
      }
    });
    worker.on("error", (error) => {
      fail(error);
      reject(error);
    });
  });
  const rpc = (msg) => {
    if (failure) return Promise.reject(failure);
    const id = nextId++;
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      worker.postMessage({ ...msg, id });
    });
  };
  return {
    memory,
    root: fsRoot.root,
    call: (fn, ...args) => rpc({ op: "call", fn, args }),
    write: (data) => rpc({ op: "write", bytes: data instanceof Uint8Array ? data : new Uint8Array(data) }),
    read: (ptr, len) => rpc({ op: "read", ptr, len }),
    threads: () => sup.report(),
    faulted: () => sup.faulted(),
    async close() {
      await sup.stop();
      fsRoot.cleanup();
    },
  };
}

// ---- CLI ---------------------------------------------------------------------
const invokedDirectly = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (invokedDirectly) {
  const [wasmPath, ...args] = process.argv.slice(2);
  if (!wasmPath) {
    process.stderr.write("usage: node wasm/ps-node-host.mjs <command.wasm> [args...]\n");
    process.exit(2);
  }
  const env = {};
  for (const [k, v] of Object.entries(process.env)) if (k.startsWith("FLATSQL_PS_") || k.startsWith("PS_")) env[k] = v;
  const timeoutMs = Number(process.env.FLATSQL_PS_TIMEOUT_MS ?? 0) || undefined;
  // The checkout is visible at its own path (the test commands read their
  // vectors from the build tree); everything else lives in a private root.
  const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const r = await runPsCommand(wasmPath, {
    args,
    env,
    timeoutMs,
    root: process.env.FLATSQL_PS_ROOT || undefined,
    keepRoot: process.env.FLATSQL_PS_KEEP_ROOT === "1",
    mounts: [{ guest: repo, host: repo }],
  });
  const t = r.threads;
  if (process.env.FLATSQL_PS_REPORT) {
    fs.writeFileSync(process.env.FLATSQL_PS_REPORT, `${JSON.stringify({ ...r, root: undefined })}\n`);
  }
  process.stderr.write(
    `ps-node-host: exit ${r.exitCode}${r.error ? ` (${r.error})` : ""}; guest threads spawned ${t.guestThreadsSpawned}, ` +
      `max concurrent ${t.maxConcurrentGuestThreads} on ${t.distinctWorkerThreadIds} distinct worker threads` +
      `${t.spawnsDeclined ? `, ${t.spawnsDeclined} spawns declined` : ""}; memory ${(t.memoryBytes / 2 ** 20).toFixed(0)} MiB; ` +
      `${(r.elapsedMs / 1000).toFixed(1)} s\n`,
  );
  process.exit(r.exitCode);
}
