// FlatSQL partition store: the per-thread imports of the Node wasi-threads
// host (design §5.6, docs/PARTITION-STORE-WASM.md).
//
// Every guest thread (the thread running `_start` or the reactor's exec
// thread, and every thread the SDK's Node pool spawns for wasi.thread-spawn)
// instantiates the module with the imports built here:
//
//   wasi_snapshot_preview1  WASI preview1 over synchronous `fs`. Descriptors
//                           live in a SharedArrayBuffer table every thread
//                           reads, so a descriptor opened on one thread works
//                           on any other (Node fds are process-wide). Clocks
//                           are process-wide (a worker's performance.now()
//                           has its own origin and would break the engine's
//                           cross-thread time comparisons).
//   env.flatsql_io_*        the seven host I/O imports: the SDK's Node
//                           sync-fs provider over a shared virtual-handle
//                           table, with the fault overlay (ps-node-host.mjs)
//                           interposed on every syscall when configured.
//
// wasi.thread-spawn is a pool of pre-started workers (createThreadPool,
// poolThreadSpawn): any guest thread claims an idle worker through the pool's
// SharedArrayBuffer, so spawning never needs an event loop, workers are
// reused, and a guest thread may spawn threads of its own.

import fs from "node:fs";
import path from "node:path";
import { randomFillSync } from "node:crypto";
import { threadId } from "node:worker_threads";

import { createFlatsqlIoImports } from "space-data-module-sdk/host/flatsql-io";
import { createNodeSyncFsIo } from "space-data-module-sdk/host/flatsql-io/node";

import { createFaultInterposer } from "./ps-node-fault.mjs";

// ---- WASI constants ------------------------------------------------------------
const E = Object.freeze({
  SUCCESS: 0, TOOBIG: 1, ACCES: 2, AGAIN: 6, BADF: 8, EXIST: 20, INVAL: 28, IO: 29, ISDIR: 31,
  LOOP: 32, MFILE: 33, NAMETOOLONG: 37, NOENT: 44, NOSPC: 51, NOSYS: 52, NOTDIR: 54,
  NOTEMPTY: 55, NOTSUP: 58, PERM: 63, SPIPE: 70, XDEV: 75, NOTCAPABLE: 76,
});
const NODE_ERRNO = {
  ENOENT: E.NOENT, EEXIST: E.EXIST, ENOTDIR: E.NOTDIR, EISDIR: E.ISDIR, ENOTEMPTY: E.NOTEMPTY,
  EACCES: E.ACCES, EPERM: E.PERM, EBADF: E.BADF, EINVAL: E.INVAL, ENOSPC: E.NOSPC,
  EMFILE: E.MFILE, ELOOP: E.LOOP, ENAMETOOLONG: E.NAMETOOLONG, EXDEV: E.XDEV, EIO: E.IO,
  EAGAIN: E.AGAIN, ENOTSUP: E.NOTSUP, E2BIG: E.TOOBIG,
};
const errnoOf = (error) => NODE_ERRNO[error?.code] ?? E.IO;

const FT = Object.freeze({ UNKNOWN: 0, CHAR: 2, DIR: 3, FILE: 4, SYMLINK: 7 });
const OFLAG_CREAT = 1;
const OFLAG_DIRECTORY = 2;
const OFLAG_EXCL = 4;
const OFLAG_TRUNC = 8;
const FDFLAG_APPEND = 1;
const RIGHT_FD_READ = 1n << 1n;
const RIGHT_FD_WRITE = 1n << 6n;
const ALL_RIGHTS = (1n << 30n) - 1n;
const LOOKUP_SYMLINK_FOLLOW = 1;
const WHENCE_SET = 0;
const WHENCE_CUR = 1;
const WHENCE_END = 2;
const CLOCK_REALTIME = 0;
const CLOCK_MONOTONIC = 1;
const CLOCK_PROCESS = 2;
const CLOCK_THREAD = 3;

// ---- shared descriptor table ---------------------------------------------------
// Int32 words per descriptor: state, kind, osFd, pathLen, appendFlag, pad x3.
// A parallel Float64Array holds each descriptor's file position. Paths are
// host paths, MAX_PATH bytes each.
const FD_WORDS = 8;
const FD_STATE = 0;
const FD_KIND = 1;
const FD_OSFD = 2;
const FD_PATHLEN = 3;
const FD_APPEND = 4;
const FD_FREE = 0;
const FD_CLAIMING = 1;
const FD_LIVE = 2;
const KIND_STDIO = 1;
const KIND_FILE = 2;
const KIND_DIR = 3;
const KIND_PREOPEN = 4;
const MAX_PATH = 1024;

/**
 * The WASI descriptor table shared by every thread of one guest process.
 * @param {{ preopens: Array<{ guest: string, host: string }>, maxFds?: number }} options
 */
export function createWasiFdTable({ preopens = [], maxFds = 1024 } = {}) {
  const words = maxFds * FD_WORDS;
  const buffer = new SharedArrayBuffer(16 + words * 4 + maxFds * 8 + maxFds * MAX_PATH);
  const header = new Int32Array(buffer, 0, 4);
  header[0] = maxFds;
  header[1] = preopens.length;
  const t = fdTableViews(buffer);
  for (let fd = 0; fd < 3; fd += 1) {
    t.words[fd * FD_WORDS + FD_KIND] = KIND_STDIO;
    t.words[fd * FD_WORDS + FD_OSFD] = fd;
    t.words[fd * FD_WORDS + FD_STATE] = FD_LIVE;
  }
  preopens.forEach((p, i) => {
    const fd = 3 + i;
    const host = Buffer.from(path.resolve(p.host));
    const guest = Buffer.from(p.guest);
    if (host.length + guest.length + 1 > MAX_PATH) throw new RangeError("preopen path too long");
    // Preopen entries store "<guest>\0<host>".
    t.paths.set(guest, fd * MAX_PATH);
    t.paths[fd * MAX_PATH + guest.length] = 0;
    t.paths.set(host, fd * MAX_PATH + guest.length + 1);
    t.words[fd * FD_WORDS + FD_KIND] = KIND_PREOPEN;
    t.words[fd * FD_WORDS + FD_OSFD] = -1;
    t.words[fd * FD_WORDS + FD_PATHLEN] = guest.length + 1 + host.length;
    t.words[fd * FD_WORDS + FD_STATE] = FD_LIVE;
  });
  return buffer;
}

function fdTableViews(buffer) {
  const maxFds = new Int32Array(buffer, 0, 4)[0];
  let off = 16;
  const words = new Int32Array(buffer, off, maxFds * FD_WORDS);
  off += maxFds * FD_WORDS * 4;
  const positions = new Float64Array(buffer, off, maxFds);
  off += maxFds * 8;
  const paths = new Uint8Array(buffer, off, maxFds * MAX_PATH);
  return { maxFds, words, positions, paths };
}

// ---- process-wide clocks -------------------------------------------------------
// hrtime is the system monotonic clock in every thread of the process.
function monotonicNs() {
  return process.hrtime.bigint();
}
function realtimeNs() {
  return BigInt(Math.round((performance.timeOrigin + performance.now()) * 1e6));
}

// ---- control block (supervisor words) --------------------------------------------
// Int32 words: 0 exited, 1 exit code, 2 fault (a guest thread died), 3 live
// guest threads, 4 max live, 5 threads started, 6 next thread-id slot,
// 7 fault tid; then CONTROL_TIDS slots of Node worker threadIds.
export const CONTROL_EXITED = 0;
export const CONTROL_EXIT_CODE = 1;
export const CONTROL_FAULT = 2;
export const CONTROL_LIVE = 3;
export const CONTROL_MAX_LIVE = 4;
export const CONTROL_STARTED = 5;
export const CONTROL_SLOT = 6;
export const CONTROL_FAULT_TID = 7;
export const CONTROL_HEADER = 16;
export const CONTROL_TIDS = 65536;

export function createControlBlock() {
  return new SharedArrayBuffer((CONTROL_HEADER + CONTROL_TIDS) * 4);
}

function registerThread(control) {
  const c = new Int32Array(control);
  const live = Atomics.add(c, CONTROL_LIVE, 1) + 1;
  for (;;) {
    const max = Atomics.load(c, CONTROL_MAX_LIVE);
    if (live <= max || Atomics.compareExchange(c, CONTROL_MAX_LIVE, max, live) === max) break;
  }
  Atomics.add(c, CONTROL_STARTED, 1);
  const slot = Atomics.add(c, CONTROL_SLOT, 1);
  if (slot < CONTROL_TIDS) Atomics.store(c, CONTROL_HEADER + slot, threadId);
  return c;
}

export class WasiExitError extends Error {
  constructor(code) {
    super(`WASI exit with code ${code}`);
    // The SDK pool worker treats this name as a clean thread exit.
    this.name = "WasiExitError";
    this.code = code;
  }
}

// ---- WASI preview1 ---------------------------------------------------------------
/**
 * @param {object} cfg the shared host config (see ps-node-host.mjs)
 * @param {() => WebAssembly.Memory} getMemory
 */
export function createWasi(cfg, getMemory) {
  const table = fdTableViews(cfg.fdTable);
  const control = new Int32Array(cfg.control);
  const encoder = new TextEncoder();
  const args = (cfg.args ?? []).map((a) => encoder.encode(a));
  const env = Object.entries(cfg.env ?? {}).map(([k, v]) => encoder.encode(`${k}=${v}`));
  const stdoutFd = Number.isInteger(cfg.stdoutFd) ? cfg.stdoutFd : 1;
  const stderrFd = Number.isInteger(cfg.stderrFd) ? cfg.stderrFd : 2;
  const sleeper = new Int32Array(new SharedArrayBuffer(4));
  let rngState = null;
  if (cfg.deterministic?.randomSeed !== undefined) {
    rngState = new BigUint64Array(cfg.deterministic.randomState);
  }

  const u8 = () => new Uint8Array(getMemory().buffer);
  const dv = () => new DataView(getMemory().buffer);
  const readString = (ptr, len) => Buffer.from(u8().slice(ptr, ptr + len)).toString("utf8");

  function entry(fd) {
    if (!Number.isInteger(fd) || fd < 0 || fd >= table.maxFds) return null;
    const base = fd * FD_WORDS;
    if (Atomics.load(table.words, base + FD_STATE) !== FD_LIVE) return null;
    return {
      fd,
      base,
      kind: table.words[base + FD_KIND],
      osFd: table.words[base + FD_OSFD],
    };
  }
  function hostPathOf(e) {
    const len = table.words[e.base + FD_PATHLEN];
    const bytes = table.paths.slice(e.fd * MAX_PATH, e.fd * MAX_PATH + len);
    if (e.kind === KIND_PREOPEN) {
      const nul = bytes.indexOf(0);
      return Buffer.from(bytes.subarray(nul + 1)).toString("utf8");
    }
    return Buffer.from(bytes).toString("utf8");
  }
  function preopenGuestName(e) {
    const len = table.words[e.base + FD_PATHLEN];
    const bytes = table.paths.subarray(e.fd * MAX_PATH, e.fd * MAX_PATH + len);
    return Buffer.from(bytes.slice(0, bytes.indexOf(0)));
  }
  function allocFd(kind, osFd, hostPath, append) {
    const bytes = Buffer.from(hostPath);
    if (bytes.length > MAX_PATH) return -E.NAMETOOLONG;
    const first = 3 + new Int32Array(cfg.fdTable, 0, 4)[1];
    for (let fd = first; fd < table.maxFds; fd += 1) {
      const base = fd * FD_WORDS;
      if (Atomics.compareExchange(table.words, base + FD_STATE, FD_FREE, FD_CLAIMING) === FD_FREE) {
        table.paths.set(bytes, fd * MAX_PATH);
        table.words[base + FD_PATHLEN] = bytes.length;
        table.words[base + FD_KIND] = kind;
        table.words[base + FD_OSFD] = osFd;
        table.words[base + FD_APPEND] = append ? 1 : 0;
        table.positions[fd] = 0;
        Atomics.store(table.words, base + FD_STATE, FD_LIVE);
        return fd;
      }
    }
    return -E.MFILE;
  }
  // Resolve a guest path relative to a directory descriptor to a host path,
  // confined below the preopen the directory belongs to.
  function resolveAt(dirfd, pathPtr, pathLen) {
    const e = entry(dirfd);
    if (!e) return { errno: E.BADF };
    if (e.kind !== KIND_DIR && e.kind !== KIND_PREOPEN) return { errno: E.NOTDIR };
    const rel = readString(pathPtr, pathLen);
    if (rel.includes("\0")) return { errno: E.INVAL };
    const base = hostPathOf(e);
    const full = path.resolve(base, rel);
    const root = rootOf(base);
    if (root !== "/" && full !== root && !full.startsWith(root + path.sep)) return { errno: E.NOTCAPABLE };
    return { full };
  }
  function rootOf(hostPath) {
    let best = null;
    const n = new Int32Array(cfg.fdTable, 0, 4)[1];
    for (let i = 0; i < n; i += 1) {
      const root = hostPathOf(entry(3 + i));
      if (root === "/" || hostPath === root || hostPath.startsWith(root + path.sep)) {
        if (!best || root.length > best.length) best = root;
      }
    }
    return best ?? "/";
  }
  function iovecs(iovsPtr, iovsLen) {
    const view = dv();
    const out = [];
    for (let i = 0; i < iovsLen; i += 1) {
      out.push([view.getUint32(iovsPtr + i * 8, true), view.getUint32(iovsPtr + i * 8 + 4, true)]);
    }
    return out;
  }
  function writeStat(ptr, st) {
    const view = dv();
    const type = st.isDirectory() ? FT.DIR : st.isFile() ? FT.FILE : st.isSymbolicLink() ? FT.SYMLINK
      : st.isCharacterDevice() ? FT.CHAR : FT.UNKNOWN;
    view.setBigUint64(ptr, BigInt(st.dev), true);
    view.setBigUint64(ptr + 8, BigInt(st.ino), true);
    view.setUint8(ptr + 16, type);
    view.setBigUint64(ptr + 24, BigInt(st.nlink), true);
    view.setBigUint64(ptr + 32, BigInt(st.size), true);
    view.setBigUint64(ptr + 40, st.atimeNs ?? BigInt(Math.round(st.atimeMs * 1e6)), true);
    view.setBigUint64(ptr + 48, st.mtimeNs ?? BigInt(Math.round(st.mtimeMs * 1e6)), true);
    view.setBigUint64(ptr + 56, st.ctimeNs ?? BigInt(Math.round(st.ctimeMs * 1e6)), true);
  }
  function outputFd(e) {
    if (e.kind === KIND_STDIO) return e.fd === 1 ? stdoutFd : e.fd === 2 ? stderrFd : -1;
    return e.osFd;
  }
  const guarded = (fn) => (...a) => {
    try {
      return fn(...a);
    } catch (error) {
      if (error instanceof WasiExitError) throw error;
      return errnoOf(error);
    }
  };

  const wasi = {
    args_sizes_get(argcPtr, bufPtr) {
      const view = dv();
      view.setUint32(argcPtr, args.length, true);
      view.setUint32(bufPtr, args.reduce((n, a) => n + a.length + 1, 0), true);
      return E.SUCCESS;
    },
    args_get(argvPtr, bufPtr) {
      const view = dv();
      const mem = u8();
      let at = bufPtr;
      args.forEach((a, i) => {
        view.setUint32(argvPtr + i * 4, at, true);
        mem.set(a, at);
        mem[at + a.length] = 0;
        at += a.length + 1;
      });
      return E.SUCCESS;
    },
    environ_sizes_get(countPtr, bufPtr) {
      const view = dv();
      view.setUint32(countPtr, env.length, true);
      view.setUint32(bufPtr, env.reduce((n, a) => n + a.length + 1, 0), true);
      return E.SUCCESS;
    },
    environ_get(envPtr, bufPtr) {
      const view = dv();
      const mem = u8();
      let at = bufPtr;
      env.forEach((a, i) => {
        view.setUint32(envPtr + i * 4, at, true);
        mem.set(a, at);
        mem[at + a.length] = 0;
        at += a.length + 1;
      });
      return E.SUCCESS;
    },
    clock_res_get(id, resPtr) {
      dv().setBigUint64(resPtr, 1000n, true);
      return E.SUCCESS;
    },
    clock_time_get(id, _precision, timePtr) {
      let ns;
      if (id === CLOCK_REALTIME) ns = realtimeNs();
      else if (id === CLOCK_MONOTONIC || id === CLOCK_PROCESS || id === CLOCK_THREAD) ns = monotonicNs();
      else return E.INVAL;
      dv().setBigUint64(timePtr, ns, true);
      return E.SUCCESS;
    },
    fd_advise: () => E.SUCCESS,
    fd_allocate: guarded((fd, offset, len) => {
      const e = entry(fd);
      if (!e || e.kind !== KIND_FILE) return E.BADF;
      const want = Number(offset + len);
      if (fs.fstatSync(e.osFd).size < want) fs.ftruncateSync(e.osFd, want);
      return E.SUCCESS;
    }),
    fd_close: guarded((fd) => {
      const e = entry(fd);
      if (!e) return E.BADF;
      if (e.kind === KIND_STDIO || e.kind === KIND_PREOPEN) return E.SUCCESS;
      if (Atomics.compareExchange(table.words, e.base + FD_STATE, FD_LIVE, FD_CLAIMING) !== FD_LIVE) {
        return E.BADF;
      }
      try {
        if (e.osFd >= 0) fs.closeSync(e.osFd);
      } finally {
        Atomics.store(table.words, e.base + FD_STATE, FD_FREE);
      }
      return E.SUCCESS;
    }),
    fd_datasync: guarded((fd) => {
      const e = entry(fd);
      if (!e) return E.BADF;
      if (e.osFd >= 0 && e.kind === KIND_FILE) fs.fdatasyncSync(e.osFd);
      return E.SUCCESS;
    }),
    fd_sync: guarded((fd) => {
      const e = entry(fd);
      if (!e) return E.BADF;
      if (e.osFd >= 0 && e.kind === KIND_FILE) fs.fsyncSync(e.osFd);
      return E.SUCCESS;
    }),
    fd_fdstat_get: guarded((fd, ptr) => {
      const e = entry(fd);
      if (!e) return E.BADF;
      const view = dv();
      const type = e.kind === KIND_STDIO ? FT.CHAR : e.kind === KIND_FILE ? FT.FILE : FT.DIR;
      view.setUint8(ptr, type);
      view.setUint16(ptr + 2, table.words[e.base + FD_APPEND] ? FDFLAG_APPEND : 0, true);
      view.setBigUint64(ptr + 8, ALL_RIGHTS, true);
      view.setBigUint64(ptr + 16, ALL_RIGHTS, true);
      return E.SUCCESS;
    }),
    fd_fdstat_set_flags: guarded((fd, flags) => {
      const e = entry(fd);
      if (!e) return E.BADF;
      table.words[e.base + FD_APPEND] = flags & FDFLAG_APPEND ? 1 : 0;
      return E.SUCCESS;
    }),
    fd_filestat_get: guarded((fd, ptr) => {
      const e = entry(fd);
      if (!e) return E.BADF;
      if (e.kind === KIND_STDIO) {
        dv().setUint8(ptr + 16, FT.CHAR);
        return E.SUCCESS;
      }
      const st = e.osFd >= 0 ? fs.fstatSync(e.osFd, { bigint: true }) : fs.statSync(hostPathOf(e), { bigint: true });
      writeStat(ptr, st);
      return E.SUCCESS;
    }),
    fd_filestat_set_size: guarded((fd, size) => {
      const e = entry(fd);
      if (!e || e.kind !== KIND_FILE) return E.BADF;
      fs.ftruncateSync(e.osFd, Number(size));
      return E.SUCCESS;
    }),
    fd_filestat_set_times: () => E.SUCCESS,
    fd_pread: guarded((fd, iovsPtr, iovsLen, offset, nreadPtr) => {
      const e = entry(fd);
      if (!e || e.kind !== KIND_FILE) return E.BADF;
      let pos = Number(offset);
      let total = 0;
      for (const [ptr, len] of iovecs(iovsPtr, iovsLen)) {
        const tmp = Buffer.alloc(len);
        const n = fs.readSync(e.osFd, tmp, 0, len, pos);
        u8().set(tmp.subarray(0, n), ptr);
        total += n;
        pos += n;
        if (n < len) break;
      }
      dv().setUint32(nreadPtr, total, true);
      return E.SUCCESS;
    }),
    fd_pwrite: guarded((fd, iovsPtr, iovsLen, offset, nwrittenPtr) => {
      const e = entry(fd);
      if (!e || e.kind !== KIND_FILE) return E.BADF;
      let pos = Number(offset);
      let total = 0;
      for (const [ptr, len] of iovecs(iovsPtr, iovsLen)) {
        const n = fs.writeSync(e.osFd, Buffer.from(u8().slice(ptr, ptr + len)), 0, len, pos);
        total += n;
        pos += n;
      }
      dv().setUint32(nwrittenPtr, total, true);
      return E.SUCCESS;
    }),
    fd_read: guarded((fd, iovsPtr, iovsLen, nreadPtr) => {
      const e = entry(fd);
      if (!e) return E.BADF;
      if (e.kind === KIND_STDIO) {
        dv().setUint32(nreadPtr, 0, true);  // no stdin
        return fd === 0 ? E.SUCCESS : E.BADF;
      }
      if (e.kind !== KIND_FILE) return E.ISDIR;
      let total = 0;
      for (const [ptr, len] of iovecs(iovsPtr, iovsLen)) {
        const tmp = Buffer.alloc(len);
        const pos = table.positions[fd];
        const n = fs.readSync(e.osFd, tmp, 0, len, pos);
        table.positions[fd] = pos + n;
        u8().set(tmp.subarray(0, n), ptr);
        total += n;
        if (n < len) break;
      }
      dv().setUint32(nreadPtr, total, true);
      return E.SUCCESS;
    }),
    fd_write: guarded((fd, iovsPtr, iovsLen, nwrittenPtr) => {
      const e = entry(fd);
      if (!e) return E.BADF;
      const out = outputFd(e);
      if (out < 0) return E.BADF;
      let total = 0;
      for (const [ptr, len] of iovecs(iovsPtr, iovsLen)) {
        if (len === 0) continue;
        const bytes = Buffer.from(u8().slice(ptr, ptr + len));
        if (e.kind === KIND_STDIO) {
          let done = 0;
          while (done < len) done += fs.writeSync(out, bytes, done, len - done);
          total += len;
          continue;
        }
        let pos = table.words[e.base + FD_APPEND] ? fs.fstatSync(out).size : table.positions[fd];
        const n = fs.writeSync(out, bytes, 0, len, pos);
        table.positions[fd] = pos + n;
        total += n;
      }
      dv().setUint32(nwrittenPtr, total, true);
      return E.SUCCESS;
    }),
    fd_prestat_get(fd, ptr) {
      const e = entry(fd);
      if (!e || e.kind !== KIND_PREOPEN) return E.BADF;
      const view = dv();
      view.setUint8(ptr, 0);
      view.setUint32(ptr + 4, preopenGuestName(e).length, true);
      return E.SUCCESS;
    },
    fd_prestat_dir_name(fd, ptr, len) {
      const e = entry(fd);
      if (!e || e.kind !== KIND_PREOPEN) return E.BADF;
      const name = preopenGuestName(e);
      u8().set(name.subarray(0, len), ptr);
      return E.SUCCESS;
    },
    fd_readdir: guarded((fd, bufPtr, bufLen, cookie, usedPtr) => {
      const e = entry(fd);
      if (!e || (e.kind !== KIND_DIR && e.kind !== KIND_PREOPEN)) return E.BADF;
      const dir = hostPathOf(e);
      const names = [".", "..", ...fs.readdirSync(dir)];
      const view = dv();
      const mem = u8();
      let at = 0;
      for (let i = Number(cookie); i < names.length; i += 1) {
        const name = Buffer.from(names[i]);
        let type = FT.UNKNOWN;
        let ino = 0n;
        try {
          const st = fs.lstatSync(path.join(dir, names[i]), { bigint: true });
          type = st.isDirectory() ? FT.DIR : st.isFile() ? FT.FILE : st.isSymbolicLink() ? FT.SYMLINK : FT.UNKNOWN;
          ino = st.ino;
        } catch {
          // raced with an unlink: report it with an unknown type
        }
        const head = new Uint8Array(24);
        const hv = new DataView(head.buffer);
        hv.setBigUint64(0, BigInt(i + 1), true);
        hv.setBigUint64(8, ino, true);
        hv.setUint32(16, name.length, true);
        hv.setUint8(20, type);
        const record = Buffer.concat([Buffer.from(head), name]);
        const room = bufLen - at;
        if (room <= 0) break;
        mem.set(record.subarray(0, Math.min(room, record.length)), bufPtr + at);
        at += Math.min(room, record.length);
        if (record.length > room) break;
      }
      view.setUint32(usedPtr, at, true);
      return E.SUCCESS;
    }),
    fd_renumber: () => E.NOTSUP,
    fd_seek: guarded((fd, offset, whence, newPtr) => {
      const e = entry(fd);
      if (!e) return E.BADF;
      if (e.kind === KIND_STDIO) return E.SPIPE;
      if (e.kind !== KIND_FILE) return E.ISDIR;
      const off = Number(offset);
      let pos;
      if (whence === WHENCE_SET) pos = off;
      else if (whence === WHENCE_CUR) pos = table.positions[fd] + off;
      else if (whence === WHENCE_END) pos = fs.fstatSync(e.osFd).size + off;
      else return E.INVAL;
      if (pos < 0) return E.INVAL;
      table.positions[fd] = pos;
      dv().setBigUint64(newPtr, BigInt(pos), true);
      return E.SUCCESS;
    }),
    fd_tell: guarded((fd, ptr) => {
      const e = entry(fd);
      if (!e || e.kind !== KIND_FILE) return E.BADF;
      dv().setBigUint64(ptr, BigInt(table.positions[fd]), true);
      return E.SUCCESS;
    }),
    path_create_directory: guarded((fd, p, n) => {
      const r = resolveAt(fd, p, n);
      if (r.errno) return r.errno;
      fs.mkdirSync(r.full);
      return E.SUCCESS;
    }),
    path_filestat_get: guarded((fd, flags, p, n, ptr) => {
      const r = resolveAt(fd, p, n);
      if (r.errno) return r.errno;
      const st = flags & LOOKUP_SYMLINK_FOLLOW
        ? fs.statSync(r.full, { bigint: true })
        : fs.lstatSync(r.full, { bigint: true });
      writeStat(ptr, st);
      return E.SUCCESS;
    }),
    path_filestat_set_times: () => E.SUCCESS,
    path_link: () => E.NOTSUP,
    path_open: guarded((dirfd, dirflags, p, n, oflags, rightsBase, _rightsInh, fdflags, fdPtr) => {
      const r = resolveAt(dirfd, p, n);
      if (r.errno) return r.errno;
      let st = null;
      try {
        st = fs.statSync(r.full);
      } catch (error) {
        if (error?.code !== "ENOENT" || !(oflags & OFLAG_CREAT)) throw error;
      }
      if (st && oflags & OFLAG_CREAT && oflags & OFLAG_EXCL) return E.EXIST;
      if (oflags & OFLAG_DIRECTORY || (st && st.isDirectory())) {
        if (st && !st.isDirectory()) return E.NOTDIR;
        if (!st) return E.NOENT;
        const fd = allocFd(KIND_DIR, -1, r.full, false);
        if (fd < 0) return -fd;
        dv().setUint32(fdPtr, fd, true);
        return E.SUCCESS;
      }
      const rights = BigInt.asUintN(64, BigInt(rightsBase));
      const read = (rights & RIGHT_FD_READ) !== 0n;
      const write = (rights & RIGHT_FD_WRITE) !== 0n || (oflags & OFLAG_TRUNC) !== 0;
      const c = fs.constants;
      let mode = write ? (read ? c.O_RDWR : c.O_WRONLY) : c.O_RDONLY;
      if (oflags & OFLAG_CREAT) mode |= c.O_CREAT;
      if (oflags & OFLAG_EXCL) mode |= c.O_EXCL;
      if (oflags & OFLAG_TRUNC) mode |= c.O_TRUNC;
      const osFd = fs.openSync(r.full, mode, 0o644);
      const fd = allocFd(KIND_FILE, osFd, r.full, (fdflags & FDFLAG_APPEND) !== 0);
      if (fd < 0) {
        fs.closeSync(osFd);
        return -fd;
      }
      dv().setUint32(fdPtr, fd, true);
      return E.SUCCESS;
    }),
    path_readlink: guarded((fd, p, n, buf, bufLen, usedPtr) => {
      const r = resolveAt(fd, p, n);
      if (r.errno) return r.errno;
      const target = Buffer.from(fs.readlinkSync(r.full));
      const count = Math.min(bufLen, target.length);
      u8().set(target.subarray(0, count), buf);
      dv().setUint32(usedPtr, count, true);
      return E.SUCCESS;
    }),
    path_remove_directory: guarded((fd, p, n) => {
      const r = resolveAt(fd, p, n);
      if (r.errno) return r.errno;
      fs.rmdirSync(r.full);
      return E.SUCCESS;
    }),
    path_rename: guarded((fd, p, n, fd2, p2, n2) => {
      const a = resolveAt(fd, p, n);
      if (a.errno) return a.errno;
      const b = resolveAt(fd2, p2, n2);
      if (b.errno) return b.errno;
      fs.renameSync(a.full, b.full);
      return E.SUCCESS;
    }),
    path_symlink: () => E.NOTSUP,
    path_unlink_file: guarded((fd, p, n) => {
      const r = resolveAt(fd, p, n);
      if (r.errno) return r.errno;
      if (fs.lstatSync(r.full).isDirectory()) return E.ISDIR;
      fs.unlinkSync(r.full);
      return E.SUCCESS;
    }),
    poll_oneoff: guarded((inPtr, outPtr, nsubs, neventsPtr) => {
      // Clock subscriptions sleep until the earliest deadline; descriptor
      // subscriptions are always ready (the guest never polls real I/O).
      const view = dv();
      const now = monotonicNs();
      let earliest = null;
      const subs = [];
      for (let i = 0; i < nsubs; i += 1) {
        const s = inPtr + i * 48;
        const userdata = view.getBigUint64(s, true);
        const tag = view.getUint8(s + 8);
        if (tag === 0) {
          const clock = view.getUint32(s + 16, true);
          const timeout = view.getBigUint64(s + 24, true);
          const absolute = view.getUint16(s + 40, true) & 1;
          let deadline;
          if (absolute) deadline = clock === CLOCK_REALTIME ? now + (timeout - realtimeNs()) : timeout;
          else deadline = now + timeout;
          subs.push({ userdata, tag, deadline });
          if (earliest === null || deadline < earliest) earliest = deadline;
        } else {
          subs.push({ userdata, tag, deadline: now });
          earliest = now;
        }
      }
      if (earliest !== null && earliest > now) {
        const ms = Number(earliest - now) / 1e6;
        Atomics.wait(sleeper, 0, 0, ms);
      }
      const after = monotonicNs();
      let n = 0;
      for (const sub of subs) {
        if (sub.deadline > after) continue;
        const o = outPtr + n * 32;
        view.setBigUint64(o, sub.userdata, true);
        view.setUint16(o + 8, 0, true);
        view.setUint8(o + 10, sub.tag);
        view.setBigUint64(o + 16, 0n, true);
        view.setUint16(o + 24, 0, true);
        n += 1;
      }
      view.setUint32(neventsPtr, n, true);
      return E.SUCCESS;
    }),
    proc_exit(code) {
      // Any thread's exit ends the whole guest: the supervisor sees the word.
      if (Atomics.compareExchange(control, CONTROL_EXITED, 0, 1) === 0) {
        Atomics.store(control, CONTROL_EXIT_CODE, code);
      }
      Atomics.notify(control, CONTROL_EXITED);
      throw new WasiExitError(code);
    },
    proc_raise: () => E.NOSYS,
    random_get(ptr, len) {
      const out = new Uint8Array(len);
      if (rngState) {
        // Deterministic mode: one process-wide xorshift64* stream.
        for (let i = 0; i < len; i += 8) {
          let x;
          for (;;) {
            const old = Atomics.load(rngState, 0);
            x = old;
            x ^= x >> 12n;
            x ^= (x << 25n) & 0xffffffffffffffffn;
            x ^= x >> 27n;
            if (Atomics.compareExchange(rngState, 0, old, x) === old) break;
          }
          let v = (x * 0x2545f4914f6cdd1dn) & 0xffffffffffffffffn;
          for (let k = 0; k < 8 && i + k < len; k += 1) {
            out[i + k] = Number(v & 0xffn);
            v >>= 8n;
          }
        }
      } else {
        randomFillSync(out);
      }
      u8().set(out, ptr);
      return E.SUCCESS;
    },
    sched_yield: () => E.SUCCESS,
    sock_accept: () => E.NOSYS,
    sock_recv: () => E.NOSYS,
    sock_send: () => E.NOSYS,
    sock_shutdown: () => E.NOSYS,
  };
  return wasi;
}

// ---- flatsql_io -------------------------------------------------------------------
export function createFlatsqlIo(cfg, getMemory) {
  const interpose = cfg.fault ? createFaultInterposer(cfg.fault, cfg.ioRoot) : undefined;
  const provider = createNodeSyncFsIo({
    root: cfg.ioRoot,
    table: cfg.ioTable,
    instanceId: cfg.instanceId,
    getMemory,
    interpose,
  });
  return {
    imports: createFlatsqlIoImports({ getMemory, provider }),
    close: () => provider.closeLocalFds(),
  };
}

/**
 * The complete import fragment for one guest thread (without wasi.thread-spawn
 * and env.memory, which the caller adds).
 */
export function createThreadImports(cfg, getMemory) {
  const wasi = createWasi(cfg, getMemory);
  const io = createFlatsqlIo(cfg, getMemory);
  return {
    imports: { wasi_snapshot_preview1: wasi, env: { ...io.imports.env } },
    close: io.close,
  };
}

export { registerThread };

// ---- thread pool ------------------------------------------------------------------
// Int32 words: 0 cap, 1 workers created, 2 workers wanted, 3 next tid,
// 4 running guest threads, 5 max running, 6 spawned, 7 declined,
// 8 pool generation (bumped whenever a worker becomes idle); then per slot
// POOL_SLOT_WORDS: state, tid, start argument, worker threadId.
export const POOL_CAP = 0;
export const POOL_CREATED = 1;
export const POOL_WANT = 2;
export const POOL_NEXT_TID = 3;
export const POOL_RUNNING = 4;
export const POOL_MAX_RUNNING = 5;
export const POOL_SPAWNED = 6;
export const POOL_DECLINED = 7;
export const POOL_GEN = 8;
export const POOL_HEADER = 16;
export const POOL_SLOT_WORDS = 4;
export const SLOT_EMPTY = 0;
export const SLOT_STARTING = 1;
export const SLOT_IDLE = 2;
export const SLOT_CLAIMED = 3;
export const SLOT_ASSIGNED = 4;
export const SLOT_RUNNING = 5;
export const SLOT_DEAD = 6;
// wasi-libc keeps a thread id in the low 30 bits of a mutex word.
const MAX_TID = (1 << 29) - 1;

export function createThreadPool(cap) {
  const buffer = new SharedArrayBuffer((POOL_HEADER + cap * POOL_SLOT_WORDS) * 4);
  const w = new Int32Array(buffer);
  w[POOL_CAP] = cap;
  w[POOL_NEXT_TID] = 1;
  return buffer;
}

/**
 * wasi.thread-spawn over the pool: returns the new thread's tid, or -6
 * (EAGAIN: pthread_create fails) when no worker frees up within waitMs and
 * the pool is at its cap.
 */
export function poolThreadSpawn(poolBuffer, waitMs = 60000) {
  const w = new Int32Array(poolBuffer);
  const cap = w[POOL_CAP];
  return (startArg) => {
    const tid = Atomics.add(w, POOL_NEXT_TID, 1);
    if (tid > MAX_TID) return -6;
    const deadline = performance.now() + waitMs;
    let askedAt = -Infinity;
    for (;;) {
      const created = Math.min(Atomics.load(w, POOL_CREATED), cap);
      for (let i = 0; i < created; i += 1) {
        const at = POOL_HEADER + i * POOL_SLOT_WORDS;
        if (Atomics.compareExchange(w, at, SLOT_IDLE, SLOT_CLAIMED) !== SLOT_IDLE) continue;
        Atomics.store(w, at + 1, tid);
        Atomics.store(w, at + 2, startArg);
        Atomics.store(w, at, SLOT_ASSIGNED);
        Atomics.notify(w, at);
        Atomics.add(w, POOL_SPAWNED, 1);
        return tid;
      }
      const now = performance.now();
      if (created < cap && now - askedAt > 50) {
        // Ask the supervisor (whose event loop is free) for one more worker.
        Atomics.add(w, POOL_WANT, 1);
        Atomics.notify(w, POOL_WANT);
        askedAt = now;
      }
      if (now > deadline) {
        Atomics.add(w, POOL_DECLINED, 1);
        return -6;
      }
      const gen = Atomics.load(w, POOL_GEN);
      Atomics.wait(w, POOL_GEN, gen, 5);
    }
  };
}

