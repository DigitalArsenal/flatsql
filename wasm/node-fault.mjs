// FlatSQL partition store: the Node host's fault overlay (design §19,
// 22.3a-7; docs/PARTITION-STORE-WASM.md).
//
// A SharedArrayBuffer page table every worker consults. It sits under the
// SDK's Node sync-fs flatsql_io provider (its `interpose` hook) and records,
// for every file, what a power loss could take away:
//   - the pre-image of every 4 KiB page written since the file's last sync,
//     and the size the file had at that sync;
//   - files created since their parent directory's last sync.
// The supervisor freezes the overlay (every later mutating call fails with
// EIO, as from a dying process), stops the guest, then applies a crash:
//   dropAll     every unsynced page write and size change is undone, and
//               entries created since the parent's last sync disappear;
//   pageSubset  each unsynced page independently survives or is undone
//               (writeback in any order), and the size is the synced or the
//               current one;
//   keepAll     kill -9 with the page cache intact: nothing is undone.
// Reads and syncs pass through. The overlay never touches data the guest has
// synced: a record acked after its durable append must survive every mode.

import fs from "node:fs";
import path from "node:path";

export const PAGE = 4096;
const MAGIC = 0x46534c54; // "FSLT"

// Header words.
const H_MAGIC = 0;
const H_FROZEN = 1;
const H_OVERFLOW = 2;
const H_MAX_FILES = 3;
const H_MAX_PAGES = 4;
const H_NEXT_PAGE = 5;
const H_WRITES = 6;
const H_SYNCS = 7;
const H_WORDS = 16;
// File words: state, lock, flags, head page entry, pathLen, pad x3.
const F_WORDS = 8;
const F_STATE = 0;
const F_LOCK = 1;
const F_FLAGS = 2;
const F_HEAD = 3;
const F_PATHLEN = 4;
const F_EMPTY = 0;
const F_CLAIM = 1;
const F_LIVE = 2;
const FLAG_TRACKED = 1; // synced size recorded
const FLAG_NEW_ENTRY = 2; // created since the parent directory's last sync
const MAX_PATH = 1024;
// Page entry words: fileId, pageNo, preimage length, next.
const P_WORDS = 4;

const mix = (s) => {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i += 1) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h;
};

/**
 * Allocate an overlay. `maxPages` bounds the unsynced pages tracked at once
 * (4 KiB each); overflowing it is reported, never silently ignored.
 */
export function createFaultOverlay({ maxFiles = 16384, maxPages = 65536 } = {}) {
  const bytes =
    H_WORDS * 4 + maxFiles * F_WORDS * 4 + maxFiles * 8 + maxFiles * MAX_PATH + maxPages * P_WORDS * 4 + maxPages * PAGE;
  const buffer = new SharedArrayBuffer(bytes);
  const h = new Int32Array(buffer, 0, H_WORDS);
  h[H_MAGIC] = MAGIC;
  h[H_MAX_FILES] = maxFiles;
  h[H_MAX_PAGES] = maxPages;
  const v = views(buffer);
  v.files.fill(0);
  for (let i = 0; i < maxFiles; i += 1) v.files[i * F_WORDS + F_HEAD] = -1;
  return { buffer, ...overlayApi(buffer) };
}

function views(buffer) {
  const h = new Int32Array(buffer, 0, H_WORDS);
  if (h[H_MAGIC] !== MAGIC) throw new TypeError("not a fault overlay");
  const maxFiles = h[H_MAX_FILES];
  const maxPages = h[H_MAX_PAGES];
  let off = H_WORDS * 4;
  const files = new Int32Array(buffer, off, maxFiles * F_WORDS);
  off += maxFiles * F_WORDS * 4;
  const syncedSize = new Float64Array(buffer, off, maxFiles);
  off += maxFiles * 8;
  const paths = new Uint8Array(buffer, off, maxFiles * MAX_PATH);
  off += maxFiles * MAX_PATH;
  const pages = new Int32Array(buffer, off, maxPages * P_WORDS);
  off += maxPages * P_WORDS * 4;
  const images = new Uint8Array(buffer, off, maxPages * PAGE);
  return { h, maxFiles, maxPages, files, syncedSize, paths, pages, images };
}

function lock(v, id) {
  const at = id * F_WORDS + F_LOCK;
  while (Atomics.compareExchange(v.files, at, 0, 1) !== 0) Atomics.wait(v.files, at, 1, 1);
}
function unlock(v, id) {
  const at = id * F_WORDS + F_LOCK;
  Atomics.store(v.files, at, 0);
  Atomics.notify(v.files, at, 1);
}

function pathOf(v, id) {
  const len = v.files[id * F_WORDS + F_PATHLEN];
  return Buffer.from(v.paths.slice(id * MAX_PATH, id * MAX_PATH + len)).toString("utf8");
}

// File id for a host path (open addressing), created on first sight.
function fileId(v, full, create) {
  const bytes = Buffer.from(full);
  if (bytes.length > MAX_PATH) return -1;
  let i = mix(full) % v.maxFiles;
  for (let n = 0; n < v.maxFiles; n += 1, i = (i + 1) % v.maxFiles) {
    const at = i * F_WORDS;
    let state = Atomics.load(v.files, at + F_STATE);
    if (state === F_EMPTY) {
      if (!create) return -1;
      if (Atomics.compareExchange(v.files, at + F_STATE, F_EMPTY, F_CLAIM) === F_EMPTY) {
        v.paths.set(bytes, i * MAX_PATH);
        v.files[at + F_PATHLEN] = bytes.length;
        v.files[at + F_FLAGS] = 0;
        v.files[at + F_HEAD] = -1;
        Atomics.store(v.files, at + F_STATE, F_LIVE);
        Atomics.notify(v.files, at + F_STATE);
        return i;
      }
      state = Atomics.load(v.files, at + F_STATE);
    }
    while (state === F_CLAIM) {
      Atomics.wait(v.files, at + F_STATE, F_CLAIM, 1);
      state = Atomics.load(v.files, at + F_STATE);
    }
    if (v.files[at + F_PATHLEN] === bytes.length && pathOf(v, i) === full) return i;
  }
  Atomics.store(v.h, H_OVERFLOW, 1);
  return -1;
}

function sizeOf(fd) {
  return fs.fstatSync(fd).size;
}

// Record the synced size the first time a file is written after a sync.
function track(v, id, fd) {
  const at = id * F_WORDS + F_FLAGS;
  if (!(v.files[at] & FLAG_TRACKED)) {
    v.syncedSize[id] = sizeOf(fd);
    v.files[at] |= FLAG_TRACKED;
  }
}

// Save the pre-image of page `p` unless it is already saved since the last
// sync. Pages wholly past the synced size need none: a crash cuts the file
// back to that size, or (pageSubset keeping the size) leaves new or zero bytes
// there, both of which a real disk can produce.
function savePage(v, id, fd, p) {
  if (p * PAGE >= v.syncedSize[id]) return true;
  for (let e = v.files[id * F_WORDS + F_HEAD]; e >= 0; e = v.pages[e * P_WORDS + 3]) {
    if (v.pages[e * P_WORDS + 1] === p) return true;
  }
  const e = Atomics.add(v.h, H_NEXT_PAGE, 1);
  if (e >= v.maxPages) {
    Atomics.store(v.h, H_OVERFLOW, 1);
    return false;
  }
  const img = v.images.subarray(e * PAGE, (e + 1) * PAGE);
  const tmp = Buffer.alloc(PAGE);
  const n = fs.readSync(fd, tmp, 0, PAGE, p * PAGE);
  img.set(tmp);
  v.pages[e * P_WORDS] = id;
  v.pages[e * P_WORDS + 1] = p;
  v.pages[e * P_WORDS + 2] = n;
  v.pages[e * P_WORDS + 3] = v.files[id * F_WORDS + F_HEAD];
  v.files[id * F_WORDS + F_HEAD] = e;
  return true;
}

function frozenError() {
  return Object.assign(new Error("fault overlay frozen"), { code: "EIO" });
}

/**
 * The `interpose` hook for one thread's SDK sync-fs provider. `root` is the
 * provider's root (paths it opens are absolute below it).
 */
export function createFaultInterposer(buffer, _root) {
  const v = views(buffer);
  const fdFile = new Map(); // this thread's fds -> file id
  const mutating = new Set(["write", "truncate", "mkdir", "unlink"]);
  return (op, fn, args) => {
    if (
      Atomics.load(v.h, H_FROZEN) &&
      (mutating.has(op) || op === "sync" || op === "syncdir" ||
        (op === "open" && (args[1] & fs.constants.O_CREAT) !== 0))
    ) {
      throw frozenError();
    }
    if (op === "open") {
      const full = args[0];
      const creating = (args[1] & fs.constants.O_CREAT) !== 0;
      const existed = creating ? fs.existsSync(full) : true;
      const fd = fn(...args);
      const id = fileId(v, full, true);
      if (id >= 0) {
        fdFile.set(fd, id);
        if (creating && !existed) {
          lock(v, id);
          v.files[id * F_WORDS + F_FLAGS] = (v.files[id * F_WORDS + F_FLAGS] | FLAG_NEW_ENTRY) & ~FLAG_TRACKED;
          // A new file's synced content is nothing.
          v.syncedSize[id] = 0;
          v.files[id * F_WORDS + F_FLAGS] |= FLAG_TRACKED;
          unlock(v, id);
        }
      }
      return fd;
    }
    if (op === "close") {
      fdFile.delete(args[0]);
      return fn(...args);
    }
    if (op === "write") {
      const [fd, view, off, len, pos] = args;
      const id = fdFile.get(fd);
      if (id === undefined) return fn(...args);
      lock(v, id);
      try {
        track(v, id, fd);
        const first = Math.floor(pos / PAGE);
        const last = Math.floor((pos + len - 1) / PAGE);
        for (let p = first; p <= last; p += 1) savePage(v, id, fd, p);
        Atomics.add(v.h, H_WRITES, 1);
        return fn(fd, view, off, len, pos);
      } finally {
        unlock(v, id);
      }
    }
    if (op === "truncate") {
      const [fd, size] = args;
      const id = fdFile.get(fd);
      if (id === undefined) return fn(...args);
      lock(v, id);
      try {
        track(v, id, fd);
        const cur = sizeOf(fd);
        for (let p = Math.floor(size / PAGE); p * PAGE < cur; p += 1) savePage(v, id, fd, p);
        return fn(...args);
      } finally {
        unlock(v, id);
      }
    }
    if (op === "sync") {
      const [fd] = args;
      const id = fdFile.get(fd);
      const r = fn(...args);
      if (id !== undefined) {
        lock(v, id);
        // Everything written before this sync returned is durable.
        v.files[id * F_WORDS + F_HEAD] = -1;
        v.files[id * F_WORDS + F_FLAGS] &= ~FLAG_TRACKED;
        unlock(v, id);
        Atomics.add(v.h, H_SYNCS, 1);
      }
      return r;
    }
    if (op === "syncdir") {
      const r = fn(...args);
      const dir = path.resolve(args[0]);
      for (let id = 0; id < v.maxFiles; id += 1) {
        if (Atomics.load(v.files, id * F_WORDS + F_STATE) !== F_LIVE) continue;
        if (!(v.files[id * F_WORDS + F_FLAGS] & FLAG_NEW_ENTRY)) continue;
        if (path.dirname(pathOf(v, id)) !== dir) continue;
        lock(v, id);
        v.files[id * F_WORDS + F_FLAGS] &= ~FLAG_NEW_ENTRY;
        unlock(v, id);
      }
      return r;
    }
    if (op === "unlink") {
      const full = args[0];
      const r = fn(...args);
      const id = fileId(v, full, false);
      if (id >= 0) {
        lock(v, id);
        v.files[id * F_WORDS + F_FLAGS] = 0;
        v.files[id * F_WORDS + F_HEAD] = -1;
        unlock(v, id);
      }
      return r;
    }
    return fn(...args);
  };
}

function overlayApi(buffer) {
  const v = views(buffer);
  return {
    /** Every later mutating call fails with EIO (the process is dying). */
    freeze() {
      Atomics.store(v.h, H_FROZEN, 1);
    },
    stats() {
      return {
        writes: Atomics.load(v.h, H_WRITES),
        syncs: Atomics.load(v.h, H_SYNCS),
        pagesUsed: Math.min(Atomics.load(v.h, H_NEXT_PAGE), v.maxPages),
        overflow: Atomics.load(v.h, H_OVERFLOW) !== 0,
      };
    },
    /**
     * Apply a crash after every guest thread has stopped, then reset the
     * overlay (unfrozen, nothing tracked). `random` returns [0, 1).
     * @returns {{ pagesUndone: number, pagesKept: number, filesTruncated: number, entriesDropped: number }}
     */
    crash(mode, random = Math.random) {
      if (Atomics.load(v.h, H_OVERFLOW)) throw new Error("fault overlay overflowed: the crash would be incomplete");
      const out = { pagesUndone: 0, pagesKept: 0, filesTruncated: 0, entriesDropped: 0 };
      for (let id = 0; id < v.maxFiles; id += 1) {
        const at = id * F_WORDS;
        if (v.files[at + F_STATE] !== F_LIVE) continue;
        const flags = v.files[at + F_FLAGS];
        const full = pathOf(v, id);
        if (mode !== "keepAll" && flags & FLAG_NEW_ENTRY) {
          try {
            fs.unlinkSync(full);
            out.entriesDropped += 1;
          } catch {
            // already gone
          }
          continue;
        }
        if (mode === "keepAll" || !(flags & FLAG_TRACKED)) continue;
        let fd;
        try {
          fd = fs.openSync(full, "r+");
        } catch {
          continue; // unlinked durably since
        }
        try {
          for (let e = v.files[at + F_HEAD]; e >= 0; e = v.pages[e * P_WORDS + 3]) {
            const undo = mode === "dropAll" || random() < 0.5;
            if (!undo) {
              out.pagesKept += 1;
              continue;
            }
            const p = v.pages[e * P_WORDS + 1];
            const n = v.pages[e * P_WORDS + 2];
            if (n > 0) fs.writeSync(fd, v.images.subarray(e * PAGE, e * PAGE + n), 0, n, p * PAGE);
            out.pagesUndone += 1;
          }
          const synced = v.syncedSize[id];
          const size = fs.fstatSync(fd).size;
          const keepSize = mode === "pageSubset" && random() < 0.5;
          if (!keepSize && size !== synced) {
            fs.ftruncateSync(fd, synced);
            out.filesTruncated += 1;
          }
        } finally {
          fs.closeSync(fd);
        }
      }
      // Reset.
      for (let id = 0; id < v.maxFiles; id += 1) {
        v.files[id * F_WORDS + F_FLAGS] = 0;
        v.files[id * F_WORDS + F_HEAD] = -1;
      }
      Atomics.store(v.h, H_NEXT_PAGE, 0);
      Atomics.store(v.h, H_FROZEN, 0);
      return out;
    },
  };
}

/** Re-attach to an overlay buffer (for example in another thread). */
export function attachFaultOverlay(buffer) {
  return { buffer, ...overlayApi(buffer) };
}
