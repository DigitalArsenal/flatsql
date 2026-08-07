/**
 * FlatSQL host I/O backends — the JavaScript half of the seven-import contract
 * (cpp/include/flatsql/flatsql_io.h).
 *
 * ONE backend interface, used by BOTH JavaScript loaders:
 *   - the emscripten browser bundle, via cpp/js/flatsql_io_library.js
 *   - the standalone/WASI shim, via wasm/standalone.js
 * ...and matched call-for-call by the Go host and by cpp/src/flatsql_io_native.cpp.
 *
 * A backend is SYNCHRONOUS, because a wasm import is. Its methods are:
 *
 *   open(path: string, flags: number): number   // handle, or negative status
 *   read(handle, dst: Uint8Array, offset: number): number
 *   write(handle, src: Uint8Array, offset: number): number
 *   truncate(handle, size: number): number
 *   sync(handle): number
 *   size(handle): number
 *   close(handle): number
 *
 * The pointer->subarray translation is done by the loader; a backend never sees
 * wasm memory. That is the entire reason two very different hosts can satisfy
 * the same seven calls without one line of runtime detection inside the module.
 *
 * ASYNC STORES (the one honest asymmetry, and it is in the shim where it
 * belongs): IndexedDB and an HTTP desktop store cannot answer synchronously.
 * createChunkedStoreBackend() therefore keeps a synchronous page cache and
 * makes durability an explicit, awaited JavaScript step:
 *
 *     await backend.hydrate(path);   // BEFORE opening the database
 *     ... engine runs entirely synchronously ...
 *     await backend.flush();         // AFTER flatsql_flush_index()
 *
 * Results are identical to the native lane in every case; only the moment at
 * which bytes become durable differs, and it is observable and awaited rather
 * than assumed. Nothing about the wasm-side contract changes.
 */

export const FLATSQL_IO_READ = 0x0001;
export const FLATSQL_IO_WRITE = 0x0002;
export const FLATSQL_IO_CREATE = 0x0004;
export const FLATSQL_IO_EXCL = 0x0008;
export const FLATSQL_IO_TRUNC = 0x0010;
export const FLATSQL_IO_DELETE_ON_CLOSE = 0x0020;
export const FLATSQL_IO_PROBE = 0x0040;
export const FLATSQL_IO_UNLINK = 0x0080;

export const FLATSQL_IO_ERR_GENERIC = -1;
export const FLATSQL_IO_ERR_NOENT = -2;
export const FLATSQL_IO_ERR_ACCESS = -3;
export const FLATSQL_IO_ERR_IO = -4;
export const FLATSQL_IO_ERR_NOSPACE = -5;
export const FLATSQL_IO_ERR_BADHANDLE = -6;

/** Default page-group size. One IndexedDB value per group. */
export const DEFAULT_CHUNK_BYTES = 64 * 1024;

/**
 * A file living in RAM as a set of fixed-size chunks. Shared by every backend
 * so that chunk boundaries, growth and zero-fill behave identically no matter
 * what the bytes are eventually persisted into.
 */
class ChunkedFile {
  constructor(path, chunkBytes) {
    this.path = path;
    this.chunkBytes = chunkBytes;
    this.chunks = new Map(); // index -> Uint8Array(chunkBytes)
    this.dirty = new Set();
    this.length = 0;
    this.metaDirty = false;
  }

  chunk(index, create) {
    let buf = this.chunks.get(index);
    if (!buf && create) {
      buf = new Uint8Array(this.chunkBytes);
      this.chunks.set(index, buf);
    }
    return buf || null;
  }

  read(dst, offset) {
    // A read past EOF is a SHORT read, never an error — the C side zero-fills
    // the remainder and reports SQLITE_IOERR_SHORT_READ, exactly as POSIX
    // pread does. Getting this wrong silently corrupts pager reads.
    const available = Math.max(0, this.length - offset);
    const want = Math.min(dst.length, available);
    let done = 0;
    while (done < want) {
      const at = offset + done;
      const index = Math.floor(at / this.chunkBytes);
      const within = at % this.chunkBytes;
      const n = Math.min(this.chunkBytes - within, want - done);
      const buf = this.chunk(index, false);
      if (buf) {
        dst.set(buf.subarray(within, within + n), done);
      } else {
        dst.fill(0, done, done + n); // sparse hole
      }
      done += n;
    }
    return want;
  }

  write(src, offset) {
    let done = 0;
    while (done < src.length) {
      const at = offset + done;
      const index = Math.floor(at / this.chunkBytes);
      const within = at % this.chunkBytes;
      const n = Math.min(this.chunkBytes - within, src.length - done);
      const buf = this.chunk(index, true);
      buf.set(src.subarray(done, done + n), within);
      this.dirty.add(index);
      done += n;
    }
    if (offset + src.length > this.length) {
      this.length = offset + src.length;
      this.metaDirty = true;
    }
    return src.length;
  }

  truncate(size) {
    if (size < this.length) {
      const lastIndex = Math.floor(size / this.chunkBytes);
      for (const index of Array.from(this.chunks.keys())) {
        if (index > lastIndex) {
          this.chunks.delete(index);
          this.dirty.add(index); // removal must be persisted too
        }
      }
      const tail = this.chunk(lastIndex, false);
      if (tail) {
        tail.fill(0, size % this.chunkBytes);
        this.dirty.add(lastIndex);
      }
    }
    this.length = size;
    this.metaDirty = true;
    return 0;
  }
}

/** Handle table shared by every backend: small, dense, reused after close. */
class HandleTable {
  constructor() {
    this.slots = [];
  }

  add(entry) {
    for (let i = 0; i < this.slots.length; i++) {
      if (!this.slots[i]) {
        this.slots[i] = entry;
        return i;
      }
    }
    this.slots.push(entry);
    return this.slots.length - 1;
  }

  get(handle) {
    return this.slots[handle] || null;
  }

  remove(handle) {
    const entry = this.slots[handle];
    this.slots[handle] = null;
    return entry;
  }
}

/**
 * Pure in-memory backend. Durable for exactly as long as the process lives —
 * which is the DOCUMENTED behaviour asserted by the Memory-store lane of the
 * test matrix, not a skipped case.
 */
export function createMemoryBackend(options = {}) {
  const chunkBytes = options.chunkBytes || DEFAULT_CHUNK_BYTES;
  const files = new Map();
  const handles = new HandleTable();

  return {
    kind: 'memory',
    files,

    open(path, flags) {
      if (flags & FLATSQL_IO_PROBE) {
        return files.has(path) ? 0 : FLATSQL_IO_ERR_NOENT;
      }
      if (flags & FLATSQL_IO_UNLINK) {
        return files.delete(path) ? 0 : FLATSQL_IO_ERR_NOENT;
      }
      let file = files.get(path);
      if (!file) {
        if (!(flags & FLATSQL_IO_CREATE)) return FLATSQL_IO_ERR_NOENT;
        file = new ChunkedFile(path, chunkBytes);
        files.set(path, file);
      } else if (flags & FLATSQL_IO_EXCL) {
        return FLATSQL_IO_ERR_ACCESS;
      }
      if (flags & FLATSQL_IO_TRUNC) file.truncate(0);
      return handles.add({ file, deleteOnClose: !!(flags & FLATSQL_IO_DELETE_ON_CLOSE) });
    },

    read(handle, dst, offset) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      return entry.file.read(dst, offset);
    },

    write(handle, src, offset) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      return entry.file.write(src, offset);
    },

    truncate(handle, size) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      return entry.file.truncate(size);
    },

    sync() {
      return 0; // RAM is already as durable as it will ever be
    },

    size(handle) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      return entry.file.length;
    },

    close(handle) {
      const entry = handles.remove(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      if (entry.deleteOnClose) files.delete(entry.file.path);
      return 0;
    },

    async hydrate() {},
    async flush() {},
    async drop(path) {
      files.delete(path);
    },
  };
}

/**
 * Node/desktop backend over real files. Fully synchronous, so sync() really is
 * durable at return — this lane has no asymmetry at all.
 *
 * `fs` is injected rather than imported so the module stays loadable in a
 * browser bundle that will never call this.
 */
export function createNodeFsBackend(fs, options = {}) {
  const root = options.root || '';
  const handles = new HandleTable();
  const resolve = (p) => (root ? `${root}/${p.replace(/^\/+/, '')}` : p);

  const statusFor = (err) => {
    if (!err || !err.code) return FLATSQL_IO_ERR_GENERIC;
    if (err.code === 'ENOENT') return FLATSQL_IO_ERR_NOENT;
    if (err.code === 'EACCES' || err.code === 'EPERM') return FLATSQL_IO_ERR_ACCESS;
    if (err.code === 'ENOSPC') return FLATSQL_IO_ERR_NOSPACE;
    return FLATSQL_IO_ERR_IO;
  };

  return {
    kind: 'node-fs',

    open(path, flags) {
      const full = resolve(path);
      try {
        if (flags & FLATSQL_IO_PROBE) {
          fs.statSync(full);
          return 0;
        }
        if (flags & FLATSQL_IO_UNLINK) {
          fs.unlinkSync(full);
          return 0;
        }
        let mode = 'r';
        if (flags & FLATSQL_IO_WRITE) {
          mode = flags & FLATSQL_IO_EXCL ? 'wx+' : flags & FLATSQL_IO_CREATE ? 'a+' : 'r+';
        }
        const fd = fs.openSync(full, mode);
        return handles.add({
          fd,
          path: full,
          deleteOnClose: !!(flags & FLATSQL_IO_DELETE_ON_CLOSE),
        });
      } catch (err) {
        return statusFor(err);
      }
    },

    read(handle, dst, offset) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      try {
        return fs.readSync(entry.fd, dst, 0, dst.length, offset);
      } catch (err) {
        return statusFor(err);
      }
    },

    write(handle, src, offset) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      try {
        return fs.writeSync(entry.fd, src, 0, src.length, offset);
      } catch (err) {
        return statusFor(err);
      }
    },

    truncate(handle, size) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      try {
        fs.ftruncateSync(entry.fd, size);
        return 0;
      } catch (err) {
        return statusFor(err);
      }
    },

    sync(handle) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      try {
        fs.fsyncSync(entry.fd);
        return 0;
      } catch (err) {
        return statusFor(err);
      }
    },

    size(handle) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      try {
        return fs.fstatSync(entry.fd).size;
      } catch (err) {
        return statusFor(err);
      }
    },

    close(handle) {
      const entry = handles.remove(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      try {
        fs.closeSync(entry.fd);
        if (entry.deleteOnClose) {
          try {
            fs.unlinkSync(entry.path);
          } catch (_) {
            /* already gone */
          }
        }
        return 0;
      } catch (err) {
        return statusFor(err);
      }
    },

    async hydrate() {},
    async flush() {},
    async drop(path) {
      try {
        fs.unlinkSync(resolve(path));
      } catch (_) {
        /* already gone */
      }
    },
  };
}

/**
 * Backend over ANY flat key->bytes store — the shape every sdn-js persistence
 * store already has (readBytes/writeBytes/deleteKey). This is what gives a
 * store with no seek and no partial write real pread/pwrite semantics:
 *
 *   <prefix><path>#<pageGroup>   fixed-size chunk of file bytes
 *   <prefix><path>#meta          JSON { length }
 *
 * Because addressing is by chunk index, a flush costs O(dirty chunks) rather
 * than O(file), and the browser lane reads and writes the same byte ranges the
 * POSIX lane does. It is not "the browser rewrites everything" — the two lanes
 * are the same algorithm over a different transport.
 *
 * `store` must provide:
 *   readBytes(key): Promise<Uint8Array|null>
 *   writeBytes(key, bytes): Promise<void>
 *   deleteKey(key): Promise<void>
 */
export function createChunkedStoreBackend(store, options = {}) {
  const chunkBytes = options.chunkBytes || DEFAULT_CHUNK_BYTES;
  const prefix = options.prefix || 'flatsql-io/';
  const files = new Map();
  const handles = new HandleTable();
  const known = new Set(options.knownPaths || []);

  const metaKey = (path) => `${prefix}${path}#meta`;
  const chunkKey = (path, index) => `${prefix}${path}#${index}`;

  /**
   * Pull a file's chunks into the synchronous cache. MUST be awaited before
   * the engine opens the database: after that point every call is synchronous
   * because a wasm import cannot await.
   */
  async function hydrate(path) {
    if (files.has(path)) return files.get(path);
    const file = new ChunkedFile(path, chunkBytes);
    let meta = null;
    const raw = await store.readBytes(metaKey(path));
    if (raw) {
      try {
        meta = JSON.parse(new TextDecoder().decode(raw));
      } catch (_) {
        meta = null;
      }
    }
    if (meta && Number.isFinite(meta.length)) {
      file.length = meta.length;
      if (meta.chunkBytes && meta.chunkBytes !== chunkBytes) {
        // A different page-group size means the persisted keys do not line up
        // with the ones we would read. Treat it as absent: derived state is
        // always rebuildable, so this costs a re-derivation and never a wrong
        // answer.
        file.length = 0;
      } else {
        const count = Math.ceil(file.length / chunkBytes);
        for (let i = 0; i < count; i++) {
          const bytes = await store.readBytes(chunkKey(path, i));
          if (bytes) {
            const buf = new Uint8Array(chunkBytes);
            buf.set(bytes.subarray(0, Math.min(bytes.length, chunkBytes)));
            file.chunks.set(i, buf);
          }
        }
      }
      known.add(path);
    }
    files.set(path, file);
    return file;
  }

  /** Persist every dirty chunk plus the length. Await this after a flush. */
  async function flush() {
    for (const [path, file] of files) {
      if (!file.dirty.size && !file.metaDirty) continue;
      for (const index of file.dirty) {
        const buf = file.chunks.get(index);
        if (buf) {
          const start = index * chunkBytes;
          const end = Math.min(chunkBytes, Math.max(0, file.length - start));
          await store.writeBytes(chunkKey(path, index), buf.subarray(0, end));
        } else {
          await store.deleteKey(chunkKey(path, index));
        }
      }
      file.dirty.clear();
      if (file.metaDirty || file.length >= 0) {
        await store.writeBytes(
          metaKey(path),
          new TextEncoder().encode(JSON.stringify({ length: file.length, chunkBytes })),
        );
        file.metaDirty = false;
      }
      known.add(path);
    }
  }

  async function drop(path) {
    const file = files.get(path);
    const count = file
      ? Math.max(Math.ceil(file.length / chunkBytes), ...[...file.chunks.keys()].map((i) => i + 1), 0)
      : 0;
    for (let i = 0; i < count; i++) await store.deleteKey(chunkKey(path, i));
    await store.deleteKey(metaKey(path));
    files.delete(path);
    known.delete(path);
  }

  return {
    kind: 'chunked-store',
    chunkBytes,
    hydrate,
    flush,
    drop,
    /** Paths this backend believes exist — used to hydrate before an open. */
    knownPaths: () => Array.from(known),

    open(path, flags) {
      if (flags & FLATSQL_IO_PROBE) {
        const file = files.get(path);
        if (file) return file.length > 0 || file.chunks.size > 0 ? 0 : FLATSQL_IO_ERR_NOENT;
        return known.has(path) ? 0 : FLATSQL_IO_ERR_NOENT;
      }
      if (flags & FLATSQL_IO_UNLINK) {
        const file = files.get(path);
        if (file) file.truncate(0);
        known.delete(path);
        return 0;
      }
      let file = files.get(path);
      if (!file) {
        // Not hydrated. Creating is fine (it is genuinely new); opening an
        // existing file that was never hydrated is a caller error we refuse
        // rather than answer with an empty file — an empty answer here is
        // indistinguishable from data loss.
        if (!(flags & FLATSQL_IO_CREATE)) return FLATSQL_IO_ERR_NOENT;
        file = new ChunkedFile(path, chunkBytes);
        file.metaDirty = true;
        files.set(path, file);
      } else if (flags & FLATSQL_IO_EXCL && file.length > 0) {
        return FLATSQL_IO_ERR_ACCESS;
      }
      if (flags & FLATSQL_IO_TRUNC) file.truncate(0);
      return handles.add({
        file,
        deleteOnClose: !!(flags & FLATSQL_IO_DELETE_ON_CLOSE),
      });
    },

    read(handle, dst, offset) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      return entry.file.read(dst, offset);
    },

    write(handle, src, offset) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      return entry.file.write(src, offset);
    },

    truncate(handle, size) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      return entry.file.truncate(size);
    },

    /**
     * A durability BARRIER, not a durability guarantee — the underlying store
     * is asynchronous and a wasm import cannot await. Bytes become durable when
     * the caller awaits flush(). Stated plainly because the alternative is a
     * shim that claims fsync semantics it cannot deliver, which is precisely
     * the class of lie this whole task exists to remove.
     */
    sync(handle) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      return 0;
    },

    size(handle) {
      const entry = handles.get(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      return entry.file.length;
    },

    close(handle) {
      const entry = handles.remove(handle);
      if (!entry) return FLATSQL_IO_ERR_BADHANDLE;
      if (entry.deleteOnClose) {
        entry.file.truncate(0);
        known.delete(entry.file.path);
      }
      return 0;
    },
  };
}

/**
 * Build the `env` import object for a directly-instantiated (WASI) module.
 * The pointer->subarray translation here is the mirror image of the one in
 * cpp/js/flatsql_io_library.js; keeping them in one file each, both feeding the
 * same backend, is what stops the lanes drifting apart.
 */
export function createFlatSqlIoImports(backend, memoryRef) {
  const heap = () => new Uint8Array(memoryRef().buffer);
  const guard = (fn) => {
    try {
      return fn();
    } catch (_) {
      return FLATSQL_IO_ERR_GENERIC;
    }
  };

  return {
    flatsql_io_open(pathPtr, pathLen, flags) {
      return guard(() => {
        const bytes = heap().subarray(pathPtr, pathPtr + pathLen);
        return backend.open(new TextDecoder().decode(bytes), flags) | 0;
      });
    },
    flatsql_io_read(handle, dstPtr, len, offset) {
      return guard(() => backend.read(handle, heap().subarray(dstPtr, dstPtr + len), offset) | 0);
    },
    flatsql_io_write(handle, srcPtr, len, offset) {
      return guard(() => backend.write(handle, heap().subarray(srcPtr, srcPtr + len), offset) | 0);
    },
    flatsql_io_truncate(handle, size) {
      return guard(() => backend.truncate(handle, size) | 0);
    },
    flatsql_io_sync(handle) {
      return guard(() => backend.sync(handle) | 0);
    },
    flatsql_io_size(handle) {
      return guard(() => backend.size(handle));
    },
    flatsql_io_close(handle) {
      return guard(() => backend.close(handle) | 0);
    },
  };
}
