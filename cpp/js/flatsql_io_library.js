/**
 * Emscripten JS library satisfying the seven-import FlatSQL host I/O contract
 * (cpp/include/flatsql/flatsql_io.h) on the browser target.
 *
 * This file does exactly ONE thing: translate wasm pointers into typed-array
 * views and hand them to a backend. It contains no storage policy, no
 * IndexedDB, no chunking, no runtime detection — all of that lives in the
 * backend, which is the same object the standalone/WASI shim uses
 * (wasm/flatsql-io.js). One backend interface, two loaders.
 *
 * Wire it up by putting a backend on the Module before instantiation:
 *
 *     const flatsql = await initFlatSQL({ io: createFlatSqlIoBackend(store) });
 *
 * With no backend installed every call returns FLATSQL_IO_ERR_ACCESS (-3), so
 * flatsql_open_db() fails closed with an error VALUE. It never silently
 * succeeds against RAM — that failure mode is the reason this layer exists
 * (docs/STORAGE-DURABILITY.md §2.2).
 */

addToLibrary({
  $flatsqlIoBridge: {
    ERR_GENERIC: -1,
    ERR_NOENT: -2,
    ERR_ACCESS: -3,
    ERR_BADHANDLE: -6,

    backend: function () {
      return Module['flatsqlIO'] || null;
    },

    // Paths arrive as (ptr, len) so the host never scans for a NUL.
    path: function (ptr, len) {
      return UTF8ToString(ptr, len);
    },
  },

  flatsql_io_open__deps: ['$flatsqlIoBridge'],
  flatsql_io_open: function (pathPtr, pathLen, flags) {
    var io = flatsqlIoBridge.backend();
    if (!io) return flatsqlIoBridge.ERR_ACCESS;
    try {
      return io.open(flatsqlIoBridge.path(pathPtr, pathLen), flags) | 0;
    } catch (e) {
      return flatsqlIoBridge.ERR_GENERIC;
    }
  },

  flatsql_io_read__deps: ['$flatsqlIoBridge'],
  flatsql_io_read: function (handle, dstPtr, len, offset) {
    var io = flatsqlIoBridge.backend();
    if (!io) return flatsqlIoBridge.ERR_ACCESS;
    try {
      return io.read(handle, HEAPU8.subarray(dstPtr, dstPtr + len), offset) | 0;
    } catch (e) {
      return flatsqlIoBridge.ERR_GENERIC;
    }
  },

  flatsql_io_write__deps: ['$flatsqlIoBridge'],
  flatsql_io_write: function (handle, srcPtr, len, offset) {
    var io = flatsqlIoBridge.backend();
    if (!io) return flatsqlIoBridge.ERR_ACCESS;
    try {
      return io.write(handle, HEAPU8.subarray(srcPtr, srcPtr + len), offset) | 0;
    } catch (e) {
      return flatsqlIoBridge.ERR_GENERIC;
    }
  },

  flatsql_io_truncate__deps: ['$flatsqlIoBridge'],
  flatsql_io_truncate: function (handle, size) {
    var io = flatsqlIoBridge.backend();
    if (!io) return flatsqlIoBridge.ERR_ACCESS;
    try {
      return io.truncate(handle, size) | 0;
    } catch (e) {
      return flatsqlIoBridge.ERR_GENERIC;
    }
  },

  flatsql_io_sync__deps: ['$flatsqlIoBridge'],
  flatsql_io_sync: function (handle) {
    var io = flatsqlIoBridge.backend();
    if (!io) return flatsqlIoBridge.ERR_ACCESS;
    try {
      return io.sync(handle) | 0;
    } catch (e) {
      return flatsqlIoBridge.ERR_GENERIC;
    }
  },

  flatsql_io_size__deps: ['$flatsqlIoBridge'],
  flatsql_io_size: function (handle) {
    var io = flatsqlIoBridge.backend();
    if (!io) return flatsqlIoBridge.ERR_ACCESS;
    try {
      return io.size(handle);
    } catch (e) {
      return flatsqlIoBridge.ERR_GENERIC;
    }
  },

  flatsql_io_close__deps: ['$flatsqlIoBridge'],
  flatsql_io_close: function (handle) {
    var io = flatsqlIoBridge.backend();
    if (!io) return flatsqlIoBridge.ERR_ACCESS;
    try {
      return io.close(handle) | 0;
    } catch (e) {
      return flatsqlIoBridge.ERR_GENERIC;
    }
  },
});
