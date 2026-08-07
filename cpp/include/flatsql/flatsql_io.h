#ifndef FLATSQL_IO_H
#define FLATSQL_IO_H

/*
 * FlatSQL host I/O contract — SEVEN imports, one contract, every host.
 * =====================================================================
 *
 * FlatSQL does not use emscripten's filesystem and does not use WASI file
 * descriptors. It registers its OWN sqlite3_vfs ("flatsql_io") whose methods
 * call out through the seven functions declared below. Every host — the
 * browser JS shim, the standalone WASI shim, WasmEdge/wazero in the Go node —
 * satisfies the SAME seven names with the SAME signatures. There is no runtime
 * detection anywhere in module code; the difference between a browser
 * key->bytes store and a real POSIX file lives entirely in the host shim.
 * See docs/STORAGE-DURABILITY.md §3.5.
 *
 * WHY NOT WASI path_open: MEASURED (§3.5). `-s WASMFS=1` routes file I/O to an
 * in-memory backend that looks durable and is not; `-s FORCE_FILESYSTEM=1`
 * emits ten emscripten `__syscall_*` imports that no host satisfies and still
 * never emits path_open. Both were reverted.
 *
 * SIGNATURE LAW — i32 and f64 ONLY.
 * Offsets and sizes are `double`, never `int64_t`. Emscripten legalizes i64 at
 * the JS boundary for the browser target (splitting one i64 parameter into two
 * i32s) but NOT for STANDALONE_WASM. An i64 in this contract would therefore
 * give the two lanes genuinely different wasm signatures for the same function
 * name — the exact divergence class this design exists to prevent. f64 is
 * exact to 2^53 (9 PB), passes through JS unmodified, and is identical in
 * every lane.
 *
 * ERROR LAW: errors are return values, never traps. The -fignore-exceptions
 * WASI artifact turns any throw into a guest-poisoning `unreachable`
 * (commit b26ed45), so nothing below may throw.
 */

#include <stdint.h>

/* ---- open flags ------------------------------------------------------- */
#define FLATSQL_IO_READ       0x0001
#define FLATSQL_IO_WRITE      0x0002
#define FLATSQL_IO_CREATE     0x0004
#define FLATSQL_IO_EXCL       0x0008
#define FLATSQL_IO_TRUNC      0x0010
/* Advisory: the host may drop the file when the handle is closed. */
#define FLATSQL_IO_DELETE_ON_CLOSE 0x0020

/* Namespace operations. These are carried on `open` rather than spent as two
 * more imports: a VFS needs xAccess and xDelete, and both are pure
 * path->status questions that allocate no handle. With either bit set,
 * flatsql_io_open returns a STATUS (0 = success/exists, negative = error or
 * absent) and no handle is created. Hosts must not allocate for them. */
#define FLATSQL_IO_PROBE      0x0040  /* xAccess: does the path exist?      */
#define FLATSQL_IO_UNLINK     0x0080  /* xDelete: remove the path           */

/* ---- status codes (all negative; every call may return any of them) ---- */
#define FLATSQL_IO_ERR_GENERIC    (-1)
#define FLATSQL_IO_ERR_NOENT      (-2)
#define FLATSQL_IO_ERR_ACCESS     (-3)
#define FLATSQL_IO_ERR_IO         (-4)
#define FLATSQL_IO_ERR_NOSPACE    (-5)
#define FLATSQL_IO_ERR_BADHANDLE  (-6)

#if defined(__wasm__)
#  define FLATSQL_IO_IMPORT(name) \
      __attribute__((import_module("env"), import_name(name)))
#else
#  define FLATSQL_IO_IMPORT(name)
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Open (or probe/unlink) `path`. Returns a non-negative opaque handle, or a
 * negative status. `pathLen` is explicit: hosts never scan for NUL. */
FLATSQL_IO_IMPORT("flatsql_io_open")
int32_t flatsql_io_open(const char* path, int32_t pathLen, int32_t flags);

/* Offset-addressed read. Returns bytes actually read (may be < len at EOF), or
 * a negative status. Never moves an implicit cursor — there isn't one. */
FLATSQL_IO_IMPORT("flatsql_io_read")
int32_t flatsql_io_read(int32_t handle, void* dst, int32_t len, double offset);

/* Offset-addressed write. Writing past EOF extends the file (the gap reads as
 * zeroes). Returns bytes written or a negative status. */
FLATSQL_IO_IMPORT("flatsql_io_write")
int32_t flatsql_io_write(int32_t handle, const void* src, int32_t len,
                         double offset);

/* Set the file length exactly (grow or shrink). */
FLATSQL_IO_IMPORT("flatsql_io_truncate")
int32_t flatsql_io_truncate(int32_t handle, double size);

/* Durability barrier. Returns 0 only when prior writes are durable. */
FLATSQL_IO_IMPORT("flatsql_io_sync")
int32_t flatsql_io_sync(int32_t handle);

/* Current length in bytes, or a negative status. */
FLATSQL_IO_IMPORT("flatsql_io_size")
double flatsql_io_size(int32_t handle);

/* Release the handle. */
FLATSQL_IO_IMPORT("flatsql_io_close")
int32_t flatsql_io_close(int32_t handle);

#ifdef __cplusplus
}  /* extern "C" */

namespace flatsql {

/* Name of the VFS registered over the seven imports above. */
extern const char* const kFlatSqlVfsName;  /* "flatsql_io" */

/* Idempotent. Registers the VFS with SQLite. `makeDefault` decides whether an
 * open with a NULL vfs name lands here. Returns SQLITE_OK on success. Never
 * throws. */
int registerFlatSqlIoVfs(bool makeDefault);

}  /* namespace flatsql */
#endif /* __cplusplus */

#endif /* FLATSQL_IO_H */
