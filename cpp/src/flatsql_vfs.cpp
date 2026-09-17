// FlatSQL's own sqlite3_vfs, built on the seven-import host I/O contract in
// include/flatsql/flatsql_io.h.
//
// This is the whole reason disk-backed FlatSQL is isomorphic: the pager below
// is identical in every lane, and the only thing that differs between a browser
// IndexedDB store and a WasmEdge preopen is which host satisfies the seven
// imports. No `#ifdef __EMSCRIPTEN__` appears in this file, and none may.
//
// Locking is a deliberate no-op. FlatSQL is opened by exactly one writer — the
// one-daemon-per-box law guarantees it on the server, and a browser tab owns
// its own store. Rollback-journal crash safety does not depend on locking; it
// depends on xSync ordering, which is honoured. WAL is NOT available (it needs
// xShmMap shared memory, which neither lane provides) — SQLITE_OMIT_WAL stays
// on every wasm target and disk-backed wasm uses TRUNCATE. Documented choice,
// see docs/STORAGE-DURABILITY.md §3.5.

#include "flatsql/flatsql_io.h"

#include <sqlite3.h>

#include <cstring>
#include <cstdlib>
#include <ctime>
#include <string>

#if !defined(__wasm__)
#include <random>
#endif

namespace {

constexpr int kMaxPathLen = 1024;

// HEAP-BACKED WAL-INDEX. SQLite's own unix VFS does exactly this when the
// database is held under an exclusive lock: "we do not really need shared
// memory. No shared memory file is created. The shared memory will be
// simulated with heap memory." (sqlite3.c, unixOpenSharedMemory).
//
// That is precisely this lane's situation — FlatSQL is opened by exactly one
// writer, which the one-daemon-per-box law guarantees on the server and tab
// ownership guarantees in the browser. The wal-index only has to be SHARED
// when several processes attach; for a single connection it is just memory.
//
// 64 regions is the cap. Regions are szRegion bytes (32 KiB in practice), so
// this covers a wal-index far larger than any WAL this engine will checkpoint,
// and the map fails loudly rather than silently wrapping past the end.
constexpr int kMaxShmRegions = 64;

struct FlatSqlFile {
    sqlite3_file base;   // must be first
    int32_t handle;
    int deleteOnClose;
    char path[kMaxPathLen];
    void* shmRegions[kMaxShmRegions];
    int shmRegionSize;
};

// Defined below with the rest of the shm methods; fsClose needs it first.
void freeShmRegions(FlatSqlFile* f);

int mapIoError(int32_t status, int fallback) {
    switch (status) {
        case FLATSQL_IO_ERR_NOENT:     return SQLITE_CANTOPEN;
        case FLATSQL_IO_ERR_ACCESS:    return SQLITE_PERM;
        case FLATSQL_IO_ERR_NOSPACE:   return SQLITE_FULL;
        case FLATSQL_IO_ERR_BADHANDLE: return SQLITE_MISUSE;
        case FLATSQL_IO_ERR_IO:        return fallback;
        default:                       return fallback;
    }
}

int fsClose(sqlite3_file* file) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    freeShmRegions(f);
    if (f->handle < 0) return SQLITE_OK;
    const int32_t rc = flatsql_io_close(f->handle);
    f->handle = -1;
    if (f->deleteOnClose && f->path[0]) {
        flatsql_io_open(f->path, static_cast<int32_t>(std::strlen(f->path)),
                        FLATSQL_IO_UNLINK);
    }
    return rc < 0 ? mapIoError(rc, SQLITE_IOERR_CLOSE) : SQLITE_OK;
}

int fsRead(sqlite3_file* file, void* buf, int amount, sqlite3_int64 offset) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    if (f->handle < 0) return SQLITE_IOERR_READ;
    const int32_t got = flatsql_io_read(f->handle, buf, amount,
                                        static_cast<double>(offset));
    if (got < 0) return mapIoError(got, SQLITE_IOERR_READ);
    if (got < amount) {
        // SQLite contract: zero-fill the remainder and report a short read.
        std::memset(static_cast<uint8_t*>(buf) + got, 0,
                    static_cast<size_t>(amount - got));
        return SQLITE_IOERR_SHORT_READ;
    }
    return SQLITE_OK;
}

int fsWrite(sqlite3_file* file, const void* buf, int amount,
            sqlite3_int64 offset) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    if (f->handle < 0) return SQLITE_IOERR_WRITE;
    int written = 0;
    while (written < amount) {
        const int32_t n = flatsql_io_write(
            f->handle, static_cast<const uint8_t*>(buf) + written,
            amount - written, static_cast<double>(offset + written));
        if (n < 0) return mapIoError(n, SQLITE_IOERR_WRITE);
        if (n == 0) return SQLITE_IOERR_WRITE;  // no progress: refuse to spin
        written += n;
    }
    return SQLITE_OK;
}

int fsTruncate(sqlite3_file* file, sqlite3_int64 size) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    if (f->handle < 0) return SQLITE_IOERR_TRUNCATE;
    const int32_t rc = flatsql_io_truncate(f->handle, static_cast<double>(size));
    return rc < 0 ? mapIoError(rc, SQLITE_IOERR_TRUNCATE) : SQLITE_OK;
}

int fsSync(sqlite3_file* file, int /*flags*/) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    if (f->handle < 0) return SQLITE_IOERR_FSYNC;
    const int32_t rc = flatsql_io_sync(f->handle);
    return rc < 0 ? mapIoError(rc, SQLITE_IOERR_FSYNC) : SQLITE_OK;
}

int fsFileSize(sqlite3_file* file, sqlite3_int64* outSize) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    if (f->handle < 0) return SQLITE_IOERR_FSTAT;
    const double size = flatsql_io_size(f->handle);
    if (size < 0) return SQLITE_IOERR_FSTAT;
    *outSize = static_cast<sqlite3_int64>(size);
    return SQLITE_OK;
}

int fsLock(sqlite3_file*, int) { return SQLITE_OK; }
int fsUnlock(sqlite3_file*, int) { return SQLITE_OK; }

int fsCheckReservedLock(sqlite3_file*, int* out) {
    *out = 0;
    return SQLITE_OK;
}

int fsFileControl(sqlite3_file*, int op, void* arg) {
    if (op == SQLITE_FCNTL_VFSNAME) {
        *static_cast<char**>(arg) = sqlite3_mprintf("flatsql_io");
        return SQLITE_OK;
    }
    return SQLITE_NOTFOUND;
}

int fsSectorSize(sqlite3_file*) { return 4096; }

int fsDeviceCharacteristics(sqlite3_file*) {
    // Claim nothing. A key->bytes browser store cannot promise atomic sector
    // writes, and claiming a capability the weakest lane lacks would make the
    // two lanes diverge on crash recovery — the pager would skip journal work
    // in one and not the other. Zero keeps recovery byte-identical everywhere.
    return 0;
}

void freeShmRegions(FlatSqlFile* f) {
    for (int i = 0; i < kMaxShmRegions; ++i) {
        if (f->shmRegions[i] != nullptr) {
            sqlite3_free(f->shmRegions[i]);
            f->shmRegions[i] = nullptr;
        }
    }
    f->shmRegionSize = 0;
}

int fsShmMap(sqlite3_file* file, int iRegion, int szRegion, int bExtend,
             void volatile** pp) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    *pp = nullptr;
    if (iRegion < 0 || iRegion >= kMaxShmRegions || szRegion <= 0) {
        return SQLITE_IOERR_SHMMAP;
    }
    if (f->shmRegionSize == 0) {
        f->shmRegionSize = szRegion;
    } else if (f->shmRegionSize != szRegion) {
        return SQLITE_IOERR_SHMMAP;
    }
    if (f->shmRegions[iRegion] == nullptr) {
        // bExtend == 0 asks "does it exist yet?" and must not allocate: the
        // pager uses the null answer to decide a recovery is needed.
        if (!bExtend) return SQLITE_OK;
        void* mem = sqlite3_malloc64(static_cast<sqlite3_uint64>(szRegion));
        if (mem == nullptr) return SQLITE_NOMEM;
        // Zeroed: SQLite requires a freshly created wal-index region to read
        // as zero, or a dirty region is read as a valid header.
        std::memset(mem, 0, static_cast<size_t>(szRegion));
        f->shmRegions[iRegion] = mem;
    }
    *pp = f->shmRegions[iRegion];
    return SQLITE_OK;
}

// One connection owns this wal-index, so every lock is trivially available.
int fsShmLock(sqlite3_file*, int, int, int) { return SQLITE_OK; }

// Single-threaded wasm: no other core's view needs ordering.
void fsShmBarrier(sqlite3_file*) {}

int fsShmUnmap(sqlite3_file* file, int /*deleteFlag*/) {
    freeShmRegions(reinterpret_cast<FlatSqlFile*>(file));
    return SQLITE_OK;
}

const sqlite3_io_methods kIoMethods = {
    2,                          // iVersion: 2 exposes xShm* (WAL)
    fsClose,
    fsRead,
    fsWrite,
    fsTruncate,
    fsSync,
    fsFileSize,
    fsLock,
    fsUnlock,
    fsCheckReservedLock,
    fsFileControl,
    fsSectorSize,
    fsDeviceCharacteristics,
    // v2 shared memory, backed by heap rather than a -shm file.
    fsShmMap, fsShmLock, fsShmBarrier, fsShmUnmap,
    nullptr, nullptr,  // v3 xFetch/xUnfetch (no mmap in any lane)
};

int32_t translateOpenFlags(int flags) {
    int32_t out = 0;
    if (flags & SQLITE_OPEN_READONLY)  out |= FLATSQL_IO_READ;
    if (flags & SQLITE_OPEN_READWRITE) out |= FLATSQL_IO_READ | FLATSQL_IO_WRITE;
    if (flags & SQLITE_OPEN_CREATE)    out |= FLATSQL_IO_CREATE;
    if (flags & SQLITE_OPEN_EXCLUSIVE) out |= FLATSQL_IO_EXCL;
    if (flags & SQLITE_OPEN_DELETEONCLOSE) out |= FLATSQL_IO_DELETE_ON_CLOSE;
    if (out == 0) out = FLATSQL_IO_READ;
    return out;
}

int fsOpen(sqlite3_vfs*, const char* name, sqlite3_file* file, int flags,
           int* outFlags) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    std::memset(f, 0, sizeof(*f));
    f->handle = -1;
    f->base.pMethods = nullptr;

    if (name == nullptr || name[0] == '\0') {
        // Anonymous temp file. SQLITE_TEMP_STORE=3 on every wasm target keeps
        // temp b-trees in memory precisely so this never happens; if it does,
        // fail loudly rather than silently spilling somewhere undurable.
        return SQLITE_CANTOPEN;
    }

    const size_t len = std::strlen(name);
    if (len >= kMaxPathLen) return SQLITE_CANTOPEN;
    std::memcpy(f->path, name, len + 1);

    const int32_t handle = flatsql_io_open(name, static_cast<int32_t>(len),
                                           translateOpenFlags(flags));
    if (handle < 0) return mapIoError(handle, SQLITE_CANTOPEN);

    f->handle = handle;
    f->deleteOnClose = (flags & SQLITE_OPEN_DELETEONCLOSE) ? 1 : 0;
    f->base.pMethods = &kIoMethods;
    if (outFlags) {
        *outFlags = (flags & SQLITE_OPEN_READONLY) ? SQLITE_OPEN_READONLY
                                                   : SQLITE_OPEN_READWRITE;
    }
    return SQLITE_OK;
}

int fsDelete(sqlite3_vfs*, const char* name, int /*syncDir*/) {
    if (!name) return SQLITE_OK;
    const int32_t rc = flatsql_io_open(name,
                                       static_cast<int32_t>(std::strlen(name)),
                                       FLATSQL_IO_UNLINK);
    if (rc == FLATSQL_IO_ERR_NOENT) return SQLITE_OK;  // already gone
    return rc < 0 ? SQLITE_IOERR_DELETE : SQLITE_OK;
}

int fsAccess(sqlite3_vfs*, const char* name, int /*flags*/, int* outResult) {
    *outResult = 0;
    if (!name) return SQLITE_OK;
    const int32_t rc = flatsql_io_open(name,
                                       static_cast<int32_t>(std::strlen(name)),
                                       FLATSQL_IO_PROBE);
    *outResult = (rc >= 0) ? 1 : 0;
    return SQLITE_OK;
}

int fsFullPathname(sqlite3_vfs*, const char* name, int outLen, char* out) {
    // Paths are host-namespace strings. The host resolves them inside whatever
    // it preopened; the module never learns the real prefix and never builds
    // one. Identity keeps browser keys and POSIX paths on the same rules.
    if (!name) return SQLITE_ERROR;
    const size_t len = std::strlen(name);
    if (static_cast<int>(len) >= outLen) return SQLITE_ERROR;
    std::memcpy(out, name, len + 1);
    return SQLITE_OK;
}

int fsRandomness(sqlite3_vfs*, int byteCount, char* out) {
    // Seeded from the clock the host already provides. SQLite uses this for
    // temp-name entropy and journal nonces only; there is no security boundary
    // here, and pulling in a stronger source would add an import.
    static uint64_t state = 0;
    if (state == 0) {
        state = static_cast<uint64_t>(std::time(nullptr)) * 6364136223846793005ULL
              + reinterpret_cast<uintptr_t>(out) + 1442695040888963407ULL;
    }
    for (int i = 0; i < byteCount; i++) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        out[i] = static_cast<char>((state >> 33) & 0xFF);
    }
    return byteCount;
}

int fsSleep(sqlite3_vfs*, int microseconds) {
    // Nothing to sleep for: single writer, no lock contention, and neither
    // wasm lane may block the host thread.
    return microseconds;
}

int fsCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64* out) {
    // Julian day number in milliseconds. time() maps to the clock_time_get the
    // module already imports; no new import.
    *out = (static_cast<sqlite3_int64>(std::time(nullptr)) * 1000)
         + 24405875LL * 8640000LL;
    return SQLITE_OK;
}

int fsCurrentTime(sqlite3_vfs* vfs, double* out) {
    sqlite3_int64 millis = 0;
    fsCurrentTimeInt64(vfs, &millis);
    *out = static_cast<double>(millis) / 86400000.0;
    return SQLITE_OK;
}

int fsGetLastError(sqlite3_vfs*, int, char*) { return 0; }

sqlite3_vfs g_vfs = {
    2,                          // iVersion
    sizeof(FlatSqlFile),        // szOsFile
    kMaxPathLen,                // mxPathname
    nullptr,                    // pNext
    "flatsql_io",               // zName
    nullptr,                    // pAppData
    fsOpen,
    fsDelete,
    fsAccess,
    fsFullPathname,
    nullptr, nullptr, nullptr, nullptr,   // dlopen family: never available
    fsRandomness,
    fsSleep,
    fsCurrentTime,
    fsGetLastError,
    fsCurrentTimeInt64,
    nullptr, nullptr, nullptr,            // v3 syscall hooks
};

bool g_registered = false;

}  // namespace

namespace flatsql {

const char* const kFlatSqlVfsName = "flatsql_io";

int registerFlatSqlIoVfs(bool makeDefault) {
    if (g_registered) {
        return SQLITE_OK;
    }
    const int rc = sqlite3_vfs_register(&g_vfs, makeDefault ? 1 : 0);
    if (rc == SQLITE_OK) {
        g_registered = true;
    }
    return rc;
}

}  // namespace flatsql
