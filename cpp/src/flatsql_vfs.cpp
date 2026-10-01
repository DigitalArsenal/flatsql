// FlatSQL's own sqlite3_vfs, built on the seven-import host I/O contract in
// include/flatsql/flatsql_io.h.
//
// This is the whole reason disk-backed FlatSQL is isomorphic: the pager below
// is identical in every lane, and the only thing that differs between a browser
// IndexedDB store and a WasmEdge preopen is which host satisfies the seven
// imports. No `#ifdef __EMSCRIPTEN__` appears in this file, and none may.
//
// PER-PATH NODES. A main database file opened with the URI parameter share=1
// attaches to the one node of its path. The node holds what the connections
// to that path share, all in the instance's memory:
//
//   - the WAL index: heap regions shared by every connection to the path
//     (xShmMap/xShmLock/xShmBarrier/xShmUnmap), with the eight shm locks;
//   - the database-file locks (xLock/xUnlock/xCheckReservedLock), with the
//     unix VFS's inode rules: SHARED / RESERVED / PENDING / EXCLUSIVE. SQLite's
//     own "am I the last connection" test (EXCLUSIVE on the database file at
//     close) then sees the other connections, so closing one connection never
//     checkpoints and deletes the WAL under the others.
//
// A file opened without share=1 (format 1: FlatSQL is opened by exactly one
// writer, which the one-daemon-per-box law guarantees on the server and tab
// ownership in the browser) gets a private node: every lock is granted and the
// WAL index is private heap memory, as before nodes existed. SQLite's own unix
// VFS simulates shared memory with heap memory the same way when a database is
// held under an exclusive lock (sqlite3.c, unixOpenSharedMemory). Store format
// 4 opens every connection with share=1: one writer and readers per file, all
// in one instance, share the node. Paths are never reused by format 4
// (generation-suffixed file names), so a node keyed by path never attaches to
// a dropped file.
//
// READAHEAD. A connection opened with the URI parameter ra=1 (format 4's
// readers) reads ahead sequentially: after three nearby reads in one
// direction it reads up to `bytes` at once, forward or backward, in up to
// `streams` streams per connection. The buffers are dropped whenever the
// connection takes or releases a WAL read-mark lock, so a page from before a
// checkpoint is never served: inside one read transaction SQLite reads from
// the database file only pages that no checkpoint may overwrite (backfill
// stops at the oldest reader's mark).
//
// DIRECTORY DURABILITY. A connection opened with the URI parameter dsync=1
// creates its database, WAL and journal files with FLATSQL_IO_CREATE_PARENTS,
// so a host fsyncs the parent directory when the file is newly created and a
// commit that lives in a new file survives power loss. Without it (format 1)
// opens are unchanged.

#include "flatsql/flatsql_io.h"

#include <sqlite3.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <unordered_map>

// Threads exist natively and on wasm32 with the atomics feature
// (wasm32-wasip1-threads). The single-threaded wasm lanes compile no mutex.
#if !defined(__wasm__) || defined(__wasm_atomics__)
#define FLATSQL_VFS_THREADS 1
#include <mutex>
#if !defined(__wasm__)
#include <time.h>
#endif
#else
#define FLATSQL_VFS_THREADS 0
#endif

namespace {

constexpr int kMaxPathLen = 1024;

// 64 regions is the cap. Regions are szRegion bytes (32 KiB in practice), so
// this covers a wal-index far larger than any WAL this engine will checkpoint,
// and the map fails loudly rather than silently wrapping past the end.
constexpr int kMaxShmRegions = 64;
constexpr int kShmLocks = SQLITE_SHM_NLOCK;   // 8
constexpr int kReadMarkFirst = 3;             // WAL_READ_LOCK(0)
constexpr int kRaStreak = 3;
constexpr int kRaMaxStreams = 8;

#if FLATSQL_VFS_THREADS
std::mutex& nodeMutex() {
    static std::mutex* m = new std::mutex();  // never destroyed: files may outlive statics
    return *m;
}
struct NodeLock {
    NodeLock() { nodeMutex().lock(); }
    ~NodeLock() { nodeMutex().unlock(); }
};
#else
struct NodeLock {};
#endif

struct FlatSqlFile;

struct Node {
    std::string path;
    bool shared = false;  // in the path table (share=1); else private to one file
    int nref = 0;  // open main-database files on this path
    // database-file locks
    int nshared = 0;  // files holding SHARED or more
    FlatSqlFile* reserved = nullptr;
    FlatSqlFile* pending = nullptr;
    FlatSqlFile* exclusive = nullptr;
    // WAL index
    void* regions[kMaxShmRegions] = {};
    int regionSize = 0;
    int nshm = 0;  // files that mapped the WAL index
    int shmShared[kShmLocks] = {};
    FlatSqlFile* shmExcl[kShmLocks] = {};
};

std::unordered_map<std::string, Node*>& nodes() {
    static auto* m = new std::unordered_map<std::string, Node*>();
    return *m;
}

std::atomic<int64_t> gShmBytes{0};
std::atomic<int64_t> gShmBytesPeak{0};
std::atomic<int64_t> gRaReads{0};
std::atomic<int64_t> gRaHits{0};
std::atomic<int64_t> gRaBytes{0};
std::atomic<int> gRaStreams{2};
std::atomic<int> gRaBytesPerRead{1 << 20};

struct RaStream {
    uint8_t* buf;
    sqlite3_int64 off;
    int len;
    sqlite3_int64 last;
    int dir;
    int streak;
    uint64_t used;
};

struct FlatSqlFile {
    sqlite3_file base;   // must be first
    int32_t handle;
    int deleteOnClose;
    char path[kMaxPathLen];
    Node* node;          // main database files only
    int lock;            // this file's SQLITE_LOCK_* level
    int mapped;          // mapped the node's WAL index
    uint16_t shmShared;  // shm locks held SHARED by this file
    uint16_t shmExcl;    // shm locks held EXCLUSIVE by this file
    // readahead (ra=1)
    int ra;
    RaStream rs[kRaMaxStreams];
    uint64_t tick;
};

void freeRegions(Node* n) {
    for (int i = 0; i < kMaxShmRegions; ++i) {
        if (n->regions[i] != nullptr) {
            sqlite3_free(n->regions[i]);
            n->regions[i] = nullptr;
            gShmBytes.fetch_sub(n->regionSize, std::memory_order_relaxed);
        }
    }
    n->regionSize = 0;
}

// Node table mutex held.
Node* nodeAttach(const char* path, bool shared) {
    if (!shared) {
        Node* n = new (std::nothrow) Node();
        if (n) n->nref = 1;
        return n;
    }
    auto& m = nodes();
    auto it = m.find(path);
    Node* n;
    if (it == m.end()) {
        n = new (std::nothrow) Node();
        if (!n) return nullptr;
        n->path = path;
        n->shared = true;
        m.emplace(n->path, n);
    } else {
        n = it->second;
    }
    n->nref++;
    return n;
}

// Node table mutex held.
void nodeRelease(Node* n) {
    if (--n->nref > 0 || n->nshm > 0) return;
    freeRegions(n);
    if (n->shared) nodes().erase(n->path);
    delete n;
}

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

void raDrop(FlatSqlFile* f) {
    for (int i = 0; i < kRaMaxStreams; ++i) {
        if (f->rs[i].buf) sqlite3_free(f->rs[i].buf);
        f->rs[i] = RaStream{nullptr, 0, 0, -1, 0, 0, 0};
    }
}

// Drops this file's shm locks and mapping. Node table mutex held.
void shmDetach(FlatSqlFile* f) {
    Node* n = f->node;
    if (!n || !f->mapped) return;
    for (int i = 0; i < kShmLocks; ++i) {
        const uint16_t b = uint16_t(1u << i);
        if (f->shmExcl & b) n->shmExcl[i] = nullptr;
        if (f->shmShared & b) n->shmShared[i]--;
    }
    f->shmExcl = f->shmShared = 0;
    f->mapped = 0;
    // The last unmap frees the WAL index: the next connection rebuilds it
    // from the -wal file (recovery), as a lone connection always did.
    if (--n->nshm == 0) freeRegions(n);
}

int fsClose(sqlite3_file* file) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    if (f->node) {
        NodeLock g;
        Node* n = f->node;
        shmDetach(f);
        if (f->lock >= SQLITE_LOCK_SHARED) n->nshared--;
        if (n->reserved == f) n->reserved = nullptr;
        if (n->pending == f) n->pending = nullptr;
        if (n->exclusive == f) n->exclusive = nullptr;
        f->lock = SQLITE_LOCK_NONE;
        nodeRelease(n);
        f->node = nullptr;
    }
    if (f->ra) raDrop(f);
    if (f->handle < 0) return SQLITE_OK;
    const int32_t rc = flatsql_io_close(f->handle);
    f->handle = -1;
    if (f->deleteOnClose && f->path[0]) {
        flatsql_io_open(f->path, static_cast<int32_t>(std::strlen(f->path)),
                        FLATSQL_IO_UNLINK);
    }
    return rc < 0 ? mapIoError(rc, SQLITE_IOERR_CLOSE) : SQLITE_OK;
}

int ioRead(FlatSqlFile* f, void* buf, int amount, sqlite3_int64 offset) {
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

// Sequential readahead (ra=1 connections).
int raRead(FlatSqlFile* f, void* buf, int amount, sqlite3_int64 offset) {
    const int streams = gRaStreams.load(std::memory_order_relaxed);
    const int raBytes = gRaBytesPerRead.load(std::memory_order_relaxed);
    f->tick++;
    for (int i = 0; i < streams; ++i) {
        RaStream* s = &f->rs[i];
        if (s->len > 0 && offset >= s->off && offset + amount <= s->off + s->len) {
            std::memcpy(buf, s->buf + (offset - s->off), static_cast<size_t>(amount));
            s->last = offset;
            s->used = f->tick;
            gRaHits.fetch_add(1, std::memory_order_relaxed);
            return SQLITE_OK;
        }
    }
    // The stream this read continues: the one whose last read is nearest
    // (within a readahead), else the least recently used one restarts.
    RaStream* s = nullptr;
    sqlite3_int64 best = INT64_MAX;
    for (int i = 0; i < streams; ++i) {
        RaStream* x = &f->rs[i];
        if (x->last < 0) continue;
        sqlite3_int64 d = offset - x->last;
        if (d < 0) d = -d;
        if (d <= raBytes && d < best) {
            best = d;
            s = x;
        }
    }
    if (!s) {
        s = &f->rs[0];
        for (int i = 1; i < streams; ++i)
            if (f->rs[i].used < s->used) s = &f->rs[i];
        s->streak = 0;
        s->dir = 0;
        s->last = offset;
        s->used = f->tick;
        s->len = 0;
        return ioRead(f, buf, amount, offset);
    }
    const sqlite3_int64 d = offset - s->last;
    const int dir = d > 0 ? 1 : d < 0 ? -1 : 0;
    if (dir != 0 && dir == s->dir) s->streak++; else s->streak = 0;
    s->dir = dir;
    s->last = offset;
    s->used = f->tick;
    if (s->streak >= kRaStreak && amount <= raBytes / 4) {
        sqlite3_int64 start = dir > 0 ? offset : offset + amount - raBytes;
        if (start < 0) start = 0;
        const double size = flatsql_io_size(f->handle);
        sqlite3_int64 end = start + raBytes;
        if (size >= 0 && end > static_cast<sqlite3_int64>(size)) end = static_cast<sqlite3_int64>(size);
        if (end >= offset + amount && end > start) {
            if (!s->buf) s->buf = static_cast<uint8_t*>(sqlite3_malloc(raBytes));
            const int len = static_cast<int>(end - start);
            if (s->buf) {
                const int32_t got = flatsql_io_read(f->handle, s->buf, len, static_cast<double>(start));
                if (got == len) {
                    s->off = start;
                    s->len = len;
                    gRaReads.fetch_add(1, std::memory_order_relaxed);
                    gRaBytes.fetch_add(len, std::memory_order_relaxed);
                    std::memcpy(buf, s->buf + (offset - start), static_cast<size_t>(amount));
                    return SQLITE_OK;
                }
            }
            s->len = 0;
        }
    }
    return ioRead(f, buf, amount, offset);
}

int fsRead(sqlite3_file* file, void* buf, int amount, sqlite3_int64 offset) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    if (f->handle < 0) return SQLITE_IOERR_READ;
    if (f->ra) return raRead(f, buf, amount, offset);
    return ioRead(f, buf, amount, offset);
}

int fsWrite(sqlite3_file* file, const void* buf, int amount,
            sqlite3_int64 offset) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    if (f->handle < 0) return SQLITE_IOERR_WRITE;
    if (f->ra) raDrop(f);
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
    if (f->ra) raDrop(f);
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

// Database-file locks in memory, with the unix VFS's inode rules.
int fsLock(sqlite3_file* file, int want) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    if (!f->node || f->lock >= want) return SQLITE_OK;
    NodeLock g;
    Node* n = f->node;
    const bool otherPending = n->pending && n->pending != f;
    const bool otherExclusive = n->exclusive && n->exclusive != f;
    if (want == SQLITE_LOCK_SHARED) {
        if (otherPending || otherExclusive) return SQLITE_BUSY;
        n->nshared++;
        f->lock = SQLITE_LOCK_SHARED;
        return SQLITE_OK;
    }
    if (want == SQLITE_LOCK_RESERVED) {
        if ((n->reserved && n->reserved != f) || otherPending || otherExclusive) return SQLITE_BUSY;
        n->reserved = f;
        f->lock = SQLITE_LOCK_RESERVED;
        return SQLITE_OK;
    }
    // EXCLUSIVE, by way of PENDING: once PENDING is held no new SHARED lock is
    // granted, and EXCLUSIVE waits for the other SHARED holders to leave.
    if (otherPending || otherExclusive) return SQLITE_BUSY;
    n->pending = f;
    if (f->lock < SQLITE_LOCK_PENDING) f->lock = SQLITE_LOCK_PENDING;
    if (n->nshared - (f->lock >= SQLITE_LOCK_SHARED ? 1 : 0) > 0) return SQLITE_BUSY;
    n->exclusive = f;
    n->pending = nullptr;
    f->lock = SQLITE_LOCK_EXCLUSIVE;
    return SQLITE_OK;
}

int fsUnlock(sqlite3_file* file, int to) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    if (!f->node || f->lock <= to) return SQLITE_OK;
    NodeLock g;
    Node* n = f->node;
    if (n->reserved == f) n->reserved = nullptr;
    if (n->pending == f) n->pending = nullptr;
    if (n->exclusive == f) n->exclusive = nullptr;
    if (to == SQLITE_LOCK_NONE && f->lock >= SQLITE_LOCK_SHARED) n->nshared--;
    f->lock = to;
    return SQLITE_OK;
}

int fsCheckReservedLock(sqlite3_file* file, int* out) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    *out = 0;
    if (!f->node) return SQLITE_OK;
    NodeLock g;
    Node* n = f->node;
    *out = (n->reserved && n->reserved != f) || (n->pending && n->pending != f) ||
           (n->exclusive && n->exclusive != f);
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

int fsShmMap(sqlite3_file* file, int iRegion, int szRegion, int bExtend,
             void volatile** pp) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    *pp = nullptr;
    if (!f->node || iRegion < 0 || iRegion >= kMaxShmRegions || szRegion <= 0) {
        return SQLITE_IOERR_SHMMAP;
    }
    NodeLock g;
    Node* n = f->node;
    if (n->regionSize == 0) {
        n->regionSize = szRegion;
    } else if (n->regionSize != szRegion) {
        return SQLITE_IOERR_SHMMAP;
    }
    if (!f->mapped) {
        f->mapped = 1;
        n->nshm++;
    }
    if (n->regions[iRegion] == nullptr) {
        // bExtend == 0 asks "does it exist yet?" and must not allocate: the
        // pager uses the null answer to decide a recovery is needed.
        if (!bExtend) return SQLITE_OK;
        void* mem = sqlite3_malloc64(static_cast<sqlite3_uint64>(szRegion));
        if (mem == nullptr) return SQLITE_NOMEM;
        // Zeroed: SQLite requires a freshly created wal-index region to read
        // as zero, or a dirty region is read as a valid header.
        std::memset(mem, 0, static_cast<size_t>(szRegion));
        n->regions[iRegion] = mem;
        const int64_t now = gShmBytes.fetch_add(szRegion, std::memory_order_relaxed) + szRegion;
        int64_t peak = gShmBytesPeak.load(std::memory_order_relaxed);
        while (now > peak && !gShmBytesPeak.compare_exchange_weak(peak, now)) {
        }
    }
    *pp = n->regions[iRegion];
    return SQLITE_OK;
}

int fsShmLock(sqlite3_file* file, int ofst, int nLocks, int flags) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    if (!f->node || !f->mapped || ofst < 0 || nLocks < 1 || ofst + nLocks > kShmLocks)
        return SQLITE_IOERR_SHMLOCK;
    // A read-mark lock starts or ends a WAL read transaction: drop the
    // readahead buffers (they may hold pages a checkpoint has rewritten since).
    if (f->ra && ofst + nLocks > kReadMarkFirst) raDrop(f);
    NodeLock g;
    Node* n = f->node;
    if (flags & SQLITE_SHM_UNLOCK) {
        for (int i = ofst; i < ofst + nLocks; ++i) {
            const uint16_t b = uint16_t(1u << i);
            if (f->shmExcl & b) {
                n->shmExcl[i] = nullptr;
                f->shmExcl = uint16_t(f->shmExcl & ~b);
            }
            if (f->shmShared & b) {
                n->shmShared[i]--;
                f->shmShared = uint16_t(f->shmShared & ~b);
            }
        }
        return SQLITE_OK;
    }
    if (flags & SQLITE_SHM_SHARED) {
        for (int i = ofst; i < ofst + nLocks; ++i)
            if (n->shmExcl[i] && n->shmExcl[i] != f) return SQLITE_BUSY;
        for (int i = ofst; i < ofst + nLocks; ++i) {
            const uint16_t b = uint16_t(1u << i);
            if (!(f->shmShared & b)) {
                n->shmShared[i]++;
                f->shmShared = uint16_t(f->shmShared | b);
            }
        }
        return SQLITE_OK;
    }
    for (int i = ofst; i < ofst + nLocks; ++i) {
        const uint16_t b = uint16_t(1u << i);
        if ((n->shmExcl[i] && n->shmExcl[i] != f) ||
            n->shmShared[i] - ((f->shmShared & b) ? 1 : 0) > 0)
            return SQLITE_BUSY;
    }
    for (int i = ofst; i < ofst + nLocks; ++i) {
        n->shmExcl[i] = f;
        f->shmExcl = uint16_t(f->shmExcl | (1u << i));
    }
    return SQLITE_OK;
}

void fsShmBarrier(sqlite3_file*) {
#if FLATSQL_VFS_THREADS
    std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
}

int fsShmUnmap(sqlite3_file* file, int /*deleteFlag*/) {
    auto* f = reinterpret_cast<FlatSqlFile*>(file);
    if (!f->node) return SQLITE_OK;
    NodeLock g;
    shmDetach(f);
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

int32_t translateOpenFlags(const char* name, int flags) {
    int32_t out = 0;
    if (flags & SQLITE_OPEN_READONLY)  out |= FLATSQL_IO_READ;
    if (flags & SQLITE_OPEN_READWRITE) out |= FLATSQL_IO_READ | FLATSQL_IO_WRITE;
    if (flags & SQLITE_OPEN_CREATE)    out |= FLATSQL_IO_CREATE;
    if (flags & SQLITE_OPEN_EXCLUSIVE) out |= FLATSQL_IO_EXCL;
    if (flags & SQLITE_OPEN_DELETEONCLOSE) out |= FLATSQL_IO_DELETE_ON_CLOSE;
    if ((flags & SQLITE_OPEN_CREATE) && (flags & (SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_WAL | SQLITE_OPEN_MAIN_JOURNAL)) &&
        sqlite3_uri_boolean(name, "dsync", 0))
        out |= FLATSQL_IO_CREATE_PARENTS;
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
                                           translateOpenFlags(name, flags));
    if (handle < 0) return mapIoError(handle, SQLITE_CANTOPEN);

    f->handle = handle;
    f->deleteOnClose = (flags & SQLITE_OPEN_DELETEONCLOSE) ? 1 : 0;
    if (flags & SQLITE_OPEN_MAIN_DB) {
        {
            NodeLock g;
            f->node = nodeAttach(f->path, sqlite3_uri_boolean(name, "share", 0) != 0);
        }
        if (!f->node) {
            flatsql_io_close(handle);
            f->handle = -1;
            return SQLITE_NOMEM;
        }
        f->ra = sqlite3_uri_boolean(name, "ra", 0) ? 1 : 0;
        if (f->ra) raDrop(f);
    }
    f->base.pMethods = &kIoMethods;
    if (outFlags) {
        *outFlags = (flags & SQLITE_OPEN_READONLY) ? SQLITE_OPEN_READONLY
                                                   : SQLITE_OPEN_READWRITE;
    }
    return SQLITE_OK;
}

int fsDelete(sqlite3_vfs*, const char* name, int syncDir) {
    if (!name) return SQLITE_OK;
    // A dsync=1 connection's deletes are durable when SQLite asks (a rollback
    // journal's delete is its commit: a crash must not bring it back as a hot
    // journal). UNLINK_IF_UNUSED makes the host fsync the parent directory.
    const bool durable = syncDir && sqlite3_uri_boolean(name, "dsync", 0);
    int32_t rc = flatsql_io_open(name, static_cast<int32_t>(std::strlen(name)),
                                 durable ? FLATSQL_IO_UNLINK | FLATSQL_IO_UNLINK_IF_UNUSED : FLATSQL_IO_UNLINK);
    if (durable && rc == FLATSQL_IO_ERR_BUSY)
        rc = flatsql_io_open(name, static_cast<int32_t>(std::strlen(name)), FLATSQL_IO_UNLINK);
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
#if FLATSQL_VFS_THREADS
    // Several connections to one path (format 4) can meet a held lock; the
    // busy handler and WAL retries sleep here. One connection never does.
    if (microseconds > 0) {
#if defined(__wasm__)
        // memory.atomic.wait32 on a word that never changes: a timed sleep
        // without a WASI import. The address reaches the wait through a
        // volatile so the memarg offset is 0 (WasmEdge 0.16.4's AOT compiler
        // drops a nonzero offset, turning the wait into a spin).
        int32_t never = 0;
        int32_t* volatile addr = &never;
        __builtin_wasm_memory_atomic_wait32(addr, 0, int64_t(microseconds) * 1000);
#else
        struct timespec ts;
        ts.tv_sec = microseconds / 1000000;
        ts.tv_nsec = static_cast<long>(microseconds % 1000000) * 1000;
        nanosleep(&ts, nullptr);
#endif
    }
#endif
    // The single-threaded lanes have one connection and nothing to wait for,
    // and may not block the host thread.
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

void setFlatSqlIoReadahead(int streams, int bytes) {
    if (streams < 1) streams = 1;
    if (streams > kRaMaxStreams) streams = kRaMaxStreams;
    if (bytes < 64 * 1024) bytes = 64 * 1024;
    gRaStreams.store(streams, std::memory_order_relaxed);
    gRaBytesPerRead.store(bytes, std::memory_order_relaxed);
}

FlatSqlIoVfsStats flatSqlIoVfsStats() {
    FlatSqlIoVfsStats s;
    s.walIndexBytes = gShmBytes.load(std::memory_order_relaxed);
    s.walIndexBytesPeak = gShmBytesPeak.load(std::memory_order_relaxed);
    s.readaheadReads = gRaReads.load(std::memory_order_relaxed);
    s.readaheadHits = gRaHits.load(std::memory_order_relaxed);
    s.readaheadBytes = gRaBytes.load(std::memory_order_relaxed);
    {
        NodeLock g;
        s.nodes = static_cast<int64_t>(nodes().size());
    }
    return s;
}

}  // namespace flatsql
