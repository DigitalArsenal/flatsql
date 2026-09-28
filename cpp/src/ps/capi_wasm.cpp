// FlatSQL partition store: wasm32-wasip1-threads entry glue (T4).
//
// Linked into flatsql-ps-threads.wasm and the wasm test commands
// (cmake/flatsql_ps_wasm.cmake); not part of the native build.
//
// - flatsql_ps_alloc / flatsql_ps_free: the host places config TLVs, paths and
//   peer tokens in the instance's memory through these, and grows the heap
//   before flatsql_ps_start (alloc then free a large block): a thread that
//   touches memory another thread has just grown can fault on some hosts
//   (V8 refreshes each instance's view of a shared memory's size lazily).
// - SQLite's OS layer: SQLITE_OS_OTHER. The lanes open ":memory:" on the
//   flatsql_ps_null VFS by name (lane_arena.cpp); the default VFS registered
//   here opens no files either, and gives SQLite time, sleep and randomness
//   without a WASI file import. SQLITE_OS_OTHER compiles only the no-op
//   mutexes, so real pthread mutexes are installed (SQLITE_CONFIG_MUTEX)
//   before SQLite initializes; lane_arena.cpp wraps whatever is installed.
// - Thread stacks: std::thread uses the default pthread attributes. The
//   wasi-libc default is 128 KiB and a wasm shadow stack has no guard page, so
//   the default is raised to 1 MiB before any thread starts. Lanes set their
//   own stacks (ReaderConfig::stackBytes).
#if defined(__wasm__)

#include <pthread.h>
#include <sqlite3.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

#include "flatsql/ps/platform.h"

extern "C" {

extern size_t __default_stacksize;  // wasi-libc (musl) pthread_create default

__attribute__((export_name("flatsql_ps_alloc"))) void* flatsql_ps_alloc(int32_t n) {
    return n > 0 ? std::malloc(size_t(n)) : nullptr;
}

__attribute__((export_name("flatsql_ps_free"))) void flatsql_ps_free(void* p) { std::free(p); }

}  // extern "C"

namespace {

int osOpen(sqlite3_vfs*, const char*, sqlite3_file* f, int, int*) {
    f->pMethods = nullptr;
    return SQLITE_CANTOPEN;
}
int osDelete(sqlite3_vfs*, const char*, int) { return SQLITE_IOERR_DELETE; }
int osAccess(sqlite3_vfs*, const char*, int, int* out) {
    *out = 0;
    return SQLITE_OK;
}
int osFullPathname(sqlite3_vfs*, const char* name, int nOut, char* out) {
    sqlite3_snprintf(nOut, out, "%s", name);
    return SQLITE_OK;
}
int osRandomness(sqlite3_vfs*, int n, char* out) {
    // WASI random_get; getentropy takes at most 256 bytes per call.
    int done = 0;
    while (done < n) {
        const int step = n - done < 256 ? n - done : 256;
        if (getentropy(out + done, size_t(step)) != 0) {
            std::memset(out + done, 0, size_t(n - done));
            break;
        }
        done += step;
    }
    return n;
}
int osSleep(sqlite3_vfs*, int us) {
    flatsql::ps::sleepNs(uint64_t(us) * 1000);
    return us;
}
int osCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64* out) {
    *out = sqlite3_int64(210866760000000ll) + flatsql::ps::wallMs();  // Julian day, ms
    return SQLITE_OK;
}
int osCurrentTime(sqlite3_vfs* v, double* out) {
    sqlite3_int64 ms;
    osCurrentTimeInt64(v, &ms);
    *out = double(ms) / 86400000.0;
    return SQLITE_OK;
}
int osGetLastError(sqlite3_vfs*, int, char*) { return 0; }

sqlite3_vfs gWasmOsVfs = {
    2,                 // iVersion
    0,                 // szOsFile
    512,               // mxPathname
    nullptr,           // pNext
    "flatsql_ps_wasm", // zName
    nullptr,           // pAppData
    osOpen, osDelete, osAccess, osFullPathname,
    nullptr, nullptr, nullptr, nullptr,  // no dynamic libraries
    osRandomness, osSleep, osCurrentTime, osGetLastError, osCurrentTimeInt64,
    nullptr, nullptr, nullptr,
};

// ---- SQLite mutexes over pthreads ---------------------------------------------
struct PsMutex {
    pthread_mutex_t m;
};
constexpr int kStaticFirst = SQLITE_MUTEX_STATIC_MAIN;
constexpr int kStaticLast = SQLITE_MUTEX_STATIC_VFS3;
PsMutex gStatic[kStaticLast - kStaticFirst + 1];

int mxInit() {
    for (auto& s : gStatic) pthread_mutex_init(&s.m, nullptr);
    return SQLITE_OK;
}
int mxEnd() { return SQLITE_OK; }
sqlite3_mutex* mxAlloc(int id) {
    if (id >= kStaticFirst && id <= kStaticLast)
        return reinterpret_cast<sqlite3_mutex*>(&gStatic[id - kStaticFirst]);
    if (id != SQLITE_MUTEX_FAST && id != SQLITE_MUTEX_RECURSIVE) return nullptr;
    auto* p = static_cast<PsMutex*>(std::malloc(sizeof(PsMutex)));
    if (!p) return nullptr;
    pthread_mutexattr_t a;
    pthread_mutexattr_init(&a);
    if (id == SQLITE_MUTEX_RECURSIVE) pthread_mutexattr_settype(&a, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&p->m, &a);
    pthread_mutexattr_destroy(&a);
    return reinterpret_cast<sqlite3_mutex*>(p);
}
void mxFree(sqlite3_mutex* x) {
    auto* p = reinterpret_cast<PsMutex*>(x);
    if (p >= gStatic && p < gStatic + (kStaticLast - kStaticFirst + 1)) return;
    pthread_mutex_destroy(&p->m);
    std::free(p);
}
void mxEnter(sqlite3_mutex* x) { pthread_mutex_lock(&reinterpret_cast<PsMutex*>(x)->m); }
int mxTry(sqlite3_mutex* x) {
    return pthread_mutex_trylock(&reinterpret_cast<PsMutex*>(x)->m) == 0 ? SQLITE_OK : SQLITE_BUSY;
}
void mxLeave(sqlite3_mutex* x) { pthread_mutex_unlock(&reinterpret_cast<PsMutex*>(x)->m); }
// Only SQLite's debug assertions call these (the artifact is built NDEBUG).
int mxHeld(sqlite3_mutex*) { return 1; }
int mxNotHeld(sqlite3_mutex*) { return 1; }

const sqlite3_mutex_methods kPthreadMutexes = {
    mxInit, mxEnd, mxAlloc, mxFree, mxEnter, mxTry, mxLeave, mxHeld, mxNotHeld,
};

// Constructors run before main (commands) or in _initialize (the reactor),
// before any SQLite call.
__attribute__((constructor)) void psSqliteMutexes() { sqlite3_config(SQLITE_CONFIG_MUTEX, &kPthreadMutexes); }

// wasi-libc declares pthread_setattr_default_np but does not implement it; the
// variable behind it (musl's) is linkable from the same static link.
constexpr size_t kThreadStack = size_t(1) << 20;
__attribute__((constructor)) void psDefaultThreadStack() {
    if (__default_stacksize < kThreadStack) __default_stacksize = kThreadStack;
}

}  // namespace

extern "C" {

int sqlite3_os_init(void) { return sqlite3_vfs_register(&gWasmOsVfs, 1); }
int sqlite3_os_end(void) { return SQLITE_OK; }

}  // extern "C"

#endif  // __wasm__
