// Store format 4: wasm32-wasip1-threads entry glue (flatsql-p4-threads.wasm
// and the wasm test commands; not part of the native build).
//
// - SQLite's OS layer is SQLITE_OS_OTHER: the only VFS is FlatSQL's own
//   flatsql_io (the seven host imports), registered as the default here. No
//   WASI file descriptor is ever opened.
// - SQLITE_OS_OTHER compiles only no-op mutexes, so pthread mutexes are
//   installed (SQLITE_CONFIG_MUTEX) before SQLite initializes. SQLite runs in
//   multi-thread mode (SQLITE_THREADSAFE=2): a connection is used by one
//   thread at a time.
// - Thread stacks: the wasi-libc default is 128 KiB and a wasm shadow stack has
//   no guard page; service threads get 1 MiB.
#if defined(__wasm__)

#include <pthread.h>
#include <sqlite3.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>

#include "flatsql/flatsql_io.h"

extern "C" {
extern size_t __default_stacksize;  // wasi-libc (musl) pthread_create default
}

namespace {

struct P4Mutex {
    pthread_mutex_t m;
};
constexpr int kStaticFirst = SQLITE_MUTEX_STATIC_MAIN;
constexpr int kStaticLast = SQLITE_MUTEX_STATIC_VFS3;
P4Mutex gStatic[kStaticLast - kStaticFirst + 1];

int mxInit() {
    for (auto& s : gStatic) pthread_mutex_init(&s.m, nullptr);
    return SQLITE_OK;
}
int mxEnd() { return SQLITE_OK; }
sqlite3_mutex* mxAlloc(int id) {
    if (id >= kStaticFirst && id <= kStaticLast) return reinterpret_cast<sqlite3_mutex*>(&gStatic[id - kStaticFirst]);
    if (id != SQLITE_MUTEX_FAST && id != SQLITE_MUTEX_RECURSIVE) return nullptr;
    auto* p = static_cast<P4Mutex*>(std::malloc(sizeof(P4Mutex)));
    if (!p) return nullptr;
    pthread_mutexattr_t a;
    pthread_mutexattr_init(&a);
    if (id == SQLITE_MUTEX_RECURSIVE) pthread_mutexattr_settype(&a, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&p->m, &a);
    pthread_mutexattr_destroy(&a);
    return reinterpret_cast<sqlite3_mutex*>(p);
}
void mxFree(sqlite3_mutex* x) {
    auto* p = reinterpret_cast<P4Mutex*>(x);
    if (p >= gStatic && p < gStatic + (kStaticLast - kStaticFirst + 1)) return;
    pthread_mutex_destroy(&p->m);
    std::free(p);
}
void mxEnter(sqlite3_mutex* x) { pthread_mutex_lock(&reinterpret_cast<P4Mutex*>(x)->m); }
int mxTry(sqlite3_mutex* x) {
    return pthread_mutex_trylock(&reinterpret_cast<P4Mutex*>(x)->m) == 0 ? SQLITE_OK : SQLITE_BUSY;
}
void mxLeave(sqlite3_mutex* x) { pthread_mutex_unlock(&reinterpret_cast<P4Mutex*>(x)->m); }
int mxHeld(sqlite3_mutex*) { return 1; }
int mxNotHeld(sqlite3_mutex*) { return 1; }

const sqlite3_mutex_methods kPthreadMutexes = {
    mxInit, mxEnd, mxAlloc, mxFree, mxEnter, mxTry, mxLeave, mxHeld, mxNotHeld,
};

__attribute__((constructor)) void p4SqliteMutexes() { sqlite3_config(SQLITE_CONFIG_MUTEX, &kPthreadMutexes); }

constexpr size_t kThreadStack = size_t(1) << 20;
__attribute__((constructor)) void p4DefaultThreadStack() {
    if (__default_stacksize < kThreadStack) __default_stacksize = kThreadStack;
}

}  // namespace

extern "C" {
int sqlite3_os_init(void) { return flatsql::registerFlatSqlIoVfs(true); }
int sqlite3_os_end(void) { return SQLITE_OK; }
}

#endif  // __wasm__
