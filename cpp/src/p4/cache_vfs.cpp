// SQLite files are derived caches. Foreground transactions never sync them;
// explicit checkpoints and connection retirement retain SQLite's sync order.
// In particular, WAL NORMAL alone still syncs a newly started WAL header.
#include "internal.h"

#include <cstddef>

namespace flatsql {
namespace p4 {
namespace {
struct CacheFile {
    sqlite3_io_methods methods;
    const sqlite3_io_methods* underlying;
};

sqlite3_vfs cacheVfs{};
sqlite3_vfs* underlyingVfs = nullptr;
size_t fileTail = 0;
thread_local unsigned checkpointDepth = 0;

struct CheckpointScope {
    CheckpointScope() { ++checkpointDepth; }
    ~CheckpointScope() { --checkpointDepth; }
};

CacheFile* tail(sqlite3_file* file) {
    return reinterpret_cast<CacheFile*>(reinterpret_cast<unsigned char*>(file) + fileTail);
}

int cacheSync(sqlite3_file* file, int flags) {
    return checkpointDepth ? tail(file)->underlying->xSync(file, flags) : SQLITE_OK;
}

int cacheOpen(sqlite3_vfs*, const char* path, sqlite3_file* file, int flags, int* actualFlags) {
    const int rc = underlyingVfs->xOpen(underlyingVfs, path, file, flags, actualFlags);
    if (rc != SQLITE_OK) return rc;
    CacheFile* extra = tail(file);
    extra->underlying = file->pMethods;
    extra->methods = *file->pMethods;
    extra->methods.xSync = cacheSync;
    file->pMethods = &extra->methods;
    return SQLITE_OK;
}
}  // namespace

int registerCacheVfs() {
    static std::once_flag once;
    static int result = SQLITE_ERROR;
    std::call_once(once, [] {
        underlyingVfs = sqlite3_vfs_find("flatsql_io");
        if (!underlyingVfs) return;
        fileTail = (size_t(underlyingVfs->szOsFile) + alignof(CacheFile) - 1) & ~(alignof(CacheFile) - 1);
        cacheVfs = *underlyingVfs;
        cacheVfs.pNext = nullptr;
        cacheVfs.zName = "flatsql_p4_cache";
        cacheVfs.szOsFile = int(fileTail + sizeof(CacheFile));
        cacheVfs.xOpen = cacheOpen;
        result = sqlite3_vfs_register(&cacheVfs, 0);
    });
    return result;
}

int cacheCheckpoint(sqlite3* db, const char* name, int mode, int* log, int* checkpointed) {
    CheckpointScope scope;
    return sqlite3_wal_checkpoint_v2(db, name, mode, log, checkpointed);
}

int cacheClose(sqlite3* db) {
    CheckpointScope scope;
    return sqlite3_close_v2(db);
}
}  // namespace p4
}  // namespace flatsql
