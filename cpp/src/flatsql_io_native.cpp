// Native satisfaction of the seven-import host I/O contract.
//
// On wasm these seven symbols are IMPORTS (see flatsql_io.h) and the host
// supplies them. Natively there is no host, so this file supplies them over
// POSIX pread/pwrite — which is what makes the VFS and the partition store
// testable in CTest.
//
// This is not a test double. It is a third lane, held to the same contract as
// the browser shim and the Go host, and the native tests that run against it
// are the cheapest parity evidence we have.
//
// Concurrency (partition store, docs/PARTITION-STORE.md §5.4): read, write,
// truncate, sync and size take NO lock. Handles index a chunked slot table
// whose chunks never move, so a lookup is two loads. Only open, close and
// unlink take the table mutex, and none of them holds it across a data
// syscall's wait for the device. A sync never holds any lock (design §22.3a
// minor 4: the previous version held one global mutex across fsync).
//
// Durability: darwin uses fcntl(F_FULLFSYNC) (plain fsync there does not
// flush the drive cache), falling back to fsync where the file system refuses
// it; Linux uses fdatasync. Directory handles use the same barrier.

#if !defined(__wasm__)

#include "flatsql/flatsql_io.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>

namespace {

constexpr int kChunkBits = 10;
constexpr int kChunkSize = 1 << kChunkBits;
constexpr int kMaxChunks = 1024;  // 1M handles

struct NativeSlot {
    std::atomic<int> fd{-1};
    std::atomic<uint32_t> inUse{0};
    bool isDir = false;
    std::string path;
};

struct Chunk {
    NativeSlot slots[kChunkSize];
};

struct Table {
    std::atomic<Chunk*> chunks[kMaxChunks];
    std::mutex mutex;  // open/close/unlink only
    int nextFree = 0;  // scan hint
    int highWater = 0;
    std::unordered_map<std::string, int> openCount;
    Table() {
        for (auto& c : chunks) c.store(nullptr, std::memory_order_relaxed);
    }
};

Table& table() {
    static Table* t = new Table();  // never destroyed: handles may outlive statics
    return *t;
}

NativeSlot* slotAt(int index) {
    if (index < 0 || index >= kChunkSize * kMaxChunks) return nullptr;
    Chunk* chunk = table().chunks[index >> kChunkBits].load(std::memory_order_acquire);
    if (!chunk) return nullptr;
    return &chunk->slots[index & (kChunkSize - 1)];
}

NativeSlot* slotFor(int32_t handle) {
    NativeSlot* slot = slotAt(handle);
    if (!slot || !slot->inUse.load(std::memory_order_acquire)) return nullptr;
    return slot;
}

int32_t errnoToStatus(int e) {
    switch (e) {
        case ENOENT: return FLATSQL_IO_ERR_NOENT;
        case EACCES:
        case EPERM:  return FLATSQL_IO_ERR_ACCESS;
        case ENOSPC:
#ifdef EDQUOT
        case EDQUOT:
#endif
            return FLATSQL_IO_ERR_NOSPACE;
        case EIO:    return FLATSQL_IO_ERR_IO;
        case EEXIST: return FLATSQL_IO_ERR_EXIST;
        case EBUSY:  return FLATSQL_IO_ERR_BUSY;
        default:     return FLATSQL_IO_ERR_GENERIC;
    }
}

int fullSync(int fd) {
#if defined(__APPLE__)
    if (::fcntl(fd, F_FULLFSYNC) == 0) return 0;
    // Some file systems (and directory fds on some volumes) refuse the full
    // barrier; fsync is the strongest request they accept.
    return ::fsync(fd);
#else
    return ::fdatasync(fd);
#endif
}

int dirSync(const std::string& dir) {
    const int fd = ::open(dir.empty() ? "." : dir.c_str(), O_RDONLY
#ifdef O_DIRECTORY
                                                              | O_DIRECTORY
#endif
    );
    if (fd < 0) return -1;
    int rc;
#if defined(__APPLE__)
    rc = ::fcntl(fd, F_FULLFSYNC);
    if (rc != 0) rc = ::fsync(fd);
#else
    rc = ::fsync(fd);
#endif
    const int saved = errno;
    ::close(fd);
    errno = saved;
    return rc;
}

std::string parentOf(const std::string& path) {
    const size_t slash = path.find_last_of('/');
    if (slash == std::string::npos) return ".";
    if (slash == 0) return "/";
    return path.substr(0, slash);
}

// mkdir -p for the parents of `path`, fsyncing the parent of every directory
// it creates. Returns 0 or a negative status.
int32_t createParents(const std::string& path) {
    const std::string dir = parentOf(path);
    if (dir == "." || dir == "/") return 0;
    struct stat st;
    if (::stat(dir.c_str(), &st) == 0) return S_ISDIR(st.st_mode) ? 0 : FLATSQL_IO_ERR_GENERIC;
    const int32_t up = createParents(dir);
    if (up < 0) return up;
    if (::mkdir(dir.c_str(), 0755) != 0) {
        if (errno != EEXIST) return errnoToStatus(errno);
        return 0;
    }
    if (dirSync(parentOf(dir)) != 0) return errnoToStatus(errno);
    return 0;
}

int32_t allocSlot(int fd, const std::string& name, bool isDir) {
    Table& t = table();
    std::lock_guard<std::mutex> guard(t.mutex);
    for (int pass = 0; pass < 2; pass++) {
        const int start = pass == 0 ? t.nextFree : 0;
        const int limit = pass == 0 ? t.highWater : t.nextFree;
        for (int i = start; i < limit; i++) {
            NativeSlot* s = slotAt(i);
            if (s && !s->inUse.load(std::memory_order_relaxed)) {
                s->path = name;
                s->isDir = isDir;
                s->fd.store(fd, std::memory_order_relaxed);
                s->inUse.store(1, std::memory_order_release);
                t.nextFree = i + 1;
                t.openCount[name]++;
                return i;
            }
        }
    }
    const int index = t.highWater;
    if (index >= kChunkSize * kMaxChunks) return FLATSQL_IO_ERR_GENERIC;
    const int c = index >> kChunkBits;
    if (!t.chunks[c].load(std::memory_order_relaxed))
        t.chunks[c].store(new Chunk(), std::memory_order_release);
    NativeSlot* s = slotAt(index);
    s->path = name;
    s->isDir = isDir;
    s->fd.store(fd, std::memory_order_relaxed);
    s->inUse.store(1, std::memory_order_release);
    t.highWater = index + 1;
    t.nextFree = index + 1;
    t.openCount[name]++;
    return index;
}

}  // namespace

extern "C" {

int32_t flatsql_io_open(const char* path, int32_t pathLen, int32_t flags) {
    if (!path || pathLen <= 0) return FLATSQL_IO_ERR_GENERIC;
    const std::string name(path, static_cast<size_t>(pathLen));

    if (flags & FLATSQL_IO_PROBE) {
        struct stat st;
        return (::stat(name.c_str(), &st) == 0) ? 0 : FLATSQL_IO_ERR_NOENT;
    }
    if (flags & FLATSQL_IO_UNLINK) {
        if (flags & FLATSQL_IO_UNLINK_IF_UNUSED) {
            Table& t = table();
            std::lock_guard<std::mutex> guard(t.mutex);
            auto it = t.openCount.find(name);
            if (it != t.openCount.end() && it->second > 0) return FLATSQL_IO_ERR_BUSY;
            if (::unlink(name.c_str()) != 0) return errnoToStatus(errno);
        } else if (::unlink(name.c_str()) != 0) {
            return errnoToStatus(errno);
        }
        if (flags & FLATSQL_IO_UNLINK_IF_UNUSED) {
            if (dirSync(parentOf(name)) != 0) return errnoToStatus(errno);
        }
        return 0;
    }
    if (flags & FLATSQL_IO_DIRECTORY) {
        if (flags & FLATSQL_IO_CREATE_PARENTS) {
            const int32_t rc = createParents(name + "/x");
            if (rc < 0) return rc;
        }
        const int fd = ::open(name.c_str(), O_RDONLY
#ifdef O_DIRECTORY
                                                 | O_DIRECTORY
#endif
        );
        if (fd < 0) return errnoToStatus(errno);
        const int32_t h = allocSlot(fd, name, true);
        if (h < 0) ::close(fd);
        return h;
    }

    int oflags = 0;
    if ((flags & FLATSQL_IO_WRITE) && (flags & FLATSQL_IO_READ)) oflags |= O_RDWR;
    else if (flags & FLATSQL_IO_WRITE)                           oflags |= O_WRONLY;
    else                                                          oflags |= O_RDONLY;
    if (flags & FLATSQL_IO_TRUNC)  oflags |= O_TRUNC;
#ifdef O_CLOEXEC
    oflags |= O_CLOEXEC;
#endif

    bool created = false;
    int fd = -1;
    if (flags & FLATSQL_IO_CREATE) {
        if (flags & FLATSQL_IO_CREATE_PARENTS) {
            const int32_t rc = createParents(name);
            if (rc < 0) return rc;
        }
        // Distinguish a new file from an existing one so the parent directory
        // is fsynced exactly when a directory entry was added.
        fd = ::open(name.c_str(), oflags | O_CREAT | O_EXCL, 0644);
        if (fd >= 0) {
            created = true;
        } else if (errno == EEXIST && !(flags & FLATSQL_IO_EXCL)) {
            fd = ::open(name.c_str(), oflags, 0644);
        }
    } else {
        fd = ::open(name.c_str(), oflags, 0644);
    }
    if (fd < 0) return errnoToStatus(errno);
    if (created && (flags & FLATSQL_IO_CREATE_PARENTS)) {
        if (dirSync(parentOf(name)) != 0) {
            const int e = errno;
            ::close(fd);
            return errnoToStatus(e);
        }
    }
    const int32_t h = allocSlot(fd, name, false);
    if (h < 0) ::close(fd);
    return h;
}

int32_t flatsql_io_read(int32_t handle, void* dst, int32_t len, double offset) {
    if (!dst || len < 0) return FLATSQL_IO_ERR_GENERIC;
    NativeSlot* slot = slotFor(handle);
    if (!slot || slot->isDir) return FLATSQL_IO_ERR_BADHANDLE;
    size_t done = 0;
    while (done < size_t(len)) {
        const ssize_t n = ::pread(slot->fd.load(std::memory_order_relaxed),
                                  static_cast<char*>(dst) + done, size_t(len) - done,
                                  static_cast<off_t>(offset) + off_t(done));
        if (n < 0) {
            if (errno == EINTR) continue;
            return errnoToStatus(errno);
        }
        if (n == 0) break;
        done += size_t(n);
    }
    return static_cast<int32_t>(done);
}

int32_t flatsql_io_write(int32_t handle, const void* src, int32_t len,
                         double offset) {
    if (!src || len < 0) return FLATSQL_IO_ERR_GENERIC;
    NativeSlot* slot = slotFor(handle);
    if (!slot || slot->isDir) return FLATSQL_IO_ERR_BADHANDLE;
    size_t done = 0;
    while (done < size_t(len)) {
        const ssize_t n = ::pwrite(slot->fd.load(std::memory_order_relaxed),
                                   static_cast<const char*>(src) + done, size_t(len) - done,
                                   static_cast<off_t>(offset) + off_t(done));
        if (n < 0) {
            if (errno == EINTR) continue;
            return errnoToStatus(errno);
        }
        done += size_t(n);
    }
    return static_cast<int32_t>(done);
}

int32_t flatsql_io_truncate(int32_t handle, double size) {
    NativeSlot* slot = slotFor(handle);
    if (!slot || slot->isDir) return FLATSQL_IO_ERR_BADHANDLE;
    if (::ftruncate(slot->fd.load(std::memory_order_relaxed), static_cast<off_t>(size)) != 0)
        return errnoToStatus(errno);
    return 0;
}

int32_t flatsql_io_sync(int32_t handle) {
    NativeSlot* slot = slotFor(handle);
    if (!slot) return FLATSQL_IO_ERR_BADHANDLE;
    if (fullSync(slot->fd.load(std::memory_order_relaxed)) != 0) return errnoToStatus(errno);
    return 0;
}

double flatsql_io_size(int32_t handle) {
    NativeSlot* slot = slotFor(handle);
    if (!slot || slot->isDir) return static_cast<double>(FLATSQL_IO_ERR_BADHANDLE);
    struct stat st;
    if (::fstat(slot->fd.load(std::memory_order_relaxed), &st) != 0)
        return static_cast<double>(errnoToStatus(errno));
    return static_cast<double>(st.st_size);
}

int32_t flatsql_io_close(int32_t handle) {
    Table& t = table();
    int fd;
    {
        std::lock_guard<std::mutex> guard(t.mutex);
        NativeSlot* slot = slotFor(handle);
        if (!slot) return FLATSQL_IO_ERR_BADHANDLE;
        fd = slot->fd.exchange(-1, std::memory_order_relaxed);
        auto it = t.openCount.find(slot->path);
        if (it != t.openCount.end() && --it->second <= 0) t.openCount.erase(it);
        slot->path.clear();
        slot->inUse.store(0, std::memory_order_release);
        if (handle < t.nextFree) t.nextFree = handle;
    }
    const int rc = ::close(fd);
    return rc == 0 ? 0 : errnoToStatus(errno);
}

}  // extern "C"

#endif  // !__wasm__
