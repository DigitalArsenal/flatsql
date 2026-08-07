// Native satisfaction of the seven-import host I/O contract.
//
// On wasm these seven symbols are IMPORTS (see flatsql_io.h) and the host
// supplies them. Natively there is no host, so this file supplies them over
// POSIX pread/pwrite — which is what makes the VFS testable in CTest.
//
// This is not a test double. It is a third lane, held to the same contract as
// the browser shim and the Go host, and the native VFS test that runs against
// it is the cheapest parity evidence we have: a divergence in the VFS itself
// (short reads, zero-fill, truncate, delete-on-close, journal recovery) fails
// the build long before anyone instantiates a wasm module.

#if !defined(__wasm__)

#include "flatsql/flatsql_io.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

struct NativeSlot {
    int fd = -1;
    std::string path;
    bool inUse = false;
};

std::mutex& slotMutex() {
    static std::mutex m;
    return m;
}

std::vector<NativeSlot>& slots() {
    static std::vector<NativeSlot> s;
    return s;
}

NativeSlot* slotFor(int32_t handle) {
    auto& table = slots();
    if (handle < 0 || static_cast<size_t>(handle) >= table.size()) return nullptr;
    NativeSlot* slot = &table[static_cast<size_t>(handle)];
    return slot->inUse ? slot : nullptr;
}

int32_t errnoToStatus() {
    switch (errno) {
        case ENOENT: return FLATSQL_IO_ERR_NOENT;
        case EACCES:
        case EPERM:  return FLATSQL_IO_ERR_ACCESS;
        case ENOSPC:
        case EDQUOT: return FLATSQL_IO_ERR_NOSPACE;
        case EIO:    return FLATSQL_IO_ERR_IO;
        default:     return FLATSQL_IO_ERR_GENERIC;
    }
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
        if (::unlink(name.c_str()) == 0) return 0;
        return errnoToStatus();
    }

    int oflags = 0;
    if ((flags & FLATSQL_IO_WRITE) && (flags & FLATSQL_IO_READ)) oflags |= O_RDWR;
    else if (flags & FLATSQL_IO_WRITE)                           oflags |= O_WRONLY;
    else                                                          oflags |= O_RDONLY;
    if (flags & FLATSQL_IO_CREATE) oflags |= O_CREAT;
    if (flags & FLATSQL_IO_EXCL)   oflags |= O_EXCL;
    if (flags & FLATSQL_IO_TRUNC)  oflags |= O_TRUNC;

    const int fd = ::open(name.c_str(), oflags, 0644);
    if (fd < 0) return errnoToStatus();

    std::lock_guard<std::mutex> guard(slotMutex());
    auto& table = slots();
    for (size_t i = 0; i < table.size(); i++) {
        if (!table[i].inUse) {
            table[i] = NativeSlot{fd, name, true};
            return static_cast<int32_t>(i);
        }
    }
    table.push_back(NativeSlot{fd, name, true});
    return static_cast<int32_t>(table.size() - 1);
}

int32_t flatsql_io_read(int32_t handle, void* dst, int32_t len, double offset) {
    if (!dst || len < 0) return FLATSQL_IO_ERR_GENERIC;
    std::lock_guard<std::mutex> guard(slotMutex());
    NativeSlot* slot = slotFor(handle);
    if (!slot) return FLATSQL_IO_ERR_BADHANDLE;
    const ssize_t n = ::pread(slot->fd, dst, static_cast<size_t>(len),
                              static_cast<off_t>(offset));
    if (n < 0) return errnoToStatus();
    return static_cast<int32_t>(n);
}

int32_t flatsql_io_write(int32_t handle, const void* src, int32_t len,
                         double offset) {
    if (!src || len < 0) return FLATSQL_IO_ERR_GENERIC;
    std::lock_guard<std::mutex> guard(slotMutex());
    NativeSlot* slot = slotFor(handle);
    if (!slot) return FLATSQL_IO_ERR_BADHANDLE;
    const ssize_t n = ::pwrite(slot->fd, src, static_cast<size_t>(len),
                               static_cast<off_t>(offset));
    if (n < 0) return errnoToStatus();
    return static_cast<int32_t>(n);
}

int32_t flatsql_io_truncate(int32_t handle, double size) {
    std::lock_guard<std::mutex> guard(slotMutex());
    NativeSlot* slot = slotFor(handle);
    if (!slot) return FLATSQL_IO_ERR_BADHANDLE;
    if (::ftruncate(slot->fd, static_cast<off_t>(size)) != 0) return errnoToStatus();
    return 0;
}

int32_t flatsql_io_sync(int32_t handle) {
    std::lock_guard<std::mutex> guard(slotMutex());
    NativeSlot* slot = slotFor(handle);
    if (!slot) return FLATSQL_IO_ERR_BADHANDLE;
#if defined(__APPLE__)
    if (::fsync(slot->fd) != 0) return errnoToStatus();
#else
    if (::fdatasync(slot->fd) != 0) return errnoToStatus();
#endif
    return 0;
}

double flatsql_io_size(int32_t handle) {
    std::lock_guard<std::mutex> guard(slotMutex());
    NativeSlot* slot = slotFor(handle);
    if (!slot) return static_cast<double>(FLATSQL_IO_ERR_BADHANDLE);
    struct stat st;
    if (::fstat(slot->fd, &st) != 0) return static_cast<double>(errnoToStatus());
    return static_cast<double>(st.st_size);
}

int32_t flatsql_io_close(int32_t handle) {
    std::lock_guard<std::mutex> guard(slotMutex());
    NativeSlot* slot = slotFor(handle);
    if (!slot) return FLATSQL_IO_ERR_BADHANDLE;
    const int rc = ::close(slot->fd);
    slot->inUse = false;
    slot->fd = -1;
    slot->path.clear();
    return rc == 0 ? 0 : errnoToStatus();
}

}  // extern "C"

#endif  // !__wasm__
