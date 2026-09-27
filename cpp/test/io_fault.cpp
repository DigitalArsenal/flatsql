// Fault-injecting in-memory host I/O (see test/ps/io_fault.h).
#include "ps/io_fault.h"

#include <algorithm>
#include <cstring>
#include <random>

#include "flatsql/ps/platform.h"

namespace flatsql {
namespace ps {
namespace test {

namespace {
std::string parentOf(const std::string& p) {
    const size_t s = p.find_last_of('/');
    return s == std::string::npos ? std::string(".") : p.substr(0, s);
}
void applyOp(std::vector<uint8_t>& img, const std::vector<uint8_t>& data, uint64_t off, uint8_t kind,
             uint64_t size, size_t limit) {
    if (kind == 1) {
        img.resize(size_t(size));
        return;
    }
    const size_t n = std::min(limit, data.size());
    if (off + n > img.size()) img.resize(size_t(off + n), 0);
    if (n) std::memcpy(img.data() + off, data.data(), n);
}
}  // namespace

bool FaultFs::countOp() {
    const uint64_t n = ops_.fetch_add(1) + 1;
    const uint64_t at = crashAt_.load();
    if (at && n >= at) frozen_.store(true);
    return !frozen_.load();
}

FaultFs::File* FaultFs::lookupHandle(int32_t h, std::string* path, bool* isDir) {
    if (h < 0 || size_t(h) >= highWater_.load(std::memory_order_acquire)) return nullptr;
    Handle& hd = handles_[h];
    if (!hd.inUse.load(std::memory_order_acquire)) return nullptr;
    if (path) *path = hd.path;
    if (isDir) *isDir = hd.isDir;
    return hd.file.load(std::memory_order_acquire);
}

int32_t FaultFs::allocHandle(File* f, const std::string& path, bool isDir) {
    // mu_ held by the caller.
    const size_t hw = highWater_.load(std::memory_order_relaxed);
    for (size_t k = 0; k < hw; k++) {
        const size_t i = (nextFree_ + k) % (hw ? hw : 1);
        Handle& h = handles_[i];
        if (!h.inUse.load(std::memory_order_relaxed)) {
            h.file.store(f, std::memory_order_relaxed);
            h.path = path;
            h.isDir = isDir;
            h.inUse.store(true, std::memory_order_release);
            nextFree_ = i + 1;
            return int32_t(i);
        }
    }
    if (hw >= kMaxHandles) return FLATSQL_IO_ERR_GENERIC;
    Handle& h = handles_[hw];
    h.file.store(f, std::memory_order_relaxed);
    h.path = path;
    h.isDir = isDir;
    h.inUse.store(true, std::memory_order_release);
    highWater_.store(hw + 1, std::memory_order_release);
    nextFree_ = hw + 1;
    return int32_t(hw);
}

int32_t FaultFs::open(const char* p, int32_t pathLen, int32_t flags) {
    const std::string path(p, size_t(pathLen));
    if (flags & FLATSQL_IO_PROBE) {
        std::lock_guard<std::mutex> g(mu_);
        return ns_.count(path) ? 0 : FLATSQL_IO_ERR_NOENT;
    }
    if (flags & FLATSQL_IO_UNLINK) {
        if (!countOp()) return FLATSQL_IO_ERR_IO;
        std::lock_guard<std::mutex> g(mu_);
        auto it = ns_.find(path);
        if (it == ns_.end()) return FLATSQL_IO_ERR_NOENT;
        if (flags & FLATSQL_IO_UNLINK_IF_UNUSED) {
            for (size_t i = 0; i < highWater_.load(); i++)
                if (handles_[i].inUse.load() && handles_[i].path == path) return FLATSQL_IO_ERR_BUSY;
            ns_.erase(it);  // durable (the host fsyncs the parent)
            return 0;
        }
        if (it->second.durableEntry) unlinked_[path] = it->second.file;
        ns_.erase(it);
        return 0;
    }
    std::lock_guard<std::mutex> g(mu_);
    if (flags & FLATSQL_IO_DIRECTORY) return allocHandle(nullptr, path, true);
    auto it = ns_.find(path);
    std::shared_ptr<File> file;
    if (it == ns_.end()) {
        if (!(flags & FLATSQL_IO_CREATE)) return FLATSQL_IO_ERR_NOENT;
        if (frozen_.load()) return FLATSQL_IO_ERR_IO;
        ops_.fetch_add(1);
        file = std::make_shared<File>();
        keepAlive_.push_back(file);
        Entry e;
        e.file = file;
        e.durableEntry = (flags & FLATSQL_IO_CREATE_PARENTS) != 0 || !tracking_;
        ns_[path] = e;
        unlinked_.erase(path);
    } else {
        if ((flags & FLATSQL_IO_CREATE) && (flags & FLATSQL_IO_EXCL)) return FLATSQL_IO_ERR_EXIST;
        file = it->second.file;
        if (flags & FLATSQL_IO_TRUNC) {
            if (frozen_.load()) return FLATSQL_IO_ERR_IO;
            ops_.fetch_add(1);
            std::lock_guard<std::mutex> fg(file->mu);
            file->cur.clear();
            if (tracking_) {
                Op op;
                op.kind = 1;
                op.seq = seq_.fetch_add(1);
                op.off = 0;
                op.size = 0;
                file->pending.push_back(std::move(op));
            }
        }
    }
    return allocHandle(file.get(), path, false);
}

int32_t FaultFs::read(int32_t handle, void* dst, int32_t len, double offset) {
    bool isDir = false;
    File* f = lookupHandle(handle, nullptr, &isDir);
    if (!f || isDir) return FLATSQL_IO_ERR_BADHANDLE;
    std::lock_guard<std::mutex> g(f->mu);
    const uint64_t off = uint64_t(offset);
    if (off >= f->cur.size()) return 0;
    const size_t n = std::min<size_t>(size_t(len), f->cur.size() - size_t(off));
    std::memcpy(dst, f->cur.data() + off, n);
    return int32_t(n);
}

int32_t FaultFs::write(int32_t handle, const void* src, int32_t len, double offset) {
    std::string path;
    bool isDir = false;
    File* f = lookupHandle(handle, &path, &isDir);
    if (!f || isDir) return FLATSQL_IO_ERR_BADHANDLE;
    if (!countOp()) return FLATSQL_IO_ERR_IO;
    if (failWriteCount_.load() > 0 && path.find(failWriteNeedle_) != std::string::npos) {
        if (failWriteCount_.fetch_sub(1) > 0) return failWriteCode_;
    }
    const uint64_t off = uint64_t(offset);
    std::lock_guard<std::mutex> g(f->mu);
    if (off + size_t(len) > f->cur.size()) f->cur.resize(size_t(off) + size_t(len), 0);
    std::memcpy(f->cur.data() + off, src, size_t(len));
    if (tracking_) {
        Op op;
        op.kind = 0;
        op.seq = seq_.fetch_add(1);
        op.off = off;
        op.size = 0;
        op.data.assign(static_cast<const uint8_t*>(src), static_cast<const uint8_t*>(src) + len);
        f->pending.push_back(std::move(op));
    }
    return len;
}

int32_t FaultFs::truncate(int32_t handle, double size) {
    bool isDir = false;
    File* f = lookupHandle(handle, nullptr, &isDir);
    if (!f || isDir) return FLATSQL_IO_ERR_BADHANDLE;
    if (!countOp()) return FLATSQL_IO_ERR_IO;
    std::lock_guard<std::mutex> g(f->mu);
    f->cur.resize(size_t(size), 0);
    if (tracking_) {
        Op op;
        op.kind = 1;
        op.seq = seq_.fetch_add(1);
        op.off = 0;
        op.size = uint64_t(size);
        f->pending.push_back(std::move(op));
    }
    return 0;
}

int32_t FaultFs::sync(int32_t handle) {
    std::string path;
    bool isDir = false;
    File* f = lookupHandle(handle, &path, &isDir);
    if (!f && !isDir) return FLATSQL_IO_ERR_BADHANDLE;
    if (!countOp()) return FLATSQL_IO_ERR_IO;
    syncs_.fetch_add(1);
    if (failSyncCount_.load() > 0 && path.find(failSyncNeedle_) != std::string::npos) {
        if (failSyncCount_.fetch_sub(1) > 0) return FLATSQL_IO_ERR_IO;
    }
    if (syncLatencyNs_) sleepNs(syncLatencyNs_);
    if (isDir) {
        std::lock_guard<std::mutex> g(mu_);
        const std::string dir = path;
        for (auto& kv : ns_)
            if (parentOf(kv.first) == dir) kv.second.durableEntry = true;
        for (auto it = unlinked_.begin(); it != unlinked_.end();)
            if (parentOf(it->first) == dir) it = unlinked_.erase(it);
            else ++it;
        return 0;
    }
    if (!tracking_) return 0;
    std::lock_guard<std::mutex> g(f->mu);
    f->durable = f->cur;
    f->pending.clear();
    return 0;
}

double FaultFs::size(int32_t handle) {
    bool isDir = false;
    File* f = lookupHandle(handle, nullptr, &isDir);
    if (!f || isDir) return FLATSQL_IO_ERR_BADHANDLE;
    std::lock_guard<std::mutex> g(f->mu);
    return double(f->cur.size());
}

int32_t FaultFs::close(int32_t handle) {
    std::lock_guard<std::mutex> g(mu_);
    if (handle < 0 || size_t(handle) >= highWater_.load() || !handles_[handle].inUse.load())
        return FLATSQL_IO_ERR_BADHANDLE;
    handles_[handle].inUse.store(false, std::memory_order_release);
    handles_[handle].path.clear();
    if (size_t(handle) < nextFree_) nextFree_ = size_t(handle);
    return 0;
}

void FaultFs::crash(CrashMode mode, uint64_t seed) {
    std::lock_guard<std::mutex> g(mu_);
    std::mt19937_64 rng(seed);
    // Globally last pending write (kTearLast).
    uint64_t lastSeq = 0;
    File* lastFile = nullptr;
    for (auto& kv : ns_) {
        File* f = kv.second.file.get();
        for (const auto& op : f->pending)
            if (op.kind == 0 && op.seq >= lastSeq) {
                lastSeq = op.seq;
                lastFile = f;
            }
    }
    std::vector<File*> seen;
    auto crashFile = [&](File* f) {
        if (std::find(seen.begin(), seen.end(), f) != seen.end()) return;
        seen.push_back(f);
        std::lock_guard<std::mutex> fg(f->mu);
        std::vector<uint8_t> img = f->durable;
        switch (mode) {
            case kDropAll:
                break;
            case kKeepAll:
                img = f->cur;
                break;
            case kDropSubset:
                for (const auto& op : f->pending)
                    if (rng() & 1) applyOp(img, op.data, op.off, op.kind, op.size, op.data.size());
                break;
            case kTearLast:
                for (const auto& op : f->pending) {
                    if (f == lastFile && op.seq == lastSeq && op.kind == 0) {
                        const size_t blocks = op.data.size() / 512;
                        const size_t keep = blocks ? size_t(rng() % blocks) * 512 : 0;
                        applyOp(img, op.data, op.off, 0, 0, keep);
                    } else {
                        applyOp(img, op.data, op.off, op.kind, op.size, op.data.size());
                    }
                }
                break;
            case kReorder: {
                std::vector<const Op*> ops;
                for (const auto& op : f->pending)
                    if (rng() & 1) ops.push_back(&op);
                std::shuffle(ops.begin(), ops.end(), rng);
                for (const Op* op : ops) applyOp(img, op->data, op->off, op->kind, op->size, op->data.size());
                break;
            }
            default:
                break;
        }
        f->durable = img;
        f->cur = img;
        f->pending.clear();
    };
    for (auto& kv : ns_) crashFile(kv.second.file.get());
    for (auto& kv : unlinked_) crashFile(kv.second.get());
    // Directory entries.
    if (mode != kKeepAll) {
        for (auto it = ns_.begin(); it != ns_.end();) {
            if (!it->second.durableEntry) it = ns_.erase(it);
            else ++it;
        }
        for (auto& kv : unlinked_) {
            Entry e;
            e.file = kv.second;
            e.durableEntry = true;
            ns_[kv.first] = e;
        }
    }
    for (auto& kv : ns_) kv.second.durableEntry = true;
    unlinked_.clear();
    for (size_t i = 0; i < highWater_.load(); i++) {
        handles_[i].inUse.store(false);
        handles_[i].path.clear();
    }
    nextFree_ = 0;
    frozen_.store(false);
    crashAt_.store(0);
    failSyncCount_.store(0);
    failWriteCount_.store(0);
}

void FaultFs::failSyncs(const std::string& needle, int count) {
    failSyncNeedle_ = needle;
    failSyncCount_.store(count);
}

void FaultFs::failWrites(const std::string& needle, int count, int32_t code) {
    failWriteNeedle_ = needle;
    failWriteCode_ = code;
    failWriteCount_.store(count);
}

bool FaultFs::exists(const std::string& path) {
    std::lock_guard<std::mutex> g(mu_);
    return ns_.count(path) != 0;
}

std::vector<uint8_t> FaultFs::contents(const std::string& path) {
    std::shared_ptr<File> f;
    {
        std::lock_guard<std::mutex> g(mu_);
        auto it = ns_.find(path);
        if (it == ns_.end()) return {};
        f = it->second.file;
    }
    std::lock_guard<std::mutex> g(f->mu);
    return f->cur;
}

std::vector<std::string> FaultFs::list(const std::string& prefix) {
    std::lock_guard<std::mutex> g(mu_);
    std::vector<std::string> out;
    for (auto& kv : ns_)
        if (kv.first.compare(0, prefix.size(), prefix) == 0) out.push_back(kv.first);
    return out;
}

uint64_t FaultFs::totalBytes(const std::string& prefix) {
    std::vector<std::shared_ptr<File>> files;
    {
        std::lock_guard<std::mutex> g(mu_);
        for (auto& kv : ns_)
            if (kv.first.compare(0, prefix.size(), prefix) == 0) files.push_back(kv.second.file);
    }
    uint64_t total = 0;
    for (auto& f : files) {
        std::lock_guard<std::mutex> g(f->mu);
        total += f->cur.size();
    }
    return total;
}

uint64_t FaultFs::openHandles() {
    std::lock_guard<std::mutex> g(mu_);
    uint64_t n = 0;
    for (size_t i = 0; i < highWater_.load(); i++)
        if (handles_[i].inUse.load()) n++;
    return n;
}

}  // namespace test
}  // namespace ps
}  // namespace flatsql
