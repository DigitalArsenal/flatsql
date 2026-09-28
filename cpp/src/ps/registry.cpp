// FlatSQL partition store: registry (see ps/registry.h).
#include "flatsql/ps/registry.h"

#include <cstring>

#include "flatsql/ps/platform.h"

namespace flatsql {
namespace ps {

namespace {
void putStr(std::vector<uint8_t>& out, const std::string& s) {
    const size_t at = out.size();
    out.resize(at + 2 + s.size());
    putU16(out.data() + at, uint16_t(s.size()));
    std::memcpy(out.data() + at + 2, s.data(), s.size());
}
bool getStr(const uint8_t*& p, const uint8_t* end, std::string* s) {
    if (p + 2 > end) return false;
    const uint16_t n = getU16(p);
    if (p + 2 + n > end) return false;
    s->assign(reinterpret_cast<const char*>(p + 2), n);
    p += 2 + n;
    return true;
}
void putU32v(std::vector<uint8_t>& out, uint32_t v) {
    const size_t at = out.size();
    out.resize(at + 4);
    putU32(out.data() + at, v);
}
void putU64v(std::vector<uint8_t>& out, uint64_t v) {
    const size_t at = out.size();
    out.resize(at + 8);
    putU64(out.data() + at, v);
}
constexpr size_t kFrameHdr = 10;  // u32 len, u32 crc, u16 kind
}  // namespace

int32_t Registry::open(IoCtx* io, const std::string& root, bool create, std::string* err) {
    io_ = io;
    root_ = root;
    PathBuf lp, hp;
    pathStore(&lp, root.c_str(), "registry.fsl");
    pathStore(&hp, root.c_str(), "registry.fsh");
    int32_t flags = FLATSQL_IO_READ | FLATSQL_IO_WRITE;
    if (create) flags |= FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS;
    int32_t rc = io->open(lp.c_str(), lp.len, flags, FileClass::Registry, &log_);
    if (rc < 0) {
        if (err) *err = "registry: open registry.fsl failed";
        return rc;
    }
    rc = io->open(hp.c_str(), hp.len, flags, FileClass::Registry, &head_);
    if (rc < 0) {
        if (err) *err = "registry: open registry.fsh failed";
        return rc;
    }
    // Head: both slots, highest valid gen.
    uint8_t slots[2][kHeadSlotBytes];
    RegistryHeadFixed best{};
    bool haveHead = false;
    for (int s = 0; s < 2; s++) {
        const int64_t n = io->read(head_, slots[s], 512, uint64_t(s) * kHeadSlotBytes);
        if (n < int64_t(sizeof(RegistryHeadFixed) + 4)) continue;
        if (!validHeadSlot(slots[s], size_t(n), kHeadRegistry)) continue;
        RegistryHeadFixed h;
        std::memcpy(&h, slots[s], sizeof(h));
        if (!haveHead || h.p.gen > best.p.gen) {
            best = h;
            haveHead = true;
        }
    }
    headGen_ = haveHead ? best.p.gen : 0;
    incarnation_ = haveHead ? best.incarnation : 0;
    // Frames: read the whole log (small) and stop at the first bad frame.
    const int64_t size = io->size(log_);
    if (size < 0) return int32_t(size);
    std::vector<uint8_t> buf(static_cast<size_t>(size));
    if (size > 0 && io->read(log_, buf.data(), buf.size(), 0) != size) {
        if (err) *err = "registry: short read";
        return FLATSQL_IO_ERR_IO;
    }
    uint64_t off = 0;
    frames_ = 0;
    while (off + kFrameHdr <= buf.size()) {
        const uint32_t len = getU32(buf.data() + off);
        if (len < 2 || off + 8 + len > buf.size()) break;
        const uint32_t crc = getU32(buf.data() + off + 4);
        if (crc32c(buf.data() + off + 8, len) != crc) break;
        const uint16_t kind = getU16(buf.data() + off + 8);
        apply(kind, buf.data() + off + 10, len - 2);
        off += 8 + len;
        frames_++;
    }
    fslEnd_ = off;
    reserve_ = off;
    if (off < buf.size()) {
        truncated_ = buf.size() - off;
        rc = io->truncate(log_, off);
        if (rc < 0) return rc;
        rc = io->sync(log_);
        if (rc < 0) return rc;
    }
    return 0;
}

int32_t Registry::beginIncarnation(uint32_t inc) {
    // A10: rewrite and fsync the head before anything starts; the new
    // incarnation numbers every batch this open writes.
    incarnation_ = inc;
    return writeHead(true);
}

void Registry::close() {
    io_->close(&log_);
    io_->close(&head_);
}

void Registry::apply(uint16_t kind, const uint8_t* p, size_t n) {
    const uint8_t* end = p + n;
    switch (kind) {
        case kRegPartitionAdd: {
            PartitionEntry e;
            if (p + 8 > end) return;
            e.pid = getU32(p);
            std::memcpy(e.fid, p + 4, 4);
            p += 8;
            if (!getStr(p, end, &e.token) || !getStr(p, end, &e.sqlName)) return;
            if (p + 8 + 4 + 8 > end) return;
            e.schemaFp = getU64(p);
            e.ordinal = getU32(p + 8);
            e.ctimeMs = int64_t(getU64(p + 12));
            parts_.push_back(e);
            if (e.pid > maxPid_) maxPid_ = e.pid;
            break;
        }
        case kRegTypeAdd: {
            TypeEntry t;
            if (p + 4 > end) return;
            std::memcpy(t.fid, p, 4);
            p += 4;
            if (!getStr(p, end, &t.schemaName)) return;
            if (p + 24 > end) return;
            t.configFp = getU64(p);
            t.indexSpecHash = getU64(p + 8);
            t.supersedeRuleHash = getU64(p + 16);
            // A later TYPE_ADD for the same fid is a schema change.
            for (auto& existing : types_) {
                if (std::memcmp(existing.fid, t.fid, 4) == 0) {
                    existing = t;
                    return;
                }
            }
            types_.push_back(t);
            break;
        }
        case kRegQuarantine:
        case kRegUnquarantine: {
            if (p + 4 > end) return;
            const uint32_t pid = getU32(p);
            for (auto& e : parts_)
                if (e.pid == pid) e.quarantined = (kind == kRegQuarantine);
            break;
        }
        case kRegDrop: {
            if (p + 4 > end) return;
            const uint32_t pid = getU32(p);
            for (auto& e : parts_)
                if (e.pid == pid) e.dropped = true;
            break;
        }
        default:
            break;
    }
}

int32_t Registry::writeFrame(uint16_t kind, const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> frame(kFrameHdr + payload.size());
    putU32(frame.data(), uint32_t(2 + payload.size()));
    putU16(frame.data() + 8, kind);
    if (!payload.empty()) std::memcpy(frame.data() + 10, payload.data(), payload.size());
    putU32(frame.data() + 4, crc32c(frame.data() + 8, 2 + payload.size()));
    uint64_t off;
    {
        std::lock_guard<std::mutex> g(wmu_);
        if (broken_) return FLATSQL_IO_ERR_IO;
        off = reserve_;
        reserve_ += frame.size();
    }
    const int32_t rc = io_->write(log_, frame.data(), frame.size(), off);
    std::unique_lock<std::mutex> lk(wmu_);
    if (rc < 0) {
        broken_ = true;
    } else {
        done_[off] = frame.size();
        for (auto it = done_.begin(); it != done_.end() && it->first == fslEnd_; it = done_.erase(it))
            fslEnd_ += it->second;
    }
    wcv_.notify_all();
    wcv_.wait(lk, [&] { return broken_ || fslEnd_ >= off + frame.size(); });
    return broken_ && fslEnd_ < off + frame.size() ? FLATSQL_IO_ERR_IO : 0;
}

int32_t Registry::syncLog() { return io_->sync(log_); }

int32_t Registry::applyFrame(uint16_t kind, const std::vector<uint8_t>& payload) {
    frames_++;
    apply(kind, payload.data(), payload.size());
    // The head is a hint (open scans past it); written without a sync.
    return writeHead(false);
}

int32_t Registry::appendFrame(uint16_t kind, const std::vector<uint8_t>& payload) {
    int32_t rc = writeFrame(kind, payload);
    if (rc >= 0) rc = syncLog();
    if (rc < 0) return rc;
    return applyFrame(kind, payload);
}

std::vector<uint8_t> Registry::encodePartition(const PartitionEntry& e) {
    std::vector<uint8_t> p;
    putU32v(p, e.pid);
    p.insert(p.end(), e.fid, e.fid + 4);
    putStr(p, e.token);
    putStr(p, e.sqlName);
    putU64v(p, e.schemaFp);
    putU32v(p, e.ordinal);
    putU64v(p, uint64_t(e.ctimeMs));
    return p;
}

std::vector<uint8_t> Registry::encodeType(const TypeEntry& t) {
    std::vector<uint8_t> p;
    p.insert(p.end(), t.fid, t.fid + 4);
    putStr(p, t.schemaName);
    putU64v(p, t.configFp);
    putU64v(p, t.indexSpecHash);
    putU64v(p, t.supersedeRuleHash);
    return p;
}

int32_t Registry::appendPartition(const PartitionEntry& e) { return appendFrame(kRegPartitionAdd, encodePartition(e)); }

int32_t Registry::appendType(const TypeEntry& t) { return appendFrame(kRegTypeAdd, encodeType(t)); }

int32_t Registry::appendQuarantine(uint32_t pid, bool on, const std::string& reason) {
    std::vector<uint8_t> p;
    putU32v(p, pid);
    putStr(p, reason);
    return appendFrame(on ? kRegQuarantine : kRegUnquarantine, p);
}

void Registry::encodeHead(bool durable, HeadSlot* out) {
    static_assert(sizeof(RegistryHeadFixed) + 4 <= sizeof(out->bytes), "registry head slot");
    std::memset(out->bytes, 0, sizeof(RegistryHeadFixed) + 4);
    RegistryHeadFixed h{};
    h.p.magic = kMagicHead;
    h.p.format = kFormat;
    h.p.kind = kHeadRegistry;
    h.p.gen = ++headGen_;
    h.p.flags = durable ? uint32_t(kHeadDurableCkpt) : 0u;
    h.frameCount = frames_;
    {
        std::lock_guard<std::mutex> g(wmu_);
        h.fslEnd = fslEnd_;
    }
    h.maxPid = maxPid_;
    h.incarnation = incarnation_;
    h.nTypes = uint32_t(types_.size());
    h.nPartitions = uint32_t(parts_.size());
    std::memcpy(out->bytes, &h, sizeof(h));
    out->used = uint32_t(sizeof(h) + 4);
    out->gen = h.p.gen;
    sealHeadSlot(out->bytes, out->used);
}

int32_t Registry::writeHeadSlot(const HeadSlot& head) {
    return io_->write(head_, head.bytes, head.used, (head.gen % 2) * kHeadSlotBytes);
}

void Registry::applyFrameDeferHead(uint16_t kind, const std::vector<uint8_t>& payload, HeadSlot* head) {
    frames_++;
    apply(kind, payload.data(), payload.size());
    encodeHead(false, head);
}

int32_t Registry::writeHead(bool sync) {
    HeadSlot slot;
    encodeHead(sync, &slot);
    int32_t rc = writeHeadSlot(slot);
    if (rc < 0) return rc;
    if (sync) {
        rc = io_->sync(head_);
        if (rc < 0) return rc;
    }
    return 0;
}

}  // namespace ps
}  // namespace flatsql
