// FlatSQL partition store: L1 runs (see ps/index.h). Built and loaded only
// on maintenance paths (merge, seal, partition activation), never per record.
#include "flatsql/ps/index.h"
#include "flatsql/ps/platform.h"

namespace flatsql {
namespace ps {

namespace {
constexpr size_t kFlushBytes = size_t(1) << 20;
}

int prefixCmp(const uint8_t* key, size_t klen, const L1Fence& f) {
    const size_t kp = klen < kFencePrefix ? klen : kFencePrefix;
    return keyCmp(key, kp, f.prefix, f.prefixLen);
}

bool l1BlockValid(const uint8_t* block) {
    const uint16_t used = getU16(block + 2);
    if (size_t(used) + 4 + 4 > kL1BlockBytes) return false;
    return crc32c(block, kL1BlockBytes - 4) == getU32(block + kL1BlockBytes - 4);
}

L1Writer::L1Writer(IoCtx* io, FileRef out, uint32_t seg, uint32_t gen, uint16_t level,
                   uint64_t firstPseq, uint64_t lastPseq)
    : io_(io), out_(out) {
    hdr_.magic = kMagicL1;
    hdr_.ver = 1;
    hdr_.seg = seg;
    hdr_.gen = gen;
    hdr_.level = level;
    hdr_.firstPseq = firstPseq;
    hdr_.lastPseq = lastPseq;
    buf_.reserve(kFlushBytes + kL1BlockBytes);
    buf_.resize(sizeof(L1Header), 0);
    block_.resize(kL1BlockBytes, 0);
}

int32_t L1Writer::beginKind(uint16_t kind, uint64_t expectedKeys) {
    if (err_) return err_;
    kinds_.emplace_back();
    cur_ = &kinds_.back();
    cur_->toc.kind = kind;
    cur_->toc.keyType = keyTypeOf(kind);
    cur_->toc.vlen = valueLenOf(kind);
    if (isLookupKind(kind)) {
        const uint32_t n = expectedKeys > 0xffffffffull ? 0xffffffffu : uint32_t(expectedKeys);
        cur_->bloom.assign(bloomBytesFor(n), 0);
    }
    blockN_ = 0;
    blockLive_ = 0;
    return 0;
}

int32_t L1Writer::add(const uint8_t* key, uint16_t klen, const uint8_t* val, bool live) {
    if (err_) return err_;
    const uint8_t vlen = cur_->toc.vlen;
    const size_t need = 2 + size_t(klen) + vlen;
    uint16_t used = blockN_ ? getU16(block_.data() + 2) : 0;
    if (blockN_ && 4 + size_t(used) + need + 4 > kL1BlockBytes) {
        const int32_t rc = flushBlock();
        if (rc) return rc;
        used = 0;
    }
    if (4 + need + 4 > kL1BlockBytes) return err_ = FLATSQL_IO_ERR_GENERIC;
    if (blockN_ == 0) {
        firstKeyLen_ = uint8_t(klen < kFencePrefix ? klen : kFencePrefix);
        std::memcpy(firstKey_, key, firstKeyLen_);
    }
    uint8_t* p = block_.data() + 4 + used;
    putU16(p, klen);
    std::memcpy(p + 2, key, klen);
    std::memcpy(p + 2 + klen, val, vlen);
    blockN_++;
    if (live) blockLive_++;
    putU16(block_.data(), uint16_t(blockN_));
    putU16(block_.data() + 2, uint16_t(used + need));
    if (!cur_->bloom.empty()) bloomAdd(cur_->bloom.data(), cur_->bloom.size(), key, klen);
    cur_->toc.nEntries++;
    total_++;
    return 0;
}

int32_t L1Writer::flushBlock() {
    if (blockN_ == 0) return 0;
    const uint16_t used = getU16(block_.data() + 2);
    std::memset(block_.data() + 4 + used, 0, kL1BlockBytes - 4 - used);
    putU32(block_.data() + kL1BlockBytes - 4, crc32c(block_.data(), kL1BlockBytes - 4));
    L1Fence f{};
    f.blockOff = off_ + buf_.size();
    f.n = blockN_;
    f.live = blockLive_;
    f.prefixLen = firstKeyLen_;
    std::memcpy(f.prefix, firstKey_, firstKeyLen_);
    cur_->fences.push_back(f);
    buf_.insert(buf_.end(), block_.begin(), block_.end());
    blockN_ = 0;
    blockLive_ = 0;
    std::memset(block_.data(), 0, 4);
    if (buf_.size() >= kFlushBytes) return flushBuf();
    return 0;
}

int32_t L1Writer::flushBuf() {
    if (buf_.empty()) return 0;
    const int32_t rc = io_->write(out_, buf_.data(), buf_.size(), off_);
    if (rc < 0) return err_ = rc;
    off_ += buf_.size();
    buf_.clear();
    return 0;
}

int32_t L1Writer::endKind() {
    if (err_) return err_;
    const int32_t rc = flushBlock();
    if (rc) return rc;
    cur_->toc.nBlocks = uint32_t(cur_->fences.size());
    cur_ = nullptr;
    return 0;
}

int64_t L1Writer::finish() {
    if (err_) return err_;
    const uint64_t metaOff = off_ + buf_.size();
    const size_t metaStart = buf_.size();
    for (auto& k : kinds_) {
        k.toc.fenceOff = off_ + buf_.size();
        const uint8_t* fp = reinterpret_cast<const uint8_t*>(k.fences.data());
        buf_.insert(buf_.end(), fp, fp + k.fences.size() * sizeof(L1Fence));
        k.toc.bloomOff = off_ + buf_.size();
        k.toc.bloomBytes = uint32_t(k.bloom.size());
        buf_.insert(buf_.end(), k.bloom.begin(), k.bloom.end());
        while (buf_.size() & 7) buf_.push_back(0);
    }
    const uint64_t tocOff = off_ + buf_.size();
    for (auto& k : kinds_) {
        const uint8_t* tp = reinterpret_cast<const uint8_t*>(&k.toc);
        buf_.insert(buf_.end(), tp, tp + sizeof(L1TocEntry));
    }
    const uint32_t tocLen = uint32_t(kinds_.size() * sizeof(L1TocEntry));
    L1Footer ft{};
    ft.tocOff = tocOff;
    ft.tocLen = tocLen;
    ft.tocCrc = crc32c(buf_.data() + metaStart, buf_.size() - metaStart);
    ft.metaOff = metaOff;
    ft.magic = kMagicL1Footer;
    ft.crc = crc32c(&ft, 28);
    const uint8_t* fp = reinterpret_cast<const uint8_t*>(&ft);
    buf_.insert(buf_.end(), fp, fp + sizeof(ft));
    // Header lives at offset 0: patch it in place when still buffered.
    hdr_.nKinds = uint16_t(kinds_.size());
    hdr_.nEntries = total_;
    if (off_ == 0) {
        std::memcpy(buf_.data(), &hdr_, sizeof(hdr_));
        const int32_t rc = flushBuf();
        if (rc) return rc;
    } else {
        int32_t rc = flushBuf();
        if (rc) return rc;
        rc = io_->write(out_, &hdr_, sizeof(hdr_), 0);
        if (rc < 0) return err_ = rc;
    }
    return int64_t(off_);
}

int32_t L1Run::load(IoCtx* io, const FileRef& f, uint64_t fileLen) {
    kinds_.clear();
    if (fileLen < sizeof(L1Header) + sizeof(L1Footer)) return -1;
    L1Header h;
    if (io->read(f, &h, sizeof(h), 0) != int64_t(sizeof(h))) return -1;
    if (h.magic != kMagicL1) return -1;
    L1Footer ft;
    if (io->read(f, &ft, sizeof(ft), fileLen - sizeof(ft)) != int64_t(sizeof(ft))) return -1;
    if (ft.magic != kMagicL1Footer || ft.crc != crc32c(&ft, 28)) return -1;
    if (ft.metaOff > ft.tocOff || ft.tocOff + ft.tocLen + sizeof(ft) != fileLen) return -1;
    std::vector<uint8_t> meta(size_t(ft.tocOff + ft.tocLen - ft.metaOff));
    if (io->read(f, meta.data(), meta.size(), ft.metaOff) != int64_t(meta.size())) return -1;
    if (crc32c(meta.data(), meta.size()) != ft.tocCrc) return -1;
    const size_t nk = ft.tocLen / sizeof(L1TocEntry);
    if (nk != h.nKinds) return -1;
    kinds_.resize(nk);
    for (size_t i = 0; i < nk; i++) {
        KindView& k = kinds_[i];
        std::memcpy(&k.toc, meta.data() + (ft.tocOff - ft.metaOff) + i * sizeof(L1TocEntry),
                    sizeof(L1TocEntry));
        if (k.toc.fenceOff < ft.metaOff || k.toc.bloomOff < ft.metaOff) return -1;
        const size_t fo = size_t(k.toc.fenceOff - ft.metaOff);
        const size_t fb = size_t(k.toc.nBlocks) * sizeof(L1Fence);
        const size_t bo = size_t(k.toc.bloomOff - ft.metaOff);
        if (fo + fb > meta.size() || bo + k.toc.bloomBytes > meta.size()) return -1;
        k.fences.resize(k.toc.nBlocks);
        std::memcpy(k.fences.data(), meta.data() + fo, fb);
        k.bloom.assign(meta.data() + bo, meta.data() + bo + k.toc.bloomBytes);
    }
    nEntries_ = h.nEntries;
    seg_ = h.seg;
    gen_ = h.gen;
    level_ = h.level;
    return 0;
}

const L1Run::KindView* L1Run::findKind(uint16_t kind) const {
    for (const auto& k : kinds_)
        if (k.toc.kind == kind) return &k;
    return nullptr;
}

bool L1Run::mayContain(uint16_t kind, const uint8_t* key, size_t klen) const {
    const KindView* k = findKind(kind);
    if (!k || k->fences.empty()) return false;
    if (k->bloom.empty()) return true;
    return bloomTest(k->bloom.data(), k->bloom.size(), key, klen);
}

size_t L1Run::firstCandidate(const KindView& k, const uint8_t* key, size_t klen) const {
    // Last block whose first-key prefix is strictly below the key's prefix.
    size_t lo = 0, hi = k.fences.size();
    while (lo < hi) {
        const size_t mid = (lo + hi) / 2;
        if (prefixCmp(key, klen, k.fences[mid]) > 0) lo = mid + 1;
        else hi = mid;
    }
    return lo == 0 ? 0 : lo - 1;
}

uint64_t L1Run::memoryBytes() const {
    uint64_t b = 0;
    for (const auto& k : kinds_) b += k.fences.size() * sizeof(L1Fence) + k.bloom.size();
    return b;
}

}  // namespace ps
}  // namespace flatsql
