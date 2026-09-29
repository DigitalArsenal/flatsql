// FlatSQL partition store: compaction (design §11, A11, A12 as amended; T3).
//
// A compaction rewrites a run of sealed, fully merged segments [seg, segEnd]
// into ONE new file set named by an INTENT_COMPACT committed first:
//
//   c-<seg>-<gen>.fsd  the surviving frames, verbatim
//   c-<seg>-<gen>.fsa  their attributes
//   c-<seg>-<gen>.fsr  the surviving rows (pseqs preserved, offsets rewritten)
//                      plus a presence directory; a missing pseq reads VOID
//   x-<seg>-<gen>.fsx  the inputs' L1 runs minus the removed rows' postings
//   mf-<gen>.fsm       the manifest naming them, written at SWAP time
//
// Pipeline (owner = the partition's writer; builder = a compaction thread):
//   1. owner plans: candidate, inputs, kill bound; INTENT_COMPACT rides the
//      next batch (A11: no output exists before its intent is durable);
//   2. builder: dead set from DEAD / TAG_DEAD postings (killer <= bound),
//      rows, frames and attributes copied in 4 MiB slices, filtered run,
//      outputs fsynced; the owner word is re-checked before every mutation
//      (A26) and the builder stops at the next slice when told to;
//   3. owner, once no merge is in flight: the manifest (its sync joins the
//      next round's first phase), SWAP{seg, gen, oldCgen, segEnd} and the
//      RETIRE set (A12) ride one batch; publish attaches the new files.
//
// What a compaction removes (kill bound = the type owner's labeled_through,
// so every removed row's death is labeled; a type-level reader whose bound is
// below a segment's killThrough reads the segment's previous file set through
// prevGen, which the reader gate keeps on disk for it):
//   - PUT, LICENCE, CTL rows with a DEAD posting whose killer <= bound;
//   - RETAG rows whose instance is TAG_DEAD (killer <= bound) or whose PUT is
//     dead in the inputs or already removed;
//   - TOMB, CTL_TOMB, TAG_TOMB rows whose target was removed by an EARLIER
//     SWAP (minor 1); the output's killThrough covers that SWAP's bound.
// pseqs never move and never repeat; a removed row's postings go with it.
#include <algorithm>
#include <cstring>

#include "internal.h"

namespace flatsql {
namespace ps {

// ---------------------------------------------------------------------------
// Formats (ps/compaction.h)
// ---------------------------------------------------------------------------
std::vector<uint8_t> encodeManifest(const ManifestDesc& m) {
    std::vector<uint8_t> out(sizeof(ManifestHeader));
    ManifestHeader h{};
    h.magic = kMagicManifest;
    h.ver = kManifestVer;
    h.nSegs = uint16_t(m.segs.size());
    h.gen = m.gen;
    h.pid = m.pid;
    h.prevGen = m.prevGen;
    std::memcpy(out.data(), &h, sizeof(h));
    for (const auto& s : m.segs) {
        ManifestSegV2 ms{};
        ms.seg = s.seg;
        ms.flags = (s.sealed ? kMsegSealed : 0u) | (s.empty ? kMsegEmpty : 0u);
        ms.firstPseq = s.firstPseq;
        ms.endPseq = s.endPseq;
        ms.mergedEnd = s.mergedEnd;
        ms.dLen = s.dLen;
        ms.rLen = s.rLen;
        ms.aLen = s.aLen;
        ms.minEpoch = s.minEpoch;
        ms.maxEpoch = s.maxEpoch;
        ms.nRuns = uint32_t(s.runs.size());
        ms.cgen = s.cgen;
        ms.killThrough = s.killThrough;
        ms.prevGen = s.prevGen;
        ms.lastSeg = s.lastSeg ? s.lastSeg : s.seg;
        ms.minArrival = s.minArrival;
        ms.maxArrival = s.maxArrival;
        const uint8_t* b = reinterpret_cast<const uint8_t*>(&ms);
        out.insert(out.end(), b, b + sizeof(ms));
        for (const auto& r : s.runs) {
            const uint8_t* rb = reinterpret_cast<const uint8_t*>(&r);
            out.insert(out.end(), rb, rb + sizeof(r));
        }
    }
    const uint32_t crc = crc32c(out.data(), out.size());
    const size_t at = out.size();
    out.resize(at + 8, 0);
    putU32(out.data() + at, crc);
    return out;
}

bool decodeManifest(const uint8_t* buf, size_t len, ManifestDesc* out) {
    *out = ManifestDesc();
    if (len < sizeof(ManifestHeader) + 8) return false;
    const size_t body = len - 8;
    if (crc32c(buf, body) != getU32(buf + body)) return false;
    ManifestHeader h;
    std::memcpy(&h, buf, sizeof(h));
    if (h.magic != kMagicManifest || (h.ver != 1 && h.ver != 2)) return false;
    out->gen = h.gen;
    out->pid = h.pid;
    out->prevGen = h.ver >= 2 ? h.prevGen : 0;
    size_t off = sizeof(h);
    for (uint16_t i = 0; i < h.nSegs; i++) {
        ManifestSegDesc d;
        uint32_t nRuns = 0;
        if (h.ver == 1) {
            ManifestSegV1 ms;
            if (off + sizeof(ms) > body) return false;
            std::memcpy(&ms, buf + off, sizeof(ms));
            off += sizeof(ms);
            d.seg = d.lastSeg = ms.seg;
            d.sealed = ms.flags & kMsegSealed;
            d.firstPseq = ms.firstPseq;
            d.endPseq = ms.endPseq;
            d.mergedEnd = ms.mergedEnd;
            d.dLen = ms.dLen;
            d.rLen = ms.rLen;
            d.aLen = ms.aLen;
            d.minEpoch = ms.minEpoch;
            d.maxEpoch = ms.maxEpoch;
            d.cgen = ms.cgen;
            nRuns = ms.nRuns;
        } else {
            ManifestSegV2 ms;
            if (off + sizeof(ms) > body) return false;
            std::memcpy(&ms, buf + off, sizeof(ms));
            off += sizeof(ms);
            d.seg = ms.seg;
            d.lastSeg = ms.lastSeg ? ms.lastSeg : ms.seg;
            d.sealed = ms.flags & kMsegSealed;
            d.empty = ms.flags & kMsegEmpty;
            d.firstPseq = ms.firstPseq;
            d.endPseq = ms.endPseq;
            d.mergedEnd = ms.mergedEnd;
            d.dLen = ms.dLen;
            d.rLen = ms.rLen;
            d.aLen = ms.aLen;
            d.minEpoch = ms.minEpoch;
            d.maxEpoch = ms.maxEpoch;
            d.cgen = ms.cgen;
            d.killThrough = ms.killThrough;
            d.prevGen = ms.prevGen;
            d.minArrival = ms.minArrival;
            d.maxArrival = ms.maxArrival;
            nRuns = ms.nRuns;
        }
        if (nRuns > 4096) return false;
        for (uint32_t r = 0; r < nRuns; r++) {
            ManifestRun mr;
            if (off + sizeof(mr) > body) return false;
            std::memcpy(&mr, buf + off, sizeof(mr));
            off += sizeof(mr);
            d.runs.push_back(mr);
        }
        out->segs.push_back(std::move(d));
    }
    std::sort(out->segs.begin(), out->segs.end(),
              [](const ManifestSegDesc& a, const ManifestSegDesc& b) { return a.seg < b.seg; });
    return true;
}

namespace {
inline uint32_t popcnt64(uint64_t v) { return uint32_t(__builtin_popcountll(v)); }
}  // namespace

int64_t CompactDir::indexOf(uint64_t pseq) const {
    if (pseq < firstPseq || pseq >= endPseq) return -1;
    const uint64_t rel = pseq - firstPseq;
    const size_t b = size_t(rel / kCompactBlockPseqs);
    if (b >= blocks.size()) return -1;
    const uint32_t bit = uint32_t(rel % kCompactBlockPseqs);
    const CompactDirBlock& blk = blocks[b];
    if (!((blk.bits[bit / 64] >> (bit % 64)) & 1)) return -1;
    uint64_t idx = blk.rank;
    for (uint32_t w = 0; w < bit / 64; w++) idx += popcnt64(blk.bits[w]);
    const uint32_t r = bit % 64;
    if (r) idx += popcnt64(blk.bits[bit / 64] & ((1ull << r) - 1));
    return int64_t(idx);
}

uint64_t CompactDir::countIn(uint64_t a, uint64_t b) const {
    if (a < firstPseq) a = firstPseq;
    if (b > endPseq) b = endPseq;
    if (a >= b) return 0;
    // rank(b) - rank(a), rank(x) = rows below x.
    auto rank = [&](uint64_t x) -> uint64_t {
        if (x >= endPseq) return nRows;
        const uint64_t rel = x - firstPseq;
        const size_t blkI = size_t(rel / kCompactBlockPseqs);
        if (blkI >= blocks.size()) return nRows;
        const uint32_t bit = uint32_t(rel % kCompactBlockPseqs);
        const CompactDirBlock& blk = blocks[blkI];
        uint64_t idx = blk.rank;
        for (uint32_t w = 0; w < bit / 64; w++) idx += popcnt64(blk.bits[w]);
        const uint32_t r = bit % 64;
        if (r) idx += popcnt64(blk.bits[bit / 64] & ((1ull << r) - 1));
        return idx;
    };
    return rank(b) - rank(a);
}

bool parseCompactHeader(const uint8_t* buf, size_t len, CompactRowsHeader* out) {
    if (len < sizeof(CompactRowsHeader)) return false;
    std::memcpy(out, buf, sizeof(*out));
    if (out->magic != kMagicCompactRows || out->ver != 1) return false;
    if (crc32c(out, offsetof(CompactRowsHeader, crc)) != out->crc) return false;
    if (out->endPseq < out->firstPseq) return false;
    const uint64_t span = out->endPseq - out->firstPseq;
    if (out->nBlocks != (span + kCompactBlockPseqs - 1) / kCompactBlockPseqs) return false;
    if (out->nRows > span) return false;
    if (out->dirOff != sizeof(CompactRowsHeader) + out->nRows * 128) return false;
    return true;
}

bool parseCompactDir(const CompactRowsHeader& h, const uint8_t* dir, size_t len, CompactDir* out) {
    if (len != size_t(h.nBlocks) * sizeof(CompactDirBlock)) return false;
    if (crc32c(dir, len) != h.dirCrc) return false;
    out->seg = h.seg;
    out->gen = h.gen;
    out->firstPseq = h.firstPseq;
    out->endPseq = h.endPseq;
    out->nRows = h.nRows;
    out->blocks.resize(h.nBlocks);
    if (len) std::memcpy(out->blocks.data(), dir, len);
    uint64_t rank = 0;
    for (const auto& b : out->blocks) {
        if (b.rank != rank) return false;
        for (uint64_t w : b.bits) rank += popcnt64(w);
    }
    return rank == h.nRows;
}

void buildCompactDir(uint64_t first, uint64_t end, const std::vector<uint64_t>& kept,
                     std::vector<CompactDirBlock>* out) {
    const uint64_t span = end - first;
    out->assign(size_t((span + kCompactBlockPseqs - 1) / kCompactBlockPseqs), CompactDirBlock{});
    for (uint64_t ps : kept) {
        const uint64_t rel = ps - first;
        CompactDirBlock& b = (*out)[size_t(rel / kCompactBlockPseqs)];
        const uint32_t bit = uint32_t(rel % kCompactBlockPseqs);
        b.bits[bit / 64] |= 1ull << (bit % 64);
    }
    uint32_t rank = 0;
    for (auto& b : *out) {
        b.rank = rank;
        for (uint64_t w : b.bits) rank += popcnt64(w);
    }
}

void retirePath(PathBuf* out, const char* root, uint32_t pid, const RetireItem& it) {
    switch (it.letter) {
        case 'd': pathPartitionSeg(out, root, pid, 'd', it.seg, "fsd"); break;
        case 'r': pathPartitionSeg(out, root, pid, 'r', it.seg, "fsr"); break;
        case 'a': pathPartitionSeg(out, root, pid, 'a', it.seg, "fsa"); break;
        case 'm': pathPartitionSeg(out, root, pid, 'm', it.seg, "fsl"); break;
        case 'D': pathPartitionCompact(out, root, pid, it.seg, it.gen, "fsd"); break;
        case 'R': pathPartitionCompact(out, root, pid, it.seg, it.gen, "fsr"); break;
        case 'A': pathPartitionCompact(out, root, pid, it.seg, it.gen, "fsa"); break;
        case 'x': pathPartitionRun(out, root, pid, it.seg, it.gen); break;
        case 'f': pathPartitionManifest(out, root, pid, it.gen); break;
        default: out->buf[0] = 0; out->len = 0; break;
    }
}

void typeRetirePath(PathBuf* out, const char* root, const uint8_t fid[4], const RetireItem& it) {
    switch (it.letter) {
        case 'm': pathTypeSeg(out, root, fid, 'm', it.seg, "fsl"); break;
        case 'x': pathTypeRun(out, root, fid, it.gen); break;
        case 'f': pathTypeManifest(out, root, fid, it.gen); break;
        case 'g': pathTypeSeg(out, root, fid, 'g', it.seg, "fsg"); break;
        case 'G': pathTypeArrivalsCompact(out, root, fid, it.gen); break;
        default: out->buf[0] = 0; out->len = 0; break;
    }
}

void appendTypeArrivalsTable(std::vector<uint8_t>* man, const std::vector<ArrOverride>& table) {
    const size_t at = man->size();
    const size_t body = 8 + table.size() * sizeof(ArrOverride);
    man->resize(at + body + 8, 0);
    uint8_t* b = man->data() + at;
    putU32(b, kMagicTypeArrivals);
    putU32(b + 4, uint32_t(table.size()));
    if (!table.empty()) std::memcpy(b + 8, table.data(), table.size() * sizeof(ArrOverride));
    putU32(b + body, crc32c(b, body));
}

bool parseTypeArrivalsTable(const uint8_t* man, size_t len, size_t at, std::vector<ArrOverride>* out) {
    out->clear();
    // Past the retire set.
    if (at + 16 <= len && getU32(man + at) == kMagicTypeRetire) {
        const uint32_t n = getU32(man + at + 4);
        if (n > (1u << 20)) return false;
        at += 8 + size_t(n) * sizeof(RetireItem) + 8;
    }
    if (at + 16 > len || getU32(man + at) != kMagicTypeArrivals) return true;
    const uint32_t n = getU32(man + at + 4);
    const size_t body = 8 + size_t(n) * sizeof(ArrOverride);
    if (n > (1u << 24) || at + body + 8 > len || crc32c(man + at, body) != getU32(man + at + body)) return false;
    out->resize(n);
    if (n) std::memcpy(out->data(), man + at + 8, size_t(n) * sizeof(ArrOverride));
    return true;
}

void appendTypeRetireSet(std::vector<uint8_t>* man, const std::vector<RetireItem>& items) {
    const size_t at = man->size();
    const size_t body = 8 + items.size() * sizeof(RetireItem);
    man->resize(at + body + 8, 0);
    uint8_t* b = man->data() + at;
    putU32(b, kMagicTypeRetire);
    putU32(b + 4, uint32_t(items.size()));
    if (!items.empty()) std::memcpy(b + 8, items.data(), items.size() * sizeof(RetireItem));
    putU32(b + body, crc32c(b, body));
}

bool parseTypeRetireSet(const uint8_t* man, size_t len, size_t at, std::vector<RetireItem>* out) {
    out->clear();
    if (at + 16 > len || getU32(man + at) != kMagicTypeRetire) return false;
    const uint32_t n = getU32(man + at + 4);
    const size_t body = 8 + size_t(n) * sizeof(RetireItem);
    if (n > (1u << 20) || at + body + 8 > len || crc32c(man + at, body) != getU32(man + at + body)) return false;
    out->resize(n);
    if (n) std::memcpy(out->data(), man + at + 8, size_t(n) * sizeof(RetireItem));
    return true;
}

// ---------------------------------------------------------------------------
// Partition state <-> manifest
// ---------------------------------------------------------------------------
ManifestSegDesc segDesc(const SegmentInfo& s) {
    ManifestSegDesc d;
    d.seg = s.seg;
    d.lastSeg = s.lastSeg ? s.lastSeg : s.seg;
    d.sealed = s.sealed;
    d.empty = s.empty;
    d.cgen = s.cgen;
    d.firstPseq = s.firstPseq;
    d.endPseq = s.endPseq;
    d.mergedEnd = s.mergedEnd;
    d.dLen = s.dLen;
    d.rLen = s.rLen;
    d.aLen = s.aLen;
    d.minEpoch = s.minEpoch;
    d.maxEpoch = s.maxEpoch;
    d.minArrival = s.minArrival;
    d.maxArrival = s.maxArrival;
    d.killThrough = s.killThrough;
    d.prevGen = s.prevGen;
    for (const auto& r : s.runs) {
        ManifestRun mr{};
        mr.gen = r.gen;
        mr.nEntries = r.run ? r.run->entries() : 0;
        mr.fileLen = r.fileLen;
        d.runs.push_back(mr);
    }
    return d;
}

void segFromDesc(const ManifestSegDesc& d, SegmentInfo* si) {
    si->seg = d.seg;
    si->lastSeg = d.lastSeg == d.seg ? 0 : d.lastSeg;
    si->sealed = d.sealed;
    si->empty = d.empty;
    si->cgen = d.cgen;
    si->firstPseq = d.firstPseq;
    si->endPseq = d.endPseq;
    si->mergedEnd = d.mergedEnd;
    si->dLen = d.dLen;
    si->rLen = d.rLen;
    si->aLen = d.aLen;
    si->minEpoch = d.minEpoch;
    si->maxEpoch = d.maxEpoch;
    si->minArrival = d.minArrival;
    si->maxArrival = d.maxArrival;
    si->killThrough = d.killThrough;
    si->prevGen = d.prevGen;
    si->runs.clear();
    for (const auto& r : d.runs) {
        SegRun run;
        run.gen = r.gen;
        run.fileLen = r.fileLen;
        si->runs.push_back(std::move(run));
    }
}

int32_t partitionLoadCompactDir(IoCtx* io, const char* root, uint32_t pid, SegmentInfo* si) {
    if (si->cdir || !si->cgen || si->empty) return 0;
    PathBuf path;
    pathPartitionCompact(&path, root, pid, si->seg, si->cgen, "fsr");
    FileRef f;
    int32_t rc = io->open(path.c_str(), path.len, FLATSQL_IO_READ, FileClass::Rows, &f);
    if (rc < 0) return rc;
    uint8_t hb[sizeof(CompactRowsHeader)];
    CompactRowsHeader h;
    auto dir = std::make_shared<CompactDir>();
    if (io->read(f, hb, sizeof(hb), 0) != int64_t(sizeof(hb)) || !parseCompactHeader(hb, sizeof(hb), &h)) {
        rc = FLATSQL_IO_ERR_IO;
    } else {
        std::vector<uint8_t> db(size_t(h.nBlocks) * sizeof(CompactDirBlock));
        if ((!db.empty() && io->read(f, db.data(), db.size(), h.dirOff) != int64_t(db.size())) ||
            !parseCompactDir(h, db.data(), db.size(), dir.get()))
            rc = FLATSQL_IO_ERR_IO;
    }
    io->close(&f);
    if (rc < 0) return rc;
    si->cdir = dir;
    return 0;
}

// ---------------------------------------------------------------------------
// The plan
// ---------------------------------------------------------------------------
struct CompactInput {
    uint32_t seg = 0;
    uint32_t lastSeg = 0;
    uint32_t cgen = 0;
    bool empty = false;
    uint64_t first = 0, end = 0;
    uint64_t dLen = 0, rLen = 0, aLen = 0;
    uint64_t killThrough = 0;
    uint64_t diskBytes = 0;          // the input's files on disk (ledger)
    std::vector<std::pair<uint32_t, uint64_t>> runs;  // (gen, fileLen)
    std::shared_ptr<const CompactDir> cdir;
};

struct PresenceSeg {
    uint64_t first = 0, end = 0;
    bool empty = false;
    uint64_t killThrough = 0;
    std::shared_ptr<const CompactDir> cdir;
};

struct CompactPlan {
    uint32_t pid = 0;
    uint32_t seg = 0, segEnd = 0, gen = 0;
    uint32_t ownerEpoch = 0;
    const std::atomic<uint64_t>* ownerWord = nullptr;
    uint64_t bound = 0;
    bool forced = false;
    SwapResult* req = nullptr;
    std::vector<CompactInput> inputs;
    std::vector<std::tuple<uint32_t, uint32_t, uint64_t>> scanRuns;  // (seg, gen, fileLen): kill postings
    std::vector<L0DirEntry> l0;
    std::vector<PresenceSeg> presence;  // compacted non-input segments, ascending
    std::atomic<bool> abort{false};
    std::atomic<int32_t> result{0};     // 0 running, 1 built, < 0 error
    uint64_t createdNs = 0;             // files retired after this stay until the SWAP or abort
    // Outputs.
    uint64_t first = 0, end = 0;
    bool empty = false;
    uint64_t dLen = 0, rLen = 0, aLen = 0, xLen = 0, xEntries = 0, mfLen = 0;
    uint64_t killThrough = 0;
    int64_t minEpoch = INT64_MAX, maxEpoch = INT64_MIN, minArrival = INT64_MAX, maxArrival = INT64_MIN;
    uint64_t rowsIn = 0, rowsKept = 0, putsDropped = 0, putBytesDropped = 0, tombsDropped = 0;
    uint64_t bytesIn = 0, bytesOut = 0, deadFrameBytes = 0;
    uint64_t buildNs = 0;
    std::shared_ptr<const CompactDir> cdir;
    SegRun run;                          // the new L1 run (accelerators loaded)
    FileRef mf;                          // the manifest (owner; synced in the SWAP round)
    std::vector<RetireItem> retire;      // what the SWAP retires
};

namespace {

constexpr int32_t kOpenRW = FLATSQL_IO_READ | FLATSQL_IO_WRITE;
constexpr int32_t kCompactNotOwner = -1001;
constexpr int32_t kCompactNotWorth = -1003;

bool planOwns(const CompactPlan& c) {
    if (c.abort.load(std::memory_order_acquire)) return false;
    if (!c.ownerWord) return true;
    const uint64_t w = c.ownerWord->load(std::memory_order_acquire);
    return ownerEpoch(w) == c.ownerEpoch && ownerState(w) == kOwnOwned;
}

SegmentInfo* findSegInfo(Partition* p, uint32_t seg) {
    for (auto& s : p->segs)
        if (s.seg == seg) return &s;
    return nullptr;
}

bool hasPendingL0(const Partition* p, const SegmentInfo& s) {
    const uint32_t last = s.lastSeg ? s.lastSeg : s.seg;
    for (uint32_t i = 0; i < p->nL0; i++)
        if (p->l0[i].mSeg >= s.seg && p->l0[i].mSeg <= last) return true;
    return false;
}

// A sealed, fully merged segment with no L0 block left: compactable.
bool compactable(const Partition* p, const SegmentInfo& s) {
    return s.sealed && s.endPseq > s.firstPseq && s.firstPseq && s.mergedEnd == s.endPseq &&
           (s.empty || s.cgen || !s.runs.empty()) && !hasPendingL0(p, s);
}

uint64_t inputDiskBytes(const Partition* p, const SegmentInfo& s) {
    uint64_t b = 0;
    if (s.cgen) {
        b += ledgerSize(p, 'D', s.seg, s.cgen) + ledgerSize(p, 'R', s.seg, s.cgen) +
             ledgerSize(p, 'A', s.seg, s.cgen);
    } else {
        b += ledgerSize(p, 'd', s.seg, 0) + ledgerSize(p, 'r', s.seg, 0) + ledgerSize(p, 'a', s.seg, 0);
    }
    for (const auto& r : s.runs) b += r.fileLen;
    return b;
}

// Chooses inputs. Returns false when nothing qualifies.
bool pickCandidate(const Engine* e, const Partition* p, uint32_t* seg, uint32_t* segEnd) {
    const EngineConfig& cfg = e->config();
    // 1. The sealed segment with the largest known dead share >= the ratio.
    const SegmentInfo* best = nullptr;
    double bestShare = 0;
    for (const auto& s : p->segs) {
        if (!compactable(p, s) || s.empty) continue;
        const uint64_t data = s.dLen ? s.dLen : 1;
        const double share = double(s.deadBytes) / double(data);
        if (share >= cfg.compactDeadRatio && share > bestShare) {
            best = &s;
            bestShare = share;
        }
    }
    if (best) {
        // Neighbors past the ratio too join the same output, within the
        // coalescing limits: updates spread over a partition bring its
        // segments to the ratio together, and one compaction per segment
        // would leave the rest of the wave on disk meanwhile.
        auto qualifies = [&](const SegmentInfo& s) {
            if (!compactable(p, s) || s.empty) return false;
            const uint64_t data = s.dLen ? s.dLen : 1;
            return double(s.deadBytes) / double(data) >= cfg.compactDeadRatio;
        };
        size_t lo = size_t(best - p->segs.data()), hi = lo;
        uint64_t bytes = inputDiskBytes(p, *best);
        for (bool grown = true; grown;) {
            grown = false;
            if (hi - lo + 1 < cfg.compactMaxInputs && hi + 1 < p->segs.size()) {
                const SegmentInfo& n = p->segs[hi + 1];
                const uint64_t nb = inputDiskBytes(p, n);
                if (n.firstPseq == p->segs[hi].endPseq && qualifies(n) && bytes + nb <= cfg.compactMaxOutputBytes) {
                    hi++;
                    bytes += nb;
                    grown = true;
                }
            }
            if (hi - lo + 1 < cfg.compactMaxInputs && lo > 0) {
                const SegmentInfo& n = p->segs[lo - 1];
                const uint64_t nb = inputDiskBytes(p, n);
                if (p->segs[lo].firstPseq == n.endPseq && qualifies(n) && bytes + nb <= cfg.compactMaxOutputBytes) {
                    lo--;
                    bytes += nb;
                    grown = true;
                }
            }
        }
        *seg = p->segs[lo].seg;
        *segEnd = p->segs[hi].lastSeg ? p->segs[hi].lastSeg : p->segs[hi].seg;
        return true;
    }
    // 2. Adjacent small sealed segments coalesce (§11: "under 8 MiB each").
    const SegmentInfo* runStart = nullptr;
    const SegmentInfo* runEnd = nullptr;
    uint32_t n = 0;
    uint64_t bytes = 0, prevEnd = 0;
    for (const auto& s : p->segs) {
        const uint64_t sz = inputDiskBytes(p, s);
        const bool small = compactable(p, s) && sz < cfg.compactSmallBytes;
        if (small && runStart && s.firstPseq == prevEnd && n < cfg.compactMaxInputs &&
            bytes + sz <= cfg.compactMaxOutputBytes) {
            runEnd = &s;
            n++;
            bytes += sz;
        } else if (small) {
            if (runStart && n >= 2) break;
            runStart = runEnd = &s;
            n = 1;
            bytes = sz;
        } else {
            if (runStart && n >= 2) break;
            runStart = runEnd = nullptr;
            n = 0;
            bytes = 0;
        }
        prevEnd = s.endPseq;
    }
    if (runStart && n >= 2) {
        *seg = runStart->seg;
        *segEnd = runEnd->lastSeg ? runEnd->lastSeg : runEnd->seg;
        return true;
    }
    // 3. After a restart the per-segment dead counts are unknown: when the
    // partition as a whole is past the ratio, survey the oldest compactable
    // segment not surveyed since warm (the build abandons it when it would
    // save under 10%).
    const uint64_t total = p->counters.totalBytes;
    if (total && double(total - std::min(total, p->counters.liveBytes)) >= cfg.compactDeadRatio * double(total)) {
        for (const auto& s : p->segs) {
            if (!compactable(p, s) || s.empty || s.deadRows) continue;  // deadRows set: known (surveyed)
            *seg = s.seg;
            *segEnd = s.lastSeg ? s.lastSeg : s.seg;
            return true;
        }
    }
    return false;
}

void compactDone(SwapResult* r, int32_t status) {
    if (!r) return;
    r->status = status;
    if (r->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1)
        wakeU32(reinterpret_cast<std::atomic<uint32_t>*>(&r->remaining), -1);
}

void queueCtl(Partition* p, uint16_t kind, const uint8_t* body, uint16_t len) {
    if (p->pendingCtlBytes + 4 + len > sizeof(p->pendingCtl)) return;
    putU16(p->pendingCtl + p->pendingCtlBytes, kind);
    putU16(p->pendingCtl + p->pendingCtlBytes + 2, len);
    std::memcpy(p->pendingCtl + p->pendingCtlBytes + 4, body, len);
    p->pendingCtlBytes += 4 + len;
    p->nPendingCtl++;
}

// Plans a compaction of [seg, segEnd] (owner, maintenance).
int32_t planCompaction(Writer* w, Partition* p, uint32_t seg, uint32_t segEnd, SwapResult* req, bool forced) {
    std::shared_ptr<CompactPlan> c = std::make_shared<CompactPlan>();
    c->pid = p->pid;
    c->seg = seg;
    c->segEnd = segEnd;
    c->req = req;
    c->forced = forced;
    c->createdNs = monoNs();
    c->ownerEpoch = p->ownerEpoch;
    c->ownerWord = &p->ring->ownerWordV;
    const uint64_t labeled = p->labeledThrough.load(std::memory_order_acquire);
    c->bound = std::min(labeled, p->pseqHi);
    if (!p->type) c->bound = p->pseqHi;  // untyped (control): no type-level reader
    uint64_t prevEnd = 0;
    for (auto& s : p->segs) {
        const uint32_t last = s.lastSeg ? s.lastSeg : s.seg;
        const bool in = s.seg >= seg && last <= segEnd;
        if (in) {
            if (!compactable(p, s)) return FLATSQL_IO_ERR_BUSY;
            if (!c->inputs.empty() && s.firstPseq != prevEnd) return FLATSQL_IO_ERR_GENERIC;
            if (s.cgen && !s.empty) {
                const int32_t rc = partitionLoadCompactDir(&w->io(), w->eng_root(), p->pid, &s);
                if (rc < 0) return rc;
            }
            CompactInput ci;
            ci.seg = s.seg;
            ci.lastSeg = last;
            ci.cgen = s.cgen;
            ci.empty = s.empty;
            ci.first = s.firstPseq;
            ci.end = s.endPseq;
            ci.dLen = s.dLen;
            ci.rLen = s.rLen;
            ci.aLen = s.aLen;
            ci.killThrough = s.killThrough;
            ci.cdir = s.cdir;
            ci.diskBytes = inputDiskBytes(p, s);
            for (const auto& r : s.runs) ci.runs.push_back({r.gen, r.fileLen});
            prevEnd = s.endPseq;
            c->inputs.push_back(std::move(ci));
        }
    }
    if (c->inputs.empty()) return FLATSQL_IO_ERR_NOENT;
    c->first = c->inputs.front().first;
    c->end = c->inputs.back().end;
    for (auto& s : p->segs) {
        const uint32_t last = s.lastSeg ? s.lastSeg : s.seg;
        const bool in = s.seg >= seg && last <= segEnd;
        // Kill postings live at or after their targets: runs of the inputs and
        // of every later segment.
        if (s.firstPseq && s.endPseq > c->first && !s.empty)
            for (const auto& r : s.runs) c->scanRuns.emplace_back(s.seg, r.gen, r.fileLen);
        else if (!s.firstPseq)  // active / unmerged entries only list runs by seg
            for (const auto& r : s.runs) c->scanRuns.emplace_back(s.seg, r.gen, r.fileLen);
        if (!in && s.cgen && s.firstPseq && s.firstPseq < c->first) {
            if (!s.empty) {
                const int32_t rc = partitionLoadCompactDir(&w->io(), w->eng_root(), p->pid, &s);
                if (rc < 0) return rc;
            }
            PresenceSeg ps;
            ps.first = s.firstPseq;
            ps.end = s.endPseq;
            ps.empty = s.empty;
            ps.killThrough = s.killThrough;
            ps.cdir = s.cdir;
            c->presence.push_back(std::move(ps));
        }
    }
    c->l0.assign(p->l0, p->l0 + p->nL0);
    c->gen = p->nextGen++;
    uint8_t body[kIntentCompactBytes];
    putU32(body, seg);
    putU32(body + 4, segEnd);
    putU32(body + 8, c->gen);
    putU32(body + 12, 0);
    queueCtl(p, kCtlIntentCompact, body, sizeof(body));
    p->cplan = c;
    p->compactPhase = kCompactIntentQueued;
    w->engine()->cCompactInFlight.fetch_add(1, std::memory_order_relaxed);
    w->ring();
    return 1;
}

// ---- the builder ------------------------------------------------------------------
struct Appender {
    IoCtx* io = nullptr;
    FileRef f;
    uint64_t pos = 0;
    std::vector<uint8_t> buf;
    uint64_t bufAt = 0;
    int32_t err = 0;
    size_t cap = 4u << 20;
    void put(const void* p, size_t n) {
        if (err) return;
        if (buf.size() + n > cap && !buf.empty()) flush();
        const uint8_t* b = static_cast<const uint8_t*>(p);
        buf.insert(buf.end(), b, b + n);
        pos += n;
    }
    void pad8() {
        static const uint8_t z[8] = {0};
        const size_t n = size_t((8 - (pos & 7)) & 7);
        if (n) put(z, n);
    }
    void flush() {
        if (err || buf.empty()) return;
        const int32_t rc = io->write(f, buf.data(), buf.size(), bufAt);
        if (rc < 0) err = rc;
        bufAt += buf.size();
        buf.clear();
    }
};

// Sequential window over an input file (frames and attributes are appended
// in pseq order, so the surviving ones are read front to back).
struct Window {
    IoCtx* io = nullptr;
    FileRef f;
    uint64_t fileLen = 0;
    std::vector<uint8_t> buf;
    uint64_t off = 0, len = 0;
    uint64_t readBytes = 0;
    const uint8_t* get(uint64_t at, uint32_t n) {
        if (at >= off && at + n <= off + len) return buf.data() + (at - off);
        const uint64_t want = std::max<uint64_t>(n, 4u << 20);
        const uint64_t avail = fileLen > at ? fileLen - at : 0;
        const uint64_t take = std::min(want, std::max<uint64_t>(avail, n));
        buf.resize(size_t(take));
        const int64_t got = io->read(f, buf.data(), size_t(take), at);
        if (got < int64_t(n)) {
            len = 0;
            return nullptr;
        }
        readBytes += uint64_t(got);
        off = at;
        len = uint64_t(got);
        return buf.data();
    }
};

int32_t openInputFile(IoCtx* io, const char* root, uint32_t pid, const CompactInput& in, char letter, FileRef* f) {
    PathBuf path;
    const char* ext = letter == 'd' ? "fsd" : letter == 'r' ? "fsr" : "fsa";
    if (in.cgen) pathPartitionCompact(&path, root, pid, in.seg, in.cgen, ext);
    else pathPartitionSeg(&path, root, pid, letter, in.seg, ext);
    const FileClass cls = letter == 'd' ? FileClass::Data : letter == 'r' ? FileClass::Rows : FileClass::Attrs;
    return io->open(path.c_str(), path.len, FLATSQL_IO_READ, cls, f);
}

// Rows [a, b) of an input (VOID where absent).
int32_t readInputRows(IoCtx* io, const CompactInput& in, const FileRef& rf, uint64_t a, uint64_t b,
                      std::vector<RecRow>* out) {
    out->resize(size_t(b - a));
    if (in.empty) {
        for (uint64_t ps = a; ps < b; ps++) voidRow(&(*out)[size_t(ps - a)], ps, in.seg);
        return 0;
    }
    if (!in.cgen) {
        const size_t bytes = size_t(b - a) * sizeof(RecRow);
        if (io->read(rf, out->data(), bytes, (a - in.first) * sizeof(RecRow)) != int64_t(bytes))
            return FLATSQL_IO_ERR_IO;
        for (uint64_t ps = a; ps < b; ps++)
            if ((*out)[size_t(ps - a)].pseq != ps) return FLATSQL_IO_ERR_IO;
        return 0;
    }
    const CompactDir& d = *in.cdir;
    const uint64_t n = d.countIn(a, b);
    std::vector<RecRow> present(static_cast<size_t>(n));
    if (n) {
        const uint64_t idx0 = d.countIn(d.firstPseq, a);
        const size_t bytes = size_t(n) * sizeof(RecRow);
        if (io->read(rf, present.data(), bytes, CompactDir::rowOffset(idx0)) != int64_t(bytes))
            return FLATSQL_IO_ERR_IO;
    }
    size_t k = 0;
    for (uint64_t ps = a; ps < b; ps++) {
        RecRow& r = (*out)[size_t(ps - a)];
        if (d.contains(ps)) {
            if (k >= present.size() || present[k].pseq != ps) return FLATSQL_IO_ERR_IO;
            r = present[k++];
        } else {
            voidRow(&r, ps, in.seg);
        }
    }
    return 0;
}

// Kill postings of a kind over [first, end): target -> smallest killer <= bound.
int32_t scanKills(IoCtx* io, const char* root, uint32_t pid, const CompactPlan& c, uint16_t kind,
                  std::vector<uint64_t>* killer, std::vector<uint8_t>& scratch) {
    killer->assign(size_t(c.end - c.first), 0);
    uint8_t lo[8], hi[8];
    putBE64(lo, c.first);
    putBE64(hi, c.end);
    auto note = [&](const uint8_t* key, uint16_t klen, const uint8_t* val) {
        if (klen != 8) return;
        const uint64_t target = getBE64(key);
        const uint64_t k = getBE64(val);
        if (target < c.first || target >= c.end || k > c.bound || k == 0) return;
        uint64_t& slot = (*killer)[size_t(target - c.first)];
        if (!slot || k < slot) slot = k;
    };
    for (const auto& rt : c.scanRuns) {
        if (!planOwns(c)) return kCompactNotOwner;
        PathBuf rp;
        pathPartitionRun(&rp, root, pid, std::get<0>(rt), std::get<1>(rt));
        FileRef f;
        int32_t rc = io->open(rp.c_str(), rp.len, FLATSQL_IO_READ, FileClass::Index, &f);
        if (rc < 0) return rc;
        L1Run run;
        rc = run.load(io, f, std::get<2>(rt));
        if (rc >= 0 && run.hasKind(kind)) {
            const int64_t n = run.scanRange(io, f, kind, lo, 8, hi, 8, scratch.data(),
                                            [&](const uint8_t* k, uint16_t kl, const uint8_t* v) {
                                                note(k, kl, v);
                                                return true;
                                            });
            if (n < 0) rc = int32_t(n);
        }
        io->close(&f);
        if (rc < 0) return rc;
    }
    // Unmerged L0 blocks (their batches are committed; m is append-only).
    FileRef mf;
    uint32_t mfSeg = UINT32_MAX;
    int32_t rc = 0;
    for (const L0DirEntry& e : c.l0) {
        if (e.mSeg != mfSeg) {
            io->close(&mf);
            PathBuf mp;
            pathPartitionSeg(&mp, root, pid, 'm', e.mSeg, "fsl");
            rc = io->open(mp.c_str(), mp.len, FLATSQL_IO_READ, FileClass::Meta, &mf);
            if (rc < 0) break;
            mfSeg = e.mSeg;
        }
        L0Header lh;
        if (io->read(mf, &lh, sizeof(lh), e.mOff + e.l0Off) != int64_t(sizeof(lh)) || lh.magic != kMagicL0) {
            rc = FLATSQL_IO_ERR_IO;
            break;
        }
        std::vector<uint8_t> block(lh.blockLen);
        if (io->read(mf, block.data(), block.size(), e.mOff + e.l0Off) != int64_t(block.size())) {
            rc = FLATSQL_IO_ERR_IO;
            break;
        }
        L0KindInfo kinds[64];
        size_t nk = 0;
        if (!parseL0Block(block.data(), block.size(), kinds, 64, &nk)) {
            rc = FLATSQL_IO_ERR_IO;
            break;
        }
        for (size_t i = 0; i < nk; i++) {
            if (kinds[i].kind != kind) continue;
            EntryIter it;
            it.p = block.data() + kinds[i].entriesOff;
            it.end = it.p + kinds[i].entriesBytes;
            it.vlen = kinds[i].vlen;
            const uint8_t *k, *v;
            uint16_t kl;
            while (it.next(&k, &kl, &v)) note(k, kl, v);
        }
    }
    io->close(&mf);
    return rc;
}

// Is pseq present in the files as of the plan? (Non-input compacted
// segments only: everything else is present.) *kill: that segment's bound.
bool presentOutside(const CompactPlan& c, uint64_t pseq, uint64_t* kill) {
    size_t a = 0, b = c.presence.size();
    while (a < b) {
        const size_t m = (a + b) / 2;
        if (c.presence[m].end <= pseq) a = m + 1;
        else b = m;
    }
    if (a >= c.presence.size() || pseq < c.presence[a].first) return true;
    const PresenceSeg& s = c.presence[a];
    const bool present = !s.empty && s.cdir && s.cdir->contains(pseq);
    if (!present) *kill = s.killThrough;
    return present;
}

struct KeepCtx {
    uint64_t first;
    const std::vector<uint8_t>* keep;
};
bool keepPosting(void* ctx, uint16_t, const uint8_t*, uint16_t, const uint8_t* val, uint8_t vlen) {
    const KeepCtx* k = static_cast<const KeepCtx*>(ctx);
    if (vlen != 8) return true;
    const uint64_t ps = getBE64(val);
    if (ps < k->first || ps - k->first >= k->keep->size()) return false;
    return (*k->keep)[size_t(ps - k->first)] != 0;
}

void pace(const EngineConfig& cfg, uint64_t busyNs) {
    if (!cfg.compactPacePct || cfg.compactPacePct >= 100) return;
    sleepNs(busyNs * cfg.compactPacePct / (100 - cfg.compactPacePct));
}

void unlinkOutputs(IoCtx* io, const char* root, uint32_t pid, uint32_t seg, uint32_t gen) {
    PathBuf p;
    const char* exts[3] = {"fsd", "fsr", "fsa"};
    for (const char* ext : exts) {
        pathPartitionCompact(&p, root, pid, seg, gen, ext);
        io->unlink(p.c_str(), p.len, true);
    }
    pathPartitionRun(&p, root, pid, seg, gen);
    io->unlink(p.c_str(), p.len, true);
    pathPartitionManifest(&p, root, pid, gen);
    io->unlink(p.c_str(), p.len, true);
}

int32_t buildCompaction(IoCtx* io, const char* root, const EngineConfig& cfg, CompactPlan& c) {
    const uint64_t t0 = monoNs();
    const uint32_t pid = c.pid;
    const uint64_t span = c.end - c.first;
    std::vector<uint8_t> scratch(kL1BlockBytes);
    std::vector<uint64_t> killer, tagKiller;
    int32_t rc = scanKills(io, root, pid, c, kIxDead, &killer, scratch);
    if (rc >= 0) rc = scanKills(io, root, pid, c, kIxTagDead, &tagKiller, scratch);
    if (rc < 0) return rc;
    // Pass 1: decide per row. `present`: the row exists in the inputs (not
    // VOID): a tombstone whose target an earlier SWAP removed from these very
    // inputs goes like one whose target is outside them (minor 1).
    std::vector<uint8_t> keep(size_t(span), 0);
    std::vector<uint8_t> present(size_t(span), 0);
    std::vector<uint64_t> kept;
    uint64_t killThrough = 0;
    for (const auto& in : c.inputs) killThrough = std::max(killThrough, in.killThrough);
    std::vector<RecRow> rows;
    for (const auto& in : c.inputs) {
        FileRef rf;
        if (!in.empty) {
            rc = openInputFile(io, root, pid, in, 'r', &rf);
            if (rc < 0) return rc;
        }
        for (uint64_t a = in.first; a < in.end && rc >= 0;) {
            if (!planOwns(c)) {
                rc = kCompactNotOwner;
                break;
            }
            const uint64_t b = std::min(in.end, a + 8192);
            rc = readInputRows(io, in, rf, a, b, &rows);
            if (rc < 0) break;
            for (const RecRow& r : rows) {
                const size_t i = size_t(r.pseq - c.first);
                if (r.kind == kRowVoid) continue;
                present[i] = 1;
                c.rowsIn++;
                bool drop = false;
                uint64_t k = 0;
                switch (r.kind) {
                    case kRowPut:
                    case kRowLicence:
                    case kRowCtl:
                        if (killer[i]) {
                            drop = true;
                            k = killer[i];
                        }
                        break;
                    case kRowRetag:
                        if (tagKiller[i]) {
                            drop = true;
                            k = tagKiller[i];
                        } else if (r.targetPseq >= c.first && r.targetPseq < c.end) {
                            const size_t t = size_t(r.targetPseq - c.first);
                            if (killer[t]) {
                                drop = true;
                                k = killer[t];
                            }
                        } else if (r.targetPseq < c.first && !presentOutside(c, r.targetPseq, &k)) {
                            drop = true;
                        }
                        if (!drop && r.targetPseq >= c.first && r.targetPseq < r.pseq &&
                            !present[size_t(r.targetPseq - c.first)])
                            drop = true;  // its PUT is gone (the inputs' kill bound covers it)
                        break;
                    case kRowTomb:
                    case kRowCtlTomb:
                    case kRowTagTomb:
                        // Minor 1: only once an earlier SWAP removed the target
                        // (outside these inputs, or VOID within them).
                        if (r.targetPseq < c.first) {
                            if (!presentOutside(c, r.targetPseq, &k)) drop = true;
                        } else if (r.targetPseq < r.pseq && !present[size_t(r.targetPseq - c.first)]) {
                            drop = true;
                        }
                        break;
                    default:
                        break;
                }
                if (drop) {
                    killThrough = std::max(killThrough, k);
                    if (r.kind == kRowPut) {
                        c.putsDropped++;
                        c.putBytesDropped += r.len - 4;
                        c.deadFrameBytes += r.len;
                    } else if (r.kind == kRowTomb || r.kind == kRowCtlTomb) {
                        c.tombsDropped++;
                    } else if (r.kind == kRowLicence || r.kind == kRowCtl) {
                        c.deadFrameBytes += r.len;
                    }
                    continue;
                }
                keep[i] = 1;
                kept.push_back(r.pseq);
            }
            a = b;
        }
        io->close(&rf);
        if (rc < 0) return rc;
    }
    // Tombs targeting rows this output removes stay (minor 1): nothing else
    // to resolve within the inputs.
    c.killThrough = killThrough;
    c.rowsKept = kept.size();
    for (const auto& in : c.inputs) c.bytesIn += in.diskBytes;
    if (!c.forced) {
        // Estimated output: surviving frame bytes + rows + attributes + index
        // share. Not worth it below a 10% saving (a survey found little).
        uint64_t inData = 0;
        for (const auto& in : c.inputs) inData += in.dLen;
        const bool coalesce = c.inputs.size() > 1;
        if (!coalesce && c.deadFrameBytes * 10 < inData && c.tombsDropped * 1280 < inData) return kCompactNotWorth;
    }
    const uint32_t outSeg = c.seg;
    c.empty = kept.empty();
    if (!c.empty) {
        // Pass 2: frames, attributes, rows (4 MiB slices, paced).
        Appender d, a, rowsOut;
        d.io = a.io = rowsOut.io = io;
        PathBuf dp, ap, rp;
        pathPartitionCompact(&dp, root, pid, outSeg, c.gen, "fsd");
        pathPartitionCompact(&ap, root, pid, outSeg, c.gen, "fsa");
        pathPartitionCompact(&rp, root, pid, outSeg, c.gen, "fsr");
        const int32_t fl = kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS;
        if (!planOwns(c)) return kCompactNotOwner;
        rc = io->open(dp.c_str(), dp.len, fl, FileClass::Data, &d.f);
        if (rc >= 0) rc = io->open(ap.c_str(), ap.len, fl, FileClass::Attrs, &a.f);
        if (rc >= 0) rc = io->open(rp.c_str(), rp.len, fl, FileClass::Rows, &rowsOut.f);
        rowsOut.pos = rowsOut.bufAt = sizeof(CompactRowsHeader);
        uint64_t sliceStart = monoNs(), sliceBytes = 0;
        size_t ki = 0;
        for (const auto& in : c.inputs) {
            if (rc < 0 || in.empty) continue;
            FileRef rf;
            Window dw, aw;
            dw.io = aw.io = io;
            dw.fileLen = in.dLen;
            aw.fileLen = in.aLen;
            rc = openInputFile(io, root, pid, in, 'r', &rf);
            if (rc >= 0) rc = openInputFile(io, root, pid, in, 'd', &dw.f);
            if (rc >= 0 && in.aLen) rc = openInputFile(io, root, pid, in, 'a', &aw.f);
            for (uint64_t lo = in.first; lo < in.end && rc >= 0;) {
                const uint64_t hi = std::min(in.end, lo + 8192);
                rc = readInputRows(io, in, rf, lo, hi, &rows);
                if (rc < 0) break;
                for (RecRow r : rows) {
                    if (r.kind == kRowVoid || !keep[size_t(r.pseq - c.first)]) continue;
                    if (r.len && (r.kind == kRowPut || r.kind == kRowLicence || r.kind == kRowCtl)) {
                        const uint8_t* fr = dw.get(r.off, r.len);
                        if (!fr) {
                            rc = FLATSQL_IO_ERR_IO;
                            break;
                        }
                        r.off = uint32_t(d.pos);
                        d.put(fr, r.len);
                        sliceBytes += r.len;
                    }
                    if ((r.flags & kRowHasAttr) && r.attrLen) {
                        if (r.flags & kRowAttrInM) {  // merged rows never carry it
                            rc = FLATSQL_IO_ERR_IO;
                            break;
                        }
                        const uint8_t* at = aw.get(r.attrOff, r.attrLen);
                        if (!at) {
                            rc = FLATSQL_IO_ERR_IO;
                            break;
                        }
                        uint8_t lenb[4];
                        putU32(lenb, r.attrLen);
                        a.put(lenb, 4);
                        r.attrOff = a.pos;
                        a.put(at, r.attrLen);
                        a.pad8();
                    }
                    if (r.kind == kRowPut) {
                        c.minEpoch = std::min(c.minEpoch, r.epochMs);
                        c.maxEpoch = std::max(c.maxEpoch, r.epochMs);
                        c.minArrival = std::min(c.minArrival, r.arrivalMs);
                        c.maxArrival = std::max(c.maxArrival, r.arrivalMs);
                    }
                    r.seg = outSeg;
                    rowsOut.put(&r, sizeof(r));
                    ki++;
                    sliceBytes += sizeof(r);
                    if (sliceBytes >= cfg.compactSliceBytes) {
                        if (!planOwns(c)) {
                            rc = kCompactNotOwner;
                            break;
                        }
                        pace(cfg, monoNs() - sliceStart);
                        sliceStart = monoNs();
                        sliceBytes = 0;
                    }
                }
                lo = hi;
            }
            io->close(&rf);
            io->close(&dw.f);
            io->close(&aw.f);
        }
        if (rc >= 0 && ki != kept.size()) rc = FLATSQL_IO_ERR_IO;
        // Directory and header.
        std::vector<CompactDirBlock> dir;
        buildCompactDir(c.first, c.end, kept, &dir);
        CompactRowsHeader h{};
        h.magic = kMagicCompactRows;
        h.ver = 1;
        h.seg = outSeg;
        h.gen = c.gen;
        h.firstPseq = c.first;
        h.endPseq = c.end;
        h.nRows = kept.size();
        h.dirOff = sizeof(CompactRowsHeader) + h.nRows * sizeof(RecRow);
        h.nBlocks = uint32_t(dir.size());
        h.dirCrc = crc32c(dir.data(), dir.size() * sizeof(CompactDirBlock));
        h.crc = crc32c(&h, offsetof(CompactRowsHeader, crc));
        if (rc >= 0) rowsOut.put(dir.data(), dir.size() * sizeof(CompactDirBlock));
        d.flush();
        a.flush();
        rowsOut.flush();
        if (rc >= 0) rc = d.err ? d.err : a.err ? a.err : rowsOut.err;
        if (rc >= 0 && !planOwns(c)) rc = kCompactNotOwner;
        if (rc >= 0) rc = io->write(rowsOut.f, &h, sizeof(h), 0);
        if (rc >= 0) rc = io->sync(d.f);
        if (rc >= 0) rc = io->sync(a.f);
        if (rc >= 0) rc = io->sync(rowsOut.f);
        c.dLen = d.pos;
        c.aLen = a.pos;
        c.rLen = rowsOut.pos;
        io->close(&d.f);
        io->close(&a.f);
        io->close(&rowsOut.f);
        if (rc < 0) return rc;
        auto cd = std::make_shared<CompactDir>();
        cd->seg = outSeg;
        cd->gen = c.gen;
        cd->firstPseq = c.first;
        cd->endPseq = c.end;
        cd->nRows = kept.size();
        cd->blocks = std::move(dir);
        c.cdir = cd;
        // The filtered L1 run.
        std::vector<FileRef> runFiles;
        std::vector<std::unique_ptr<L1Run>> runObjs;
        std::vector<MergeRunInput> runIns;
        for (const auto& in : c.inputs) {
            for (const auto& r : in.runs) {
                PathBuf xp;
                pathPartitionRun(&xp, root, pid, in.seg, r.first);
                FileRef f;
                rc = io->open(xp.c_str(), xp.len, FLATSQL_IO_READ, FileClass::Index, &f);
                if (rc < 0) break;
                runFiles.push_back(f);
                std::unique_ptr<L1Run> run(new L1Run());
                rc = run->load(io, f, r.second);
                if (rc < 0) break;
                runIns.push_back({run.get(), f});
                runObjs.push_back(std::move(run));
            }
            if (rc < 0) break;
        }
        PathBuf xp;
        pathPartitionRun(&xp, root, pid, outSeg, c.gen);
        if (rc >= 0 && !planOwns(c)) rc = kCompactNotOwner;
        if (rc >= 0)
            rc = io->open(xp.c_str(), xp.len, fl, FileClass::Index, &c.run.file);
        if (rc >= 0) {
            KeepCtx kc{c.first, &keep};
            uint64_t entries = 0;
            const int64_t xl = mergeToL1(io, c.run.file, outSeg, c.gen, 1, c.first, c.end - 1, {}, runIns, &entries,
                                         keepPosting, &kc);
            if (xl < 0) rc = int32_t(xl);
            c.xLen = xl > 0 ? uint64_t(xl) : 0;
            c.xEntries = entries;
        }
        for (auto& f : runFiles) io->close(&f);
        if (rc >= 0) rc = io->sync(c.run.file);
        if (rc >= 0) {
            c.run.gen = c.gen;
            c.run.fileLen = c.xLen;
            c.run.run.reset(new L1Run());
            rc = c.run.run->load(io, c.run.file, c.xLen);
        }
        if (rc < 0) {
            io->close(&c.run.file);
            return rc;
        }
    }
    c.bytesOut = c.dLen + c.aLen + c.rLen + c.xLen;
    c.buildNs = monoNs() - t0;
    return 0;
}

}  // namespace

// ---------------------------------------------------------------------------
// Owner-side steps
// ---------------------------------------------------------------------------
namespace {

// Queues the SWAP: the manifest (written now, synced in the next round's
// first phase), SWAP and the RETIRE set (in the same batch).
int32_t queueSwap(Writer* w, Partition* p) {
    CompactPlan& c = *p->cplan;
    const char* root = w->eng_root();
    // The partition as the SWAP leaves it.
    ManifestDesc md;
    md.gen = c.gen;
    md.pid = p->pid;
    md.prevGen = p->manifestGen;
    ManifestSegDesc out;
    out.seg = c.seg;
    out.lastSeg = c.inputs.back().lastSeg;
    out.sealed = true;
    out.empty = c.empty;
    out.cgen = c.gen;
    out.firstPseq = c.first;
    out.endPseq = c.end;
    out.mergedEnd = c.end;
    out.dLen = c.dLen;
    out.rLen = c.rLen;
    out.aLen = c.aLen;
    out.minEpoch = c.minEpoch;
    out.maxEpoch = c.maxEpoch;
    out.minArrival = c.minArrival;
    out.maxArrival = c.maxArrival;
    out.killThrough = c.killThrough;
    out.prevGen = p->manifestGen;
    if (!c.empty) {
        ManifestRun mr{};
        mr.gen = c.gen;
        mr.nEntries = c.xEntries;
        mr.fileLen = c.xLen;
        out.runs.push_back(mr);
    }
    bool placed = false;
    for (const auto& s : p->segs) {
        const uint32_t last = s.lastSeg ? s.lastSeg : s.seg;
        if (s.seg >= c.seg && last <= c.segEnd) {
            if (!placed) md.segs.push_back(out);
            placed = true;
            continue;
        }
        if (!s.firstPseq && s.runs.empty()) continue;  // m-only placeholder
        md.segs.push_back(segDesc(s));
    }
    const std::vector<uint8_t> man = encodeManifest(md);
    PathBuf mp;
    pathPartitionManifest(&mp, root, p->pid, c.gen);
    int32_t rc = w->io().open(mp.c_str(), mp.len,
                              kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS,
                              FileClass::Manifest, &c.mf);
    if (rc >= 0) rc = w->io().write(c.mf, man.data(), man.size(), 0);
    if (rc < 0) {
        w->io().close(&c.mf);
        return rc;
    }
    c.mfLen = man.size();
    // What the SWAP retires: the previous manifest, the inputs' files.
    c.retire.clear();
    if (p->manifestGen) c.retire.push_back(retireItem('f', 0, p->manifestGen, ledgerSize(p, 'f', 0, p->manifestGen)));
    for (const auto& in : c.inputs) {
        if (!in.empty) {
            if (in.cgen) {
                for (char L : {'D', 'R', 'A'})
                    c.retire.push_back(retireItem(L, in.seg, in.cgen, ledgerSize(p, L, in.seg, in.cgen)));
            } else {
                for (char L : {'d', 'r', 'a'})
                    c.retire.push_back(retireItem(L, in.seg, 0, ledgerSize(p, L, in.seg, 0)));
            }
        }
        for (const auto& r : in.runs) c.retire.push_back(retireItem('x', in.seg, r.first, r.second));
    }
    uint8_t body[kSwapBytes];
    putU32(body, c.seg);
    putU32(body + 4, c.gen);
    putU32(body + 8, c.inputs.front().cgen);
    putU32(body + 12, c.segEnd);
    queueCtl(p, kCtlSwap, body, sizeof(body));
    p->compactPhase = kCompactSwapQueued;
    p->swapInFlight = c.req;
    p->swapSeg = c.seg;
    p->swapGen = c.gen;
    w->ring();
    return 1;
}

}  // namespace

bool partitionCompactBusy(const Partition* p) { return p->compactPhase != kCompactIdle; }

uint64_t partitionCompactPinNs(const Partition* p) {
    // A plan reads the runs, m segments and files current when it was made:
    // anything retired since stays until it is applied or abandoned.
    return p->cplan && p->compactPhase != kCompactIdle ? p->cplan->createdNs : UINT64_MAX;
}

int32_t partitionCompactStep(Writer* w, Partition* p) {
    Engine* e = w->engine();
    const EngineConfig& cfg = e->config();
    if (p->quarantined) {
        // Requests on a quarantined partition end now (with an error).
        while (!p->swaps.empty()) {
            compactDone(p->swaps.front(), FLATSQL_IO_ERR_ACCESS);
            p->swaps.erase(p->swaps.begin());
        }
        return 0;
    }
    switch (p->compactPhase) {
        case kCompactIdle: {
            // The intent may ride with a merge's; only the SWAP needs no merge in flight.
            if (p->swapInFlight) return 0;
            SwapResult* req = p->swaps.empty() ? nullptr : p->swaps.front();
            if (!req) {
                // Candidates come from state a warm partition already holds:
                // looking never warms an idle one.
                if (!cfg.autoCompact || cfg.cooperative || !p->warm) return 0;
                const uint64_t now = monoNs();
                if (now - p->lastCompactCheckNs < 50000000ull) return 0;  // 50 ms between looks
                p->lastCompactCheckNs = now;
            }
            const int saved = tHotPathDepth;
            tHotPathDepth = 0;
            int32_t rc = partitionWarm(w, p);
            uint32_t seg = 0, segEnd = 0;
            bool have = false;
            if (rc >= 0) {
                if (req && req->requestSeg != UINT32_MAX) {
                    seg = req->requestSeg;
                    segEnd = req->requestSegEnd ? req->requestSegEnd : seg;
                    const SegmentInfo* s = findSegInfo(p, seg);
                    if (s && s->lastSeg > segEnd) segEnd = s->lastSeg;
                    have = s != nullptr;
                } else {
                    have = !req && pickCandidate(e, p, &seg, &segEnd);
                    if (!have && req) {
                        // Requested without a target: the oldest-generation
                        // compactable segment (the T2 seam's rule).
                        const SegmentInfo* cand = nullptr;
                        for (const auto& s : p->segs) {
                            if (!compactable(p, s)) continue;
                            if (!cand || s.cgen < cand->cgen || (s.cgen == cand->cgen && s.seg < cand->seg)) cand = &s;
                        }
                        if (cand) {
                            seg = cand->seg;
                            segEnd = cand->lastSeg ? cand->lastSeg : cand->seg;
                            have = true;
                        }
                    }
                }
            }
            if (req) p->swaps.erase(p->swaps.begin());
            if (rc >= 0 && have) rc = planCompaction(w, p, seg, segEnd, req, req != nullptr);
            else if (rc >= 0) rc = FLATSQL_IO_ERR_NOENT;
            tHotPathDepth = saved;
            if (rc < 0) {
                if (req) compactDone(req, rc);
                return rc == FLATSQL_IO_ERR_NOENT || rc == FLATSQL_IO_ERR_BUSY || rc == FLATSQL_IO_ERR_GENERIC ? 0 : rc;
            }
            return rc;
        }
        case kCompactIntentDurable: {
            std::shared_ptr<CompactPlan> c = p->cplan;
            c->result.store(0, std::memory_order_release);
            p->compactPhase = kCompactBuilding;
            Writer* owner = w;
            const std::string root = w->eng_root();
            if (cfg.cooperative || !cfg.compactThreads) {
                const int saved = tHotPathDepth;
                tHotPathDepth = 0;
                const int32_t rc = buildCompaction(&w->io(), root.c_str(), cfg, *c);
                tHotPathDepth = saved;
                c->result.store(rc < 0 ? rc : 1, std::memory_order_release);
            } else {
                e->submitCompaction([c, owner, root, e](IoCtx* io) {
                    const int32_t rc = buildCompaction(io, root.c_str(), e->config(), *c);
                    c->result.store(rc < 0 ? rc : 1, std::memory_order_release);
                    owner->ring();
                });
            }
            return 1;
        }
        case kCompactBuilding: {
            const int32_t res = p->cplan->result.load(std::memory_order_acquire);
            if (res == 0) return 0;
            if (res < 0 || p->cplan->ownerEpoch != p->ownerEpoch) {
                if (res == kCompactNotWorth) {
                    // A survey: remember what it found so it is not retried.
                    for (auto& s : p->segs) {
                        const uint32_t last = s.lastSeg ? s.lastSeg : s.seg;
                        if (s.seg >= p->cplan->seg && last <= p->cplan->segEnd) {
                            s.deadRows = p->cplan->putsDropped + 1;
                            s.deadBytes = p->cplan->deadFrameBytes;
                        }
                    }
                }
                SwapResult* req = p->cplan->req;
                partitionCompactAbort(w, p);
                if (req) compactDone(req, res < 0 ? res : kCompactNotOwner);
                // A build that failed leaves the inputs as they were: the
                // partition carries on (NOSPACE starts the emergency).
                return res == FLATSQL_IO_ERR_NOSPACE ? res : 0;
            }
            p->compactPhase = kCompactBuilt;
        }
        // fall through
        case kCompactBuilt: {
            if (p->mergePhase != kMergeIdle || p->nPendingCtl || p->swapInFlight) return 0;
            const int saved = tHotPathDepth;
            tHotPathDepth = 0;
            const int32_t rc = queueSwap(w, p);
            tHotPathDepth = saved;
            if (rc < 0) {
                SwapResult* req = p->cplan->req;
                partitionCompactAbort(w, p);
                if (req) compactDone(req, rc);
                return rc;
            }
            return 1;
        }
        default:
            return 0;
    }
}

void partitionCompactIntentDurable(Partition* p) {
    if (p->compactPhase != kCompactIntentQueued || !p->cplan) return;
    p->cIntentSeg = p->cplan->seg;
    p->cIntentGen = p->cplan->gen;
    p->compactPhase = kCompactIntentDurable;
}

void partitionCompactStage(const Partition* p, Counters* ct) {
    if (p->compactPhase != kCompactSwapQueued || !p->cplan) return;
    const CompactPlan& c = *p->cplan;
    ct->totalCount -= std::min(ct->totalCount, c.putsDropped);
    ct->totalBytes -= std::min(ct->totalBytes, c.putBytesDropped);
    ct->tombCount -= std::min(ct->tombCount, c.tombsDropped);
}

const std::vector<RetireItem>* partitionCompactRetiring(const Partition* p) {
    if (p->compactPhase != kCompactSwapQueued || !p->cplan) return nullptr;
    return &p->cplan->retire;
}

const FileRef* partitionCompactManifestFile(const Partition* p) {
    if (p->compactPhase != kCompactSwapQueued || !p->cplan) return nullptr;
    return &p->cplan->mf;
}

void partitionCompactApply(Writer* w, Partition* p) {
    if (p->compactPhase != kCompactSwapQueued || !p->cplan) return;
    std::shared_ptr<CompactPlan> cp = p->cplan;
    CompactPlan& c = *cp;
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    // Replace the inputs by the output.
    SegmentInfo out;
    out.seg = c.seg;
    out.lastSeg = c.inputs.back().lastSeg == c.seg ? 0 : c.inputs.back().lastSeg;
    out.sealed = true;
    out.empty = c.empty;
    out.cgen = c.gen;
    out.firstPseq = c.first;
    out.endPseq = c.end;
    out.mergedEnd = c.end;
    out.dLen = c.dLen;
    out.rLen = c.rLen;
    out.aLen = c.aLen;
    out.minEpoch = c.minEpoch;
    out.maxEpoch = c.maxEpoch;
    out.minArrival = c.minArrival;
    out.maxArrival = c.maxArrival;
    out.killThrough = c.killThrough;
    out.prevGen = p->manifestGen;
    out.cdir = c.cdir;
    out.deadRows = 1;  // known: freshly compacted
    if (!c.empty) out.runs.push_back(std::move(c.run));
    c.run = SegRun();
    std::vector<SegmentInfo> next;
    next.reserve(p->segs.size());
    bool placed = false;
    for (auto& s : p->segs) {
        const uint32_t last = s.lastSeg ? s.lastSeg : s.seg;
        if (s.seg >= c.seg && last <= c.segEnd) {
            w->io().close(&s.r);
            w->io().close(&s.a);
            w->io().close(&s.d);
            for (auto& r : s.runs) w->io().close(&r.file);
            // m handles of retired meta segments are closed by A9.
            if (s.m.valid()) w->io().close(&s.m);
            if (!placed) next.push_back(std::move(out));
            placed = true;
            continue;
        }
        next.push_back(std::move(s));
    }
    p->segs.swap(next);
    // Disk accounting: the new files join the ledger (the retired stay until
    // unlinked, A12).
    if (!c.empty) {
        ledgerSet(p, retireItem('D', c.seg, c.gen, c.dLen));
        ledgerSet(p, retireItem('R', c.seg, c.gen, c.rLen));
        ledgerSet(p, retireItem('A', c.seg, c.gen, c.aLen));
        ledgerSet(p, retireItem('x', c.seg, c.gen, c.xLen));
    }
    ledgerSet(p, retireItem('f', 0, c.gen, c.mfLen));
    w->io().close(&c.mf);
    p->manifestGen = c.gen;
    p->cIntentSeg = 0;
    p->cIntentGen = 0;
    p->compactPhase = kCompactIdle;
    p->swapInFlight = nullptr;
    Engine* e = w->engine();
    e->cCompactions.fetch_add(1, std::memory_order_relaxed);
    e->cCompactInFlight.fetch_sub(1, std::memory_order_relaxed);
    e->cCompactBytesIn.fetch_add(c.bytesIn, std::memory_order_relaxed);
    e->cCompactBytesOut.fetch_add(c.bytesOut, std::memory_order_relaxed);
    if (c.req) {
        SwapResult* r = c.req;
        r->seg = c.seg;
        r->segEnd = c.segEnd;
        r->oldCgen = c.inputs.front().cgen;
        r->gen = c.gen;
        r->rowsIn = c.rowsIn;
        r->rowsKept = c.rowsKept;
        r->putsDropped = c.putsDropped;
        r->tombsDropped = c.tombsDropped;
        r->bytesIn = c.bytesIn;
        r->bytesOut = c.bytesOut;
        r->killThrough = c.killThrough;
        r->buildNs = c.buildNs;
        r->status = 0;  // the batch's ticket releases it
    }
    p->cplan.reset();
    p->lastCompactCheckNs = 0;  // the next candidate is looked for at once
    partitionAccount(p);
    tHotPathDepth = saved;
}

void partitionCompactAbort(Writer* w, Partition* p) {
    if (p->compactPhase == kCompactIdle || !p->cplan) {
        p->compactPhase = kCompactIdle;
        return;
    }
    std::shared_ptr<CompactPlan> c = p->cplan;
    c->abort.store(true, std::memory_order_release);
    if (p->compactPhase == kCompactBuilding)
        while (c->result.load(std::memory_order_acquire) == 0) sleepNs(1000000);
    c->run.run.reset();
    w->io().close(&c->run.file);
    w->io().close(&c->mf);
    if (p->compactPhase >= kCompactIntentDurable) {
        // Outputs are named by a durable intent: unlink them now; open repeats
        // it if any remain (A11).
        unlinkOutputs(&w->io(), w->eng_root(), p->pid, c->seg, c->gen);
        if (!p->metaSinceCkpt) p->metaSinceCkpt = 1;  // a checkpoint head clears the intent
    }
    if (p->compactPhase == kCompactIntentQueued) removePendingCtl(p, kCtlIntentCompact);
    if (p->compactPhase == kCompactSwapQueued) removePendingCtl(p, kCtlSwap);
    p->cIntentSeg = 0;
    p->cIntentGen = 0;
    p->swapInFlight = nullptr;
    p->compactPhase = kCompactIdle;
    p->cplan.reset();
    w->engine()->cCompactAborts.fetch_add(1, std::memory_order_relaxed);
    w->engine()->cCompactInFlight.fetch_sub(1, std::memory_order_relaxed);
}

void partitionCompactSignalAbort(Partition* p) {
    if (p->cplan) p->cplan->abort.store(true, std::memory_order_release);
}

void unlinkCompactOutputs(IoCtx* io, const char* root, uint32_t pid, uint32_t seg, uint32_t gen) {
    unlinkOutputs(io, root, pid, seg, gen);
}

}  // namespace ps
}  // namespace flatsql
