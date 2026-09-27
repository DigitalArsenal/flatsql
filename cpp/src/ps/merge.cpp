// FlatSQL partition store: partition maintenance (design §4.5 merge, §4.2
// seal, A9 segmented meta log, A11 merge intents). Runs between commits on
// the owning writer, never on the per-record path.
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace ps {

namespace {

constexpr int32_t kOpenRW = FLATSQL_IO_READ | FLATSQL_IO_WRITE;

int32_t openSeg(Writer* w, Partition* p, char letter, uint32_t seg, int32_t flags, FileRef* out) {
    PathBuf path;
    const char* ext = letter == 'r' ? "fsr" : letter == 'a' ? "fsa" : letter == 'd' ? "fsd" : "fsl";
    pathPartitionSeg(&path, w->eng_root(), p->pid, letter, seg, ext);
    const FileClass cls = letter == 'r' ? FileClass::Rows : letter == 'a' ? FileClass::Attrs
                          : letter == 'd' ? FileClass::Data : FileClass::Meta;
    return w->io().open(path.c_str(), path.len, flags, cls, out);
}

SegmentInfo* segInfo(Partition* p, uint32_t seg, bool create) {
    for (auto& s : p->segs)
        if (s.seg == seg) return &s;
    if (!create) return nullptr;
    p->segs.emplace_back();
    SegmentInfo& si = p->segs.back();
    si.seg = seg;
    std::sort(p->segs.begin(), p->segs.end(),
              [](const SegmentInfo& a, const SegmentInfo& b) { return a.seg < b.seg; });
    for (auto& s : p->segs)
        if (s.seg == seg) return &s;
    return nullptr;
}

#pragma pack(push, 1)
struct ManifestHeader {
    uint32_t magic;
    uint16_t ver;
    uint16_t nSegs;
    uint32_t gen;
    uint32_t pid;
    uint64_t pad;
};
struct ManifestSeg {
    uint32_t seg;
    uint32_t flags;  // 1 = sealed
    uint64_t firstPseq;
    uint64_t endPseq;
    uint64_t mergedEnd;
    uint64_t dLen;
    uint64_t rLen;
    uint64_t aLen;
    int64_t minEpoch;
    int64_t maxEpoch;
    uint32_t nRuns;
    uint32_t pad;
};
struct ManifestRun {
    uint32_t gen;
    uint32_t pad;
    uint64_t nEntries;
    uint64_t fileLen;
};
#pragma pack(pop)
static_assert(sizeof(ManifestHeader) == 24, "ManifestHeader");
static_assert(sizeof(ManifestSeg) == 80, "ManifestSeg");
static_assert(sizeof(ManifestRun) == 24, "ManifestRun");

std::vector<uint8_t> encodeManifest(const Partition* p, uint32_t gen) {
    std::vector<uint8_t> out(sizeof(ManifestHeader));
    ManifestHeader h{};
    h.magic = kMagicManifest;
    h.ver = 1;
    h.nSegs = uint16_t(p->segs.size());
    h.gen = gen;
    h.pid = p->pid;
    std::memcpy(out.data(), &h, sizeof(h));
    for (const auto& s : p->segs) {
        ManifestSeg ms{};
        ms.seg = s.seg;
        ms.flags = s.sealed ? 1 : 0;
        ms.firstPseq = s.firstPseq;
        ms.endPseq = s.endPseq;
        ms.mergedEnd = s.mergedEnd;
        ms.dLen = s.dLen;
        ms.rLen = s.rLen;
        ms.aLen = s.aLen;
        ms.minEpoch = s.minEpoch;
        ms.maxEpoch = s.maxEpoch;
        ms.nRuns = uint32_t(s.runs.size());
        const uint8_t* b = reinterpret_cast<const uint8_t*>(&ms);
        out.insert(out.end(), b, b + sizeof(ms));
        for (const auto& r : s.runs) {
            ManifestRun mr{};
            mr.gen = r.gen;
            mr.nEntries = r.run ? r.run->entries() : 0;
            mr.fileLen = r.fileLen;
            const uint8_t* rb = reinterpret_cast<const uint8_t*>(&mr);
            out.insert(out.end(), rb, rb + sizeof(mr));
        }
    }
    const uint32_t crc = crc32c(out.data(), out.size());
    const size_t at = out.size();
    out.resize(at + 8, 0);
    putU32(out.data() + at, crc);
    return out;
}

}  // namespace

const char* Writer::eng_root() const { return eng_->root().c_str(); }

int32_t partitionEnsureFiles(Writer* w, Partition* p) {
    int32_t rc;
    if (!p->h.valid()) {
        PathBuf path;
        pathPartition(&path, w->eng_root(), p->pid, "h.fsh");
        rc = w->io().open(path.c_str(), path.len,
                          kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS, FileClass::Head, &p->h);
        if (rc < 0) return rc;
    }
    if (!p->m.valid()) {
        rc = openSeg(w, p, 'm', p->mSeg, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS, &p->m);
        if (rc < 0) return rc;
        const int64_t sz = w->io().size(p->m);
        p->mExtent = sz > 0 ? uint64_t(sz) : 0;
        if (p->mExtent < p->mEnd) p->mExtent = p->mEnd;
    }
    if (!p->d.valid()) {
        rc = openSeg(w, p, 'd', p->dSeg, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS, &p->d);
        if (rc < 0) return rc;
        const int64_t sz = w->io().size(p->d);
        p->dExtent = sz > 0 ? uint64_t(sz) : 0;
        if (p->dExtent < p->dLen) p->dExtent = p->dLen;
    }
    if (!p->l.valid()) {
        PathBuf path;
        pathPartition(&path, w->eng_root(), p->pid, "l.fsl");
        rc = w->io().open(path.c_str(), path.len,
                          kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS, FileClass::Lanes, &p->l);
        if (rc < 0) return rc;
    }
    return 0;
}

int32_t partitionLoadLanes(Writer* w, Partition* p) {
    if (p->lanesLoaded) return 0;
    int32_t rc = partitionEnsureFiles(w, p);
    if (rc < 0) return rc;
    const int64_t size = w->io().size(p->l);
    if (size < 0) return int32_t(size);
    std::vector<uint8_t> buf(static_cast<size_t>(size));
    if (size && w->io().read(p->l, buf.data(), buf.size(), 0) != size) return FLATSQL_IO_ERR_IO;
    // Counters known at open (head + adopted deltas), by lane id.
    std::unordered_map<uint32_t, LaneCounter> known;
    for (const auto& l : p->lanes) known[l.id] = l.c;
    p->lanes.clear();
    p->laneByHash.clear();
    uint64_t off = 0;
    uint32_t maxId = 0;
    while (off + 8 <= buf.size()) {
        const uint32_t len = getU32(buf.data() + off);
        if (len < 14 || off + 8 + len > buf.size()) break;
        if (crc32c(buf.data() + off + 8, len) != getU32(buf.data() + off + 4)) break;
        const uint8_t* q = buf.data() + off + 8;
        const uint8_t* end = q + len;
        Lane l;
        l.id = getU32(q);
        q += 4;
        std::string* fields[5] = {&l.provider, &l.source, &l.batch, &l.peer, &l.pubkey};
        bool ok = true;
        for (auto* f : fields) {
            if (q + 2 > end) {
                ok = false;
                break;
            }
            const uint16_t n = getU16(q);
            if (q + 2 + n > end) {
                ok = false;
                break;
            }
            f->assign(reinterpret_cast<const char*>(q + 2), n);
            q += 2 + n;
        }
        if (!ok) break;
        auto it = known.find(l.id);
        if (it != known.end()) l.c = it->second;
        l.c.laneId = l.id;
        if (l.id > maxId) maxId = l.id;
        TagView t;
        t.present = true;
        t.provider = reinterpret_cast<const uint8_t*>(l.provider.data());
        t.providerLen = l.provider.size();
        t.source = reinterpret_cast<const uint8_t*>(l.source.data());
        t.sourceLen = l.source.size();
        t.batch = reinterpret_cast<const uint8_t*>(l.batch.data());
        t.batchLen = l.batch.size();
        t.peer = reinterpret_cast<const uint8_t*>(l.peer.data());
        t.peerLen = l.peer.size();
        t.pubkey = reinterpret_cast<const uint8_t*>(l.pubkey.data());
        t.pubkeyLen = l.pubkey.size();
        const uint64_t h = laneHash(t);
        p->lanes.push_back(std::move(l));
        p->laneByHash.emplace(h, uint32_t(p->lanes.size() - 1));
        off += 8 + len;
    }
    p->lExtent = off;
    if (maxId + 1 > p->nextLaneId) p->nextLaneId = maxId + 1;
    p->lanesLoaded = true;
    return 0;
}

int32_t partitionWarm(Writer* w, Partition* p) {
    if (p->warm) return 0;
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    int32_t rc = partitionEnsureFiles(w, p);
    if (rc >= 0) rc = partitionLoadLanes(w, p);
    // Manifest: sealed/merged segments and their L1 runs.
    if (rc >= 0 && p->manifestGen && p->segs.empty()) {
        PathBuf path;
        pathPartitionManifest(&path, w->eng_root(), p->pid, p->manifestGen);
        FileRef f;
        rc = w->io().open(path.c_str(), path.len, FLATSQL_IO_READ, FileClass::Manifest, &f);
        if (rc >= 0) {
            const int64_t size = w->io().size(f);
            std::vector<uint8_t> buf(size > 0 ? size_t(size) : 0);
            if (size < int64_t(sizeof(ManifestHeader) + 8) ||
                w->io().read(f, buf.data(), buf.size(), 0) != size) {
                rc = FLATSQL_IO_ERR_IO;
            } else {
                const size_t body = buf.size() - 8;
                if (crc32c(buf.data(), body) != getU32(buf.data() + body)) rc = FLATSQL_IO_ERR_IO;
                ManifestHeader h;
                std::memcpy(&h, buf.data(), sizeof(h));
                size_t off = sizeof(h);
                for (uint16_t i = 0; rc >= 0 && i < h.nSegs; i++) {
                    if (off + sizeof(ManifestSeg) > body) {
                        rc = FLATSQL_IO_ERR_IO;
                        break;
                    }
                    ManifestSeg ms;
                    std::memcpy(&ms, buf.data() + off, sizeof(ms));
                    off += sizeof(ms);
                    SegmentInfo si;
                    si.seg = ms.seg;
                    si.sealed = ms.flags & 1;
                    si.firstPseq = ms.firstPseq;
                    si.endPseq = ms.endPseq;
                    si.mergedEnd = ms.mergedEnd;
                    si.dLen = ms.dLen;
                    si.rLen = ms.rLen;
                    si.aLen = ms.aLen;
                    si.minEpoch = ms.minEpoch;
                    si.maxEpoch = ms.maxEpoch;
                    for (uint32_t r = 0; r < ms.nRuns; r++) {
                        if (off + sizeof(ManifestRun) > body) {
                            rc = FLATSQL_IO_ERR_IO;
                            break;
                        }
                        ManifestRun mr;
                        std::memcpy(&mr, buf.data() + off, sizeof(mr));
                        off += sizeof(mr);
                        SegRun run;
                        run.gen = mr.gen;
                        run.fileLen = mr.fileLen;
                        si.runs.push_back(std::move(run));
                    }
                    p->segs.push_back(std::move(si));
                }
            }
            w->io().close(&f);
        }
        for (auto& si : p->segs) {
            for (auto& run : si.runs) {
                if (rc < 0) break;
                PathBuf rp;
                pathPartitionRun(&rp, w->eng_root(), p->pid, si.seg, run.gen);
                rc = w->io().open(rp.c_str(), rp.len, FLATSQL_IO_READ, FileClass::Index, &run.file);
                if (rc < 0) break;
                run.run.reset(new L1Run());
                rc = run.run->load(&w->io(), run.file, run.fileLen);
            }
        }
    } else if (rc >= 0) {
        for (auto& si : p->segs) {
            for (auto& run : si.runs) {
                if (rc < 0 || run.run) continue;
                PathBuf rp;
                pathPartitionRun(&rp, w->eng_root(), p->pid, si.seg, run.gen);
                if (!run.file.valid())
                    rc = w->io().open(rp.c_str(), rp.len, FLATSQL_IO_READ, FileClass::Index, &run.file);
                if (rc < 0) break;
                run.run.reset(new L1Run());
                rc = run.run->load(&w->io(), run.file, run.fileLen);
            }
        }
    }
    // Accelerators for the unmerged L0 blocks (read from m; never from d).
    if (rc >= 0 && !p->acc) p->acc.reset(new L0Accel[kMaxL0Dir]);
    for (uint32_t i = 0; rc >= 0 && i < p->nL0; i++) {
        L0Accel& a = p->acc[i];
        const L0DirEntry& e = p->l0[i];
        a.mSeg = e.mSeg;
        a.mOff = e.mOff;
        a.firstPseq = e.firstPseq;
        a.nRows = e.nRows;
        a.batchLen = e.batchLen;
        a.l0Off = e.mOff + e.l0Off;
        a.chainPos = 0;
        a.nKinds = 0;
        FileRef* f = &p->m;
        FileRef tmp;
        if (e.mSeg != p->mSeg) {
            rc = openSeg(w, p, 'm', e.mSeg, FLATSQL_IO_READ, &tmp);
            if (rc < 0) break;
            f = &tmp;
        }
        L0Header lh;
        if (w->io().read(*f, &lh, sizeof(lh), a.l0Off) != int64_t(sizeof(lh)) || lh.magic != kMagicL0) {
            rc = FLATSQL_IO_ERR_IO;
        } else {
            std::vector<uint8_t> block(lh.blockLen);
            if (w->io().read(*f, block.data(), block.size(), a.l0Off) != int64_t(block.size())) {
                rc = FLATSQL_IO_ERR_IO;
            } else {
                a.l0Len = lh.blockLen;
                rc = loadAccelFromBlock(p, w->engine()->pool(), i, block.data(), lh.blockLen);
            }
        }
        if (tmp.valid()) w->io().close(&tmp);
    }
    tHotPathDepth = saved;
    if (rc < 0) return rc;
    p->warm = true;
    return 0;
}

void partitionCool(Writer* w, Partition* p) {
    // Idle partitions hold no pool slabs and no handles (§17, T1 #9).
    p->chain.freeAll(w->engine()->pool());
    p->acc.reset();
    for (auto& si : p->segs) {
        w->io().close(&si.r);
        w->io().close(&si.a);
        w->io().close(&si.d);
        w->io().close(&si.m);
        for (auto& run : si.runs) {
            w->io().close(&run.file);
            run.run.reset();
        }
    }
    w->io().close(&p->d);
    w->io().close(&p->m);
    w->io().close(&p->l);
    w->io().close(&p->rA);
    w->io().close(&p->aA);
    // The head handle stays: it is written on every commit, and closing it
    // would force a reopen on the next one.
    p->warm = false;
}

bool partitionWantsMerge(const Engine* e, const Partition* p) {
    if (p->nL0 == 0 || p->quarantined) return false;
    const L0DirEntry& first = p->l0[0];
    const uint64_t firstLast = first.firstPseq + first.nRows - 1;
    if (firstLast > p->labeledThrough.load(std::memory_order_acquire)) return false;
    if (first.mSeg != p->mSeg) return true;  // a sealed segment still has L0 blocks
    if (p->nL0 >= e->config().mergeL0Blocks) return true;
    uint64_t bytes = 0;
    for (uint32_t i = 0; i < p->nL0; i++) bytes += p->l0[i].batchLen;
    return bytes >= e->config().mergeL0Bytes;
}

// ---------------------------------------------------------------------------
// Merge pipeline (§4.5, A11). No sync of its own: INTENT_MERGE rides a batch,
// outputs are written between rounds and synced in the next round's first
// sync phase, MERGE_DONE rides that round's batch (second sync phase).
// ---------------------------------------------------------------------------
namespace {

void queueCtl(Partition* p, uint16_t kind, const uint8_t* body, uint16_t len) {
    if (p->pendingCtlBytes + 4 + len > sizeof(p->pendingCtl)) return;
    putU16(p->pendingCtl + p->pendingCtlBytes, kind);
    putU16(p->pendingCtl + p->pendingCtlBytes + 2, len);
    std::memcpy(p->pendingCtl + p->pendingCtlBytes + 4, body, len);
    p->pendingCtlBytes += 4 + len;
    p->nPendingCtl++;
}

int32_t planMerge(Writer* w, Partition* p) {
    const uint64_t labeled = p->labeledThrough.load(std::memory_order_acquire);
    const uint32_t seg = p->l0[0].mSeg;
    uint32_t k = 0;
    while (k < p->nL0 && p->l0[k].mSeg == seg && p->l0[k].firstPseq + p->l0[k].nRows - 1 <= labeled) k++;
    if (k == 0) return 0;
    MergePlan& m = p->mplan;
    m = MergePlan();
    m.seg = seg;
    m.k = k;
    m.firstPseq = p->l0[0].firstPseq;
    m.through = p->l0[k - 1].firstPseq + p->l0[k - 1].nRows - 1;
    SegmentInfo* si = segInfo(p, seg, true);
    if (si->firstPseq == 0) {
        si->firstPseq = (seg == p->dSeg) ? p->segFirstPseq : m.firstPseq;
        si->mergedEnd = si->firstPseq;
    }
    m.gen = p->nextGen++;
    m.rOff = (m.firstPseq - si->firstPseq) * sizeof(RecRow);
    m.aOff = si->aLen;
    uint8_t intent[40];
    putU32(intent, seg);
    putU32(intent + 4, m.gen);
    putU64(intent + 8, m.rOff);
    putU64(intent + 16, m.aOff);
    putU64(intent + 24, m.through);
    putU64(intent + 32, m.firstPseq);
    queueCtl(p, kCtlIntentMerge, intent, sizeof(intent));
    p->mergePhase = kMergeIntentQueued;
    (void)w;
    return 0;
}

int32_t writeMergeOutputs(Writer* w, Partition* p) {
    MergePlan& m = p->mplan;
    int32_t rc = 0;
    FileRef* mf = &p->m;
    FileRef mTmp;
    if (m.seg != p->mSeg) {
        rc = openSeg(w, p, 'm', m.seg, FLATSQL_IO_READ, &mTmp);
        if (rc < 0) return rc;
        mf = &mTmp;
    }
    SegmentInfo* si = segInfo(p, m.seg, true);
    std::vector<uint8_t> rbuf, abuf;
    std::vector<std::vector<uint8_t>> blocks(m.k);
    m.minEpoch = si->minEpoch;
    m.maxEpoch = si->maxEpoch;
    for (uint32_t i = 0; i < m.k && rc >= 0; i++) {
        const L0DirEntry& de = p->l0[i];
        std::vector<uint8_t> b(de.batchLen);
        if (w->io().read(*mf, b.data(), b.size(), de.mOff) != int64_t(b.size())) {
            rc = FLATSQL_IO_ERR_IO;
            break;
        }
        BatchHeader bh;
        std::memcpy(&bh, b.data(), sizeof(bh));
        const uint64_t attrBase = de.mOff + sizeof(BatchHeader) + uint64_t(bh.nRows) * sizeof(RecRow);
        const uint64_t aShift = m.aOff + abuf.size();
        for (uint32_t r = 0; r < bh.nRows; r++) {
            RecRow row;
            std::memcpy(&row, b.data() + sizeof(BatchHeader) + size_t(r) * sizeof(RecRow), sizeof(row));
            if (row.flags & kRowAttrInM) {
                row.attrOff = aShift + (row.attrOff - attrBase);
                row.flags &= uint8_t(~kRowAttrInM);
            }
            if (row.kind == kRowPut) {
                if (row.epochMs < m.minEpoch) m.minEpoch = row.epochMs;
                if (row.epochMs > m.maxEpoch) m.maxEpoch = row.epochMs;
            }
            const uint8_t* rb = reinterpret_cast<const uint8_t*>(&row);
            rbuf.insert(rbuf.end(), rb, rb + sizeof(row));
        }
        const uint8_t* ab = b.data() + sizeof(BatchHeader) + size_t(bh.nRows) * sizeof(RecRow);
        abuf.insert(abuf.end(), ab, ab + bh.attrBytes);
        blocks[i].assign(b.begin() + de.l0Off, b.begin() + de.l0Off + bh.l0Bytes);
    }
    if (mTmp.valid()) w->io().close(&mTmp);
    if (rc < 0) return rc;
    // Rows and attributes at offsets derived from the plan: a redo is
    // idempotent (A11).
    rc = openSeg(w, p, 'r', m.seg, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS, &m.r);
    if (rc >= 0) rc = openSeg(w, p, 'a', m.seg, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS, &m.a);
    if (rc >= 0 && !rbuf.empty()) rc = w->io().write(m.r, rbuf.data(), rbuf.size(), m.rOff);
    if (rc >= 0 && !abuf.empty()) rc = w->io().write(m.a, abuf.data(), abuf.size(), m.aOff);
    m.rLen = m.rOff + rbuf.size();
    m.aLen = m.aOff + abuf.size();
    // L1 run over the merged L0 blocks (fresh file, A11), folding the newest
    // runs of the segment while each is at most twice what is being merged:
    // O(log n) runs per segment, O(n log n) total rewriting. Folded run files
    // stay on disk until reclamation (T3); readers holding an older manifest
    // keep reading them.
    uint64_t newEntries = 0;
    for (uint32_t i = 0; i < m.k; i++) {
        L0KindInfo ki[L0Accel::kMaxKinds];
        size_t nk = 0;
        if (parseL0Block(blocks[i].data(), blocks[i].size(), ki, L0Accel::kMaxKinds, &nk))
            for (size_t j = 0; j < nk; j++) newEntries += ki[j].n;
    }
    // Tiered with fanout 4: fold only when at least three similar-sized
    // tail runs exist (write amplification ~log4 of the segment's runs).
    m.fold = 0;
    uint64_t acc = newEntries;
    uint32_t cand = 0;
    for (size_t i = si->runs.size(); i-- > 0;) {
        const SegRun& r = si->runs[i];
        if (!r.run || r.run->entries() > 2 * acc) break;
        acc += r.run->entries();
        cand++;
    }
    if (cand >= 3) m.fold = cand;
    PathBuf xp;
    pathPartitionRun(&xp, w->eng_root(), p->pid, m.seg, m.gen);
    if (rc >= 0)
        rc = w->io().open(xp.c_str(), xp.len,
                          kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS,
                          FileClass::Index, &m.run.file);
    int64_t xLen = 0;
    if (rc >= 0) {
        std::vector<MergeL0Input> l0s;
        for (uint32_t i = 0; i < m.k; i++) l0s.push_back({blocks[i].data(), blocks[i].size()});
        std::vector<MergeRunInput> folded;
        for (size_t i = si->runs.size() - m.fold; i < si->runs.size(); i++)
            folded.push_back({si->runs[i].run.get(), si->runs[i].file});
        const uint64_t first = m.fold ? si->firstPseq : m.firstPseq;
        uint64_t n = 0;
        xLen = mergeToL1(&w->io(), m.run.file, m.seg, m.gen, uint16_t(m.fold ? 1 : 0), first, m.through, l0s,
                         folded, &n);
        if (xLen < 0) rc = int32_t(xLen);
    }
    m.run.gen = m.gen;
    m.run.fileLen = uint64_t(xLen > 0 ? xLen : 0);
    // Manifest describing the segments as MERGE_DONE will leave them.
    if (rc >= 0) {
        SegmentInfo saved;
        saved.mergedEnd = si->mergedEnd;
        saved.rLen = si->rLen;
        saved.aLen = si->aLen;
        saved.minEpoch = si->minEpoch;
        saved.maxEpoch = si->maxEpoch;
        si->mergedEnd = m.through + 1;
        si->rLen = m.rLen;
        si->aLen = m.aLen;
        si->minEpoch = m.minEpoch;
        si->maxEpoch = m.maxEpoch;
        // Temporarily present the post-merge run list to the encoder.
        std::vector<SegRun> kept;
        for (size_t i = si->runs.size() - m.fold; i < si->runs.size(); i++) kept.push_back(std::move(si->runs[i]));
        si->runs.resize(si->runs.size() - m.fold);
        SegRun probe;
        probe.gen = m.gen;
        probe.fileLen = m.run.fileLen;
        si->runs.push_back(std::move(probe));
        std::vector<uint8_t> man = encodeManifest(p, m.gen);
        si->runs.pop_back();
        for (auto& r : kept) si->runs.push_back(std::move(r));
        si->mergedEnd = saved.mergedEnd;
        si->rLen = saved.rLen;
        si->aLen = saved.aLen;
        si->minEpoch = saved.minEpoch;
        si->maxEpoch = saved.maxEpoch;
        PathBuf mp;
        pathPartitionManifest(&mp, w->eng_root(), p->pid, m.gen);
        rc = w->io().open(mp.c_str(), mp.len,
                          kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS,
                          FileClass::Manifest, &m.mf);
        if (rc >= 0) rc = w->io().write(m.mf, man.data(), man.size(), 0);
    }
    // Accelerators of the new run, loaded now so MERGE_DONE applies without
    // allocating on the commit path.
    if (rc >= 0) {
        m.run.run.reset(new L1Run());
        rc = m.run.run->load(&w->io(), m.run.file, m.run.fileLen);
        si->runs.reserve(si->runs.size() + 1);
    }
    return rc;
}

}  // namespace

void partitionMergeAbort(Writer* w, Partition* p) {
    // A26: HANDOFF (or a failure) aborts an in-flight merge; its outputs are
    // discarded exactly as open discards an INTENT without MERGE_DONE.
    if (p->mergePhase == kMergeIdle) return;
    MergePlan& m = p->mplan;
    w->io().close(&m.r);
    w->io().close(&m.a);
    w->io().close(&m.mf);
    w->io().close(&m.run.file);
    m.run.run.reset();
    if (p->mergePhase >= kMergeIntentDurable) {
        PathBuf xp, mfp, rp, ap;
        pathPartitionRun(&xp, w->eng_root(), p->pid, m.seg, m.gen);
        pathPartitionManifest(&mfp, w->eng_root(), p->pid, m.gen);
        // Closed above; a BUSY (another holder) leaves an orphan for open to
        // discard with the INTENT.
        w->io().unlink(xp.c_str(), xp.len, true);
        w->io().unlink(mfp.c_str(), mfp.len, true);
        FileRef rf, af;
        if (openSeg(w, p, 'r', m.seg, kOpenRW, &rf) == 0) {
            if (w->io().size(rf) > int64_t(m.rOff)) w->io().truncate(rf, m.rOff);
            w->io().close(&rf);
        }
        if (openSeg(w, p, 'a', m.seg, kOpenRW, &af) == 0) {
            if (w->io().size(af) > int64_t(m.aOff)) w->io().truncate(af, m.aOff);
            w->io().close(&af);
        }
    }
    p->nPendingCtl = 0;
    p->pendingCtlBytes = 0;
    p->intentGen = 0;
    p->intentSeg = 0;
    p->mergePhase = kMergeIdle;
}

int32_t partitionMergeStep(Writer* w, Partition* p) {
    Engine* e = w->engine();
    if (p->quarantined) return 0;
    if (p->mergePhase == kMergeIdle) {
        if (!partitionWantsMerge(e, p)) return 0;
        const int32_t rc = partitionWarm(w, p);
        if (rc < 0) return rc;
        return planMerge(w, p);
    }
    if (p->mergePhase == kMergeIntentDurable) {
        const int32_t rc = writeMergeOutputs(w, p);
        if (rc < 0) {
            partitionMergeAbort(w, p);
            return rc;
        }
        p->mergePhase = kMergeOutputsWritten;
        return 1;
    }
    return 0;
}

// MERGE_DONE durable: attach the run, advance merged_through, drop the
// merged L0 blocks and their accelerators. Allocation-free.
void partitionMergeApply(Writer* w, Partition* p) {
    MergePlan& m = p->mplan;
    SegmentInfo* si = nullptr;
    for (auto& s : p->segs)
        if (s.seg == m.seg) si = &s;
    if (!si) return;
    si->mergedEnd = m.through + 1;
    si->rLen = m.rLen;
    si->aLen = m.aLen;
    si->minEpoch = m.minEpoch;
    si->maxEpoch = m.maxEpoch;
    for (uint32_t i = 0; i < m.fold && !si->runs.empty(); i++) {
        w->io().close(&si->runs.back().file);
        si->runs.pop_back();  // unique_ptr release: maintenance-sized, not per record
    }
    si->runs.push_back(std::move(m.run));
    m.run = SegRun();
    w->io().close(&m.r);
    w->io().close(&m.a);
    w->io().close(&m.mf);
    p->mergedThrough = m.through;
    p->manifestGen = m.gen;
    p->intentGen = 0;
    p->intentSeg = 0;
    const uint32_t k = m.k;
    const uint32_t remaining = p->nL0 - k;
    for (uint32_t i = 0; i < remaining; i++) {
        p->l0[i] = p->l0[i + k];
        if (p->acc) p->acc[i] = p->acc[i + k];
    }
    p->nL0 = remaining;
    if (p->acc && remaining) {
        uint64_t minPos = UINT64_MAX;
        for (uint32_t i = 0; i < remaining; i++)
            if (p->acc[i].chainPos && p->acc[i].chainPos < minPos) minPos = p->acc[i].chainPos;
        if (minPos != UINT64_MAX) p->chain.freeBefore(w->engine()->pool(), minPos);
    } else {
        p->chain.freeAll(w->engine()->pool());
    }
    p->mergePhase = kMergeIdle;
    w->engine()->cMerges.fetch_add(1, std::memory_order_relaxed);
}

void mergeDoneBody(const Partition* p, uint8_t body[40]) {
    const MergePlan& m = p->mplan;
    putU32(body, m.seg);
    putU32(body + 4, m.gen);
    putU64(body + 8, m.through);
    putU64(body + 16, m.rLen);
    putU64(body + 24, m.aLen);
    putU32(body + 32, m.gen);
    putU32(body + 36, 0);
}

int32_t partitionPrecreate(Writer* w, Partition* p) {
    // §4.2: create the next segment's files at 50% so a seal never waits for
    // a directory entry. Deterministic names: a crash leaves no unnamed orphan.
    if (p->precreated) return 0;
    FileRef d, m;
    int32_t rc = openSeg(w, p, 'd', p->nextSeg,
                         kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS, &d);
    if (rc < 0) return rc;
    rc = openSeg(w, p, 'm', p->nextSeg,
                 kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS, &m);
    w->io().close(&d);
    w->io().close(&m);
    if (rc < 0) return rc;
    p->precreated = true;
    return 0;
}

}  // namespace ps
}  // namespace flatsql
