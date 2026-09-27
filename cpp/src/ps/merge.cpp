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

// Writes a ctl-only batch (merge intents, merge completion) and makes it
// durable. The head follows without a sync; open scans past it.
int32_t writeCtlBatch(Writer* w, Partition* p, uint16_t kind, const void* body, uint16_t len) {
    const size_t ctlLen = pad8(4 + 4 + len);
    const size_t deltasLen = pad8(4);
    const size_t batchLen = sizeof(BatchHeader) + deltasLen + ctlLen + sizeof(BatchTrailer);
    std::vector<uint8_t> b(batchLen, 0);
    BatchHeader hdr{};
    hdr.magic = kMagicBatch;
    hdr.ver = 1;
    hdr.commitSeq = p->commitSeq + 1;
    hdr.firstPseq = p->pseqHi + 1;
    hdr.dSeg = p->dSeg;
    hdr.batchLen = uint32_t(batchLen);
    hdr.dOff = p->dLen;
    std::memcpy(b.data(), &hdr, sizeof(hdr));
    size_t off = sizeof(BatchHeader);
    putU32(b.data() + off, 0);  // no lane deltas
    off += deltasLen;
    putU32(b.data() + off, 1);
    putU16(b.data() + off + 4, kind);
    putU16(b.data() + off + 6, len);
    std::memcpy(b.data() + off + 8, body, len);
    off += ctlLen;
    BatchTrailer tr{};
    tr.magic = kMagicTrailer;
    tr.batchLen = uint32_t(batchLen);
    tr.commitSeq = hdr.commitSeq;
    tr.pseqHi = p->pseqHi;
    tr.dCommitted = p->dLen;
    tr.activeSeg = p->dSeg;
    tr.ownerEpoch = p->ownerEpoch;
    tr.counters = p->counters;
    tr.incarnation = w->engine()->incarnation();
    tr.writerId = w->id();
    std::memcpy(b.data() + off, &tr, sizeof(tr));
    putU32(b.data() + off + offsetof(BatchTrailer, crc), crc32c(b.data(), off + offsetof(BatchTrailer, crc)));
    int32_t rc = ensureExtent(&w->io(), p->m, &p->mExtent, p->mEnd + batchLen,
                              w->engine()->config().zeroFillStep);
    if (rc < 0) return rc;
    rc = w->io().write(p->m, b.data(), batchLen, p->mEnd);
    if (rc < 0) return rc;
    rc = w->io().sync(p->m);
    if (rc < 0) return rc;
    p->commitSeq++;
    p->mEnd += batchLen;
    p->incarnation = w->engine()->incarnation();
    p->metaSinceCkpt += batchLen;
    p->durableCommitSeq.store(p->commitSeq, std::memory_order_release);
    p->durableMEnd.store(p->mEnd, std::memory_order_release);
    return 0;
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
    for (uint32_t i = 0; i < p->nL0; i++) p->acc[i].nKinds = 0;
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

int32_t partitionMerge(Writer* w, Partition* p) {
    Engine* e = w->engine();
    if (!partitionWantsMerge(e, p)) return 0;
    int32_t rc = partitionWarm(w, p);
    if (rc < 0) return rc;
    const uint64_t labeled = p->labeledThrough.load(std::memory_order_acquire);
    const uint32_t seg = p->l0[0].mSeg;
    uint32_t k = 0;
    while (k < p->nL0 && p->l0[k].mSeg == seg &&
           p->l0[k].firstPseq + p->l0[k].nRows - 1 <= labeled)
        k++;
    if (k == 0) return 0;
    const uint64_t firstPseq = p->l0[0].firstPseq;
    const uint64_t through = p->l0[k - 1].firstPseq + p->l0[k - 1].nRows - 1;
    SegmentInfo* si = segInfo(p, seg, true);
    if (si->firstPseq == 0) {
        si->firstPseq = (seg == p->dSeg) ? p->segFirstPseq : firstPseq;
        si->mergedEnd = si->firstPseq;
    }
    const uint32_t gen = p->nextGen++;
    const uint64_t rOff = (firstPseq - si->firstPseq) * sizeof(RecRow);
    const uint64_t aOff = si->aLen;
    // 1. INTENT_MERGE, durable before any output exists (A11).
    uint8_t intent[40];
    putU32(intent, seg);
    putU32(intent + 4, gen);
    putU64(intent + 8, rOff);
    putU64(intent + 16, aOff);
    putU64(intent + 24, through);
    putU64(intent + 32, firstPseq);
    p->intentSeg = seg;
    p->intentGen = gen;
    p->intentROff = rOff;
    p->intentAOff = aOff;
    p->intentThrough = through;
    rc = writeCtlBatch(w, p, kCtlIntentMerge, intent, sizeof(intent));
    if (rc < 0) return rc;
    rc = partitionWriteHead(w, p, false);
    if (rc < 0) return rc;
    // 2. Outputs.
    FileRef* mf = &p->m;
    FileRef mTmp;
    if (seg != p->mSeg) {
        rc = openSeg(w, p, 'm', seg, FLATSQL_IO_READ, &mTmp);
        if (rc < 0) return rc;
        mf = &mTmp;
    }
    std::vector<uint8_t> rbuf, abuf;
    std::vector<std::vector<uint8_t>> blocks(k);
    int64_t minEpoch = si->minEpoch, maxEpoch = si->maxEpoch;
    for (uint32_t i = 0; i < k && rc >= 0; i++) {
        const L0DirEntry& de = p->l0[i];
        std::vector<uint8_t> b(de.batchLen);
        if (w->io().read(*mf, b.data(), b.size(), de.mOff) != int64_t(b.size())) {
            rc = FLATSQL_IO_ERR_IO;
            break;
        }
        BatchHeader bh;
        std::memcpy(&bh, b.data(), sizeof(bh));
        const uint64_t attrBase = de.mOff + sizeof(BatchHeader) + uint64_t(bh.nRows) * sizeof(RecRow);
        const uint64_t aShift = aOff + abuf.size();
        for (uint32_t r = 0; r < bh.nRows; r++) {
            RecRow row;
            std::memcpy(&row, b.data() + sizeof(BatchHeader) + size_t(r) * sizeof(RecRow), sizeof(row));
            if (row.flags & kRowAttrInM) {
                row.attrOff = aShift + (row.attrOff - attrBase);
                row.flags &= uint8_t(~kRowAttrInM);
            }
            if (row.kind == kRowPut) {
                if (row.epochMs < minEpoch) minEpoch = row.epochMs;
                if (row.epochMs > maxEpoch) maxEpoch = row.epochMs;
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
    if (!p->rA.valid() || p->rA.cls != FileClass::Rows) {
        w->io().close(&p->rA);
        w->io().close(&p->aA);
    }
    FileRef rf, af, xf, mff;
    rc = openSeg(w, p, 'r', seg, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS, &rf);
    if (rc >= 0) rc = openSeg(w, p, 'a', seg, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS, &af);
    if (rc >= 0 && !rbuf.empty()) rc = w->io().write(rf, rbuf.data(), rbuf.size(), rOff);
    if (rc >= 0 && !abuf.empty()) rc = w->io().write(af, abuf.data(), abuf.size(), aOff);
    // L1 run over the merged L0 blocks, kind by kind.
    PathBuf xp;
    pathPartitionRun(&xp, w->eng_root(), p->pid, seg, gen);
    if (rc >= 0)
        rc = w->io().open(xp.c_str(), xp.len,
                          kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS,
                          FileClass::Index, &xf);
    int64_t xLen = 0;
    uint64_t nEntries = 0;
    if (rc >= 0) {
        struct Ent {
            const uint8_t* key;
            uint16_t klen;
            const uint8_t* val;
        };
        std::vector<std::vector<L0KindInfo>> infos(k);
        std::vector<uint16_t> kinds;
        for (uint32_t i = 0; i < k; i++) {
            L0KindInfo ki[L0Accel::kMaxKinds];
            size_t nk = 0;
            if (!parseL0Block(blocks[i].data(), blocks[i].size(), ki, L0Accel::kMaxKinds, &nk)) {
                rc = FLATSQL_IO_ERR_IO;
                break;
            }
            infos[i].assign(ki, ki + nk);
            for (size_t j = 0; j < nk; j++) kinds.push_back(ki[j].kind);
        }
        std::sort(kinds.begin(), kinds.end());
        kinds.erase(std::unique(kinds.begin(), kinds.end()), kinds.end());
        L1Writer lw(&w->io(), xf, seg, gen, 0, firstPseq, through);
        std::vector<Ent> ents;
        for (uint16_t kind : kinds) {
            if (rc < 0) break;
            ents.clear();
            uint8_t vlen = valueLenOf(kind);
            for (uint32_t i = 0; i < k; i++) {
                for (const auto& ki : infos[i]) {
                    if (ki.kind != kind) continue;
                    vlen = ki.vlen;
                    EntryIter it;
                    it.p = blocks[i].data() + ki.entriesOff;
                    it.end = it.p + ki.entriesBytes;
                    it.vlen = ki.vlen;
                    const uint8_t *ek, *ev;
                    uint16_t el;
                    while (it.next(&ek, &el, &ev)) ents.push_back({ek, el, ev});
                }
            }
            std::sort(ents.begin(), ents.end(), [vlen](const Ent& a, const Ent& b) {
                const int c = keyCmp(a.key, a.klen, b.key, b.klen);
                if (c) return c < 0;
                return std::memcmp(a.val, b.val, vlen) < 0;
            });
            rc = lw.beginKind(kind, ents.size());
            for (const auto& en : ents) {
                if (rc < 0) break;
                rc = lw.add(en.key, en.klen, en.val);
            }
            if (rc >= 0) rc = lw.endKind();
        }
        if (rc >= 0) {
            xLen = lw.finish();
            if (xLen < 0) rc = int32_t(xLen);
            nEntries = lw.entries();
        }
    }
    // Manifest: every segment, with this merge applied.
    SegmentInfo updated = std::move(*si);
    SegRun newRun;
    newRun.gen = gen;
    newRun.fileLen = uint64_t(xLen);
    const uint32_t newManifestGen = gen;
    if (rc >= 0) {
        // Stage the manifest content with the merge applied (restored below
        // if anything fails before MERGE_DONE).
        updated.mergedEnd = through + 1;
        updated.rLen = rOff + rbuf.size();
        updated.aLen = aOff + abuf.size();
        updated.minEpoch = minEpoch;
        updated.maxEpoch = maxEpoch;
        updated.runs.push_back(std::move(newRun));
    }
    *si = std::move(updated);
    if (rc >= 0) {
        std::vector<uint8_t> man = encodeManifest(p, newManifestGen);
        PathBuf mp;
        pathPartitionManifest(&mp, w->eng_root(), p->pid, newManifestGen);
        rc = w->io().open(mp.c_str(), mp.len,
                          kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS,
                          FileClass::Manifest, &mff);
        if (rc >= 0) rc = w->io().write(mff, man.data(), man.size(), 0);
    }
    if (rc >= 0) rc = w->io().sync(rf);
    if (rc >= 0) rc = w->io().sync(af);
    if (rc >= 0) rc = w->io().sync(xf);
    if (rc >= 0) rc = w->io().sync(mff);
    w->io().close(&rf);
    w->io().close(&af);
    w->io().close(&mff);
    if (rc < 0) {
        w->io().close(&xf);
        // Leave the intent outstanding: open (or the next merge) discards the
        // outputs of an INTENT without MERGE_DONE.
        if (!si->runs.empty() && si->runs.back().gen == gen) si->runs.pop_back();
        si->mergedEnd = firstPseq;
        return rc;
    }
    // 3. MERGE_DONE.
    uint8_t done[40];
    putU32(done, seg);
    putU32(done + 4, gen);
    putU64(done + 8, through);
    putU64(done + 16, si->rLen);
    putU64(done + 24, si->aLen);
    putU32(done + 32, newManifestGen);
    putU32(done + 36, 0);
    p->mergedThrough = through;
    p->manifestGen = newManifestGen;
    p->intentGen = 0;
    p->intentSeg = 0;
    // Drop the merged L0 blocks and their accelerators.
    const uint32_t remaining = p->nL0 - k;
    for (uint32_t i = 0; i < remaining; i++) {
        p->l0[i] = p->l0[i + k];
        p->acc[i] = p->acc[i + k];
    }
    p->nL0 = remaining;
    if (remaining) {
        uint64_t minPos = UINT64_MAX;
        for (uint32_t i = 0; i < remaining; i++)
            if (p->acc[i].chainPos && p->acc[i].chainPos < minPos) minPos = p->acc[i].chainPos;
        if (minPos != UINT64_MAX) p->chain.freeBefore(e->pool(), minPos);
    } else {
        p->chain.freeAll(e->pool());
    }
    rc = writeCtlBatch(w, p, kCtlMergeDone, done, sizeof(done));
    if (rc < 0) return rc;
    // Reopen the run read-only for lookups and load its accelerators.
    SegRun& run = si->runs.back();
    run.file = xf;
    run.run.reset(new L1Run());
    rc = run.run->load(&w->io(), run.file, run.fileLen);
    if (rc < 0) return rc;
    (void)nEntries;
    p->pubLock.writeBegin();
    p->pub.commitSeq = p->commitSeq;
    p->pub.pseqHi = p->pseqHi;
    p->pub.nL0 = p->nL0;
    std::memcpy(p->pub.l0, p->l0, sizeof(L0DirEntry) * p->nL0);
    p->pubLock.writeEnd();
    e->cMerges.fetch_add(1, std::memory_order_relaxed);
    return partitionWriteHead(w, p, false);
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
