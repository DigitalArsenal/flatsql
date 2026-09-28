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
            ManifestDesc md;
            if (size <= 0 || w->io().read(f, buf.data(), buf.size(), 0) != size ||
                !decodeManifest(buf.data(), buf.size(), &md)) {
                rc = FLATSQL_IO_ERR_IO;
            } else {
                for (const auto& d : md.segs) {
                    SegmentInfo si;
                    segFromDesc(d, &si);
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
    partitionAccount(p);
    return 0;
}

void partitionAccount(Partition* p) {
    uint64_t acc = 0;
    for (const auto& si : p->segs)
        for (const auto& r : si.runs)
            if (r.run) acc += r.run->memoryBytes();
    p->accelBytes.store(acc, std::memory_order_relaxed);
    p->laneCount.store(uint32_t(p->lanes.size()), std::memory_order_relaxed);
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
    partitionAccount(p);
}

void removePendingCtl(Partition* p, uint16_t kind) {
    // Keeps every queued ctl record except those of `kind` (a merge and a
    // compaction may both have one queued).
    uint8_t keep[sizeof(p->pendingCtl)];
    uint32_t n = 0, bytes = 0;
    for (uint32_t off = 0; off + 4 <= p->pendingCtlBytes;) {
        const uint16_t k = getU16(p->pendingCtl + off);
        const uint16_t len = getU16(p->pendingCtl + off + 2);
        if (k != kind) {
            std::memcpy(keep + bytes, p->pendingCtl + off, 4u + len);
            bytes += 4u + len;
            n++;
        }
        off += 4u + len;
    }
    std::memcpy(p->pendingCtl, keep, bytes);
    p->pendingCtlBytes = bytes;
    p->nPendingCtl = n;
}

bool partitionWantsMerge(const Engine* e, const Partition* p) {
    if (p->nL0 == 0 || p->quarantined) return false;
    // T3: a built compaction waits for no merge in flight to SWAP; new merges
    // wait for it (one commit round).
    if (p->compactPhase == kCompactBuilt || p->compactPhase == kCompactSwapQueued) return false;
    const L0DirEntry& first = p->l0[0];
    const uint64_t firstLast = first.firstPseq + first.nRows - 1;
    if (firstLast > p->labeledThrough.load(std::memory_order_acquire)) return false;
    if (first.mSeg != p->mSeg) return true;  // a sealed segment still has L0 blocks
    uint64_t bytes = 0;
    for (uint32_t i = 0; i < p->nL0; i++) bytes += p->l0[i].batchLen;
    if (bytes >= e->config().mergeL0Bytes) return true;
    if (p->nL0 >= kMaxL0Dir - 8) return true;  // near the head's L0 directory cap
    // 16 blocks trigger a merge once they carry real volume; tiny commits
    // (a record or two each) wait, so a merge never rewrites a few rows.
    return p->nL0 >= e->config().mergeL0Blocks && bytes >= e->config().mergeMinL0Bytes;
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

void resetPlan(MergePlan& m) {
    m.seg = m.gen = m.k = m.fold = m.ownerEpoch = 0;
    m.firstPseq = m.through = m.rOff = m.aOff = m.rLen = m.aLen = m.segFirstPseq = 0;
    m.minEpoch = INT64_MAX;
    m.maxEpoch = INT64_MIN;
    m.batches.clear();
    m.foldRuns.clear();
    m.snap.clear();
    m.retire.clear();
    m.minArrival = INT64_MAX;
    m.maxArrival = INT64_MIN;
    m.prevManifestGen = 0;
    m.mfLen = 0;
    m.r = m.a = m.mf = FileRef();
    m.run = SegRun();
    m.result.store(0, std::memory_order_relaxed);
}

int32_t openSegAt(IoCtx* io, const char* root, uint32_t pid, char letter, uint32_t seg, int32_t flags,
                  FileRef* out) {
    PathBuf path;
    const char* ext = letter == 'r' ? "fsr" : letter == 'a' ? "fsa" : letter == 'd' ? "fsd" : "fsl";
    pathPartitionSeg(&path, root, pid, letter, seg, ext);
    const FileClass cls = letter == 'r' ? FileClass::Rows : letter == 'a' ? FileClass::Attrs
                          : letter == 'd' ? FileClass::Data : FileClass::Meta;
    return io->open(path.c_str(), path.len, flags, cls, out);
}

// Plans a merge on the owner: everything the output build needs is copied
// into the plan, so the build can run on a helper thread (A26).
int32_t planMerge(Writer* w, Partition* p) {
    Engine* e = w->engine();
    const int savedDepth = tHotPathDepth;
    tHotPathDepth = 0;  // maintenance
    struct Restore {
        int d;
        ~Restore() { tHotPathDepth = d; }
    } restore{savedDepth};
    const uint64_t labeled = p->labeledThrough.load(std::memory_order_acquire);
    const uint32_t seg = p->l0[0].mSeg;
    uint32_t k = 0;
    while (k < p->nL0 && p->l0[k].mSeg == seg && p->l0[k].firstPseq + p->l0[k].nRows - 1 <= labeled) k++;
    if (k == 0) return 0;
    MergePlan& m = p->mplan;
    resetPlan(m);
    m.seg = seg;
    m.k = k;
    m.firstPseq = p->l0[0].firstPseq;
    m.through = p->l0[k - 1].firstPseq + p->l0[k - 1].nRows - 1;
    SegmentInfo* si = segInfo(p, seg, true);
    if (si->firstPseq == 0) {
        si->firstPseq = (seg == p->dSeg) ? p->segFirstPseq : m.firstPseq;
        si->mergedEnd = si->firstPseq;
    }
    m.segFirstPseq = si->firstPseq;
    m.gen = p->nextGen++;
    m.rOff = (m.firstPseq - si->firstPseq) * sizeof(RecRow);
    m.aOff = si->aLen;
    m.minEpoch = si->minEpoch;
    m.maxEpoch = si->maxEpoch;
    m.minArrival = si->minArrival;
    m.maxArrival = si->maxArrival;
    m.prevManifestGen = p->manifestGen;
    m.ownerEpoch = p->ownerEpoch;
    m.ownerWord = &p->ring->ownerWordV;
    m.batches.assign(p->l0, p->l0 + k);
    // Tiered folding (fanout 4), decided from the accelerators' entry counts.
    uint64_t newEntries = 0;
    if (p->acc)
        for (uint32_t i = 0; i < k; i++)
            for (int j = 0; j < p->acc[i].nKinds; j++) newEntries += p->acc[i].kinds[j].n;
    // A fold is capped at mergeFoldMaxEntries (and skipped when the L0
    // directory is under pressure): a merge must finish before the directory
    // fills, or the partition's staging waits for it. Bigger runs are T3's.
    const uint64_t foldCap = e->config().mergeFoldMaxEntries;
    const bool pressure = p->nL0 >= kMaxL0Dir / 2;
    uint64_t acc = newEntries ? newEntries : 1;
    uint32_t cand = 0;
    for (size_t i = si->runs.size(); i-- > 0 && !pressure;) {
        const SegRun& r = si->runs[i];
        if (!r.run || r.run->entries() > 2 * acc || acc + r.run->entries() > foldCap) break;
        acc += r.run->entries();
        cand++;
    }
    m.fold = cand >= 3 ? cand : 0;
    for (size_t i = si->runs.size() - m.fold; i < si->runs.size(); i++)
        m.foldRuns.push_back({si->runs[i].run.get(), si->runs[i].file});
    // Manifest snapshot (as MERGE_DONE will leave the partition).
    for (const auto& s2 : p->segs) {
        if (!s2.firstPseq && s2.runs.empty() && s2.seg != seg) continue;  // m-only placeholder
        ManifestSegDesc sn = segDesc(s2);
        if (s2.seg == seg) sn.runs.resize(s2.runs.size() - m.fold);
        m.snap.push_back(std::move(sn));
    }
    // What MERGE_DONE retires (A12): the folded runs and the manifest it replaces.
    for (size_t i = si->runs.size() - m.fold; i < si->runs.size(); i++)
        m.retire.push_back(retireItem('x', seg, si->runs[i].gen, si->runs[i].fileLen));
    if (p->manifestGen) m.retire.push_back(retireItem('f', 0, p->manifestGen, ledgerSize(p, 'f', 0, p->manifestGen)));
    si->runs.reserve(si->runs.size() + 1);  // MERGE_DONE applies without allocating
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

std::vector<uint8_t> encodeSnapManifest(uint32_t pid, uint32_t gen, uint32_t prevGen,
                                        const std::vector<ManifestSegDesc>& snap) {
    ManifestDesc md;
    md.gen = gen;
    md.pid = pid;
    md.prevGen = prevGen;
    md.segs = snap;
    return encodeManifest(md);
}

// Builds a planned merge's outputs (rows, attributes, the L1 run, the
// manifest) without syncing them. Reads only the plan and immutable files, so
// it may run on a helper thread.
// A26: a helper result is written only while the plan's epoch still owns the
// partition. (HANDOFF waits for an in-flight helper, so a revoked epoch here is
// a defect; it is counted and the outputs are abandoned like an INTENT.)
constexpr int32_t kMergeNotOwner = -1000;
bool planStillOwns(const MergePlan& m) {
    if (!m.ownerWord) return true;
    const uint64_t w = m.ownerWord->load(std::memory_order_acquire);
    return ownerEpoch(w) == m.ownerEpoch && ownerState(w) == kOwnOwned;
}

int32_t writeMergeOutputs(IoCtx* io, const char* root, uint32_t pid, MergePlan& m) {
    int32_t rc = 0;
    FileRef mf;
    rc = openSegAt(io, root, pid, 'm', m.seg, FLATSQL_IO_READ, &mf);
    if (rc < 0) return rc;
    std::vector<uint8_t> rbuf, abuf;
    std::vector<std::vector<uint8_t>> blocks(m.k);
    for (uint32_t i = 0; i < m.k && rc >= 0; i++) {
        const L0DirEntry& de = m.batches[i];
        std::vector<uint8_t> b(de.batchLen);
        if (io->read(mf, b.data(), b.size(), de.mOff) != int64_t(b.size())) {
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
                if (row.arrivalMs < m.minArrival) m.minArrival = row.arrivalMs;
                if (row.arrivalMs > m.maxArrival) m.maxArrival = row.arrivalMs;
            }
            const uint8_t* rb = reinterpret_cast<const uint8_t*>(&row);
            rbuf.insert(rbuf.end(), rb, rb + sizeof(row));
        }
        const uint8_t* ab = b.data() + sizeof(BatchHeader) + size_t(bh.nRows) * sizeof(RecRow);
        abuf.insert(abuf.end(), ab, ab + bh.attrBytes);
        blocks[i].assign(b.begin() + de.l0Off, b.begin() + de.l0Off + bh.l0Bytes);
    }
    io->close(&mf);
    if (rc < 0) return rc;
    // Rows and attributes at offsets derived from the plan: a redo is
    // idempotent (A11).
    if (!planStillOwns(m)) return kMergeNotOwner;
    rc = openSegAt(io, root, pid, 'r', m.seg, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS, &m.r);
    if (rc >= 0) rc = openSegAt(io, root, pid, 'a', m.seg, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS, &m.a);
    if (rc >= 0 && !planStillOwns(m)) rc = kMergeNotOwner;
    if (rc >= 0 && !rbuf.empty()) rc = io->write(m.r, rbuf.data(), rbuf.size(), m.rOff);
    if (rc >= 0 && !planStillOwns(m)) rc = kMergeNotOwner;
    if (rc >= 0 && !abuf.empty()) rc = io->write(m.a, abuf.data(), abuf.size(), m.aOff);
    m.rLen = m.rOff + rbuf.size();
    m.aLen = m.aOff + abuf.size();
    // The L1 run (fresh file, A11), folding the planned runs by a streaming
    // k-way merge. Folded run files stay until reclamation (T3); readers
    // holding an older manifest keep reading them.
    PathBuf xp;
    pathPartitionRun(&xp, root, pid, m.seg, m.gen);
    if (rc >= 0 && !planStillOwns(m)) rc = kMergeNotOwner;
    if (rc >= 0)
        rc = io->open(xp.c_str(), xp.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS,
                      FileClass::Index, &m.run.file);
    int64_t xLen = 0;
    uint64_t nEntries = 0;
    if (rc >= 0) {
        std::vector<MergeL0Input> l0s;
        for (uint32_t i = 0; i < m.k; i++) l0s.push_back({blocks[i].data(), blocks[i].size()});
        const uint64_t first = m.fold ? m.segFirstPseq : m.firstPseq;
        xLen = mergeToL1(io, m.run.file, m.seg, m.gen, uint16_t(m.fold ? 1 : 0), first, m.through, l0s, m.foldRuns,
                         &nEntries);
        if (xLen < 0) rc = int32_t(xLen);
    }
    m.run.gen = m.gen;
    m.run.fileLen = uint64_t(xLen > 0 ? xLen : 0);
    if (rc >= 0) {
        for (auto& sn : m.snap) {
            if (sn.seg != m.seg) continue;
            sn.mergedEnd = m.through + 1;
            sn.rLen = m.rLen;
            sn.aLen = m.aLen;
            sn.minEpoch = m.minEpoch;
            sn.maxEpoch = m.maxEpoch;
            sn.minArrival = m.minArrival;
            sn.maxArrival = m.maxArrival;
            ManifestRun mr{};
            mr.gen = m.gen;
            mr.nEntries = nEntries;
            mr.fileLen = m.run.fileLen;
            sn.runs.push_back(mr);
        }
        const std::vector<uint8_t> man = encodeSnapManifest(pid, m.gen, m.prevManifestGen, m.snap);
        m.mfLen = man.size();
        PathBuf mp;
        pathPartitionManifest(&mp, root, pid, m.gen);
        if (!planStillOwns(m)) rc = kMergeNotOwner;
        if (rc >= 0)
            rc = io->open(mp.c_str(), mp.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS,
                      FileClass::Manifest, &m.mf);
        if (rc >= 0) rc = io->write(m.mf, man.data(), man.size(), 0);
    }
    // Accelerators of the new run, loaded now so MERGE_DONE applies without
    // allocating on the commit path.
    if (rc >= 0) {
        m.run.run.reset(new L1Run());
        rc = m.run.run->load(io, m.run.file, m.run.fileLen);
    }
    // Outputs are durable before MERGE_DONE is staged: the builder syncs them,
    // off the commit path (no sync round of the writer carries them).
    if (rc >= 0 && !planStillOwns(m)) rc = kMergeNotOwner;
    if (rc >= 0) rc = io->sync(m.r);
    if (rc >= 0) rc = io->sync(m.a);
    if (rc >= 0) rc = io->sync(m.run.file);
    if (rc >= 0) rc = io->sync(m.mf);
    return rc;
}

}  // namespace

void partitionMergeAbort(Writer* w, Partition* p) {
    // A26: HANDOFF (or a failure) aborts an in-flight merge; its outputs are
    // discarded exactly as open discards an INTENT without MERGE_DONE.
    if (p->mergePhase == kMergeIdle) return;
    MergePlan& m = p->mplan;
    if (p->mergePhase == kMergeBuilding) {
        // The helper owns the plan until it finishes.
        if (m.result.load(std::memory_order_acquire) == 0)
            w->engine()->cHandoffHelperWaits.fetch_add(1, std::memory_order_relaxed);
        while (m.result.load(std::memory_order_acquire) == 0) sleepNs(1000000);
    }
    w->io().close(&m.r);
    w->io().close(&m.a);
    w->io().close(&m.mf);
    w->io().close(&m.run.file);
    m.run.run.reset();
    if (p->mergePhase >= kMergeIntentDurable) {
        // The durable head may still name this INTENT: the next checkpoint
        // head (at the latest the one stop writes) clears it, so open has no
        // orphan cleanup to do after a clean stop.
        if (!p->metaSinceCkpt) p->metaSinceCkpt = 1;
        PathBuf xp, mfp;
        pathPartitionRun(&xp, w->eng_root(), p->pid, m.seg, m.gen);
        pathPartitionManifest(&mfp, w->eng_root(), p->pid, m.gen);
        // Closed above; a BUSY (another holder) leaves an orphan for open to
        // discard with the INTENT.
        w->io().unlink(xp.c_str(), xp.len, true);
        w->io().unlink(mfp.c_str(), mfp.len, true);
        FileRef rf, af;
        // T3: r/a a first merge created name nothing: they go.
        const bool first = m.rOff == 0 && m.aOff == 0;
        if (first) {
            PathBuf rp, ap;
            pathPartitionSeg(&rp, w->eng_root(), p->pid, 'r', m.seg, "fsr");
            pathPartitionSeg(&ap, w->eng_root(), p->pid, 'a', m.seg, "fsa");
            for (auto& s2 : p->segs)
                if (s2.seg == m.seg) {
                    w->io().close(&s2.r);
                    w->io().close(&s2.a);
                }
            w->io().close(&p->rA);
            w->io().close(&p->aA);
            w->io().unlink(rp.c_str(), rp.len, true);
            w->io().unlink(ap.c_str(), ap.len, true);
        } else {
            if (openSeg(w, p, 'r', m.seg, kOpenRW, &rf) == 0) {
                if (w->io().size(rf) > int64_t(m.rOff)) w->io().truncate(rf, m.rOff);
                w->io().close(&rf);
            }
            if (openSeg(w, p, 'a', m.seg, kOpenRW, &af) == 0) {
                if (w->io().size(af) > int64_t(m.aOff)) w->io().truncate(af, m.aOff);
                w->io().close(&af);
            }
        }
    }
    removePendingCtl(p, kCtlIntentMerge);
    p->intentGen = 0;
    p->intentSeg = 0;
    p->mergePhase = kMergeIdle;
}

int32_t partitionMergeStep(Writer* w, Partition* p) {
    Engine* e = w->engine();
    if (p->quarantined) return 0;
    // A queued SWAP owns the manifest until it publishes.
    if (p->compactPhase == kCompactSwapQueued && p->mergePhase == kMergeIdle) return 0;
    if (p->mergePhase == kMergeIdle) {
        if (p->mergeRetryNs && monoNs() < p->mergeRetryNs) return 0;
        if (!partitionWantsMerge(e, p)) return 0;
        const int32_t rc = partitionWarm(w, p);
        if (rc < 0) return rc;
        return planMerge(w, p);
    }
    if (p->mergePhase == kMergeIntentDurable) {
        MergePlan* m = &p->mplan;
        m->result.store(0, std::memory_order_release);
        const uint32_t pid = p->pid;
        if (e->config().mergeHelpers && !e->config().cooperative) {
            p->mergePhase = kMergeBuilding;
            Writer* owner = w;
            e->submitMaintenance([m, pid, owner, e](IoCtx* io) {
                const EngineConfig& cfg = e->config();
                if (cfg.testHelperStallNs && cfg.testHelperStallEvery &&
                    e->cHelperJobs.fetch_add(1, std::memory_order_relaxed) % cfg.testHelperStallEvery == 0) {
                    e->cHelperStalls.fetch_add(1, std::memory_order_relaxed);
                    sleepNs(cfg.testHelperStallNs);
                }
                const int32_t rc = writeMergeOutputs(io, owner->eng_root(), pid, *m);
                if (rc == kMergeNotOwner) e->cMergeNotOwner.fetch_add(1, std::memory_order_relaxed);
                m->result.store(rc < 0 ? rc : 1, std::memory_order_release);
                owner->ring();
            });
            return 1;
        }
        const int32_t rc = writeMergeOutputs(&w->io(), w->eng_root(), pid, *m);
        m->result.store(rc < 0 ? rc : 1, std::memory_order_release);
        p->mergePhase = kMergeBuilding;
    }
    if (p->mergePhase == kMergeBuilding) {
        const int32_t res = p->mplan.result.load(std::memory_order_acquire);
        if (res == 0) return 0;
        if (res < 0 || p->mplan.ownerEpoch != p->ownerEpoch) {
            partitionMergeAbort(w, p);
            // T3: a failed build retries a little later (a full disk fails
            // every attempt until the emergency frees space).
            if (res < 0 && res != kMergeNotOwner) p->mergeRetryNs = monoNs() + 100000000ull;
            return res < 0 && res != kMergeNotOwner ? res : 0;
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
    for (auto& s2 : p->segs)
        if (s2.seg == m.seg) si = &s2;
    if (!si) return;
    si->mergedEnd = m.through + 1;
    si->rLen = m.rLen;
    si->aLen = m.aLen;
    si->minEpoch = m.minEpoch;
    si->maxEpoch = m.maxEpoch;
    si->minArrival = m.minArrival;
    si->maxArrival = m.maxArrival;
    // Disk accounting: r/a grew, a run and a manifest were added (the folded
    // runs and the old manifest stay in the ledger until unlinked, A12).
    ledgerSet(p, retireItem('r', m.seg, 0, m.rLen));
    ledgerSet(p, retireItem('a', m.seg, 0, m.aLen));
    ledgerSet(p, retireItem('x', m.seg, m.gen, m.run.fileLen));
    ledgerSet(p, retireItem('f', 0, m.gen, m.mfLen));
    for (uint32_t i = 0; i < m.fold && !si->runs.empty(); i++) {
        w->io().close(&si->runs.back().file);
        si->runs.pop_back();
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
    partitionAccount(p);
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
    ledgerSet(p, retireItem('d', p->nextSeg, 0, 0));
    ledgerSet(p, retireItem('m', p->nextSeg, 0, 0));
    return 0;
}


}  // namespace ps
}  // namespace flatsql
