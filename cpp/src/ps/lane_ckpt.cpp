// FlatSQL partition store: paged lane checkpoints (terabyte design §2.9 K0,
// TB03; PARTITION-STORE.md §41). Level 3.
//
// A batch carries only the lanes it changed (its lane-delta section, one
// LaneDelta per lane). Up to 32 live lanes the head holds the whole table
// inline, as at level 2. Past 32, the batch that crosses writes the table
// once (LANE_CKPT) and the head from then on names a LaneRef: a base (the
// paged checkpoint lk-<gen>.fsl, or that LANE_CKPT) and the batch position
// its deltas replay from. The owner cuts a new checkpoint at a batch
// boundary; a maintenance helper writes it. There is no size cap.
//
// Crash protocol (§2.9):
//   1. The helper writes lk-<g> and syncs it; its directory entry is durable
//      from creation (CREATE_PARENTS). One cut is in flight per partition, so
//      g is always the ref's gen + 1.
//   2. The owner installs the ref and writes a head naming lk-<g> and its
//      replay offset, synced.
//   3. Only once a durable head names it may a RETIRE list lk-<g-1> (letter
//      'L') or a meta segment below the ref's floor.
//   4. Open stats lk-<head gen + 1> and unlinks it: no durable head named it.
//      A torn-head rebuild folds from the LANE_REF ctl that every RETIRE
//      batch carries.
#include <algorithm>
#include <unordered_map>

#include "internal.h"

namespace flatsql {
namespace ps {

void pathPartitionLaneCkpt(PathBuf* out, const char* root, uint32_t pid, uint32_t gen) {
    const int n = snprintf(out->buf, sizeof(out->buf), "%s/fsql2/p/%08x/lk-%06x.fsl", root, pid, gen);
    out->len = n < 0 ? 0 : (size_t(n) >= sizeof(out->buf) ? sizeof(out->buf) - 1 : size_t(n));
}

// ---------------------------------------------------------------------------
// Format
// ---------------------------------------------------------------------------
void laneApplyDelta(LaneCounter* c, const LaneDelta& d) {
    if (c->count == 0 && d.dCount > 0) {
        // A lane at count 0 has no history: checkpoints and head tables drop
        // it, so a delta that makes it live again starts it afresh. Every
        // table (the writer's memory, a fold from any base, a replay) then
        // holds the same counters whether or not a base dropped the lane.
        const uint32_t id = c->laneId;
        *c = LaneCounter{};
        c->laneId = id;
    }
    c->count += d.dCount;
    c->bytes += d.dBytes;
    if (d.maxPseq > c->maxPseq) c->maxPseq = d.maxPseq;
    if (d.dCount > 0 && (c->firstSeen == 0 || d.firstSeen < c->firstSeen)) c->firstSeen = d.firstSeen;
    if (d.updated > c->updated) c->updated = d.updated;
}

void laneCkptEncode(uint32_t pid, uint32_t gen, uint32_t mintedThrough, uint32_t replaySeg, uint64_t replayOff,
                    const LaneCounter* lanes, size_t n, std::vector<uint8_t>* out) {
    const uint32_t pages = uint32_t((n + kLanesPerPage - 1) / kLanesPerPage);
    out->assign(size_t(kLanePageBytes) * (1 + pages), 0);
    LaneCkptHeader h{};
    h.magic = kMagicLaneCkpt;
    h.ver = 1;
    h.pid = pid;
    h.gen = gen;
    h.nLanes = uint32_t(n);
    h.nPages = pages;
    h.mintedThrough = mintedThrough;
    h.replaySeg = replaySeg;
    h.replayOff = replayOff;
    h.crc = crc32c(&h, offsetof(LaneCkptHeader, crc));
    std::memcpy(out->data(), &h, sizeof(h));
    for (uint32_t pg = 0; pg < pages; pg++) {
        uint8_t* b = out->data() + size_t(kLanePageBytes) * (1 + pg);
        const size_t first = size_t(pg) * kLanesPerPage;
        const uint32_t k = uint32_t(std::min<size_t>(kLanesPerPage, n - first));
        std::memcpy(b + 8, lanes + first, size_t(k) * sizeof(LaneCounter));
        putU32(b, k);
        putU32(b + 4, crc32c(b + 8, size_t(k) * sizeof(LaneCounter)));
    }
}

int32_t laneLoadBase(LaneFileReader* rd, uint32_t pid, const LaneRef& ref, std::vector<LaneCounter>* out) {
    out->clear();
    if (ref.lkGen) {
        LaneCkptHeader h;
        int64_t n = rd->readCkpt(ref.lkGen, &h, sizeof(h), 0);
        if (n < 0) return int32_t(n);
        if (n != int64_t(sizeof(h)) || h.magic != kMagicLaneCkpt || h.ver != 1 ||
            h.crc != crc32c(&h, offsetof(LaneCkptHeader, crc)) || h.pid != pid || h.gen != ref.lkGen ||
            h.replaySeg != ref.replaySeg || h.replayOff != ref.replayOff ||
            uint64_t(h.nPages) * kLanesPerPage < h.nLanes ||
            (h.nLanes && uint64_t(h.nPages - 1) * kLanesPerPage >= h.nLanes))
            return FLATSQL_IO_ERR_IO;
        out->reserve(h.nLanes);
        // Pages are read in chunks: a large table never needs one huge read.
        constexpr uint32_t kChunkPages = 64;
        std::vector<uint8_t> buf;
        uint32_t lastId = 0;
        for (uint32_t pg = 0; pg < h.nPages; pg += kChunkPages) {
            const uint32_t np = std::min(kChunkPages, h.nPages - pg);
            buf.resize(size_t(np) * kLanePageBytes);
            n = rd->readCkpt(ref.lkGen, buf.data(), buf.size(), uint64_t(kLanePageBytes) * (1 + pg));
            if (n < 0) return int32_t(n);
            if (n != int64_t(buf.size())) return FLATSQL_IO_ERR_IO;
            for (uint32_t i = 0; i < np; i++) {
                const uint8_t* b = buf.data() + size_t(i) * kLanePageBytes;
                const uint32_t k = getU32(b);
                const size_t want = std::min<size_t>(kLanesPerPage, h.nLanes - size_t(pg + i) * kLanesPerPage);
                if (k != want || crc32c(b + 8, size_t(k) * sizeof(LaneCounter)) != getU32(b + 4))
                    return FLATSQL_IO_ERR_IO;
                for (uint32_t j = 0; j < k; j++) {
                    LaneCounter c;
                    std::memcpy(&c, b + 8 + size_t(j) * sizeof(c), sizeof(c));
                    if (c.laneId <= lastId) return FLATSQL_IO_ERR_IO;  // sorted, unique
                    lastId = c.laneId;
                    out->push_back(c);
                }
            }
        }
        return 0;
    }
    if (!ref.baseOff) return 0;  // empty base
    uint8_t nb[4];
    int64_t n = rd->readMeta(ref.baseSeg, nb, 4, ref.baseOff);
    if (n < 0) return int32_t(n);
    if (n != 4) return FLATSQL_IO_ERR_IO;
    const uint32_t cnt = getU32(nb);
    if (cnt > (1u << 20)) return FLATSQL_IO_ERR_IO;
    out->resize(cnt);
    if (cnt) {
        n = rd->readMeta(ref.baseSeg, out->data(), size_t(cnt) * sizeof(LaneCounter), ref.baseOff + 4);
        if (n < 0) return int32_t(n);
        if (n != int64_t(size_t(cnt) * sizeof(LaneCounter))) return FLATSQL_IO_ERR_IO;
    }
    return 0;
}

int32_t laneFold(LaneFileReader* rd, uint32_t pid, const LaneRef& ref, uint32_t endSeg, uint64_t endOff,
                 std::vector<LaneCounter>* out, uint32_t* batches, uint32_t* deltaBatches) {
    int32_t rc = laneLoadBase(rd, pid, ref, out);
    if (rc < 0) return rc;
    std::unordered_map<uint32_t, size_t> at;
    at.reserve(out->size() * 2 + 16);
    for (size_t i = 0; i < out->size(); i++) at[(*out)[i].laneId] = i;
    uint32_t seg = ref.replaySeg;
    uint64_t off = ref.replayOff;
    uint64_t prevCommit = 0;
    uint32_t nb = 0, nd = 0;
    std::vector<uint8_t> tail;
    while (seg != endSeg || off != endOff) {
        if (seg > endSeg || (seg == endSeg && off > endOff)) return FLATSQL_IO_ERR_IO;  // overran the head
        BatchHeader h;
        int64_t n = rd->readMeta(seg, &h, sizeof(h), off);
        if (n < 0) return int32_t(n);
        if (n != int64_t(sizeof(h)) || h.magic != kMagicBatch || h.ver != 1 ||
            h.batchLen < sizeof(BatchHeader) + sizeof(BatchTrailer) || (h.batchLen & 7) || h.dSeg != seg ||
            (prevCommit && h.commitSeq != prevCommit + 1))
            return FLATSQL_IO_ERR_IO;
        const uint64_t dOff = sizeof(BatchHeader) + uint64_t(h.nRows) * sizeof(RecRow) + h.attrBytes + h.l0Bytes;
        if (dOff + 8 + sizeof(BatchTrailer) > h.batchLen) return FLATSQL_IO_ERR_IO;
        // The lane deltas, the ctl records and the trailer: never the rows.
        tail.resize(size_t(h.batchLen - dOff));
        n = rd->readMeta(seg, tail.data(), tail.size(), off + dOff);
        if (n < 0) return int32_t(n);
        if (n != int64_t(tail.size())) return FLATSQL_IO_ERR_IO;
        BatchTrailer t;
        std::memcpy(&t, tail.data() + tail.size() - sizeof(t), sizeof(t));
        if (t.magic != kMagicTrailer || t.batchLen != h.batchLen || t.commitSeq != h.commitSeq)
            return FLATSQL_IO_ERR_IO;
        const size_t ctlEnd = tail.size() - sizeof(BatchTrailer);
        const uint32_t ndl = getU32(tail.data());
        const size_t dl = pad8(4 + size_t(ndl) * sizeof(LaneDelta));
        if (dl + 4 > ctlEnd) return FLATSQL_IO_ERR_IO;
        for (uint32_t i = 0; i < ndl; i++) {
            LaneDelta d;
            std::memcpy(&d, tail.data() + 4 + size_t(i) * sizeof(d), sizeof(d));
            auto it = at.find(d.laneId);
            if (it == at.end()) {
                LaneCounter c{};
                c.laneId = d.laneId;
                out->push_back(c);
                it = at.emplace(d.laneId, out->size() - 1).first;
            }
            laneApplyDelta(&(*out)[it->second], d);
        }
        const uint32_t nc = getU32(tail.data() + dl);
        size_t q = dl + 4;
        bool sealed = false;
        for (uint32_t i = 0; i < nc; i++) {
            if (q + 4 > ctlEnd) return FLATSQL_IO_ERR_IO;
            const uint16_t kind = getU16(tail.data() + q);
            const uint16_t len = getU16(tail.data() + q + 2);
            if (q + 4 + len > ctlEnd) return FLATSQL_IO_ERR_IO;
            if (kind == kCtlSeal) sealed = true;
            q += 4 + size_t(len);
        }
        nb++;
        if (ndl) nd++;
        prevCommit = h.commitSeq;
        if (sealed) {
            seg++;  // meta segments follow one another (a SEAL switches to seg + 1)
            off = 0;
        } else {
            off += h.batchLen;
        }
    }
    std::sort(out->begin(), out->end(),
              [](const LaneCounter& a, const LaneCounter& b) { return a.laneId < b.laneId; });
    if (batches) *batches = nb;
    if (deltaBatches) *deltaBatches = nd;
    return 0;
}

// ---------------------------------------------------------------------------
// Writer side
// ---------------------------------------------------------------------------
IoLaneFileReader::~IoLaneFileReader() {
    io_->close(&meta_);
    io_->close(&ckpt_);
}

int64_t IoLaneFileReader::readMeta(uint32_t seg, void* dst, size_t len, uint64_t off) {
    if (!meta_.valid() || metaSeg_ != seg) {
        io_->close(&meta_);
        PathBuf mp;
        pathPartitionSeg(&mp, root_, pid_, 'm', seg, "fsl");
        const int32_t rc = io_->open(mp.c_str(), mp.len, FLATSQL_IO_READ, FileClass::Meta, &meta_);
        if (rc < 0) return rc;
        metaSeg_ = seg;
    }
    return io_->read(meta_, dst, len, off);
}

int64_t IoLaneFileReader::readCkpt(uint32_t gen, void* dst, size_t len, uint64_t off) {
    if (!ckpt_.valid() || ckptGen_ != gen) {
        io_->close(&ckpt_);
        PathBuf kp;
        pathPartitionLaneCkpt(&kp, root_, pid_, gen);
        const int32_t rc = io_->open(kp.c_str(), kp.len, FLATSQL_IO_READ, FileClass::Lanes, &ckpt_);
        if (rc < 0) return rc;
        ckptGen_ = gen;
    }
    return io_->read(ckpt_, dst, len, off);
}

int32_t laneCkptWriteFile(IoCtx* io, const char* root, uint32_t pid, uint32_t gen, const std::vector<uint8_t>& bytes) {
    PathBuf kp;
    pathPartitionLaneCkpt(&kp, root, pid, gen);
    // A file of the same name is what a failed attempt left: it names nothing.
    int32_t rc = io->unlink(kp.c_str(), kp.len, true);
    if (rc < 0 && rc != FLATSQL_IO_ERR_NOENT) return rc;
    FileRef f;
    rc = io->open(kp.c_str(), kp.len,
                  FLATSQL_IO_READ | FLATSQL_IO_WRITE | FLATSQL_IO_CREATE | FLATSQL_IO_EXCL | FLATSQL_IO_CREATE_PARENTS,
                  FileClass::Lanes, &f);
    if (rc < 0) return rc;
    rc = io->write(f, bytes.data(), bytes.size(), 0);
    if (rc >= 0) rc = io->sync(f);
    io->close(&f);
    return rc;
}

// Every change to Partition::lanes goes through laneIndexAdd (one lane
// appended) or laneIndexRebuild, so the index is complete: an id it does not
// hold is not in the table.
Lane* laneById(Partition* p, uint32_t id) {
    if (id >= p->laneSlot.size()) return nullptr;
    const uint32_t s = p->laneSlot[id];
    if (!s) return nullptr;
    if (s - 1 < p->lanes.size() && p->lanes[s - 1].id == id) return &p->lanes[s - 1];
    laneIndexRebuild(p);  // defensive: never taken while every change is indexed
    return id < p->laneSlot.size() && p->laneSlot[id] ? &p->lanes[p->laneSlot[id] - 1] : nullptr;
}

void laneIndexAdd(Partition* p, uint32_t idx) {
    const uint32_t id = p->lanes[idx].id;
    if (id >= p->laneSlot.size()) {
        const int saved = tHotPathDepth;
        tHotPathDepth = 0;  // lane interning (registration, not the record path)
        p->laneSlot.resize(std::max<size_t>(size_t(id) + 1, p->laneSlot.size() * 2), 0);
        tHotPathDepth = saved;
    }
    p->laneSlot[id] = idx + 1;
}

void laneIndexRebuild(Partition* p) {
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    uint32_t maxId = 0;
    for (const auto& l : p->lanes) maxId = std::max(maxId, l.id);
    p->laneSlot.assign(size_t(maxId) + 1, 0);
    uint32_t live = 0;
    for (uint32_t i = 0; i < p->lanes.size(); i++) {
        p->laneSlot[p->lanes[i].id] = i + 1;
        if (p->lanes[i].c.count != 0) live++;
    }
    p->laneLive = live;
    tHotPathDepth = saved;
}

size_t partitionLaneCtlRoom(const Partition* p) {
    if (!p->laneCkpt) return 64 + (p->lanes.size() + 1) * sizeof(LaneCounter);  // level 2: the whole table
    if (p->laneMode == kLaneModeRef) return 64 + 4 + sizeof(LaneRef);                  // LANE_REF only
    // Inline: the crossing (or A9) table, the lanes live at staging plus one
    // per entry staged (an entry makes at most one lane live; staging stops
    // at kLaneInlineStageDeltas deltas, so kLaneInlineStageDeltas + 1 entries
    // at most add one).
    const uint32_t lanes = std::min<uint32_t>(kLaneCrossMaxLanes, p->laneLive + kLaneInlineStageDeltas + 1);
    return 64 + size_t(lanes) * sizeof(LaneCounter);
}

bool partitionLaneMetaMayRetire(const Partition* p, uint32_t seg) {
    if (p->laneMode != kLaneModeRef) return true;
    return seg < laneRefFloorSeg(p->laneRef) && p->lastDurableHeadNs >= p->laneRefNamedNs;
}

// A replaced checkpoint staged for RETIRE whose batch has not committed.
static bool laneRetirePending(const Partition* p) {
    for (const auto& it : p->retiring)
        if (it.letter == 'L') return true;
    return false;
}

// Maintenance (owner thread): installs a finished cut and names it durably,
// retires the checkpoint it replaced once that is durable, and starts a cut
// when one is due. Returns 1 when it wrote a head, < 0 on an error.
int32_t partitionLaneStep(Writer* w, Partition* p) {
    Engine* e = w->engine();
    if (!e->laneCkptLevel() || p->laneMode != kLaneModeRef || p->quarantined) return 0;
    const uint64_t now = monoNs();
    if (LaneCut* cut = p->laneCut.get()) {
        const int32_t res = cut->result.load(std::memory_order_acquire);
        if (res == 0) return 0;
        const int saved = tHotPathDepth;
        tHotPathDepth = 0;
        if (res < 0 || cut->ownerEpoch != p->ownerEpoch) {
            // Its file names nothing: gone now (BUSY: open finds it, lk-<gen + 1>).
            PathBuf kp;
            pathPartitionLaneCkpt(&kp, w->eng_root(), p->pid, cut->gen);
            w->io().unlink(kp.c_str(), kp.len, true);
            p->laneCut.reset();
            p->laneCutBytes.store(0, std::memory_order_relaxed);
            p->laneCutRetryNs = now + 100000000ull;
            tHotPathDepth = saved;
            return res == FLATSQL_IO_ERR_NOSPACE ? res : 0;
        }
        LaneRef r = p->laneRef;
        const uint32_t oldGen = r.lkGen;
        r.lkGen = cut->gen;
        r.baseSeg = 0;
        r.baseOff = 0;
        r.replaySeg = cut->cutSeg;
        r.replayOff = cut->cutOff;
        r.mintedThrough = cut->minted;
        r.deltaBatches = r.deltaBatches >= cut->deltaBatches ? r.deltaBatches - cut->deltaBatches : 0;
        r.batches = r.batches >= cut->batches ? r.batches - cut->batches : 0;
        p->laneRef = r;
        ledgerSet(p, retireItem('L', 0, cut->gen, cut->fileBytes));
        partitionPublishDisk(p);
        if (oldGen) p->lkRetire.push_back(oldGen);
        e->cLaneCuts.fetch_add(1, std::memory_order_relaxed);
        e->cLaneCutBytes.fetch_add(cut->fileBytes, std::memory_order_relaxed);
        p->laneCut.reset();
        p->laneCutBytes.store(0, std::memory_order_relaxed);
        // Step 2: a head naming lk-<gen>, synced with this iteration's jobs.
        int32_t hrc = p->h.valid() ? 0 : partitionWarm(w, p);
        if (hrc >= 0) hrc = partitionWriteHead(w, p, true);
        if (hrc >= 0) {
            w->queueHeadSync(p);
            p->laneRefNamedNs = p->pendingDurableHeadNs;
            p->lastCkptNs = now;
            p->metaSinceCkpt = 0;
        } else {
            // A13: the next commit writes the head; a checkpoint head follows.
            p->laneRefNamedNs = now;
            if (!p->metaSinceCkpt) p->metaSinceCkpt = 1;
        }
        tHotPathDepth = saved;
        return hrc < 0 && hrc != FLATSQL_IO_ERR_NOSPACE ? hrc : 1;
    }
    // Step 3: the replaced checkpoint goes once a durable head names the new one.
    if (!p->lkRetire.empty() && p->lastDurableHeadNs >= p->laneRefNamedNs) {
        const int saved = tHotPathDepth;
        tHotPathDepth = 0;
        for (uint32_t g : p->lkRetire) p->retiring.push_back(retireItem('L', 0, g, ledgerSize(p, 'L', 0, g)));
        p->lkRetire.clear();
        p->retireDirty = true;
        tHotPathDepth = saved;
        w->ring();
    }
    // A new cut: after a crossing (the base is a LANE_CKPT in the meta log),
    // when a meta segment it replays from is to retire, or once a fold would
    // walk laneCkptBatches batches with lane deltas or 4x that many batches
    // in all (a fold reads every batch; maintenance batches count a quarter,
    // so the RETIRE and UNLINKED a cut itself causes never make the next one
    // due). One at a time, and not while an older checkpoint still waits to
    // be retired: until the RETIRE naming lk-<g - 1> commits, only the head
    // names what open cleans up (lk-<gen - 1>, lk-<gen + 1>), so the gens on
    // disk stay within g - 1 .. g + 1.
    const uint32_t every = std::max<uint32_t>(1, e->config().laneCkptBatches);
    const bool due = p->laneCutWanted || (p->laneRef.lkGen == 0 && p->laneRef.baseOff) ||
                     p->laneRef.deltaBatches >= every || uint64_t(p->laneRef.batches) >= 4ull * every;
    if (!due || !p->lkRetire.empty() || laneRetirePending(p) || now < p->laneCutRetryNs) return 0;
    if (p->laneRef.deltaBatches == 0 && p->laneRef.lkGen && p->laneRef.replaySeg == p->mSeg &&
        p->laneRef.replayOff == p->mEnd) {
        p->laneCutWanted = false;  // nothing since the last cut
        return 0;
    }
    // The install writes a head: the partition's handles open now (a cold
    // partition keeps its head handle; one never warmed since open has none).
    if (!p->warm) {
        const int32_t wrc = partitionWarm(w, p);
        if (wrc < 0) return wrc;
    }
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;  // maintenance, never per record
    std::unique_ptr<LaneCut> cut(new LaneCut());
    cut->gen = p->laneRef.lkGen + 1;
    cut->cutSeg = p->mSeg;
    cut->cutOff = p->mEnd;
    cut->minted = p->nextLaneId ? p->nextLaneId - 1 : 0;
    cut->deltaBatches = p->laneRef.deltaBatches;
    cut->batches = p->laneRef.batches;
    cut->ownerEpoch = p->ownerEpoch;
    std::vector<LaneCounter> live;
    live.reserve(p->laneLive);
    for (const auto& l : p->lanes) {
        if (l.c.count == 0) continue;
        LaneCounter c = l.c;
        c.laneId = l.id;
        live.push_back(c);
    }
    std::sort(live.begin(), live.end(), [](const LaneCounter& a, const LaneCounter& b) { return a.laneId < b.laneId; });
    cut->nLive = uint32_t(live.size());
    laneCkptEncode(p->pid, cut->gen, cut->minted, cut->cutSeg, cut->cutOff, live.data(), live.size(), &cut->bytes);
    cut->fileBytes = cut->bytes.size();
    p->laneCutWanted = false;
    LaneCut* job = cut.get();
    p->laneCut = std::move(cut);
    // Held until the helper writes it: counted with the writer's memory.
    p->laneCutBytes.store(job->bytes.capacity(), std::memory_order_relaxed);
    tHotPathDepth = saved;
    const uint32_t pid = p->pid;
    if (e->config().mergeHelpers && !e->config().cooperative) {
        Writer* owner = w;
        e->submitMaintenance([job, pid, owner](IoCtx* io) {
            const int32_t rc = laneCkptWriteFile(io, owner->eng_root(), pid, job->gen, job->bytes);
            job->result.store(rc < 0 ? rc : 1, std::memory_order_release);
            owner->ring();
        });
        return 0;
    }
    const int32_t rc = laneCkptWriteFile(&w->io(), w->eng_root(), pid, job->gen, job->bytes);
    job->result.store(rc < 0 ? rc : 1, std::memory_order_release);
    return 0;
}

void Writer::queueHeadSync(Partition* p) { queueSync(p->h, p, 3); }

}  // namespace ps
}  // namespace flatsql
