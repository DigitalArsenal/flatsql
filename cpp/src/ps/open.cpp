// FlatSQL partition store: open, recovery and registration (design §10,
// amended by A4, A9, A10, A11; §4.7 intent rule).
//
// Open reads STORE, MIGRATED, the registry, then per type and per partition
// the head (two slots, the used prefix only) and the meta-log tail past the
// head. It never reads a d-* byte and never parses a frame. A tail batch is
// adopted only after the data and meta files are fsynced and a DURABLE_CKPT
// head is written and fsynced (A4): nothing written before a crash becomes
// visible (or labelled, or published) unless it is durable.
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace ps {

namespace {

constexpr int32_t kOpenRW = FLATSQL_IO_READ | FLATSQL_IO_WRITE;

int32_t readHeadSlots(IoCtx* io, const FileRef& f, uint16_t kind, std::vector<uint8_t>* best) {
    int bestSlot = -1;
    uint64_t bestGen = 0;
    uint8_t slots[2][kHeadSlotBytes];
    for (int s = 0; s < 2; s++) {
        int64_t n = io->read(f, slots[s], kHeadReadBytes, uint64_t(s) * kHeadSlotBytes);
        if (n < int64_t(sizeof(HeadPrefix))) continue;
        HeadPrefix hp;
        std::memcpy(&hp, slots[s], sizeof(hp));
        if (hp.magic != kMagicHead || hp.kind != kind || hp.usedLen > kHeadSlotBytes) continue;
        if (hp.usedLen > n) {
            const int64_t more = io->read(f, slots[s] + n, hp.usedLen - size_t(n), uint64_t(s) * kHeadSlotBytes + n);
            if (more < 0 || n + more < hp.usedLen) continue;
            n += more;
        }
        if (!validHeadSlot(slots[s], size_t(n), kind)) continue;
        if (bestSlot < 0 || hp.gen > bestGen) {
            bestSlot = s;
            bestGen = hp.gen;
        }
    }
    if (bestSlot < 0) return 0;
    HeadPrefix hp;
    std::memcpy(&hp, slots[bestSlot], sizeof(hp));
    best->assign(slots[bestSlot], slots[bestSlot] + hp.usedLen);
    return 1;
}

bool validBatchHeader(const BatchHeader& h, uint64_t off, int64_t size) {
    return h.magic == kMagicBatch && h.ver == 1 &&
           h.batchLen >= sizeof(BatchHeader) + sizeof(BatchTrailer) &&
           off + h.batchLen <= uint64_t(size) && (h.batchLen & 7) == 0 &&
           uint64_t(h.nRows) * sizeof(RecRow) + sizeof(BatchHeader) + sizeof(BatchTrailer) <= h.batchLen;
}

}  // namespace

// ---------------------------------------------------------------------------
// Partition tail replay (shared by open and the -3 rebuild)
// ---------------------------------------------------------------------------
struct TailReplay {
    uint32_t adopted = 0;
    uint32_t maxIncarnation = 0;
    bool touchedSegs[2] = {false, false};
    std::vector<uint32_t> dSegs;  // data segments whose d must be fsynced
    // A9 torn-head rebuild from the first live meta segment: its first batch
    // anchors the commit chain.
    bool anchor = false;
    // T3 (A12): the outstanding RETIRE set as replayed.
    std::vector<RetireItem> retired;
};

// Applies one validated batch to the in-memory partition state.
static void applyBatch(Partition* p, const uint8_t* b, uint64_t mOff, TailReplay* tr,
                       std::vector<std::pair<uint16_t, std::vector<uint8_t>>>* ctls) {
    BatchHeader h;
    std::memcpy(&h, b, sizeof(h));
    BatchTrailer t;
    std::memcpy(&t, b + h.batchLen - sizeof(BatchTrailer), sizeof(t));
    p->commitSeq = t.commitSeq;
    p->pseqHi = t.pseqHi;
    p->counters.totalCount = t.counters.totalCount;
    p->counters.totalBytes = t.counters.totalBytes;
    p->counters.liveCount = t.counters.liveCount;
    p->counters.liveBytes = t.counters.liveBytes;
    p->counters.tombCount = t.counters.tombCount;
    p->counters.minEpoch = t.counters.minEpoch;
    p->counters.maxEpoch = t.counters.maxEpoch;
    p->counters.latestArrival = t.counters.latestArrival;
    p->dLen = t.dCommitted;
    p->mEnd = mOff + h.batchLen;
    p->incarnation = t.incarnation;
    if (t.incarnation > tr->maxIncarnation) tr->maxIncarnation = t.incarnation;
    if (h.dLen) tr->dSegs.push_back(h.dSeg);
    const size_t rowsLen = size_t(h.nRows) * sizeof(RecRow);
    size_t off = sizeof(BatchHeader) + rowsLen + h.attrBytes;
    if (h.nRows && p->nL0 < kMaxL0Dir) {
        L0DirEntry& e = p->l0[p->nL0++];
        e.mSeg = p->mSeg;
        e.nRows = h.nRows;
        e.mOff = mOff;
        e.firstPseq = h.firstPseq;
        e.batchLen = h.batchLen;
        e.l0Off = uint32_t(off);
        p->segRecords += h.nRows;
    }
    off += h.l0Bytes;
    // Lane deltas (by lane id; tuples load lazily from l.fsl).
    const uint32_t nd = getU32(b + off);
    for (uint32_t i = 0; i < nd; i++) {
        LaneDelta d;
        std::memcpy(&d, b + off + 4 + size_t(i) * sizeof(LaneDelta), sizeof(d));
        Lane* lane = nullptr;
        for (auto& l : p->lanes)
            if (l.id == d.laneId) lane = &l;
        if (!lane) {
            p->lanes.emplace_back();
            lane = &p->lanes.back();
            lane->id = d.laneId;
            lane->c.laneId = d.laneId;
        }
        lane->c.count += d.dCount;
        lane->c.bytes += d.dBytes;
        if (d.maxPseq > lane->c.maxPseq) lane->c.maxPseq = d.maxPseq;
        if (d.dCount > 0 && (lane->c.firstSeen == 0 || d.firstSeen < lane->c.firstSeen))
            lane->c.firstSeen = d.firstSeen;
        if (d.updated > lane->c.updated) lane->c.updated = d.updated;
        if (d.laneId >= p->nextLaneId) p->nextLaneId = d.laneId + 1;
    }
    off += pad8(4 + size_t(nd) * sizeof(LaneDelta));
    const uint32_t nc = getU32(b + off);
    size_t q = off + 4;
    for (uint32_t i = 0; i < nc; i++) {
        const uint16_t kind = getU16(b + q);
        const uint16_t len = getU16(b + q + 2);
        ctls->push_back({kind, std::vector<uint8_t>(b + q + 4, b + q + 4 + len)});
        if (kind == kCtlRetire) {
            // T3: the head written after this open points at the latest set.
            p->retireSeg = p->mSeg;
            p->retireOff = mOff + q + 4;
            p->retireN = len >= 4 ? getU32(b + q + 4) : 0;
        }
        if (kind == kCtlLaneCkpt) {
            // Full table of non-zero lanes after this batch; the head written
            // after this open points at it (as publish does).
            p->lanesOverflowSeg = p->mSeg;
            p->lanesOverflowOff = mOff + q + 4;
            const uint8_t* body = b + q + 4;
            const uint32_t n = getU32(body);
            for (auto& l : p->lanes) l.c.count = 0, l.c.bytes = 0;
            for (uint32_t k = 0; k < n; k++) {
                LaneCounter c;
                std::memcpy(&c, body + 4 + size_t(k) * sizeof(LaneCounter), sizeof(c));
                Lane* lane = nullptr;
                for (auto& l : p->lanes)
                    if (l.id == c.laneId) lane = &l;
                if (!lane) {
                    p->lanes.emplace_back();
                    lane = &p->lanes.back();
                    lane->id = c.laneId;
                }
                lane->c = c;
            }
        }
        q += 4 + len;
    }
}

// Applies ctl records that move state across files (seal, merges).
static void applyCtls(Partition* p, const std::vector<std::pair<uint16_t, std::vector<uint8_t>>>& ctls,
                      bool* sealed, TailReplay* tr) {
    for (const auto& c : ctls) {
        const uint8_t* b = c.second.data();
        switch (c.first) {
            case kCtlSeal:
                *sealed = true;
                break;
            case kCtlIntentMerge:
                p->intentSeg = getU32(b);
                p->intentGen = getU32(b + 4);
                p->intentROff = getU64(b + 8);
                p->intentAOff = getU64(b + 16);
                p->intentThrough = getU64(b + 24);
                if (p->intentGen + 1 > p->nextGen) p->nextGen = p->intentGen + 1;
                break;
            case kCtlIntentCompact: {
                // T3: compaction outputs are named before they exist (A11).
                if (c.second.size() < kIntentCompactBytes) break;
                p->cIntentSeg = getU32(b);
                p->cIntentGen = getU32(b + 8);
                if (p->cIntentGen + 1 > p->nextGen) p->nextGen = p->cIntentGen + 1;
                break;
            }
            case kCtlSwap: {
                // The manifest the SWAP committed (it names c-* files).
                const uint32_t gen = getU32(b + 4);
                p->manifestGen = gen;
                if (gen + 1 > p->nextGen) p->nextGen = gen + 1;
                if (p->cIntentGen == gen) {
                    p->cIntentGen = 0;
                    p->cIntentSeg = 0;
                }
                break;
            }
            case kCtlRetire: {
                // A12: the whole outstanding set, and (A9) the state a rebuild
                // from a later meta segment needs.
                RetireSetHeader h;
                if (retireSetDecode(b, c.second.size(), &h, &tr->retired)) {
                    if (h.manifestGen) p->manifestGen = h.manifestGen;
                    if (h.mergedThrough > p->mergedThrough) p->mergedThrough = h.mergedThrough;
                    if (h.firstLiveMSeg > p->firstLiveMSeg) p->firstLiveMSeg = h.firstLiveMSeg;
                    if (h.nextGen > p->nextGen) p->nextGen = h.nextGen;
                    uint32_t k = 0;
                    while (k < p->nL0 && p->l0[k].firstPseq + p->l0[k].nRows - 1 <= p->mergedThrough) k++;
                    for (uint32_t i = 0; i + k < p->nL0; i++) p->l0[i] = p->l0[i + k];
                    p->nL0 -= k;
                }
                break;
            }
            case kCtlMergeDone: {
                const uint32_t gen = getU32(b + 4);
                const uint64_t through = getU64(b + 8);
                p->mergedThrough = through;
                p->manifestGen = getU32(b + 32);
                p->intentGen = 0;
                p->intentSeg = 0;
                if (gen + 1 > p->nextGen) p->nextGen = gen + 1;
                uint32_t k = 0;
                while (k < p->nL0 && p->l0[k].firstPseq + p->l0[k].nRows - 1 <= through) k++;
                for (uint32_t i = 0; i + k < p->nL0; i++) p->l0[i] = p->l0[i + k];
                p->nL0 -= k;
                break;
            }
            default:
                break;
        }
    }
}

static int32_t replayPartitionTail(Engine* e, IoCtx* io, Partition* p, uint32_t incFloor, TailReplay* tr) {
    const std::string& root = e->root();
    uint32_t lastInc = incFloor;
    for (;;) {
        PathBuf mp;
        pathPartitionSeg(&mp, root.c_str(), p->pid, 'm', p->mSeg, "fsl");
        FileRef mf;
        int32_t rc = io->open(mp.c_str(), mp.len, FLATSQL_IO_READ, FileClass::Meta, &mf);
        if (rc == FLATSQL_IO_ERR_NOENT) return 0;
        if (rc < 0) return rc;
        const int64_t size = io->size(mf);
        bool sealed = false;
        uint64_t off = p->mEnd;
        std::vector<uint8_t> batch;
        while (off + sizeof(BatchHeader) <= uint64_t(size)) {
            BatchHeader h;
            if (io->read(mf, &h, sizeof(h), off) != int64_t(sizeof(h))) break;
            if (tr->anchor && tr->adopted == 0 && validBatchHeader(h, off, size) && h.commitSeq) {
                // A9 rebuild: the first batch of the first live meta segment.
                p->commitSeq = h.commitSeq - 1;
                p->pseqHi = h.firstPseq - 1;
                p->segFirstPseq = h.firstPseq;
            }
            if (!validBatchHeader(h, off, size) || h.commitSeq != p->commitSeq + 1 ||
                h.firstPseq != p->pseqHi + 1 || h.dSeg != p->mSeg)
                break;
            batch.resize(h.batchLen);
            if (io->read(mf, batch.data(), h.batchLen, off) != int64_t(h.batchLen)) break;
            BatchTrailer t;
            std::memcpy(&t, batch.data() + h.batchLen - sizeof(t), sizeof(t));
            if (t.magic != kMagicTrailer || t.batchLen != h.batchLen || t.commitSeq != h.commitSeq)
                break;
            if (crc32c(batch.data(), h.batchLen - sizeof(t) + offsetof(BatchTrailer, crc)) != t.crc) break;
            // Stale batches of an older incarnation never chain (open never
            // zeroes: the incarnation does).
            if (t.incarnation < lastInc) break;
            // A8: a journaled batch is durable only through its journal
            // record; present in m but not replayed, its d/l bytes may be lost.
            if ((h.flags & kBatchJournaled) && !e->journalReplayed(kJrnMeta, p->pid, p->mSeg, off)) break;
            lastInc = t.incarnation;
            std::vector<std::pair<uint16_t, std::vector<uint8_t>>> ctls;
            applyBatch(p, batch.data(), off, tr, &ctls);
            applyCtls(p, ctls, &sealed, tr);
            tr->adopted++;
            off += h.batchLen;
            if (sealed) break;
        }
        io->close(&mf);
        if (!sealed) return 0;
        // Continue in the next segment (minor 3).
        p->mSeg = p->nextSeg;
        p->dSeg = p->nextSeg;
        p->nextSeg++;
        p->mEnd = 0;
        p->dLen = 0;
        p->segFirstPseq = p->pseqHi + 1;
        p->segRecords = 0;
    }
}

// ---------------------------------------------------------------------------
// Engine::open
// ---------------------------------------------------------------------------
int32_t Engine::open(const EngineConfig& cfgIn, std::unique_ptr<Engine>* out, std::string* err) {
    std::unique_ptr<Engine> e(new Engine());
    e->cfg_ = cfgIn;
    EngineConfig& cfg = e->cfg_;
    if (cfg.writers == 0) cfg.writers = 1;
    if (cfg.writers > 64) cfg.writers = 64;
    // The largest ring entry (writer TLV 19). The frames half of each
    // writer's arena holds two, so an entry of that size always stages.
    if (cfg.maxEntryBytes < (64u << 10)) cfg.maxEntryBytes = 64u << 10;
    if (cfg.maxEntryBytes > (1ull << 30)) cfg.maxEntryBytes = 1ull << 30;
    if (cfg.arenaBytes < 4 * (cfg.maxEntryBytes + 4096)) cfg.arenaBytes = 4 * (cfg.maxEntryBytes + 4096);
    if (!cfg.io) cfg.io = importIo();
    e->openIoHolder_.reset(new IoCtx(cfg.io, &e->openIoStats_));
    e->openIo_ = e->openIoHolder_.get();
    e->helperIo_.reset(new IoCtx(cfg.io, &e->helperIoStats_));
    if (!e->pool_.init(cfg.poolBytes, cfg.slabBytes)) {
        if (err) *err = "slab pool allocation failed";
        return FLATSQL_IO_ERR_NOSPACE;
    }
    e->reserveSlabs_ = uint32_t(cfg.reserveBytes / cfg.slabBytes);
    e->partsCap_ = 1u << 16;
    e->parts_.reset(new std::atomic<Partition*>[e->partsCap_]);
    for (uint32_t i = 0; i < e->partsCap_; i++) e->parts_[i].store(nullptr, std::memory_order_relaxed);
    for (uint32_t i = 0; i < cfg.writers; i++) e->writers_.emplace_back(new Writer(e.get(), uint8_t(i)));
    int32_t rc = e->openStore(err);
    if (rc < 0) return rc;
    // A8: journaled rounds reach their files before any head is read.
    rc = e->replayJournals(err);
    if (rc < 0) return rc;
    rc = e->openTypes(err);
    if (rc < 0) return rc;
    rc = e->openPartitions(err);
    if (rc < 0) return rc;
    e->journalMeta_.clear();
    rc = e->writeOpenTypeHeads(err);
    if (rc < 0) return rc;
    // T3: the planner's state and the ballast (A13).
    rc = engineOpenQuota(e.get(), e->openIo_, err);
    if (rc < 0) return rc;
    // New incarnation: strictly above every incarnation any batch carries.
    uint32_t inc = e->registry_.incarnation();
    for (const auto& p : e->partStore_)
        if (p && p->incarnation > inc) inc = p->incarnation;
    for (const auto& t : e->typeStore_)
        if (t->incarnation > inc) inc = t->incarnation;
    e->incarnation_ = inc + 1;
    rc = e->registry_.beginIncarnation(e->incarnation_);
    if (rc < 0) {
        if (err) *err = "registry head write failed";
        return rc;
    }
    // Pin types, then partitions, to the least-loaded writer (§5.2).
    for (auto& t : e->typeStore_) {
        const uint8_t w = e->leastLoadedWriter();
        t->ownerWriter.store(w);
        e->writers_[w]->types_.push_back(t.get());
        e->writers_[w]->pinned_.fetch_add(1);
    }
    for (auto& p : e->partStore_) {
        if (!p) continue;
        const uint8_t w = e->leastLoadedWriter();
        p->ownerWriter.store(w);
        p->ring->ownerWordV.store(ownerWord(p->ownerEpoch, w, kOwnOwned));
        e->writers_[w]->owned_.push_back(p.get());
        e->writers_[w]->ownedCount_.fetch_add(1);
        e->writers_[w]->pinned_.fetch_add(1);
    }
    // A8: every writer's journal files exist from the start, so writer ids
    // with journals are contiguous and replay stops at the first gap.
    if (cfg.commitJournal) {
        for (auto& w : e->writers_) {
            if (w->journalOpen() < 0) {
                if (err) *err = "commit journal create failed";
                return FLATSQL_IO_ERR_IO;
            }
        }
    }
    *out = std::move(e);
    return 0;
}

int32_t Engine::openStore(std::string* err) {
    IoCtx* io = openIo_;
    PathBuf sp, mp;
    pathStore(&sp, cfg_.root.c_str(), "STORE");
    pathStore(&mp, cfg_.root.c_str(), "MIGRATED");
    StoreFile sf{};
    bool fresh = false;
    FileRef f;
    int32_t rc = io->open(sp.c_str(), sp.len, FLATSQL_IO_READ, FileClass::Store, &f);
    if (rc == 0) {
        const int64_t n = io->read(f, &sf, sizeof(sf), 0);
        io->close(&f);
        const bool valid = n == int64_t(sizeof(sf)) && sf.magic == kMagicStore && sf.format == kFormat &&
                           sf.crc == crc32c(&sf, offsetof(StoreFile, crc));
        if (!valid) {
            // A torn STORE can only come from a crash while creating a fresh
            // store (it is written once, before anything else). With no
            // registry frames it is recreated; otherwise refuse.
            PathBuf rp;
            pathStore(&rp, cfg_.root.c_str(), "registry.fsl");
            FileRef rf;
            bool empty = true;
            if (io->open(rp.c_str(), rp.len, FLATSQL_IO_READ, FileClass::Registry, &rf) == 0) {
                empty = io->size(rf) == 0;
                io->close(&rf);
            }
            if (!empty || !cfg_.create) {
                if (err) *err = "STORE is corrupt";
                return FLATSQL_IO_ERR_IO;
            }
            io->unlink(sp.c_str(), sp.len, false);
            io->unlink(mp.c_str(), mp.len, false);
            fresh = true;
        }
    } else if (rc == FLATSQL_IO_ERR_NOENT) {
        if (!cfg_.create) {
            if (err) *err = "no store at root";
            return rc;
        }
        fresh = true;
    } else {
        if (err) *err = "cannot open STORE";
        return rc;
    }
    if (fresh) {
        sf = StoreFile{};
        sf.magic = kMagicStore;
        sf.format = kFormat;
        const uint64_t a = hash64(cfg_.root.data(), cfg_.root.size(), uint64_t(nowMs()));
        const uint64_t b = hash64(&a, 8, monoNs());
        std::memcpy(sf.uuid, &a, 8);
        std::memcpy(sf.uuid + 8, &b, 8);
        sf.createdMs = nowMs();
        sf.gseqFloor = 1;
        sf.migratedFrom = 0;
        sf.crc = crc32c(&sf, offsetof(StoreFile, crc));
        rc = io->open(sp.c_str(), sp.len,
                      kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_EXCL | FLATSQL_IO_CREATE_PARENTS,
                      FileClass::Store, &f);
        if (rc < 0) {
            if (err) *err = "cannot create STORE";
            return rc;
        }
        rc = io->write(f, &sf, sizeof(sf), 0);
        if (rc >= 0) rc = io->sync(f);
        io->close(&f);
        if (rc < 0) {
            if (err) *err = "cannot write STORE";
            return rc;
        }
        if (cfg_.freshMarksMigrated) {
            MigratedFile m{};
            m.magic = kMagicMigrated;
            m.format = kFormat;
            std::memcpy(m.uuid, sf.uuid, 16);
            m.migratedMs = sf.createdMs;
            m.crc = crc32c(&m, offsetof(MigratedFile, crc));
            rc = io->open(mp.c_str(), mp.len,
                          kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_EXCL | FLATSQL_IO_CREATE_PARENTS,
                          FileClass::Store, &f);
            if (rc >= 0) rc = io->write(f, &m, sizeof(m), 0);
            if (rc >= 0) rc = io->sync(f);
            io->close(&f);
            if (rc < 0) {
                if (err) *err = "cannot write MIGRATED";
                return rc;
            }
        }
    }
    if (cfg_.requireMigrated) {
        MigratedFile m{};
        rc = io->open(mp.c_str(), mp.len, FLATSQL_IO_READ, FileClass::Store, &f);
        bool ok = false;
        if (rc == 0) {
            ok = io->read(f, &m, sizeof(m), 0) == int64_t(sizeof(m)) && m.magic == kMagicMigrated &&
                 m.crc == crc32c(&m, offsetof(MigratedFile, crc)) &&
                 std::memcmp(m.uuid, sf.uuid, 16) == 0;
            io->close(&f);
        }
        if (!ok) {
            if (err) *err = "store is not MIGRATED (format 2 refuses to start)";
            return FLATSQL_IO_ERR_ACCESS;
        }
    }
    gseqNext_.store(sf.gseqFloor ? sf.gseqFloor : 1);
    return registry_.open(io, cfg_.root, cfg_.create || fresh, err);
}

int32_t Engine::loadTypeConfig(const TypeEntry& te, std::string* err) {
    IoCtx* io = openIo_;
    PathBuf cp;
    pathTypeConfig(&cp, cfg_.root.c_str(), te.fid, te.configFp);
    FileRef f;
    int32_t rc = io->open(cp.c_str(), cp.len, FLATSQL_IO_READ, FileClass::Config, &f);
    if (rc < 0) {
        if (err) *err = "type config missing";
        return rc;
    }
    const int64_t size = io->size(f);
    std::vector<uint8_t> buf(size > 0 ? size_t(size) : 0);
    const bool readOk = size >= 16 && io->read(f, buf.data(), buf.size(), 0) == size;
    io->close(&f);
    if (!readOk || getU32(buf.data()) != kMagicTypeConfig ||
        getU32(buf.data() + 8) != buf.size() - 16 ||
        crc32c(buf.data() + 16, buf.size() - 16) != getU32(buf.data() + 12)) {
        if (err) *err = "type config corrupt";
        return FLATSQL_IO_ERR_IO;
    }
    auto cfg = std::make_shared<TypeConfig>();
    const std::string perr = cfg->parse(buf.data() + 16, buf.size() - 16);
    if (!perr.empty()) {
        if (err) *err = perr;
        return FLATSQL_IO_ERR_GENERIC;
    }
    std::unique_ptr<TypeOwner> t(new TypeOwner());
    std::memcpy(t->fid, te.fid, 4);
    t->cfg = cfg;
    t->noticeCap = cfg_.noticeQueue;
    t->notices.reset(new std::atomic<uint32_t>[t->noticeCap ? t->noticeCap : 1]);
    typeByFid_[fidU32(te.fid)] = t.get();
    typeStore_.push_back(std::move(t));
    return 0;
}

int32_t Engine::openTypes(std::string* err) {
    IoCtx* io = openIo_;
    for (const TypeEntry& te : registry_.types()) {
        int32_t rc = loadTypeConfig(te, err);
        if (rc < 0) return rc;
        TypeOwner* t = typeStore_.back().get();
        PathBuf hp;
        pathType(&hp, cfg_.root.c_str(), t->fid, "h.fsh");
        FileRef hf;
        rc = io->open(hp.c_str(), hp.len, FLATSQL_IO_READ, FileClass::TypeHead, &hf);
        std::vector<uint8_t> head;
        if (rc == 0) {
            readHeadSlots(io, hf, kHeadType, &head);
            io->close(&hf);
        }
        uint32_t incFloor = 0;
        uint64_t labelCkptOff = 0;
        uint32_t labelCkptSeg = 0;
        bool labelsInline = true;
        if (!head.empty()) {
            TypeHeadFixed h;
            std::memcpy(&h, head.data(), sizeof(h));
            t->headGen = h.p.gen;
            t->commitSeq = h.commitSeq;
            t->gseqHi = h.gseqHi;
            t->gSeg = h.gSeg;
            t->incarnation = h.incarnation;
            incFloor = h.incarnation;
            t->gLen = h.gLen;
            t->mSeg = h.mSeg;
            t->nextSeg = h.nextSeg ? h.nextSeg : 1;
            t->mEnd = h.mEnd;
            t->arrivalsCount = h.arrivalsCount;
            t->firstLiveCount = h.firstLiveCount;
            t->firstLiveBytes = h.firstLiveBytes;
            t->nextGen = h.nextGen ? h.nextGen : 1;
            t->gSegFirstGseq = h.gSegFirstGseq;
            t->nL0 = h.nL0 <= kMaxTypeL0Dir ? h.nL0 : 0;
            size_t off = sizeof(h);
            std::memcpy(t->l0, head.data() + off, sizeof(TypeL0DirEntry) * t->nL0);
            off += sizeof(TypeL0DirEntry) * h.nL0;
            if (h.nLabels != 0xffff) {
                for (uint16_t i = 0; i < h.nLabels; i++) {
                    LabelEntry le;
                    std::memcpy(&le, head.data() + off + size_t(i) * sizeof(le), sizeof(le));
                    t->labeled[le.pid] = le.labeledThrough;
                }
            } else {
                labelsInline = false;
                labelCkptOff = h.labelCkptOff;
                labelCkptSeg = h.labelCkptSeg;
            }
            t->manifestGenLoaded = h.manifestGen;
            t->firstLiveMSeg = h.firstLiveMSeg;
        }
        // Label checkpoint (A10, > 128 pids): the FULL_LABELS batch, then the
        // label deltas of every later batch up to the head (bounded by the
        // checkpoint interval).
        // T3: the checkpoint's segment may precede the head's (a rotation
        // before the next checkpoint): later segments are read whole.
        for (uint32_t sg = labelCkptSeg; !labelsInline && sg <= t->mSeg; sg++) {
            PathBuf mp;
            pathTypeSeg(&mp, cfg_.root.c_str(), t->fid, 'm', sg, "fsl");
            FileRef mf;
            if (io->open(mp.c_str(), mp.len, FLATSQL_IO_READ, FileClass::TypeMeta, &mf) == 0) {
                uint64_t off = sg == labelCkptSeg ? labelCkptOff : 0;
                const uint64_t end = sg == t->mSeg ? t->mEnd : uint64_t(io->size(mf));
                while (off + sizeof(TypeBatchHeader) <= end) {
                    TypeBatchHeader bh;
                    if (io->read(mf, &bh, sizeof(bh), off) != int64_t(sizeof(bh)) || bh.magic != kMagicTypeBatch ||
                        bh.batchLen < sizeof(bh) + 8 || off + bh.batchLen > end)
                        break;
                    std::vector<uint8_t> labels(size_t(bh.nLabel) * sizeof(LabelEntry));
                    if (bh.nLabel && io->read(mf, labels.data(), labels.size(), off + sizeof(bh)) !=
                                         int64_t(labels.size()))
                        break;
                    for (uint32_t i = 0; i < bh.nLabel; i++) {
                        LabelEntry le;
                        std::memcpy(&le, labels.data() + size_t(i) * sizeof(le), sizeof(le));
                        t->labeled[le.pid] = le.labeledThrough;
                    }
                    off += bh.batchLen;
                }
                io->close(&mf);
            }
        }
        if (!labelsInline) {
            t->labelCkptOff = labelCkptOff;
            t->labelCkptSeg = labelCkptSeg;
            t->haveLabelCkpt = true;
        }
        // Tail of the type log (A4, A10).
        PathBuf mp, gp;
        pathTypeSeg(&mp, cfg_.root.c_str(), t->fid, 'm', t->mSeg, "fsl");
        pathTypeSeg(&gp, cfg_.root.c_str(), t->fid, 'g', t->gSeg, "fsg");
        FileRef mf, gf;
        uint32_t adopted = 0;
        if (io->open(mp.c_str(), mp.len, kOpenRW, FileClass::TypeMeta, &mf) == 0) {
            io->open(gp.c_str(), gp.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS,
                     FileClass::Arrivals, &gf);
            int64_t size = io->size(mf);
            uint64_t off = t->mEnd;
            uint32_t lastInc = incFloor;
            std::vector<uint8_t> b;
            bool sealAdopted = false;
            uint32_t adoptedHere = 0;
            for (;;) {
                while (off + sizeof(TypeBatchHeader) <= uint64_t(size)) {
                    TypeBatchHeader h;
                    if (io->read(mf, &h, sizeof(h), off) != int64_t(sizeof(h))) break;
                    // A15: a batch may open the next arrivals segment.
                    const bool seal = h.gSeg == t->gSeg + 1 && h.gOff == 0 && h.nArrivals > 0;
                    if (h.magic != kMagicTypeBatch || h.ver != 1 || h.commitSeq != t->commitSeq + 1 ||
                        h.batchLen < sizeof(h) + 8 || off + h.batchLen > uint64_t(size) ||
                        h.incarnation < lastInc || (h.gSeg != t->gSeg && !seal))
                        break;
                    b.resize(h.batchLen);
                    if (io->read(mf, b.data(), h.batchLen, off) != int64_t(h.batchLen)) break;
                    if (crc32c(b.data(), h.batchLen - 8) != getU32(b.data() + h.batchLen - 8)) break;
                    if (h.flags & kTypeBatchJournaled) {
                        uint32_t fidv;
                        std::memcpy(&fidv, t->fid, 4);
                        if (!journalReplayed(kJrnTypeMeta, fidv, t->mSeg, off)) break;
                    }
                    // Arrivals must be durable too: validate them by their CRC.
                    if (h.nArrivals) {
                        FileRef next;
                        if (seal) {
                            PathBuf np;
                            pathTypeSeg(&np, cfg_.root.c_str(), t->fid, 'g', h.gSeg, "fsg");
                            if (io->open(np.c_str(), np.len, kOpenRW, FileClass::Arrivals, &next) != 0) break;
                        } else if (h.gOff != t->gLen) {
                            break;
                        }
                        FileRef& src = seal ? next : gf;
                        std::vector<uint8_t> g(size_t(h.nArrivals) * kArrivalBytes);
                        if (!src.valid() || io->read(src, g.data(), g.size(), h.gOff) != int64_t(g.size()) ||
                            crc32c(g.data(), g.size()) != h.gCrc) {
                            io->close(&next);
                            break;
                        }
                        if (seal) {
                            // The sealed segment's tail was adopted durable
                            // already; switch to the new one.
                            if (gf.valid() && adopted && io->sync(gf) < 0) {
                                io->close(&next);
                                if (err) *err = "arrivals fsync failed";
                                return FLATSQL_IO_ERR_IO;
                            }
                            io->close(&gf);
                            gf = next;
                            t->gSeg = h.gSeg;
                            t->gLen = 0;
                            sealAdopted = true;
                        }
                        if (h.gOff == 0) t->gSegFirstGseq = h.firstGseq;
                    }
                    lastInc = h.incarnation;
                    t->commitSeq = h.commitSeq;
                    t->incarnation = h.incarnation;
                    if (h.gseqHi > t->gseqHi) t->gseqHi = h.gseqHi;
                    t->gLen = h.gOff + uint64_t(h.nArrivals) * kArrivalBytes;
                    t->arrivalsCount = h.arrivalsCount;
                    t->firstLiveCount = h.firstLiveCount;
                    t->firstLiveBytes = h.firstLiveBytes;
                    for (uint32_t i = 0; i < h.nLabel; i++) {
                        LabelEntry le;
                        std::memcpy(&le, b.data() + sizeof(h) + size_t(i) * sizeof(le), sizeof(le));
                        t->labeled[le.pid] = le.labeledThrough;
                    }
                    if (h.flags & 1) {
                        t->labelCkptOff = off;
                        t->labelCkptSeg = t->mSeg;
                        t->haveLabelCkpt = true;
                    }
                    if (h.flags & 4) {
                        // MERGE_DONE: L0 blocks of commits <= mergedThroughCommit
                        // live in the run named by mergeGen.
                        uint32_t k = 0;
                        while (k < t->nL0 && t->l0[k].commitSeq <= h.mergedThroughCommit) k++;
                        for (uint32_t i = 0; i + k < t->nL0; i++) t->l0[i] = t->l0[i + k];
                        t->nL0 -= k;
                        t->manifestGenLoaded = h.mergeGen;
                        if (h.mergeGen + 1 > t->nextGen) t->nextGen = h.mergeGen + 1;
                    }
                    const uint32_t l0Len = h.batchLen - 8 - h.l0Off;
                    if (l0Len && t->nL0 < kMaxTypeL0Dir) {
                        TypeL0DirEntry& de = t->l0[t->nL0++];
                        de.mSeg = t->mSeg;
                        de.l0Len = l0Len;
                        de.mOff = off;
                        de.commitSeq = h.commitSeq;
                        de.batchLen = h.batchLen;
                        de.l0Off = h.l0Off;
                    }
                    off += h.batchLen;
                    t->mEnd = off;
                    adopted++;
                    adoptedHere++;
                }
                // T3 (A9): the chain goes on at the start of the next segment
                // (a rotation cut this one at its last batch).
                PathBuf np;
                pathTypeSeg(&np, cfg_.root.c_str(), t->fid, 'm', t->mSeg + 1, "fsl");
                FileRef nf;
                if (io->open(np.c_str(), np.len, kOpenRW, FileClass::TypeMeta, &nf) != 0) break;
                TypeBatchHeader nh;
                if (io->read(nf, &nh, sizeof(nh), 0) != int64_t(sizeof(nh)) || nh.magic != kMagicTypeBatch ||
                    nh.commitSeq != t->commitSeq + 1) {
                    io->close(&nf);
                    break;
                }
                if (adoptedHere && io->sync(mf) < 0) {
                    io->close(&nf);
                    if (err) *err = "type tail fsync failed";
                    return FLATSQL_IO_ERR_IO;
                }
                io->close(&mf);
                mf = nf;
                t->mSeg++;
                t->mEnd = 0;
                off = 0;
                size = io->size(mf);
                adoptedHere = 0;
            }
            if (t->nextSeg <= t->mSeg) t->nextSeg = t->mSeg + 1;
            if (adopted) {
                // A4: make the adopted tail durable before anything is published.
                if ((gf.valid() && io->sync(gf) < 0) || io->sync(mf) < 0) {
                    if (err) *err = "type tail fsync failed";
                    return FLATSQL_IO_ERR_IO;
                }
            }
            (void)sealAdopted;
            io->close(&mf);
            if (gf.valid()) io->close(&gf);
        }
        // A15: the fence index covers exactly the sealed segments; entries of
        // seals that never committed are cut, and a next segment they created
        // is removed.
        t->gSegLastGseq = t->gLen ? t->gseqHi : 0;
        {
            PathBuf fp;
            pathType(&fp, cfg_.root.c_str(), t->fid, kArrivalFenceName);
            FileRef ff;
            const uint64_t want = uint64_t(t->gSeg) * sizeof(ArrivalFence);
            if (io->open(fp.c_str(), fp.len, kOpenRW, FileClass::Arrivals, &ff) == 0) {
                const int64_t fsz = io->size(ff);
                std::vector<uint8_t> fb(size_t(std::min<uint64_t>(fsz > 0 ? uint64_t(fsz) : 0, want)));
                bool okF = fb.empty() || io->read(ff, fb.data(), fb.size(), 0) == int64_t(fb.size());
                for (uint32_t i = 0; okF && i < t->gSeg; i++) {
                    ArrivalFence f;
                    if ((uint64_t(i) + 1) * sizeof(f) > fb.size()) {
                        okF = false;
                        break;
                    }
                    std::memcpy(&f, fb.data() + size_t(i) * sizeof(f), sizeof(f));
                    if (f.seg != i || f.crc != crc32c(&f, offsetof(ArrivalFence, crc))) okF = false;
                }
                if (okF && uint64_t(fsz) > want) okF = io->truncate(ff, want) >= 0 && io->sync(ff) >= 0;
                io->close(&ff);
                if (!okF) {
                    if (err) *err = "arrivals fence index invalid";
                    return FLATSQL_IO_ERR_IO;
                }
            } else if (t->gSeg != 0) {
                if (err) *err = "arrivals fence index missing";
                return FLATSQL_IO_ERR_IO;
            }
            t->fenceLen = want;
            PathBuf np;
            pathTypeSeg(&np, cfg_.root.c_str(), t->fid, 'g', t->gSeg + 1, "fsg");
            if (io->probe(np.c_str(), np.len) == 0) io->unlink(np.c_str(), np.len, true);
        }
        // Type manifest: live catalog runs.
        std::vector<RetireItem> typeRetired;
        if (t->manifestGenLoaded) {
            PathBuf mfp;
            pathTypeManifest(&mfp, cfg_.root.c_str(), t->fid, t->manifestGenLoaded);
            FileRef f;
            if (io->open(mfp.c_str(), mfp.len, FLATSQL_IO_READ, FileClass::Manifest, &f) == 0) {
                const int64_t size = io->size(f);
                std::vector<uint8_t> man(size > 0 ? size_t(size) : 0);
                if (size >= 24 && io->read(f, man.data(), man.size(), 0) == size &&
                    getU32(man.data()) == kMagicManifest) {
                    const uint32_t n = getU32(man.data() + 4);
                    const size_t body = 16 + size_t(n) * 16;
                    if (body + 8 <= man.size() && crc32c(man.data(), body) == getU32(man.data() + body)) {
                        for (uint32_t i = 0; i < n; i++) {
                            SegRun r;
                            r.gen = getU32(man.data() + 16 + size_t(i) * 16);
                            r.fileLen = getU64(man.data() + 16 + size_t(i) * 16 + 8);
                            t->runs.push_back(std::move(r));
                        }
                        parseTypeRetireSet(man.data(), man.size(), body + 8, &typeRetired);
                    }
                    t->manifestBytes = uint64_t(size);
                }
                io->close(&f);
            }
        }
        // T3: runs and manifests the manifest retired, meta segments below
        // first_live_m_seg, outputs of a merge the crash cut short.
        rc = typeOpenReclaim(io, cfg_.root.c_str(), t, typeRetired);
        if (rc < 0) {
            if (err) *err = "type reclamation failed";
            return rc;
        }
        const uint64_t floor = t->gseqHi + 1;
        if (floor > gseqNext_.load()) gseqNext_.store(floor);
        if (adopted) {
            adoptedBatches += adopted;
            // A4: the DURABLE_CKPT head is written once the partitions are
            // attached (its inline labels come from them), before any thread.
            t->openHeadDue = true;
        }
        t->publishedGseqHi.store(t->gseqHi);
        t->publishedArrivals.store(t->arrivalsCount);
    }
    return 0;
}

// Type heads for adopted type tails (A4), written after openPartitions has
// attached every partition and set its labeled_through: a head written earlier
// would list no labels and, once durable, lose them.
int32_t Engine::writeOpenTypeHeads(std::string* err) {
    IoCtx* io = openIo_;
    for (auto& tp : typeStore_) {
        TypeOwner* t = tp.get();
        if (!t->openHeadDue) continue;
        t->openHeadDue = false;
        int32_t rc = typeEnsureFiles(io, this, t);
        if (rc >= 0) {
            uint8_t slot[kHeadSlotBytes];
            t->headGen++;
            uint32_t used;
            encodeTypeHead(t, slot, &used, true);
            const uint64_t off = (t->headGen % 2) * kHeadSlotBytes;
            if (off + kHeadSlotBytes > t->hExtent) {  // whole slots until the file holds both
                std::memset(slot + used, 0, kHeadSlotBytes - used);
                used = kHeadSlotBytes;
            }
            rc = io->write(t->h, slot, used, off);
            if (rc >= 0 && off + used > t->hExtent) t->hExtent = off + used;
            if (rc >= 0) rc = io->sync(t->h);
            typePublishDisk(t);
        }
        io->close(&t->h);
        io->close(&t->m);
        io->close(&t->g);
        if (rc < 0) {
            if (err) *err = "type head write failed";
            return rc;
        }
    }
    return 0;
}

Partition* Engine::makePartition(const PartitionEntry& e, uint8_t writer) {
    std::unique_ptr<Partition> p(new Partition());
    p->pid = e.pid;
    std::memcpy(p->fid, e.fid, 4);
    p->token = e.token;
    p->type = type(e.fid);
    const uint64_t cap = p->type && p->type->cfg && p->type->cfg->ringCap() ? p->type->cfg->ringCap()
                                                                            : cfg_.defaultRingCap;
    p->ring = ringCreate(e.pid, cap, cfg_.maxEntryBytes, cfg_.slabBytes);
    p->ownerWriter.store(writer);
    p->ring->ownerWordV.store(ownerWord(1, writer, kOwnOwned));
    p->segOpenedMs = nowMs();
    Partition* raw = p.get();
    if (partStore_.size() <= e.pid) partStore_.resize(e.pid + 1);
    partStore_[e.pid] = std::move(p);
    if (e.pid < partsCap_) parts_[e.pid].store(raw, std::memory_order_release);
    if (e.pid > maxPidPub_.load(std::memory_order_relaxed)) maxPidPub_.store(e.pid, std::memory_order_release);
    nParts_.fetch_add(1);
    if (raw->type) typeAddPartition(raw->type, raw);
    std::string key = raw->token;
    key.append(reinterpret_cast<const char*>(e.fid), 4);
    pidByKey_[key] = e.pid;
    return raw;
}

int32_t Engine::openPartitions(std::string* err) {
    IoCtx* io = openIo_;
    for (const PartitionEntry& pe : registry_.partitions()) {
        if (pe.dropped) continue;
        Partition* p = makePartition(pe, 0);
        p->quarantined = pe.quarantined;
        PathBuf hp;
        pathPartition(&hp, cfg_.root.c_str(), p->pid, "h.fsh");
        FileRef hf;
        int32_t rc = io->open(hp.c_str(), hp.len, FLATSQL_IO_READ, FileClass::Head, &hf);
        std::vector<uint8_t> head;
        if (rc == 0) {
            readHeadSlots(io, hf, kHeadPartition, &head);
            io->close(&hf);
        }
        uint32_t incFloor = 0;
        if (!head.empty()) {
            PartitionHeadFixed h;
            std::memcpy(&h, head.data(), sizeof(h));
            p->headGen = h.p.gen;
            p->ownerEpoch = h.p.ownerEpoch ? h.p.ownerEpoch : 1;
            p->commitSeq = h.commitSeq;
            p->pseqHi = h.pseqHi;
            p->mSeg = h.mSeg;
            p->incarnation = h.incarnation;
            incFloor = h.incarnation;
            p->mEnd = h.mEnd;
            p->dSeg = h.dSeg;
            p->nextSeg = h.nextSeg ? h.nextSeg : h.mSeg + 1;
            p->dLen = h.dLen;
            p->mergedThrough = h.mergedThrough;
            p->manifestGen = h.manifestGen;
            p->nextGen = h.nextGen ? h.nextGen : 1;
            p->counters = h.counters;
            p->nextLaneId = h.nextLaneId ? h.nextLaneId : 1;
            p->segFirstPseq = h.segFirstPseq ? h.segFirstPseq : 1;
            p->intentSeg = h.intentSeg;
            p->intentGen = h.intentGen;
            p->intentROff = h.intentROff;
            p->intentAOff = h.intentAOff;
            p->intentThrough = h.intentThrough;
            p->firstLiveMSeg = h.firstLiveMSeg;
            p->cIntentSeg = h.cIntentSeg;
            p->cIntentGen = h.cIntentGen;
            p->retireSeg = h.retireSeg;
            p->retireN = h.retireN;
            p->retireOff = h.retireOff;
            p->nL0 = h.nL0 <= kMaxL0Dir ? h.nL0 : 0;
            size_t off = sizeof(h);
            std::memcpy(p->l0, head.data() + off, sizeof(L0DirEntry) * p->nL0);
            off += sizeof(L0DirEntry) * h.nL0;
            if (h.nLanes != 0xffff) {
                for (uint16_t i = 0; i < h.nLanes; i++) {
                    Lane l;
                    std::memcpy(&l.c, head.data() + off + size_t(i) * sizeof(LaneCounter), sizeof(LaneCounter));
                    l.id = l.c.laneId;
                    p->lanes.push_back(std::move(l));
                }
            } else if (h.lanesOverflowOff) {
                // The head written after this open points at the same table
                // unless a batch carries a new one (a MERGE_DONE, SEAL or SWAP
                // batch has no lane deltas and writes none).
                p->lanesOverflowSeg = h.lanesOverflowSeg;
                p->lanesOverflowOff = h.lanesOverflowOff;
                PathBuf mp;
                pathPartitionSeg(&mp, cfg_.root.c_str(), p->pid, 'm', h.lanesOverflowSeg, "fsl");
                FileRef mf;
                if (io->open(mp.c_str(), mp.len, FLATSQL_IO_READ, FileClass::Meta, &mf) == 0) {
                    uint8_t nb[4];
                    if (io->read(mf, nb, 4, h.lanesOverflowOff) == 4) {
                        const uint32_t n = getU32(nb);
                        std::vector<LaneCounter> lc(n);
                        if (io->read(mf, lc.data(), n * sizeof(LaneCounter), h.lanesOverflowOff + 4) ==
                            int64_t(n * sizeof(LaneCounter))) {
                            for (const auto& c : lc) {
                                Lane l;
                                l.id = c.laneId;
                                l.c = c;
                                p->lanes.push_back(std::move(l));
                            }
                        }
                    }
                    io->close(&mf);
                }
            }
        }
        TailReplay tr;
        bool adoptedHead = false;
        if (head.empty()) {
            // No valid head: an empty partition, or both slots torn (-3):
            // rebuild from the meta log alone (still no payload byte). A9:
            // older meta segments may be retired; the rebuild starts at the
            // first one that exists (names are deterministic: probe).
            p->mSeg = 0;
            p->dSeg = 0;
            p->nextSeg = 1;
            p->segFirstPseq = 1;
            PathBuf m0;
            pathPartitionSeg(&m0, cfg_.root.c_str(), p->pid, 'm', 0, "fsl");
            FileRef hf2;
            int64_t hsize = 0;
            if (io->open(hp.c_str(), hp.len, FLATSQL_IO_READ, FileClass::Head, &hf2) == 0) {
                hsize = io->size(hf2);
                io->close(&hf2);
            }
            if (hsize > 0 && io->probe(m0.c_str(), m0.len) != 0) {
                for (uint32_t s = 1; s < (1u << 20); s++) {
                    PathBuf ms;
                    pathPartitionSeg(&ms, cfg_.root.c_str(), p->pid, 'm', s, "fsl");
                    if (io->probe(ms.c_str(), ms.len) != 0) continue;
                    p->mSeg = p->dSeg = s;
                    p->nextSeg = s + 1;
                    p->firstLiveMSeg = s;
                    tr.anchor = true;
                    break;
                }
            }
        }
        // T3 (A12): the RETIRE set the head points at (the tail may replace it).
        if (p->retireN && !head.empty()) {
            PathBuf mp;
            pathPartitionSeg(&mp, cfg_.root.c_str(), p->pid, 'm', p->retireSeg, "fsl");
            FileRef mf;
            if (io->open(mp.c_str(), mp.len, FLATSQL_IO_READ, FileClass::Meta, &mf) == 0) {
                RetireSetHeader h;
                if (io->read(mf, &h, sizeof(h), p->retireOff) == int64_t(sizeof(h)) && h.n < (1u << 20)) {
                    std::vector<uint8_t> body(sizeof(h) + size_t(h.n) * sizeof(RetireItem));
                    if (io->read(mf, body.data(), body.size(), p->retireOff) == int64_t(body.size())) {
                        RetireSetHeader h2;
                        retireSetDecode(body.data(), body.size(), &h2, &tr.retired);
                    }
                }
                io->close(&mf);
            }
        }
        rc = replayPartitionTail(this, io, p, incFloor, &tr);
        if (rc < 0) {
            if (err) *err = "partition tail scan failed";
            return rc;
        }
        if (tr.adopted) {
            // A4: fsync d and m before adopting, then a durable checkpoint head.
            std::sort(tr.dSegs.begin(), tr.dSegs.end());
            tr.dSegs.erase(std::unique(tr.dSegs.begin(), tr.dSegs.end()), tr.dSegs.end());
            bool ok = true;
            for (uint32_t seg : tr.dSegs) {
                PathBuf dp;
                pathPartitionSeg(&dp, cfg_.root.c_str(), p->pid, 'd', seg, "fsd");
                FileRef df;
                if (io->open(dp.c_str(), dp.len, kOpenRW, FileClass::Data, &df) == 0) {
                    ok = ok && io->sync(df) == 0;
                    io->close(&df);
                }
            }
            for (uint32_t seg = (tr.dSegs.empty() ? p->mSeg : tr.dSegs.front()); seg <= p->mSeg; seg++) {
                PathBuf mp;
                pathPartitionSeg(&mp, cfg_.root.c_str(), p->pid, 'm', seg, "fsl");
                FileRef mf;
                if (io->open(mp.c_str(), mp.len, kOpenRW, FileClass::Meta, &mf) == 0) {
                    ok = ok && io->sync(mf) == 0;
                    io->close(&mf);
                }
            }
            if (!ok) {
                // Never adopt what cannot be made durable (A4): quarantine.
                p->quarantined = true;
                p->ring->state.store(kRingQuarantined);
            } else {
                adoptedBatches += tr.adopted;
                adoptedHead = true;  // the DURABLE_CKPT head is written below
            }
        }
        // A11: outputs of a merge INTENT without MERGE_DONE are discarded, and
        // r/a are cut back to the extents the last MERGE_DONE covers.
        if (p->intentGen) {
            PathBuf xp, mfp, rp, ap;
            pathPartitionRun(&xp, cfg_.root.c_str(), p->pid, p->intentSeg, p->intentGen);
            pathPartitionManifest(&mfp, cfg_.root.c_str(), p->pid, p->intentGen);
            io->unlink(xp.c_str(), xp.len, true);   // durable; nothing holds them at open
            io->unlink(mfp.c_str(), mfp.len, true);
            pathPartitionSeg(&rp, cfg_.root.c_str(), p->pid, 'r', p->intentSeg, "fsr");
            pathPartitionSeg(&ap, cfg_.root.c_str(), p->pid, 'a', p->intentSeg, "fsa");
            FileRef rf, af;
            // T3: a first merge of the segment (offset 0) created r/a; they
            // name nothing, so they go (the orphan check walks the directory).
            if (p->intentROff == 0) {
                io->unlink(rp.c_str(), rp.len, true);
            } else if (io->open(rp.c_str(), rp.len, kOpenRW, FileClass::Rows, &rf) == 0) {
                if (io->size(rf) > int64_t(p->intentROff) && io->truncate(rf, p->intentROff) >= 0) io->sync(rf);
                io->close(&rf);
            }
            if (p->intentROff == 0 && p->intentAOff == 0) {
                io->unlink(ap.c_str(), ap.len, true);
            } else if (io->open(ap.c_str(), ap.len, kOpenRW, FileClass::Attrs, &af) == 0) {
                if (io->size(af) > int64_t(p->intentAOff) && io->truncate(af, p->intentAOff) >= 0) io->sync(af);
                io->close(&af);
            }
            p->intentGen = 0;
            p->intentSeg = 0;
        }
        // T3: compaction outputs of an INTENT_COMPACT without SWAP (A11), and
        // every file of the persisted RETIRE set (A12), are unlinked; a
        // durable head then stops naming either.
        bool rewriteHead = adoptedHead;
        if (p->cIntentGen) {
            unlinkCompactOutputs(io, cfg_.root.c_str(), p->pid, p->cIntentSeg, p->cIntentGen);
            p->cIntentGen = 0;
            p->cIntentSeg = 0;
            rewriteHead = true;
        }
        if (!tr.retired.empty() || p->retireN) {
            uint32_t n = 0;
            rc = partitionOpenReclaim(io, cfg_.root.c_str(), p, tr.retired, &n);
            if (rc < 0) {
                if (err) *err = "retired file unlink failed";
                return rc;
            }
            if (p->retired.empty()) {
                p->retireN = 0;
                p->retireSeg = 0;
                p->retireOff = 0;
            }
            rewriteHead = true;
        }
        rc = partitionOpenLedger(io, cfg_.root.c_str(), p);
        if (rc < 0) {
            if (err) *err = "partition manifest unreadable";
            return rc;
        }
        if (rewriteHead && !p->quarantined) {
            FileRef h;
            rc = io->open(hp.c_str(), hp.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS, FileClass::Head,
                          &h);
            if (rc >= 0) {
                uint8_t slot[kHeadSlotBytes];
                p->headGen++;
                uint32_t used;
                encodePartitionHead(p, slot, &used, true);
                const uint64_t at = (p->headGen % 2) * kHeadSlotBytes;
                rc = io->write(h, slot, used, at);
                if (rc >= 0) rc = io->sync(h);
                io->close(&h);
                if (at + used > p->hExtent) p->hExtent = at + used;
                partitionPublishDisk(p);
            }
            if (rc < 0) {
                if (err) *err = "partition checkpoint head write failed";
                return rc;
            }
        }
        p->firstLiveMSegPub.store(p->firstLiveMSeg);
        p->lastDurableHeadNs = monoNs();
        p->pub.commitSeq = p->commitSeq;
        p->pub.pseqHi = p->pseqHi;
        p->pub.nL0 = p->nL0;
        std::memcpy(p->pub.l0, p->l0, sizeof(L0DirEntry) * p->nL0);
        p->durablePseqHi.store(p->pseqHi);
        p->durableCommitSeq.store(p->commitSeq);
        p->durableMEnd.store(p->mEnd);
        if (p->type) {
            auto it = p->type->labeled.find(p->pid);
            p->labeledThrough.store(it == p->type->labeled.end() ? 0 : it->second);
        }
        p->lastCkptNs = monoNs();
        p->lastActivityNs = monoNs();
    }
    return 0;
}

// ---------------------------------------------------------------------------
// Registration (rare; the registration lock is not on any record path)
// ---------------------------------------------------------------------------
int32_t Engine::registerType(const std::vector<uint8_t>& config, std::string* err) {
    auto cfg = std::make_shared<TypeConfig>();
    const std::string perr = cfg->parse(config.data(), config.size());
    if (!perr.empty()) {
        if (err) *err = perr;
        return FLATSQL_IO_ERR_GENERIC;
    }
    const std::string key(reinterpret_cast<const char*>(cfg->fid()), 4);
    std::unique_lock<std::mutex> lk(regMutex_);
    uint64_t t0 = monoNs();
    auto hold = [&] {
        if (cfg_.lockStats) lockHist_.record(monoNs() - t0);
    };
    for (;;) {
        auto pit = pendingReg_.find("t" + key);
        if (pit == pendingReg_.end()) break;
        std::shared_ptr<PendingReg> pr = pit->second;
        hold();
        regCv_.wait(lk, [&] { return pr->ready; });
        t0 = monoNs();
    }
    TypeOwner* existing = type(cfg->fid());
    if (existing && existing->cfg->fingerprint() == cfg->fingerprint()) {
        hold();
        return 0;
    }
    if (existing) {
        if (err) *err = "schema change of a registered type is a separate operation";
        hold();
        return FLATSQL_IO_ERR_GENERIC;
    }
    auto pr = std::make_shared<PendingReg>();
    pendingReg_["t" + key] = pr;
    hold();
    lk.unlock();
    // Durable config, then the registry frame; no lock across either fsync.
    IoCtx* io = openIo_;
    PathBuf cp;
    pathTypeConfig(&cp, cfg_.root.c_str(), cfg->fid(), cfg->fingerprint());
    FileRef f;
    int32_t rc = io->open(cp.c_str(), cp.len,
                          kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC | FLATSQL_IO_CREATE_PARENTS,
                          FileClass::Config, &f);
    if (rc >= 0) {
        std::vector<uint8_t> file(16 + config.size());
        putU32(file.data(), kMagicTypeConfig);
        putU32(file.data() + 4, 1);
        putU32(file.data() + 8, uint32_t(config.size()));
        putU32(file.data() + 12, crc32c(config.data(), config.size()));
        std::memcpy(file.data() + 16, config.data(), config.size());
        rc = io->write(f, file.data(), file.size(), 0);
        if (rc >= 0) rc = io->sync(f);
        io->close(&f);
    }
    TypeEntry te;
    std::memcpy(te.fid, cfg->fid(), 4);
    te.schemaName = cfg->schemaName();
    te.configFp = cfg->fingerprint();
    const std::vector<uint8_t> payload = Registry::encodeType(te);
    if (rc >= 0) rc = registry_.writeFrame(kRegTypeAdd, payload);
    if (rc >= 0) rc = registry_.syncLog();
    lk.lock();
    t0 = monoNs();
    Registry::HeadSlot regHead;
    if (rc >= 0) registry_.applyFrameDeferHead(kRegTypeAdd, payload, &regHead);
    if (rc >= 0) {
        std::unique_ptr<TypeOwner> t(new TypeOwner());
        std::memcpy(t->fid, te.fid, 4);
        t->cfg = cfg;
        t->noticeCap = cfg_.noticeQueue;
        t->notices.reset(new std::atomic<uint32_t>[t->noticeCap ? t->noticeCap : 1]);
        t->mSeg = 0;
        t->gSeg = 0;
        TypeOwner* raw = t.get();
        typeByFid_[fidU32(te.fid)] = raw;
        typeStore_.push_back(std::move(t));
        const uint8_t w = leastLoadedWriter();
        writers_[w]->pinned_.fetch_add(1);
        raw->ownerWriter.store(w);
        if (!started_) {
            writers_[w]->types_.push_back(raw);
        } else {
            Cmd c;
            c.kind = kCmdAdoptType;
            c.ptr = raw;
            while (!writers_[w]->mailbox().push(c)) cpuRelax();
            writers_[w]->ring();
        }
    }
    pr->ready = true;
    pr->rc = rc;
    pendingReg_.erase("t" + key);
    regCv_.notify_all();
    hold();
    lk.unlock();
    if (rc >= 0 && regHead.used) registry_.writeHeadSlot(regHead);  // hint head, outside the lock
    return rc;
}

int32_t Engine::registerPartition(const uint8_t* peer, size_t peerLen, const uint8_t fid[4], uint32_t* pidOut) {
    const std::string token = producerToken(peer, peerLen);
    std::string key = token;
    key.append(reinterpret_cast<const char*>(fid), 4);
    std::unique_lock<std::mutex> lk(regMutex_);
    uint64_t t0 = monoNs();
    auto hold = [&] {
        if (cfg_.lockStats) lockHist_.record(monoNs() - t0);
    };
    for (;;) {
        auto it = pidByKey_.find(key);
        if (it != pidByKey_.end()) {
            *pidOut = it->second;
            hold();
            return 0;
        }
        auto pit = pendingReg_.find("p" + key);
        if (pit == pendingReg_.end()) break;
        std::shared_ptr<PendingReg> pr = pit->second;
        hold();
        regCv_.wait(lk, [&] { return pr->ready; });
        t0 = monoNs();
        if (pr->rc < 0) {
            hold();
            return pr->rc;
        }
    }
    TypeOwner* t = type(fid);
    if (!t) {
        hold();
        return FLATSQL_IO_ERR_NOENT;
    }
    PartitionEntry e;
    // pid = 1 + the largest pid durable or in flight (A10: never reused, even
    // when a registration fails after its frame was written).
    e.pid = std::max(registry_.maxPid(), reservedPid_) + 1;
    if (e.pid >= partsCap_) {
        hold();
        return FLATSQL_IO_ERR_NOSPACE;
    }
    std::memcpy(e.fid, fid, 4);
    e.token = token;
    std::string typeName = t->cfg->schemaName();
    const size_t dot = typeName.find('.');
    if (dot != std::string::npos) typeName = typeName.substr(0, dot);
    e.sqlName = "sds_p_" + token + "__" + typeName;
    e.schemaFp = t->cfg->fingerprint();
    e.ordinal = e.pid - 1;
    e.ctimeMs = nowMs();
    const std::vector<uint8_t> payload = Registry::encodePartition(e);
    reservedPid_ = e.pid;
    auto pr = std::make_shared<PendingReg>();
    pendingReg_["p" + key] = pr;
    hold();
    lk.unlock();
    // Intent rule (§4.7): the frame is durable before p/<pid>/ exists. No
    // lock is held across the frame's pwrite or the fsync.
    int32_t rc = registry_.writeFrame(kRegPartitionAdd, payload);
    if (rc >= 0) rc = registry_.syncLog();
    if (rc >= 0) {
        PathBuf hp;
        pathPartition(&hp, cfg_.root.c_str(), e.pid, "h.fsh");
        FileRef h;
        // A10: created with EXCL; EEXIST (a reused pid) fails the REGISTER.
        rc = openIo_->open(hp.c_str(), hp.len,
                           kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_EXCL | FLATSQL_IO_CREATE_PARENTS,
                           FileClass::Head, &h);
        if (rc >= 0) openIo_->close(&h);
    }
    lk.lock();
    t0 = monoNs();
    Registry::HeadSlot regHead;
    if (rc >= 0) registry_.applyFrameDeferHead(kRegPartitionAdd, payload, &regHead);
    if (rc >= 0) {
        const uint8_t w = leastLoadedWriter();
        Partition* p = makePartition(e, w);
        p->mSeg = 0;
        p->dSeg = 0;
        p->nextSeg = 1;
        p->segFirstPseq = 1;
        p->lanesLoaded = false;
        p->lastCkptNs = monoNs();
        p->lastActivityNs = monoNs();
        writers_[w]->pinned_.fetch_add(1);
        if (!started_) {
            writers_[w]->owned_.push_back(p);
            writers_[w]->ownedCount_.fetch_add(1);
        } else {
            Cmd c;
            c.kind = kCmdAdoptPartition;
            c.a = e.pid;
            c.b = 0;
            while (!writers_[w]->mailbox().push(c)) cpuRelax();
            writers_[w]->ring();
        }
        *pidOut = e.pid;
    }
    pr->ready = true;
    pr->rc = rc;
    pendingReg_.erase("p" + key);
    regCv_.notify_all();
    hold();
    lk.unlock();
    if (rc >= 0 && regHead.used) registry_.writeHeadSlot(regHead);  // hint head, outside the lock
    return rc;
}

}  // namespace ps
}  // namespace flatsql
