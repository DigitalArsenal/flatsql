// FlatSQL partition store: instance C ABI (see ps/flatsql_ps.h).
#include "flatsql/ps/flatsql_ps.h"

#include <cstddef>
#include <cstring>
#include <memory>

#include "flatsql/ps/platform.h"
#include "flatsql/ps/lane.h"
#include "flatsql/ps/writer.h"

namespace {
std::unique_ptr<flatsql::ps::Engine>& instance() {
    static std::unique_ptr<flatsql::ps::Engine> e;
    return e;
}
std::unique_ptr<flatsql::ps::ReaderInstance>& reader() {
    static std::unique_ptr<flatsql::ps::ReaderInstance> r;
    return r;
}
inline uint32_t addr(const void* p) { return uint32_t(reinterpret_cast<uintptr_t>(p)); }

int32_t initReader(int32_t role, const uint8_t* cfg, int32_t cfgLen) {
    using namespace flatsql::ps;
    if (reader() || instance()) return FLATSQL_IO_ERR_BUSY;
    ReaderConfig c;
    c.cls = role == FLATSQL_PS_ROLE_READER_BULK      ? LaneClass::Bulk
            : role == FLATSQL_PS_ROLE_READER_SANDBOX ? LaneClass::Sandbox
                                                     : LaneClass::Interactive;
    int32_t off = 0;
    while (cfg && off + 6 <= cfgLen) {
        const uint16_t tag = getU16(cfg + off);
        const uint32_t len = getU32(cfg + off + 2);
        if (off + 6 + int64_t(len) > cfgLen) return FLATSQL_IO_ERR_GENERIC;
        const uint8_t* v = cfg + off + 6;
        auto u32 = [&]() { return len >= 4 ? getU32(v) : 0u; };
        auto u64 = [&]() { return len >= 8 ? getU64(v) : 0ull; };
        switch (tag) {
            case 1: c.root.assign(reinterpret_cast<const char*>(v), len); break;
            case 20: c.lanes = u32(); break;
            case 21: c.arenaBytes = u64(); break;
            case 22: c.cacheBytes = u64(); break;
            case 23: c.maxParked = u32(); break;
            case 24: c.slots = u32(); break;
            case 25: c.reqBytes = u32(); break;
            case 26: c.ringBytes = u32(); break;
            case 27: c.laneCacheBytes = u64(); break;
            case 28: c.stackBytes = u64(); break;
            case 29: c.sandboxMaxRowsExamined = u64(); break;
            case 30: c.sandboxMaxBytesRead = u64(); break;
            default: break;
        }
        off += 6 + int32_t(len);
    }
    if (c.root.empty()) return FLATSQL_IO_ERR_GENERIC;
    std::string err;
    return ReaderInstance::open(c, &reader(), &err);
}
}  // namespace

using namespace flatsql::ps;

extern "C" {

int32_t flatsql_ps_init(int32_t role, const uint8_t* cfg, int32_t cfgLen) {
    if (role >= FLATSQL_PS_ROLE_READER_INTERACTIVE && role <= FLATSQL_PS_ROLE_READER_SANDBOX)
        return initReader(role, cfg, cfgLen);
    if (role != FLATSQL_PS_ROLE_WRITER) return FLATSQL_IO_ERR_GENERIC;
    if (instance() || reader()) return FLATSQL_IO_ERR_BUSY;
    EngineConfig c;
    int32_t off = 0;
    while (cfg && off + 6 <= cfgLen) {
        const uint16_t tag = getU16(cfg + off);
        const uint32_t len = getU32(cfg + off + 2);
        if (off + 6 + int64_t(len) > cfgLen) return FLATSQL_IO_ERR_GENERIC;
        const uint8_t* v = cfg + off + 6;
        auto u32 = [&]() { return len >= 4 ? getU32(v) : 0u; };
        auto u64 = [&]() { return len >= 8 ? getU64(v) : 0ull; };
        switch (tag) {
            case 1: c.root.assign(reinterpret_cast<const char*>(v), len); break;
            case 2: c.writers = u32(); break;
            case 3: c.syncThreads = u32(); break;
            case 4: c.poolBytes = u64(); break;
            case 5: c.slabBytes = u32(); break;
            case 6: c.defaultRingCap = u64(); break;
            case 7: c.cooperative = len && v[0]; break;
            case 8: c.create = len && v[0]; break;
            case 9: c.requireMigrated = len && v[0]; break;
            case 10: c.zeroFillStep = u64(); break;
            case 11: c.arenaBytes = u64(); break;
            case 12: c.sealBytes = u64(); break;
            case 13: c.quotaBytes = u64(); break;
            case 14: c.ballastBytes = u64(); break;
            case 15: c.reclaimGraceMs = u64(); break;
            case 16: c.autoCompact = len && v[0]; break;
            case 17: c.compactThreads = u32(); break;
            case 18: c.commitJournal = len && v[0]; break;
            case 19: c.maxEntryBytes = u64(); break;
            default: break;
        }
        off += 6 + int32_t(len);
    }
    if (c.root.empty()) return FLATSQL_IO_ERR_GENERIC;
    std::string err;
    const int32_t rc = Engine::open(c, &instance(), &err);
    if (rc >= 0 && c.quotaBytes) instance()->setQuota(c.quotaBytes);
    return rc;
}

int32_t flatsql_ps_reader_gate(double oldestStartNs) {
    Engine* e = instance().get();
    if (!e) return FLATSQL_IO_ERR_BADHANDLE;
    e->setHostReaderGate(oldestStartNs < 0 ? UINT64_MAX : uint64_t(oldestStartNs));
    return 0;
}

int32_t flatsql_ps_set_quota(double bytes) {
    Engine* e = instance().get();
    if (!e) return FLATSQL_IO_ERR_BADHANDLE;
    e->setQuota(bytes > 0 ? uint64_t(bytes) : 0);
    return 0;
}

int32_t flatsql_ps_start(void) {
    if (reader()) return reader()->start();
    return instance() ? instance()->start() : FLATSQL_IO_ERR_BADHANDLE;
}

int32_t flatsql_ps_reader_layout(FlatsqlPsReaderLayout* out) {
    ReaderInstance* r = reader().get();
    if (!r || !out) return FLATSQL_IO_ERR_BADHANDLE;
    std::memset(out, 0, sizeof(*out));
    const ReaderConfig& c = r->config();
    out->version = 1;
    out->nLanes = r->laneCount();
    out->nSlots = r->slotCount();
    out->slotBase = addr(r->slot(0));
    out->slotStride = uint32_t(r->slotStride());
    out->headerSize = sizeof(SlotHeader);
    out->reqBytes = c.reqBytes;
    out->ringBytes = c.ringBytes;
    out->offState = offsetof(SlotHeader, state);
    out->offCancel = offsetof(SlotHeader, cancel);
    out->offOutSeq = offsetof(SlotHeader, outSeq);
    out->offSpaceSeq = offsetof(SlotHeader, spaceSeq);
    out->offFlags = offsetof(SlotHeader, flags);
    out->offLane = offsetof(SlotHeader, lane);
    out->offReqId = offsetof(SlotHeader, reqId);
    out->offSqlLen = offsetof(SlotHeader, sqlLen);
    out->offParamsLen = offsetof(SlotHeader, paramsLen);
    out->offReqCap = offsetof(SlotHeader, reqCap);
    out->offRingCap = offsetof(SlotHeader, ringCap);
    out->offMaxRowsExamined = offsetof(SlotHeader, maxRowsExamined);
    out->offMaxBytesRead = offsetof(SlotHeader, maxBytesRead);
    out->offMaxResultRows = offsetof(SlotHeader, maxResultRows);
    out->offMaxResultBytes = offsetof(SlotHeader, maxResultBytes);
    out->offRingHead = offsetof(SlotHeader, ringHead);
    out->offRingTail = offsetof(SlotHeader, ringTail);
    out->offStatus = offsetof(SlotHeader, status);
    out->offErrLen = offsetof(SlotHeader, errLen);
    out->offRowsOut = offsetof(SlotHeader, rowsOut);
    out->offRowsExamined = offsetof(SlotHeader, rowsExamined);
    out->offBytesRead = offsetof(SlotHeader, bytesRead);
    out->offIndexEntries = offsetof(SlotHeader, indexEntries);
    out->offFenceReads = offsetof(SlotHeader, fenceReads);
    out->offSubmitNs = offsetof(SlotHeader, submitNs);
    out->offStartNs = offsetof(SlotHeader, startNs);
    out->offEndNs = offsetof(SlotHeader, endNs);
    out->offErr = offsetof(SlotHeader, err);
    out->queueCells = addr(r->queueCells());
    out->queueMask = uint32_t(r->queueMask());
    out->queueEnq = addr(r->queueEnq());
    out->queueDeq = addr(r->queueDeq());
    out->stopWord = addr(&r->stopWord());
    for (uint32_t i = 0; i < r->laneCount() && i < 64; i++) {
        out->laneDoorbell[i] = addr(&r->laneShared(i).doorbell);
        out->laneState[i] = addr(&r->laneShared(i).state);
        out->laneAnnounce[i] = addr(&r->laneShared(i).announce);
    }
    return int32_t(sizeof(*out));
}

int32_t flatsql_ps_layout(FlatsqlPsLayout* out) {
    Engine* e = instance().get();
    if (!e || !out) return FLATSQL_IO_ERR_BADHANDLE;
    std::memset(out, 0, sizeof(*out));
    out->version = 1;
    out->nWriters = e->writerCount();
    out->ringDescSize = sizeof(RingDesc);
    out->offTail = offsetof(RingDesc, tail);
    out->offHead = offsetof(RingDesc, head);
    out->offAckedRseq = offsetof(RingDesc, ackedRseq);
    out->offAckGen = offsetof(RingDesc, ackGen);
    out->offMapGen = offsetof(RingDesc, mapGen);
    out->offWantPage = offsetof(RingDesc, wantPage);
    out->offProdBusy = offsetof(RingDesc, prodBusy);
    out->offReclaim = offsetof(RingDesc, reclaim);
    out->offProdWaiting = offsetof(RingDesc, prodWaiting);
    out->offState = offsetof(RingDesc, state);
    out->offOwnerWord = offsetof(RingDesc, ownerWordV);
    out->offHandoffTo = offsetof(RingDesc, handoffTo);
    out->offRejectHead = offsetof(RingDesc, rejectHead);
    out->offRejectTail = offsetof(RingDesc, rejectTail);
    out->offRejects = offsetof(RingDesc, rejects);
    out->offPages = uint32_t(sizeof(RingDesc));
    out->offCap = offsetof(RingDesc, cap);
    out->offMaxEntry = offsetof(RingDesc, maxEntry);
    out->offNSlots = offsetof(RingDesc, nSlots);
    out->offSlabBytes = offsetof(RingDesc, slabBytes);
    out->offNextRseq = offsetof(RingDesc, nextRseq);
    out->offMappedPages = offsetof(RingDesc, mappedPages);
    out->poolBase = uint32_t(reinterpret_cast<uintptr_t>(e->pool().ptr(0)));
    out->slabBytes = e->pool().slabBytes();
    out->entryHeaderSize = sizeof(EntryHeader);
    for (uint32_t i = 0; i < e->writerCount() && i < 64; i++) {
        out->writerSeq[i] = uint32_t(reinterpret_cast<uintptr_t>(&e->writer(i)->doorbellSeq()));
        out->writerSleeping[i] = uint32_t(reinterpret_cast<uintptr_t>(&e->writer(i)->sleeping()));
    }
    return int32_t(sizeof(*out));
}

int32_t flatsql_ps_wake(uint32_t* addr, int32_t n) {
    wakeU32(reinterpret_cast<std::atomic<uint32_t>*>(addr), n);
    return 0;
}

int32_t flatsql_ps_pump(double budgetUs) {
    return instance() ? instance()->pump(uint64_t(budgetUs)) : FLATSQL_IO_ERR_BADHANDLE;
}

int32_t flatsql_ps_stop(double deadlineMs) {
    if (reader()) {
        const int32_t rc = reader()->stop(uint64_t(deadlineMs));
        reader().reset();
        return rc;
    }
    if (!instance()) return FLATSQL_IO_ERR_BADHANDLE;
    const int32_t rc = instance()->stop(uint64_t(deadlineMs));
    instance().reset();
    return rc;
}

int32_t flatsql_ps_stats(uint8_t* out, int32_t len) {
    if (ReaderInstance* r = reader().get()) {
        const ReaderStats st = r->stats();
        const uint64_t v[] = {st.statements, st.parks,    st.needsBulk, st.noMem,
                              st.snapshotGone, st.cancelled, st.timeouts, st.errors};
        const int32_t n = int32_t(sizeof(v));
        if (!out || len < n) return n;
        for (size_t i = 0; i < sizeof(v) / 8; i++) putU64(out + i * 8, v[i]);
        return n;
    }
    Engine* e = instance().get();
    if (!e) return FLATSQL_IO_ERR_BADHANDLE;
    const EngineStats s = e->stats();
    const uint64_t v[] = {s.commits, s.syncRounds, s.iterationsWithCommit, s.rowsAppended,
                          s.dedupeHits, s.retags, s.tombs, s.rejects, s.merges, s.seals,
                          s.typeCommits, s.firstLabels, s.repeatLabels, s.promotions,
                          s.noticesDropped, s.framesParsedAtOpen, s.openReadBytes,
                          s.openDataBytes, s.openMetaBytes, s.adoptedBatches, s.poolSlabsInUse,
                          s.poolSlabsPeak, s.poolCommittedBytes, s.committedBytes,
                          s.migratedGseqs, s.migratedGseqFallbacks, s.splits, s.unsplits,
                          s.prepPrepared, s.prepUsed, s.prepStolen, s.prepWasted, s.prepHinted,
                          s.l0FullStalls, s.arrivalSegsCompacted, s.arrivalEntriesDropped};
    const int32_t n = int32_t(sizeof(v));
    if (!out || len < n) return n;
    for (size_t i = 0; i < sizeof(v) / 8; i++) putU64(out + i * 8, v[i]);
    return n;
}

int32_t flatsql_ps_register_type(const uint8_t* cfg, int32_t cfgLen) {
    Engine* e = instance().get();
    if (!e || !cfg || cfgLen <= 0) return FLATSQL_IO_ERR_BADHANDLE;
    std::vector<uint8_t> c(cfg, cfg + cfgLen);
    std::string err;
    return e->registerType(c, &err);
}

int32_t flatsql_ps_register_partition(const uint8_t* peer, int32_t peerLen, const uint8_t* fid) {
    Engine* e = instance().get();
    if (!e || !fid || peerLen < 0) return FLATSQL_IO_ERR_BADHANDLE;
    uint32_t pid = 0;
    const int32_t rc = e->registerPartition(peer, size_t(peerLen), fid, &pid);
    return rc < 0 ? rc : int32_t(pid);
}

double flatsql_ps_ring(int32_t pid) {
    Engine* e = instance().get();
    if (!e || pid <= 0) return 0;
    return double(reinterpret_cast<uintptr_t>(e->ring(uint32_t(pid))));
}

}  // extern "C"
