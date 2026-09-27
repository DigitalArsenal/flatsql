// FlatSQL partition store: instance C ABI (see ps/flatsql_ps.h).
#include "flatsql/ps/flatsql_ps.h"

#include <cstddef>
#include <cstring>
#include <memory>

#include "flatsql/ps/platform.h"
#include "flatsql/ps/writer.h"

namespace {
std::unique_ptr<flatsql::ps::Engine>& instance() {
    static std::unique_ptr<flatsql::ps::Engine> e;
    return e;
}
}  // namespace

using namespace flatsql::ps;

extern "C" {

int32_t flatsql_ps_init(int32_t role, const uint8_t* cfg, int32_t cfgLen) {
    if (role != FLATSQL_PS_ROLE_WRITER) return FLATSQL_IO_ERR_GENERIC;
    if (instance()) return FLATSQL_IO_ERR_BUSY;
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
            default: break;
        }
        off += 6 + int32_t(len);
    }
    if (c.root.empty()) return FLATSQL_IO_ERR_GENERIC;
    std::string err;
    return Engine::open(c, &instance(), &err);
}

int32_t flatsql_ps_start(void) {
    return instance() ? instance()->start() : FLATSQL_IO_ERR_BADHANDLE;
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
    if (!instance()) return FLATSQL_IO_ERR_BADHANDLE;
    const int32_t rc = instance()->stop(uint64_t(deadlineMs));
    instance().reset();
    return rc;
}

int32_t flatsql_ps_stats(uint8_t* out, int32_t len) {
    Engine* e = instance().get();
    if (!e) return FLATSQL_IO_ERR_BADHANDLE;
    const EngineStats s = e->stats();
    const uint64_t v[] = {s.commits, s.syncRounds, s.iterationsWithCommit, s.rowsAppended,
                          s.dedupeHits, s.retags, s.tombs, s.rejects, s.merges, s.seals,
                          s.typeCommits, s.firstLabels, s.repeatLabels, s.promotions,
                          s.noticesDropped, s.framesParsedAtOpen, s.openReadBytes,
                          s.openDataBytes, s.openMetaBytes, s.adoptedBatches, s.poolSlabsInUse,
                          s.poolSlabsPeak, s.poolCommittedBytes, s.committedBytes};
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
