// FlatSQL partition store: the quota planner, the space emergency and the
// ballast (design §13, A13, owner decision §22.4-3; see ps/quota.h).
#include <algorithm>
#include <set>
#include <tuple>

#include "internal.h"

namespace flatsql {
namespace ps {

namespace {
constexpr int32_t kOpenRW = FLATSQL_IO_READ | FLATSQL_IO_WRITE;

struct Eviction {
    uint32_t pid = 0;
    uint32_t seg = 0;
    uint32_t lastSeg = 0;
    uint64_t bytes = 0;
    std::atomic<int32_t> ticket{1};
    SwapResult compact;
    uint8_t phase = 0;  // 0 tombstones in flight, 1 compaction in flight
};
}  // namespace

struct QuotaState {
    uint64_t lastNs = 0;
    std::vector<std::unique_ptr<Eviction>> inflight;
    std::set<std::tuple<uint32_t, uint32_t, uint32_t>> tried;  // (pid, seg, cgen) already evicted
    std::atomic<bool> noSpace{false};
    bool emergency = false;
    uint64_t emergencyCap = 0;
    uint64_t passes = 0, evicted = 0, emergencies = 0, releases = 0, restores = 0;
    std::atomic<uint64_t> usage{0}, cap{0};
    // Ballast (A13).
    FileRef ballast;
    uint64_t ballastLen = 0;
    bool ballastReleased = false;
};

// ---------------------------------------------------------------------------
// Published inputs
// ---------------------------------------------------------------------------
uint64_t segmentDiskBytes(const Partition* p, const SegmentInfo& s) {
    uint64_t b = 0;
    if (s.cgen) {
        b += ledgerSize(p, 'D', s.seg, s.cgen) + ledgerSize(p, 'R', s.seg, s.cgen) + ledgerSize(p, 'A', s.seg, s.cgen);
    } else {
        b += ledgerSize(p, 'd', s.seg, 0) + ledgerSize(p, 'r', s.seg, 0) + ledgerSize(p, 'a', s.seg, 0);
    }
    for (const auto& r : s.runs) b += r.fileLen;
    return b;
}

void partitionPublishSummary(Partition* p) {
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;  // a maintenance event (seal, merge, SWAP)
    std::vector<SegSummary> v;
    v.reserve(p->segs.size());
    for (const auto& s : p->segs) {
        if (!s.sealed || !s.firstPseq || s.endPseq <= s.firstPseq) continue;
        SegSummary x;
        x.pid = p->pid;
        x.seg = s.seg;
        x.lastSeg = s.lastSeg ? s.lastSeg : s.seg;
        x.cgen = s.cgen;
        x.minArrival = s.minArrival;
        x.maxArrival = s.maxArrival;
        x.firstPseq = s.firstPseq;
        x.endPseq = s.endPseq;
        x.bytes = segmentDiskBytes(p, s);
        x.empty = s.empty;
        v.push_back(x);
    }
    uint64_t retired = 0;
    for (const auto& r : p->retired) retired += r.it.size;
    {
        std::lock_guard<std::mutex> g(p->sumMu);
        p->summary.swap(v);
    }
    p->retiredBytesPub.store(retired, std::memory_order_relaxed);
    tHotPathDepth = saved;
}

// ---------------------------------------------------------------------------
// Ballast
// ---------------------------------------------------------------------------
namespace {
void ballastPath(PathBuf* out, const char* root) { pathStore(out, root, "ballast"); }

// Grows the ballast by up to `step` bytes of zeros. Returns 1 when complete,
// 0 when more is needed, < 0 on an error (NOSPACE: space has not returned).
int32_t ballastGrow(IoCtx* io, const char* root, QuotaState& q, uint64_t want, uint64_t step) {
    if (!want) return 1;
    if (!q.ballast.valid()) {
        PathBuf bp;
        ballastPath(&bp, root);
        const int32_t rc = io->open(bp.c_str(), bp.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS,
                                    FileClass::Store, &q.ballast);
        if (rc < 0) return rc;
        const int64_t n = io->size(q.ballast);
        q.ballastLen = n > 0 ? uint64_t(n) : 0;
    }
    if (q.ballastLen >= want) return 1;
    const uint64_t n = std::min(step, want - q.ballastLen);
    int32_t rc = io->writeZeros(q.ballast, q.ballastLen, n);
    if (rc >= 0) rc = io->sync(q.ballast);
    if (rc < 0) {
        // Give back whatever this attempt reserved.
        io->truncate(q.ballast, q.ballastLen);
        return rc;
    }
    q.ballastLen += n;
    return q.ballastLen >= want ? 1 : 0;
}
}  // namespace

int32_t engineOpenQuota(Engine* e, IoCtx* io, std::string* err) {
    auto q = std::make_shared<QuotaState>();
    e->quotaAttach(q);
    const EngineConfig& cfg = e->config();
    if (!cfg.ballastBytes) return 0;
    for (;;) {
        const int32_t rc = ballastGrow(io, cfg.root.c_str(), *q, cfg.ballastBytes, 4ull << 20);
        if (rc == 1) break;
        if (rc < 0) {
            if (rc == FLATSQL_IO_ERR_NOSPACE) {
                // A full disk at open: start in the emergency (evict first).
                q->ballastReleased = true;
                e->signalNoSpace();
                break;
            }
            if (err) *err = "ballast create failed";
            return rc;
        }
    }
    io->close(&q->ballast);
    return 0;
}

// ---------------------------------------------------------------------------
// The planner (writer 0's maintenance)
// ---------------------------------------------------------------------------
void Engine::signalNoSpace() {
    if (quota_) quota_->noSpace.store(true, std::memory_order_release);
}

QuotaStats Engine::quotaStats() const {
    QuotaStats s;
    if (!quota_) return s;
    s.passes = quota_->passes;
    s.segmentsEvicted = quota_->evicted;
    s.emergencies = quota_->emergencies;
    s.ballastReleases = quota_->releases;
    s.ballastRestores = quota_->restores;
    s.emergency = spaceEmergency();
    s.usageBytes = quota_->usage.load();
    s.capBytes = quota_->cap.load();
    return s;
}

namespace {
// An emergency wave evicts to the low-water mark, and at least the ballast's
// size (it must come back before ingest resumes), but never more than half
// of the store in one episode.
uint64_t emergencyCapOf(uint64_t usage, const EngineConfig& cfg) {
    const uint64_t low = uint64_t(double(usage) * cfg.quotaLowWater);
    const uint64_t roomed = usage > cfg.ballastBytes ? usage - cfg.ballastBytes : 0;
    return std::max(usage / 2, std::min(low, roomed));
}
}  // namespace

void quotaStep(Writer* w) {
    Engine* e = w->engine();
    QuotaState* qp = e->quotaState();
    if (!qp) return;
    QuotaState& q = *qp;
    const EngineConfig& cfg = e->config();
    const uint64_t now = monoNs();
    const bool noSpace = q.noSpace.exchange(false, std::memory_order_acq_rel);
    if (!noSpace && now - q.lastNs < uint64_t(cfg.quotaIntervalMs) * 1000000ull) return;
    q.lastNs = now;
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    struct Restore {
        int d;
        ~Restore() { tHotPathDepth = d; }
    } restore{saved};
    // Usage (§13): what the partitions and the type logs hold, less files
    // that go once readers let go.
    const uint32_t maxPid = e->maxPid();
    uint64_t usage = 0;
    for (uint32_t pid = 1; pid <= maxPid; pid++) {
        const Partition* p = e->partition(pid);
        if (!p) continue;
        const uint64_t d = p->diskBytesPub.load(std::memory_order_relaxed);
        const uint64_t r = p->retiredBytesPub.load(std::memory_order_relaxed);
        usage += d > r ? d - r : 0;
    }
    {
        uint64_t tr = 0;
        const uint64_t td = e->typeDiskBytes(&tr);
        usage += td > tr ? td - tr : 0;
    }
    q.usage.store(usage);
    if (noSpace) {
        // A13: tombstones, SWAPs and reclamation need room: the ballast goes.
        // Later ENOSPCs of the same episode (the eviction's own commits
        // racing for the ballast's room) change nothing: the waves go on.
        if (!q.emergency) {
            q.emergency = true;
            q.emergencies++;
            q.emergencyCap = emergencyCapOf(usage, cfg);
            e->setSpaceEmergency(true);
        }
        if (!q.ballastReleased && cfg.ballastBytes) {
            PathBuf bp;
            ballastPath(&bp, w->eng_root());
            w->io().close(&q.ballast);
            if (w->io().unlink(bp.c_str(), bp.len, true) >= 0) {
                q.ballastReleased = true;
                q.ballastLen = 0;
                q.releases++;
            }
        }
    }
    // Advance the wave in flight.
    for (size_t i = 0; i < q.inflight.size();) {
        Eviction& ev = *q.inflight[i];
        if (ev.phase == 0 && ev.ticket.load(std::memory_order_acquire) == 0) {
            ev.phase = 1;
            ev.compact.requestSeg = ev.seg;
            ev.compact.requestSegEnd = ev.lastSeg;
            if (e->swapSegment(ev.pid, &ev.compact) != 0) {
                q.inflight.erase(q.inflight.begin() + long(i));
                continue;
            }
        }
        if (ev.phase == 1 && ev.compact.remaining.load(std::memory_order_acquire) == 0) {
            q.inflight.erase(q.inflight.begin() + long(i));
            continue;
        }
        i++;
    }
    uint64_t cap = e->quotaCap();
    if (q.emergency) cap = cap ? std::min(cap, q.emergencyCap) : q.emergencyCap;
    q.cap.store(cap);
    if (!q.inflight.empty()) return;  // one wave at a time
    if (!cap || usage <= cap) {
        if (q.emergency) {
            // Space is back: the ballast first, then ingest resumes.
            const int32_t rc = cfg.ballastBytes
                                   ? ballastGrow(&w->io(), w->eng_root(), q, cfg.ballastBytes, 4ull << 20)
                                   : 1;
            if (rc == 1) {
                w->io().close(&q.ballast);
                if (q.ballastReleased) q.restores++;
                q.ballastReleased = false;
                q.emergency = false;
                e->setSpaceEmergency(false);
                for (uint32_t k = 0; k < e->writerCount(); k++) e->writer(k)->ring();
            } else if (rc < 0) {
                // Still full (another tenant of the disk): one more wave.
                q.emergencyCap = emergencyCapOf(usage, cfg);
            }
        }
        if (!cap || usage <= uint64_t(double(cap) * cfg.quotaLowWater)) q.tried.clear();
        return;
    }
    // Evict whole sealed segments in arrival order down to the low-water mark.
    const uint64_t target = q.emergency ? cap : uint64_t(double(cap) * cfg.quotaLowWater);
    std::vector<SegSummary> all;
    for (uint32_t pid = 1; pid <= maxPid; pid++) {
        Partition* p = e->partition(pid);
        if (!p) continue;
        std::lock_guard<std::mutex> g(p->sumMu);
        for (const auto& s : p->summary)
            if (!s.empty && s.bytes && !q.tried.count(std::make_tuple(s.pid, s.seg, s.cgen))) all.push_back(s);
    }
    std::sort(all.begin(), all.end(), [](const SegSummary& a, const SegSummary& b) {
        if (a.minArrival != b.minArrival) return a.minArrival < b.minArrival;
        if (a.pid != b.pid) return a.pid < b.pid;
        return a.seg < b.seg;
    });
    uint64_t freed = 0;
    for (const auto& s : all) {
        if (usage - std::min(usage, freed) <= target) break;
        // Evicting writes tombstones (a row and postings per record) before
        // compaction frees anything: in an emergency, one segment per wave,
        // so the ballast only ever has to hold one segment's tombstones.
        if (q.emergency && !q.inflight.empty()) break;
        auto ev = std::unique_ptr<Eviction>(new Eviction());
        ev->pid = s.pid;
        ev->seg = s.seg;
        ev->lastSeg = s.lastSeg;
        ev->bytes = s.bytes;
        // The range names the segment's rows by pseq: until the owner starts
        // it, maintenance may coalesce the segment into an output named by
        // an older one, and a range by segment id would then find nothing.
        if (e->tombRange(s.pid, s.seg, INT64_MAX, &ev->ticket, s.firstPseq, s.endPseq) != 0) continue;
        q.tried.insert(std::make_tuple(s.pid, s.seg, s.cgen));
        freed += s.bytes;
        q.evicted++;
        q.inflight.push_back(std::move(ev));
    }
    if (!q.inflight.empty()) {
        q.passes++;
    } else if (q.emergency) {
        // Nothing left to evict: what remains is heads, control rows and
        // tombstones. Stop lowering the cap; the ballast is retried next pass.
        q.emergencyCap = usage;
    }
}

void quotaClose(Engine* e, IoCtx* io) {
    QuotaState* q = e->quotaState();
    if (!q) return;
    io->close(&q->ballast);
    // Requests in flight reference the wave: the engine is stopping (writers
    // joined), so nothing completes them any more.
    q->inflight.clear();
}

}  // namespace ps
}  // namespace flatsql
