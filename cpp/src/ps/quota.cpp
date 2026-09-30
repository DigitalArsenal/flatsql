// FlatSQL partition store: the quota planner, the space emergency and the
// ballast (design §13, A13, owner decision §22.4-3; see ps/quota.h).
#include <algorithm>
#include <cstdio>
#include <cstdlib>
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
    // Counters: writer 0 counts, quotaStats() reads them on any thread.
    std::atomic<uint64_t> passes{0}, evicted{0}, emergencies{0}, releases{0}, restores{0};
    std::atomic<uint64_t> usage{0}, cap{0};
    // Ballast (A13).
    FileRef ballast;
    uint64_t ballastLen = 0;
    bool ballastReleased = false;
};

// ---------------------------------------------------------------------------
// Published inputs (B4: incremental; see PartitionLedger in ps/quota.h)
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

namespace {
bool summarized(const SegmentInfo& s) { return s.sealed && s.firstPseq && s.endPseq > s.firstPseq; }

SegSummary summaryOf(const Partition* p, const SegmentInfo& s) {
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
    return x;
}

std::vector<SegSummary> summaryWalk(const Partition* p) {
    std::vector<SegSummary> v;
    v.reserve(p->segs.size());
    for (const auto& s : p->segs)
        if (summarized(s)) v.push_back(summaryOf(p, s));
    return v;
}

bool sameSummary(const SegSummary& a, const SegSummary& b) {
    return a.pid == b.pid && a.seg == b.seg && a.lastSeg == b.lastSeg && a.cgen == b.cgen &&
           a.minArrival == b.minArrival && a.maxArrival == b.maxArrival && a.firstPseq == b.firstPseq &&
           a.endPseq == b.endPseq && a.bytes == b.bytes && a.empty == b.empty;
}

[[noreturn]] void bookFail(const char* what, uint32_t pid, uint64_t key) {
    std::fprintf(stderr, "flatsql ps bookkeeping check failed: %s (pid %u, key %llu)\n", what, pid,
                 (unsigned long long)key);
    std::abort();
}

size_t summaryPos(const std::vector<SegSummary>& v, uint32_t seg) {
    return size_t(std::lower_bound(v.begin(), v.end(), seg,
                                   [](const SegSummary& x, uint32_t k) { return x.seg < k; }) -
                  v.begin());
}
}  // namespace

std::atomic<int> gBookTime{0};
LockHist& bookSummaryHist() {
    static LockHist h;
    return h;
}
LockHist& bookPickHist() {
    static LockHist h;
    return h;
}

// The planner's view of the partition's sealed segments. Only segments the
// change feed names are recomputed (O(changed x log S) per commit); open
// rebuilds it in one walk.
void partitionPublishSummary(Partition* p) {
    const uint64_t t0 = gBookTime.load(std::memory_order_relaxed) ? monoNs() : 0;
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;  // a maintenance event (seal, merge, SWAP)
    PartitionLedger& L = p->ledger;
    if (L.sumAll) {
        std::vector<SegSummary> v = summaryWalk(p);
        {
            std::lock_guard<std::mutex> g(p->sumMu);
            p->summary.swap(v);
        }
        L.sumAll = false;
        L.sumDirty.clear();
    } else if (!L.sumDirty.empty()) {
        std::vector<uint32_t>& d = L.sumDirty;
        std::sort(d.begin(), d.end());
        d.erase(std::unique(d.begin(), d.end()), d.end());
        struct Up {
            uint32_t seg;
            uint32_t coverEnd;  // a coalesced output replaces entries up to here
            bool present;
            SegSummary x;
        };
        std::vector<Up> ups;
        ups.reserve(d.size());
        for (const uint32_t seg : d) {
            Up u;
            u.seg = seg;
            const SegmentInfo* s = segFind(p, seg);
            u.present = s && summarized(*s);
            u.coverEnd = s && s->lastSeg > seg ? s->lastSeg : seg;
            if (u.present) u.x = summaryOf(p, *s);
            ups.push_back(u);
        }
        std::lock_guard<std::mutex> g(p->sumMu);
        std::vector<SegSummary>& v = p->summary;
        for (const Up& u : ups) {
            size_t i = summaryPos(v, u.seg);
            const bool have = i < v.size() && v[i].seg == u.seg;
            if (u.coverEnd > u.seg) {
                const size_t a = have ? i + 1 : i;
                size_t b = a;
                while (b < v.size() && v[b].seg <= u.coverEnd) b++;
                if (b > a) v.erase(v.begin() + long(a), v.begin() + long(b));
            }
            if (u.present) {
                if (have) v[i] = u.x;
                else v.insert(v.begin() + long(i), u.x);
            } else if (have) {
                v.erase(v.begin() + long(i));
            }
        }
        d.clear();
    }
    p->retiredBytesPub.store(L.retiredBytes, std::memory_order_relaxed);
    if (gBookCheck.load(std::memory_order_relaxed)) {
        const std::vector<SegSummary> ref = summaryWalk(p);
        uint64_t retired = 0;
        for (const auto& r : p->retired) retired += r.it.size;
        if (retired != L.retiredBytes) bookFail("retired bytes", p->pid, retired);
        if (L.bytesWalk() != p->ledgerBytes) bookFail("ledger bytes", p->pid, p->ledgerBytes);
        uint64_t accel = 0;
        for (const auto& si : p->segs)
            for (const auto& r : si.runs)
                if (r.run) accel += r.run->memoryBytes();
        if (accel != p->accelBytes.load(std::memory_order_relaxed)) bookFail("accelerator bytes", p->pid, accel);
        std::lock_guard<std::mutex> g(p->sumMu);
        if (ref.size() != p->summary.size()) bookFail("summary size", p->pid, ref.size());
        for (size_t i = 0; i < ref.size(); i++)
            if (!sameSummary(ref[i], p->summary[i])) bookFail("summary entry", p->pid, ref[i].seg);
    }
    tHotPathDepth = saved;
    if (t0) bookSummaryHist().record(monoNs() - t0);
}

// ---------------------------------------------------------------------------
// Compaction candidates (B4): compaction.cpp's selection rules over an index
// kept from the change feed, instead of a walk of every segment every look.
// ---------------------------------------------------------------------------
namespace {
bool hasPendingL0B(const Partition* p, const SegmentInfo& s) {
    const uint32_t last = s.lastSeg ? s.lastSeg : s.seg;
    for (uint32_t i = 0; i < p->nL0; i++)
        if (p->l0[i].mSeg >= s.seg && p->l0[i].mSeg <= last) return true;
    return false;
}

// A sealed, fully merged segment with no L0 block left: compactable.
bool compactableB(const Partition* p, const SegmentInfo& s) {
    return s.sealed && s.endPseq > s.firstPseq && s.firstPseq && s.mergedEnd == s.endPseq &&
           (s.empty || s.cgen || !s.runs.empty()) && !hasPendingL0B(p, s);
}

double deadShare(const SegmentInfo& s) {
    const uint64_t data = s.dLen ? s.dLen : 1;
    return double(s.deadBytes) / double(data);
}

bool deadQualifies(const Partition* p, const SegmentInfo& s, double ratio) {
    return compactableB(p, s) && !s.empty && deadShare(s) >= ratio;
}

bool smallSeg(const Partition* p, const SegmentInfo& s, uint64_t smallBytes, uint64_t* sz) {
    *sz = segmentDiskBytes(p, s);
    return compactableB(p, s) && *sz < smallBytes;
}

// The candidate state of entry i, and the sets updated to it.
void candUpdate(Partition* p, size_t i) {
    PartitionLedger& L = p->ledger;
    const SegmentInfo& s = p->segs[i];
    PartitionLedger::Cand c;
    const bool comp = compactableB(p, s);
    c.share = deadShare(s);
    // Step 1's best needs a share above 0 too (the walk started from 0), while
    // its neighbours join at the ratio alone (deadQualifies).
    c.dead = comp && !s.empty && c.share >= L.ratio && c.share > 0;
    c.survey = comp && !s.empty && !s.deadRows;
    uint64_t sz = 0, sz2 = 0;
    if (L.maxInputs >= 2 && i + 1 < p->segs.size() && smallSeg(p, s, L.smallBytes, &sz)) {
        const SegmentInfo& n = p->segs[i + 1];
        c.pair = n.firstPseq == s.endPseq && smallSeg(p, n, L.smallBytes, &sz2) && sz + sz2 <= L.maxOutBytes;
    }
    auto it = L.cand.find(s.seg);
    if (it != L.cand.end()) {
        const PartitionLedger::Cand& o = it->second;
        if (o.dead && (!c.dead || o.share != c.share)) L.deadQ.erase(std::make_pair(-o.share, s.seg));
        if (o.pair && !c.pair) L.pairStarts.erase(s.seg);
        if (o.survey && !c.survey) L.survey.erase(s.seg);
        if (c.dead && (!o.dead || o.share != c.share)) L.deadQ.insert(std::make_pair(-c.share, s.seg));
        if (c.pair && !o.pair) L.pairStarts.insert(s.seg);
        if (c.survey && !o.survey) L.survey.insert(s.seg);
        if (!c.dead && !c.pair && !c.survey) L.cand.erase(it);
        else it->second = c;
        return;
    }
    if (!c.dead && !c.pair && !c.survey) return;
    if (c.dead) L.deadQ.insert(std::make_pair(-c.share, s.seg));
    if (c.pair) L.pairStarts.insert(s.seg);
    if (c.survey) L.survey.insert(s.seg);
    L.cand.emplace(s.seg, c);
}

void candRemove(PartitionLedger& L, uint32_t seg) {
    auto it = L.cand.find(seg);
    if (it == L.cand.end()) return;
    if (it->second.dead) L.deadQ.erase(std::make_pair(-it->second.share, seg));
    if (it->second.pair) L.pairStarts.erase(seg);
    if (it->second.survey) L.survey.erase(seg);
    L.cand.erase(it);
}

void candRefresh(const EngineConfig& cfg, Partition* p) {
    PartitionLedger& L = p->ledger;
    if (L.ratio != cfg.compactDeadRatio || L.smallBytes != cfg.compactSmallBytes ||
        L.maxOutBytes != cfg.compactMaxOutputBytes || L.maxInputs != cfg.compactMaxInputs) {
        L.ratio = cfg.compactDeadRatio;
        L.smallBytes = cfg.compactSmallBytes;
        L.maxOutBytes = cfg.compactMaxOutputBytes;
        L.maxInputs = cfg.compactMaxInputs;
        L.candAll = true;
    }
    if (L.candAll) {
        L.cand.clear();
        L.deadQ.clear();
        L.pairStarts.clear();
        L.survey.clear();
        for (size_t i = 0; i < p->segs.size(); i++) candUpdate(p, i);
        L.candAll = false;
        L.candDirty.clear();
        return;
    }
    if (L.candDirty.empty()) return;
    std::vector<uint32_t>& d = L.candDirty;
    std::sort(d.begin(), d.end());
    d.erase(std::unique(d.begin(), d.end()), d.end());
    for (const uint32_t seg : d) {
        const size_t i = size_t(std::lower_bound(p->segs.begin(), p->segs.end(), seg,
                                                 [](const SegmentInfo& s, uint32_t v) { return s.seg < v; }) -
                                p->segs.begin());
        if (i < p->segs.size() && p->segs[i].seg == seg) candUpdate(p, i);
        else candRemove(L, seg);
        // The entry before it pairs with whatever follows it now.
        if (i > 0) candUpdate(p, i - 1);
    }
    d.clear();
}

// compaction.cpp's pickCandidate before B4, kept as the reference the tests
// check the index against (gBookCheck).
bool pickCandidateWalk(const EngineConfig& cfg, const Partition* p, uint32_t* seg, uint32_t* segEnd) {
    const SegmentInfo* best = nullptr;
    double bestShare = 0;
    for (const auto& s : p->segs) {
        if (!compactableB(p, s) || s.empty) continue;
        const double share = deadShare(s);
        if (share >= cfg.compactDeadRatio && share > bestShare) {
            best = &s;
            bestShare = share;
        }
    }
    if (best) {
        auto qualifies = [&](const SegmentInfo& s) { return deadQualifies(p, s, cfg.compactDeadRatio); };
        size_t lo = size_t(best - p->segs.data()), hi = lo;
        uint64_t bytes = segmentDiskBytes(p, *best);
        for (bool grown = true; grown;) {
            grown = false;
            if (hi - lo + 1 < cfg.compactMaxInputs && hi + 1 < p->segs.size()) {
                const SegmentInfo& n = p->segs[hi + 1];
                const uint64_t nb = segmentDiskBytes(p, n);
                if (n.firstPseq == p->segs[hi].endPseq && qualifies(n) && bytes + nb <= cfg.compactMaxOutputBytes) {
                    hi++;
                    bytes += nb;
                    grown = true;
                }
            }
            if (hi - lo + 1 < cfg.compactMaxInputs && lo > 0) {
                const SegmentInfo& n = p->segs[lo - 1];
                const uint64_t nb = segmentDiskBytes(p, n);
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
    const SegmentInfo* runStart = nullptr;
    const SegmentInfo* runEnd = nullptr;
    uint32_t n = 0;
    uint64_t bytes = 0, prevEnd = 0;
    for (const auto& s : p->segs) {
        uint64_t sz = 0;
        const bool small = smallSeg(p, s, cfg.compactSmallBytes, &sz);
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
    // By value: the counters are packed, and std::min binds references.
    const uint64_t total = p->counters.totalBytes, live = p->counters.liveBytes;
    if (total && double(total - std::min(total, live)) >= cfg.compactDeadRatio * double(total)) {
        for (const auto& s : p->segs) {
            if (!compactableB(p, s) || s.empty || s.deadRows) continue;
            *seg = s.seg;
            *segEnd = s.lastSeg ? s.lastSeg : s.seg;
            return true;
        }
    }
    return false;
}

bool pickFromIndex(const EngineConfig& cfg, Partition* p, uint32_t* seg, uint32_t* segEnd) {
    PartitionLedger& L = p->ledger;
    auto indexOf = [&](uint32_t sg) {
        return size_t(std::lower_bound(p->segs.begin(), p->segs.end(), sg,
                                       [](const SegmentInfo& s, uint32_t v) { return s.seg < v; }) -
                      p->segs.begin());
    };
    // 1. The sealed segment with the largest dead share past the ratio (the
    // lowest id among equals), grown by qualifying neighbours.
    if (!L.deadQ.empty()) {
        const size_t b = indexOf(L.deadQ.begin()->second);
        auto qualifies = [&](const SegmentInfo& s) { return deadQualifies(p, s, cfg.compactDeadRatio); };
        size_t lo = b, hi = b;
        uint64_t bytes = segmentDiskBytes(p, p->segs[b]);
        for (bool grown = true; grown;) {
            grown = false;
            if (hi - lo + 1 < cfg.compactMaxInputs && hi + 1 < p->segs.size()) {
                const SegmentInfo& n = p->segs[hi + 1];
                const uint64_t nb = segmentDiskBytes(p, n);
                if (n.firstPseq == p->segs[hi].endPseq && qualifies(n) && bytes + nb <= cfg.compactMaxOutputBytes) {
                    hi++;
                    bytes += nb;
                    grown = true;
                }
            }
            if (hi - lo + 1 < cfg.compactMaxInputs && lo > 0) {
                const SegmentInfo& n = p->segs[lo - 1];
                const uint64_t nb = segmentDiskBytes(p, n);
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
    // 2. The first run of adjacent small segments, from the first pair start.
    if (!L.pairStarts.empty()) {
        size_t i = indexOf(*L.pairStarts.begin());
        uint64_t bytes = 0;
        (void)smallSeg(p, p->segs[i], cfg.compactSmallBytes, &bytes);
        uint32_t n = 1;
        size_t end = i;
        for (size_t j = i + 1; j < p->segs.size(); j++) {
            uint64_t sz = 0;
            if (!smallSeg(p, p->segs[j], cfg.compactSmallBytes, &sz) || p->segs[j].firstPseq != p->segs[j - 1].endPseq ||
                n >= cfg.compactMaxInputs || bytes + sz > cfg.compactMaxOutputBytes)
                break;
            end = j;
            n++;
            bytes += sz;
        }
        *seg = p->segs[i].seg;
        *segEnd = p->segs[end].lastSeg ? p->segs[end].lastSeg : p->segs[end].seg;
        return true;
    }
    // 3. After a restart: the oldest segment not surveyed, when the partition
    // as a whole is past the ratio.
    const uint64_t total = p->counters.totalBytes, live = p->counters.liveBytes;  // by value (packed)
    if (!L.survey.empty() && total &&
        double(total - std::min(total, live)) >= cfg.compactDeadRatio * double(total)) {
        const SegmentInfo& s = p->segs[indexOf(*L.survey.begin())];
        *seg = s.seg;
        *segEnd = s.lastSeg ? s.lastSeg : s.seg;
        return true;
    }
    return false;
}
}  // namespace

bool bookPickCandidate(const Engine* e, Partition* p, uint32_t* seg, uint32_t* segEnd) {
    const uint64_t t0 = gBookTime.load(std::memory_order_relaxed) ? monoNs() : 0;
    const EngineConfig& cfg = e->config();
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;  // maintenance (a look every 50 ms at most)
    candRefresh(cfg, p);
    const bool have = pickFromIndex(cfg, p, seg, segEnd);
    if (gBookCheck.load(std::memory_order_relaxed)) {
        uint32_t s2 = 0, e2 = 0;
        const bool have2 = pickCandidateWalk(cfg, p, &s2, &e2);
        if (have != have2 || (have && (s2 != *seg || e2 != *segEnd))) bookFail("compaction candidate", p->pid, s2);
    }
    tHotPathDepth = saved;
    if (t0) bookPickHist().record(monoNs() - t0);
    return have;
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
