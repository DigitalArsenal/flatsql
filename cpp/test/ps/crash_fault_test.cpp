// Crash-fault harness (acceptance T1 #1 with A4, A10, A11 crash points).
//
// Each trial runs a random multi-type workload (new records, resends,
// re-tags across batches, RECONCILE, CAT supersede, kills by cid, control
// transactions, registrations mid-flight, frequent merges), freezes the host
// at a random mutating I/O call, applies a random crash mode (drop all
// unsynced writes, drop a random subset, tear the last write at 512 B,
// reorder, or kill -9), reopens and checks:
//   - every acked record is present with identical bytes (sha256 == its CID);
//   - each partition's pseqs are a gap-free prefix;
//   - head counters (and lane counters) equal a recount from rows;
//   - no gseq published before the crash maps to a different (pid, pseq);
//   - open read 0 bytes of any d-* file and parsed 0 frames;
//   - arrivals up to the published length are never zero-filled (A10) and no
//     durable pid is reused (A10);
//   - control transactions are all-or-nothing.
// Every other trial is an A4 trial: crash -> reopen -> resend the unacked
// records and publish -> a second crash that drops all unsynced writes.
#include <algorithm>
#include <deque>
#include <map>
#include <random>
#include <set>

#include "flatsql/ps/platform.h"
#include "ps/ps_test.h"

using namespace pst;

namespace {

// 64 types: 16 variants of each of OMM, MPE, CAT, IQC (same tables, distinct
// file identifiers).
std::vector<TestType>& crashTypes(int n) {
    static std::vector<TestType> types;
    if (int(types.size()) >= n) return types;
    types.reserve(64);
    const char letters[4] = {'O', 'M', 'C', 'I'};
    for (int k = int(types.size()); k < n; k++) {
        char fid[5];
        std::snprintf(fid, sizeof(fid), "%c%03d", letters[k % 4], k);
        types.push_back(makeTypeVariant(k % 4, fid, std::string(fid) + ".fbs"));
    }
    return types;
}

struct SentRec {
    uint64_t rseq;
    uint64_t id;
    uint32_t len;
    uint8_t cid[kCidLen];
    bool ctl = false;
};

struct PState {
    uint32_t pid = 0;
    int type = 0;           // index into crashTypes
    std::deque<SentRec> inflight;
    std::vector<SentRec> acked;
    std::vector<SentRec> lost;   // unacked when the host crashed (A4 resends them)
    uint64_t nextId = 1;
    std::string token;
};

std::vector<uint8_t> makeFrame(const TestType& base, int kind, uint64_t id, uint64_t salt) {
    switch (kind) {
        case 0: return ommRecord(uint32_t(id), "O" + std::to_string(id % 997), "2026-06-01T00:00:00Z", double(id) + salt);
        case 1: return mpeRecord("E" + std::to_string(id), 1.6e9 + double(id), double(salt));
        case 2: return catRecord(uint32_t(id % 50 + 1), "C" + std::to_string(id % 50), "", "", "N" + std::to_string(id) + "/" + std::to_string(salt));
        default: return iqcRecord("I" + std::to_string(id), "2026-06-02T00:00:00Z", 1024 + size_t(id % 5) * 1024, id * 7 + salt);
    }
    (void)base;
}

std::vector<uint8_t> withFid(std::vector<uint8_t> f, const uint8_t fid[4]) {
    std::memcpy(f.data() + 8, fid, 4);
    return f;
}

struct Harness {
    std::mt19937_64 rng;
    Store s;
    int nTypes;
    uint32_t nParts;
    std::vector<PState> parts;
    uint32_t ctlPid = 0;
    std::map<uint64_t, int> txnSize;         // acked txns: id -> CTL count
    std::map<uint32_t, std::string> durablePids;  // pid -> token (registered)
    std::map<std::string, std::map<uint64_t, std::pair<uint32_t, uint64_t>>> published;  // fid -> gseq -> (pid, pseq)
    uint64_t batchEpoch = 1;
    uint64_t salt = 0;
    int failures0 = 0;

    Harness(uint64_t seed, int types, uint32_t partitions)
        : rng(seed), s(true, 4, true), nTypes(types), nParts(partitions) {
        s.cfg.mergeL0Blocks = 4;      // frequent merges: A11 crash points
        s.cfg.mergeL0Bytes = 256u << 10;
        s.cfg.mergeMinL0Bytes = 0;
        s.cfg.mergeHelpers = (seed & 4) ? 0 : 1;  // helper-built and inline merges
        s.cfg.poolBytes = 96ull << 20;
        s.cfg.ckptIntervalMs = 20;
        s.cfg.ckptMetaBytes = 16u << 10;
        s.cfg.sealBytes = 1u << 20;   // seals too
        s.cfg.sealRecords = (seed & 2) ? 24 : 200;  // some stores seal often
        s.cfg.zeroFillStep = (seed & 1) ? (64u << 10) : 0;
        s.cfg.arrivalsSegBytes = (seed & 8) ? 24 * 16 : 24 * 256;  // A15 arrivals seals
    }

    TestType& type(int k) { return crashTypes(nTypes)[size_t(k)]; }

    bool setup() {
        if (s.open() != 0) return false;
        std::vector<TestType*> ts;
        for (int k = 0; k < nTypes; k++) ts.push_back(&type(k));
        ts.push_back(&ctlType());
        s.registerTypes(ts);
        for (uint32_t i = 0; i < nParts / 2; i++) addPartition();
        ctlPid = s.partition("node-local", ctlType());
        durablePids[ctlPid] = "node_local";
        return true;
    }

    void addPartition() {
        PState ps;
        ps.type = int(parts.size() % size_t(nTypes));
        ps.token = "producer" + std::to_string(parts.size());
        uint32_t pid = 0;
        const int32_t rc = s.e->registerPartition(reinterpret_cast<const uint8_t*>(ps.token.data()), ps.token.size(),
                                                  type(ps.type).fid, &pid);
        if (rc < 0) return;  // frozen host
        ps.pid = pid;
        durablePids[pid] = ps.token;
        parts.push_back(std::move(ps));
    }

    std::vector<uint8_t> attrFor(const PState& p, uint64_t batch) {
        return buildRecordAttr(p.token, "prov", "src" + std::to_string(p.pid % 3), "b" + std::to_string(batch));
    }

    // Drives the workload until `ops` entries were attempted or the host froze.
    void workload(uint64_t ops) {
        std::vector<std::unique_ptr<Producer>> prods;
        for (auto& p : parts) prods.emplace_back(new Producer(s.e.get(), p.pid));
        Producer ctl(s.e.get(), ctlPid);
        std::map<uint64_t, int> pendingTxn;
        uint64_t ctlLast = 0;
        for (uint64_t op = 0; op < ops && !s.fs->frozen(); op++) {
            const uint32_t r = uint32_t(rng() % 1000);
            if (r < 3 && parts.size() < nParts) {
                addPartition();
                prods.emplace_back(new Producer(s.e.get(), parts.back().pid));
                continue;
            }
            if (r < 8) {
                batchEpoch++;
                continue;
            }
            const size_t k = rng() % parts.size();
            PState& p = parts[k];
            Producer& prod = *prods[k];
            const TestType& t = type(p.type);
            const int kind = p.type % 4;
            if (r < 14) {
                // RECONCILE of this partition's source against the current batch.
                const std::string prov = "prov", src = "src" + std::to_string(p.pid % 3), keep = "b" + std::to_string(batchEpoch);
                std::vector<uint8_t> payload;
                for (const std::string* x : {&prov, &src, &keep}) {
                    const size_t at = payload.size();
                    payload.resize(at + 2 + x->size());
                    putU16(payload.data() + at, uint16_t(x->size()));
                    std::memcpy(payload.data() + at + 2, x->data(), x->size());
                }
                uint64_t rs = 0;
                if (prod.enqueue(kEntReconcile, 0, 0, nullptr, nullptr, 0, payload.data(), uint32_t(payload.size()), &rs, false) == 0) {
                    SentRec sr{};
                    sr.rseq = rs;
                    sr.ctl = true;
                    p.inflight.push_back(sr);
                }
                continue;
            }
            if (r >= 20 && r < 22) {
                // Quota-planner eviction of one segment (TOMB_RANGE, §13).
                s.e->tombRange(p.pid, uint32_t(rng() % 3), INT64_MAX, nullptr);
                rangeCmds++;
                continue;
            }
            if (r < 20 && !p.acked.empty()) {
                // Kill a random acked record by cid (partition level).
                // Prefer shared records: killing a FIRST copy promotes a REPEAT.
                const SentRec* pick = &p.acked[rng() % p.acked.size()];
                for (int tries = 0; tries < 8 && pick->id < 900000000; tries++)
                    pick = &p.acked[rng() % p.acked.size()];
                const SentRec& victim = *pick;
                uint64_t rs = 0;
                if (prod.enqueue(kEntTombCid, 0, 0, victim.cid, nullptr, 0, nullptr, 0, &rs, false) == 0) {
                    SentRec sr{};
                    sr.rseq = rs;
                    sr.ctl = true;
                    p.inflight.push_back(sr);
                }
                continue;
            }
            if (r < 26) {
                // A control transaction of 1..4 entries; each body carries a
                // harness-unique marker, its index and the transaction size.
                const int n = 1 + int(rng() % 4);
                const uint64_t marker = ++txnMarker;
                uint64_t begin = 0, rs = 0;
                if (ctl.enqueue(kEntTxnBegin, 0, 0, nullptr, nullptr, 0, nullptr, 0, &begin, false) != 0) continue;
                for (int i = 0; i < n; i++) {
                    const auto a = buildRecordAttr("node", "", "", "", "", "", "",
                                                   "tbl" + std::to_string(i) + std::string("\0", 1) + std::to_string(rng() % 20));
                    std::vector<uint8_t> body(40, 0);
                    putU32(body.data(), 36);
                    putU64(body.data() + 4, marker);
                    putU32(body.data() + 12, uint32_t(i));
                    putU32(body.data() + 16, uint32_t(n));
                    ctl.enqueue(kEntCtl, 0, 0, nullptr, a.data(), uint32_t(a.size()), body.data(), uint32_t(body.size()), &rs, true);
                }
                ctl.enqueue(kEntTxnEnd, 0, 0, nullptr, nullptr, 0, nullptr, 0, &rs, true);
                pendingTxn[rs] = n;
                (void)begin;
                ctlLast = rs;
                txnSizeByEnd[rs] = {marker, n};
                continue;
            }
            // A record: mostly new, some resends (dedupe or RETAG), some shared
            // by every producer of the type (REPEAT labels, promotions).
            uint64_t id, space;
            if (r < 200 && p.nextId > 1) {
                id = 1 + rng() % (p.nextId - 1);
                space = uint64_t(p.pid) * 1000000;
            } else if (r < 350) {
                id = 1 + rng() % 200;
                space = 900000000;
            } else {
                id = p.nextId++;
                space = uint64_t(p.pid) * 1000000;
            }
            const uint64_t mySalt = kind == 2 ? (rng() % 3) : 0;
            auto frame = withFid(makeFrame(t, kind, id + space, mySalt), t.fid);
            const auto attr = attrFor(p, batchEpoch);
            SentRec sr{};
            frameCid(frame, sr.cid);
            sr.id = id + space;
            sr.len = uint32_t(frame.size());
            if (prod.enqueue(kEntRecord, kEntCidPresent, int64_t(op), sr.cid, attr.data(), uint32_t(attr.size()),
                             frame.data(), uint32_t(frame.size()), &sr.rseq, false) == 0)
                p.inflight.push_back(sr);
            if ((op & 63) == 0) pollAcks(prods, ctl);
        }
        pollAcks(prods, ctl);
        (void)ctlLast;
    }
    std::map<uint64_t, std::pair<uint64_t, int>> txnSizeByEnd;  // end rseq -> (marker, size)
    uint64_t txnMarker = 0;

    void pollAcks(std::vector<std::unique_ptr<Producer>>& prods, Producer& ctl) {
        for (size_t k = 0; k < parts.size(); k++) {
            PState& p = parts[k];
            const uint64_t acked = prods[k]->ringDesc()->ackedRseq.load();
            while (!p.inflight.empty() && p.inflight.front().rseq <= acked) {
                SentRec sr = p.inflight.front();
                p.inflight.pop_front();
                if (sr.ctl) continue;
                if (prods[k]->rejectCode(sr.rseq) != 0) continue;
                p.acked.push_back(sr);
            }
        }
        const uint64_t ca = ctl.ringDesc()->ackedRseq.load();
        for (auto it = txnSizeByEnd.begin(); it != txnSizeByEnd.end();) {
            if (it->first <= ca) {
                txnSize[it->second.first] = it->second.second;
                it = txnSizeByEnd.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Snapshot of published arrivals (page cache view, before the crash).
    void snapshotPublished() {
        Inspector ins(s.fs.get(), s.root);
        for (int k = 0; k < nTypes; k++) {
            const auto tv = ins.type(type(k).fid);
            if (!tv.ok) continue;
            auto& m = published[std::string(reinterpret_cast<const char*>(type(k).fid), 4)];
            for (const auto& a : tv.arrivals)
                if (a.gseq <= tv.head.gseqHi) m[a.gseq] = {a.pid, a.pseq};
        }
    }

    EngineStats totals{};
    uint64_t ackedTotal = 0, adoptedTotal = 0;
    std::map<int, size_t> arrivalSegsMax;  // sealed arrivals segments seen per type
    uint64_t rangeCmds = 0;                // TOMB_RANGE commands issued
    void crash(FaultFs::CrashMode mode) {
        if (s.e) {
            const EngineStats st = s.e->stats();
            totals.rowsAppended += st.rowsAppended;
            totals.merges += st.merges;
            totals.seals += st.seals;
            totals.retags += st.retags;
            totals.tombs += st.tombs;
            totals.dedupeHits += st.dedupeHits;
            totals.firstLabels += st.firstLabels;
            totals.promotions += st.promotions;
        }
        s.fs->freeze();
        if (s.e) s.e->abandon();
        s.e.reset();
        snapshotPublished();
        s.fs->crash(mode, rng());
        // Unacked entries were never acked; the new engine's rings number
        // entries from 1 again, so they leave the in-flight list.
        for (auto& p : parts) {
            for (const auto& sr : p.inflight)
                if (!sr.ctl) p.lost.push_back(sr);
            p.inflight.clear();
        }
        txnSizeByEnd.clear();
    }

    bool reopenAndVerify(const char* phase) {
        const int before = gFailures;
        std::string err;
        EngineConfig c = s.cfg;
        if (Engine::open(c, &s.e, &err) != 0) {
            std::fprintf(stderr, "  [%s] reopen failed: %s\n", phase, err.c_str());
            gFailures++;
            return false;
        }
        const EngineStats st = s.e->stats();
        adoptedTotal += st.adoptedBatches;
        CHECK_EQ(st.openDataBytes, uint64_t(0));
        CHECK_EQ(st.framesParsedAtOpen, uint64_t(0));
        // A10: durable pids keep their identity.
        for (const auto& kv : durablePids) {
            Partition* p = s.e->partition(kv.first);
            if (!p) continue;  // registration not durable before the crash
            if (p->token != kv.second) {
                std::fprintf(stderr, "  [%s] pid %u token changed\n", phase, kv.first);
                gFailures++;
            }
        }
        Inspector ins(s.fs.get(), s.root);
        for (auto& p : parts) {
            if (!s.e->partition(p.pid)) {
                if (!p.acked.empty()) {
                    std::fprintf(stderr, "  [%s] pid %u lost with acked records\n", phase, p.pid);
                    gFailures++;
                }
                continue;
            }
            PartView v = ins.partition(p.pid);
            if (!v.ok || !v.err.empty()) {
                std::fprintf(stderr, "  [%s] pid %u: %s\n", phase, p.pid, v.err.c_str());
                gFailures++;
                continue;
            }
            const Recount rc = recount(v);
            if (v.head.counters.totalCount != rc.total || v.head.counters.liveCount != rc.live ||
                v.head.counters.liveBytes != rc.liveBytes || v.head.counters.tombCount != rc.tombs ||
                v.head.counters.totalBytes != rc.totalBytes) {
                std::fprintf(stderr, "  [%s] pid %u counters differ from recount\n", phase, p.pid);
                gFailures++;
            }
            std::map<uint32_t, std::pair<int64_t, int64_t>> head, rec;
            for (const auto& l : v.lanes)
                if (l.count) head[l.laneId] = {l.count, l.bytes};
            for (const auto& kv : rc.lanes)
                if (kv.second.first) rec[kv.first] = kv.second;
            if (head != rec) {
                std::fprintf(stderr, "  [%s] pid %u lane counters differ from recount\n", phase, p.pid);
                gFailures++;
            }
            // Acked records: a PUT with that cid whose stored bytes hash to it.
            std::map<std::string, const RecRow*> puts;
            for (const auto& row : v.rows)
                if (row.kind == kRowPut) puts[std::string(reinterpret_cast<const char*>(row.cid), kCidLen)] = &row;
            int missing = 0, badBytes = 0;
            for (const auto& a : p.acked) {
                auto it = puts.find(std::string(reinterpret_cast<const char*>(a.cid), kCidLen));
                if (it == puts.end()) {
                    missing++;
                    continue;
                }
                const auto f = ins.frame(p.pid, *it->second);
                uint8_t c[kCidLen];
                if (f.size() < 12) {
                    badBytes++;
                    continue;
                }
                computeCid(f.data() + 4, f.size() - 4, c);
                if (std::memcmp(c, a.cid, kCidLen) != 0) badBytes++;
            }
            if (missing || badBytes) {
                std::fprintf(stderr, "  [%s] pid %u: %d acked records missing, %d with different bytes\n", phase,
                             p.pid, missing, badBytes);
                gFailures++;
            }
        }
        // Published gseqs never move; arrivals are never zero-filled.
        for (int k = 0; k < nTypes; k++) {
            const auto tv = ins.type(type(k).fid);
            const auto& pub = published[std::string(reinterpret_cast<const char*>(type(k).fid), 4)];
            std::map<uint64_t, std::pair<uint32_t, uint64_t>> now;
            if (!tv.fenceErr.empty()) {
                std::fprintf(stderr, "  [%s] arrivals segments: %s\n", phase, tv.fenceErr.c_str());
                gFailures++;
            }
            if (tv.fence.size() > arrivalSegsMax[k]) arrivalSegsMax[k] = tv.fence.size();
            for (const auto& a : tv.arrivals) {
                if (a.gseq == 0 || a.pid == 0 || a.pseq == 0) {
                    std::fprintf(stderr, "  [%s] zero-filled arrival visible\n", phase);
                    gFailures++;
                    break;
                }
                now[a.gseq] = {a.pid, a.pseq};
            }
            for (const auto& kv : pub) {
                auto it = now.find(kv.first);
                if (it == now.end() || it->second != kv.second) {
                    std::fprintf(stderr, "  [%s] published gseq %llu reassigned or lost\n", phase,
                                 (unsigned long long)kv.first);
                    gFailures++;
                    break;
                }
            }
        }
        // Control transactions: all-or-nothing, acked ones present.
        if (s.e->partition(ctlPid)) {
            PartView v = ins.partition(ctlPid);
            std::map<uint64_t, std::pair<int, int>> byMarker;  // marker -> (rows, size)
            for (const auto& row : v.rows) {
                if (row.kind != kRowCtl) continue;
                const auto f = ins.frame(ctlPid, row);
                if (f.size() < 20) continue;
                auto& e = byMarker[getU64(f.data() + 4)];
                e.first++;
                e.second = int(getU32(f.data() + 16));
            }
            for (const auto& kv : byMarker) {
                if (kv.second.first != kv.second.second) {
                    std::fprintf(stderr, "  [%s] txn %llu not whole (%d of %d)\n", phase,
                                 (unsigned long long)kv.first, kv.second.first, kv.second.second);
                    gFailures++;
                    break;
                }
            }
            for (const auto& kv : txnSize) {
                if (!byMarker.count(kv.first)) {
                    std::fprintf(stderr, "  [%s] acked txn %llu missing\n", phase, (unsigned long long)kv.first);
                    gFailures++;
                    break;
                }
            }
        }
        s.e->start();
        return gFailures == before;
    }

    // A4: resend every unacked record and publish, then crash dropping all.
    void resendUnacked() {
        std::vector<std::unique_ptr<Producer>> prods;
        for (auto& p : parts) prods.emplace_back(new Producer(s.e.get(), p.pid));
        Producer ctl(s.e.get(), ctlPid);
        for (size_t k = 0; k < parts.size(); k++) {
            PState& p = parts[k];
            if (!s.e->partition(p.pid)) continue;
            std::deque<SentRec> again;
            for (SentRec sr : p.lost) {
                const TestType& t = type(p.type);
                // Regenerate the same bytes: find by cid among candidate salts.
                for (uint64_t sl = 0; sl < 3; sl++) {
                    auto frame = withFid(makeFrame(t, p.type % 4, sr.id, sl), t.fid);
                    uint8_t c[kCidLen];
                    frameCid(frame, c);
                    if (std::memcmp(c, sr.cid, kCidLen) != 0) continue;
                    const auto attr = attrFor(p, batchEpoch);
                    if (prods[k]->enqueue(kEntRecord, kEntCidPresent, 1, sr.cid, attr.data(), uint32_t(attr.size()),
                                          frame.data(), uint32_t(frame.size()), &sr.rseq, true) == 0)
                        again.push_back(sr);
                    break;
                }
            }
            p.lost.clear();
            p.inflight.swap(again);
        }
        for (size_t k = 0; k < parts.size(); k++)
            if (!parts[k].inflight.empty()) prods[k]->waitAcked(parts[k].inflight.back().rseq, 30000000000ull);
        pollAcks(prods, ctl);
    }
};

void runTrials(int trials, int types, uint32_t partitions, uint64_t seed0) {
    int done = 0;
    EngineStats sum{};
    uint64_t acked = 0, adopted = 0, arrivalSeals = 0, rangeTotal = 0;
    uint64_t seed = seed0;
    const int phasesPerStore = 6;
    std::map<int, int> modes;
    while (done < trials) {
        Harness h(seed++, types, partitions);
        if (!h.setup()) {
            gFailures++;
            return;
        }
        for (int ph = 0; ph < phasesPerStore && done < trials; ph++) {
            const uint64_t ops = 200 + h.rng() % 2500;
            h.s.fs->armCrashAtOp(h.s.fs->mutatingOps() + 50 + h.rng() % 4000);
            h.workload(ops);
            const auto mode = FaultFs::CrashMode(h.rng() % FaultFs::kModeCount);
            modes[mode]++;
            h.crash(mode);
            if (!h.reopenAndVerify("crash")) {
                std::fprintf(stderr, "  trial %d (seed %llu, mode %d) failed\n", done, (unsigned long long)(seed - 1), int(mode));
                return;
            }
            done++;
            if (done % 2 == 0) {
                // A4: resend + publish, then a second crash dropping all unsynced writes.
                h.resendUnacked();
                h.workload(100 + h.rng() % 300);
                h.crash(FaultFs::kDropAll);
                if (!h.reopenAndVerify("a4-second-crash")) {
                    std::fprintf(stderr, "  A4 trial %d (seed %llu) failed\n", done, (unsigned long long)(seed - 1));
                    return;
                }
            }
            if (done % 100 == 0) {
                std::printf("  ... %d trials\n", done);
                std::fflush(stdout);
            }
        }
        for (const auto& p : h.parts) acked += p.acked.size();
        adopted += h.adoptedTotal;
        for (const auto& kv : h.arrivalSegsMax) arrivalSeals += kv.second;
        rangeTotal += h.rangeCmds;
        sum.rowsAppended += h.totals.rowsAppended;
        sum.merges += h.totals.merges;
        sum.seals += h.totals.seals;
        sum.retags += h.totals.retags;
        sum.tombs += h.totals.tombs;
        sum.dedupeHits += h.totals.dedupeHits;
        sum.firstLabels += h.totals.firstLabels;
        sum.promotions += h.totals.promotions;
        h.s.close();
    }
    report("crash_trials", double(done), "trials");
    report("crash_acked_records_verified", double(acked), "records");
    report("crash_rows_appended", double(sum.rowsAppended), "rows");
    report("crash_merges", double(sum.merges), "merges");
    report("crash_seals", double(sum.seals), "seals");
    report("crash_retags", double(sum.retags), "rows");
    report("crash_tombs", double(sum.tombs), "rows");
    report("crash_dedupe_hits", double(sum.dedupeHits), "entries");
    report("crash_first_labels", double(sum.firstLabels), "labels");
    report("crash_promotions", double(sum.promotions), "promotions");
    report("crash_adopted_tail_batches", double(adopted), "batches");
    report("crash_arrivals_segments_sealed", double(arrivalSeals), "segments");
    report("crash_tomb_range_commands", double(rangeTotal), "commands");
    for (const auto& kv : modes) {
        const char* names[] = {"drop_all", "drop_subset", "tear_last_512", "reorder", "kill9_keep_all"};
        char key[64];
        std::snprintf(key, sizeof(key), "crash_mode_%s", names[kv.first]);
        report(key, double(kv.second), "trials");
    }
}

}  // namespace

PS_TEST(crash_faults_T1_1) {
    runTrials(int(argInt("crash-trials", 60)), int(argInt("crash-types", 16)),
              uint32_t(argInt("crash-partitions", 64)), uint64_t(argInt("crash-seed", 1)));
}

PS_SLOW_TEST(crash_faults_T1_1_full) {
    runTrials(int(argInt("crash-trials", 10000)), 64, 256, uint64_t(argInt("crash-seed", 1000)));
}
