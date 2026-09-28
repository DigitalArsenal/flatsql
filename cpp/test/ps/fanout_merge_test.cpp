// Fan-out correctness (T2 acceptance #2, A14): randomized multi-producer
// workloads — the same CID across up to 8 producers, supersede, type-level
// deletes, RECONCILE kills of FIRST copies (promotions), epoch-less types —
// checked against a brute-force reference built from the committed files by
// the independent Inspector:
//   - one row per live CID per type, from a live copy of that CID;
//   - FIRST semantics: the arrivals copy when it lives, else a promoted copy
//     that keeps the arrivals gseq;
//   - default order (floor(epoch/1000) DESC, text CID ASC), gseq order;
//   - rows_examined <= LIMIT x (partitions touched + 1).
// Plus readers running concurrently with FIRST deaths: no live CID is ever
// absent, and a CID's gseq never changes while any copy lives.
#include <algorithm>
#include <atomic>
#include <map>
#include <random>
#include <set>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {

std::vector<uint8_t> reconcilePayload(const std::string& provider, const std::string& source,
                                      const std::string& keep) {
    std::vector<uint8_t> out;
    for (const std::string* s : {&provider, &source, &keep}) {
        const size_t at = out.size();
        out.resize(at + 2 + s->size());
        putU16(out.data() + at, uint16_t(s->size()));
        std::memcpy(out.data() + at + 2, s->data(), s->size());
    }
    return out;
}

uint64_t reconcile(Producer& prod, const std::string& provider, const std::string& source, const std::string& keep) {
    const auto payload = reconcilePayload(provider, source, keep);
    uint64_t rseq = 0;
    prod.enqueue(kEntReconcile, 0, 0, nullptr, nullptr, 0, payload.data(), uint32_t(payload.size()), &rseq);
    return rseq;
}

// Base 0 = OMM (string epochs), 1 = MPE (f64 epochs), 2 = CAT (supersede,
// epoch-less: epoch = the copy's arrival).
std::vector<uint8_t> makeRecord(const TestType& t, int base, int w, int i, int version) {
    char epoch[40];
    std::snprintf(epoch, sizeof(epoch), "2026-07-%02dT%02d:%02d:%02d.%03dZ", 1 + (i % 5), (i * 7) % 24, (i * 13) % 60,
                  (i * 3) % 60, (i * 37) % 1000);
    switch (base) {
        case 0:
            return buildRecord(t, {Field::str("OBJECT_NAME", "W" + std::to_string(w) + "-" + std::to_string(i)),
                                   Field::str("OBJECT_ID", "O" + std::to_string(i)), Field::u64("NORAD_CAT_ID", uint64_t(i + 1)),
                                   Field::str("EPOCH", epoch), Field::f64("MEAN_MOTION", 15.0 + i)});
        case 1:
            return buildRecord(t, {Field::str("ENTITY_ID", "E" + std::to_string(i)),
                                   Field::f64("EPOCH", 1780000000.0 + double((i % 7) * 1000) + double(i % 3) / 4),
                                   Field::f64("MEAN_MOTION", double(w) + i)});
        default:
            return buildRecord(t, {Field::str("OBJECT_NAME", "W" + std::to_string(w) + "-" + std::to_string(i) + "v" +
                                                                   std::to_string(version)),
                                   Field::str("OBJECT_ID", "O" + std::to_string(i)), Field::u64("NORAD_CAT_ID", uint64_t(i + 1)),
                                   Field::i64("OBJECT_TYPE", i % 3), Field::i64("OPS_STATUS_CODE", 0)});
    }
}

struct WorkloadTypes {
    std::vector<TestType> types;
    std::vector<int> base;
};

WorkloadTypes& workloadTypes(int n) {
    static WorkloadTypes wt;
    while (int(wt.types.size()) < n) {
        const int k = int(wt.types.size());
        char fid[5], name[16];
        std::snprintf(fid, sizeof(fid), "W%03d", k);
        std::snprintf(name, sizeof(name), "W%03d.fbs", k);
        const int base = k % 3 == 0 ? 0 : k % 3 == 1 ? 2 : 1;
        wt.types.push_back(makeTypeVariant(base, fid, name));
        wt.base.push_back(base);
    }
    return wt;
}

struct Copy {
    uint32_t pid;
    uint64_t pseq;
    int64_t epochMs;
};

struct Workload {
    int type = 0;
    std::vector<uint32_t> pids;
    std::vector<std::string> tokens;
    std::vector<std::vector<uint8_t>> frames;  // every distinct record sent
};

// The brute-force reference of one type from the committed files.
struct Reference {
    std::map<std::string, std::vector<Copy>> live;      // cid text -> live copies
    std::map<uint64_t, std::pair<uint32_t, uint64_t>> arrivals;  // gseq -> (pid, pseq)
    std::map<std::pair<uint32_t, uint64_t>, std::string> cidOf;  // (pid, pseq) -> cid text (every PUT)
    uint32_t nonEmpty = 0;
    bool ok = true;
    std::string err;
};

Reference buildReference(Store& s, const TestType& t, const std::vector<uint32_t>& pids) {
    Reference ref;
    Inspector ins(s.fs.get(), s.root);
    for (uint32_t pid : pids) {
        PartView v = ins.partition(pid);
        if (!v.ok || !v.err.empty()) {
            ref.ok = false;
            ref.err = "partition " + std::to_string(pid) + ": " + v.err;
            return ref;
        }
        if (!v.rows.empty()) ref.nonEmpty++;
        const Recount rc = recount(v);
        for (const RecRow& r : v.rows) {
            if (r.kind != kRowPut) continue;
            const std::string cid = cidToText(r.cid);
            ref.cidOf[{pid, r.pseq}] = cid;
            if (!rc.dead.count(r.pseq)) ref.live[cid].push_back({pid, r.pseq, r.epochMs});
        }
    }
    const auto tv = ins.type(t.fid);
    if (!tv.ok || !tv.fenceErr.empty()) {
        ref.ok = false;
        ref.err = "type view: " + tv.fenceErr;
        return ref;
    }
    for (const ArrivalEntry& a : tv.arrivals) ref.arrivals[a.gseq] = {a.pid, a.pseq};
    return ref;
}

struct Checker {
    int failures = 0;
    std::string first;
    void fail(const std::string& m) {
        if (!failures) first = m;
        failures++;
    }
};

// Runs every query shape against one workload and compares with the reference.
void checkWorkload(Reader& bulk, Reader& inter, Store& s, const TestType& t, const Workload& w, std::mt19937_64& rng,
                   Checker* ck) {
    const std::string tn = t.name.substr(0, t.name.find('.'));
    Reference ref = buildReference(s, t, w.pids);
    if (!ref.ok) {
        ck->fail(tn + ": reference: " + ref.err);
        return;
    }
    // Q1: default order, every row.
    Rows all = bulk.q("SELECT _cid, _pid, _pseq, _epoch, _gseq FROM " + tn);
    if (all.status != 0) {
        ck->fail(tn + ": Q1 status " + std::to_string(all.status) + " " + all.error);
        return;
    }
    if (all.rows.size() != ref.live.size()) {
        ck->fail(tn + ": Q1 rows " + std::to_string(all.rows.size()) + " != live cids " + std::to_string(ref.live.size()));
        if (argInt("diag", 0)) {
            std::set<std::string> got;
            for (size_t i = 0; i < all.rows.size(); i++) got.insert(all.s(i, 0));
            LaneStoreConfig lc;
            lc.root = s.root;
            lc.io = s.fs.get();
            LaneStore st(lc);
            st.open();
            TypeSnap ts;
            st.loadType(t.fid, &ts);
            for (const auto& kv : ref.live) {
                if (got.count(kv.first)) continue;
                std::fprintf(stderr, "  missing %s\n", kv.first.c_str());
                for (const Copy& c : kv.second) {
                    uint8_t label = 0;
                    uint64_t g = 0;
                    bool found = false;
                    st.labelOf(ts, c.pid, c.pseq, &label, &g, &found);
                    std::fprintf(stderr, "    live copy pid %u pseq %llu label %d gseq %llu found %d labeled_through %llu\n",
                                 c.pid, (unsigned long long)c.pseq, label, (unsigned long long)g, int(found),
                                 (unsigned long long)ts.labeledThrough(c.pid));
                }
                uint8_t cid[kCidLen];
                cidFromText(kv.first.data(), kv.first.size(), cid);
                std::vector<CatalogCopy> cat;
                st.catalog(ts, cid, &cat);
                for (const auto& c : cat)
                    std::fprintf(stderr, "    catalog pid %u pseq %llu tcs %llu label %d gseq %llu\n", c.pid,
                                 (unsigned long long)c.pseq, (unsigned long long)c.tcs, c.label,
                                 (unsigned long long)c.gseq);
                for (const auto& a : ref.arrivals) {
                    auto co = ref.cidOf.find(a.second);
                    if (co != ref.cidOf.end() && co->second == kv.first)
                        std::fprintf(stderr, "    arrival gseq %llu -> pid %u pseq %llu\n", (unsigned long long)a.first,
                                     a.second.first, (unsigned long long)a.second.second);
                }
            }
        }
        return;
    }
    std::map<std::string, std::tuple<int64_t, int64_t, int64_t>> chosen;  // cid -> (pid, pseq, gseq)
    for (size_t i = 0; i < all.rows.size(); i++) {
        const std::string& cid = all.s(i, 0);
        const uint32_t pid = uint32_t(all.i(i, 1));
        const uint64_t pseq = uint64_t(all.i(i, 2));
        const int64_t gseq = all.i(i, 4);
        auto it = ref.live.find(cid);
        if (it == ref.live.end()) {
            ck->fail(tn + ": Q1 returned a dead or unknown cid");
            return;
        }
        if (chosen.count(cid)) {
            ck->fail(tn + ": Q1 duplicate cid");
            return;
        }
        bool isLiveCopy = false;
        for (const Copy& c : it->second)
            if (c.pid == pid && c.pseq == pseq) isLiveCopy = true;
        if (!isLiveCopy) {
            ck->fail(tn + ": Q1 row is not a live copy");
            return;
        }
        // FIRST semantics: the arrivals entry of the row's gseq names this
        // CID; if that copy lives it is the row, else the row was promoted.
        auto ar = ref.arrivals.find(uint64_t(gseq));
        if (ar == ref.arrivals.end()) {
            ck->fail(tn + ": Q1 gseq not in arrivals");
            return;
        }
        auto co = ref.cidOf.find(ar->second);
        if (co == ref.cidOf.end() || co->second != cid) {
            ck->fail(tn + ": Q1 gseq's arrivals entry names another cid");
            return;
        }
        bool arrivalsCopyLive = false;
        for (const Copy& c : it->second)
            if (c.pid == ar->second.first && c.pseq == ar->second.second) arrivalsCopyLive = true;
        if (arrivalsCopyLive && (ar->second.first != pid || ar->second.second != pseq)) {
            ck->fail(tn + ": Q1 not the FIRST copy although it lives");
            return;
        }
        chosen[cid] = {pid, int64_t(pseq), gseq};
        if (i > 0) {
            const int64_t a = epochSecFloor(all.i(i - 1, 3)), b = epochSecFloor(all.i(i, 3));
            if (!(a > b || (a == b && all.s(i - 1, 0) < cid))) {
                ck->fail(tn + ": Q1 default order violated");
                return;
            }
        }
    }
    // Q2: LIMIT on an interactive lane equals the prefix; examined bound.
    const size_t limit = 1 + size_t(rng() % 40);
    Rows lim = inter.q("SELECT _cid, _pid, _pseq FROM " + tn + " LIMIT " + std::to_string(limit));
    if (lim.status != 0) {
        ck->fail(tn + ": Q2 status " + std::to_string(lim.status) + " " + lim.error);
        return;
    }
    const size_t expectN = std::min(limit, all.rows.size());
    if (lim.rows.size() != expectN) {
        ck->fail(tn + ": Q2 row count");
        return;
    }
    for (size_t i = 0; i < expectN; i++)
        if (lim.s(i, 0) != all.s(i, 0) || lim.i(i, 1) != all.i(i, 1) || lim.i(i, 2) != all.i(i, 2)) {
            ck->fail(tn + ": Q2 prefix differs");
            return;
        }
    const uint64_t bound = uint64_t(limit) * (uint64_t(ref.nonEmpty) + 1);
    if (lim.outcome.rowsExamined > bound) {
        ck->fail(tn + ": Q2 rows_examined " + std::to_string(lim.outcome.rowsExamined) + " > " + std::to_string(bound));
        return;
    }
    // Q3: CID lookups (catalog) for every CID ever sent.
    for (const auto& f : w.frames) {
        const std::string cid = cidTextOf(f);
        Rows r = inter.q("SELECT _pid, _pseq, _gseq FROM " + tn + " WHERE _cid = ?", {Param::text(cid)});
        if (r.status != 0) {
            ck->fail(tn + ": Q3 status " + std::to_string(r.status) + " " + r.error);
            return;
        }
        auto ch = chosen.find(cid);
        if (ch == chosen.end()) {
            if (!r.rows.empty()) {
                ck->fail(tn + ": Q3 returned a dead cid");
                return;
            }
            continue;
        }
        if (r.rows.size() != 1 || r.i(0, 0) != std::get<0>(ch->second) || r.i(0, 1) != std::get<1>(ch->second) ||
            r.i(0, 2) != std::get<2>(ch->second)) {
            ck->fail(tn + ": Q3 differs from Q1");
            return;
        }
    }
    // Q4: gseq order (arrivals): the same rows, gseq strictly increasing.
    Rows g = bulk.q("SELECT _cid, _gseq, _pid, _pseq FROM " + tn + " ORDER BY _gseq");
    if (g.status != 0 || g.rows.size() != all.rows.size()) {
        ck->fail(tn + ": Q4 status/rows");
        return;
    }
    for (size_t i = 0; i < g.rows.size(); i++) {
        if (i && g.i(i, 1) <= g.i(i - 1, 1)) {
            ck->fail(tn + ": Q4 gseq not increasing");
            return;
        }
        auto ch = chosen.find(g.s(i, 0));
        if (ch == chosen.end() || std::get<2>(ch->second) != g.i(i, 1) || std::get<0>(ch->second) != g.i(i, 2) ||
            std::get<1>(ch->second) != g.i(i, 3)) {
            ck->fail(tn + ": Q4 differs from Q1");
            return;
        }
    }
    // Q5: partition level sees every live copy (no FIRST filter).
    for (size_t k = 0; k < w.pids.size(); k++) {
        const std::string pn = "sds_p_" + w.tokens[k] + "__" + tn;
        Rows pr = bulk.q("SELECT count(*) FROM " + pn);
        size_t expect = 0;
        for (const auto& kv : ref.live)
            for (const Copy& c : kv.second)
                if (c.pid == w.pids[k]) expect++;
        if (pr.status != 0 || pr.rows.size() != 1 || pr.i(0, 0) != int64_t(expect)) {
            ck->fail(tn + ": Q5 partition count");
            return;
        }
    }
}

void runFanoutWorkloads(int total, uint64_t seed) {
    const int perStore = 100;
    WorkloadTypes& wt = workloadTypes(std::min(total, perStore));
    std::mt19937_64 rng(seed);
    Checker ck;
    int done = 0;
    uint64_t promotions = 0, deletes = 0, supersedes = 0, reconciles = 0;
    while (done < total) {
        const int n = std::min(perStore, total - done);
        Store s(false, 2, true);
        REQUIRE(s.open() == 0);
        std::vector<TestType*> tys;
        for (int k = 0; k < n; k++) tys.push_back(&wt.types[size_t(k)]);
        s.registerTypes(tys);
        std::vector<Workload> ws(static_cast<size_t>(n));
        std::vector<uint32_t> allPids;
        for (int k = 0; k < n; k++) {
            Workload& w = ws[size_t(k)];
            w.type = k;
            const TestType& t = wt.types[size_t(k)];
            const int base = wt.base[size_t(k)];
            const int nProd = 1 + int(rng() % 8);
            const int nRec = 5 + int(rng() % 36);
            for (int p = 0; p < nProd; p++) {
                w.tokens.push_back("w" + std::to_string(done + k) + "p" + std::to_string(p));
                w.pids.push_back(s.partition(w.tokens.back(), t));
                allPids.push_back(w.pids.back());
            }
            std::vector<std::vector<uint8_t>> recs;
            for (int i = 0; i < nRec; i++) recs.push_back(makeRecord(t, base, done + k, i, 0));
            w.frames = recs;
            std::vector<std::unique_ptr<Producer>> prods;
            std::vector<uint64_t> last(size_t(nProd), 0);
            for (int p = 0; p < nProd; p++) prods.emplace_back(new Producer(s.e.get(), w.pids[size_t(p)]));
            // Rounds of sends; later rounds overlap earlier CIDs (REPEATs).
            for (int round = 0; round < 3; round++) {
                for (int p = 0; p < nProd; p++) {
                    const std::string src = (p % 2) ? "s1" : "s2";
                    const auto attr = buildRecordAttr(w.tokens[size_t(p)], "prov", src, "b1");
                    for (int i = 0; i < nRec; i++) {
                        if (rng() % 3 != 0) continue;
                        const int64_t arrival = 1780000000000ll + int64_t(rng() % 5000000);
                        last[size_t(p)] = send(s.e.get(), *prods[size_t(p)], recs[size_t(i)], attr, arrival);
                    }
                    // CAT: supersede a few in this lane.
                    if (base == 2 && rng() % 2) {
                        const int i = int(rng() % nRec);
                        auto v = makeRecord(t, base, done + k, i, 1 + int(rng() % 3));
                        last[size_t(p)] = send(s.e.get(), *prods[size_t(p)], v, attr, 1780000000000ll + int64_t(rng() % 5000000));
                        w.frames.push_back(v);
                        supersedes++;
                    }
                }
                for (int p = 0; p < nProd; p++)
                    if (last[size_t(p)]) CHECK_EQ(prods[size_t(p)]->waitAcked(last[size_t(p)], 30000000000ull), 0);
                CHECK(waitLabeledEngine(s.e.get(), w.pids, 30000000000ull));
                // FIRST deaths: RECONCILE one producer's lane away.
                if (nProd > 1 && rng() % 2) {
                    const int p = int(rng() % nProd);
                    const std::string src = (p % 2) ? "s1" : "s2";
                    const uint64_t r = reconcile(*prods[size_t(p)], "prov", src, "b9");
                    CHECK_EQ(prods[size_t(p)]->waitAcked(r, 30000000000ull), 0);
                    reconciles++;
                }
                // Type-level deletes.
                if (rng() % 3 == 0) {
                    uint8_t cid[kCidLen];
                    frameCid(recs[size_t(rng() % nRec)], cid);
                    std::atomic<int32_t> remaining{0};
                    CHECK_EQ(s.e->deleteCid(t.fid, cid, &remaining), 0);
                    const uint64_t until = monoNs() + 30000000000ull;
                    while (remaining.load() != 0 && monoNs() < until) sleepNs(200000);
                    CHECK_EQ(remaining.load(), 0);
                    deletes++;
                }
                CHECK(waitLabeledEngine(s.e.get(), w.pids, 30000000000ull));
            }
        }
        CHECK(waitLabeledEngine(s.e.get(), allPids, 60000000000ull));
        for (int k = 0; k < n; k++)
            CHECK(waitTypeVisible(s.fs.get(), s.root, wt.types[size_t(k)].fid, ws[size_t(k)].pids, 30000000000ull));
        promotions += s.e->stats().promotions;
        {
            Reader bulk(s, LaneClass::Bulk, 1);
            Reader inter(s, LaneClass::Interactive, 1);
            REQUIRE(bulk.inst && inter.inst);
            for (int k = 0; k < n && ck.failures < 5; k++)
                checkWorkload(bulk, inter, s, wt.types[size_t(k)], ws[size_t(k)], rng, &ck);
        }
        s.close();
        done += n;
    }
    if (ck.failures) std::fprintf(stderr, "  first failure: %s\n", ck.first.c_str());
    CHECK_EQ(ck.failures, 0);
    report("fanout_workloads", double(done), "workloads");
    report("fanout_promotions", double(promotions), "promotions");
    report("fanout_type_deletes", double(deletes), "deletes");
    report("fanout_supersedes", double(supersedes), "supersedes");
    report("fanout_reconciles", double(reconciles), "reconciles");
}

}  // namespace

PS_TEST(fanout_randomized_vs_bruteforce_T2_2) {
    runFanoutWorkloads(int(argInt("workloads", 200)), uint64_t(argInt("seed", 7)));
}

PS_SLOW_TEST(fanout_randomized_vs_bruteforce_T2_2_full) {
    runFanoutWorkloads(int(argInt("workloads", 10000)), uint64_t(argInt("seed", 20260928)));
}

// A14 on the read side (T2 #2 and #6 additions): readers run while FIRST
// copies die over and over. Two producers alternate: each round re-sends
// the set to one partition (REPEAT copies, labeled), then RECONCILEs the
// other partition's lane away, killing the FIRST copies; the type owner
// promotes the REPEATs in the same type commit. Every CID stays live the
// whole time, so no reader may ever miss one, and its gseq never changes.
namespace {
void runConcurrentFirstDeaths(uint64_t seconds) {
    Store s(false, 2, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pa = s.partition("alpha", ommType());
    const uint32_t pb = s.partition("beta", ommType());
    const int n = 64;
    std::vector<std::vector<uint8_t>> recs;
    for (int i = 0; i < n; i++) recs.push_back(makeRecord(ommType(), 0, 1, i, 0));
    Producer prodA(s.e.get(), pa), prodB(s.e.get(), pb);
    Producer* prods[2] = {&prodA, &prodB};
    const uint32_t pids[2] = {pa, pb};
    auto sendAll = [&](int k, int round) {
        const auto attr = buildRecordAttr(k ? "beta" : "alpha", "prov", "src", "r" + std::to_string(round));
        uint64_t last = 0;
        for (int i = 0; i < n; i++) last = send(s.e.get(), *prods[k], recs[size_t(i)], attr, 1780000000000ll + round);
        CHECK_EQ(prods[k]->waitAcked(last, 30000000000ull), 0);
    };
    sendAll(0, 0);
    REQUIRE(waitLabeledEngine(s.e.get(), {pa, pb}, 30000000000ull));
    REQUIRE(waitTypeVisible(s.fs.get(), s.root, ommType().fid, {pa, pb}, 30000000000ull));
    Reader bulk(s, LaneClass::Bulk, 2);
    Reader inter(s, LaneClass::Interactive, 2);
    REQUIRE(bulk.inst && inter.inst);
    // The gseq of every CID, fixed from the start.
    std::map<std::string, int64_t> gseqOf;
    {
        Rows r = bulk.q("SELECT _cid, _gseq FROM OMM");
        REQUIRE(r.status == 0 && r.rows.size() == size_t(n));
        for (size_t i = 0; i < r.rows.size(); i++) gseqOf[r.s(i, 0)] = r.i(i, 1);
    }
    std::vector<std::string> cids;
    for (const auto& f : recs) cids.push_back(cidTextOf(f));
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> queries{0}, missing{0}, gseqChanged{0}, errors{0};
    std::vector<std::thread> readers;
    for (int t = 0; t < 4; t++) {
        readers.emplace_back([&, t] {
            std::mt19937_64 rng(uint64_t(t) + 1);
            while (!stop.load()) {
                const int kind = int(rng() % 3);
                if (kind == 0) {
                    Rows r = bulk.q("SELECT _cid, _gseq FROM OMM");
                    if (r.status != 0) {
                        errors++;
                        continue;
                    }
                    std::set<std::string> seen;
                    for (size_t i = 0; i < r.rows.size(); i++) {
                        seen.insert(r.s(i, 0));
                        auto it = gseqOf.find(r.s(i, 0));
                        if (it == gseqOf.end() || it->second != r.i(i, 1)) gseqChanged++;
                    }
                    if (seen.size() != size_t(n) || r.rows.size() != size_t(n)) missing++;
                } else if (kind == 1) {
                    const std::string& cid = cids[rng() % cids.size()];
                    Rows r = inter.q("SELECT _gseq FROM OMM WHERE _cid = ?", {Param::text(cid)});
                    if (r.status != 0) {
                        errors++;
                        continue;
                    }
                    if (r.rows.size() != 1) missing++;
                    else if (r.i(0, 0) != gseqOf[cid]) gseqChanged++;
                } else {
                    Rows r = bulk.q("SELECT _cid, _gseq FROM OMM ORDER BY _gseq");
                    if (r.status != 0) {
                        errors++;
                        continue;
                    }
                    if (r.rows.size() != size_t(n)) missing++;
                    for (size_t i = 0; i < r.rows.size(); i++)
                        if (gseqOf[r.s(i, 0)] != r.i(i, 1)) gseqChanged++;
                }
                queries++;
            }
        });
    }
    const uint64_t until = monoNs() + seconds * 1000000000ull;
    int round = 1;
    uint64_t promotions0 = s.e->stats().promotions;
    int badRound = -1;
    while (monoNs() < until) {
        const int live = (round + 1) % 2;  // holds the FIRST copies now
        const int next = round % 2;
        sendAll(next, round);  // REPEAT copies, durable
        REQUIRE(waitLabeledEngine(s.e.get(), {pa, pb}, 30000000000ull));
        const uint64_t before = s.e->stats().promotions;
        const uint64_t r = reconcile(*prods[live], "prov", "src", "none");
        CHECK_EQ(prods[live]->waitAcked(r, 30000000000ull), 0);
        REQUIRE(waitLabeledEngine(s.e.get(), {pa, pb}, 30000000000ull));
        const uint64_t got = s.e->stats().promotions - before;
        if (got != uint64_t(n) && badRound < 0) {
            badRound = round;
            std::fprintf(stderr, "  round %d: %llu promotions of %d\n", round, (unsigned long long)got, n);
            LaneStoreConfig lc;
            lc.root = s.root;
            lc.io = s.fs.get();
            LaneStore st(lc);
            st.open();
            TypeSnap ts;
            st.loadType(ommType().fid, &ts);
            std::fprintf(stderr, "  type head: commit %llu nL0 %zu runs %zu gseqHi %llu firstLive %llu\n",
                         (unsigned long long)ts.head.commitSeq, ts.l0.size(), ts.runs.size(),
                         (unsigned long long)ts.head.gseqHi, (unsigned long long)ts.head.firstLiveCount);
            for (int i = 0; i < 3; i++) {
                uint8_t cid[kCidLen];
                frameCid(recs[size_t(i)], cid);
                std::vector<CatalogCopy> cat;
                st.catalog(ts, cid, &cat);
                std::sort(cat.begin(), cat.end(), [](const CatalogCopy& a, const CatalogCopy& b) { return a.tcs < b.tcs; });
                std::fprintf(stderr, "  cid %d: %zu copies; last:", i, cat.size());
                for (size_t k = cat.size() > 6 ? cat.size() - 6 : 0; k < cat.size(); k++)
                    std::fprintf(stderr, " [p%u s%llu t%llu L%d g%llu]", cat[k].pid, (unsigned long long)cat[k].pseq,
                                 (unsigned long long)cat[k].tcs, cat[k].label, (unsigned long long)cat[k].gseq);
                std::fprintf(stderr, "\n");
            }
        }
        (void)pids;
        round++;
    }
    CHECK_EQ(badRound, -1);
    stop = true;
    for (auto& t : readers) t.join();
    const uint64_t promotions = s.e->stats().promotions - promotions0;
    report("a14_rounds", double(round - 1), "rounds");
    report("a14_promotions", double(promotions), "promotions");
    report("a14_reader_statements", double(queries.load()), "statements");
    CHECK(promotions >= uint64_t(n) * uint64_t(round - 2));
    CHECK_EQ(missing.load(), uint64_t(0));
    CHECK_EQ(gseqChanged.load(), uint64_t(0));
    CHECK_EQ(errors.load(), uint64_t(0));
    s.close();
}
}  // namespace

PS_TEST(fanout_readers_during_first_deaths_A14) { runConcurrentFirstDeaths(uint64_t(argInt("seconds", 10))); }

PS_SLOW_TEST(fanout_readers_during_first_deaths_A14_full) { runConcurrentFirstDeaths(uint64_t(argInt("seconds", 600))); }
