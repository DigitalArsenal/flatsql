// Format-2 query gaps found by T6's fixture comparisons (PARTITION-STORE.md
// §37): text CID order from the cid catalog, per-object point profiles on
// OBJECT_EPOCH, the sandbox window read from arrivals, tag conditions on
// every live copy, and gseq-ordered tag pages. Each plan is held to a
// reference computed from what the test sent.
#include <algorithm>
#include <ctime>
#include <map>
#include <set>

#include <flatbuffers/reflection.h>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {

struct Tag {
    std::string provider, source, batch, peer;
    bool operator==(const Tag& o) const {
        return provider == o.provider && source == o.source && batch == o.batch && peer == o.peer;
    }
};

struct Cond {
    const char* provider = nullptr;
    const char* source = nullptr;
    const char* batch = nullptr;
    const char* peer = nullptr;
    bool any() const { return provider || source || batch || peer; }
    bool matches(const Tag& t) const {
        return (!provider || t.provider == provider) && (!source || t.source == source) &&
               (!batch || t.batch == batch) && (!peer || t.peer == peer);
    }
};

struct CopyModel {
    std::vector<Tag> insts;  // live instances
    bool live = true;
    bool tagged = false;     // ever carried a tag (RECONCILE spares untagged copies)
};

struct RecModel {
    std::vector<uint8_t> frame;
    std::string cid;
    uint32_t norad = 0;
    std::string objectId;
    int64_t epochSec = 0;
    std::map<uint32_t, CopyModel> copies;  // pid -> copy
    bool live() const {
        for (const auto& kv : copies)
            if (kv.second.live) return true;
        return false;
    }
    bool matches(const Cond& c) const {
        for (const auto& kv : copies) {
            if (!kv.second.live) continue;
            for (const Tag& t : kv.second.insts)
                if (c.matches(t)) return true;
        }
        return false;
    }
    // The legacy entity key (epoch_profiles.go): NORAD, else OBJECT_ID, else
    // the CID (no object key: _object is NULL).
    std::string entity() const {
        if (norad) return std::to_string(norad);
        if (!objectId.empty()) return objectId;
        return "cid:" + cid;
    }
};

std::string isoOf(int64_t sec) {
    time_t t = time_t(sec);
    struct tm tm;
    gmtime_r(&t, &tm);
    char b[40];
    std::snprintf(b, sizeof(b), "%04d-%02d-%02dT%02d:%02d:%02dZ", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                  tm.tm_hour, tm.tm_min, tm.tm_sec);
    return b;
}

std::vector<uint8_t> reconcilePayload(const std::string& provider, const std::string& source, const std::string& keep) {
    std::vector<uint8_t> out;
    for (const std::string* s : {&provider, &source, &keep}) {
        const size_t at = out.size();
        out.resize(at + 2 + s->size());
        putU16(out.data() + at, uint16_t(s->size()));
        std::memcpy(out.data() + at + 2, s->data(), s->size());
    }
    return out;
}

constexpr int64_t kBase = 1788220800;  // 2026-09-01T00:00:00Z
constexpr size_t kPad = 1500;          // payload bytes dominate every read that touches frames

struct World {
    Store s{true, 2, true};
    std::map<std::string, uint32_t> pid;  // producer -> pid
    std::vector<RecModel> recs;

    void send(const std::string& producer, int i, const Tag& t, int64_t arrivalMs) {
        Producer prod(s.e.get(), pid[producer]);
        const auto attr = buildRecordAttr(producer, t.provider, t.source, t.batch, "", t.peer);
        const uint64_t r = pst::send(s.e.get(), prod, recs[size_t(i)].frame, attr, arrivalMs);
        CHECK_EQ(prod.waitAcked(r, 20000000000ull), 0);
        CopyModel& c = recs[size_t(i)].copies[pid[producer]];
        const bool hasTag = !t.provider.empty() || !t.source.empty() || !t.batch.empty() || !t.peer.empty();
        if (hasTag && std::find(c.insts.begin(), c.insts.end(), t) == c.insts.end()) c.insts.push_back(t);
        c.tagged = c.tagged || hasTag;
    }
    void reconcile(const std::string& producer, const std::string& provider, const std::string& source,
                   const std::string& keep) {
        Producer prod(s.e.get(), pid[producer]);
        const auto payload = reconcilePayload(provider, source, keep);
        uint64_t rseq = 0;
        prod.enqueue(kEntReconcile, 0, 0, nullptr, nullptr, 0, payload.data(), uint32_t(payload.size()), &rseq);
        CHECK_EQ(prod.waitAcked(rseq, 20000000000ull), 0);
        for (RecModel& r : recs) {
            auto it = r.copies.find(pid[producer]);
            if (it == r.copies.end() || !it->second.live) continue;
            CopyModel& c = it->second;
            c.insts.erase(std::remove_if(c.insts.begin(), c.insts.end(),
                                         [&](const Tag& t) {
                                             return t.provider == provider && t.source == source && t.batch != keep;
                                         }),
                          c.insts.end());
            if (c.tagged && c.insts.empty()) c.live = false;
        }
    }
    void kill(int i) {
        uint8_t cid[kCidLen];
        frameCid(recs[size_t(i)].frame, cid);
        std::atomic<int32_t> remaining{0};
        CHECK_EQ(s.e->deleteCid(ommType().fid, cid, &remaining), 0);
        const uint64_t until = monoNs() + 20000000000ull;
        while (remaining.load() > 0 && monoNs() < until) sleepNs(1000000);
        CHECK_EQ(remaining.load(), 0);
        for (auto& kv : recs[size_t(i)].copies) kv.second.live = false;
    }
    std::vector<uint32_t> pids() const {
        std::vector<uint32_t> v;
        for (const auto& kv : pid) v.push_back(kv.second);
        return v;
    }
};

// Records: 40 NORAD objects with 10 versions each (some at one epoch second:
// ties), OBJECT_ID-only objects, and objectless records; copies in several
// partitions, RETAGs, a RECONCILE that promotes REPEAT copies, kills.
void buildWorld(World& w) {
    REQUIRE(w.s.open() == 0);
    w.s.registerTypes({&ommType()});
    for (const char* p : {"pA", "pB", "pC", "pD"}) w.pid[p] = w.s.partition(p, ommType());
    for (int i = 0; i < 520; i++) {
        RecModel r;
        if (i < 400) {
            r.norad = uint32_t(1 + i % 40);
            r.objectId = "1998-0" + std::to_string(i % 40);
            // Versions 3600 s apart; versions 4 and 5 of an object share a second.
            const int v = i / 40;
            r.epochSec = kBase + int64_t(v == 5 ? 4 : v) * 3600 + (i % 40) * 7;
        } else if (i < 440) {
            r.objectId = "OID-" + std::to_string(i % 5);
            r.epochSec = kBase + int64_t(i - 400) * 1800;
        } else if (i < 460) {
            r.epochSec = kBase + int64_t(i - 440) * 2700;  // no object key
        } else {
            r.norad = uint32_t(100 + i % 20);
            r.objectId = "PROMO";
            r.epochSec = kBase + int64_t(i - 460) * 900;
        }
        r.frame = ommRecord(r.norad, r.objectId, isoOf(r.epochSec), 1.0 + i * 0.001, kPad);
        r.cid = cidTextOf(r.frame);
        w.recs.push_back(std::move(r));
    }
    const Tag t1{"P1", "S1", "b1", "peer1"}, t2{"P2", "S2", "b2", "peer2"}, t3{"P1", "S1", "b3", "peer1"},
        t4{"P2", "S2", "b4", "peer2"}, t5{"P3", "S3", "b5", "peer3"}, none{};
    int64_t arrival = 1790000000000ll;
    for (int i = 0; i < 460; i++) w.send("pA", i, t1, arrival++);
    for (int i = 0; i < 460; i += 5) w.send("pB", i, t2, arrival++);   // REPEAT copies, another tag
    for (int i = 0; i < 460; i += 7) w.send("pA", i, t3, arrival++);   // RETAG, same source
    for (int i = 0; i < 460; i += 11) w.send("pA", i, t4, arrival++);  // RETAG, another source
    for (int i = 1; i < 460; i += 13) w.send("pC", i, none, arrival++);  // untagged REPEAT copies
    for (int i = 460; i < 520; i++) w.send("pD", i, t5, arrival++);
    for (int i = 460; i < 500; i++) w.send("pB", i, t2, arrival++);    // REPEATs of pD's records
    CHECK(waitLabeledEngine(w.s.e.get(), w.pids(), 20000000000ull));
    // pD's lane goes: its copies die, pB's REPEATs are promoted (460..499),
    // 500..519 die with it.
    w.reconcile("pD", "P3", "S3", "b-none");
    for (int i = 3; i < 400; i += 17) w.kill(i);
    CHECK(waitLabeledEngine(w.s.e.get(), w.pids(), 20000000000ull));
    REQUIRE(waitTypeVisible(w.s.fs.get(), w.s.root, ommType().fid, w.pids(), 20000000000ull));
}

std::set<std::string> cidSet(const Rows& r, size_t col = 0) {
    std::set<std::string> out;
    for (size_t i = 0; i < r.rows.size(); i++) out.insert(r.s(i, col));
    return out;
}

}  // namespace

// Gap 1 (A17): ORDER BY _cid reads the cid catalog in text order; an OFFSET
// skips catalog entries without reading their rows.
PS_TEST(query_gaps_cid_order_from_catalog_A17) {
    World w;
    buildWorld(w);
    std::vector<std::string> live;
    for (const RecModel& r : w.recs)
        if (r.live()) live.push_back(r.cid);
    std::sort(live.begin(), live.end());
    REQUIRE(live.size() > 300);
    Reader inter(w.s, LaneClass::Interactive, 1);
    Reader bulk(w.s, LaneClass::Bulk, 1);
    REQUIRE(inter.inst && bulk.inst);
    for (bool desc : {false, true}) {
        std::vector<std::string> want = live;
        if (desc) std::reverse(want.begin(), want.end());
        for (size_t off : {size_t(0), size_t(7), size_t(133), want.size() - 5, want.size() + 3}) {
            for (size_t lim : {size_t(1), size_t(10), size_t(1000)}) {
                const std::string sql = std::string("SELECT _cid FROM OMM ORDER BY _cid") + (desc ? " DESC" : "") +
                                        " LIMIT " + std::to_string(lim) + " OFFSET " + std::to_string(off);
                const Rows r = inter.q(sql);  // index-bounded: admitted on an interactive lane
                CHECK_EQ(r.status, 0);
                std::vector<std::string> got;
                for (size_t i = 0; i < r.rows.size(); i++) got.push_back(r.s(i, 0));
                std::vector<std::string> exp;
                for (size_t i = off; i < want.size() && exp.size() < lim; i++) exp.push_back(want[i]);
                CHECK(got == exp);
                // Skipped rows are counted from the catalog, never read.
                CHECK(r.outcome.rowsExamined <= got.size());
            }
        }
    }
    // Whole type, no LIMIT: every live CID once, in text order (bulk lane).
    Rows r = bulk.q("SELECT _cid FROM OMM ORDER BY _cid");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == live.size());
    for (size_t i = 0; i < live.size(); i++) CHECK(r.s(i, 0) == live[i]);
    // With a tag condition (checked per row on every copy, SQLite's LIMIT).
    std::vector<std::string> tagged;
    for (const RecModel& m : w.recs)
        if (m.live() && m.matches(Cond{"P2", nullptr, nullptr, nullptr})) tagged.push_back(m.cid);
    std::sort(tagged.begin(), tagged.end());
    r = bulk.q("SELECT _cid FROM OMM WHERE _provider = 'P2' ORDER BY _cid LIMIT 25 OFFSET 10");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == std::min<size_t>(25, tagged.size() - 10));
    for (size_t i = 0; i < r.rows.size(); i++) CHECK(r.s(i, 0) == tagged[i + 10]);
    // Partition level: that partition's live copies in text order.
    std::vector<std::string> inB;
    for (const RecModel& m : w.recs) {
        auto it = m.copies.find(w.pid["pB"]);
        if (it != m.copies.end() && it->second.live) inB.push_back(m.cid);
    }
    std::sort(inB.begin(), inB.end());
    r = bulk.q("SELECT _cid FROM sds_p_pB__OMM ORDER BY _cid");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == inB.size());
    for (size_t i = 0; i < inB.size(); i++) CHECK(r.s(i, 0) == inB[i]);
    w.s.close();
}

// Gap 2 (A18): _asof / _forward / _nearest choose per object from
// OBJECT_EPOCH keys (no payload read for the ranking), with the tag and
// epoch conditions applied before the choice; objectless records are their
// own entities.
PS_TEST(query_gaps_point_profiles_per_object_A18) {
    World w;
    buildWorld(w);
    Reader bulk(w.s, LaneClass::Bulk, 1);
    REQUIRE(bulk.inst);
    struct Q {
        const char* col;
        int64_t T;
        Cond cond;
        int64_t lo = INT64_MIN, hi = INT64_MAX;  // epoch ms range, [lo, hi)
    };
    const int64_t mid = kBase + 4 * 3600 + 100;
    const std::vector<Q> qs = {
        {"_asof", mid, {}},
        {"_forward", mid, {}},
        {"_nearest", mid, {}},
        {"_nearest", kBase + 4 * 3600 + 7 * 13, {}},  // exactly on a version's second (ties at distance 0)
        {"_asof", kBase + 2 * 3600, {nullptr, "S2", nullptr, nullptr}},
        {"_forward", kBase - 100, {"P1", nullptr, "b3", nullptr}},
        {"_nearest", kBase + 30000, {nullptr, "S1", nullptr, nullptr}, (kBase + 3600) * 1000, (kBase + 9 * 3600) * 1000},
        {"_asof", kBase + 100000, {}},
        {"_forward", kBase + 100000, {}},
    };
    for (const Q& q : qs) {
        // Reference: per entity, the live matching records at its best second.
        std::map<std::string, std::vector<const RecModel*>> ents;
        for (const RecModel& m : w.recs) {
            if (!m.live() || (q.cond.any() && !m.matches(q.cond))) continue;
            if (m.epochSec * 1000 < q.lo || m.epochSec * 1000 >= q.hi) continue;
            ents[m.entity()].push_back(&m);
        }
        std::set<std::pair<std::string, std::string>> want;  // (entity, cid)
        for (const auto& kv : ents) {
            bool hb = false, ha = false;
            int64_t B = INT64_MIN, A = INT64_MAX;
            for (const RecModel* m : kv.second) {
                if (m->epochSec <= q.T) {
                    hb = true;
                    B = std::max(B, m->epochSec);
                }
                if (m->epochSec >= q.T) {
                    ha = true;
                    A = std::min(A, m->epochSec);
                }
            }
            const std::string c = q.col;
            int64_t best;
            if (c == "_asof") {
                if (!hb) continue;
                best = B;
            } else if (c == "_forward") {
                if (!ha) continue;
                best = A;
            } else {
                best = hb && (!ha || q.T - B <= A - q.T) ? B : A;
            }
            for (const RecModel* m : kv.second)
                if (m->epochSec == best) want.insert({kv.first, m->cid});
        }
        std::string sql = std::string("SELECT _cid, _object, _epoch FROM OMM WHERE ") + q.col + " = ?";
        std::vector<Param> ps = {Param::i64(q.T)};
        if (q.cond.provider) sql += " AND _provider = '" + std::string(q.cond.provider) + "'";
        if (q.cond.source) sql += " AND _source_name = '" + std::string(q.cond.source) + "'";
        if (q.cond.batch) sql += " AND _batch = '" + std::string(q.cond.batch) + "'";
        if (q.lo != INT64_MIN) {
            sql += " AND _epoch >= ? AND _epoch < ?";
            ps.push_back(Param::i64(q.lo));
            ps.push_back(Param::i64(q.hi));
        }
        const Rows r = bulk.q(sql, ps);
        CHECK_EQ(r.status, 0);
        std::set<std::pair<std::string, std::string>> got;
        for (size_t i = 0; i < r.rows.size(); i++) {
            const bool nullObject = r.rows[i][1].type == rb1::kNull;
            got.insert({nullObject ? "cid:" + r.s(i, 0) : r.s(i, 1), r.s(i, 0)});
        }
        CHECK_EQ(got.size(), r.rows.size());
        CHECK(got == want);
        if (got != want)
            std::fprintf(stderr, "  %s T=%lld: %zu rows, reference %zu\n", q.col, (long long)q.T, got.size(), want.size());
    }
    // Conditions the plan cannot apply before its choice are refused.
    Rows r = bulk.q("SELECT _cid FROM OMM WHERE _asof = ? AND NORAD_CAT_ID = 3", {Param::i64(mid)});
    CHECK(r.status != 0);
    w.s.close();
}

// Gap 3 (A18): untrusted SQL reads <TYPE> (and <TYPE>@<source>) as its newest
// N arrivals, whatever index the statement names: the work is bounded by N.
PS_TEST(query_gaps_sandbox_window_from_arrivals_A18) {
    World w;
    buildWorld(w);
    const uint64_t N = 60;
    ReaderConfig cfg;
    cfg.root = w.s.root;
    cfg.io = w.s.fs.get();
    cfg.cls = LaneClass::Sandbox;
    cfg.lanes = 1;
    cfg.hotWindow = {{"OMM", N}};
    cfg.sandboxMaxRowsExamined = 4 * N;  // the whole type would exceed it
    Reader sb(cfg);
    Reader bulk(w.s, LaneClass::Bulk, 1);
    REQUIRE(sb.inst && bulk.inst);
    // The window: the newest N arrivals entries, dead ones included. New
    // CIDs arrived in record order (buildWorld sends each first copy in
    // index order, waiting for its ack), so they are records 520-N..519.
    for (const Cond& c : {Cond{}, Cond{nullptr, "S1", nullptr, nullptr}, Cond{nullptr, "S2", nullptr, nullptr}}) {
        std::set<std::string> want;
        for (size_t i = w.recs.size() - N; i < w.recs.size(); i++) {
            const RecModel& m = w.recs[i];
            if (m.live() && (!c.any() || m.matches(c))) want.insert(m.cid);
        }
        for (const std::string& sql :
             {c.source ? "SELECT _cid FROM \"OMM@" + std::string(c.source) + "\"" : std::string("SELECT _cid FROM OMM"),
              c.source ? "SELECT _cid FROM OMM WHERE _source_name = '" + std::string(c.source) + "' ORDER BY _epoch"
                       : std::string("SELECT _cid FROM OMM ORDER BY _epoch ASC"),
              std::string("SELECT _cid FROM OMM WHERE NORAD_CAT_ID > 0") +
                  (c.source ? " AND _source_name = '" + std::string(c.source) + "'" : std::string())}) {
            const Rows r = sb.q(sql, {}, flatsql::ps::kReqSandbox);
            CHECK_EQ(r.status, 0);
            std::set<std::string> exp = want;
            if (sql.find("NORAD_CAT_ID > 0") != std::string::npos)
                for (const RecModel& m : w.recs)
                    if (m.norad == 0) exp.erase(m.cid);
            CHECK(cidSet(r) == exp);
            CHECK_EQ(cidSet(r).size(), r.rows.size());
            CHECK(r.outcome.rowsExamined <= 2 * N);
            if (cidSet(r) != exp)
                std::fprintf(stderr, "  sandbox %s: %zu rows, reference %zu\n", sql.c_str(), r.rows.size(), exp.size());
        }
    }
    w.s.close();
}

// Gaps 4 and 5 (A2): type-level tag conditions match a live instance of any
// live copy, in every plan, each record once; gseq-ordered tag pages are
// served from arrivals (common tags) or collected postings (rare tags) and
// page exactly.
PS_TEST(query_gaps_type_tags_every_copy_and_gseq_pages_A2) {
    World w;
    buildWorld(w);
    Reader bulk(w.s, LaneClass::Bulk, 1);
    Reader inter(w.s, LaneClass::Interactive, 1);
    REQUIRE(bulk.inst && inter.inst);
    Rows all = bulk.q("SELECT _cid, _gseq FROM OMM");
    REQUIRE(all.status == 0);
    std::map<std::string, int64_t> gseqOf;
    for (size_t i = 0; i < all.rows.size(); i++) gseqOf[all.s(i, 0)] = all.i(i, 1);
    const std::vector<Cond> conds = {
        {"P2", nullptr, nullptr, nullptr}, {nullptr, "S2", nullptr, nullptr}, {nullptr, nullptr, "b2", nullptr},
        {nullptr, nullptr, "b4", nullptr}, {nullptr, "S1", "b3", nullptr},    {nullptr, nullptr, nullptr, "peer2"},
        {"P3", nullptr, nullptr, nullptr}, {"P1", "S2", nullptr, nullptr},    {nullptr, "S1", nullptr, nullptr},
    };
    for (const Cond& c : conds) {
        std::set<std::string> want;
        std::vector<std::pair<int64_t, std::string>> byGseq;
        for (const RecModel& m : w.recs)
            if (m.live() && m.matches(c)) {
                want.insert(m.cid);
                byGseq.push_back({gseqOf[m.cid], m.cid});
            }
        std::sort(byGseq.begin(), byGseq.end());
        std::string where = " WHERE 1";
        if (c.provider) where += " AND _provider = '" + std::string(c.provider) + "'";
        if (c.source) where += " AND _source_name = '" + std::string(c.source) + "'";
        if (c.batch) where += " AND _batch = '" + std::string(c.batch) + "'";
        if (c.peer) where += " AND _peer_id = '" + std::string(c.peer) + "'";
        // Posting-driven plans (tag, source), an epoch-ordered window, a
        // column plan, arrivals order: every record once.
        for (const std::string& sql : {"SELECT _cid FROM OMM" + where, "SELECT _cid FROM OMM" + where + " ORDER BY _epoch DESC",
                                       "SELECT _cid FROM OMM" + where + " AND NORAD_CAT_ID > 0",
                                       "SELECT _cid FROM OMM" + where + " ORDER BY _gseq"}) {
            const Rows r = bulk.q(sql);
            CHECK_EQ(r.status, 0);
            std::set<std::string> exp = want;
            if (sql.find("NORAD_CAT_ID") != std::string::npos)
                for (const RecModel& m : w.recs)
                    if (m.norad == 0) exp.erase(m.cid);
            CHECK(cidSet(r) == exp);
            CHECK_EQ(r.rows.size(), exp.size());
            if (cidSet(r) != exp)
                std::fprintf(stderr, "  %s: %zu rows, reference %zu\n", sql.c_str(), r.rows.size(), exp.size());
        }
        // A datasync page sequence by tag: gseq strictly increasing, the
        // pages together are the reference in gseq order.
        std::vector<std::pair<int64_t, std::string>> paged;
        int64_t after = 0;
        for (int page = 0; page < 1000; page++) {
            const Rows r = inter.q("SELECT _gseq, _cid FROM OMM" + where + " AND _gseq > ? ORDER BY _gseq LIMIT 17",
                                   {Param::i64(after)});
            CHECK_EQ(r.status, 0);
            if (r.status != 0 || r.rows.empty()) break;
            for (size_t i = 0; i < r.rows.size(); i++) {
                CHECK(r.i(i, 0) > after);
                after = r.i(i, 0);
                paged.push_back({r.i(i, 0), r.s(i, 1)});
            }
        }
        CHECK(paged == byGseq);
        if (paged != byGseq)
            std::fprintf(stderr, "  pages %s: %zu, reference %zu\n", where.c_str(), paged.size(), byGseq.size());
    }
    // The promoted records (FIRST copies died with pD's lane) keep matching
    // through their surviving copies' tags, and each record comes once.
    Rows r = bulk.q("SELECT count(*), count(DISTINCT _cid) FROM OMM WHERE _source_name = 'S2'");
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.i(0, 0), r.i(0, 1));
    w.s.close();
}

// §38: a record stored with its own size prefix (a FinishSizePrefixed buffer
// kept as is: dataset-publication PNMs, the local EPM) is accepted, keyed and
// projected like the same record without it; its stored bytes and CID are
// the record's own.
namespace {

std::vector<uint8_t> readVector(const std::string& name) {
    std::vector<uint8_t> out;
    FILE* f = std::fopen((std::string(PS_VECTOR_DIR) + "/" + name).c_str(), "rb");
    if (!f) return out;
    uint8_t buf[65536];
    size_t n;
    while ((n = std::fread(buf, 1, sizeof(buf), f)) > 0) out.insert(out.end(), buf, buf + n);
    std::fclose(f);
    return out;
}

TestType typeFromBfbs(const std::vector<uint8_t>& bfbs, const char* name, const char* fid, const std::string& rules) {
    TestType t;
    t.name = name;
    std::memcpy(t.fid, fid, 4);
    t.bfbs = bfbs;
    t.rules = rules;
    t.config = TypeConfig::build(t.name, t.fid, t.bfbs, t.rules, t.maxFrame, t.ringCap,
                                 TypeConfig::kVerifyBfbs | TypeConfig::kVerifyCid);
    t.schema = reflection::GetSchema(t.bfbs.data());
    return t;
}

// The record with its own size prefix, framed: [u32 n + 4][u32 n][FlatBuffer].
std::vector<uint8_t> prefixedFrame(const std::vector<uint8_t>& frame) {
    std::vector<uint8_t> out(4);
    putU32(out.data(), uint32_t(frame.size()));
    out.insert(out.end(), frame.begin(), frame.end());
    return out;
}

}  // namespace

PS_TEST(query_gaps_size_prefixed_records_accepted) {
    const std::vector<uint8_t> pnmBfbs = readVector("PNM.bfbs");
    REQUIRE(!pnmBfbs.empty());
    TestType pnm = typeFromBfbs(pnmBfbs, "PNM.fbs", "$PNM", "col 1 str:FILE_ID\n");
    // The local EPM (SDN registers it with no extraction rules).
    TestType epm = makeTypeVariant(2, "$EPM", "EPM.fbs");
    epm.rules.clear();
    epm.config = TypeConfig::build(epm.name, epm.fid, epm.bfbs, "", epm.maxFrame, epm.ringCap,
                                   TypeConfig::kVerifyBfbs | TypeConfig::kVerifyCid);
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType(), &catType(), &pnm, &epm});
    struct Case {
        TestType* t;
        std::vector<uint8_t> frame;  // the bare record, framed
        std::string col;             // an indexed column and its value
        std::string strVal;
        int64_t intVal = 0;
    };
    std::vector<Case> cases;
    for (int placement = 0; placement < 2; placement++) {
        const std::string tag = placement ? "P" : "B";
        cases.push_back({&ommType(), ommRecord(uint32_t(70000 + placement), "2026-0" + tag, "2026-09-01T00:00:0" + std::to_string(placement) + "Z", 15.5),
                         "NORAD_CAT_ID", "", 70000 + placement});
        cases.push_back({&catType(), catRecord(uint32_t(71000 + placement), "CAT-" + tag, "uri://" + tag, "c" + tag, "SAT " + tag),
                         "NORAD_CAT_ID", "", 71000 + placement});
        cases.push_back({&pnm, buildRecord(pnm, {Field::str("FILE_ID", "file-" + tag), Field::str("FILE_NAME", "n-" + tag)}),
                         "FILE_ID", "file-" + tag, 0});
        cases.push_back({&epm, catRecord(uint32_t(72000 + placement), "EPM-" + tag, "uri://e" + tag, "e" + tag, "ENT " + tag),
                         "", "", 0});
        if (placement) {
            // The EPM variant shares CAT's layout under "$EPM".
            cases.back().frame = buildRecord(epm, {Field::u64("NORAD_CAT_ID", 72001), Field::str("OBJECT_ID", "EPM-P")});
            for (size_t i = cases.size() - 4; i < cases.size(); i++) cases[i].frame = prefixedFrame(cases[i].frame);
        } else {
            cases.back().frame = buildRecord(epm, {Field::u64("NORAD_CAT_ID", 72000), Field::str("OBJECT_ID", "EPM-B")});
        }
    }
    int64_t arrival = 1790000000000ll;
    for (Case& c : cases) {
        const uint32_t pid = s.partition("prod-" + c.t->name, *c.t);
        Producer prod(s.e.get(), pid);
        const uint64_t r = send(s.e.get(), prod, c.frame, buildRecordAttr("p", "prov", "src", "b1"), arrival++);
        REQUIRE(r != 0);
        CHECK_EQ(prod.waitAcked(r, 20000000000ull), 0);
        CHECK_EQ(prod.rejectCode(r), 0);
        if (prod.rejectCode(r) != 0) std::fprintf(stderr, "  %s rejected %d\n", c.t->name.c_str(), prod.rejectCode(r));
    }
    std::vector<uint32_t> pids;
    for (TestType* t : {&ommType(), &catType(), &pnm, &epm}) pids.push_back(s.partition("prod-" + t->name, *t));
    REQUIRE(waitLabeledEngine(s.e.get(), pids, 20000000000ull));
    Reader bulk(s, LaneClass::Bulk, 1);
    REQUIRE(bulk.inst);
    for (const Case& c : cases) {
        const std::string typ = c.t->name.substr(0, 3);
        // Stored as sent: _data is the record (with its own prefix when it
        // came with one), and the CID is the record's.
        const std::string cid = cidTextOf(c.frame);
        Rows r = bulk.q("SELECT _data, _cid FROM " + typ + " WHERE _cid = ?", {Param::text(cid)});
        CHECK_EQ(r.status, 0);
        REQUIRE(r.rows.size() == 1);
        CHECK(r.s(0, 0) == std::string(c.frame.begin() + 4, c.frame.end()));
        CHECK(r.s(0, 1) == cid);
        if (c.col.empty()) continue;
        // Keys were extracted from the FlatBuffer after the record's prefix:
        // the column index finds it and the column projects.
        const Param key = c.strVal.empty() ? Param::i64(c.intVal) : Param::text(c.strVal);
        r = bulk.q("SELECT _cid, " + c.col + " FROM " + typ + " WHERE " + c.col + " = ?", {key});
        CHECK_EQ(r.status, 0);
        REQUIRE(r.rows.size() == 1);
        CHECK(r.s(0, 0) == cid);
        if (c.strVal.empty()) CHECK_EQ(r.i(0, 1), c.intVal);
        else CHECK(r.s(0, 1) == c.strVal);
    }
    // The OMM epoch came from the payload in both placements.
    Rows r = bulk.q("SELECT NORAD_CAT_ID, _epoch FROM OMM ORDER BY NORAD_CAT_ID");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 2);
    CHECK_EQ(r.i(0, 1), int64_t(1788220800000ll));
    CHECK_EQ(r.i(1, 1), int64_t(1788220801000ll));
    // A record whose inner prefix does not match its length is still refused.
    {
        std::vector<uint8_t> bad = prefixedFrame(buildRecord(pnm, {Field::str("FILE_ID", "bad")}));
        putU32(bad.data() + 4, getU32(bad.data() + 4) + 1);
        Producer prod(s.e.get(), pids[2]);
        const uint64_t rq = send(s.e.get(), prod, bad, buildRecordAttr("p", "prov", "src", "b1"), arrival++);
        REQUIRE(rq != 0);
        CHECK_EQ(prod.waitAcked(rq, 20000000000ull), 0);
        CHECK_EQ(prod.rejectCode(rq), int32_t(kRejFid));
    }
    s.close();
}
