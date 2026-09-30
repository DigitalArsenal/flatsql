// Reader lanes (T2): the snapshot protocol end to end through SQL, the
// partition and fan-out vtabs, meta vtabs, the mailbox ABI (parking,
// cancellation), admission, and the sandbox.
#include <algorithm>
#include <map>
#include <set>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {
std::string ep(int i) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-08-%02dT%02d:%02d:%02dZ", 1 + (i / 3600) % 28, (i / 60) % 24, i % 60, i % 60);
    return b;
}

struct Fixture {
    Store s{true, 2, true};
    std::vector<uint32_t> pids;
    std::vector<std::vector<uint8_t>> recs;
    std::map<std::string, int> cidIndex;
    Fixture() {}
};
}  // namespace

PS_TEST(lane_partition_and_type_queries) {
    Store s(true, 2, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType(), &catType()});
    const uint32_t p1 = s.partition("peerA", ommType());
    const uint32_t p2 = s.partition("peerB", ommType());
    std::vector<std::vector<uint8_t>> recs;
    for (int i = 0; i < 300; i++) recs.push_back(ommRecord(uint32_t(40000 + i), "OBJ-" + std::to_string(i), ep(i), 15.0 + i));
    {
        Producer a(s.e.get(), p1), b(s.e.get(), p2);
        const auto attrA = buildRecordAttr("peerA", "prov", "srcA", "b1");
        const auto attrB = buildRecordAttr("peerB", "prov", "srcB", "b1");
        uint64_t la = 0, lb = 0;
        // peerA first (labeled), so records 100..199 are FIRST in peerA.
        for (int i = 0; i < 200; i++) la = send(s.e.get(), a, recs[size_t(i)], attrA, 1000 + i);
        CHECK_EQ(a.waitAcked(la, 10000000000ull), 0);
        CHECK(waitLabeledEngine(s.e.get(), {p1}, 10000000000ull));
        for (int i = 100; i < 300; i++) lb = send(s.e.get(), b, recs[size_t(i)], attrB, 2000 + i);
        CHECK_EQ(b.waitAcked(lb, 10000000000ull), 0);
    }
    REQUIRE(waitTypeVisible(s.fs.get(), s.root, ommType().fid, {p1, p2}, 10000000000ull));
    Reader bulk(s, LaneClass::Bulk, 2);
    REQUIRE(bulk.inst);
    // Partition level: every row of the partition.
    Rows r = bulk.q("SELECT count(*), min(_pseq), max(_pseq) FROM sds_p_peerA__OMM");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 1);
    CHECK_EQ(r.i(0, 0), 200);
    CHECK_EQ(r.i(0, 1), 1);
    CHECK_EQ(r.i(0, 2), 200);
    // Type level: one row per cid (300 distinct), FIRST copies only.
    r = bulk.q("SELECT count(*), count(DISTINCT _cid) FROM OMM");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 1);
    CHECK_EQ(r.i(0, 0), 300);
    CHECK_EQ(r.i(0, 1), 300);
    // CID lookup through the catalog; schema columns project.
    const std::string cid = cidTextOf(recs[150]);
    r = bulk.q("SELECT NORAD_CAT_ID, OBJECT_ID, _producer, _source, _pid FROM OMM WHERE _cid = ?", {Param::text(cid)});
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 1);
    CHECK_EQ(r.i(0, 0), 40150);
    CHECK(r.s(0, 1) == "OBJ-150");
    CHECK(r.s(0, 2) == "peerA");  // FIRST copy: peerA acked it first
    CHECK(r.s(0, 3) == "OMM@srcA");
    CHECK_EQ(r.i(0, 4), int64_t(p1));
    // Default order (A19): seconds DESC, CID ASC; LIMIT stops the merge.
    Reader inter(s, LaneClass::Interactive, 2);
    r = inter.q("SELECT _epoch, _cid FROM OMM LIMIT 20");
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.rows.size(), size_t(20));
    for (size_t i = 1; i < r.rows.size(); i++) {
        const int64_t a = epochSecFloor(r.i(i - 1, 0)), b = epochSecFloor(r.i(i, 0));
        CHECK(a > b || (a == b && r.s(i - 1, 1) < r.s(i, 1)));
    }
    CHECK(r.outcome.rowsExamined <= 20 * 3);
    // Unbounded on an interactive lane: NEEDS_BULK before any row.
    r = inter.q("SELECT count(*) FROM OMM");
    CHECK_EQ(r.status, int32_t(kRsNeedsBulk));
    CHECK_EQ(r.outcome.rowsExamined, uint64_t(0));
    // Column index (COL 0 = NORAD_CAT_ID) is bounded.
    r = inter.q("SELECT _cid FROM OMM WHERE NORAD_CAT_ID = ?", {Param::i64(40042)});
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 1);
    CHECK(r.s(0, 0) == cidTextOf(recs[42]));
    // Source window (the IQC incident shape).
    r = inter.q("SELECT _cid, _epoch FROM OMM WHERE _source = 'OMM@srcB' ORDER BY _epoch DESC LIMIT 5");
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.rows.size(), size_t(5));
    // Alias vtab: records with a live srcB instance on any live copy
    // (PARTITION-STORE.md §38): 200..299 (FIRST in peerB) and 100..199
    // (FIRST in peerA, REPEAT copies in peerB).
    r = bulk.q("SELECT count(*) FROM \"OMM@srcB\"");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 1);
    CHECK_EQ(r.i(0, 0), 200);
    // Meta: counters from heads.
    r = inter.q("SELECT pid, live_count, total_count FROM flatsql_partitions ORDER BY pid");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 2);
    CHECK_EQ(r.i(0, 1), 200);
    CHECK_EQ(r.i(1, 1), 200);
    r = inter.q("SELECT source, count, bytes FROM flatsql_lanes ORDER BY pid");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 2);
    CHECK(r.s(0, 0) == "srcA");
    CHECK_EQ(r.i(0, 1), 200);
    // Arrivals: 300 FIRST entries in gseq order.
    r = bulk.q("SELECT gseq FROM flatsql_arrivals WHERE type = 'OMM'");
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.rows.size(), size_t(300));
    for (size_t i = 1; i < r.rows.size(); i++) CHECK(r.i(i, 0) > r.i(i - 1, 0));
    // _rowid = _gseq at type level; gseq order is arrivals order.
    r = inter.q("SELECT _rowid, _gseq FROM OMM ORDER BY _rowid LIMIT 10");
    CHECK_EQ(r.status, 0);
    REQUIRE(r.rows.size() == 10);
    for (size_t i = 0; i < r.rows.size(); i++) CHECK_EQ(r.i(i, 0), r.i(i, 1));
    // Raw stream: the stored frames verbatim.
    Outcome o;
    const auto frames = bulk.raw("SELECT _data FROM sds_p_peerA__OMM WHERE _pseq BETWEEN 1 AND 3", {}, &o);
    CHECK_EQ(o.status, 0);
    REQUIRE(frames.size() == 3);
    for (int i = 0; i < 3; i++)
        CHECK(frames[size_t(i)] == std::string(reinterpret_cast<const char*>(recs[size_t(i)].data()) + 4, recs[size_t(i)].size() - 4));
    s.close();
}

PS_TEST(lane_parking_and_cancellation) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t p1 = s.partition("peerA", ommType());
    {
        Producer a(s.e.get(), p1);
        const auto attr = buildRecordAttr("peerA", "prov", "src", "b1");
        uint64_t last = 0;
        for (int i = 0; i < 2000; i++) last = send(s.e.get(), a, ommRecord(uint32_t(i + 1), "X", ep(i), 1.0 + i, 200), attr, i);
        CHECK_EQ(a.waitAcked(last, 10000000000ull), 0);
    }
    REQUIRE(waitTypeVisible(s.fs.get(), s.root, ommType().fid, {p1}, 10000000000ull));
    // One lane, a 16 KiB result ring: the first statement parks, the lane
    // serves others meanwhile (A28: never blocks on output).
    ReaderConfig cfg;
    cfg.root = s.root;
    cfg.io = s.fs.get();
    cfg.cls = LaneClass::Bulk;
    cfg.lanes = 1;
    cfg.ringBytes = 16u << 10;
    Reader r(cfg);
    REQUIRE(r.inst);
    ReaderClient c(r.inst.get());
    Request big;
    big.sql = "SELECT _data FROM sds_p_peerA__OMM";
    uint32_t slot;
    REQUIRE(c.submit(big, &slot) == 0);
    // Wait until it parks.
    const uint64_t until = monoNs() + 5000000000ull;
    while (r.inst->slot(slot)->state.load() != kSlotParked && monoNs() < until) sleepNs(100000);
    CHECK_EQ(r.inst->slot(slot)->state.load(), uint32_t(kSlotParked));
    // Another statement completes on the same lane while the first is parked.
    Rows small = r.q("SELECT count(*) FROM flatsql_partitions");
    CHECK_EQ(small.status, 0);
    // Drain the parked statement fully and decode it.
    std::vector<uint8_t> out, buf(4096);
    for (;;) {
        const int64_t n = c.read(slot, buf.data(), buf.size(), 5000000000ull);
        if (n <= 0) break;
        out.insert(out.end(), buf.begin(), buf.begin() + n);
    }
    Outcome o = c.finish(slot);
    CHECK_EQ(o.status, 0);
    rb1::Decoder d;
    CHECK(d.feed(out.data(), out.size()) && d.done());
    CHECK_EQ(d.rows().size(), size_t(2000));
    CHECK(r.inst->stats().parks > 0);
    // Cancellation of a parked statement.
    REQUIRE(c.submit(big, &slot) == 0);
    while (r.inst->slot(slot)->state.load() != kSlotParked && monoNs() < until + 5000000000ull) sleepNs(100000);
    c.cancel(slot);
    for (;;) {
        const int64_t n = c.read(slot, buf.data(), buf.size(), 5000000000ull);
        if (n <= 0) break;
    }
    o = c.finish(slot);
    CHECK_EQ(o.status, int32_t(kRsCancelled));
    s.close();
}

PS_TEST(lane_sandbox_contract_A28) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t p1 = s.partition("peerA", ommType());
    {
        Producer a(s.e.get(), p1);
        const auto attr = buildRecordAttr("peerA", "prov", "src", "b1");
        uint64_t last = 0;
        for (int i = 0; i < 3000; i++) last = send(s.e.get(), a, ommRecord(uint32_t(i + 1), "X", ep(i), 1.0 + i), attr, i);
        CHECK_EQ(a.waitAcked(last, 10000000000ull), 0);
    }
    REQUIRE(waitTypeVisible(s.fs.get(), s.root, ommType().fid, {p1}, 10000000000ull));
    ReaderConfig cfg;
    cfg.root = s.root;
    cfg.io = s.fs.get();
    cfg.cls = LaneClass::Sandbox;
    cfg.lanes = 1;
    cfg.sandboxMaxRowsExamined = 20000;
    Reader sb(cfg);
    Reader bulk(s, LaneClass::Bulk, 1);
    const uint32_t kSb = flatsql::ps::kReqSandbox;
    // A bounded public read works.
    Rows r = sb.q("SELECT NORAD_CAT_ID FROM OMM WHERE NORAD_CAT_ID = 7", {}, kSb);
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.rows.size(), size_t(1));
    // Meta tables are not public; writes and multiple statements are refused.
    r = sb.q("SELECT * FROM flatsql_partitions", {}, kSb);
    CHECK(r.status != 0);
    r = sb.q("SELECT 1; SELECT 2", {}, kSb);
    CHECK_EQ(r.status, int32_t(kRsNotAuthorized));
    r = sb.q("CREATE TABLE t(x)", {}, kSb);
    CHECK(r.status != 0);
    // A cartesian join returns `timeout` (the work budget) on the sandbox lane,
    // while the bulk lane stays free.
    std::atomic<int32_t> sbStatus{1};
    std::thread t([&] { sbStatus = sb.q("SELECT count(*) FROM OMM a, OMM b", {}, kSb).status; });
    const uint64_t t0 = monoNs();
    Rows br = bulk.q("SELECT count(*) FROM sds_p_peerA__OMM");
    const uint64_t bulkNs = monoNs() - t0;
    t.join();
    CHECK_EQ(sbStatus.load(), int32_t(kRsTimeout));
    CHECK_EQ(br.status, 0);
    report("bulk_statement_during_sandbox_join_ms", double(bulkNs) / 1e6, "ms");
    // Sandbox SQL is refused on the shared bulk lane.
    r = bulk.q("SELECT NORAD_CAT_ID FROM OMM WHERE NORAD_CAT_ID = 7", {}, kSb);
    CHECK_EQ(r.status, int32_t(kRsNeedsBulk));
    s.close();
}

// The reader C ABI (flatsql_ps.h): a statement driven through raw memory the
// way the Go router does it (claim, write, queue, doorbell, read the ring).
#include <filesystem>

#include "flatsql/ps/flatsql_ps.h"

PS_TEST(lane_c_abi_mailbox_protocol) {
    const std::string dir = std::filesystem::temp_directory_path().string() + "/flatsql-ps-capi-" +
                            std::to_string(monoNs());
    std::filesystem::create_directories(dir);
    {
        Store s(false, 1, true);
        s.cfg.io = nullptr;  // the native seven-import host
        s.cfg.root = dir;
        s.root = dir;
        REQUIRE(s.open() == 0);
        s.registerTypes({&ommType()});
        const uint32_t p1 = s.partition("capi", ommType());
        Producer a(s.e.get(), p1);
        const auto attr = buildRecordAttr("capi", "prov", "src", "b1");
        uint64_t last = 0;
        for (int i = 0; i < 50; i++) last = send(s.e.get(), a, ommRecord(uint32_t(i + 1), "C", ep(i), 1.0), attr, i);
        CHECK_EQ(a.waitAcked(last, 10000000000ull), 0);
        s.close();
    }
    std::vector<uint8_t> cfg;
    auto tlv = [&](uint16_t tag, const void* v, uint32_t n) {
        const size_t at = cfg.size();
        cfg.resize(at + 6 + n);
        putU16(cfg.data() + at, tag);
        putU32(cfg.data() + at + 2, n);
        std::memcpy(cfg.data() + at + 6, v, n);
    };
    tlv(1, dir.data(), uint32_t(dir.size()));
    const uint32_t lanes = 2;
    tlv(20, &lanes, 4);
    REQUIRE(flatsql_ps_init(FLATSQL_PS_ROLE_READER_INTERACTIVE, cfg.data(), int32_t(cfg.size())) == 0);
    REQUIRE(flatsql_ps_start() == 0);
    FlatsqlPsReaderLayout L;
    REQUIRE(flatsql_ps_reader_layout(&L) == int32_t(sizeof(L)));
    CHECK_EQ(L.nLanes, 2u);
    // Layout addresses are wasm32 offsets (the low 32 bits natively): this
    // native test drives an instance opened the same way through the
    // layout's field offsets, which must match the struct they describe.
    CHECK_EQ(L.offState, uint32_t(offsetof(SlotHeader, state)));
    CHECK_EQ(L.headerSize, uint32_t(sizeof(SlotHeader)));
    // Drive slot 0 through raw offsets on a ReaderInstance opened the same way.
    ReaderConfig rc;
    rc.root = dir;
    rc.lanes = 1;
    Reader r(rc);
    REQUIRE(r.inst);
    uint8_t* slot = reinterpret_cast<uint8_t*>(r.inst->slot(0));
    auto u32at = [&](uint32_t off) { return reinterpret_cast<std::atomic<uint32_t>*>(slot + off); };
    auto u64at = [&](uint32_t off) { return reinterpret_cast<std::atomic<uint64_t>*>(slot + off); };
    uint32_t expect = kSlotFree;
    REQUIRE(u32at(L.offState)->compare_exchange_strong(expect, kSlotClaimed));
    const std::string sql = "SELECT count(*), sum(live_count) FROM flatsql_partitions";
    std::memcpy(slot + L.headerSize, sql.data(), sql.size());
    putU32(slot + L.offSqlLen, uint32_t(sql.size()));
    putU32(slot + L.offParamsLen, 0);
    putU32(slot + L.offFlags, 0);
    u64at(L.offRingHead)->store(0);
    u64at(L.offRingTail)->store(0);
    u32at(L.offState)->store(kSlotQueued);
    REQUIRE(r.inst->enqueue(0) == 0);
    const uint64_t until = monoNs() + 5000000000ull;
    while (u32at(L.offState)->load() != kSlotDone && monoNs() < until) sleepNs(100000);
    REQUIRE(u32at(L.offState)->load() == kSlotDone);
    const uint64_t tail = u64at(L.offRingTail)->load();
    const uint8_t* ring = slot + L.headerSize + r.inst->config().reqBytes;
    rb1::Decoder d;
    CHECK(d.feed(ring, size_t(tail)) && d.done());
    REQUIRE(d.rows().size() == 1);
    CHECK_EQ(d.rows()[0][0].i, 1);
    CHECK_EQ(d.rows()[0][1].i, 50);
    CHECK_EQ(int32_t(getU32(slot + L.offStatus)), 0);
    u32at(L.offState)->store(kSlotFree);
    CHECK_EQ(flatsql_ps_stop(5000), 0);
    std::filesystem::remove_all(dir);
}

// Tag conditions have the legacy tag table's ANY-row semantics (A2): a record
// matches when one live tag instance, its PUT's own tag or a RETAG, satisfies
// every condition. The vtab evaluates them itself; SQLite's re-check against
// the projected columns (the PUT's own tag only) used to drop every record
// matched through a RETAG (T6 #7: a provider window of 200 read as 100).
PS_TEST(lane_tag_conditions_match_any_instance_A2) {
    Store s(true, 2, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t gp = s.partition("gp", ommType());
    const uint32_t gp2 = s.partition("gp2", ommType());
    std::vector<std::vector<uint8_t>> recs;
    for (int i = 0; i < 350; i++) recs.push_back(ommRecord(uint32_t(60000 + i), "T-" + std::to_string(i), ep(i), 1.0 + i));
    struct Tag {
        std::string provider, source, batch, peer;
    };
    const Tag t1{"P1", "S1", "b1", "peer1"}, t2{"P2", "S2", "b2", "peer2"}, t3{"P1", "S1", "b3", "peer3"};
    // Instances per partition: record index -> tags.
    std::map<uint32_t, std::map<int, std::vector<Tag>>> inst;
    auto sendRange = [&](uint32_t pid, const std::string& peer, int from, int to, const Tag& t) {
        Producer prod(s.e.get(), pid);
        const auto attr = buildRecordAttr(peer, t.provider, t.source, t.batch, "", t.peer);
        uint64_t last = 0;
        for (int i = from; i < to; i++) {
            last = send(s.e.get(), prod, recs[size_t(i)], attr, 5000 + i);
            inst[pid][i].push_back(t);
        }
        CHECK_EQ(prod.waitAcked(last, 10000000000ull), 0);
        CHECK(waitLabeledEngine(s.e.get(), {gp, gp2}, 10000000000ull));
    };
    sendRange(gp, "gp", 0, 200, t1);     // PUTs tagged t1
    sendRange(gp, "gp", 100, 300, t2);   // 100..199 RETAG with t2; 200..299 PUTs tagged t2
    sendRange(gp2, "gp2", 250, 350, t3); // 250..299 REPEAT copies; 300..349 FIRST in gp2
    REQUIRE(waitTypeVisible(s.fs.get(), s.root, ommType().fid, {gp, gp2}, 10000000000ull));
    // Every instance of every copy is live: RETAG rows were written.
    CHECK_EQ(s.e->stats().retags, uint64_t(100));
    struct Cond {
        const char* provider = nullptr;
        const char* source = nullptr;
        const char* batch = nullptr;
        const char* peer = nullptr;
    };
    // Type level (§38): every live copy's instances count (the record held by
    // gp and gp2 matches on either); partition level: that partition's.
    auto oracle = [&](uint32_t pid, bool typeLevel, const Cond& c) {
        std::set<std::string> out;
        for (int i = 0; i < 350; i++) {
            for (uint32_t at : typeLevel ? std::vector<uint32_t>{gp, gp2} : std::vector<uint32_t>{pid}) {
                auto it = inst[at].find(i);
                if (it == inst[at].end()) continue;
                for (const Tag& t : it->second)
                    if ((!c.provider || t.provider == c.provider) && (!c.source || t.source == c.source) &&
                        (!c.batch || t.batch == c.batch) && (!c.peer || t.peer == c.peer))
                        out.insert(cidTextOf(recs[size_t(i)]));
            }
        }
        return out;
    };
    Reader bulk(s, LaneClass::Bulk, 2);
    REQUIRE(bulk.inst);
    auto run = [&](const std::string& table, const Cond& c, const std::string& extraWhere = "") {
        std::string sql = "SELECT _cid FROM \"" + table + "\" WHERE 1";
        std::vector<Param> ps;
        if (c.provider) { sql += " AND _provider = ?"; ps.push_back(Param::text(c.provider)); }
        if (c.source) { sql += " AND _source_name = ?"; ps.push_back(Param::text(c.source)); }
        if (c.batch) { sql += " AND _batch = ?"; ps.push_back(Param::text(c.batch)); }
        if (c.peer) { sql += " AND _peer_id = ?"; ps.push_back(Param::text(c.peer)); }
        sql += extraWhere;
        const Rows r = bulk.q(sql, ps);
        CHECK_EQ(r.status, 0);
        std::set<std::string> got;
        for (size_t i = 0; i < r.rows.size(); i++) got.insert(r.s(i, 0));
        CHECK_EQ(got.size(), r.rows.size());  // one row per record
        return got;
    };
    const std::vector<Cond> conds = {
        {"P2", nullptr, nullptr, nullptr}, {nullptr, nullptr, "b2", nullptr}, {nullptr, nullptr, nullptr, "peer2"},
        {nullptr, "S2", nullptr, nullptr}, {"P1", nullptr, nullptr, nullptr}, {nullptr, "S2", "b2", nullptr},
        {nullptr, "S1", "b2", nullptr},    {"P1", nullptr, "b1", nullptr},    {"P1", nullptr, "b3", nullptr},
        {"P1", "S1", "b1", "peer1"},       {"P2", "S1", nullptr, nullptr},
    };
    size_t retagMatched = 0;
    for (const Cond& c : conds) {
        const auto want = oracle(0, true, c);
        const auto got = run("OMM", c);
        CHECK(got == want);
        if (got != want)
            std::fprintf(stderr, "  OMM %s/%s/%s/%s: %zu rows, oracle %zu\n", c.provider ? c.provider : "-",
                         c.source ? c.source : "-", c.batch ? c.batch : "-", c.peer ? c.peer : "-", got.size(),
                         want.size());
        for (uint32_t pid : {gp, gp2}) {
            const auto pw = oracle(pid, false, c);
            const auto pg = run(pid == gp ? "sds_p_gp__OMM" : "sds_p_gp2__OMM", c);
            CHECK(pg == pw);
        }
        for (int i = 100; i < 200; i++) retagMatched += got.count(cidTextOf(recs[size_t(i)])) && c.provider &&
                                                         std::string(c.provider) == "P2";
    }
    CHECK_EQ(retagMatched, size_t(100));  // the RETAG'd records are in the P2 window
    CHECK_EQ(oracle(0, true, {"P2", nullptr, nullptr, nullptr}).size(), size_t(200));
    // The full '<TYPE>@<source>' form, an epoch-ordered window, and the alias.
    Rows r = bulk.q("SELECT count(*) FROM OMM WHERE _source = 'OMM@S2'");
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.i(0, 0), 200);
    r = bulk.q("SELECT count(*) FROM OMM WHERE _source = 'omm@S2'");  // compared as projected
    CHECK_EQ(r.i(0, 0), 0);
    r = bulk.q("SELECT count(*) FROM \"OMM@S2\" WHERE _batch = 'b2'");
    CHECK_EQ(r.i(0, 0), 200);
    r = bulk.q("SELECT count(*) FROM \"OMM@S2\" WHERE _batch = 'b1'");
    CHECK_EQ(r.i(0, 0), 0);
    Reader inter(s, LaneClass::Interactive, 2);
    r = inter.q("SELECT _cid, _epoch FROM OMM WHERE _source = 'OMM@S2' ORDER BY _epoch DESC LIMIT 150");
    CHECK_EQ(r.status, 0);
    CHECK_EQ(r.rows.size(), size_t(150));
    for (size_t i = 1; i < r.rows.size(); i++) CHECK(r.i(i - 1, 1) >= r.i(i, 1));
    // Other plans (a column index, a CID) apply the conditions to any live
    // instance of the record.
    r = bulk.q("SELECT count(*) FROM OMM WHERE NORAD_CAT_ID = ? AND _provider = 'P2'", {Param::i64(60150)});
    CHECK_EQ(r.i(0, 0), 1);
    r = bulk.q("SELECT count(*) FROM OMM WHERE NORAD_CAT_ID = ? AND _provider = 'P2'", {Param::i64(60050)});
    CHECK_EQ(r.i(0, 0), 0);
    r = bulk.q("SELECT count(*) FROM OMM WHERE _cid = ? AND _batch = 'b2'", {Param::text(cidTextOf(recs[150]))});
    CHECK_EQ(r.i(0, 0), 1);
    r = bulk.q("SELECT count(*) FROM OMM WHERE _cid = ? AND _batch = 'b3'", {Param::text(cidTextOf(recs[260]))});
    CHECK_EQ(r.i(0, 0), 1);  // b3 tags only the REPEAT copy in gp2: every copy counts (§38)
    // The projection still shows the PUT's own tag.
    r = bulk.q("SELECT _provider, _batch FROM OMM WHERE _cid = ?", {Param::text(cidTextOf(recs[150]))});
    REQUIRE(r.rows.size() == 1);
    CHECK(r.s(0, 0) == "P1" && r.s(0, 1) == "b1");
    s.close();
}
