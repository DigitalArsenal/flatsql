// Contract v3-v9 semantics in the engine (CONTRACT.md §0.3): C-21 and C-26
// (identities through a migration, and a repeat's tags on the held record),
// C-22 (a torn STORE reopens as a migration target; a migrate COPY keeps the
// holder's bytes and ts), C-25 (a type with an empty BFBS), C-27 (REBUILD 8
// runs integrity_check), and the file rebuild by VACUUM INTO (design §7) and
// a crash between a file's creation and its schema (journal replay).
#include <cstring>
#include <fstream>
#include <set>
#include <thread>

#include "internal.h"
#include "p4/p4_test.h"

#if !defined(__wasm__)
#include "sqlite3.h"
#endif

namespace flatsql {
namespace p4 {
P4Engine* currentEngine();
}
}  // namespace flatsql

using namespace p4t;
namespace fp = flatsql::p4;

namespace {

std::vector<uint8_t> cidOfFrame(const std::vector<uint8_t>& frame) {
    uint8_t c[36];
    cidOf(frame, c);
    return std::vector<uint8_t>(c, c + 36);
}

// TAGS (op 2) for one CID: the set of batch names.
std::set<std::string> batchesOf(const std::string& type, const std::vector<uint8_t>& cid) {
    TlvW w;
    w.text(1, type);
    std::vector<uint8_t> list(4);
    fp::st32(list.data(), 1);
    list.insert(list.end(), cid.begin(), cid.end());
    w.raw(40, list.data(), list.size());
    Result r = call(P4_OPC_TAGS, w.b);
    std::set<std::string> out;
    for (size_t i = 0; i < r.rows.size(); i++) out.insert(r.s(i, "batch"));
    return out;
}

Result rebuild(uint32_t what, const std::string& type = "") {
    TlvW w;
    if (!type.empty()) w.text(1, type);
    w.u32(63, what);
    return call(P4_OPC_REBUILD, w.b);
}

std::vector<uint8_t> pnmFrame(const TestType& t, uint64_t id, size_t pad = 200) {
    return buildFrame(t, {Field::str("FILE_ID", "f" + std::to_string(id)), Field::str("NAME", "n" + std::to_string(id)),
                          Field::raw("BODY", std::vector<uint8_t>(pad, uint8_t(id)))});
}

Batch pnmBatch(const TestType& t, const std::string& peer, const std::string& batch, uint64_t from, int n) {
    Batch b;
    b.type = t.name;
    b.peer = peer;
    b.tags.push_back(Tag{"prov", "src", "", batch, "", "", ""});
    b.at = 1790000000;
    for (int i = 0; i < n; i++) {
        In in;
        in.frame = pnmFrame(t, from + uint64_t(i));
        in.ts = 1790000000;
        b.recs.push_back(std::move(in));
    }
    return b;
}

}  // namespace

// C-21, C-26, C-22 (COPY): a migrated IQC record keeps its ingest identity;
// a later re-pull is IDENT_DUP and tags the held record; a migrate COPY
// stores the holder's bytes and ts.
P4_TEST(t_contract_migrate_identity_and_copy) {
    const std::string root = scratchDir("c21") + "/fsql4";
    EngineOpts o;
    o.createMode = 2;
    o.gseqFloor = 1000;
    REQUIRE(openEngine(root, o) == P4_OK, "open the migration target");
    REQUIRE(registerType(iqcType()) == P4_OK, "register IQC");
    TestType pnm = pnmLikeType("PNM");
    REQUIRE(registerType(pnm) == P4_OK, "register PNM");
    Batch m;
    m.type = "IQC";
    m.peer = "12D3KooWSigmf";
    m.mode = 1;
    m.tags.push_back(Tag{"iqengine", "IQEngine", "", "b0", "", "", ""});
    In a;
    a.frame = iqcFrame("E1", "2026-01-10T00:00:00Z", 64, 1);
    a.ts = 1789000000;
    a.seq = 5;
    a.hasIdent = true;
    std::memset(a.ident, 7, 32);
    a.tags.push_back({0, 1789000001});
    m.recs = {a};
    Result r = put(m);
    REQUIRE(r.status == P4_OK && r.rows.size() == 1, r.err);
    CHECK_EQ(r.i(0, "action"), int64_t(P4_ACT_MIGRATED), "migrated");
    // A migrate COPY keeps the holder's ts (C-22): the same bytes from another
    // producer with another ts.
    Batch c1;
    c1.type = "PNM";
    c1.peer = "12D3KooWA";
    c1.mode = 1;
    c1.tags.push_back(Tag{"prov", "src", "", "b0", "", "", ""});
    In x;
    x.frame = pnmFrame(pnm, 1);
    x.ts = 100;
    x.seq = 7;
    x.tags.push_back({0, 101});
    c1.recs = {x};
    REQUIRE(put(c1).status == P4_OK, "holder");
    Batch c2 = c1;
    c2.peer = "12D3KooWB";
    c2.recs[0].ts = 200;
    Result cr = put(c2);
    REQUIRE(cr.status == P4_OK && cr.rows.size() == 1, cr.err);
    CHECK_EQ(cr.i(0, "action"), int64_t(P4_ACT_COPY), "a copy");
    Result every = get("PNM", {cidOfFrame(x.frame)}, true, true);
    CHECK_EQ(every.rows.size(), size_t(2), "two copies");
    for (size_t i = 0; i < every.rows.size(); i++) CHECK_EQ(every.i(i, "ts"), int64_t(100), "the holder's ts");
    CHECK_EQ(flatsql_p4_activate(), P4_OK, "activate");
    closeEngine();

    o.createMode = 0;
    REQUIRE(openEngine(root, o) == P4_OK, "reopen activated");
    Batch pull;
    pull.type = "IQC";
    pull.peer = "12D3KooWSigmf";
    pull.tags.push_back(Tag{"iqengine", "IQEngine", "", "b1", "", "", ""});
    pull.at = 1790000000;
    In a2;
    a2.frame = iqcFrame("E1", "2026-01-10T00:00:00Z", 64, 2);  // a re-fetch: other bytes, same capture
    a2.ts = 1790000000;
    a2.hasIdent = true;
    std::memset(a2.ident, 7, 32);
    pull.recs = {a2};
    Result d = put(pull);
    REQUIRE(d.status == P4_OK && d.rows.size() == 1, d.err);
    CHECK_EQ(d.i(0, "action"), int64_t(P4_ACT_IDENT_DUP), "a re-pull of a migrated capture is IDENT_DUP (C-21)");
    CHECK_EQ(d.i(0, "seq"), int64_t(5), "the migrated record's seq");
    const std::set<std::string> want = {"b0", "b1"};
    CHECK(batchesOf("IQC", cidOfFrame(a.frame)) == want, "the repeat's tag instance is on the held record (C-26)");
    CHECK_EQ(get("IQC", {cidOfFrame(a2.frame)}).rows.size(), size_t(0), "the repeat's bytes are not stored");
    Result v = rebuild(8);
    for (size_t i = 0; i < v.rows.size(); i++) CHECK_EQ(v.i(i, "mismatches"), int64_t(0), "verify " + v.s(i, "type"));
    closeEngine();
}

// C-22: a torn STORE next to a valid MIGRATED reopens in create mode 2 and
// activation rewrites it; a valid STORE refuses create mode 2.
P4_TEST(t_contract_torn_store) {
    const std::string root = scratchDir("c22") + "/fsql4";
    EngineOpts o;
    o.createMode = 2;
    REQUIRE(openEngine(root, o) == P4_OK, "open the migration target");
    TestType pnm = pnmLikeType("PNM");
    REQUIRE(registerType(pnm) == P4_OK, "register");
    CHECK_EQ(flatsql_p4_activate(), P4_OK, "activate");
    closeEngine();
    CHECK_EQ(openEngine(root, o), P4_E_FORMAT, "a valid STORE refuses create mode 2");
    std::vector<uint8_t> torn(64, 0xA5);
    REQUIRE(fp::ioWriteNew(root + "/STORE", torn.data(), torn.size()) == P4_OK, "tear STORE");
    REQUIRE(openEngine(root, o) == P4_OK, "a torn STORE with a valid MIGRATED reopens in mode 2");
    CHECK_EQ(flatsql_p4_activate(), P4_OK, "activate rewrites STORE");
    closeEngine();
    o.createMode = 0;
    CHECK_EQ(openEngine(root, o), P4_OK, "activated again");
    closeEngine();
}

// C-25: an empty BFBS (flags without verify-BFBS): no extraction, CID dedupe,
// served by CID, seq and tags.
P4_TEST(t_contract_empty_bfbs) {
    const std::string root = scratchDir("c25") + "/fsql4";
    REQUIRE(openEngine(root) == P4_OK, "open");
    TestType k;
    k.name = "KMF";
    std::memcpy(k.fid, "$KMF", 4);
    k.flags = 2;  // verify CID only
    REQUIRE(registerType(k) == P4_OK, "an empty BFBS registers");
    Batch b;
    b.type = "KMF";
    b.peer = "12D3KooWKeys";
    b.tags.push_back(Tag{"prov", "src", "", "b1", "", "", ""});
    b.at = 1790000000;
    std::vector<std::vector<uint8_t>> cids;
    for (int i = 0; i < 3; i++) {
        In in;
        std::vector<uint8_t> body = {8, 0, 0, 0, '$', 'K', 'M', 'F'};
        for (int j = 0; j < 40; j++) body.push_back(uint8_t(i * 7 + j));
        in.frame.resize(4);
        fp::st32(in.frame.data(), uint32_t(body.size()));
        in.frame.insert(in.frame.end(), body.begin(), body.end());
        in.ts = 1790000000 + i;
        cids.push_back(cidOfFrame(in.frame));
        b.recs.push_back(std::move(in));
    }
    Result r = put(b);
    REQUIRE(r.status == P4_OK && r.rows.size() == 3, r.err);
    for (size_t i = 0; i < 3; i++) CHECK_EQ(r.i(i, "action"), int64_t(P4_ACT_NEW), "new");
    Result again = put(b);
    REQUIRE(again.rows.size() == 3, again.err);
    CHECK_EQ(again.i(0, "action"), int64_t(P4_ACT_DUP), "CID dedupe");
    Result g = get("KMF", cids);
    CHECK_EQ(g.rows.size(), size_t(3), "GET by CID");
    CHECK(batchesOf("KMF", cids[1]) == std::set<std::string>{"b1"}, "TAGS");
    TlvW sc;
    sc.text(1, "KMF").u8(5, P4_ORDER_SEQ_ASC);
    CHECK_EQ(call(P4_OPC_SCAN, sc.b).rows.size(), size_t(3), "SCAN by seq");
    Result h = call(P4_OPC_HEAD, TlvW().text(1, "KMF").b);
    CHECK(h.rows.size() == 1 && h.i(0, "n") == 3, "HEAD");
    closeEngine();
}

// Design §7: a file whose free pages reach the share and floor is rewritten
// by VACUUM INTO its next generation (maintenance, here with a 1-byte floor),
// swapped in; the old generation goes; answers and verify are unchanged.
P4_TEST(t_file_rebuild) {
    const std::string root = scratchDir("rebuild-file") + "/fsql4";
    EngineOpts o;
    o.extra = TlvW().u32(45, 100).u64(46, 1).b;  // 10% free and 1 byte rebuild a file
    REQUIRE(openEngine(root, o) == P4_OK, "open");
    TestType pnm = pnmLikeType("PNM");
    REQUIRE(registerType(pnm) == P4_OK, "register");
    REQUIRE(put(pnmBatch(pnm, "12D3KooWA", "b1", 0, 3000)).status == P4_OK, "b1");
    REQUIRE(put(pnmBatch(pnm, "12D3KooWA", "b2", 2000, 1000)).status == P4_OK, "b2 retags 2000..2999");
    const std::string gen0 = root + "/P/PNM/1/0.0.db";
    REQUIRE(fp::ioExists(gen0), "generation 0");
    TlvW s;
    s.text(1, "PNM").text(11, "prov").text(12, "src").text(60, "b2").u8(61, 1);
    Result sup = call(P4_OPC_SUPERSEDE, s.b);
    REQUIRE(sup.status == P4_OK && sup.rows.size() == 1, sup.err);
    CHECK_EQ(sup.i(0, "records_deleted"), int64_t(2000), "2,000 rows deleted");
    const std::string gen1 = root + "/P/PNM/1/0.1.db";
    for (int i = 0; i < 1500 && !(fp::ioExists(gen1) && !fp::ioExists(gen0)); i++) flatsql::ps::sleepNs(10000000);
    CHECK(fp::ioExists(gen1), "rebuilt into generation 1");
    CHECK(!fp::ioExists(gen0), "generation 0 unlinked");
    TlvW sc;
    sc.text(1, "PNM").u8(5, P4_ORDER_SEQ_ASC);
    CHECK_EQ(call(P4_OPC_SCAN, sc.b).rows.size(), size_t(1000), "1,000 records after the rebuild");
    CHECK_EQ(get("PNM", {cidOfFrame(pnmFrame(pnm, 2500))}).rows.size(), size_t(1), "GET");
    Result v = rebuild(8);
    for (size_t i = 0; i < v.rows.size(); i++) CHECK_EQ(v.i(i, "mismatches"), int64_t(0), "verify");
    CHECK(fp::ioSize(gen1) < 3000 * 200, "the free pages are gone");
    REQUIRE(put(pnmBatch(pnm, "12D3KooWA", "b3", 5000, 10)).status == P4_OK, "writes go to the new generation");
    closeEngine();
    REQUIRE(openEngine(root, o) == P4_OK, "reopen");
    CHECK_EQ(call(P4_OPC_SCAN, sc.b).rows.size(), size_t(1010), "after reopen");
    v = rebuild(8);
    for (size_t i = 0; i < v.rows.size(); i++) CHECK_EQ(v.i(i, "mismatches"), int64_t(0), "verify after reopen");
    closeEngine();
}

#if !defined(__wasm__)
namespace {
// Runs SQL on a closed store's file with the stock VFS (test-side tampering).
int64_t sqlInt(const std::string& path, const std::string& sql) {
    sqlite3* db = nullptr;
    int64_t v = -1;
    if (sqlite3_open(path.c_str(), &db) == SQLITE_OK) {
        sqlite3_stmt* s = nullptr;
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &s, nullptr) == SQLITE_OK && sqlite3_step(s) == SQLITE_ROW)
            v = sqlite3_column_int64(s, 0);
        sqlite3_finalize(s);
    }
    sqlite3_close(db);
    return v;
}
}  // namespace

// C-27: REBUILD 8 runs integrity_check on every live file; a damaged index
// b-tree is a mismatch, named in the slot err, and nothing is changed.
P4_TEST(t_contract_integrity_check) {
    const std::string root = scratchDir("c27") + "/fsql4";
    REQUIRE(openEngine(root) == P4_OK, "open");
    TestType pnm = pnmLikeType("PNM");
    REQUIRE(registerType(pnm) == P4_OK, "register");
    REQUIRE(put(pnmBatch(pnm, "12D3KooWA", "b1", 0, 2000)).status == P4_OK, "put");
    Result v = rebuild(8, "PNM");
    REQUIRE(v.rows.size() == 1, v.err);
    CHECK_EQ(v.i(0, "mismatches"), int64_t(0), "intact");
    closeEngine();
    const std::string f = root + "/P/PNM/1/0.0.db";
    sqlInt(f, "PRAGMA wal_checkpoint(TRUNCATE)");
    const int64_t root_w = sqlInt(f, "SELECT rootpage FROM sqlite_schema WHERE name='r_w'");
    const int64_t ps = sqlInt(f, "PRAGMA page_size");
    REQUIRE(root_w > 1 && ps > 0, "the r_w index root");
    {
        // The root's cells (its child page numbers) sit at the page's end.
        std::fstream io(f, std::ios::in | std::ios::out | std::ios::binary);
        io.seekp((root_w - 1) * ps + ps - 512);
        std::vector<char> junk(512, char(0x5A));
        io.write(junk.data(), std::streamsize(junk.size()));
    }
    REQUIRE(openEngine(root) == P4_OK, "reopen");
    v = rebuild(8, "PNM");
    REQUIRE(v.status == P4_OK && v.rows.size() == 1, v.err);
    CHECK(v.i(0, "mismatches") >= 1, "the damaged file is a mismatch");
    CHECK(v.err.find("integrity_check failed: ") == 0 && v.err.find("0.0.db") != std::string::npos, "named: " + v.err);
    closeEngine();
}

// A crash after a file was created (empty, WAL mode) and its J_FILE
// journaled, before its schema committed: the reopen completes the schema
// instead of trusting the file (the migrate kill -9 landing blocker).
P4_TEST(t_replay_torn_create) {
    const std::string root = scratchDir("torn-create") + "/fsql4";
    REQUIRE(openEngine(root) == P4_OK, "open");
    TestType t = pnmLikeType("PNM");
    t.rules += "bucket str:NAME\n";
    auto frame = [&](uint64_t id, const char* month) {
        return buildFrame(t, {Field::str("FILE_ID", "f" + std::to_string(id)), Field::str("NAME", month),
                              Field::raw("BODY", std::vector<uint8_t>(64, uint8_t(id)))});
    };
    REQUIRE(registerType(t) == P4_OK, "register");
    Batch b;
    b.type = "PNM";
    b.peer = "12D3KooWA";
    b.tags.push_back(Tag{"prov", "src", "", "b1", "", "", ""});
    b.at = 1790000000;
    for (int i = 0; i < 10; i++) {
        In in;
        in.frame = frame(uint64_t(i), "2026-06-15T00:00:00");
        in.ts = 1790000000;
        b.recs.push_back(std::move(in));
    }
    REQUIRE(put(b).status == P4_OK, "June");
    closeEngine();
    // The torn state: July's file exists in WAL mode without tables, and the
    // journal holds its J_FILE.
    const std::string july = root + "/P/PNM/1/202607.0.db";
    sqlInt(july, "PRAGMA journal_mode=WAL");
    CHECK_EQ(sqlInt(july, "SELECT count(*) FROM sqlite_schema"), int64_t(0), "no tables");
    sqlInt(root + "/T/PNM.jnl",
           "INSERT INTO j(op, tb, k, c, pid, seq, gen, s, v) VALUES(4, 202607, NULL, NULL, 1, 0, 0, NULL, 0) RETURNING id");
    REQUIRE(openEngine(root) == P4_OK, "reopen");
    Batch jb = b;
    for (int i = 0; i < 10; i++) jb.recs[size_t(i)].frame = frame(uint64_t(100 + i), "2026-07-15T00:00:00");
    Result r = put(jb);
    CHECK_EQ(r.status, P4_OK, "July accepts writes: " + r.err);
    CHECK_EQ(get("PNM", {cidOfFrame(jb.recs[3].frame)}).rows.size(), size_t(1), "readable");
    Result v = rebuild(8, "PNM");
    REQUIRE(v.rows.size() == 1, v.err);
    CHECK_EQ(v.i(0, "mismatches"), int64_t(0), "verify");
    closeEngine();
}
#endif

namespace {
struct CursorRun {
    int32_t status = P4_OK;
    std::vector<int64_t> seqs;
    std::vector<int64_t> keys;
    uint64_t examined = 0;
};
// The reader API (p4_reader.h) on a lane of the test's own, as p4sql calls it.
CursorRun cursorRun(const char* type, uint64_t bound, uint8_t order, const P4Pred* preds, uint32_t n) {
    CursorRun out;
    P4Lane lane;
    lane.e = fp::currentEngine();
    P4ScanSpec spec{};
    spec.type = type;
    spec.preds = preds;
    spec.nPreds = n;
    spec.order = order;
    spec.bound = bound;
    P4Cursor* c = nullptr;
    out.status = p4_cursor_open(&lane, &spec, &c);
    if (out.status != P4_OK) return out;
    P4Row row;
    int32_t k;
    while ((k = p4_cursor_next(c, &row)) == 1) {
        out.seqs.push_back(row.seq);
        out.keys.push_back(row.keyType == 1 ? row.keyInt : -1);
    }
    if (k < 0) out.status = k;
    p4_cursor_close(c);
    uint64_t bytes = 0;
    p4_lane_counters(&lane, &out.examined, &bytes);
    return out;
}
}  // namespace

// R17/R19: an object-column predicate inside the A18 bound reads its
// objects' rows through the object index, not the window, with the same
// answer as the rule (newest N of the type, then the predicate).
P4_TEST(t_reader_object_key_in_bound) {
    const std::string root = scratchDir("objkey") + "/fsql4";
    REQUIRE(openEngine(root) == P4_OK, "open");
    REQUIRE(registerType(ommType()) == P4_OK, "register OMM");
    REQUIRE(registerType(catType()) == P4_OK, "register CAT");
    Batch b;
    b.type = "OMM";
    b.peer = "12D3KooWGp";
    b.tags.push_back(Tag{"celestrak", "celestrak-gp", "", "b1", "", "", ""});
    b.at = 1790000000;
    std::vector<uint32_t> noradOf;
    for (int i = 0; i < 3000; i++) {
        In in;
        const uint32_t norad = uint32_t(100 + i % 50);
        in.frame = ommFrame(norad, "1998-067A", isoTime(unixOf(2026, 9, 1) + int64_t(i / 50) * 7200 + i % 50));
        in.ts = 1790000000;
        noradOf.push_back(norad);
        b.recs.push_back(std::move(in));
    }
    Result r = put(b);
    REQUIRE(r.status == P4_OK && r.rows.size() == 3000, r.err);
    std::vector<int64_t> seqOf;
    for (size_t i = 0; i < r.rows.size(); i++) seqOf.push_back(r.i(i, "seq"));
    std::vector<int64_t> sorted = seqOf;
    std::sort(sorted.begin(), sorted.end());
    const uint64_t bound = 1000;
    const int64_t floor = sorted[sorted.size() - bound];
    auto expect = [&](uint32_t lo, uint32_t hi) {
        std::vector<int64_t> v;
        for (size_t i = 0; i < seqOf.size(); i++)
            if (seqOf[i] >= floor && noradOf[i] >= lo && noradOf[i] <= hi) v.push_back(seqOf[i]);
        std::sort(v.rbegin(), v.rend());
        return v;
    };
    P4Value v7{1, 107, 0, nullptr, 0};
    P4Pred eq{P4_F_COL0, P4_OP_EQ, 1, &v7};
    CursorRun a = cursorRun("OMM", bound, P4_ORDER_SEQ_DESC, &eq, 1);
    CHECK_EQ(a.status, P4_OK, "eq");
    CHECK(a.seqs == expect(107, 107), "COL0 = 107 inside the bound: " + std::to_string(a.seqs.size()) + " rows");
    CHECK(a.examined <= 2 * a.seqs.size() + 2, "examined only the object's rows: " + std::to_string(a.examined));
    P4Value range[2] = {{1, 105, 0, nullptr, 0}, {1, 109, 0, nullptr, 0}};
    P4Pred btw{P4_F_COL0, P4_OP_BETWEEN, 2, range};
    CursorRun bt = cursorRun("OMM", bound, P4_ORDER_SEQ_DESC, &btw, 1);
    CHECK(bt.status == P4_OK && bt.seqs == expect(105, 109), "BETWEEN 105 AND 109: " + std::to_string(bt.seqs.size()));
    CursorRun w = cursorRun("OMM", bound, P4_ORDER_W_DESC, &eq, 1);
    CHECK_EQ(w.seqs.size(), expect(107, 107).size(), "W order: the same rows");
    CursorRun none = cursorRun("OMM", 0, P4_ORDER_SEQ_ASC, &eq, 1);
    CHECK_EQ(none.seqs.size(), size_t(60), "no bound: every row of the object");
    // An object type without an epoch (r_k).
    Batch cb;
    cb.type = "CAT";
    cb.peer = "12D3KooWSatcat";
    cb.tags.push_back(Tag{"celestrak", "celestrak-satcat", "", "c1", "", "", ""});
    cb.at = 1790000000;
    for (int i = 0; i < 500; i++) {
        In in;
        in.frame = catFrame(uint32_t(1000 + i), "2000-001A", "OBJ " + std::to_string(i));
        in.ts = 1790000000;
        cb.recs.push_back(std::move(in));
    }
    REQUIRE(put(cb).status == P4_OK, "CAT");
    P4Value c9{1, 1009, 0, nullptr, 0};
    P4Pred ceq{P4_F_COL0, P4_OP_EQ, 1, &c9};
    CursorRun cr = cursorRun("CAT", 10000, P4_ORDER_SEQ_DESC, &ceq, 1);
    CHECK(cr.status == P4_OK && cr.seqs.size() == 1 && cr.keys[0] == 1009, "CAT COL0 = 1009");
    closeEngine();
}
