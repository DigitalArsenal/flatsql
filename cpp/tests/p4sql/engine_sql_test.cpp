// The SQL surface over the real engine (CONTRACT.md §3.5 ops 30/31, §3.9):
// records PUT through the mailbox, then op 30 (SQL) and op 31 (SURFACE)
// answered by p4sql on the engine's lanes, cursors from the engine's reader.
// Built into the engine's test binaries (native and wasm, the engine's CMake
// globs this directory); the fake reader's suite (FLATSQL_P4SQL_FAKE) is the
// other build of this directory and leaves this file out.
#ifndef FLATSQL_P4SQL_FAKE

#include <cstring>

#include "internal.h"   // the engine's (cpp/src/p4): the slot ring, for raw streams
#include "p4/p4_test.h"

namespace flatsql {
namespace p4 {
P4Engine* currentEngine();   // the test client's engine (cpp/tests/p4/p4_test_main.cpp)
}  // namespace p4
}  // namespace flatsql

using namespace p4t;

namespace {

const char* kSrc = "celestrak-gp";

Batch ommBatch(int from, int n, const std::string& source, const std::string& batch) {
    Batch b;
    b.type = "OMM";
    b.peer = "12D3KooWExample";
    b.tags.push_back(Tag{"space-data-network-02", source, "", batch, "", "", ""});
    b.at = 1790000000;
    for (int i = from; i < from + n; i++) {
        In r;
        r.frame = ommFrame(uint32_t(25000 + i), "1998-0" + std::to_string(i) + "A", isoTime(1789000000 + i * 3600));
        r.ts = 1790000000 + i;
        b.recs.push_back(std::move(r));
    }
    return b;
}

Batch catBatch(int from, int n, const std::string& source) {
    Batch b;
    b.type = "CAT";
    b.peer = "12D3KooWExample";
    b.tags.push_back(Tag{"space-data-network-02", source, "", source + "-b1", "", "", ""});
    b.at = 1790000000;
    for (int i = from; i < from + n; i++) {
        In r;
        r.frame = catFrame(uint32_t(40000 + i), "2000-0" + std::to_string(i) + "A", "CAT " + std::to_string(i));
        r.ts = 1790000000 + i;
        b.recs.push_back(std::move(r));
    }
    return b;
}

std::vector<uint8_t> sqlReq(const std::string& sql, const std::vector<Cell>& params = {}) {
    TlvW w;
    w.text(70, sql);
    if (!params.empty()) {
        std::vector<uint8_t> pb;
        flatsql::ps::rb1::encodeParams(params, &pb);
        w.raw(71, pb.data(), pb.size());
    }
    return w.b;
}

Result sql(const std::string& q, uint32_t flags = 0, const std::vector<Cell>& params = {}) {
    CallOpts o;
    o.flags = flags;
    return call(P4_OPC_SQL, sqlReq(q, params), o);
}

Cell intCell(int64_t v) {
    Cell c;
    c.type = flatsql::ps::rb1::kInt;
    c.i = v;
    return c;
}

// A raw stream (flag RAW): the slot's ring bytes, read as wait() reads RB1.
int32_t raw(const std::string& q, const std::vector<Cell>& params, std::vector<uint8_t>* out) {
    CallOpts o;
    o.flags = P4_SLOT_RAW | P4_SLOT_SANDBOX;
    const uint32_t slot = submit(P4_OPC_SQL, sqlReq(q, params), o);
    if (slot == UINT32_MAX) return -995;
    flatsql::p4::Engine* e = flatsql::p4::currentEngine();
    flatsql::p4::SlotHeader* h = e->slot(slot);
    const uint8_t* ring = e->slotRing(slot);
    const uint64_t cap = e->ringBytes[e->poolOf(slot)];
    for (;;) {
        const uint32_t seq = h->outSeq.load(std::memory_order_acquire);
        const uint32_t state = h->state.load(std::memory_order_acquire);
        const uint64_t tail = h->ringTail.load(std::memory_order_acquire);
        const uint64_t head = h->ringHead.load(std::memory_order_relaxed);
        if (tail > head) {
            for (uint64_t p = head; p < tail; p++) out->push_back(ring[p & (cap - 1)]);
            h->ringHead.store(tail, std::memory_order_release);
            h->spaceSeq.fetch_add(1);
            const uint32_t th = h->thread;
            if (th < e->nThreads) {
                e->bells[th].doorbell.fetch_add(1);
                flatsql_p4_wake(reinterpret_cast<uint32_t*>(&e->bells[th].doorbell), 1);
            }
            continue;
        }
        if (state == P4_SLOT_DONE) break;
        flatsql::ps::waitU32(&h->outSeq, seq, 10ull * 1000 * 1000);
    }
    const int32_t st = h->status;
    h->state.store(P4_SLOT_FREE, std::memory_order_release);
    return st;
}

}  // namespace

// Relations, columns, pushdown and A18 on engine-stored records.
P4_TEST(t_sql_relations_over_the_engine) {
    const std::string root = scratchDir("t_sql_relations");
    REQUIRE(openEngine(root) == P4_OK, "open");
    REQUIRE(registerType(ommType()) == P4_OK, "register OMM");
    TestType cat = catType();
    cat.a18 = 10;   // a small A18 bound: the window is the newest 10 of the type
    REQUIRE(registerType(cat) == P4_OK, "register CAT");
    const Batch ob = ommBatch(1, 30, kSrc, "b1");
    Result p = put(ob);
    REQUIRE(p.status == P4_OK && p.rows.size() == 30, p.err);

    Result r = sql("SELECT COUNT(*) FROM OMM");
    CHECK_EQ(r.status, P4_OK, r.err);
    CHECK(r.rows.size() == 1 && r.rows[0][0].i == 30, "count");
    r = sql("SELECT _seq, _cid, NORAD_CAT_ID, OBJECT_ID, _source FROM \"OMM@celestrak-gp\" WHERE NORAD_CAT_ID = 25007");
    CHECK_EQ(r.status, P4_OK, r.err);
    REQUIRE(r.rows.size() == 1, "one object");
    uint8_t cid[36];
    cidOf(ob.recs[6].frame, cid);
    CHECK(r.s(0, "_cid") == cidText(cid), "_cid");
    CHECK_EQ(r.i(0, "_seq"), p.i(6, "seq"), "_seq is the PUT's seq");
    CHECK(r.s(0, "OBJECT_ID") == "1998-07A", "OBJECT_ID");
    CHECK(r.s(0, "_source") == "OMM@celestrak-gp", "_source");
    // Never past the window; how far below it the reader's COL0 seek goes is
    // the engine's (reported).
    CHECK(r.rowsExamined <= 30, "COL0 equality: within the window");
    report("t_sql_col0_eq_rows_examined", double(r.rowsExamined), "rows (of a 30-record window)");

    // _data: the record without its size prefix (format 1's payload).
    std::vector<uint8_t> frames;
    CHECK_EQ(raw("SELECT _data FROM OMM WHERE NORAD_CAT_ID = ?1", {intCell(25007)}, &frames), P4_OK, "raw");
    const std::vector<uint8_t>& f = ob.recs[6].frame;
    std::vector<uint8_t> want(4);
    const uint32_t n = uint32_t(f.size() - 4);
    std::memcpy(want.data(), &n, 4);
    want.insert(want.end(), f.begin() + 4, f.end());
    CHECK(frames == want, "one frame, the record");

    // A18: the type's newest 10; the older source's relation is empty.
    REQUIRE(put(catBatch(1, 15, "celestrak-satcat-csv")).status == P4_OK, "csv");
    REQUIRE(put(catBatch(16, 15, "celestrak-satcat")).status == P4_OK, "satcat");
    r = sql("SELECT COUNT(*) FROM \"CAT@celestrak-satcat-csv\"", P4_SLOT_SANDBOX);
    CHECK_EQ(r.status, P4_OK, r.err);
    CHECK(r.rows.size() == 1 && r.rows[0][0].i == 0, "csv is outside the window");
    CHECK(r.rowsExamined <= 10, "never past the bound");
    r = sql("SELECT _source, COUNT(*) FROM CAT GROUP BY _source", P4_SLOT_SANDBOX);
    CHECK_EQ(r.status, P4_OK, r.err);
    CHECK(r.rows.size() == 1 && r.rows[0][0].s == "CAT@celestrak-satcat" && r.rows[0][1].i == 10, "one source, 10");
    r = sql("SELECT _seq FROM CAT ORDER BY _seq DESC LIMIT 3");
    CHECK(r.rows.size() == 3 && r.rows[0][0].i > r.rows[1][0].i && r.rows[1][0].i > r.rows[2][0].i, "descending");
    closeEngine();
    removeTree(root);
}

// Statuses, never traps: result caps, the sandbox, malformed parameters.
P4_TEST(t_sql_caps_and_sandbox) {
    const std::string root = scratchDir("t_sql_caps");
    REQUIRE(openEngine(root) == P4_OK, "open");
    REQUIRE(registerType(ommType()) == P4_OK, "register");
    REQUIRE(put(ommBatch(1, 20, kSrc, "b1")).status == P4_OK, "put");
    CallOpts o;
    o.maxResultRows = 5;
    Result r = call(P4_OPC_SQL, sqlReq("SELECT _seq FROM OMM"), o);
    CHECK_EQ(r.status, P4_E_BUDGET, r.err);
    r = sql("PRAGMA table_info(OMM)", P4_SLOT_SANDBOX);
    CHECK_EQ(r.status, P4_E_SQL, r.err);
    r = sql("SELECT 1; SELECT 2", P4_SLOT_SANDBOX);
    CHECK_EQ(r.status, P4_E_SQL, r.err);
    r = sql("SELECT length(randomblob(100000000))", P4_SLOT_SANDBOX);   // past the lane heap cap
    CHECK_EQ(r.status, P4_E_BUDGET, r.err);
    r = sql("SELECT ?1 + ?2", 0, {intCell(1)});
    CHECK_EQ(r.status, P4_E_SQL, r.err);
    r = sql("SELECT COUNT(*) FROM OMM");   // the lane serves on
    CHECK(r.status == P4_OK && r.rows.size() == 1 && r.rows[0][0].i == 20, r.err);
    closeEngine();
    removeTree(root);
}

// SURFACE: the relation per type and per source, format 1's columns.
P4_TEST(t_sql_surface) {
    const std::string root = scratchDir("t_sql_surface");
    REQUIRE(openEngine(root) == P4_OK, "open");
    REQUIRE(registerType(ommType()) == P4_OK, "register");
    REQUIRE(put(ommBatch(1, 3, kSrc, "b1")).status == P4_OK, "put");
    const Result r = call(P4_OPC_SURFACE, {});
    CHECK_EQ(r.status, P4_OK, r.err);
    size_t base = 0, alias = 0;
    for (size_t i = 0; i < r.rows.size(); i++) {
        if (r.s(i, "name") == "OMM") {
            base++;
            CHECK(r.s(i, "kind") == "view", "OMM is a view");
            CHECK_EQ(r.i(i, "bound"), int64_t(400000), "bound");
        }
        if (r.s(i, "name") == "OMM@celestrak-gp") {
            alias++;
            CHECK(r.s(i, "kind") == "table" && r.s(i, "source") == kSrc, "per-source table");
        }
    }
    // The test schema's 9 fields, then _source, _rowid, _offset, _data.
    CHECK_EQ(base, size_t(13), "OMM columns");
    CHECK_EQ(alias, size_t(13), "OMM@celestrak-gp columns");
    closeEngine();
    removeTree(root);
}

#endif  // FLATSQL_P4SQL_FAKE
