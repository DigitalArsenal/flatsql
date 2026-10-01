// The SQL surface over the real engine (CONTRACT.md §3.5 ops 30/31, §3.9):
// records PUT through the mailbox, then op 30 (SQL) and op 31 (SURFACE)
// answered by p4sql on the engine's lanes, cursors from the engine's reader.
// Built into the engine's test binaries (native and wasm, the engine's CMake
// globs this directory).

#include "flatsql/p4/p4_reader.h"
#include "p4/p4_test.h"

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

}  // namespace

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

