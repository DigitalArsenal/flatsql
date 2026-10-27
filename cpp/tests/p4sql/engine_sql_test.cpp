#include "QRP/main_generated.h"
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
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<QRPValue>> values;
    for (const auto& c : params) {
        flatbuffers::Offset<QRPValue> v;
        switch (c.type) {
            case flatsql::ps::rb1::kInt: v = CreateQRPValue(builder, qrpValueKind_Int, false, c.i); break;
            case flatsql::ps::rb1::kReal: v = CreateQRPValue(builder, qrpValueKind_Float, false, 0, c.d); break;
            case flatsql::ps::rb1::kText: v = CreateQRPValue(builder, qrpValueKind_Text, false, 0, 0, builder.CreateString(c.s)); break;
            case flatsql::ps::rb1::kBlob: v = CreateQRPValue(builder, qrpValueKind_Bytes, false, 0, 0, 0,
                builder.CreateVector(reinterpret_cast<const uint8_t*>(c.s.data()), c.s.size())); break;
            default: v = CreateQRPValue(builder); break;
        }
        values.push_back(v);
    }
    auto text = builder.CreateString(sql);
    auto bindings = builder.CreateVector(values);
    QRPBuilder q(builder);
    q.add_SQL(text);
    q.add_PARAMETERS(bindings);
    q.add_REPRESENTATION(qrpRepresentation_FlatBuffer);
    FinishSizePrefixedQRPBuffer(builder, q.Finish());
    std::vector<uint8_t> bytes(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
    return bytes;
}

Result sql(const std::string& q, uint32_t flags = 0, const std::vector<Cell>& params = {}) {
    CallOpts o;
    o.flags = flags;
    return call(FLATSQL_OPC_SQL, sqlReq(q, params), o);
}

Cell intCell(int64_t v) {
    Cell c;
    c.type = flatsql::ps::rb1::kInt;
    c.i = v;
    return c;
}

}  // namespace

// Statuses, never traps: result caps, the sandbox, malformed parameters.
FLATSQL_TEST(t_sql_caps_and_sandbox) {
    const std::string root = scratchDir("t_sql_caps");
    REQUIRE(openEngine(root) == FLATSQL_OK, "open");
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register");
    REQUIRE(put(ommBatch(1, 20, kSrc, "b1")).status == FLATSQL_OK, "put");
    CallOpts o;
    o.maxResultRows = 5;
    Result r = call(FLATSQL_OPC_SQL, sqlReq("SELECT _seq FROM OMM"), o);
    CHECK_EQ(r.status, FLATSQL_E_BUDGET, r.err);
    r = sql("PRAGMA table_info(OMM)", FLATSQL_SLOT_SANDBOX);
    CHECK_EQ(r.status, FLATSQL_E_SQL, r.err);
    r = sql("SELECT 1; SELECT 2", FLATSQL_SLOT_SANDBOX);
    CHECK_EQ(r.status, FLATSQL_E_SQL, r.err);
    r = sql("SELECT length(randomblob(100000000))", FLATSQL_SLOT_SANDBOX);   // past the lane heap cap
    CHECK_EQ(r.status, FLATSQL_E_BUDGET, r.err);
    r = sql("SELECT ?1 + ?2", 0, {intCell(1)});
    CHECK_EQ(r.status, FLATSQL_E_SQL, r.err);
    r = sql("SELECT COUNT(*) FROM OMM");   // the lane serves on
    CHECK(r.status == FLATSQL_OK && r.rows.size() == 1 && r.rows[0][0].i == 20, r.err);
    closeEngine();
    removeTree(root);
}

// SURFACE: the relation per type and per source, format 1's columns.
FLATSQL_TEST(t_sql_surface) {
    const std::string root = scratchDir("t_sql_surface");
    REQUIRE(openEngine(root) == FLATSQL_OK, "open");
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register");
    REQUIRE(put(ommBatch(1, 3, kSrc, "b1")).status == FLATSQL_OK, "put");
    const Result r = call(FLATSQL_OPC_SURFACE, {});
    CHECK_EQ(r.status, FLATSQL_OK, r.err);
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

