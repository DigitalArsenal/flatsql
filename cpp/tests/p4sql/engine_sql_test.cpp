#include "QRP/main_generated.h"
// The SQL surface over the real engine (CONTRACT.md §3.5 ops 30/31, §3.9):
// records PUT through the mailbox, then op 30 (SQL) and op 31 (SURFACE)
// answered by p4sql on the engine's lanes, cursors from the engine's reader.
// Built into the engine's test binaries (native and wasm, the engine's CMake
// globs this directory).

#include <algorithm>
#include <chrono>
#include <cmath>
#include <set>

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

std::string joined(const std::vector<int64_t>& ids, const char* sep, const char* each = "") {
    std::string s;
    for (size_t i = 0; i < ids.size(); i++) {
        if (i) s += sep;
        s += each;
        s += std::to_string(ids[i]);
    }
    return s;
}

// `_rowid = a OR _rowid = b ...` as a balanced tree: SQLite refuses a
// left-deep chain past its expression depth (1,000), whatever the relation.
std::string orTree(const std::vector<int64_t>& ids, size_t lo, size_t hi) {
    if (hi - lo == 1) return "_rowid = " + std::to_string(ids[lo]);
    const size_t mid = lo + (hi - lo) / 2;
    return "(" + orTree(ids, lo, mid) + " OR " + orTree(ids, mid, hi) + ")";
}

// Closes the engine and removes its root however the test returns: a REQUIRE
// returns early, and an engine left open fails every later test's open.
struct OpenedEngine {
    std::string root;
    ~OpenedEngine() {
        closeEngine();
        removeTree(root);
    }
};

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

// Rowid lookups spread over a large type (stack issue 676). SQLite answers
// `_rowid IN (...)`, and an OR of `_rowid =` terms, with one xFilter, so one
// reader cursor, per value. Each cursor's A18 cut walked the type's seqs from
// its rowid down to the bound N, below the range it reads, and counted every
// seq walked as a row examined: 16 rowids spread over 150,000 records walked
// over 1,000,000 seqs and tripped the sandbox's rows-examined cap ("reader
// cursor failed"), and 7 near the newest end did too.
FLATSQL_TEST(t_sql_scattered_rowid_lookups) {
    const std::string root = scratchDir("t_sql_rowid_lookups");
    EngineOpts eo;
    TlvW req;
    req.u32(10, 1u << 20);   // read requests up to 1 MiB (the JS host gives 8 MiB): a 10,000-value IN list
    eo.extra = req.b;
    REQUIRE(openEngine(root, eo) == FLATSQL_OK, "open");
    const OpenedEngine opened{root};
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register");   // A18 bound 400,000: every record is in the window
    const int kRecords = 150000, kBatch = 5000;
    for (int from = 1; from <= kRecords; from += kBatch)
        REQUIRE(put(ommBatch(from, kBatch, kSrc, "b" + std::to_string(from))).status == FLATSQL_OK, "put");
    // Every record's rowid and NORAD id, in 10,000-row pages.
    std::vector<int64_t> rowids;
    std::map<int64_t, int64_t> norad;
    for (int64_t after = 0;;) {
        const Result r = sql("SELECT _rowid AS RID, NORAD_CAT_ID AS N FROM OMM WHERE _rowid > ?1 ORDER BY _rowid LIMIT 10000",
                             FLATSQL_SLOT_SANDBOX, {intCell(after)});
        REQUIRE(r.status == FLATSQL_OK, r.err);
        if (r.rows.empty()) break;
        for (size_t i = 0; i < r.rows.size(); i++) {
            rowids.push_back(r.i(i, "RID"));
            norad[r.i(i, "RID")] = r.i(i, "N");
        }
        after = rowids.back();
    }
    REQUIRE(rowids.size() == size_t(kRecords), "every record has a rowid");
    auto at = [&](std::initializer_list<size_t> positions) {
        std::vector<int64_t> v;
        for (size_t p : positions) v.push_back(rowids[p]);
        return v;
    };
    // n rowids spread over the whole type, out of order (a stride prime to the count).
    auto spread = [&](size_t n) {
        std::vector<int64_t> v;
        for (size_t j = 0; j < n; j++) v.push_back(rowids[(j * 7919 + 16 * 7919) % rowids.size()]);
        return v;
    };
    auto lookup = [&](const std::string& what, const std::string& where, const std::vector<int64_t>& want,
                      const std::string& from = "OMM") {
        const auto t0 = std::chrono::steady_clock::now();
        const Result r = sql("SELECT _rowid AS RID, NORAD_CAT_ID AS N FROM " + from + " WHERE " + where, FLATSQL_SLOT_SANDBOX);
        if (want.size() >= 10000) {
            report(("rowid lookups." + what + ".ms").c_str(),
                   std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count(), "ms");
            report(("rowid lookups." + what + ".rows_examined").c_str(), double(r.rowsExamined), "rows");
        }
        CHECK_EQ(r.status, FLATSQL_OK, what + ": " + r.err);
        CHECK_EQ(r.rows.size(), want.size(), what);
        std::set<int64_t> got;
        bool own = true;
        for (size_t i = 0; i < r.rows.size(); i++) {
            got.insert(r.i(i, "RID"));
            own = own && norad[r.i(i, "RID")] == r.i(i, "N");
        }
        CHECK(got == std::set<int64_t>(want.begin(), want.end()), what + ": the rows asked for");
        CHECK(own, what + ": each row from its own record");
        // A lookup examines its own record, not the window below it.
        CHECK(r.rowsExamined <= 4 * want.size(), what + ": " + std::to_string(r.rowsExamined) + " rows examined");
    };
    // The issue's rowids: 15 answered, the 16th (in any order) failed.
    std::vector<int64_t> issue;
    for (size_t i = 0; i < 16; i++) issue.push_back(rowids[((16 + i) * 7919) % 147000]);
    lookup("the issue's 15", "_rowid IN (" + joined({issue.begin(), issue.end() - 1}, ",") + ")",
           {issue.begin(), issue.end() - 1});
    std::vector<int64_t> reversed(issue.rbegin(), issue.rend());
    lookup("the issue's 16", "_rowid IN (" + joined(reversed, ",") + ")", issue);
    // Not a count: 7 rowids among the newest failed the same way.
    const std::vector<int64_t> newest = at({149999, 149000, 148000, 147000, 146000, 145000, 144000});
    lookup("7 newest", "_rowid IN (" + joined(newest, ",") + ")", newest);
    for (size_t n : {size_t(16), size_t(64), size_t(1024), size_t(10000)}) {
        const std::vector<int64_t> ids = spread(n);
        lookup(std::to_string(n) + " IN", "_rowid IN (" + joined(ids, ",") + ")", ids);
        lookup(std::to_string(n) + " OR", orTree(ids, 0, ids.size()), ids);
        if (n <= 64) lookup(std::to_string(n) + " OR chain", joined(ids, " OR ", "_rowid = "), ids);
    }
    // The per-source relation reads the same cursors (its lane filter's files).
    const std::vector<int64_t> some = spread(1024);
    lookup("1024 IN, per source", "_rowid IN (" + joined(some, ",") + ")", some, "\"OMM@" + std::string(kSrc) + "\"");
    // The store serves on: a count, a contiguous list, a write and its read-back.
    Result r = sql("SELECT COUNT(*) FROM OMM", FLATSQL_SLOT_SANDBOX);
    CHECK(r.status == FLATSQL_OK && r.rows.size() == 1 && r.rows[0][0].i == kRecords, r.err);
    lookup("256 contiguous", "_rowid IN (" + joined({rowids.begin(), rowids.begin() + 256}, ",") + ")",
           {rowids.begin(), rowids.begin() + 256});
    REQUIRE(put(ommBatch(kRecords + 1, 16, kSrc, "after")).status == FLATSQL_OK, "put after");
    r = sql("SELECT _rowid AS RID FROM OMM WHERE _rowid > ?1 ORDER BY _rowid", FLATSQL_SLOT_SANDBOX, {intCell(rowids.back())});
    CHECK(r.status == FLATSQL_OK && r.rows.size() == 16, r.err);
    std::vector<int64_t> fresh;
    for (size_t i = 0; i < r.rows.size(); i++) fresh.push_back(r.i(i, "RID"));
    std::vector<int64_t> mixed = fresh;
    for (int64_t id : spread(16)) mixed.push_back(id);
    for (size_t i = 0; i < r.rows.size(); i++) norad[fresh[i]] = kRecords + 1 + int64_t(i) + 25000;
    lookup("16 new and 16 old", "_rowid IN (" + joined(mixed, ",") + ")", mixed);
}

// The A18 window of a range read stays what it was: the newest N records at
// or below the range's top, then the range (a point lookup always finds its
// record). The lookups' fix only stops the cut's walk at the range's floor.
FLATSQL_TEST(t_sql_window_cut_ranges) {
    const std::string root = scratchDir("t_sql_window_cut");
    REQUIRE(openEngine(root) == FLATSQL_OK, "open");
    const OpenedEngine opened{root};
    TestType small = ommType();
    small.a18 = 100;
    REQUIRE(registerType(small) == FLATSQL_OK, "register");
    REQUIRE(put(ommBatch(1, 1000, kSrc, "b1")).status == FLATSQL_OK, "put");
    const Result first = sql("SELECT _rowid AS RID FROM OMM ORDER BY _rowid");
    REQUIRE(first.status == FLATSQL_OK && first.rows.size() == 100, first.err);   // the newest 100
    const int64_t top = first.i(99, "RID"), base = top - 999;   // rowids base..top, one per record
    CHECK_EQ(first.i(0, "RID"), top - 99, "the window is the newest 100");
    auto count = [&](const std::string& where) {
        const Result r = sql("SELECT COUNT(_rowid) AS C, MIN(_rowid) AS LO, MAX(_rowid) AS HI FROM OMM WHERE " + where);
        return r.status == FLATSQL_OK && r.rows.size() == 1
                   ? std::vector<int64_t>{r.i(0, "C"), r.i(0, "LO"), r.i(0, "HI")}
                   : std::vector<int64_t>{-1, -1, -1};
    };
    auto expect = [&](const std::string& where, int64_t n, int64_t lo, int64_t hi) {
        const std::vector<int64_t> got = count(where);
        CHECK(got == (std::vector<int64_t>{n, lo, hi}),
              where + ": " + std::to_string(got[0]) + " rows " + std::to_string(got[1]) + ".." + std::to_string(got[2]));
    };
    const std::string b = std::to_string(base);
    auto at = [&](int64_t k) { return std::to_string(base + k); };   // the (k+1)-th record's rowid
    expect("_rowid <= " + at(499), 100, base + 400, base + 499);                              // newest 100 at or below
    expect("_rowid > " + at(299) + " AND _rowid <= " + at(499), 100, base + 400, base + 499);  // 200 in range, cut to 100
    expect("_rowid BETWEEN " + at(449) + " AND " + at(499), 51, base + 449, base + 499);      // fewer than 100 in range
    expect("_rowid BETWEEN " + at(400) + " AND " + at(499), 100, base + 400, base + 499);     // exactly 100
    expect("_rowid BETWEEN " + at(399) + " AND " + at(499), 100, base + 400, base + 499);     // 101: the oldest is cut
    expect("_rowid IN (" + b + ", " + at(499) + ", " + at(999) + ")", 3, base, base + 999);    // each lookup finds its record
    expect("_rowid > " + at(899), 100, base + 900, base + 999);
    expect("_rowid > " + at(949), 50, base + 950, base + 999);
    expect("_rowid >= " + b + " AND _rowid <= " + at(0), 1, base, base);   // the oldest record, by its own range
}

// SQLite's built-in math functions (SQLITE_ENABLE_MATH_FUNCTIONS; stack
// issue 676: "no such function: pow"), in the sandbox and over a relation's
// columns.
FLATSQL_TEST(t_sql_math_functions) {
    const std::string root = scratchDir("t_sql_math");
    REQUIRE(openEngine(root) == FLATSQL_OK, "open");
    const OpenedEngine opened{root};
    REQUIRE(registerType(ommType()) == FLATSQL_OK, "register");
    REQUIRE(put(ommBatch(1, 3, kSrc, "b1")).status == FLATSQL_OK, "put");
    auto real = [](const Result& r, size_t row, const std::string& c) {
        const int k = r.col(c);
        return k < 0 || row >= r.rows.size() || r.rows[row][size_t(k)].type != flatsql::ps::rb1::kReal
                   ? std::nan("")
                   : r.rows[row][size_t(k)].d;
    };
    auto near = [](double a, double b) { return std::fabs(a - b) <= 1e-12 * std::max(1.0, std::fabs(b)); };
    const Result r = sql("SELECT pow(2, 10) AS P, power(1.5, 2) AS PW, sqrt(2.25) AS Q, sin(0.5) AS S, cos(0.5) AS C, "
                         "atan2(1, 2) AS A, ln(10) AS L, log10(1000) AS G, log2(8) AS L2, exp(1) AS E, pi() AS PI, "
                         "degrees(pi()) AS D, radians(180) AS R, mod(7.5, 2) AS M, floor(-1.5) AS F, ceil(1.25) AS CE",
                         FLATSQL_SLOT_SANDBOX);
    REQUIRE(r.status == FLATSQL_OK && r.rows.size() == 1, r.err);
    const double pi = std::acos(-1.0);
    const std::pair<const char*, double> want[] = {
        {"P", 1024.0},        {"PW", 2.25},       {"Q", 1.5},    {"S", std::sin(0.5)},
        {"C", std::cos(0.5)}, {"A", std::atan2(1.0, 2.0)},     {"L", std::log(10.0)}, {"G", 3.0},
        {"L2", 3.0},          {"E", std::exp(1.0)}, {"PI", pi},  {"D", 180.0},
        {"R", pi},            {"M", 1.5},         {"F", -2.0},   {"CE", 2.0},
    };
    for (const auto& w : want)
        CHECK(near(real(r, 0, w.first), w.second), std::string(w.first) + " = " + std::to_string(real(r, 0, w.first)));
    // Over a relation: MEAN_MOTION is 15.5 in every test OMM.
    const Result o = sql("SELECT pow(MEAN_MOTION, 2) AS MM2, sqrt(MEAN_MOTION * 4) AS S FROM OMM ORDER BY _rowid", FLATSQL_SLOT_SANDBOX);
    REQUIRE(o.status == FLATSQL_OK && o.rows.size() == 3, o.err);
    for (size_t i = 0; i < o.rows.size(); i++) {
        CHECK(near(real(o, i, "MM2"), 240.25), "pow over a column");
        CHECK(near(real(o, i, "S"), std::sqrt(62.0)), "sqrt over a column");
    }
    // Out of a function's domain: NULL, as SQLite documents.
    const Result d = sql("SELECT sqrt(-1) AS N, ln(0) AS Z", FLATSQL_SLOT_SANDBOX);
    CHECK(d.status == FLATSQL_OK && d.rows.size() == 1 && d.null(0, "N") && d.null(0, "Z"), d.err);
}
