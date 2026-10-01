// The record relations against format 1 (CONTRACT.md §3.9, §6 "sql"): the
// same records in the fake reader and in format 1's engine (the hot window
// SDN fills), the same statements, equal columns, rows and raw frames.
// Shapes: benchset.json R17 (A18 <TYPE>@<source>), R18 (epoch SQL), R19
// (sandboxed and module SQL).
#ifdef FLATSQL_P4SQL_FAKE

#include <algorithm>
#include <cstring>

#ifndef P4SQL_NO_ORACLE
#include "format1_oracle.h"
#endif
#include "../../src/p4sql/internal.h"
#include "p4sql_test.h"

using namespace p4sqlt;

namespace {

const std::vector<uint8_t>& bfbsOf(const std::string& t) {
    static std::map<std::string, std::vector<uint8_t>> cache;
    auto it = cache.find(t);
    if (it == cache.end()) it = cache.emplace(t, readFile(vectorDir() + "/" + t + ".bfbs")).first;
    return it->second;
}

std::string cidOf(const std::string& type, int64_t seq) {
    std::string c = "bafkrei" + type + std::to_string(seq);
    for (char& ch : c) ch = char(std::tolower(uint8_t(ch)));
    while (c.size() < 59) c += 'a';
    return c;
}

// SDN's rules (storage/format2/typeconfig.go typeRules): COL0/COL1.
const char* kOmmRules = "epoch str:EPOCH|str:CREATION_DATE\ncol 0 u64pos:NORAD_CAT_ID\ncol 1 str:OBJECT_ID\nepoch_day 4\nobject 0,1\n";
const char* kCatRules =
    "col 0 u64pos:NORAD_CAT_ID\ncol 1 str:OBJECT_ID\ncol 2 enum:OBJECT_TYPE\ncol 3 enum:OPS_STATUS_CODE\nobject 0,1\n"
    "supersede pair:uri:CATALOG_URI,CATALOG_OBJECT_ID|u64:norad:NORAD_CAT_ID|str:object:OBJECT_ID\n";

p4fake::Tag tagOf(const std::string& source, int64_t at) {
    p4fake::Tag t;
    t.provider = "space-data-network-02";
    t.source = source;
    t.batch = source + "-b000";
    t.at = at;
    return t;
}

// An OMM: every third one stored size-prefixed (format 1's engine and _data
// see it without the prefix), some without optional fields.
p4fake::Rec omm(int64_t seq, uint32_t norad, int64_t epoch, const std::vector<std::string>& sources) {
    std::string j = "{\"OBJECT_NAME\":\"SAT " + std::to_string(norad) + "\",\"NORAD_CAT_ID\":" + std::to_string(norad) +
                    ",\"EPOCH\":\"2026-09-" + std::to_string(10 + seq % 15) + "T00:00:00\"";
    j += ",\"USER_DEFINED_EPOCH_TIMESTAMP\":" + std::to_string(epoch) + ".25";
    if (seq % 2) j += ",\"OBJECT_ID\":\"1998-0" + std::to_string(norad % 100) + "A\",\"MEAN_MOTION\":15.49";
    if (seq % 5 == 0) j += ",\"TIME_SYSTEM\":2,\"COVARIANCE\":[1.5,2.5],\"ELEMENT_SET_NO\":4000000000";
    if (seq % 7 == 0) j += ",\"CCSDS_OMM_VERS\":3.0,\"CLASSIFICATION_TYPE\":\"U\"";
    j += "}";
    p4fake::Rec r;
    r.seq = seq;
    r.cid = cidOf("omm", seq);
    r.producer = "source_celestrak";
    r.peer = "12D3KooWExample";
    r.ts = 1790000000 + seq;
    r.hasEpoch = true;
    r.epoch = epoch;
    r.data = buildRecord(bfbsOf("OMM"), j, seq % 3 == 0);
    for (size_t i = 0; i < sources.size(); i++) r.tags.push_back(tagOf(sources[i], 1790000000 + seq + int64_t(i)));
    r.hasCol0 = true;
    r.col0 = norad;
    if (seq % 2) {
        r.hasCol1 = true;
        r.col1 = "1998-0" + std::to_string(norad % 100) + "A";
    }
    return r;
}

p4fake::Rec cat(int64_t seq, uint32_t norad, const std::vector<std::string>& sources) {
    const std::string j = "{\"OBJECT_NAME\":\"CAT " + std::to_string(norad) + "\",\"NORAD_CAT_ID\":" +
                          std::to_string(norad) + ",\"OBJECT_ID\":\"2000-0" + std::to_string(seq) + "A\"}";
    p4fake::Rec r;
    r.seq = seq;
    r.cid = cidOf("cat", seq);
    r.producer = "source_celestrak";
    r.peer = "12D3KooWExample";
    r.ts = 1790000000 + seq;
    r.data = buildRecord(bfbsOf("CAT"), j, false);
    for (size_t i = 0; i < sources.size(); i++) r.tags.push_back(tagOf(sources[i], 1790000000 + seq));
    r.hasCol0 = true;
    r.col0 = norad;
    return r;
}

#ifndef P4SQL_NO_ORACLE
// Rows compared with format 1's; the columns at `mask` (_rowid, _offset:
// format 1's are its hot-window positions, format 4's the seq and 0) skipped.
void sameRows(Harness& h, Format1& f1, const std::string& sql, const std::vector<rb1::Cell>& params = {},
              std::vector<size_t> mask = {}, bool sorted = false) {
    const Result r = h.sql(sql, params);
    CHECK_EQ(r.status, 0);
    if (r.status) std::fprintf(stderr, "    %s: %s\n", sql.c_str(), r.error.c_str());
    std::vector<std::string> names;
    std::vector<std::vector<rb1::Cell>> rows;
    std::string err;
    CHECK(f1.query(sql, params, &names, &rows, &err));
    if (!err.empty()) std::fprintf(stderr, "    format 1: %s\n", err.c_str());
    CHECK(r.names == names);
    auto masked = [&](std::vector<std::vector<rb1::Cell>> v) {
        for (auto& row : v)
            for (size_t m : mask)
                if (m < row.size()) row[m] = rb1::Cell();
        if (sorted)
            std::sort(v.begin(), v.end(), [](const std::vector<rb1::Cell>& a, const std::vector<rb1::Cell>& b) {
                return showRow(a) < showRow(b);
            });
        return v;
    };
    const auto a = masked(r.rows), b = masked(rows);
    CHECK_EQ(a.size(), b.size());
    for (size_t i = 0; i < std::min(a.size(), b.size()); i++)
        if (!(a[i] == b[i])) {
            failAt(__FILE__, __LINE__, sql + ": row " + std::to_string(i) + " " + showRow(a[i]) + " vs format 1 " + showRow(b[i]));
            break;
        }
    CHECK(r.ended);
    CHECK_EQ(r.endRows, uint64_t(a.size()));
}

// Raw frames compared with format 1's raw stream.
void sameFrames(Harness& h, Format1& f1, const std::string& sql, const std::vector<rb1::Cell>& params = {},
                uint32_t flags = P4_SLOT_RAW | P4_SLOT_SANDBOX, size_t* frames = nullptr) {
    const Result r = h.sql(sql, params, flags);
    CHECK_EQ(r.status, 0);
    if (r.status) std::fprintf(stderr, "    %s: %s\n", sql.c_str(), r.error.c_str());
    std::vector<uint8_t> want;
    std::string err;
    CHECK(f1.raw(sql, params, &want, &err));
    if (!err.empty()) std::fprintf(stderr, "    format 1: %s\n", err.c_str());
    CHECK_EQ(r.raw.size(), want.size());
    CHECK(r.raw == want);
    if (frames) {
        std::vector<std::string> fr;
        CHECK(rb1::rawSplit(r.raw.data(), r.raw.size(), &fr));
        *frames = fr.size();
    }
}

#endif

std::vector<rb1::Cell> P(std::initializer_list<rb1::Cell> c) { return std::vector<rb1::Cell>(c); }

}  // namespace

#ifndef P4SQL_NO_ORACLE
// OMM, one source: every column of every row (enum, table and vector
// columns read as format 1 reads them), and the R17/R18/R19 OMM shapes.
P4SQL_TEST(omm_rows_and_frames_equal_format1) {
    Harness h;
    CHECK_EQ(h.initStatus(), 0);
    p4fake::Type& t = h.addType("OMM", "$OMM", bfbsOf("OMM"), 400000);
    t.rules = kOmmRules;   // NORAD_CAT_ID and OBJECT_ID constraints reach the reader
    for (int64_t s = 1; s <= 120; s++)
        h.put("OMM", omm(s * 3, uint32_t(25000 + (s * 37) % 900), 1789000000 + s * 4000, {"celestrak-gp"}));
    Format1 f1({"OMM"}, {"$OMM"});
    f1.sources({"celestrak-gp"});
    f1.feedWindow(t);
    const size_t ns = 40;
    sameRows(h, f1, "SELECT * FROM OMM", {}, {ns + 1, ns + 2});
    // Format 1 answers the exact text `SELECT * FROM <one table>` from a
    // tabular fast path (sqlite_engine.cpp tryFastPath) that skips SQLite:
    // its _data is NULL "for performance" and a uint32 is not narrowed the
    // way its SQL path narrows it. Format 4 has no such path; it is compared
    // with format 1's SQL path (`WHERE 1` takes it).
    sameRows(h, f1, "SELECT * FROM \"OMM@celestrak-gp\" WHERE 1", {}, {ns + 1, ns + 2});
    // R19
    sameRows(h, f1, "SELECT COUNT(*) FROM OMM");
    sameRows(h, f1,
             "SELECT OBJECT_NAME, EPOCH, NORAD_CAT_ID FROM OMM WHERE NORAD_CAT_ID BETWEEN 25000 AND 25600 ORDER BY "
             "EPOCH DESC LIMIT 100");
    sameFrames(h, f1, "SELECT _data FROM OMM WHERE NORAD_CAT_ID = ?1", P({cInt(25544)}));
    sameFrames(h, f1, "SELECT _data FROM OMM WHERE NORAD_CAT_ID = ?1", P({cInt(25000 + (7 * 37) % 900)}));
    sameRows(h, f1, "SELECT _source, COUNT(*) FROM OMM GROUP BY _source");
    // R17
    sameFrames(h, f1, "SELECT _data FROM \"OMM@celestrak-gp\"");
    sameFrames(h, f1, "SELECT _data FROM \"OMM@celestrak-gp\" WHERE NORAD_CAT_ID = 25000 + 37");
    // R18: format 1's engine epoch SQL (storage/engine_records.go), all three
    // profiles, all sources and one.
    const char* r18[3] = {
        "SELECT _data FROM (SELECT _data, ROW_NUMBER() OVER (PARTITION BY NORAD_CAT_ID ORDER BY "
        "ABS(USER_DEFINED_EPOCH_TIMESTAMP - ?2)) rn FROM OMM WHERE (?1 = '' OR _source = ?1)) WHERE rn = 1 LIMIT ?3",
        "SELECT _data FROM (SELECT _data, ROW_NUMBER() OVER (PARTITION BY NORAD_CAT_ID ORDER BY "
        "USER_DEFINED_EPOCH_TIMESTAMP DESC) rn FROM OMM WHERE (?1 = '' OR _source = ?1) AND "
        "USER_DEFINED_EPOCH_TIMESTAMP <= ?2) WHERE rn = 1 LIMIT ?3",
        "SELECT _data FROM (SELECT _data, ROW_NUMBER() OVER (PARTITION BY NORAD_CAT_ID ORDER BY "
        "USER_DEFINED_EPOCH_TIMESTAMP ASC) rn FROM OMM WHERE (?1 = '' OR _source = ?1) AND "
        "USER_DEFINED_EPOCH_TIMESTAMP >= ?2) WHERE rn = 1 LIMIT ?3",
    };
    for (const char* q : r18) {
        size_t frames = 0;
        sameFrames(h, f1, q, P({cText(""), cReal(1789240000.0), cInt(-1)}), P4_SLOT_RAW, &frames);
        CHECK(frames > 10);
        sameFrames(h, f1, q, P({cText("OMM@celestrak-gp"), cReal(1789240000.0), cInt(50)}), P4_SLOT_RAW);
        sameFrames(h, f1, q, P({cText("OMM@nope"), cReal(1789240000.0), cInt(50)}), P4_SLOT_RAW, &frames);
        CHECK_EQ(frames, size_t(0));
    }
    // The hidden columns: the reader's row.
    const Result hid = h.sql("SELECT _seq, _cid, _ts, _epoch, _producer, _peer, _rowid, _offset FROM OMM WHERE _seq = 30");
    CHECK_EQ(hid.status, 0);
    CHECK_EQ(hid.rows.size(), size_t(1));
    if (hid.rows.size() == 1) {
        const p4fake::Rec r = omm(30, uint32_t(25000 + (10 * 37) % 900), 1789000000 + 10 * 4000, {});
        CHECK(hid.rows[0][0] == cInt(30));
        CHECK(hid.rows[0][1] == cText(r.cid));
        CHECK(hid.rows[0][2] == cInt(r.ts));
        CHECK(hid.rows[0][3] == cInt(r.epoch));
        CHECK(hid.rows[0][4] == cText("source_celestrak"));
        CHECK(hid.rows[0][5] == cText("12D3KooWExample"));
        CHECK(hid.rows[0][6] == cInt(30));
        CHECK(hid.rows[0][7] == cInt(0));
    }
}

// A18 is format 1's window: the type's newest N, then the source. CAT's
// newest N are all one source's, so the other source's relation is empty
// (benchset R17: CAT@celestrak-satcat-csv = 0 frames).
P4SQL_TEST(a18_window_is_the_types_newest_n) {
    Harness h;
    p4fake::Type& t = h.addType("CAT", "$CAT", bfbsOf("CAT"), 10);
    t.rules = kCatRules;
    for (int64_t s = 1; s <= 15; s++) h.put("CAT", cat(s, uint32_t(40000 + s), {"celestrak-satcat-csv"}));
    for (int64_t s = 16; s <= 30; s++)
        h.put("CAT", cat(s, uint32_t(40000 + s), s % 4 == 0 ? std::vector<std::string>{"celestrak-satcat", "celestrak-satcat-csv"}
                                                              : std::vector<std::string>{"celestrak-satcat"}));
    Format1 f1({"CAT"}, {"$CAT"});
    f1.sources({"celestrak-satcat", "celestrak-satcat-csv"});
    f1.feedWindow(t);
    size_t frames = 0;
    sameFrames(h, f1, "SELECT _data FROM \"CAT@celestrak-satcat\"", {}, P4_SLOT_RAW | P4_SLOT_SANDBOX, &frames);
    CHECK_EQ(frames, size_t(10));
    Result r = h.sql("SELECT _data FROM \"CAT@celestrak-satcat-csv\"", {}, P4_SLOT_RAW | P4_SLOT_SANDBOX);
    sameFrames(h, f1, "SELECT _data FROM \"CAT@celestrak-satcat-csv\"", {}, P4_SLOT_RAW | P4_SLOT_SANDBOX, &frames);
    CHECK_EQ(frames, size_t(2));   // seq 24 and 28 of the window (21..30) carry both sources
    // Never past the bound: one cursor over the window (C-17).
    CHECK(r.rowsExamined <= 10);
    sameRows(h, f1, "SELECT _source, COUNT(*) FROM CAT GROUP BY _source");
    sameRows(h, f1, "SELECT NORAD_CAT_ID, _source FROM CAT");   // the sources one after another
    sameRows(h, f1, "SELECT COUNT(*) FROM CAT");
    sameRows(h, f1, "SELECT _data FROM CAT WHERE NORAD_CAT_ID = ?1", P({cInt(40024)}));
    sameFrames(h, f1, "SELECT _data FROM CAT WHERE NORAD_CAT_ID = ?1", P({cInt(40024)}));
    r = h.sql("SELECT COUNT(*) FROM CAT");
    CHECK(r.rowsExamined <= 2 * 10);   // a cursor per source, each over the window
    // Ordered by seq: merged across the sources.
    r = h.sql("SELECT _seq, _source FROM CAT ORDER BY _seq");
    CHECK_EQ(r.rows.size(), size_t(12));
    for (size_t i = 1; i < r.rows.size(); i++) CHECK(r.rows[i - 1][0].i <= r.rows[i][0].i);
    r = h.sql("SELECT _seq FROM CAT ORDER BY _seq DESC");
    CHECK_EQ(r.rows.size(), size_t(12));
    CHECK(!r.rows.empty() && r.rows[0][0] == cInt(30));
    for (size_t i = 1; i < r.rows.size(); i++) CHECK(r.rows[i - 1][0].i >= r.rows[i][0].i);
    // A source the type never saw is no relation (format 1: no such table).
    r = h.sql("SELECT _data FROM \"CAT@unknown\"");
    CHECK_EQ(r.status, P4_E_SQL);
    CHECK(r.error.find("no such table") != std::string::npos);
}

#endif  // P4SQL_NO_ORACLE

// One source: one bounded cursor, never past N rows.
P4SQL_TEST(a18_single_source_reads_n_rows) {
    Harness h;
    h.addType("OMM", "$OMM", bfbsOf("OMM"), 20);
    for (int64_t s = 1; s <= 100; s++) h.put("OMM", omm(s, uint32_t(25000 + s), 1789000000 + s, {"celestrak-gp"}));
    Result r = h.sql("SELECT _data FROM \"OMM@celestrak-gp\"", {}, P4_SLOT_RAW | P4_SLOT_SANDBOX);
    CHECK_EQ(r.status, 0);
    std::vector<std::string> fr;
    CHECK(rb1::rawSplit(r.raw.data(), r.raw.size(), &fr));
    CHECK_EQ(fr.size(), size_t(20));
    CHECK_EQ(r.rowsExamined, uint64_t(20));
    CHECK_EQ(r.opened.size(), size_t(1));
    if (!r.opened.empty()) {
        CHECK_EQ(r.opened[0].bound, uint64_t(20));
        CHECK_EQ(int(r.opened[0].hydrate), 1);
    }
    r = h.sql("SELECT COUNT(*) FROM OMM");
    CHECK(!r.rows.empty() && r.rows[0][0] == cInt(20));
    CHECK_EQ(r.rowsExamined, uint64_t(20));
    if (!r.opened.empty()) CHECK_EQ(int(r.opened[0].hydrate), 0);   // no column needs the record bytes
}

// Pushdown: what the reader is asked for, and the same rows as without it
// (`+col` turns a constraint into an expression SQLite evaluates itself).
P4SQL_TEST(pushdown_seq_epoch_source) {
    Harness h;
    h.addType("OMM", "$OMM", bfbsOf("OMM"), 400000);
    for (int64_t s = 1; s <= 60; s++) h.put("OMM", omm(s, uint32_t(25000 + s), 1789000000 + s * 100, {"celestrak-gp"}));
    auto same = [&](const std::string& pushed, const std::string& plain) {
        const Result a = h.sql(pushed), b = h.sql(plain);
        CHECK_EQ(a.status, 0);
        CHECK_EQ(b.status, 0);
        CHECK(a.rows == b.rows);
        return a;
    };
    Result r = same("SELECT _seq FROM OMM WHERE _seq > 10 AND _seq <= 20", "SELECT _seq FROM OMM WHERE +_seq > 10 AND +_seq <= 20");
    CHECK_EQ(r.rows.size(), size_t(10));
    CHECK(!r.opened.empty() && r.opened[0].seqAfter == 10 && r.opened[0].seqThrough == 20);
    r = same("SELECT _seq FROM OMM WHERE _rowid = 15", "SELECT _seq FROM OMM WHERE +_rowid = 15");
    CHECK(!r.opened.empty() && r.opened[0].seqAfter == 14 && r.opened[0].seqThrough == 15);
    r = same("SELECT _seq FROM OMM WHERE _seq >= 10.5 AND _seq < 12.5", "SELECT _seq FROM OMM WHERE +_seq >= 10.5 AND +_seq < 12.5");
    CHECK_EQ(r.rows.size(), size_t(2));
    r = same("SELECT _seq FROM OMM WHERE _seq = 7.5", "SELECT _seq FROM OMM WHERE +_seq = 7.5");
    CHECK_EQ(r.rows.size(), size_t(0));
    r = same("SELECT _seq FROM OMM WHERE _epoch >= 1789003000 AND _epoch < 1789003500",
             "SELECT _seq FROM OMM WHERE +_epoch >= 1789003000 AND +_epoch < 1789003500");
    CHECK_EQ(r.rows.size(), size_t(5));
    CHECK(!r.opened.empty() && r.opened[0].preds.size() == 2 && r.opened[0].preds[0].field == P4_F_EPOCH);
    r = same("SELECT _seq FROM OMM WHERE _ts > 1790000050", "SELECT _seq FROM OMM WHERE +_ts > 1790000050");
    CHECK(!r.opened.empty() && r.opened[0].preds.size() == 1 && r.opened[0].preds[0].field == P4_F_TS);
    r = same("SELECT _seq FROM OMM WHERE _source = 'OMM@celestrak-gp'", "SELECT _seq FROM OMM");
    CHECK_EQ(r.rows.size(), size_t(60));
    r = h.sql("SELECT _seq FROM OMM WHERE _source = 'OMM@nope'");
    CHECK_EQ(r.rows.size(), size_t(0));
    CHECK(r.opened.empty());
    r = h.sql("SELECT _seq FROM OMM WHERE _seq = NULL");
    CHECK_EQ(r.rows.size(), size_t(0));
    r = h.sql("SELECT _seq FROM \"OMM@celestrak-gp\" ORDER BY _seq DESC LIMIT 3");
    CHECK_EQ(r.rows.size(), size_t(3));
    CHECK(!r.rows.empty() && r.rows[0][0] == cInt(60));
    CHECK(!r.opened.empty() && r.opened[0].order == P4_ORDER_SEQ_DESC);
    CHECK(r.rowsExamined <= 3);   // the descending cursor stops with the statement
}

// Visible-through: the relations never see a seq the reader hides.
P4SQL_TEST(relations_stop_at_visible_through) {
    Harness h;
    p4fake::Type& t = h.addType("OMM", "$OMM", bfbsOf("OMM"), 5);
    for (int64_t s = 1; s <= 10; s++) h.put("OMM", omm(s, uint32_t(25000 + s), 1789000000 + s, {"celestrak-gp"}));
    t.visibleThrough = 8;
    const Result r = h.sql("SELECT _seq FROM OMM");
    CHECK_EQ(r.rows.size(), size_t(5));
    CHECK(!r.rows.empty() && r.rows.front()[0] == cInt(4) && r.rows.back()[0] == cInt(8));
}

// Unknown names and the case of type and source names (SQLite identifiers
// are case-insensitive, as format 1's tables are).
P4SQL_TEST(relation_names) {
    Harness h;
    h.addType("OMM", "$OMM", bfbsOf("OMM"), 100);
    for (int64_t s = 1; s <= 5; s++) h.put("OMM", omm(s, uint32_t(25000 + s), 1789000000 + s, {"celestrak-gp"}));
    Result r = h.sql("SELECT COUNT(*) FROM omm");
    CHECK(!r.rows.empty() && r.rows[0][0] == cInt(5));
    r = h.sql("SELECT COUNT(*), MIN(_source) FROM \"omm@CELESTRAK-GP\"");
    CHECK(!r.rows.empty() && r.rows[0][0] == cInt(5) && r.rows[0][1] == cText("OMM@celestrak-gp"));
    r = h.sql("SELECT * FROM NOPE");
    CHECK_EQ(r.status, P4_E_SQL);
    CHECK(r.error.find("no such table: NOPE") != std::string::npos);
    CHECK(r.ended && r.endStatus == P4_E_SQL);
}


// COL0/COL1 (the type's `col 0`/`col 1` rules, C-16): pushed as reader
// predicates only where they keep every row SQLite keeps. COL0 is `u64pos`
// (zero and negatives are absent there): equality on a positive value, or a
// range with a positive lower bound. COL1 is `str`, trimmed, empty = absent:
// equality on non-empty text without surrounding space.
P4SQL_TEST(pushdown_col0_col1) {
    Harness h;
    p4fake::Type& t = h.addType("OMM", "$OMM", bfbsOf("OMM"), 400000);
    t.rules = kOmmRules;
    for (int64_t s = 1; s <= 60; s++) h.put("OMM", omm(s, uint32_t(25000 + s % 20), 1789000000 + s, {"celestrak-gp"}));
    struct Case {
        const char* where;
        std::vector<std::pair<uint8_t, uint8_t>> preds;   // field, op
        const char* plainWhere = nullptr;   // the same rows without pushdown (default: `+column`)
    };
    const Case cases[] = {
        {"NORAD_CAT_ID = 25010", {{P4_F_COL0, P4_OP_EQ}}},
        // The column's INTEGER affinity makes '25010' an integer; `+column`
        // would drop the affinity, so the plain plan compares the integer.
        {"NORAD_CAT_ID = '25010'", {{P4_F_COL0, P4_OP_EQ}}, "+NORAD_CAT_ID = 25010"},
        {"NORAD_CAT_ID = 0", {}},
        {"NORAD_CAT_ID BETWEEN 25005 AND 25012", {{P4_F_COL0, P4_OP_GE}, {P4_F_COL0, P4_OP_LE}}},
        {"NORAD_CAT_ID > 25015", {{P4_F_COL0, P4_OP_GE}}},
        {"NORAD_CAT_ID < 25003", {}},
        {"NORAD_CAT_ID >= -5 AND NORAD_CAT_ID < 25003", {}},
        {"OBJECT_ID = '1998-001A'", {{P4_F_COL1, P4_OP_EQ}}},
        {"OBJECT_ID = ' 1998-001A'", {}},
        {"OBJECT_ID = ''", {}},
        {"OBJECT_ID > '1998-001A'", {}},
    };
    for (const Case& c : cases) {
        const std::string pushed = std::string("SELECT _seq, NORAD_CAT_ID, OBJECT_ID FROM OMM WHERE ") + c.where;
        std::string plain = c.plainWhere ? std::string("SELECT _seq, NORAD_CAT_ID, OBJECT_ID FROM OMM WHERE ") + c.plainWhere
                                         : pushed;
        for (const char* col : {"NORAD_CAT_ID", "OBJECT_ID"}) {
            if (c.plainWhere) break;
            const std::string w = std::string(" ") + col + " ";
            size_t at = plain.find("WHERE");
            while ((at = plain.find(w, at)) != std::string::npos) {
                plain.replace(at, w.size(), std::string(" +") + col + " ");
                at += w.size() + 1;
            }
        }
        const Result a = h.sql(pushed), b = h.sql(plain);
        CHECK_EQ(a.status, 0);
        CHECK_EQ(b.status, 0);
        if (!(a.rows == b.rows)) failAt(__FILE__, __LINE__, std::string(c.where) + ": rows differ from the plain plan");
        CHECK_EQ(a.opened.size(), size_t(1));
        if (a.opened.size() != 1) continue;
        std::vector<std::pair<uint8_t, uint8_t>> got;
        for (const auto& p : a.opened[0].preds) got.push_back({p.field, p.op});
        if (got != c.preds) failAt(__FILE__, __LINE__, std::string(c.where) + ": unexpected reader predicates");
    }
    // The pushed equality reads one object's rows only.
    const Result r = h.sql("SELECT _data FROM OMM WHERE NORAD_CAT_ID = 25010");
    CHECK_EQ(r.rows.size(), size_t(3));
    CHECK(r.rowsExamined == 60);   // the fake filters by examining; the engine seeks (r_dk)
}


// The A18 raw stream (`SELECT _data FROM <relation>`, raw mode) is served
// without SQLite; its bytes, caps, errors and cancel equal the SQL path's
// (`WHERE 1` sends the same statement through SQLite).
P4SQL_TEST(raw_data_stream_fast_path_equals_sql) {
    Harness h;
    p4fake::Type& ct = h.addType("CAT", "$CAT", bfbsOf("CAT"), 10);
    ct.rules = kCatRules;
    for (int64_t s = 1; s <= 15; s++) h.put("CAT", cat(s, uint32_t(40000 + s), {"celestrak-satcat-csv"}));
    for (int64_t s = 16; s <= 30; s++)
        h.put("CAT", cat(s, uint32_t(40000 + s), s % 4 == 0 ? std::vector<std::string>{"celestrak-satcat", "celestrak-satcat-csv"}
                                                              : std::vector<std::string>{"celestrak-satcat"}));
    h.addType("OMM", "$OMM", bfbsOf("OMM"), 400000);
    for (int64_t s = 1; s <= 40; s++) h.put("OMM", omm(s, uint32_t(25000 + s), 1789000000 + s, {"celestrak-gp"}));
    auto* ls = static_cast<flatsql::p4sql::LaneState*>(h.lane.sqlState);
    const uint32_t sb = P4_SLOT_RAW | P4_SLOT_SANDBOX;
    struct Case {
        const char* fast;
        const char* sql;
        const char* relation;   // lower case
    };
    const Case cases[] = {
        {"SELECT _data FROM \"CAT@celestrak-satcat\"", "SELECT _data FROM \"CAT@celestrak-satcat\" WHERE 1", "cat@celestrak-satcat"},
        {"SELECT _data FROM \"CAT@celestrak-satcat-csv\"", "SELECT _data FROM \"CAT@celestrak-satcat-csv\" WHERE 1",
         "cat@celestrak-satcat-csv"},
        {"SELECT _data FROM CAT", "SELECT _data FROM CAT WHERE 1", "cat"},
        {"  select _DATA\n from \"omm@CELESTRAK-GP\" ; ", "SELECT _data FROM \"OMM@celestrak-gp\" WHERE 1", "omm@celestrak-gp"},
        {"SELECT \"_data\" FROM omm;", "SELECT _data FROM OMM WHERE 1", "omm"},
    };
    for (const Case& c : cases) {
        const Result fast = h.sql(c.fast, {}, sb);
        CHECK_EQ(fast.status, 0);
        if (ls->relations.count(c.relation)) failAt(__FILE__, __LINE__, std::string(c.fast) + ": went through SQLite");
        const Result slow = h.sql(c.sql, {}, sb);
        CHECK_EQ(slow.status, 0);
        CHECK(ls->relations.count(c.relation) == 1);
        if (fast.raw != slow.raw) failAt(__FILE__, __LINE__, std::string(c.fast) + ": bytes differ from the SQL path");
    }
    // Other shapes and other modes go through SQLite.
    Result r = h.sql("SELECT _data FROM \"CAT@celestrak-satcat\" LIMIT 3", {}, sb);
    std::vector<std::string> frames;
    CHECK(rb1::rawSplit(r.raw.data(), r.raw.size(), &frames) && frames.size() == 3);
    r = h.sql("SELECT _data FROM CAT", {}, P4_SLOT_SANDBOX);
    CHECK(r.ended && r.rows.size() == 12);   // RB1, not raw
    r = h.sql("SELECT _data FROM \"CAT@nope\"", {}, sb);   // not a relation: SQLite says so
    CHECK_EQ(r.status, P4_E_SQL);
    CHECK(r.error.find("no such table") != std::string::npos);
    // Caps and cancel, as on the SQL path.
    Caps caps;
    caps.maxResultRows = 3;
    r = h.sql("SELECT _data FROM OMM", {}, sb, caps);
    CHECK_EQ(r.status, P4_E_BUDGET);
    CHECK(r.error.find("row-cap") == 0);
    frames.clear();   // rawSplit appends
    CHECK(rb1::rawSplit(r.raw.data(), r.raw.size(), &frames) && frames.size() == 3);
    Caps bytes;
    bytes.maxResultBytes = 500;
    r = h.sql("SELECT _data FROM OMM", {}, sb, bytes);
    CHECK_EQ(r.status, P4_E_BUDGET);
    CHECK(r.raw.size() <= 500);
    Caps cancel;
    cancel.cancelAfterRows = 5;
    r = h.sql("SELECT _data FROM OMM", {}, sb, cancel);
    CHECK_EQ(r.status, P4_E_CANCELLED);
    // A record with no bytes: _data is NULL, not a record stream (both paths).
    p4fake::Rec empty = omm(41, 25041, 1789000041, {"celestrak-gp"});
    empty.data.clear();
    h.put("OMM", empty);
    r = h.sql("SELECT _data FROM OMM", {}, sb);
    CHECK_EQ(r.status, P4_E_SQL);
    CHECK(r.error.find("not-a-record-stream") == 0);
    r = h.sql("SELECT _data FROM OMM WHERE 1", {}, sb);
    CHECK_EQ(r.status, P4_E_SQL);
}

#endif  // FLATSQL_P4SQL_FAKE
