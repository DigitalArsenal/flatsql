// benchset.json R17/R18/R19 on a slice of the host-02-sized format-1 fixture
// (CONTRACT.md §6 "sql"): the newest P4_FIXTURE_SLICE index rows of each
// type (default 12,000; the A18 bound of every type but OMM is 10,000), with their records, peers, timestamps and source
// tags, served by the fake reader (seq = format 1's index rowid, the A18
// bounds of the contract) and by format 1's engine fed as SDN fills its hot
// window. Frames, rows and columns must be equal.
//
// P4_FIXTURE: a format-1 control.flatsqldb (a cp -c clone; opened read-only
// and immutable). Skips without it.
#ifdef FLATSQL_P4SQL_FAKE

#include <sqlite3.h>

#include <chrono>
#include <cstdlib>
#include <map>
#include <set>

#include "format1_oracle.h"
#include "p4sql_test.h"

using namespace p4sqlt;

namespace {

struct Slice {
    std::string type;
    uint64_t bound;
    std::vector<std::string> tables;   // partition tables of the type
    std::string rules;                 // SDN's (storage/format2/typeconfig.go typeRules)
    p4fake::Type* t = nullptr;
};

std::string text(sqlite3_stmt* s, int i) {
    const unsigned char* p = sqlite3_column_text(s, i);
    return p ? std::string(reinterpret_cast<const char*>(p), size_t(sqlite3_column_bytes(s, i))) : std::string();
}

// Loads index rows into the fake (newest first), skipping seqs already
// loaded: the newest n of the type (cids empty), or the rows of `cids`.
bool loadRows(sqlite3* db, Harness& h, Slice& sl, int64_t n, const std::vector<std::string>& cids,
              std::set<int64_t>* seen, std::string* err) {
    const std::string schema = sl.type + ".fbs";
    sqlite3_stmt* idx = nullptr;
    sqlite3_prepare_v2(db,
                       cids.empty() ? "SELECT rowid, cid, norad_cat_id, entity_id, epoch_unix, source_timestamp FROM "
                                      "sdn_record_index WHERE schema_name = ?1 ORDER BY rowid DESC LIMIT ?2"
                                    : "SELECT rowid, cid, norad_cat_id, entity_id, epoch_unix, source_timestamp FROM "
                                      "sdn_record_index WHERE schema_name = ?1 AND cid = ?3",
                       -1, &idx, nullptr);
    sqlite3_stmt* tags = nullptr;
    sqlite3_prepare_v2(db,
                       "SELECT provider_id, source_name, ifnull(source_url, ''), batch_id, content_key_id, "
                       "producer_peer_id, producer_public_key, ifnull(created_at, 0) FROM sdn_record_source_tags "
                       "WHERE schema_name = ?1 AND cid = ?2",
                       -1, &tags, nullptr);
    std::vector<sqlite3_stmt*> parts;
    for (const std::string& t : sl.tables) {
        sqlite3_stmt* p = nullptr;
        if (sqlite3_prepare_v2(db, ("SELECT peer_id, timestamp, data FROM \"" + t + "\" WHERE cid = ?1").c_str(), -1, &p,
                               nullptr) != SQLITE_OK) {
            *err = sqlite3_errmsg(db);
            return false;
        }
        parts.push_back(p);
    }
    sqlite3_bind_text(idx, 1, schema.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(idx, 2, n);
    size_t next = 0;
    for (;;) {
        if (!cids.empty()) {
            if (next >= cids.size()) break;
            sqlite3_reset(idx);
            sqlite3_bind_text(idx, 3, cids[next++].c_str(), -1, SQLITE_TRANSIENT);
        }
        if (sqlite3_step(idx) != SQLITE_ROW) {
            if (cids.empty()) break;
            continue;
        }
        p4fake::Rec r;
        r.seq = sqlite3_column_int64(idx, 0);
        if (!seen->insert(r.seq).second) continue;
        r.cid = text(idx, 1);
        if (sqlite3_column_type(idx, 2) != SQLITE_NULL && sqlite3_column_int64(idx, 2) > 0) {
            r.hasCol0 = true;
            r.col0 = sqlite3_column_int64(idx, 2);
        }
        if (sqlite3_column_type(idx, 3) != SQLITE_NULL && sqlite3_column_bytes(idx, 3) > 0) {
            r.hasCol1 = true;
            r.col1 = text(idx, 3);
        }
        if (sqlite3_column_type(idx, 4) != SQLITE_NULL) {
            r.hasEpoch = true;
            r.epoch = sqlite3_column_int64(idx, 4);
        }
        r.ts = sqlite3_column_int64(idx, 5);
        bool found = false;
        for (size_t i = 0; i < parts.size() && !found; i++) {
            sqlite3_bind_text(parts[i], 1, r.cid.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(parts[i]) == SQLITE_ROW) {
                found = true;
                r.peer = text(parts[i], 0);
                r.producer = sl.tables[i].substr(6, sl.tables[i].rfind("__") - 6);
                const uint8_t* d = static_cast<const uint8_t*>(sqlite3_column_blob(parts[i], 2));
                r.data.assign(d, d + sqlite3_column_bytes(parts[i], 2));
            }
            sqlite3_reset(parts[i]);
        }
        if (!found) continue;   // an orphan index row: format 1 serves no record for it
        sqlite3_bind_text(tags, 1, schema.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(tags, 2, r.cid.c_str(), -1, SQLITE_TRANSIENT);
        while (sqlite3_step(tags) == SQLITE_ROW) {
            p4fake::Tag t;
            t.provider = text(tags, 0);
            t.source = text(tags, 1);
            t.sourceUrl = text(tags, 2);
            t.batch = text(tags, 3);
            t.contentKeyId = text(tags, 4);
            t.producerPeer = text(tags, 5);
            t.producerPubkey = text(tags, 6);
            t.at = sqlite3_column_int64(tags, 7);
            r.tags.push_back(t);
        }
        sqlite3_reset(tags);
        h.put(sl.type, std::move(r));
    }
    sqlite3_finalize(idx);
    sqlite3_finalize(tags);
    for (sqlite3_stmt* p : parts) sqlite3_finalize(p);
    return true;
}

// The newest n index rows of the type, plus 200 records of every source it
// has (through the tag index), so a source whose records all sit below the
// window still exists (benchset R17: CAT@celestrak-satcat-csv).
bool loadSlice(sqlite3* db, Harness& h, Slice& sl, int64_t n, std::string* err) {
    std::set<int64_t> seen;
    if (!loadRows(db, h, sl, n, {}, &seen, err)) return false;
    const std::string schema = sl.type + ".fbs";
    sqlite3_stmt* src = nullptr;
    sqlite3_prepare_v2(db, "SELECT DISTINCT source_name FROM sdn_record_source_summary WHERE schema_name = ?1", -1,
                       &src, nullptr);
    sqlite3_bind_text(src, 1, schema.c_str(), -1, SQLITE_TRANSIENT);
    std::vector<std::string> sources;
    while (sqlite3_step(src) == SQLITE_ROW) sources.push_back(text(src, 0));
    sqlite3_finalize(src);
    sqlite3_stmt* pick = nullptr;
    sqlite3_prepare_v2(db, "SELECT cid FROM sdn_record_source_tags WHERE schema_name = ?1 AND source_name = ?2 LIMIT 200",
                       -1, &pick, nullptr);
    for (const std::string& source : sources) {
        std::vector<std::string> cids;
        sqlite3_bind_text(pick, 1, schema.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(pick, 2, source.c_str(), -1, SQLITE_TRANSIENT);
        while (sqlite3_step(pick) == SQLITE_ROW) cids.push_back(text(pick, 0));
        sqlite3_reset(pick);
        if (!loadRows(db, h, sl, 0, cids, &seen, err)) return false;
    }
    sqlite3_finalize(pick);
    return true;
}

// Every source the slice carries (format 1 registers its sources store-wide).
std::vector<std::string> allSources(const p4fake::Type& t) {
    std::set<std::string> s;
    for (const p4fake::Rec& r : t.recs)
        for (const p4fake::Tag& g : r.tags) s.insert(g.source);
    return std::vector<std::string>(s.begin(), s.end());
}

std::vector<std::string> sourcesOf(const p4fake::Type& t) {
    std::set<std::string> s;
    const size_t n = t.recs.size();
    const size_t first = t.bound && n > t.bound ? n - size_t(t.bound) : 0;
    for (size_t i = first; i < n; i++)
        for (const p4fake::Tag& g : t.recs[i].tags) s.insert(g.source);
    return std::vector<std::string>(s.begin(), s.end());
}

size_t framesOf(const std::vector<uint8_t>& raw) {
    std::vector<std::string> fr;
    return rb1::rawSplit(raw.data(), raw.size(), &fr) ? fr.size() : size_t(-1);
}

}  // namespace

P4SQL_TEST(fixture_slice_r17_r18_r19_equal_format1) {
    const std::string fx = env("P4_FIXTURE");
    if (fx.empty()) {
        std::printf("    SKIP: P4_FIXTURE not set\n");
        return;
    }
    const int64_t n = env("P4_FIXTURE_SLICE").empty() ? 12000 : std::atoll(env("P4_FIXTURE_SLICE").c_str());
    sqlite3* db = nullptr;
    if (sqlite3_open_v2(("file:" + fx + "?mode=ro&immutable=1").c_str(), &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI,
                        nullptr) != SQLITE_OK) {
        failAt(__FILE__, __LINE__, "cannot open " + fx);
        return;
    }
    Harness h;
    std::vector<Slice> slices = {
        {"OMM", 400000, {"sds_p_source_celestrak__OMM"},
         "epoch str:EPOCH|str:CREATION_DATE\ncol 0 u64pos:NORAD_CAT_ID\ncol 1 str:OBJECT_ID\nepoch_day 4\nobject 0,1\n"},
        {"MPE", 10000, {"sds_p_source_celestrak__MPE"}, "epoch f64floor:EPOCH\ncol 1 str:ENTITY_ID\nepoch_day 4\nobject 1\n"},
        {"CAT", 10000, {"sds_p_source_celestrak__CAT"},
         "col 0 u64pos:NORAD_CAT_ID\ncol 1 str:OBJECT_ID\ncol 2 enum:OBJECT_TYPE\ncol 3 enum:OPS_STATUS_CODE\nobject 0,1\n"},
        {"IQC", 10000, {"sds_p_source_sigmf__IQC", "sds_p_16Uiu2HAm1LbvwjEHW2GDP2ZQZvwHLZrz2jbYoRLQmJEQ3wZ5Fm45__IQC"},
         "bucket str:CAPTURE_START\n"},
    };
    const auto t0 = std::chrono::steady_clock::now();
    for (Slice& sl : slices) {
        const std::string fid = "$" + sl.type;
        sl.t = &h.addType(sl.type, fid.c_str(), readFile(vectorDir() + "/" + sl.type + ".bfbs"), sl.bound);
        sl.t->rules = sl.rules;
        std::string err;
        CHECK(loadSlice(db, h, sl, n, &err));
        if (!err.empty()) std::fprintf(stderr, "    %s\n", err.c_str());
        std::printf("    %s: %zu records, window sources:", sl.type.c_str(), sl.t->recs.size());
        for (const std::string& s : sourcesOf(*sl.t)) std::printf(" %s", s.c_str());
        std::printf("; all:");
        for (const std::string& s : allSources(*sl.t)) std::printf(" %s", s.c_str());
        std::printf("\n");
    }
    sqlite3_close(db);
    report("fixture_slice_load_s", std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count(), "s");

    // One format-1 engine per type (each its own table graph and sources).
    std::map<std::string, std::unique_ptr<Format1>> f1;
    for (Slice& sl : slices) {
        f1[sl.type].reset(new Format1({sl.type}, {"$" + sl.type}));
        f1[sl.type]->sources(allSources(*sl.t));
        f1[sl.type]->feedWindow(*sl.t);
    }
    auto frames = [&](const std::string& type, const std::string& sql, const std::vector<rb1::Cell>& params,
                      uint32_t flags) {
        const Result r = h.sql(sql, params, flags);
        std::vector<uint8_t> want;
        std::string err;
        const bool ok = f1[type]->raw(sql, params, &want, &err);
        CHECK(ok);
        CHECK_EQ(r.status, 0);
        if (r.status) std::fprintf(stderr, "    %s: %s\n", sql.c_str(), r.error.c_str());
        CHECK_EQ(r.raw.size(), want.size());
        CHECK(r.raw == want);
        std::printf("    %-90.90s frames %zu (format 1 %zu), examined %llu\n", sql.c_str(), framesOf(r.raw),
                    framesOf(want), (unsigned long long)r.rowsExamined);
        return r;
    };
    auto rows = [&](const std::string& type, const std::string& sql, const std::vector<rb1::Cell>& params) {
        const Result r = h.sql(sql, params, P4_SLOT_SANDBOX);
        std::vector<std::string> names;
        std::vector<std::vector<rb1::Cell>> want;
        std::string err;
        CHECK(f1[type]->query(sql, params, &names, &want, &err));
        CHECK_EQ(r.status, 0);
        CHECK(r.names == names);
        CHECK_EQ(r.rows.size(), want.size());
        CHECK(r.rows == want);
        std::printf("    %-90.90s rows %zu (format 1 %zu)\n", sql.c_str(), r.rows.size(), want.size());
    };
    // R17: A18 relations (sandboxed raw streams).
    const uint32_t sb = P4_SLOT_RAW | P4_SLOT_SANDBOX;
    Result r = frames("CAT", "SELECT _data FROM \"CAT@celestrak-satcat\"", {}, sb);
    CHECK(r.rowsExamined <= 10000);
    r = frames("CAT", "SELECT _data FROM \"CAT@celestrak-satcat-csv\"", {}, sb);
    CHECK_EQ(framesOf(r.raw), size_t(0));   // benchset R17: the window is all celestrak-satcat
    r = frames("IQC", "SELECT _data FROM \"IQC@IQEngine\"", {}, sb);
    CHECK_EQ(framesOf(r.raw), size_t(10000));
    CHECK(r.rowsExamined <= 10000);
    r = frames("MPE", "SELECT _data FROM \"MPE@celestrak-gp\"", {}, sb);
    CHECK_EQ(framesOf(r.raw), size_t(10000));
    CHECK(r.rowsExamined <= 10000);
    frames("OMM", "SELECT _data FROM \"OMM@celestrak-gp\" WHERE NORAD_CAT_ID = 25544", {}, sb);
    // R19: sandboxed and module SQL.
    rows("OMM", "SELECT COUNT(*) FROM OMM", {});
    frames("OMM", "SELECT _data FROM OMM WHERE NORAD_CAT_ID = ?1", {cInt(25544)}, sb);
    rows("OMM",
         "SELECT OBJECT_NAME, EPOCH, NORAD_CAT_ID FROM OMM WHERE NORAD_CAT_ID BETWEEN 25000 AND 25600 ORDER BY EPOCH "
         "DESC LIMIT 100",
         {});
    rows("MPE", "SELECT _source, COUNT(*) FROM MPE GROUP BY _source", {});
    frames("CAT", "SELECT _data FROM CAT WHERE NORAD_CAT_ID = ?1", {cInt(40463)}, sb);
    // R18: the engine epoch stream (trusted module SQL, raw).
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
        const std::vector<rb1::Cell> params = {cText("OMM@celestrak-gp"), cReal(1789371001.0), cInt(50000)};
        r = frames("OMM", q, params, P4_SLOT_RAW);
        const auto q0 = std::chrono::steady_clock::now();
        h.sql(q, params, P4_SLOT_RAW);   // the SQL surface alone (the fake reader serves from memory)
        report("r18_p4sql_ms", std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - q0).count(), "ms");
        if (q == r18[0]) CHECK(framesOf(r.raw) > 0);   // nearest always has an answer per object
    }
}

#endif  // FLATSQL_P4SQL_FAKE
