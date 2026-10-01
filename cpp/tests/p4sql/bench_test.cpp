// The SQL surface's own cost per row (P4SQL_BENCH=1; skipped otherwise): the
// fake reader serves rows from memory, so these times are p4sql's (vtab,
// SQLite VM, RB1/raw encoding, copies) plus a negligible cursor. The brief's
// gate compares whole shapes with formats 1 and 2 (benchset R17: format 1
// serves CAT@celestrak-satcat's 10,000 frames in 3.1 ms warm, 0.31 us per
// frame); the engine's read cost adds to these.
#ifdef FLATSQL_P4SQL_FAKE

#include <algorithm>
#include <chrono>
#include <cstring>

#include "p4sql_test.h"

using namespace p4sqlt;

namespace {

double ms(const std::chrono::steady_clock::time_point& t0) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
}

// p50 of n runs.
double timeIt(Harness& h, const std::string& q, uint32_t flags, const std::vector<rb1::Cell>& params, int n,
              size_t* bytes) {
    std::vector<double> t;
    h.sql(q, params, flags);   // warm-up
    for (int i = 0; i < n; i++) {
        const auto t0 = std::chrono::steady_clock::now();
        const Result r = h.sql(q, params, flags);
        t.push_back(ms(t0));
        CHECK_EQ(r.status, 0);
        *bytes = r.raw.size();
    }
    std::sort(t.begin(), t.end());
    return t[t.size() / 2];
}

// The same relation read straight from the reader into raw frames, without
// SQL: what the fake reader and the frame copies cost on their own.
double directFrames(Harness& h, const char* type, const char* source, uint64_t bound, int n) {
    std::vector<double> t;
    std::vector<uint8_t> out;
    for (int i = 0; i < n + 1; i++) {
        out.clear();
        const auto t0 = std::chrono::steady_clock::now();
        P4ScanSpec sp;
        std::memset(&sp, 0, sizeof(sp));
        sp.type = type;
        sp.lane.source = source;
        sp.bound = bound;
        sp.hydrate = 1;
        sp.order = P4_ORDER_SEQ_ASC;
        P4Cursor* c = nullptr;
        p4_cursor_open(&h.lane, &sp, &c);
        P4Row r;
        while (p4_cursor_next(c, &r) > 0) rb1::rawFrame(r.data, r.dataLen, &out);
        p4_cursor_close(c);
        if (i) t.push_back(ms(t0));
    }
    std::sort(t.begin(), t.end());
    return t[t.size() / 2];
}

}  // namespace

P4SQL_TEST(bench_sql_surface_per_row) {
    if (env("P4SQL_BENCH") != "1" && env("PS_P4SQL_BENCH") != "1") {   // PS_: the wasm host passes PS_* only
        std::printf("    SKIP: P4SQL_BENCH not set\n");
        return;
    }
    Harness h;
    const std::vector<uint8_t> omm = readFile(vectorDir() + "/OMM.bfbs");
    const std::vector<uint8_t> cat = readFile(vectorDir() + "/CAT.bfbs");
    p4fake::Type& ot = h.addType("OMM", "$OMM", omm, 400000);
    ot.rules = "col 0 u64pos:NORAD_CAT_ID\ncol 1 str:OBJECT_ID\n";
    h.addType("CAT", "$CAT", cat, 10000);
    // 1,000 distinct OMM records (~400 bytes) over 400,000 seqs; 10,000 CATs.
    std::vector<std::vector<uint8_t>> ommRecs;
    for (int i = 0; i < 1000; i++)
        ommRecs.push_back(buildRecord(omm,
                                      "{\"OBJECT_NAME\":\"STARLINK-" + std::to_string(i) + "\",\"OBJECT_ID\":\"2020-0" +
                                          std::to_string(i) + "A\",\"NORAD_CAT_ID\":" + std::to_string(25000 + i) +
                                          ",\"EPOCH\":\"2026-09-20T12:00:00.000000\",\"CREATION_DATE\":\"2026-09-20T13:00:00\","
                                          "\"MEAN_MOTION\":15.06,\"ECCENTRICITY\":0.0001,\"INCLINATION\":53.05,"
                                          "\"RA_OF_ASC_NODE\":120.5,\"ARG_OF_PERICENTER\":90.1,\"MEAN_ANOMALY\":270.2,"
                                          "\"BSTAR\":0.0001,\"MEAN_MOTION_DOT\":0.00001,\"ELEMENT_SET_NO\":999,"
                                          "\"REV_AT_EPOCH\":12345,\"USER_DEFINED_EPOCH_TIMESTAMP\":1789900000.0,"
                                          "\"CLASSIFICATION_TYPE\":\"U\",\"ORIGINATOR\":\"18 SPCS\",\"CENTER_NAME\":\"EARTH\"}",
                                      false));
    for (int64_t s = 1; s <= 400000; s++) {
        p4fake::Rec r;
        r.seq = s;
        r.cid = "bafkreibench" + std::to_string(s);
        r.producer = "source_celestrak";
        r.peer = "12D3KooWExample";
        r.ts = 1790000000 + s;
        r.data = ommRecs[size_t(s % 1000)];
        r.tags.push_back(p4fake::Tag{"space-data-network-02", "celestrak-gp", "", "b1", "", "", "", r.ts});
        r.hasCol0 = true;
        r.col0 = 25000 + s % 1000;
        h.put("OMM", std::move(r));
    }
    const std::vector<uint8_t> catRec =
        buildRecord(cat, "{\"OBJECT_NAME\":\"CAT\",\"NORAD_CAT_ID\":40000,\"OBJECT_ID\":\"2000-001A\"}", false);
    for (int64_t s = 1; s <= 10000; s++) {
        p4fake::Rec r;
        r.seq = s;
        r.cid = "bafkreicat" + std::to_string(s);
        r.ts = 1790000000 + s;
        r.data = catRec;
        r.tags.push_back(p4fake::Tag{"space-data-network-02", "celestrak-satcat", "", "b1", "", "", "", r.ts});
        h.put("CAT", std::move(r));
    }
    size_t bytes = 0;
    const uint32_t sb = P4_SLOT_RAW | P4_SLOT_SANDBOX;
    double t = timeIt(h, "SELECT _data FROM \"CAT@celestrak-satcat\"", sb, {}, 31, &bytes);
    const double base = directFrames(h, "CAT", "celestrak-satcat", 10000, 31);
    report("r17_cat_10k_frames_sql_ms", t, "ms");
    report("r17_cat_10k_frames_reader_only_ms", base, "ms");
    report("r17_cat_per_frame_sql_layer_us", (t - base) * 1000.0 / 10000, "us");
    t = timeIt(h, "SELECT _data FROM \"OMM@celestrak-gp\"", sb, {}, 7, &bytes);
    const double obase = directFrames(h, "OMM", "celestrak-gp", 400000, 7);
    report("r17_omm_400k_frames_sql_ms", t, "ms");
    report("r17_omm_400k_frames_reader_only_ms", obase, "ms");
    report("r17_omm_per_frame_sql_layer_us", (t - obase) * 1000.0 / 400000, "us");
    t = timeIt(h, "SELECT COUNT(*) FROM OMM", P4_SLOT_SANDBOX, {}, 7, &bytes);
    report("r19_omm_count_400k_p4sql_ms", t, "ms");
    t = timeIt(h, "SELECT _data FROM OMM WHERE NORAD_CAT_ID = ?1", sb, {cInt(25544)}, 7, &bytes);
    report("r19_omm_norad_eq_p4sql_ms (reader examines the window: fake)", t, "ms");
    t = timeIt(h,
               "SELECT _data FROM (SELECT _data, ROW_NUMBER() OVER (PARTITION BY NORAD_CAT_ID ORDER BY "
               "ABS(USER_DEFINED_EPOCH_TIMESTAMP - ?2)) rn FROM OMM WHERE (?1 = '' OR _source = ?1)) WHERE rn = 1 LIMIT ?3",
               P4_SLOT_RAW, {cText("OMM@celestrak-gp"), cReal(1789371001.0), cInt(50000)}, 5, &bytes);
    report("r18_nearest_400k_p4sql_ms", t, "ms");
}

#endif  // FLATSQL_P4SQL_FAKE
