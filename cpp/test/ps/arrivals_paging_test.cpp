// Arrivals paging (T2 acceptance #6, #7, #8; A14, A15, A16):
//   #6 datasync v1 pages over arrivals while 8 writers produce: MaxRowID is
//      the published gseq_hi at the first page; gseq strictly increases
//      within and across pages; no gseq <= MaxRowID appears after the
//      snapshot (checked against the final arrivals, the oracle); the union
//      of pages is the FIRST-live set at the snapshot; a CID's gseq never
//      changes.
//   #7 offset paging: page 10,000 of 100 rows (OFFSET 999,900) in gseq order
//      examines <= 2 x 100 rows plus fence reads; pages equal the reference
//      under scattered deaths, ascending and descending.
//   #8 a full sync of a 99%-dead history examines <= 2 x (live rows + fence
//      reads).
#include <algorithm>
#include <atomic>
#include <map>
#include <random>
#include <set>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {

std::vector<uint8_t> reconcilePayload(const std::string& provider, const std::string& source,
                                      const std::string& keep) {
    std::vector<uint8_t> out;
    for (const std::string* s : {&provider, &source, &keep}) {
        const size_t at = out.size();
        out.resize(at + 2 + s->size());
        putU16(out.data() + at, uint16_t(s->size()));
        std::memcpy(out.data() + at + 2, s->data(), s->size());
    }
    return out;
}

std::vector<uint8_t> rec(int p, int i) {
    char epoch[40];
    std::snprintf(epoch, sizeof(epoch), "2026-04-%02dT%02d:%02d:%02dZ", 1 + (i / 86400) % 28, (i / 3600) % 24,
                  (i / 60) % 60, i % 60);
    return ommRecord(uint32_t(p * 10000000 + i + 1), "A" + std::to_string(p) + "-" + std::to_string(i), epoch, 15.0);
}

// Loads `n` records into `parts` partitions; record k goes to partition k %
// parts with tag batch "keep" when keep(k), else "drop". Then RECONCILE
// (keep="keep") kills every "drop" record: the type sees them die.
void loadWithDeaths(Store& s, int parts, int n, const std::function<bool(int)>& keep, bool reconcileNow,
                    std::vector<uint32_t>* pids) {
    s.registerTypes({&ommType()});
    std::vector<std::unique_ptr<Producer>> prods;
    for (int p = 0; p < parts; p++) {
        pids->push_back(s.partition("ap" + std::to_string(p), ommType()));
        prods.emplace_back(new Producer(s.e.get(), pids->back()));
    }
    std::vector<uint64_t> last(size_t(parts), 0);
    const auto attrKeep = buildRecordAttr("x", "prov", "src", "keep");
    const auto attrDrop = buildRecordAttr("x", "prov", "src", "drop");
    for (int k = 0; k < n; k++) {
        const int p = k % parts;
        last[size_t(p)] = send(s.e.get(), *prods[size_t(p)], rec(p, k), keep(k) ? attrKeep : attrDrop, 1780000000000ll + k);
    }
    for (int p = 0; p < parts; p++)
        if (last[size_t(p)]) CHECK_EQ(prods[size_t(p)]->waitAcked(last[size_t(p)], 600000000000ull), 0);
    CHECK(waitLabeledEngine(s.e.get(), *pids, 600000000000ull));
    if (reconcileNow) {
        for (int p = 0; p < parts; p++) {
            const auto payload = reconcilePayload("prov", "src", "keep");
            uint64_t rseq = 0;
            prods[size_t(p)]->enqueue(kEntReconcile, 0, 0, nullptr, nullptr, 0, payload.data(), uint32_t(payload.size()),
                                      &rseq);
            CHECK_EQ(prods[size_t(p)]->waitAcked(rseq, 600000000000ull), 0);
        }
        CHECK(waitLabeledEngine(s.e.get(), *pids, 600000000000ull));
    }
    CHECK(waitTypeVisible(s.fs.get(), s.root, ommType().fid, *pids, 600000000000ull));
}

}  // namespace

PS_TEST(arrivals_paging_while_writers_produce_T2_6) {
    Store s(false, 8, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const int parts = 32;
    std::vector<uint32_t> pids;
    for (int p = 0; p < parts; p++) pids.push_back(s.partition("wp" + std::to_string(p), ommType()));
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> sent{0};
    // 8 producer threads; half the records are REPEAT copies of another
    // producer's records (same bytes).
    std::vector<std::thread> producers;
    for (int t = 0; t < 8; t++)
        producers.emplace_back([&, t] {
            std::mt19937_64 rng(uint64_t(t) + 100);
            std::vector<std::unique_ptr<Producer>> ps;
            for (int p = t; p < parts; p += 8) ps.emplace_back(new Producer(s.e.get(), pids[size_t(p)]));
            const auto attr = buildRecordAttr("w", "prov", "src", "b1");
            int i = 0;
            while (!stop.load()) {
                for (auto& pr : ps) {
                    const int base = (rng() % 2) ? t : int(rng() % 8);
                    send(s.e.get(), *pr, rec(base, i), attr, 1780000000000ll + i, true);
                    sent++;
                }
                i++;
                if (i % 64 == 0) sleepNs(200000);
            }
        });
    Reader bulk(s, LaneClass::Bulk, 2);
    Reader inter(s, LaneClass::Interactive, 2);
    REQUIRE(bulk.inst && inter.inst);
    struct Sync {
        int64_t maxRowId;
        std::vector<int64_t> gseqs;
        std::vector<std::string> cids;
    };
    std::vector<Sync> syncs;
    uint64_t pages = 0, violations = 0;
    const uint64_t until = monoNs() + uint64_t(argInt("seconds", 4)) * 1000000000ull;
    sleepNs(300000000);
    while (monoNs() < until) {
        Rows t = inter.q("SELECT gseq_hi FROM flatsql_types WHERE type = 'OMM'");
        REQUIRE(t.status == 0 && t.rows.size() == 1);
        Sync sy;
        sy.maxRowId = t.i(0, 0);
        int64_t after = 0;
        for (;;) {
            Rows pg = inter.q("SELECT _gseq, _cid FROM OMM WHERE _gseq > ? AND _gseq <= ? ORDER BY _gseq LIMIT 500",
                              {Param::i64(after), Param::i64(sy.maxRowId)});
            if (pg.status != 0) {
                violations++;
                break;
            }
            pages++;
            for (size_t i = 0; i < pg.rows.size(); i++) {
                const int64_t g = pg.i(i, 0);
                if (g <= after || g > sy.maxRowId) violations++;  // strictly increasing, bounded
                after = g;
                sy.gseqs.push_back(g);
                sy.cids.push_back(pg.s(i, 1));
            }
            if (pg.rows.size() < 500) break;  // A15 page fill: only the last page is short
        }
        syncs.push_back(std::move(sy));
    }
    stop = true;
    for (auto& t : producers) t.join();
    CHECK(waitLabeledEngine(s.e.get(), pids, 60000000000ull));
    CHECK(waitTypeVisible(s.fs.get(), s.root, ommType().fid, pids, 60000000000ull));
    // Oracle: the final arrivals (no deaths: every entry is FIRST-live).
    Rows all = bulk.q("SELECT _gseq, _cid FROM OMM ORDER BY _gseq");
    REQUIRE(all.status == 0);
    std::map<std::string, int64_t> finalGseq;
    for (size_t i = 0; i < all.rows.size(); i++) finalGseq[all.s(i, 1)] = all.i(i, 0);
    uint64_t unionMismatch = 0, gseqChanged = 0;
    for (const Sync& sy : syncs) {
        std::vector<int64_t> expect;
        for (size_t i = 0; i < all.rows.size(); i++)
            if (all.i(i, 0) <= sy.maxRowId) expect.push_back(all.i(i, 0));
        if (expect != sy.gseqs) unionMismatch++;
        for (size_t i = 0; i < sy.cids.size(); i++)
            if (finalGseq[sy.cids[i]] != sy.gseqs[i]) gseqChanged++;
    }
    report("syncs", double(syncs.size()), "syncs");
    report("pages", double(pages), "pages");
    report("records_sent", double(sent.load()), "records");
    report("final_first_entries", double(all.rows.size()), "entries");
    CHECK(syncs.size() >= 3);
    CHECK_EQ(violations, uint64_t(0));
    CHECK_EQ(unionMismatch, uint64_t(0));
    CHECK_EQ(gseqChanged, uint64_t(0));
    s.close();
}

namespace {
void offsetPagingWithDeaths() {
    Store s(false, 2, true);
    REQUIRE(s.open() == 0);
    std::vector<uint32_t> pids;
    std::mt19937_64 rng(5);
    std::vector<bool> keepv(20000);
    for (auto&& k : keepv) k = rng() % 10 < 7;  // 30% die, scattered
    loadWithDeaths(s, 4, 20000, [&](int k) { return bool(keepv[size_t(k)]); }, true, &pids);
    Reader bulk(s, LaneClass::Bulk, 1);
    Reader inter(s, LaneClass::Interactive, 1);
    Rows all = bulk.q("SELECT _gseq, _cid FROM OMM ORDER BY _gseq");
    REQUIRE(all.status == 0);
    int live = 0;
    for (bool k : keepv) live += k;
    CHECK_EQ(all.rows.size(), size_t(live));
    for (int trial = 0; trial < 60; trial++) {
        const bool desc = trial % 2;
        const size_t off = size_t(rng() % (all.rows.size() + 50));
        Rows pg = inter.q(std::string("SELECT _gseq FROM OMM ORDER BY _gseq ") + (desc ? "DESC " : "") + "LIMIT 100 OFFSET " +
                          std::to_string(off));
        CHECK_EQ(pg.status, 0);
        std::vector<int64_t> expect;
        for (size_t i = 0; i < 100; i++) {
            const size_t k = off + i;
            if (k >= all.rows.size()) break;
            expect.push_back(desc ? all.i(all.rows.size() - 1 - k, 0) : all.i(k, 0));
        }
        std::vector<int64_t> got;
        for (auto& r : pg.rows) got.push_back(r[0].i);
        CHECK(got == expect);
        CHECK(pg.outcome.rowsExamined <= 200);
    }
    s.close();
}
}  // namespace

PS_TEST(offset_paging_matches_reference_with_deaths) { offsetPagingWithDeaths(); }

// The same with two fences per page: GONE counts from L1 fences cross fence
// pages (FenceView).
PS_TEST(offset_paging_matches_reference_small_fence_pages) {
    gReaderFencePage = 2;
    offsetPagingWithDeaths();
    gReaderFencePage = 0;
}

namespace {
void runOffsetPage(int records) {
    Store s(false, 8, true);
    s.cfg.poolBytes = 256ull << 20;
    REQUIRE(s.open() == 0);
    std::vector<uint32_t> pids;
    const uint64_t t0 = monoNs();
    loadWithDeaths(s, 16, records, [](int k) { return k % 10 != 3; }, true, &pids);  // 10% scattered deaths
    report("load_seconds", double(monoNs() - t0) / 1e9, "s");
    Reader inter(s, LaneClass::Interactive, 1);
    Reader bulk(s, LaneClass::Bulk, 1);
    Rows cnt = inter.q("SELECT first_live_count, arrivals FROM flatsql_types WHERE type = 'OMM'");
    REQUIRE(cnt.status == 0 && cnt.rows.size() == 1);
    const int64_t live = cnt.i(0, 0);
    report("type_records", double(records), "records");
    report("type_live", double(live), "records");
    // Page 10,000 of 100 rows (scaled to the type size for small runs).
    const int64_t pageNo = std::min<int64_t>(10000, live / 100 - 1);
    const int64_t offset = (pageNo - 1) * 100;
    Rows pg = inter.q("SELECT _gseq, _cid FROM OMM ORDER BY _gseq LIMIT 100 OFFSET " + std::to_string(offset));
    CHECK_EQ(pg.status, 0);
    CHECK_EQ(pg.rows.size(), size_t(100));
    report("page_number", double(pageNo), "");
    report("page_rows_examined", double(pg.outcome.rowsExamined), "rows");
    report("page_index_entries", double(pg.outcome.indexEntries), "entries");
    report("page_ms", double(pg.outcome.runNs) / 1e6, "ms");
    CHECK(pg.outcome.rowsExamined <= 200);
    report("page_fence_reads", double(pg.outcome.fenceReads), "reads");
    // The page equals the reference: the same rows from a full bulk scan.
    Rows ref = bulk.q("SELECT _gseq FROM OMM ORDER BY _gseq");
    CHECK_EQ(ref.status, 0);
    REQUIRE(ref.rows.size() == size_t(live));
    for (size_t i = 0; i < pg.rows.size(); i++) CHECK(ref.i(size_t(offset) + i, 0) == pg.i(i, 0));
    s.close();
}

// Full sync of a type whose history is 99% dead.
void runDeadHistory(bool clustered, int records) {
    Store s(false, 4, true);
    s.cfg.poolBytes = 256ull << 20;
    REQUIRE(s.open() == 0);
    std::vector<uint32_t> pids;
    auto keep = [&](int k) { return clustered ? k >= records - records / 100 : k % 100 == 57; };
    loadWithDeaths(s, 8, records, keep, true, &pids);
    Reader inter(s, LaneClass::Interactive, 1);
    uint64_t rows = 0, examined = 0, entries = 0, pages = 0, fences = 0;
    int64_t after = 0;
    Rows t = inter.q("SELECT gseq_hi, first_live_count FROM flatsql_types WHERE type = 'OMM'");
    REQUIRE(t.status == 0 && t.rows.size() == 1);
    const int64_t maxRowId = t.i(0, 0);
    for (;;) {
        Rows pg = inter.q("SELECT _gseq FROM OMM WHERE _gseq > ? AND _gseq <= ? ORDER BY _gseq LIMIT 1000",
                          {Param::i64(after), Param::i64(maxRowId)});
        REQUIRE(pg.status == 0);
        pages++;
        rows += pg.rows.size();
        examined += pg.outcome.rowsExamined;
        entries += pg.outcome.indexEntries;
        fences += pg.outcome.fenceReads;
        if (!pg.rows.empty()) after = pg.i(pg.rows.size() - 1, 0);
        if (pg.rows.size() < 1000) break;
    }
    const char* tag = clustered ? "clustered" : "scattered";
    char key[96];
    std::snprintf(key, sizeof(key), "dead99_%s_live_rows", tag);
    report(key, double(rows), "rows");
    std::snprintf(key, sizeof(key), "dead99_%s_rows_examined", tag);
    report(key, double(examined), "rows");
    std::snprintf(key, sizeof(key), "dead99_%s_arrival_entries_read", tag);
    report(key, double(entries), "entries");
    std::snprintf(key, sizeof(key), "dead99_%s_fence_reads", tag);
    report(key, double(fences), "reads");
    std::snprintf(key, sizeof(key), "dead99_%s_history_entries", tag);
    report(key, double(records), "entries");
    CHECK_EQ(int64_t(rows), t.i(0, 1));
    // #8: rows examined (row reads) and arrival entries examined, each
    // <= 2 x (live rows + fence reads).
    CHECK(examined <= 2 * (rows + fences));
    CHECK(entries <= 2 * (rows + fences));
    s.close();
}
}  // namespace

PS_TEST(offset_paging_page_10000_T2_7) { runOffsetPage(int(argInt("records", 150000))); }
PS_SLOW_TEST(offset_paging_page_10000_T2_7_full) { runOffsetPage(int(argInt("records", 3000000))); }
PS_TEST(dead_history_full_sync_T2_8) {
    runDeadHistory(true, int(argInt("records", 100000)));
    runDeadHistory(false, int(argInt("records", 100000)));
}
