// Third review of stack #651 (unlimited SQL visibility): SQL over the real
// engine. (1) Records written BEFORE the layout-2 promotion, checkpointed
// and in the tail, are reachable by COUNT, key, key + epoch, CID, sequence
// range, ordered scan and full text, before and after a checkpoint and a
// restart. (2) Many concurrent long scans with a row-order cache budget far
// below their working set: every statement returns its complete answer or an
// explicit status, never a short result; the cache never exceeds its budget;
// every pin is released once the statements end, including early closes and
// statements stopped by a result cap.
#include "QRP/main_generated.h"

#include <atomic>
#include <thread>

#include "flatsql/p4/p4_reader.h"
#include "p4/p4_test.h"
#include "internal.h"
#include "checkpoint.h"

namespace flatsql { namespace p4 { P4Engine* currentEngine(); } }

using namespace p4t;

namespace {

const char* kReviewSrc = "celestrak-gp";
constexpr int kObjects = 300;

// Record i (1-based): object 30000 + i % 300, epoch 1700000000 + 600 i.
Batch reviewBatch(int from, int n) {
    Batch b;
    b.type = "OMM";
    b.peer = "12D3KooWExample";
    b.tags.push_back(Tag{"space-data-network-02", kReviewSrc, "", "review651-" + std::to_string(from), "", "", ""});
    b.at = 1790000000;
    for (int i = from; i < from + n; i++) {
        In r;
        const uint32_t norad = 30000 + uint32_t(i % kObjects);
        r.frame = ommFrame(norad, "2001-" + std::to_string(norad % 1000) + "A", isoTime(1700000000 + int64_t(i) * 600));
        r.ts = 1790000000 + i;
        b.recs.push_back(std::move(r));
    }
    return b;
}

std::vector<uint8_t> reviewSqlReq(const std::string& text, const std::vector<int64_t>& params = {}) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<QRPValue>> values;
    for (int64_t v : params) values.push_back(CreateQRPValue(builder, qrpValueKind_Int, false, v));
    auto q = builder.CreateString(text);
    auto bindings = builder.CreateVector(values);
    QRPBuilder b(builder);
    b.add_SQL(q);
    b.add_PARAMETERS(bindings);
    b.add_REPRESENTATION(qrpRepresentation_FlatBuffer);
    FinishSizePrefixedQRPBuffer(builder, b.Finish());
    return std::vector<uint8_t>(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
}

Result reviewSql(const std::string& text, const std::vector<int64_t>& params = {}, const CallOpts& o = CallOpts()) {
    return call(FLATSQL_OPC_SQL, reviewSqlReq(text, params), o);
}

// Full text trails ingest in the background and answers BUSY until it has
// caught up: wait for it, a bounded time.
Result reviewSqlUntilReady(const std::string& text) {
    for (int i = 0;; i++) {
        Result r = reviewSql(text);
        if (r.status != FLATSQL_E_BUSY || i > 1200) return r;
        flatsql::ps::sleepNs(50000000);
    }
}

void putReview(int from, int n) {
    for (int at = from; at < from + n; at += 500) {
        const Result r = put(reviewBatch(at, std::min(500, from + n - at)));
        REQUIRE(r.status == FLATSQL_OK, r.err);
    }
}

int expectedForObject(int total, int k) {
    int n = 0;
    for (int i = 1; i <= total; i++) n += i % kObjects == k;
    return n;
}

struct ReviewEngine {
    std::string root;
    ~ReviewEngine() {
        closeEngine();
        removeTree(root);
    }
};

}  // namespace

FLATSQL_TEST(t_sql_review651_pre_promotion_records_complete) {
    ReviewEngine guard{scratchDir("review651_pre_promotion")};
    const std::string& root = guard.root;
    REQUIRE(openEngine(root) == FLATSQL_OK, "open");
    auto type = ommType();
    type.a18 = 100;
    type.fullText = true;
    REQUIRE(registerType(type) == FLATSQL_OK, "bounded registration");
    putReview(1, 3000);
    REQUIRE(closeEngine() == FLATSQL_OK, "checkpoint the bounded records");
    REQUIRE(openEngine(root) == FLATSQL_OK, "reopen bounded");
    putReview(3001, 500);  // a tail since the checkpoint, still before the promotion
    {
        const Result bounded = reviewSql("SELECT COUNT(*) AS n FROM OMM");
        REQUIRE(bounded.status == FLATSQL_OK, bounded.err);
        CHECK_EQ(bounded.i(0, "n"), int64_t(100), "bounded relation before the promotion");
    }
    type.a18 = 0;
    REQUIRE(registerType(type) == FLATSQL_OK, "promote to unlimited");
    const int total = 3500;
    for (int pass = 0; pass < 3; pass++) {
        // pass 0: right after the promotion; 1: after a checkpoint and a
        // restart; 2: after more records and another restart.
        const int n = pass < 2 ? total : total + 250;
        const Result count = reviewSql("SELECT COUNT(*) AS n FROM OMM");
        REQUIRE(count.status == FLATSQL_OK, count.err);
        CHECK_EQ(count.i(0, "n"), int64_t(n), "COUNT(*) is every record");
        const Result cidCount = reviewSql("SELECT COUNT(_cid) AS n FROM OMM");
        CHECK_EQ(cidCount.i(0, "n"), int64_t(n), "COUNT(_cid) is every record");
        for (const char* order : {"ASC", "DESC"}) {
            const Result scan = reviewSql(std::string("SELECT _seq FROM OMM ORDER BY _seq ") + order);
            REQUIRE(scan.status == FLATSQL_OK, scan.err);
            REQUIRE(scan.rows.size() == size_t(n), "ordered scan is complete");
            bool exact = true;
            for (size_t i = 0; i < scan.rows.size(); i++)
                exact = exact && scan.i(i, "_seq") == (order[0] == 'A' ? int64_t(i + 1) : int64_t(n - i));
            CHECK(exact, "ordered scan has no gap or duplicate");
        }
        for (int k : {0, 1, 150, 299}) {
            const Result byKey = reviewSql("SELECT _seq FROM OMM WHERE NORAD_CAT_ID = ?1", {30000 + k});
            REQUIRE(byKey.status == FLATSQL_OK, byKey.err);
            CHECK_EQ(byKey.rows.size(), size_t(expectedForObject(n, k)), "every record of an object by key");
        }
        {
            // The oldest records of one object by key and epoch: i = 1, 301, 601, 901.
            const Result window = reviewSql("SELECT _seq FROM OMM WHERE NORAD_CAT_ID = ?1 AND _epoch >= ?2 AND _epoch <= ?3 ORDER BY _seq",
                                            {30001, 1700000000, 1700000000 + 1000 * 600});
            REQUIRE(window.status == FLATSQL_OK, window.err);
            REQUIRE(window.rows.size() == 4, "oldest epoch window of one object");
            CHECK_EQ(window.i(0, "_seq"), int64_t(1), "the first record");
            CHECK_EQ(window.i(3, "_seq"), int64_t(901), "the fourth record");
        }
        for (int i : {1, 2, 1500, 3001}) {
            const auto b = reviewBatch(i, 1);
            uint8_t cid[36];
            cidOf(b.recs[0].frame, cid);
            const Result byCid = reviewSql("SELECT _seq, _data FROM OMM WHERE _cid = '" + cidText(cid) + "'");
            REQUIRE(byCid.status == FLATSQL_OK, byCid.err);
            REQUIRE(byCid.rows.size() == 1, "a record written before the promotion by CID");
            CHECK_EQ(byCid.i(0, "_seq"), int64_t(i), "its sequence");
            const auto& frame = b.recs[0].frame;
            CHECK(byCid.s(0, "_data") == std::string(reinterpret_cast<const char*>(frame.data() + 4), frame.size() - 4),
                  "its exact bytes");
        }
        {
            const Result range = reviewSql("SELECT _seq FROM OMM WHERE _seq > 10 AND _seq <= 60 ORDER BY _seq");
            REQUIRE(range.status == FLATSQL_OK, range.err);
            CHECK_EQ(range.rows.size(), size_t(50), "an old sequence range");
        }
        {
            const Result text = reviewSqlUntilReady("SELECT _seq FROM OMM WHERE _search MATCH '30007'");
            REQUIRE(text.status == FLATSQL_OK, text.err);
            CHECK_EQ(text.rows.size(), size_t(expectedForObject(n, 7)), "full text finds every record of an object");
        }
        if (pass == 0) {
            REQUIRE(closeEngine() == FLATSQL_OK, "checkpoint after the promotion");
            REQUIRE(openEngine(root) == FLATSQL_OK, "restart promoted");
        } else if (pass == 1) {
            putReview(total + 1, 250);
            REQUIRE(closeEngine() == FLATSQL_OK, "checkpoint again");
            REQUIRE(openEngine(root) == FLATSQL_OK, "restart again");
        }
    }
}

// COUNT(*) of a large unlimited relation while the type ingests: the
// metadata count answers only when no write is in flight (p4_type_rows
// returns BUSY otherwise) and the statement then counts by scanning every
// row, which an examined-rows budget (the public sandbox's is 1,000,000)
// refuses. A bounded relation's COUNT always answers from metadata.
// Characterization: prints how often each answer occurs.
FLATSQL_TEST(t_sql_review651_count_while_ingesting) {
    ReviewEngine guard{scratchDir("review651_count_ingest")};
    REQUIRE(openEngine(guard.root) == FLATSQL_OK, "open");
    auto type = ommType();
    type.a18 = 0;
    REQUIRE(registerType(type) == FLATSQL_OK, "unlimited registration");
    putReview(1, 4000);
    std::atomic<bool> writing{true};
    std::atomic<int> written{4000};
    std::thread writer([&] {
        int next = 4001;
        while (writing.load()) {
            if (put(reviewBatch(next, 25)).status == FLATSQL_OK) { next += 25; written = next - 1; }
        }
    });
    int metadata = 0, budget = 0, other = 0;
    CallOpts o;
    o.maxRowsExamined = 1000;  // far below the relation: only a metadata count fits
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < until) {
        const int before = written.load();
        const Result r = reviewSql("SELECT COUNT(*) AS n FROM OMM", {}, o);
        if (r.status == FLATSQL_OK && r.i(0, "n") >= before) metadata++;
        else if (r.status == FLATSQL_E_BUDGET) budget++;
        else other++;
    }
    writing = false;
    writer.join();
    std::printf("  COUNT(*) while ingesting (examined-rows budget 1,000): %d answered, %d budget errors, %d other; %d records at the end\n",
                metadata, budget, other, written.load());
    CHECK_EQ(other, 0, "every COUNT answers or fails explicitly");
    const Result idle = reviewSql("SELECT COUNT(*) AS n FROM OMM", {}, o);
    CHECK_EQ(idle.status, FLATSQL_OK, idle.err);
    CHECK_EQ(idle.i(0, "n"), int64_t(written.load()), "idle COUNT is exact");
}

FLATSQL_TEST(t_sql_review651_concurrent_scans_tiny_cache) {
    namespace p4 = flatsql::p4;
    ReviewEngine guard{scratchDir("review651_concurrent_scans")};
    const std::string& root = guard.root;
    REQUIRE(openEngine(root) == FLATSQL_OK, "open");
    auto type = ommType();
    type.a18 = 0;
    REQUIRE(registerType(type) == FLATSQL_OK, "unlimited registration");
    const int total = 6000;
    putReview(1, total);
    REQUIRE(closeEngine() == FLATSQL_OK, "checkpoint: row orders on disk");
    REQUIRE(openEngine(root) == FLATSQL_OK, "reopen");
    auto* e = p4::currentEngine();
    for (int phase = 0; phase < 3; phase++) {
        // 0: 150 KiB; 1: 512 KiB; 2: 512 KiB with all but 16 KiB held by
        // another pin, so concurrent page loads contend for admission.
        const size_t limit = phase == 0 ? size_t(150) << 10 : size_t(512) << 10;
        e->rowCache->trim(std::chrono::seconds(0));
        e->rowCache->configure(limit);
        // Let the checkpoint worker verify the row orders under this budget.
        bool served = false;
        for (int i = 0; i < 200 && !served; i++) {
            const Result warm = reviewSql("SELECT _seq FROM OMM ORDER BY _seq LIMIT 3");
            REQUIRE(warm.status == FLATSQL_OK || warm.status == FLATSQL_E_CACHE_BUDGET, warm.err);
            auto* t = e->findType("OMM");
            std::lock_guard<std::mutex> g(t->mu);
            for (auto& f : t->feeds)
                if (f->created && !f->fsg[0].expired()) served = true;
            if (!served) flatsql::ps::sleepNs(20000000);
        }
        CHECK(served, "the row-order path serves scans under the budget");
        std::shared_ptr<const int> pressure;
        if (phase == 2) {
            const size_t used = e->rowCache->used();
            REQUIRE(used + (size_t(48) << 10) < limit, "room for the pressure pin");
            pressure = e->rowCache->load<int>("test/review651-pressure", limit - used - (size_t(16) << 10) - 1024,
                                              [] { return std::make_shared<const int>(1); });
            REQUIRE(pressure, "pressure pin admitted");
        }
        std::atomic<bool> running{true};
        std::atomic<size_t> maxUsed{0};
        std::thread sampler([&] {
            while (running.load()) {
                const size_t u = e->rowCache->used();
                size_t m = maxUsed.load();
                while (u > m && !maxUsed.compare_exchange_weak(m, u)) {}
                std::this_thread::yield();
            }
        });
        std::atomic<int> complete{0}, explicitStatus{0}, wrong{0};
        std::mutex wrongMu;
        std::string firstWrong;
        auto bad = [&](const std::string& what) {
            wrong++;
            std::lock_guard<std::mutex> g(wrongMu);
            if (firstWrong.empty()) firstWrong = what;
        };
        const int threads = 24, rounds = 6;
        std::vector<std::thread> th;
        for (int w = 0; w < threads; w++)
            th.emplace_back([&, w] {
                for (int r = 0; r < rounds; r++) {
                    const int op = (w + r) % 6;
                    Result res;
                    CallOpts o;
                    size_t want = 0;
                    if (op == 0) { res = reviewSql("SELECT _seq FROM OMM ORDER BY _seq"); want = total; }
                    else if (op == 1) { res = reviewSql("SELECT _seq FROM OMM ORDER BY _seq DESC"); want = total; }
                    else if (op == 2) { res = reviewSql("SELECT _seq FROM OMM ORDER BY _seq LIMIT 7"); want = 7; }
                    else if (op == 3) { o.maxResultRows = 100; res = reviewSql("SELECT _seq FROM OMM ORDER BY _seq", {}, o); }
                    else if (op == 4) {
                        const int k = (w * 7 + r) % kObjects;
                        res = reviewSql("SELECT _seq FROM OMM WHERE NORAD_CAT_ID = ?1", {30000 + k});
                        want = size_t(expectedForObject(total, k));
                    } else {
                        const int64_t a = (w * 131 + r * 17) % (total - 500);
                        res = reviewSql("SELECT _seq FROM OMM WHERE _seq > ?1 AND _seq <= ?2 ORDER BY _seq", {a, a + 500});
                        want = 500;
                    }
                    if (op == 3) {
                        if (res.status == FLATSQL_E_BUDGET || res.status == FLATSQL_E_CACHE_BUDGET) explicitStatus++;
                        else bad("result cap: status " + std::to_string(res.status) + " rows " + std::to_string(res.rows.size()));
                        continue;
                    }
                    if (res.status == FLATSQL_E_CACHE_BUDGET) { explicitStatus++; continue; }
                    if (res.status != FLATSQL_OK) { bad("op " + std::to_string(op) + " status " + std::to_string(res.status) + ": " + res.err); continue; }
                    if (res.rows.size() != want) { bad("op " + std::to_string(op) + " short result " + std::to_string(res.rows.size()) + " of " + std::to_string(want)); continue; }
                    complete++;
                }
            });
        for (auto& t : th) t.join();
        running = false;
        sampler.join();
        std::printf("  budget %zu KiB%s: %d complete, %d explicit statuses (%d of them result caps), %d wrong, max cache bytes %zu\n",
                    limit >> 10, phase == 2 ? " minus a pressure pin" : "", complete.load(), explicitStatus.load(),
                    threads * rounds / 6, wrong.load(), maxUsed.load());
        CHECK_EQ(wrong.load(), 0, firstWrong);
        if (phase < 2) CHECK(complete.load() > 0, "some statements complete under the budget");
        CHECK(maxUsed.load() <= limit, "the cache never exceeds its budget");
        pressure.reset();
        // A row-order verification the scans queued can still be loading on
        // the checkpoint worker (its reservation is not a pin): wait for it,
        // a bounded time. A leaked pin keeps its bytes for ever.
        size_t left = 0;
        for (int i = 0; i < 500; i++) {
            e->rowCache->trim(std::chrono::seconds(0));
            if ((left = e->rowCache->used()) == 0) break;
            flatsql::ps::sleepNs(10000000);
        }
        CHECK_EQ(left, size_t(0), "every pin released after the statements end");
    }
    e->rowCache->configure(size_t(32) << 20);
}
