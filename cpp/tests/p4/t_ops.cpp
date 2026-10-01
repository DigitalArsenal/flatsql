// t_typecfg, t_supersede, t_quota, t_rebuild, t_budget, t_group (design gates
// G2-G3, G6; CONTRACT v2 §3.7-§3.8, §6 engine row).
#include <atomic>
#include <cstring>
#include <filesystem>
#include <thread>
#if !defined(__wasm__)
#include <sys/resource.h>
#endif

#include "internal.h"
#include "p4/p4_test.h"

using namespace p4t;
namespace fp = flatsql::p4;

namespace {

TestType typedPnm(const std::string& name) { return pnmLikeType(name); }

std::vector<uint8_t> pnmF(const TestType& t, uint64_t id, size_t pad = 120) {
    return buildFrame(t, {Field::str("FILE_ID", "f" + std::to_string(id)), Field::str("NAME", "n" + std::to_string(id)),
                          Field::raw("BODY", std::vector<uint8_t>(pad, uint8_t(id)))});
}

Batch batchOf(const TestType& t, const std::string& peer, const Tag& tag, uint64_t from, int n) {
    Batch b;
    b.type = t.name;
    b.peer = peer;
    b.tags.push_back(tag);
    b.at = 1790000000;
    for (int i = 0; i < n; i++) {
        In in;
        in.frame = pnmF(t, from + uint64_t(i));
        in.ts = 1790000000 + i;
        b.recs.push_back(std::move(in));
    }
    return b;
}

std::vector<uint8_t> cidVec(const std::vector<uint8_t>& frame) {
    uint8_t c[36];
    cidOf(frame, c);
    return std::vector<uint8_t>(c, c + 36);
}

int64_t headN(const std::string& type, const std::vector<std::pair<uint16_t, std::string>>& lane = {}) {
    TlvW w;
    w.text(1, type);
    for (auto& kv : lane) w.text(kv.first, kv.second);
    Result r = call(P4_OPC_HEAD, w.b);
    return r.rows.size() == 1 ? r.i(0, "n") : -1;
}

int64_t walBytes(const std::string& dir) {
    int64_t n = 0;
    std::error_code ec;
    for (auto& d : std::filesystem::recursive_directory_iterator(dir, ec)) {
        const std::string p = d.path().string();
        if (p.size() > 7 && p.compare(p.size() - 7, 7, ".db-wal") == 0) n += int64_t(std::filesystem::file_size(d.path(), ec));
    }
    return n;
}

}  // namespace

// IQC exposes no epoch (format 1's rule) and its partition is one file. C-5:
// a type with data keeps its epoch rule. The ingest identity dedupes
// (IDENT_DUP).
P4_TEST(t_typecfg_identity) {
    const std::string root = scratchDir("typecfg") + "/fsql4";
    REQUIRE(openEngine(root) == P4_OK, "open");
    REQUIRE(registerType(iqcType()) == P4_OK, "register IQC");
    Batch b;
    b.type = "IQC";
    b.peer = "12D3KooWSigmf";
    b.tags.push_back(Tag{"iqengine", "IQEngine", "https://iqengine.org/x", "b1", "", "", ""});
    b.at = 1790000000;
    In a;
    a.frame = iqcFrame("E1", "2026-01-10T00:00:00Z", 64, 1);
    a.ts = 1790000000;
    a.hasIdent = true;
    std::memset(a.ident, 7, 32);
    In c;
    c.frame = iqcFrame("E2", "2026-03-05T00:00:00Z", 64, 2);
    c.ts = 1790000000;
    c.hasIdent = true;
    std::memset(c.ident, 8, 32);
    b.recs = {a, c};
    Result r = put(b);
    REQUIRE(r.status == P4_OK && r.rows.size() == 2, r.err);
    CHECK(fp::ioExists(root + "/P/IQC/1.db"), "the partition's one file");
    Result g = get("IQC", {cidVec(a.frame), cidVec(c.frame)});
    REQUIRE(g.rows.size() == 2, "both");
    CHECK(g.null(0, "epoch") && g.null(1, "epoch"), "no exposed epoch");
    // The same identity again (different bytes: a re-fetch) is IDENT_DUP.
    Batch b2 = b;
    In a2;
    a2.frame = iqcFrame("E1", "2026-01-10T00:00:00Z", 64, 99);
    a2.ts = 1790000500;
    a2.hasIdent = true;
    std::memset(a2.ident, 7, 32);
    b2.recs = {a2};
    Result d = put(b2);
    REQUIRE(d.rows.size() == 1, d.err);
    CHECK_EQ(d.i(0, "action"), int64_t(P4_ACT_IDENT_DUP), "IDENT_DUP");
    CHECK_EQ(d.i(0, "seq"), r.i(0, "seq"), "the holder's seq");
    CHECK_EQ(get("IQC", {cidVec(a2.frame)}).rows.size(), size_t(0), "the repeat's bytes are not stored");
    // C-5: identical re-registration is a no-op; a changed epoch rule is refused.
    CHECK_EQ(registerType(iqcType()), P4_OK, "identical");
    TestType changed = iqcType();
    changed.rules = "epoch str:RETRIEVED_AT\n";
    CHECK_EQ(registerType(changed), P4_E_FORMAT, "epoch rule change refused");
    TestType widened = iqcType();
    widened.rules = "col 1 str:ENTITY_ID\n";
    CHECK_EQ(registerType(widened), P4_OK, "other rules may change");
    closeEngine();
    // Specs are persisted (C-5): a reopen serves before re-registration.
    REQUIRE(openEngine(root) == P4_OK, "reopen");
    CHECK_EQ(get("IQC", {cidVec(a.frame)}).rows.size(), size_t(1), "served without re-registration");
    closeEngine();
}

// §3.8.7: batch supersede; a lane row goes at 0; an emptied partition keeps
// its one file (C-32).
// §3.7: CAT supersede-on-ingest by (source, object identity).
P4_TEST(t_supersede) {
    const std::string root = scratchDir("supersede") + "/fsql4";
    REQUIRE(openEngine(root) == P4_OK, "open");
    TestType t = typedPnm("PNM");
    REQUIRE(registerType(t) == P4_OK, "register");
    const Tag b1{"prov", "src", "u1", "b1", "", "", ""}, b2{"prov", "src", "u2", "b2", "", "", ""};
    REQUIRE(put(batchOf(t, "peerA", b1, 0, 100)).status == P4_OK, "b1");
    REQUIRE(put(batchOf(t, "peerA", b2, 50, 100)).status == P4_OK, "b2");
    TlvW s;
    s.text(1, "PNM").text(11, "prov").text(12, "src").text(60, "b2").u8(61, 0);
    Result count = call(P4_OPC_SUPERSEDE, s.b);
    REQUIRE(count.status == P4_OK && count.rows.size() == 1, count.err);
    CHECK_EQ(count.i(0, "tags_deleted"), int64_t(100), "count: b1's 100 instances");
    CHECK_EQ(count.i(0, "records_deleted"), int64_t(50), "count: 0..49 keep no tag");
    CHECK_EQ(headN("PNM"), int64_t(150), "count changes nothing");
    TlvW sa;
    sa.text(1, "PNM").text(11, "prov").text(12, "src").text(60, "b2").u8(61, 1);
    Result ap = call(P4_OPC_SUPERSEDE, sa.b);
    REQUIRE(ap.status == P4_OK && ap.rows.size() == 1, ap.err);
    CHECK_EQ(ap.i(0, "tags_deleted"), int64_t(100), "apply: tags");
    CHECK_EQ(ap.i(0, "records_deleted"), int64_t(50), "apply: records");
    CHECK_EQ(headN("PNM"), int64_t(100), "100 records left");
    CHECK_EQ(get("PNM", {cidVec(pnmF(t, 10))}).rows.size(), size_t(0), "a b1-only record is gone");
    CHECK_EQ(get("PNM", {cidVec(pnmF(t, 60))}).rows.size(), size_t(1), "a b2 record stays");
    TlvW s3;
    s3.u8(45, 3).text(1, "PNM");
    Result lanes = call(P4_OPC_SUMMARY, s3.b);
    CHECK_EQ(lanes.rows.size(), size_t(1), "the lane count is back to the live set (b2 only)");
    if (lanes.rows.size() == 1) {
        CHECK(lanes.s(0, "batch") == "b2", "b2");
        CHECK_EQ(lanes.i(0, "records"), int64_t(100), "b2's records");
    }
    // Everything superseded: the file stays, empty.
    TlvW sz;
    sz.text(1, "PNM").text(11, "prov").text(12, "src").text(60, "none").u8(61, 1);
    Result all = call(P4_OPC_SUPERSEDE, sz.b);
    CHECK_EQ(all.i(0, "files_deleted"), int64_t(0), "a partition keeps its file");
    CHECK_EQ(headN("PNM"), int64_t(0), "empty");
    REQUIRE(put(batchOf(t, "peerA", b1, 500, 3)).status == P4_OK, "a later write");
    CHECK(fp::ioExists(root + "/P/PNM/1.db"), "the same file");
    CHECK_EQ(headN("PNM"), int64_t(3), "the later records");
    // CAT supersede on ingest.
    REQUIRE(registerType(catType()) == P4_OK, "register CAT");
    auto cats = [&](const std::string& source, const std::string& batch, const std::string& name) {
        Batch b;
        b.type = "CAT";
        b.peer = "12D3KooWCat";
        b.tags.push_back(Tag{"celestrak", source, "", batch, "", "", ""});
        for (uint32_t n = 1; n <= 10; n++) {
            In in;
            in.frame = catFrame(n, "OBJ" + std::to_string(n), name + std::to_string(n));
            in.ts = 1790000000;
            b.recs.push_back(std::move(in));
        }
        return b;
    };
    REQUIRE(put(cats("satcat-txt", "a", "OLD")).status == P4_OK, "edition a");
    REQUIRE(put(cats("satcat-txt", "b", "NEW")).status == P4_OK, "edition b");
    CHECK_EQ(headN("CAT"), int64_t(10), "one row per object per source");
    CHECK_EQ(get("CAT", {cidVec(catFrame(3, "OBJ3", "OLD3"))}).rows.size(), size_t(0), "the old edition is retired");
    REQUIRE(put(cats("satcat-csv", "a", "CSV")).status == P4_OK, "another source");
    CHECK_EQ(headN("CAT"), int64_t(20), "another source keeps its own rows");
    TlvW rb;
    rb.u32(63, 8);
    Result v = call(P4_OPC_REBUILD, rb.b);
    for (size_t i = 0; i < v.rows.size(); i++) CHECK_EQ(v.i(i, "mismatches"), int64_t(0), "verify after supersede");
    closeEngine();
}

// §3.8.11 (v11): quota deletes the oldest records by arrival, across the
// type's partitions; the counts and the index follow.
P4_TEST(t_quota) {
    const std::string root = scratchDir("quota") + "/fsql4";
    REQUIRE(openEngine(root) == P4_OK, "open");
    REQUIRE(registerType(ommType()) == P4_OK, "register");
    for (int m = 0; m < 3; m++) {
        Batch b;
        b.type = "OMM";
        b.peer = m == 1 ? "12D3KooWQ2" : "12D3KooWQ1";  // two partitions
        b.tags.push_back(Tag{"p", "s", "", "b", "", "", ""});
        for (int i = 0; i < 2000; i++) {
            In in;
            in.frame = ommFrame(uint32_t(m * 10000 + i + 1), "X", "2025-01-15T00:00:00", 15, 300);
            in.ts = 1790000000 + m;
            b.recs.push_back(std::move(in));
        }
        REQUIRE(put(b).status == P4_OK, "batch");
    }
    CHECK_EQ(headN("OMM"), int64_t(6000), "6,000");
    TlvW s4;
    s4.u8(45, 4);
    Result disk = call(P4_OPC_SUMMARY, s4.b);
    REQUIRE(disk.rows.size() == 1, disk.err);
    const int64_t used = disk.i(0, "db_bytes") - disk.i(0, "free_bytes") + disk.i(0, "wal_bytes") + disk.i(0, "index_bytes") +
                         disk.i(0, "journal_bytes");
    // A quota about half of what the store uses: the oldest arrivals go first.
    TlvW q;
    q.u64(62, uint64_t(used / 2));
    Result qg = call(P4_OPC_QUOTA_GC, q.b);
    REQUIRE(qg.status == P4_OK && qg.rows.size() == 1, qg.err);
    const int64_t dropped = qg.i(0, "records_dropped");
    CHECK(dropped > 0, "records dropped");
    CHECK_EQ(qg.i(0, "files_dropped"), int64_t(0), "partitions keep their files");
    CHECK(get("OMM", {cidVec(ommFrame(1, "X", "2025-01-15T00:00:00", 15, 300))}).rows.empty(), "the first arrival is gone");
    CHECK_EQ(get("OMM", {cidVec(ommFrame(22000, "X", "2025-01-15T00:00:00", 15, 300))}).rows.size(), size_t(1),
             "the last arrival stays");
    CHECK_EQ(headN("OMM"), 6000 - dropped, "counts follow");
    closeEngine();
    REQUIRE(openEngine(root) == P4_OK, "reopen");
    CHECK_EQ(headN("OMM"), 6000 - dropped, "after reopen");
    TlvW rb;
    rb.u32(63, 8);
    Result v = call(P4_OPC_REBUILD, rb.b);
    for (size_t i = 0; i < v.rows.size(); i++) CHECK_EQ(v.i(i, "mismatches"), int64_t(0), "verify");
    closeEngine();
}

// §3.8.9: the type index is derived: verify reports damage, REBUILD 2 repairs
// it; REBUILD 1 builds the secondary indexes a migration deferred.
P4_TEST(t_rebuild) {
    const std::string root = scratchDir("rebuild") + "/fsql4";
    EngineOpts o;
    o.flushEntries = 100;
    REQUIRE(openEngine(root, o) == P4_OK, "open");
    TestType t = typedPnm("PNM");
    REQUIRE(registerType(t) == P4_OK, "register");
    REQUIRE(put(batchOf(t, "peerA", Tag{"p", "s", "", "b", "", "", ""}, 0, 500)).status == P4_OK, "put");
    closeEngine();
    // Damage: remove 25 index entries behind the engine's back.
    {
        sqlite3* db = nullptr;
        sqlite3_open((root + "/T/PNM.idx").c_str(), &db);
        CHECK_EQ(sqlite3_exec(db, "DELETE FROM c WHERE cid IN (SELECT cid FROM c LIMIT 25)", nullptr, nullptr, nullptr),
                 SQLITE_OK, "damage");
        sqlite3_close(db);
    }
    REQUIRE(openEngine(root, o) == P4_OK, "reopen");
    TlvW v8;
    v8.u32(63, 8);
    Result bad = call(P4_OPC_REBUILD, v8.b);
    REQUIRE(bad.rows.size() == 1, bad.err);
    CHECK(bad.i(0, "mismatches") > 0, "verify sees the damage");
    TlvW r2;
    r2.u32(63, 2);
    Result fix = call(P4_OPC_REBUILD, r2.b);
    CHECK_EQ(fix.status, P4_OK, fix.err);
    Result good = call(P4_OPC_REBUILD, v8.b);
    if (good.rows.size() == 1) CHECK_EQ(good.i(0, "mismatches"), int64_t(0), "repaired");
    CHECK_EQ(headN("PNM"), int64_t(500), "count");
    // Migrate mode: rows keep their seqs; secondary indexes wait for REBUILD 1.
    TestType m = typedPnm("MIG");
    REQUIRE(registerType(m) == P4_OK, "register MIG");
    Batch b;
    b.type = "MIG";
    b.peer = "peerM";
    b.mode = 1;
    b.tags.push_back(Tag{"p", "s", "", "b", "", "", ""});
    for (int i = 0; i < 50; i++) {
        In in;
        in.frame = pnmF(m, uint64_t(1000 + i));
        in.ts = 1700000000;
        in.seq = 10 + i * 3;
        in.tags.push_back({0, 1700000100});
        b.recs.push_back(std::move(in));
    }
    Result mr = put(b);
    REQUIRE(mr.status == P4_OK && mr.rows.size() == 50, mr.err);
    for (size_t i = 0; i < 50; i++) {
        CHECK_EQ(mr.i(i, "action"), int64_t(P4_ACT_MIGRATED), "MIGRATED");
        CHECK_EQ(mr.i(i, "seq"), int64_t(10 + i * 3), "the legacy seq");
    }
    Result again = put(b);
    for (size_t i = 0; i < again.rows.size(); i++) CHECK_EQ(again.i(i, "action"), int64_t(P4_ACT_MIGRATED), "a rerun is idempotent");
    Batch clash = b;
    clash.recs.resize(1);
    clash.recs[0].frame = pnmF(m, 99999);
    clash.recs[0].seq = 10;
    Result cl = put(clash);
    if (cl.rows.size() == 1) CHECK_EQ(cl.i(0, "reject"), int64_t(P4_REJ_SEQ), "a seq held by another CID");
    auto hasIndex = [&](const char* name) {
        sqlite3* db = nullptr;
        sqlite3_open_v2((root + "/P/MIG/1.db").c_str(), &db, SQLITE_OPEN_READONLY, nullptr);
        sqlite3_stmt* s;
        sqlite3_prepare_v2(db, "SELECT count(*) FROM sqlite_schema WHERE type='index' AND name=?1", -1, &s, nullptr);
        sqlite3_bind_text(s, 1, name, -1, SQLITE_STATIC);
        const bool yes = sqlite3_step(s) == SQLITE_ROW && sqlite3_column_int(s, 0) == 1;
        sqlite3_finalize(s);
        sqlite3_close(db);
        return yes;
    };
    CHECK(!hasIndex("r_w"), "no secondary index after the bulk append");
    TlvW r1;
    r1.u32(63, 1).text(1, "MIG");
    CHECK_EQ(call(P4_OPC_REBUILD, r1.b).status, P4_OK, "REBUILD 1");
    CHECK(hasIndex("r_w") && hasIndex("rl_sid") && hasIndex("r_c"), "indexes built");
    closeEngine();
}

// Caps return P4_E_BUDGET, never a trap; the engine stays usable. 10 and 30
// types x 8 readers stay inside the memory budget.
P4_TEST(t_budget) {
    const std::string root = scratchDir("budget") + "/fsql4";
    EngineOpts o;
    o.readSlots = 64;
    REQUIRE(openEngine(root, o) == P4_OK, "open");
    const int nTypes = int(argInt("types", 30));
    std::vector<TestType> types;
    for (int i = 0; i < nTypes; i++) {
        types.push_back(typedPnm("T" + std::to_string(i)));
        REQUIRE(registerType(types.back()) == P4_OK, "register");
        REQUIRE(put(batchOf(types.back(), "peer", Tag{"p", "s", "", "b", "", "", ""}, uint64_t(i) * 10000, 2000)).status == P4_OK,
                "put");
    }
    TlvW sc;
    sc.text(1, "T0").u8(2, 1);
    CallOpts capRows;
    capRows.maxRowsExamined = 10;
    Result r = call(P4_OPC_SCAN, sc.b, capRows);
    CHECK_EQ(r.status, P4_E_BUDGET, "rows examined");
    CallOpts capOut;
    capOut.maxResultRows = 5;
    CHECK_EQ(call(P4_OPC_SCAN, sc.b, capOut).status, P4_E_BUDGET, "result rows");
    CallOpts capBytes;
    capBytes.maxResultBytes = 4096;
    CHECK_EQ(call(P4_OPC_SCAN, sc.b, capBytes).status, P4_E_BUDGET, "result bytes");
    CallOpts cancel;
    cancel.cancelAfterSubmit = true;
    Result cr = call(P4_OPC_SCAN, sc.b, cancel);
    const int32_t cs = cr.status;
    CHECK(cs == P4_E_CANCELLED || cs == P4_OK, "cancel: " + std::to_string(cs) + " " + cr.err);
    CHECK_EQ(call(P4_OPC_SCAN, sc.b).rows.size(), size_t(2000), "usable after the trips");
    // 8 readers over every type.
    std::atomic<int> errors{0};
    std::vector<std::thread> rs;
    const uint64_t until = flatsql::ps::monoNs() + uint64_t(argInt("budget_ms", 4000)) * 1000000ull;
    for (int k = 0; k < 8; k++)
        rs.emplace_back([&, k] {
            int i = k;
            while (flatsql::ps::monoNs() < until) {
                TlvW w;
                w.text(1, types[size_t(i % nTypes)].name).u8(2, 1).u64(3, 500);
                if (call(P4_OPC_SCAN, w.b).status != P4_OK) errors++;
                i++;
            }
        });
    for (auto& t : rs) t.join();
    CHECK_EQ(errors.load(), 0, "every read succeeded");
    const auto st = stats();
    REQUIRE(st.size() >= size_t(fp::kStCount), "stats");
    report(("t_budget.heap_peak_" + std::to_string(nTypes) + "_types").c_str(), double(st[fp::kStHeapPeak]) / 1048576.0, "MiB");
    report("t_budget.reader_conns", double(st[fp::kStReaderConns]), "connections");
#if !defined(__wasm__)
    {
        struct rusage ru;
        getrusage(RUSAGE_SELF, &ru);
#if defined(__APPLE__)
        report("t_budget.max_rss", double(ru.ru_maxrss) / 1048576.0, "MiB");  // bytes on macOS
#else
        report("t_budget.max_rss", double(ru.ru_maxrss) / 1024.0, "MiB");     // KiB on Linux
#endif
    }
#endif
    // C-30: the heap is the SQL surface's allocator count (0 without it).
    CHECK(st[fp::kStHeapPeak] <= (640ull << 20), "inside the hard heap (640 MiB)");
    closeEngine();
}

// M3/G3: one-record calls cost about what batched calls cost (group commit):
// WAL bytes per record at most 2x the 4,096-record shape. Group commit merges
// the calls queued while a commit runs, so a group is as large as the calls in
// flight: 1,024 one-record calls are kept in flight here (many single-record
// sources at once); 48 synchronous callers make 48-record groups.
P4_TEST(t_group) {
    TestType t = typedPnm("PNM");
    auto run = [&](bool single) -> double {
        const std::string root = scratchDir(single ? "group1" : "group4096") + "/fsql4";
        EngineOpts o;
        o.writers = 1;
        o.writeSlots = single ? 1100 : 16;
        o.writeReqBytes = single ? (128u << 10) : (8u << 20);
        TlvW x;
        x.u32(30, 100000000);          // no checkpoint while measuring: the WAL holds every frame
        if (single) x.u32(11, 4096);   // small rings: a PUT answer is a few hundred bytes
        o.extra = x.b;
        if (openEngine(root, o) != P4_OK || registerType(t) != P4_OK) return -1;
        const int n = 8192;
        const uint64_t t0 = flatsql::ps::monoNs();
        if (!single) {
            put(batchOf(t, "peer", Tag{"p", "s", "", "b", "", "", ""}, 0, 4096));
            put(batchOf(t, "peer", Tag{"p", "s", "", "b", "", "", ""}, 4096, 4096));
        } else {
            std::vector<uint32_t> inflight;
            int next = 0, done = 0;
            while (done < n) {
                while (next < n && inflight.size() < 1024)
                    inflight.push_back(submit(P4_OPC_PUT, encodePut(batchOf(t, "peer", Tag{"p", "s", "", "b", "", "", ""}, uint64_t(next++), 1))));
                for (uint32_t sl : inflight) {
                    Result r = wait(sl);
                    if (r.status == P4_OK) done++;
                    else if (r.status == P4_E_BUSY) next--, done += 0;
                    else done++;
                }
                inflight.clear();
            }
        }
        const double secs = double(flatsql::ps::monoNs() - t0) / 1e9;
        const int64_t wal = walBytes(root + "/P");
        const auto st = stats();
        if (st.size() > size_t(fp::kStGroupCommits))
            report(single ? "t_group.one_record_calls.records_per_group" : "t_group.4096_record_calls.records_per_group",
                   double(st[fp::kStPutRecords]) / double(std::max<uint64_t>(1, st[fp::kStGroupCommits])), "records");
        report(single ? "t_group.one_record_calls.rate" : "t_group.4096_record_calls.rate", n / secs, "records/s");
        CHECK_EQ(headN("PNM"), int64_t(n), "every record stored");
        closeEngine();
        return double(wal) / n;
    };
    const double batched = run(false);
    const double single = run(true);
    report("t_group.wal_bytes_per_record.4096", batched, "B");
    report("t_group.wal_bytes_per_record.one", single, "B");
    CHECK(batched > 0 && single > 0, "measured");
    CHECK(single <= 2 * batched, "one-record calls at batch cost (<= 2x)");
}
