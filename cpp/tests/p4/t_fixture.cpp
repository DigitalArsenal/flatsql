// G2 on the host-02-sized fixture (format 1, 4,364,873 copies of 3,945,845
// records): load it the way store-migrate does (migrate-mode PUTs keeping
// format 1's rowids and tag instances, then REBUILD 1), measure bytes per copy
// (below both engines), and check answers against format 1 per shape. Native only (it
// reads format 1 with SQLite directly, as the oracle). Skips cleanly without
// --fixture.
//
//   flatsql_p4_test --test=g2_fixture --fixture=<control.flatsqldb>
//       --bfbs=<sdn-server/internal/sds/search-schemas> [--store=<dir>] [--keep=1]
#include <sys/stat.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <mutex>
#include <cstring>
#include <map>
#include <random>
#include <set>
#include <thread>

#include <flatbuffers/reflection.h>

#include "flatsql/flatsql_io.h"
#include "internal.h"
#include "p4/p4_test.h"

using namespace p4t;
namespace fp = flatsql::p4;

#if !defined(__wasm__)
#include <filesystem>

namespace {

struct Part1 {
    std::string table, type, schema;
    int64_t lo, hi;
};

std::vector<uint8_t> readFile(const std::string& p) {
    std::vector<uint8_t> b;
    FILE* f = std::fopen(p.c_str(), "rb");
    if (!f) return b;
    std::fseek(f, 0, SEEK_END);
    b.resize(size_t(std::ftell(f)));
    std::fseek(f, 0, SEEK_SET);
    if (std::fread(b.data(), 1, b.size(), f) != b.size()) b.clear();
    std::fclose(f);
    return b;
}

TestType realType(const std::string& dir, const std::string& name) {
    TestType t;
    t.name = name;
    t.bfbs = readFile(dir + "/" + name + ".bfbs");
    const std::string fid = "$" + name;
    std::memcpy(t.fid, fid.data(), 4);
    if (name == "OMM") {
        t.rules = "epoch str:EPOCH|str:CREATION_DATE\ncol 0 u64pos:NORAD_CAT_ID\ncol 1 str:OBJECT_ID\nepoch_day 4\nobject 0,1\n";
        t.a18 = 400000;
        t.profile = 1;
    } else if (name == "MPE") {
        t.rules = "epoch f64floor:EPOCH\ncol 1 str:ENTITY_ID\nepoch_day 4\nobject 1\n";
        t.profile = 2;
    } else if (name == "CAT") {
        t.rules = "col 0 u64pos:NORAD_CAT_ID\ncol 1 str:OBJECT_ID\ncol 2 enum:OBJECT_TYPE\ncol 3 enum:OPS_STATUS_CODE\nobject 0,1\n"
                  "supersede pair:uri:CATALOG_URI,CATALOG_OBJECT_ID|u64:norad:NORAD_CAT_ID|str:object:OBJECT_ID\n";
        t.fullText = true;
    } else if (name == "IQC") {
        t.rules = "";
        t.identity = true;
        t.pageSize = 16384;
    }
    return t;
}

std::vector<uint8_t> unhexSig(const unsigned char* s, int n) {
    std::vector<uint8_t> b;
    auto v = [](unsigned char c) { return c <= '9' ? c - '0' : (c | 0x20) - 'a' + 10; };
    for (int i = 0; i + 1 < n; i += 2) b.push_back(uint8_t(v(s[i]) << 4 | v(s[i + 1])));
    return b;
}

int64_t duBytes(const std::string& dir, int64_t* files) {
    int64_t n = 0;
    std::error_code ec;
    for (auto& d : std::filesystem::recursive_directory_iterator(dir, ec)) {
        if (!d.is_regular_file(ec)) continue;
        n += int64_t(d.file_size(ec));
        if (files) (*files)++;
    }
    return n;
}

const char* ct(sqlite3_stmt* s, int i) {
    const unsigned char* t = sqlite3_column_text(s, i);
    return t ? reinterpret_cast<const char*>(t) : "";
}

}  // namespace

P4_SLOW_TEST(g2_fixture) {
    const std::string fixture = argStr("fixture", "");
    const std::string bfbsDir = argStr("bfbs", "");
    if (fixture.empty() || bfbsDir.empty()) {
        std::printf("  skipped: --fixture and --bfbs\n");
        return;
    }
    {
        // The engine configures SQLite (the SQL surface's allocator) before
        // anything initializes it: the format-1 oracle below opens SQLite too.
        const std::string warm = scratchDir("g2init") + "/fsql4";
        REQUIRE(openEngine(warm) == P4_OK, "engine init");
        closeEngine();
        removeTree(warm.substr(0, warm.size() - 6));
    }
    const std::string uri = "file:" + fixture + "?mode=ro&immutable=1";
    sqlite3* fx = nullptr;
    REQUIRE(sqlite3_open_v2(uri.c_str(), &fx, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, nullptr) == SQLITE_OK, "fixture");
    // Partition tables: (table, type) with their rowid ranges, from format 1.
    std::vector<Part1> parts;
    {
        sqlite3_stmt* s;
        sqlite3_prepare_v2(fx, "SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'sds_p_%'", -1, &s, nullptr);
        while (sqlite3_step(s) == SQLITE_ROW) {
            const std::string t = ct(s, 0);
            const size_t at = t.rfind("__");
            if (at == std::string::npos) continue;
            Part1 p{t, t.substr(at + 2), t.substr(at + 2) + ".fbs", 0, 0};
            const std::string only = argStr("types", "OMM,MPE,CAT,IQC");
            if (only.find(p.type) == std::string::npos) continue;
            parts.push_back(p);
        }
        sqlite3_finalize(s);
    }
    int64_t maxRowid = 0, copiesF1 = 0;
    std::map<std::string, int64_t> uniqF1;
    {
        sqlite3_stmt* s;
        sqlite3_prepare_v2(fx, "SELECT schema_name, count(*), max(rowid) FROM sdn_record_index GROUP BY schema_name", -1, &s, nullptr);
        while (sqlite3_step(s) == SQLITE_ROW) {
            uniqF1[ct(s, 0)] = sqlite3_column_int64(s, 1);
            maxRowid = std::max<int64_t>(maxRowid, sqlite3_column_int64(s, 2));
        }
        sqlite3_finalize(s);
        for (auto& p : parts) {
            sqlite3_prepare_v2(fx, ("SELECT count(*) FROM \"" + p.table + "\"").c_str(), -1, &s, nullptr);
            if (sqlite3_step(s) == SQLITE_ROW) copiesF1 += sqlite3_column_int64(s, 0);
            sqlite3_finalize(s);
        }
    }
    const std::string root = argStr("store", scratchDir("g2")) + "/fsql4";
    EngineOpts o;
    o.createMode = 2;
    o.writers = 4;
    o.writeSlots = 16;
    o.gseqFloor = uint64_t(maxRowid) + 1 + (1u << 20);
    std::map<std::string, TestType> types;
    for (auto& p : parts)
        if (!types.count(p.type)) {
            types[p.type] = realType(bfbsDir, p.type);
            REQUIRE(!types[p.type].bfbs.empty(), "BFBS " + p.type);
        }
    struct stat stStore;
    const bool reuse = argInt("reuse", 0) != 0 && ::stat((root + "/STORE").c_str(), &stStore) == 0;
    if (!reuse) {
    REQUIRE(openEngine(root, o) == P4_OK, "open the migration target");
    for (auto& kv : types) REQUIRE(registerType(kv.second) == P4_OK, "register " + kv.first);
    // Load: one thread per format-1 partition table.
    std::atomic<int64_t> loaded{0}, rejected{0}, frameBytes{0}, orphans{0};
    std::map<std::string, std::string> firstTable;
    for (auto& p : parts)
        if (!firstTable.count(p.type)) firstTable[p.type] = p.table;
    std::atomic<int> failures{0};
    const uint64_t t0 = flatsql::ps::monoNs();
    std::vector<std::thread> th;
    for (auto& p : parts)
        th.emplace_back([&, p] {
            sqlite3* db = nullptr;
            sqlite3_open_v2(uri.c_str(), &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI | SQLITE_OPEN_NOMUTEX, nullptr);
            sqlite3_exec(db, "PRAGMA cache_size=-8192", nullptr, nullptr, nullptr);
            // The table in its own rowid order (store-migrate's order); the
            // seq is format 1's index rowid; tags are per record in format 1
            // and go with the copy in the type's first table.
            const bool tagsHere = p.table == firstTable[p.type];
            sqlite3_stmt* s = nullptr;
            sqlite3_stmt* ix = nullptr;
            sqlite3_stmt* tq = nullptr;
            if (sqlite3_prepare_v2(db, ("SELECT cid, peer_id, timestamp, data, signature_hex FROM \"" + p.table + "\" ORDER BY rowid").c_str(),
                                   -1, &s, nullptr) != SQLITE_OK ||
                sqlite3_prepare_v2(db, "SELECT rowid FROM sdn_record_index WHERE schema_name=?1 AND cid=?2", -1, &ix, nullptr) != SQLITE_OK ||
                sqlite3_prepare_v2(db,
                                   "SELECT provider_id, source_name, coalesce(source_url,''), batch_id, content_key_id, producer_peer_id,"
                                   " producer_public_key, created_at FROM sdn_record_source_tags WHERE schema_name=?1 AND cid=?2",
                                   -1, &tq, nullptr) != SQLITE_OK) {
                std::fprintf(stderr, "  %s: %s\n", p.table.c_str(), sqlite3_errmsg(db));
                failures++;
                return;
            }
            sqlite3_bind_text(ix, 1, p.schema.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(tq, 1, p.schema.c_str(), -1, SQLITE_TRANSIENT);
            Batch b;
            size_t bytes = 0;
            std::map<std::string, uint16_t> tagIdx;
            auto flush = [&] {
                if (b.recs.empty()) return;
                Result r = put(b);
                if (r.status != P4_OK || r.rows.size() != b.recs.size()) {
                    std::fprintf(stderr, "  %s: PUT %d %s\n", p.table.c_str(), r.status, r.err.c_str());
                    failures++;
                } else {
                    for (size_t i = 0; i < r.rows.size(); i++)
                        if (r.i(i, "action") == P4_ACT_REJECTED) {
                            if (rejected.fetch_add(1) < 5)
                                std::fprintf(stderr, "  %s: reject %lld\n", p.table.c_str(), (long long)r.i(i, "reject"));
                        }
                    loaded += int64_t(r.rows.size());
                }
                b.recs.clear();
                b.tags.clear();
                tagIdx.clear();
                bytes = 0;
            };
            int rc;
            while ((rc = sqlite3_step(s)) == SQLITE_ROW) {
                const char* cid = ct(s, 0);
                const std::string peer = ct(s, 1);
                sqlite3_bind_text(ix, 2, cid, -1, SQLITE_TRANSIENT);
                const bool indexed = sqlite3_step(ix) == SQLITE_ROW;
                const int64_t rowid = indexed ? sqlite3_column_int64(ix, 0) : 0;
                sqlite3_reset(ix);
                if (!indexed) {
                    orphans++;
                    continue;
                }
                if (b.recs.size() >= 4096 || bytes > (5u << 20)) flush();
                if (b.recs.empty()) {
                    b.type = p.type;
                    b.peer = peer;
                    b.mode = 1;
                }
                In in;
                const int n = sqlite3_column_bytes(s, 3);
                in.frame.resize(size_t(n) + 4);
                fp::st32(in.frame.data(), uint32_t(n));
                std::memcpy(in.frame.data() + 4, sqlite3_column_blob(s, 3), size_t(n));
                uint8_t d[32];
                if (!fp::cidDigestFromText(cid, std::strlen(cid), d)) {
                    failures++;
                    continue;
                }
                in.explicitCid = true;
                in.cid[0] = 0x01; in.cid[1] = 0x55; in.cid[2] = 0x12; in.cid[3] = 0x20;
                std::memcpy(in.cid + 4, d, 32);
                in.ts = sqlite3_column_int64(s, 2);
                in.seq = rowid;
                if (peer != b.peer) in.peer = peer;
                if (sqlite3_column_type(s, 4) != SQLITE_NULL) in.sig = unhexSig(sqlite3_column_text(s, 4), sqlite3_column_bytes(s, 4));
                bytes += size_t(n) + 64;
                frameBytes += n;
                if (tagsHere) {
                    sqlite3_bind_text(tq, 2, cid, -1, SQLITE_TRANSIENT);
                    while (sqlite3_step(tq) == SQLITE_ROW) {
                        Tag t{ct(tq, 0), ct(tq, 1), ct(tq, 2), ct(tq, 3), ct(tq, 4), ct(tq, 5), ct(tq, 6)};
                        const std::string key = t.provider + '\x1f' + t.source + '\x1f' + t.url + '\x1f' + t.batch + '\x1f' + t.ckey +
                                                '\x1f' + t.ppeer + '\x1f' + t.pkey;
                        auto it = tagIdx.find(key);
                        uint16_t idx;
                        if (it == tagIdx.end()) {
                            idx = uint16_t(b.tags.size());
                            b.tags.push_back(t);
                            tagIdx[key] = idx;
                        } else {
                            idx = it->second;
                        }
                        in.tags.push_back({idx, sqlite3_column_int64(tq, 7)});
                    }
                    sqlite3_reset(tq);
                }
                b.recs.push_back(std::move(in));
            }
            if (rc != SQLITE_DONE) {
                std::fprintf(stderr, "  %s: read %d %s\n", p.table.c_str(), rc, sqlite3_errmsg(db));
                failures++;
            }
            sqlite3_finalize(ix);
            sqlite3_finalize(tq);
            flush();
            sqlite3_finalize(s);
            sqlite3_close(db);
        });
    for (auto& t : th) t.join();
    const double loadSecs = double(flatsql::ps::monoNs() - t0) / 1e9;
    report("g2.load.records", double(loaded.load()), "copies");
    report("g2.load.seconds", loadSecs, "s");
    report("g2.load.rate", double(loaded.load()) / loadSecs, "copies/s");
    report("g2.load.orphans", double(orphans.load()), "rows without an index row");
    CHECK_EQ(loaded.load(), copiesF1 - orphans.load(), "every copy loaded");
    CHECK_EQ(failures.load(), 0, "load failures");
    CHECK_EQ(rejected.load(), int64_t(0), "rejects (CID, frame or extraction)");
    TlvW r1;
    r1.u32(63, 1);
    const uint64_t ti = flatsql::ps::monoNs();
    CHECK_EQ(call(P4_OPC_REBUILD, r1.b).status, P4_OK, "REBUILD 1");
    report("g2.rebuild1.seconds", double(flatsql::ps::monoNs() - ti) / 1e9, "s");
    CHECK_EQ(flatsql_p4_activate(), P4_OK, "activate");
    closeEngine(600000);
    }
    // Full text builds after activation (§11 step 5).
    o.createMode = 0;
    REQUIRE(openEngine(root, o) == P4_OK, "reopen");
    {
        const uint64_t tf = flatsql::ps::monoNs();
        bool ready = false;
        while (!ready && flatsql::ps::monoNs() - tf < 900ull * 1000 * 1000 * 1000) {
            TlvW f5;
            f5.u8(45, 5);
            Result st = call(P4_OPC_SUMMARY, f5.b);
            ready = st.status == P4_OK;
            for (size_t i = 0; i < st.rows.size(); i++)
                if (st.s(i, "state") == "building") ready = false;
            if (!ready) std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        CHECK(ready, "full text ready");
        report("g2.fts.seconds", double(flatsql::ps::monoNs() - tf) / 1e9, "s");
        TlvW f4;
        f4.u8(45, 4);
        Result st = call(P4_OPC_SUMMARY, f4.b);
        for (size_t i = 0; i < st.rows.size(); i++)
            std::printf("  %s: files %lld db %lld wal %lld journal %lld index %lld fts %lld free %lld\n", st.s(i, "type").c_str(),
                        (long long)st.i(i, "files"), (long long)st.i(i, "db_bytes"), (long long)st.i(i, "wal_bytes"),
                        (long long)st.i(i, "journal_bytes"), (long long)st.i(i, "index_bytes"), (long long)st.i(i, "fts_bytes"),
                        (long long)st.i(i, "free_bytes"));
    }
    closeEngine(600000);
    int64_t files = 0;
    const int64_t onDisk = duBytes(root, &files);
    int64_t uniqTotal = 0;
    for (auto& kv : uniqF1)
        if (argStr("types", "OMM,MPE,CAT,IQC").find(kv.first.substr(0, 3)) != std::string::npos) uniqTotal += kv.second;
    report("g2.store.bytes", double(onDisk), "B");
    report("g2.store.files", double(files), "files");
    report("g2.bytes_per_copy", double(onDisk) / double(copiesF1), "B");
    report("g2.bytes_per_record", double(onDisk) / double(uniqTotal), "B");
    // The slimmer gate: below both engines (format 2 1,742 B, format 1 2,366 B per copy on this fixture).
    CHECK(double(onDisk) / double(copiesF1) < 1742.0, "below both engines' bytes per copy");

    // Equivalence with format 1, per shape.
    REQUIRE(openEngine(root, o) == P4_OK, "reopen");
    int64_t mism = 0;
    for (auto& kv : types) {
        const std::string& type = kv.first;
        // counts (SUMMARY 1 records = format 1's index rows; copies = partition rows)
        TlvW sw;
        sw.u8(45, 1).text(1, type);
        Result s1 = call(P4_OPC_SUMMARY, sw.b);
        if (s1.rows.size() == 1 && s1.i(0, "records") != uniqF1[type + ".fbs"]) {
            mism++;
            std::printf("  %s records %lld vs %lld\n", type.c_str(), (long long)s1.i(0, "records"), (long long)uniqF1[type + ".fbs"]);
        }
        // GET samples: bytes, seq, ts, peer; TAGS: identities, urls, at
        sqlite3_stmt* q;
        sqlite3_prepare_v2(fx, "SELECT rowid, cid FROM sdn_record_index WHERE schema_name=?1 AND rowid % 997 = 0 LIMIT 400", -1, &q, nullptr);
        sqlite3_bind_text(q, 1, (type + ".fbs").c_str(), -1, SQLITE_TRANSIENT);
        int checked = 0;
        while (sqlite3_step(q) == SQLITE_ROW) {
            const int64_t rowid = sqlite3_column_int64(q, 0);
            const std::string cid = ct(q, 1);
            uint8_t d[32], c36[36] = {0x01, 0x55, 0x12, 0x20};
            fp::cidDigestFromText(cid.data(), cid.size(), d);
            std::memcpy(c36 + 4, d, 32);
            Result g = get(type, {std::vector<uint8_t>(c36, c36 + 36)}, true);
            std::string tbl;
            for (auto& p : parts)
                if (p.type == type) tbl = p.table;
            sqlite3_stmt* r;
            sqlite3_prepare_v2(fx, ("SELECT data, timestamp, peer_id FROM \"" + tbl + "\" WHERE cid=?1").c_str(), -1, &r, nullptr);
            sqlite3_bind_text(r, 1, cid.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(r) == SQLITE_ROW) {
                const std::string data(static_cast<const char*>(sqlite3_column_blob(r, 0)), size_t(sqlite3_column_bytes(r, 0)));
                if (g.rows.empty() || g.s(0, "data") != data || g.i(0, "seq") != rowid || g.i(0, "ts") != sqlite3_column_int64(r, 1) ||
                    g.s(0, "peer") != ct(r, 2)) {
                    if (mism < 10) std::printf("  %s GET %s differs\n", type.c_str(), cid.c_str());
                    mism++;
                }
            }
            sqlite3_finalize(r);
            // tags
            TlvW tw;
            tw.text(1, type);
            std::vector<uint8_t> cl(4);
            fp::st32(cl.data(), 1);
            cl.insert(cl.end(), c36, c36 + 36);
            tw.raw(40, cl.data(), cl.size());
            Result tg = call(P4_OPC_TAGS, tw.b);
            std::set<std::string> a, bset;
            for (size_t i = 0; i < tg.rows.size(); i++)
                a.insert(tg.s(i, "provider") + "|" + tg.s(i, "source") + "|" + tg.s(i, "source_url") + "|" + tg.s(i, "batch") + "|" +
                         tg.s(i, "content_key_id") + "|" + tg.s(i, "producer_peer") + "|" + tg.s(i, "producer_pubkey") + "|" +
                         std::to_string(tg.i(i, "at")));
            sqlite3_prepare_v2(fx,
                               "SELECT provider_id, source_name, coalesce(source_url,''), batch_id, content_key_id, producer_peer_id,"
                               " producer_public_key, created_at FROM sdn_record_source_tags WHERE schema_name=?1 AND cid=?2",
                               -1, &r, nullptr);
            sqlite3_bind_text(r, 1, (type + ".fbs").c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(r, 2, cid.c_str(), -1, SQLITE_TRANSIENT);
            while (sqlite3_step(r) == SQLITE_ROW)
                bset.insert(std::string(ct(r, 0)) + "|" + ct(r, 1) + "|" + ct(r, 2) + "|" + ct(r, 3) + "|" + ct(r, 4) + "|" + ct(r, 5) + "|" +
                            ct(r, 6) + "|" + std::to_string(sqlite3_column_int64(r, 7)));
            sqlite3_finalize(r);
            if (a != bset) {
                if (mism < 10) std::printf("  %s TAGS %s differ (%zu vs %zu)\n", type.c_str(), cid.c_str(), a.size(), bset.size());
                mism++;
            }
            checked++;
        }
        sqlite3_finalize(q);
        // SCAN first page = format 1's rowid order
        TlvW sc;
        sc.text(1, type).u64(3, 1000);
        Result scan = call(P4_OPC_SCAN, sc.b);
        sqlite3_prepare_v2(fx, "SELECT rowid FROM sdn_record_index WHERE schema_name=?1 ORDER BY rowid LIMIT 1000", -1, &q, nullptr);
        sqlite3_bind_text(q, 1, (type + ".fbs").c_str(), -1, SQLITE_TRANSIENT);
        size_t i = 0;
        bool same = true;
        while (sqlite3_step(q) == SQLITE_ROW) {
            if (i >= scan.rows.size() || scan.i(i, "seq") != sqlite3_column_int64(q, 0)) same = false;
            i++;
        }
        sqlite3_finalize(q);
        if (!same || i != scan.rows.size()) {
            mism++;
            std::printf("  %s SCAN order differs\n", type.c_str());
        }
        // WINDOW (COALESCE(epoch_unix, source_timestamp) DESC, cid ASC), first 300
        TlvW wn;
        wn.text(1, type).u64(3, 300).u64(4, 100);
        Result w = call(P4_OPC_WINDOW, wn.b);
        sqlite3_prepare_v2(fx,
                           "SELECT cid FROM sdn_record_index WHERE schema_name=?1 ORDER BY coalesce(epoch_unix, source_timestamp) DESC,"
                           " cid ASC LIMIT 300 OFFSET 100",
                           -1, &q, nullptr);
        sqlite3_bind_text(q, 1, (type + ".fbs").c_str(), -1, SQLITE_TRANSIENT);
        i = 0;
        same = true;
        while (sqlite3_step(q) == SQLITE_ROW) {
            if (i >= w.rows.size() || w.s(i, "cid") != ct(q, 0)) same = false;
            i++;
        }
        sqlite3_finalize(q);
        if (!same || i != w.rows.size()) {
            mism++;
            std::printf("  %s WINDOW differs (%zu rows vs %zu)\n", type.c_str(), w.rows.size(), i);
        }
        // INDEX_PAGE (epoch_unix DESC, cid ASC), offset 1000
        TlvW ip;
        ip.text(1, type).u64(3, 50).u64(4, 1000);
        Result pg = call(P4_OPC_INDEX_PAGE, ip.b);
        sqlite3_prepare_v2(fx, "SELECT cid FROM sdn_record_index WHERE schema_name=?1 ORDER BY epoch_unix DESC, cid ASC LIMIT 50 OFFSET 1000",
                           -1, &q, nullptr);
        sqlite3_bind_text(q, 1, (type + ".fbs").c_str(), -1, SQLITE_TRANSIENT);
        i = 0;
        same = true;
        while (sqlite3_step(q) == SQLITE_ROW) {
            if (i >= pg.rows.size() || pg.s(i, "cid") != ct(q, 0)) same = false;
            i++;
        }
        sqlite3_finalize(q);
        if (!same || i != pg.rows.size()) {
            mism++;
            std::printf("  %s INDEX_PAGE differs (%zu rows vs %zu)\n", type.c_str(), pg.rows.size(), i);
        }
        // EPOCH points (as_of, forward, nearest) for every object: format 1's
        // queryPointEpochRecords ranking, on its index joined to its table.
        if (type == "OMM" || type == "MPE") {
            const std::string ent =
                type == "OMM" ? "COALESCE(CASE WHEN idx.norad_cat_id IS NOT NULL THEN CAST(idx.norad_cat_id AS TEXT) END,"
                                " NULLIF(idx.entity_id, ''), idx.cid)"
                              : "COALESCE(NULLIF(idx.entity_id, ''), CASE WHEN idx.norad_cat_id IS NOT NULL THEN"
                                " CAST(idx.norad_cat_id AS TEXT) END, idx.cid)";
            int64_t lo = 0, hi = 0;
            sqlite3_prepare_v2(fx, "SELECT min(epoch_unix), max(epoch_unix) FROM sdn_record_index WHERE schema_name=?1", -1, &q,
                               nullptr);
            sqlite3_bind_text(q, 1, (type + ".fbs").c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(q) == SQLITE_ROW) {
                lo = sqlite3_column_int64(q, 0);
                hi = sqlite3_column_int64(q, 1);
            }
            sqlite3_finalize(q);
            std::string tbl;
            for (auto& p : parts)
                if (p.type == type) tbl = p.table;
            const int64_t ats[3] = {lo + (hi - lo) / 4, lo + (hi - lo) / 2, hi - (hi - lo) / 20};
            for (int profile = 2; profile <= 4; profile++)
                for (int64_t at : ats) {
                    std::string where = "idx.epoch_unix IS NOT NULL";
                    std::string rank = "e DESC, cid ASC";
                    if (profile == 3) where += " AND idx.epoch_unix <= ?2";
                    if (profile == 4) {
                        where += " AND idx.epoch_unix >= ?2";
                        rank = "e ASC, cid ASC";
                    }
                    if (profile == 2) rank = "ABS(e - ?2) ASC, CASE WHEN e <= ?2 THEN 0 ELSE 1 END ASC, e DESC, cid ASC";
                    const std::string sql =
                        "WITH c AS (SELECT d.cid AS cid, " + ent + " AS k, idx.epoch_unix AS e FROM \"" + tbl +
                        "\" d JOIN sdn_record_index idx ON idx.schema_name=?1 AND idx.cid=d.cid WHERE " + where +
                        "), r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY " + rank +
                        ") rn FROM c) SELECT k, cid, e FROM r WHERE rn=1 ORDER BY k";
                    std::vector<std::string> want;
                    const uint64_t f1t = flatsql::ps::monoNs();
                    if (sqlite3_prepare_v2(fx, sql.c_str(), -1, &q, nullptr) != SQLITE_OK) {
                        std::printf("  F1 epoch SQL: %s\n", sqlite3_errmsg(fx));
                        mism++;
                        continue;
                    }
                    sqlite3_bind_text(q, 1, (type + ".fbs").c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int64(q, 2, at);
                    while (sqlite3_step(q) == SQLITE_ROW)
                        want.push_back(std::string(ct(q, 0)) + "|" + ct(q, 1) + "|" + std::to_string(sqlite3_column_int64(q, 2)));
                    sqlite3_finalize(q);
                    const double f1ms = double(flatsql::ps::monoNs() - f1t) / 1e6;
                    TlvW ep;
                    ep.text(1, type).u8(30, uint8_t(profile)).i64(31, at);
                    const uint64_t f4t = flatsql::ps::monoNs();
                    Result er = call(P4_OPC_EPOCH, ep.b, CallOpts{0, 0, 0, 0, 0, 0, false, 600000});
                    const double f4ms = double(flatsql::ps::monoNs() - f4t) / 1e6;
                    std::vector<std::string> got;
                    for (size_t i = 0; i < er.rows.size(); i++)
                        got.push_back(er.s(i, "key") + "|" + er.s(i, "cid") + "|" + std::to_string(er.i(i, "epoch")));
                    std::printf("  %s EPOCH %d at %lld: %zu objects (F1 %zu), %.1f ms (F1 SQL %.1f ms), examined %llu\n",
                                type.c_str(), profile, (long long)at, got.size(), want.size(), f4ms, f1ms,
                                (unsigned long long)er.rowsExamined);
                    if (er.status != P4_OK || got != want) {
                        mism++;
                        size_t d = 0;
                        while (d < got.size() && d < want.size() && got[d] == want[d]) d++;
                        std::printf("    differs at %zu: %s vs %s\n", d, d < got.size() ? got[d].c_str() : "-",
                                    d < want.size() ? want[d].c_str() : "-");
                    }
                }
        }
        std::printf("  %s: %d sampled records checked\n", type.c_str(), checked);
    }
    TlvW v8;
    v8.u32(63, 8);
    Result v = call(P4_OPC_REBUILD, v8.b);
    for (size_t i = 0; i < v.rows.size(); i++) CHECK_EQ(v.i(i, "mismatches"), int64_t(0), "verify " + v.s(i, "type"));
    closeEngine(600000);
    sqlite3_close(fx);
    report("g2.equivalence.mismatches", double(mism), "differences");
    CHECK_EQ(mism, int64_t(0), "answers equal format 1 per shape");
    if (argInt("keep", 0) == 0) removeTree(root.substr(0, root.size() - 6));
}
#endif

// The A18 shapes (benchset R17) through the reader API on a loaded store, as
// the SQL surface runs them: the type's newest N, a source, the bytes. The
// first run after the open is cold for the engine (a fresh cp -c clone is
// cold for the OS too).
//   flatsql_p4_test --test=a18_probe --store=<dir with fsql4> [--reps=5]
#if !defined(__wasm__)
namespace flatsql {
namespace p4 {
P4Engine* currentEngine();
}
}  // namespace flatsql
P4_SLOW_TEST(a18_probe) {
    const std::string store = argStr("store", "");
    if (store.empty()) {
        std::printf("  skipped: --store\n");
        return;
    }
    EngineOpts o;
    o.createMode = 0;
    if (argInt("reader_cache_kib", 0)) o.extra = TlvW().u32(24, uint32_t(argInt("reader_cache_kib", 0))).b;
    REQUIRE(openEngine(store + "/fsql4", o) == P4_OK, "open");
    if (argInt("rebuild", argInt("rebuild1", 0))) {
        // REBUILD what (1: partition secondary indexes a newer engine adds; 2: the type index).
        TlvW r1;
        r1.u32(63, uint32_t(argInt("rebuild", 1)));
        CHECK_EQ(call(P4_OPC_REBUILD, r1.b, CallOpts{0, 0, 0, 0, 0, 0, false, 3600000}).status, P4_OK, "REBUILD 1");
        closeEngine(600000);
        return;
    }
    struct Shape {
        const char* type;
        const char* source;
        uint64_t bound;
        int64_t norad;  // COL0 equality, or -1
    };
    const Shape shapes[] = {{"OMM", "celestrak-gp", 400000, 25544},
                            {"IQC", "IQEngine", 10000, -1},
                            {"CAT", "celestrak-satcat", 10000, -1},
                            {"CAT", "celestrak-satcat-csv", 10000, -1},
                            {"MPE", "celestrak-gp", 10000, -1}};
    const int reps = int(argInt("reps", 5));
    const std::string only = argStr("only", "");
    for (const Shape& sh : shapes) {
        if (!only.empty() && only != std::string(sh.type) + "@" + sh.source) continue;
        std::vector<double> ms;
        uint64_t rows = 0, examined = 0, bytes = 0;
        for (int rep = 0; rep <= reps; rep++) {
            P4Lane lane;
            lane.e = fp::currentEngine();
            P4ScanSpec spec{};
            spec.type = sh.type;
            spec.lane.source = sh.source;
            spec.order = P4_ORDER_SEQ_DESC;
            spec.hydrate = 1;
            spec.bound = sh.bound;
            P4Value v{1, sh.norad, 0, nullptr, 0};
            P4Pred p{P4_F_COL0, P4_OP_EQ, 1, &v};
            if (sh.norad >= 0) {
                spec.preds = &p;
                spec.nPreds = 1;
            }
            const uint64_t t0 = flatsql::ps::monoNs();
            P4Cursor* c = nullptr;
            int32_t rc = p4_cursor_open(&lane, &spec, &c);
            P4Row row;
            uint64_t n = 0, b = 0;
            int32_t k = 0;
            while (rc == P4_OK && (k = p4_cursor_next(c, &row)) == 1) {
                n++;
                b += row.dataLen;
            }
            if (c) p4_cursor_close(c);
            ms.push_back(double(flatsql::ps::monoNs() - t0) / 1e6);
            CHECK(rc == P4_OK && k == 0, std::string("cursor ") + sh.type);
            uint64_t rb = 0;
            p4_lane_counters(&lane, &examined, &rb);
            rows = n;
            bytes = b;
        }
        std::vector<double> warm(ms.begin() + 1, ms.end());
        std::sort(warm.begin(), warm.end());
        std::printf("  %s@%s%s: %llu frames (%llu B), examined %llu; cold %.2f ms, warm p50 %.2f max %.2f ms (load %.1f)\n",
                    sh.type, sh.source, sh.norad >= 0 ? " NORAD=25544" : "", (unsigned long long)rows,
                    (unsigned long long)bytes, (unsigned long long)examined, ms[0],
                    warm.empty() ? 0.0 : warm[warm.size() / 2], warm.empty() ? 0.0 : warm.back(), loadAvg());
    }
    // HEAD by an exact CID (tag 8): one type-index probe, not a type scan.
    for (const char* type : {"OMM", "IQC"}) {
        TlvW sc;
        sc.text(1, type).u8(5, P4_ORDER_SEQ_DESC).u64(3, 1);
        Result one = call(P4_OPC_SCAN, sc.b);
        if (one.rows.size() != 1) continue;
        uint8_t d[32], c36[36] = {0x01, 0x55, 0x12, 0x20};
        const std::string cid = one.s(0, "cid");
        fp::cidDigestFromText(cid.data(), cid.size(), d);
        std::memcpy(c36 + 4, d, 32);
        std::vector<double> ms;
        int64_t n = -1;
        for (int rep = 0; rep < 6; rep++) {
            TlvW h;
            h.text(1, type).raw(8, c36, 36);
            const uint64_t t0 = flatsql::ps::monoNs();
            Result r = call(P4_OPC_HEAD, h.b);
            ms.push_back(double(flatsql::ps::monoNs() - t0) / 1e6);
            n = r.rows.size() == 1 ? r.i(0, "n") : -1;
        }
        std::printf("  %s HEAD by CID: n %lld; first %.3f ms, then max %.3f ms (load %.1f)\n", type, (long long)n, ms[0],
                    *std::max_element(ms.begin() + 1, ms.end()), loadAvg());
    }
    closeEngine();
}
#endif

// G3 measurements.
//   --mode=producers [--producers=8] [--calls=40]: OMM 4,096-record calls
//       from N producers at once (one partition each): rec/s, call p50/p99.
//   --mode=w06 --store=<fixture clone>: batch supersede of OMM keeping
//       OMM-celestrak-gp-b053 (benchset W06: every OMM record goes).
//   --mode=w10 --store=<fixture clone>: quota GC to 90% of the store's bytes
//       (benchset W10: the oldest arrivals go first).
P4_SLOW_TEST(g3_bench) {
    const std::string mode = argStr("mode", "producers");
    if (mode == "producers") {
        const std::string root = scratchDir("g3") + "/fsql4";
        EngineOpts o;
        o.writers = uint32_t(argInt("writers", 8));
        o.writeSlots = 64;
        REQUIRE(openEngine(root, o) == P4_OK, "open");
        REQUIRE(registerType(ommType()) == P4_OK, "register");
        const int producers = int(argInt("producers", 8));
        const int calls = int(argInt("calls", 40));
        std::vector<std::vector<double>> lat(static_cast<size_t>(producers));
        std::atomic<int> failures{0};
        const uint64_t t0 = flatsql::ps::monoNs();
        std::vector<std::thread> th;
        for (int pi = 0; pi < producers; pi++)
            th.emplace_back([&, pi] {
                for (int c = 0; c < calls; c++) {
                    Batch b;
                    b.type = "OMM";
                    b.peer = "12D3KooWProducer" + std::to_string(pi);
                    b.tags.push_back(Tag{"prov", "src" + std::to_string(pi), "", "b" + std::to_string(c), "", "", ""});
                    b.at = 1790000000 + c;
                    for (int i = 0; i < 4096; i++) {
                        In in;
                        const uint32_t norad = uint32_t(10000 + (pi * 7919 + c * 4096 + i) % 60000);
                        in.frame = ommFrame(norad, "2026-001A",
                                            isoTime(unixOf(2026, 9, 1) + int64_t(c) * 3600 + int64_t(pi) * 60 + i % 60), 15.5,
                                            300);
                        in.ts = 1790000000 + c;
                        b.recs.push_back(std::move(in));
                    }
                    const uint64_t s = flatsql::ps::monoNs();
                    Result r = put(b);
                    lat[size_t(pi)].push_back(double(flatsql::ps::monoNs() - s) / 1e6);
                    if (r.status != P4_OK) failures++;
                }
            });
        for (auto& t : th) t.join();
        const double secs = double(flatsql::ps::monoNs() - t0) / 1e9;
        std::vector<double> all;
        for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
        std::sort(all.begin(), all.end());
        const double recs = double(producers) * calls * 4096;
        report("g3.producers", double(producers), "producers");
        report("g3.rate", recs / secs, "rec/s");
        report("g3.call_p50", all[all.size() / 2], "ms");
        report("g3.call_p99", all[size_t(double(all.size() - 1) * 0.99)], "ms");
        CHECK_EQ(failures.load(), 0, "every call");
        closeEngine(600000);
        removeTree(root.substr(0, root.size() - 6));
        return;
    }
    if (mode == "growth") {
        // The count-scaled growth step's write shape (sds-tb-gen): --producers
        // (8) callers, each call 4,096 new OMM records from one of --peers
        // (1,024) producers picked by Zipf(1.1), until --records. A call that
        // answers P4_E_BUSY is retried; one that makes no progress for
        // --stuck-s (60) seconds fails the run (a hang, not slowness).
        const std::string root = argStr("store", "").empty() ? scratchDir("g3g") + "/fsql4" : argStr("store", "") + "/fsql4";
        EngineOpts o;
        o.writers = uint32_t(argInt("writers", 8));
        o.writeSlots = 64;
        if (argInt("wal-total-mib", 0) > 0) {
            TlvW x;
            x.u64(32, uint64_t(argInt("wal-total-mib", 0)) << 20);
            o.extra = x.b;
        }
        REQUIRE(openEngine(root, o) == P4_OK, "open");
        REQUIRE(registerType(ommType()) == P4_OK, "register");
        const int producers = int(argInt("producers", 8));
        const int peers = int(argInt("peers", 1024));
        const int64_t target = argInt("records", 8000000);
        const int per = int(argInt("call", 4096));
        const int64_t stuckNs = int64_t(argInt("stuck-s", 60)) * 1000000000ll;
        std::vector<double> cdf(static_cast<size_t>(peers));
        double sum = 0;
        const double zipf = double(argInt("zipf-milli", 1100)) / 1000.0;
        for (int k = 0; k < peers; k++) cdf[size_t(k)] = (sum += 1.0 / std::pow(double(k + 1), zipf));
        for (double& x : cdf) x /= sum;
        std::atomic<int64_t> written{0}, busy{0}, calls{0};
        std::atomic<bool> stop{false}, stuck{false};
        std::atomic<double> maxMs{0};
        std::vector<std::atomic<bool>> used(static_cast<size_t>(peers));
        std::atomic<int> partitions{0};
        std::mutex latMu;
        std::vector<double> lat;
        const uint64_t t0 = flatsql::ps::monoNs();
        std::vector<std::thread> th;
        for (int pi = 0; pi < producers; pi++)
            th.emplace_back([&, pi] {
                std::mt19937_64 rng(uint64_t(pi) * 7919 + 1);
                std::uniform_real_distribution<double> u(0, 1);
                for (int64_t c = 0; !stop.load() && written.load() < target; c++) {
                    const int peer = int(std::lower_bound(cdf.begin(), cdf.end(), u(rng)) - cdf.begin());
                    if (!used[size_t(peer)].exchange(true)) partitions++;
                    Batch b;
                    b.type = "OMM";
                    b.peer = "12D3KooWGrowthPeer" + std::to_string(peer);
                    b.tags.push_back(Tag{"prov", "src" + std::to_string(peer % 16), "", "b" + std::to_string(c), "", "", ""});
                    b.at = 1790000000 + c;
                    for (int i = 0; i < per; i++) {
                        In in;
                        const uint64_t uniq = (uint64_t(pi) << 40) | (uint64_t(c) << 13) | uint64_t(i);
                        in.frame = ommFrame(uint32_t(10000 + uniq % 60000), "2026-001A",
                                            isoTime(unixOf(2026, 9, 1) + int64_t(uniq % 86400000)), 15.5 + double(uniq >> 13) * 1e-9,
                                            300);
                        in.ts = 1790000000 + c;
                        b.recs.push_back(std::move(in));
                    }
                    const uint64_t s = flatsql::ps::monoNs();
                    Result r;
                    for (;;) {
                        r = put(b);
                        if (r.status != P4_E_BUSY) break;
                        busy++;
                        if (int64_t(flatsql::ps::monoNs() - s) > stuckNs || stop.load()) break;
                        std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    }
                    const double ms = double(flatsql::ps::monoNs() - s) / 1e6;
                    if (r.status != P4_OK) {
                        std::fprintf(stderr, "  call failed after %.0f ms: %d %s\n", ms, r.status, r.err.c_str());
                        stuck = true;
                        stop = true;
                        return;
                    }
                    written += per;
                    calls++;
                    double m = maxMs.load();
                    while (ms > m && !maxMs.compare_exchange_weak(m, ms)) {
                    }
                    std::lock_guard<std::mutex> g(latMu);
                    lat.push_back(ms);
                }
            });
        uint64_t lastPrint = t0;
        while (!stop.load() && written.load() < target) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            const uint64_t now = flatsql::ps::monoNs();
            if (now - lastPrint >= 5000000000ull) {
                lastPrint = now;
                const std::vector<uint64_t> st = stats();
                std::printf("  t=%.0fs records=%lld calls=%lld partitions=%d busy=%lld max=%.0fms wal=%.0fMiB load=%.1f\n",
                            double(now - t0) / 1e9, (long long)written.load(), (long long)calls.load(), partitions.load(),
                            (long long)busy.load(), maxMs.load(), st.size() > 19 ? double(st[19]) / 1048576.0 : -1.0,
                            loadAvg());
                std::fflush(stdout);
            }
        }
        for (auto& t : th) t.join();
        const double secs = double(flatsql::ps::monoNs() - t0) / 1e9;
        std::sort(lat.begin(), lat.end());
        report("growth.records", double(written.load()), "records");
        report("growth.partitions", double(partitions.load()), "partitions");
        report("growth.rate", double(written.load()) / secs, "rec/s");
        if (!lat.empty()) {
            report("growth.call_p50", lat[lat.size() / 2], "ms");
            report("growth.call_p99", lat[size_t(double(lat.size() - 1) * 0.99)], "ms");
            report("growth.call_max", lat.back(), "ms");
        }
        report("growth.busy", double(busy.load()), "answers");
        CHECK(!stuck.load(), "every call completes (no hang)");
        closeEngine(600000);
        if (argStr("store", "").empty() && !argInt("keep", 0)) removeTree(root.substr(0, root.size() - 6));
        return;
    }
    const std::string store = argStr("store", "");
    if (store.empty()) {
        std::printf("  skipped: --store\n");
        return;
    }
    EngineOpts o;
    o.createMode = 0;
    if (argInt("writer-cache-kib", 0)) o.extra = TlvW().u32(22, uint32_t(argInt("writer-cache-kib", 0))).b;
    REQUIRE(openEngine(store + "/fsql4", o) == P4_OK, "open");
    CallOpts slow{0, 0, 0, 0, 0, 0, false, 3600000};
#if !defined(__wasm__)
    // The WAL while the op runs: the engine's stat and the -wal files on disk.
    std::atomic<bool> walStop{false};
    std::atomic<int64_t> walStatMax{0}, walFilesMax{0};
    std::thread walSampler([&] {
        while (!walStop.load()) {
            const std::vector<uint64_t> st = stats();
            if (st.size() > 19 && int64_t(st[19]) > walStatMax.load()) walStatMax = int64_t(st[19]);
            int64_t files = 0;
            std::error_code ec;
            for (auto it = std::filesystem::recursive_directory_iterator(store + "/fsql4", ec);
                 !ec && it != std::filesystem::recursive_directory_iterator(); it.increment(ec)) {
                const std::string pth = it->path().string();
                if (pth.size() > 4 && pth.compare(pth.size() - 4, 4, "-wal") == 0) files += int64_t(it->file_size(ec));
            }
            if (files > walFilesMax.load()) walFilesMax = files;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });
    if (mode == "w01") {
        // Benchset W01's OMM shape: --records (32,015) new OMM records into
        // the fixture's OMM partition (its peer), in --call (4,096) record
        // calls: rec/s and call p50/p99.
        TlvW s2;
        s2.u8(45, 2).text(1, "OMM");
        Result parts = call(P4_OPC_SUMMARY, s2.b);
        REQUIRE(parts.status == P4_OK && !parts.rows.empty(), parts.err);
        size_t big = 0;
        for (size_t i = 1; i < parts.rows.size(); i++)
            if (parts.i(i, "records") > parts.i(big, "records")) big = i;
        const std::string peer = parts.s(big, "peer");
        TestType real = realType(argStr("bfbs", ""), "OMM");
        REQUIRE(!real.bfbs.empty(), "--bfbs (the SDN search schemas: frames must verify against the registered OMM)");
        real.schema = reflection::GetSchema(real.bfbs.data());
        const int records = int(argInt("records", 32015)), per = int(argInt("call", 4096));
        const int64_t epoch0 = unixOf(2026, 9, 30) + int64_t(argInt("round", 0)) * 86400;
        std::vector<double> lat;
        std::map<int64_t, int64_t> actions;
        const uint64_t t0 = flatsql::ps::monoNs();
        for (int at = 0; at < records; at += per) {
            Batch b;
            b.type = "OMM";
            b.peer = peer;
            b.tags.push_back(Tag{"space-data-network-02", "celestrak-gp", "", "OMM-celestrak-gp-bench", "", "", ""});
            b.at = 1790700000;
            for (int i = at; i < records && i < at + per; i++) {
                In in;
                // GP-like: the same 32,015 objects again, each batch a new epoch.
                const int obj = i % 32015, gp = i / 32015;
                in.frame = buildFrame(real, {Field::str("OBJECT_NAME", "BENCH " + std::to_string(obj)), Field::str("OBJECT_ID", "2026-001A"),
                                             Field::u64("NORAD_CAT_ID", uint64_t(1 + obj)),
                                             Field::str("EPOCH", isoTime(epoch0 + int64_t(gp) * 43200 + obj % 7200)),
                                             Field::f64("MEAN_MOTION", 15.5 + double(i % 13) * 0.001),
                                             Field::f64("ECCENTRICITY", 0.0001), Field::f64("INCLINATION", 51.6)});
                in.ts = 1790700000;
                b.recs.push_back(std::move(in));
            }
            const uint64_t s = flatsql::ps::monoNs();
            Result r = put(b);
            lat.push_back(double(flatsql::ps::monoNs() - s) / 1e6);
            CHECK_EQ(r.status, P4_OK, r.err);
            for (size_t i = 0; i < r.rows.size(); i++) actions[r.i(i, "action") * 1000 + (r.i(i, "reject") ? -r.i(i, "reject") : 0)]++;
        }
        const double secs = double(flatsql::ps::monoNs() - t0) / 1e9;
        if (argInt("verbose", 0)) {
            std::printf("  calls (ms):");
            for (double x : lat) std::printf(" %.0f", x);
            std::printf("\n");
        }
        if (lat.size() >= 20) {
            // Growth: the median call of the first and the last tenth.
            const size_t k = lat.size() / 10;
            std::vector<double> a(lat.begin(), lat.begin() + long(k)), z(lat.end() - long(k), lat.end());
            std::sort(a.begin(), a.end());
            std::sort(z.begin(), z.end());
            report("w01.first_tenth_p50", a[a.size() / 2], "ms");
            report("w01.last_tenth_p50", z[z.size() / 2], "ms");
        }
        std::sort(lat.begin(), lat.end());
        for (auto& kv : actions) std::printf("  action %lld reject %lld: %lld\n", (long long)(kv.first / 1000), (long long)(kv.first % 1000), (long long)kv.second);
        report("w01.records", double(records), "records");
        report("w01.rate", double(records) / secs, "rec/s");
        report("w01.call_p50", lat[lat.size() / 2], "ms");
        report("w01.call_p99", lat.back(), "ms");
    } else
#endif
    if (mode == "w06") {
        TlvW s;
        s.text(1, "OMM").text(11, "space-data-network-02").text(12, "celestrak-gp").text(60, "OMM-celestrak-gp-b053").u8(61, 1);
        const uint64_t t0 = flatsql::ps::monoNs();
        Result r = call(P4_OPC_SUPERSEDE, s.b, slow);
        const double secs = double(flatsql::ps::monoNs() - t0) / 1e9;
        REQUIRE(r.status == P4_OK && r.rows.size() == 1, r.err);
        report("w06.seconds", secs, "s");
        report("w06.records_deleted", double(r.i(0, "records_deleted")), "records");
        report("w06.tags_deleted", double(r.i(0, "tags_deleted")), "tags");
        CHECK_EQ(r.i(0, "records_deleted"), int64_t(1696780), "benchset W06 expected_records_deleted");
    } else if (mode == "w10") {
        TlvW f4;
        f4.u8(45, 4);
        Result st = call(P4_OPC_SUMMARY, f4.b);
        int64_t total = 0;
        for (size_t i = 0; i < st.rows.size(); i++)
            total += st.i(i, "db_bytes") + st.i(i, "wal_bytes") + st.i(i, "index_bytes") + st.i(i, "fts_bytes");
        TlvW q;
        q.u64(62, uint64_t(double(total) * 0.9));
        const uint64_t t0 = flatsql::ps::monoNs();
        Result r = call(P4_OPC_QUOTA_GC, q.b, slow);
        const double secs = double(flatsql::ps::monoNs() - t0) / 1e9;
        REQUIRE(r.status == P4_OK && r.rows.size() == 1, r.err);
        report("w10.seconds", secs, "s");
        report("w10.files_dropped", double(r.i(0, "files_dropped")), "files");
        report("w10.records_dropped", double(r.i(0, "records_dropped")), "records");
    }
#if !defined(__wasm__)
    walStop = true;
    walSampler.join();
    report("wal.stat_max", double(walStatMax.load()) / 1048576.0, "MiB");
    report("wal.files_max", double(walFilesMax.load()) / 1048576.0, "MiB");
#endif
    if (argInt("exit-dirty", 0)) std::_Exit(0);  // as a kill: WALs left for the next open
    TlvW v8;
    v8.u32(63, 8);
    Result v = call(P4_OPC_REBUILD, v8.b, slow);
    for (size_t i = 0; i < v.rows.size(); i++) CHECK_EQ(v.i(i, "mismatches"), int64_t(0), "verify " + v.s(i, "type"));
    closeEngine(600000);
}

// G6 measurements.
//   --mode=nearest [--producers=101] [--months=30] [--per=100] [--objects=1000]
//       [--store=<dir>] [--keep=1]: an OMM store of 101 producers (one file
//       each) with epochs over 30 months, then EPOCH nearest (profile 2) over
//       every object at 20 epochs: p50/p99 (gate: under format 2's p99).
P4_SLOW_TEST(g6_bench) {
    const std::string mode = argStr("mode", "nearest");
    if (mode == "iqc") {
        // IQC (ingest identities) to --entries records in
        // 4,096-record calls: the write cost per entry at the start and the end.
        const std::string root = argStr("store", scratchDir("g6iqc")) + "/fsql4";
        EngineOpts o;
        o.writers = 4;
        REQUIRE(openEngine(root, o) == P4_OK, "open");
        REQUIRE(registerType(iqcType()) == P4_OK, "register");
        const int64_t entries = argInt("entries", 2000000);
        const int calls = int(entries / 4096);
        std::vector<double> perEntryUs;
        const uint64_t t0 = flatsql::ps::monoNs();
        for (int c = 0; c < calls; c++) {
            Batch b;
            b.type = "IQC";
            b.peer = "12D3KooWSigmf" + std::to_string(c % 4);
            b.tags.push_back(Tag{"iqengine", "IQEngine", "", "b" + std::to_string(c / 16), "", "", ""});
            b.at = 1790000000;
            for (int i = 0; i < 4096; i++) {
                In in;
                const uint64_t n = uint64_t(c) * 4096 + uint64_t(i);
                in.frame = iqcFrame("E" + std::to_string(n % 5000), isoTime(unixOf(2024, 1, 1) + int64_t(n) * 37), 16, n);
                in.ts = 1790000000;
                in.hasIdent = true;
                std::memset(in.ident, 0, 32);
                fp::st64(in.ident, n);
                b.recs.push_back(std::move(in));
            }
            const uint64_t s = flatsql::ps::monoNs();
            Result r = put(b);
            perEntryUs.push_back(double(flatsql::ps::monoNs() - s) / 1e3 / 4096);
            if (r.status != P4_OK) {
                CHECK_EQ(r.status, P4_OK, r.err);
                break;
            }
        }
        const size_t k = std::max<size_t>(1, perEntryUs.size() / 10);
        auto mean = [](std::vector<double>::const_iterator a, std::vector<double>::const_iterator b) {
            double sum = 0;
            size_t n = 0;
            for (; a != b; ++a, ++n) sum += *a;
            return n ? sum / double(n) : 0.0;
        };
        report("g6.iqc.entries", double(perEntryUs.size()) * 4096, "entries");
        report("g6.iqc.rate", double(perEntryUs.size()) * 4096 / (double(flatsql::ps::monoNs() - t0) / 1e9), "rec/s");
        report("g6.iqc.first_tenth_us", mean(perEntryUs.begin(), perEntryUs.begin() + long(k)), "us/entry");
        report("g6.iqc.last_tenth_us", mean(perEntryUs.end() - long(k), perEntryUs.end()), "us/entry");
        closeEngine(600000);
        if (!argInt("keep", 0)) removeTree(root.substr(0, root.size() - 6));
        return;
    }
    if (mode != "nearest") return;
    const int producers = int(argInt("producers", 101)), months = int(argInt("months", 30)), per = int(argInt("per", 100));
    const int objects = int(argInt("objects", 1000));
    const std::string root = argStr("store", scratchDir("g6")) + "/fsql4";
    struct stat st0;
    const bool exists = ::stat((root + "/STORE").c_str(), &st0) == 0;
    EngineOpts o;
    o.writers = 8;
    o.writeSlots = 64;
    REQUIRE(openEngine(root, o) == P4_OK, "open");
    REQUIRE(registerType(ommType()) == P4_OK, "register");
    const int64_t t0 = unixOf(2024, 1, 1);
    auto monthStart = [&](int m) { return unixOf(2024 + m / 12, 1 + m % 12, 1); };
    if (!exists) {
        std::atomic<int> next{0}, failures{0};
        const uint64_t s0 = flatsql::ps::monoNs();
        std::vector<std::thread> th;
        for (int w = 0; w < 8; w++)
            th.emplace_back([&] {
                for (int job; (job = next.fetch_add(1)) < producers * months;) {
                    const int pi = job / months, m = job % months;
                    Batch b;
                    b.type = "OMM";
                    b.peer = "12D3KooWG6Producer" + std::to_string(pi);
                    b.tags.push_back(Tag{"prov", "src", "", "m" + std::to_string(m), "", "", ""});
                    b.at = 1790000000;
                    for (int i = 0; i < per; i++) {
                        In in;
                        const uint32_t norad = uint32_t(1 + (pi * 10 + i) % objects);
                        in.frame = ommFrame(norad, "2024-001A", isoTime(monthStart(m) + int64_t(i) * 20000 + pi * 7), 15.5);
                        in.ts = 1790000000;
                        b.recs.push_back(std::move(in));
                    }
                    if (put(b).status != P4_OK) failures++;
                }
            });
        for (auto& t : th) t.join();
        report("g6.build.seconds", double(flatsql::ps::monoNs() - s0) / 1e9, "s");
        CHECK_EQ(failures.load(), 0, "build");
    }
    {
        TlvW f4;
        f4.u8(45, 4).text(1, "OMM");
        Result st = call(P4_OPC_SUMMARY, f4.b);
        if (st.rows.size() == 1) report("g6.files", double(st.i(0, "files")), "files");
    }
    std::mt19937 rng(11);
    std::vector<double> ms;
    size_t rows = 0;
    for (int q = 0; q < int(argInt("queries", 20)); q++) {
        const int64_t at = t0 + int64_t(rng() % uint32_t(monthStart(months) - t0));
        TlvW ep;
        ep.text(1, "OMM").u8(30, 2).i64(31, at).u64(3, 50000);
        const uint64_t s = flatsql::ps::monoNs();
        Result r = call(P4_OPC_EPOCH, ep.b, CallOpts{0, 0, 0, 0, 0, 0, false, 600000});
        ms.push_back(double(flatsql::ps::monoNs() - s) / 1e6);
        CHECK_EQ(r.status, P4_OK, r.err);
        rows = r.rows.size();
        if (q == 0) report("g6.nearest.examined", double(r.rowsExamined), "rows");
    }
    std::sort(ms.begin(), ms.end());
    report("g6.nearest.rows", double(rows), "objects");
    report("g6.nearest.p50", ms[ms.size() / 2], "ms");
    report("g6.nearest.p99", ms.back(), "ms");
    closeEngine(600000);
    if (!argInt("keep", 0)) removeTree(root.substr(0, root.size() - 6));
}

// Opens a store and verifies it (REBUILD 8: the type index against the
// files, integrity_check on every file), printing each type's answer.
//   flatsql_p4_test --test=store_check --store=<dir with fsql4> [--mode=0|1|2]
P4_SLOW_TEST(store_check) {
    const std::string store = argStr("store", "");
    if (store.empty()) {
        std::printf("  skipped: --store\n");
        return;
    }
    EngineOpts o;
    o.createMode = uint8_t(argInt("mode", 0));
    const int32_t rc = openEngine(store + "/fsql4", o);
    REQUIRE(rc == P4_OK, "open: " + std::to_string(rc));
    TlvW rb;
    rb.u32(63, uint32_t(argInt("what", 8)));
    const uint64_t t0 = flatsql::ps::monoNs();
    Result v = call(P4_OPC_REBUILD, rb.b, CallOpts{0, 0, 0, 0, 0, 0, false, 3600000});
    std::printf("  REBUILD %ld: %.0f ms (load %.1f)\n", argInt("what", 8), double(flatsql::ps::monoNs() - t0) / 1e6, loadAvg());
    CHECK_EQ(v.status, P4_OK, v.err);
    for (size_t i = 0; i < v.rows.size(); i++) {
        std::printf("  %s: %lld entries, %lld mismatches\n", v.s(i, "type").c_str(), (long long)v.i(i, "entries"),
                    (long long)v.i(i, "mismatches"));
        CHECK_EQ(v.i(i, "mismatches"), int64_t(0), "verify " + v.s(i, "type"));
    }
    closeEngine(600000);
}

#if !defined(__wasm__)
// First open with every SDN type registered (about 232), as the daemon does:
// a type's T/ files appear with its first write, so the open and the close
// cost O(types with data).
//   flatsql_p4_test --test=open_bench [--types=232]
P4_SLOW_TEST(open_bench) {
    const std::string root = scratchDir("openbench") + "/fsql4";
    const int n = int(argInt("types", 232));
    std::vector<TestType> types;
    for (int i = 0; i < n; i++) types.push_back(pnmLikeType("T" + std::to_string(i)));
    auto countT = [&] {
        int64_t files = 0;
        std::error_code ec;
        for (auto& d : std::filesystem::directory_iterator(root + "/T", ec)) {
            (void)d;
            files++;
        }
        return files;
    };
    for (int round = 0; round < 2; round++) {
        const uint64_t t0 = flatsql::ps::monoNs();
        REQUIRE(openEngine(root) == P4_OK, "open");
        const uint64_t t1 = flatsql::ps::monoNs();
        for (auto& t : types) REQUIRE(registerType(t) == P4_OK, "register");
        const uint64_t t2 = flatsql::ps::monoNs();
        if (round == 0) REQUIRE(put(Batch{"T7", "12D3KooWOpen", {Tag{"p", "s", "", "b", "", "", ""}}, 1790000000, 0,
                                          {In{buildFrame(types[7], {Field::str("FILE_ID", "x")}), 1790000000}}})
                                    .status == P4_OK,
                                "one write");
        const uint64_t t3 = flatsql::ps::monoNs();
        closeEngine();
        const uint64_t t4 = flatsql::ps::monoNs();
        std::printf("  round %d: open %.1f ms, register %d types %.1f ms, close %.1f ms; T/ files %lld\n", round,
                    double(t1 - t0) / 1e6, n, double(t2 - t1) / 1e6, double(t4 - t3) / 1e6, (long long)countT());
    }
    report("open_bench.t_files", double(countT()), "files");
    removeTree(root.substr(0, root.size() - 6));
}
#endif

// The read gate's material shapes (GATES-r1), as the SDN backend sends them
// to the engine, on a fixture store: one cold call and three warm ones each.
//   flatsql_p4_test --test=reads_bench --store=<dir with fsql4> [--only=<substring>[|...]]
P4_SLOW_TEST(reads_bench) {
    const std::string store = argStr("store", "");
    if (store.empty()) {
        std::printf("  skipped: --store\n");
        return;
    }
    EngineOpts o;
    o.createMode = 0;
    const uint64_t t0 = flatsql::ps::monoNs();
    REQUIRE(openEngine(store + "/fsql4", o) == P4_OK, "open");
    std::printf("  open %.1f ms (load %.1f)\n", double(flatsql::ps::monoNs() - t0) / 1e6, loadAvg());
    if (argInt("open-only", 0)) {
        closeEngine(600000);
        return;
    }
    auto text = [](uint8_t field, uint8_t op, const std::string& v) {
        std::vector<uint8_t> p = {field, op, 1, 0, 3};
        uint8_t n[4];
        fp::st32(n, uint32_t(v.size()));
        p.insert(p.end(), n, n + 4);
        p.insert(p.end(), v.begin(), v.end());
        return p;
    };
    struct Shape {
        std::string name;
        uint32_t op;
        TlvW t;
    };
    std::vector<Shape> shapes;
    auto add = [&](const std::string& name, uint32_t op, TlvW t) { shapes.push_back(Shape{name, op, std::move(t)}); };
    const std::string day14 = "2026-09-14", day20 = "2026-09-20";
    {
        TlvW t;
        t.text(1, "OMM").text(12, "celestrak-gp").text(13, "OMM-celestrak-gp-b052");
        add("R10 HEAD OMM source+batch", P4_OPC_HEAD, t);
    }
    {
        TlvW t;
        t.text(1, "IQC").text(12, "IQEngine");
        add("R10 HEAD IQC source", P4_OPC_HEAD, t);
    }
    {
        TlvW t;
        t.text(1, "OMM");
        auto p = text(P4_F_EPOCH_DAY, P4_OP_EQ, day14);
        t.raw(17, p.data(), p.size());
        add("R10 HEAD OMM EPOCH_DAY", P4_OPC_HEAD, t);
    }
    for (int prof : {2, 3, 4}) {
        TlvW t;
        t.text(1, "OMM").text(12, "celestrak-gp").u8(30, uint8_t(prof)).i64(31, 1789371001).u64(3, 50000);
        add(std::string("R18 EPOCH OMM@celestrak-gp ") + (prof == 2 ? "nearest" : prof == 3 ? "as_of" : "forward"), P4_OPC_EPOCH, t);
    }
    {
        TlvW t;
        t.text(1, "OMM").u8(30, 2).i64(31, 1789371001).u64(3, 50000);
        add("R16 EPOCH OMM nearest (no source)", P4_OPC_EPOCH, t);
    }
    for (int prof : {2, 3}) {
        TlvW t;
        t.text(1, "OMM").text(12, "celestrak-gp").text(13, "OMM-celestrak-gp-b030").u8(30, uint8_t(prof)).i64(31, 1789371001).u64(3, 50000);
        add(std::string("R18 EPOCH OMM@celestrak-gp b030 ") + (prof == 2 ? "nearest" : "as_of"), P4_OPC_EPOCH, t);
        TlvW m;
        m.text(1, "MPE").text(12, "celestrak-gp").u8(30, uint8_t(prof)).i64(31, 1789371001).u64(3, 50000);
        add(std::string("R18 EPOCH MPE@celestrak-gp ") + (prof == 2 ? "nearest" : "as_of"), P4_OPC_EPOCH, m);
    }
    {
        TlvW t;
        t.u8(45, 4);
        add("R20 SUMMARY 4 (DiskUsage)", P4_OPC_SUMMARY, t);
    }
    {
        TlvW t;
        t.text(1, "OMM").text(12, "celestrak-gp").u64(3, 50).u64(4, 50);
        add("R11 INDEX_PAGE OMM src page 2", P4_OPC_INDEX_PAGE, t);
    }
    {
        TlvW t;
        t.text(1, "IQC").text(12, "IQEngine").u64(3, 50).u64(4, 100);
        add("R11 INDEX_PAGE IQC src page 3", P4_OPC_INDEX_PAGE, t);
    }
    {
        TlvW t;
        t.text(1, "MPE").text(12, "celestrak-gp").u64(3, 50).u64(4, 200);
        add("R11 INDEX_PAGE MPE src page 5", P4_OPC_INDEX_PAGE, t);
    }
    {
        TlvW t;
        t.text(1, "CAT").u64(3, 50);
        auto p = text(P4_F_COL0, P4_OP_LIKE, "%2554%");
        t.raw(17, p.data(), p.size());
        add("R11 INDEX_PAGE CAT norad LIKE", P4_OPC_INDEX_PAGE, t);
        add("R11 HEAD CAT norad LIKE", P4_OPC_HEAD, t);
    }
    for (uint64_t off : {uint64_t(19950), uint64_t(99950), uint64_t(1690000)})
        for (const char* ty : {"OMM", "MPE"}) {
            TlvW t;
            t.text(1, ty).u64(3, 50).u64(4, off);
            add(std::string("R11 INDEX_PAGE ") + ty + " offset " + std::to_string(off), P4_OPC_INDEX_PAGE, t);
        }
    {
        TlvW t;
        t.text(1, "CAT").text(12, "celestrak-satcat").u64(3, 50).u64(4, 100);
        add("R11 INDEX_PAGE CAT satcat page 3", P4_OPC_INDEX_PAGE, t);
        TlvW h;
        h.text(1, "CAT").text(12, "celestrak-satcat");
        add("R11 HEAD CAT satcat", P4_OPC_HEAD, h);
        TlvW c;
        c.text(1, "CAT").u8(2, 1).u64(3, 100);
        add("R12 WINDOW CAT off=0", P4_OPC_WINDOW, c);
        TlvW c2;
        c2.text(1, "CAT").u8(2, 1).u64(3, 100).u64(4, 1000);
        add("R12 WINDOW CAT off=1000", P4_OPC_WINDOW, c2);
    }
    for (const char* ty : {"CAT", "IQC", "MPE"}) {
        TlvW t;
        t.text(1, ty).u64(3, 50);
        add(std::string("R11 INDEX_PAGE ") + ty + " page 1", P4_OPC_INDEX_PAGE, t);
        TlvW t2;
        t2.text(1, ty).u64(3, 50).u64(4, 950);
        add(std::string("R11 INDEX_PAGE ") + ty + " page 20", P4_OPC_INDEX_PAGE, t2);
    }
    {
        TlvW t;
        t.text(1, "OMM").text(12, "celestrak-gp").u64(19, 33554432);
        add("R15 HEAD OMM source cap", P4_OPC_HEAD, t);
        TlvW u;
        u.text(1, "OMM").u64(19, 33554432);
        add("R15 HEAD OMM cap (no filter)", P4_OPC_HEAD, u);
        TlvW c;
        c.text(1, "CAT").text(12, "celestrak-satcat-csv").u64(19, 1048576);
        add("R15 HEAD CAT csv cap", P4_OPC_HEAD, c);
    }
    for (const char* b : {"OMM-celestrak-gp-b052", "OMM-celestrak-gp-b010"}) {
        TlvW t;
        t.text(1, "OMM").text(12, "celestrak-gp").text(13, b).u64(19, 33554432);
        add(std::string("R15 HEAD OMM source+batch cap ") + b, P4_OPC_HEAD, t);
    }
    for (const char* q : {"SELECT COUNT(*) FROM OMM", "SELECT COUNT(*) FROM MPE", "SELECT COUNT(*) FROM CAT",
                          "SELECT COUNT(*) FROM IQC", "SELECT _source, COUNT(*) FROM MPE GROUP BY _source",
                          "SELECT OBJECT_NAME, EPOCH, NORAD_CAT_ID FROM OMM WHERE NORAD_CAT_ID BETWEEN 25000 AND 25600"}) {
        TlvW t;
        t.text(70, q);
        add(std::string("R19 SQL ") + q, P4_OPC_SQL, t);
    }
    for (const char* pat : {"%7%", "%00%", "%25544%"})
        for (const char* ty : {"CAT", "OMM"}) {
            TlvW t;
            t.text(1, ty).u64(3, 50);
            auto p = text(P4_F_COL0, P4_OP_LIKE, pat);
            t.raw(17, p.data(), p.size());
            add(std::string("R11 INDEX_PAGE ") + ty + " LIKE " + pat, P4_OPC_INDEX_PAGE, t);
            add(std::string("R11 HEAD ") + ty + " LIKE " + pat, P4_OPC_HEAD, t);
        }
    {
        TlvW t;
        t.text(1, "OMM").u8(2, 1).u64(3, 1000);
        auto p = text(P4_F_EPOCH_DAY, P4_OP_EQ, day14);
        t.raw(17, p.data(), p.size());
        add("R12 WINDOW OMM day", P4_OPC_WINDOW, t);
    }
    for (const char* ty : {"CAT", "OMM", "MPE", "IQC"}) {
        TlvW t;
        t.text(1, ty).u8(2, 1).u64(3, 100).u64(4, 20000);
        add(std::string("R12 WINDOW ") + ty + " off=20000", P4_OPC_WINDOW, t);
    }
    {
        TlvW t;
        t.text(1, "OMM").u8(30, 1).u64(3, 200);
        auto p = text(P4_F_EPOCH_DAY, P4_OP_EQ, day20);
        t.raw(17, p.data(), p.size());
        add("R16 EPOCH OMM window day", P4_OPC_EPOCH, t);
    }
    {
        TlvW t;
        t.text(1, "OMM").u8(30, 5);
        add("R16 EPOCH OMM coverage", P4_OPC_EPOCH, t);
    }
    {
        TlvW t;
        t.text(1, "OMM").text(12, "celestrak-gp").text(13, "OMM-celestrak-gp-b052").u8(5, P4_ORDER_CID).u8(2, 1).u64(3, 1000);
        add("R13 WINDOW OMM src+batch cid", P4_OPC_WINDOW, t);
    }
    {
        TlvW t;
        t.text(1, "IQC").u8(5, P4_ORDER_CID).u8(2, 1).u64(3, 1000).u64(4, 100000);
        add("R13 WINDOW IQC cid off=100000", P4_OPC_WINDOW, t);
    }
    {
        TlvW t;
        t.text(1, "CAT").u8(5, P4_ORDER_CID).u8(2, 1).u64(3, 200).u64(4, 60000);
        add("R13 WINDOW CAT cid off=60000", P4_OPC_WINDOW, t);
    }
    {
        TlvW t;
        t.text(1, "OMM").text(11, "space-data-network-02").text(12, "celestrak-gp").text(13, "OMM-celestrak-gp-b052").u8(2, 1).u64(3, 1000);
        add("R09 SCAN OMM batch b052", P4_OPC_SCAN, t);
    }
    for (uint64_t off : {uint64_t(0), uint64_t(1000)})
        for (const char* src : {"celestrak-satcat-csv", "celestrak-satcat"}) {
            TlvW t;
            t.text(1, "CAT").text(12, src).u8(2, 1).u64(3, 100).u64(4, off);
            add(std::string("R14 WINDOW CAT ") + src + " off=" + std::to_string(off), P4_OPC_WINDOW, t);
        }
    {
        TlvW t;
        t.text(1, "IQC").text(12, "IQEngine").u8(2, 1).u64(3, 100).u64(4, 1000);
        add("R14 WINDOW IQC IQEngine off=1000", P4_OPC_WINDOW, t);
    }
    {
        TlvW t;
        t.text(1, "OMM").u8(2, 1).u64(3, 500);
        auto p = text(P4_F_EPOCH_DAY, P4_OP_EQ, day14);
        t.raw(17, p.data(), p.size());
        add("R08 SCAN OMM day", P4_OPC_SCAN, t);
    }
    {
        TlvW t;
        t.text(1, "OMM").u8(2, 1).u64(3, 100);
        add("R05 SCAN OMM limit=100", P4_OPC_SCAN, t);
    }
    {
        TlvW t;
        t.text(1, "OMM").u8(30, 2).i64(31, 1789371001).u64(3, 200);
        add("R16 EPOCH OMM nearest limit=200", P4_OPC_EPOCH, t);
    }
    {
        TlvW t;
        t.text(1, "OMM").u64(3, 50);
        add("R11 INDEX_PAGE OMM page 1", P4_OPC_INDEX_PAGE, t);
    }
    for (int prof : {2, 3}) {
        TlvW t;
        t.text(1, "OMM").u8(30, uint8_t(prof)).i64(31, 1789371001).u8(33, 1);
        add(std::string("R16 EPOCH OMM count ") + (prof == 2 ? "nearest" : "as_of"), P4_OPC_EPOCH, t);
    }
    {
        TlvW t;
        t.text(1, "OMM").u8(30, 2).i64(31, 1789371001).u64(3, 2000);
        add("R16 EPOCH OMM nearest limit=2000", P4_OPC_EPOCH, t);
    }
    {
        TlvW t;
        t.text(1, "OMM");
        add("R10 HEAD OMM (no filter)", P4_OPC_HEAD, t);
    }
    {
        // The /epoch handler's counts: epoch.window (from .. to) and epoch.day.
        std::vector<uint8_t> p = {P4_F_EPOCH, P4_OP_BETWEEN, 2, 0};
        for (int64_t v : {int64_t(1788314401), int64_t(1793491200)}) {
            p.push_back(1);
            uint8_t b[8];
            fp::st64(b, uint64_t(v));
            p.insert(p.end(), b, b + 8);
        }
        for (const char* ty : {"OMM", "MPE"}) {
            TlvW t;
            t.text(1, ty).u8(30, 1).u8(33, 1);
            t.raw(17, p.data(), p.size());
            add(std::string("R16 EPOCH ") + ty + " window count", P4_OPC_EPOCH, t);
            TlvW d;
            d.text(1, ty).u8(30, 1).u8(33, 1);
            auto q = text(P4_F_EPOCH_DAY, P4_OP_EQ, day20);
            d.raw(17, q.data(), q.size());
            add(std::string("R16 EPOCH ") + ty + " day count", P4_OPC_EPOCH, d);
        }
    }
    {
        TlvW t;
        t.text(1, "OMM").u8(2, 1).u64(3, 100).u64(4, 1000);
        add("R12 WINDOW OMM off=1000", P4_OPC_WINDOW, t);
    }
    const std::string only = argStr("only", "");
    CallOpts co{0, 0, 0, 0, 0, 0, false, 600000};
#if !defined(__wasm__)
    if (const long secs = argInt("m01", 0)) {
        // M01 (proof harness m01.go), native: R01 / R05 / R11 / R16 (count +
        // nearest limit=200, as the /epoch handler) at 20 calls a second each,
        // idle for --idle-s, then a supersede of every OMM record keeping b053
        // and OMM batch (32,015 records, a new batch each cycle) + supersede
        // keeping it, until --m01 seconds. Samples the -wal files every 100 ms.
        TestType real = realType(argStr("bfbs", ""), "OMM");
        REQUIRE(!real.bfbs.empty(), "--bfbs");
        real.schema = reflection::GetSchema(real.bfbs.data());
        TlvW s2;
        s2.u8(45, 2).text(1, "OMM");
        Result parts = call(P4_OPC_SUMMARY, s2.b);
        REQUIRE(parts.status == P4_OK && !parts.rows.empty(), parts.err);
        size_t big = 0;
        for (size_t i = 1; i < parts.rows.size(); i++)
            if (parts.i(i, "records") > parts.i(big, "records")) big = i;
        const std::string peer = parts.s(big, "peer");
        std::vector<std::vector<uint8_t>> hits;
        for (const char* ty : {"OMM", "MPE"}) {
            TlvW sc;
            sc.text(1, ty).u8(2, 1).u64(3, 50);
            Result r = call(P4_OPC_SCAN, sc.b, co);
            for (size_t i = 0; i < r.rows.size(); i++) {
                const std::string c = r.s(i, "cid");
                uint8_t d[32], c36[36] = {0x01, 0x55, 0x12, 0x20};
                fp::cidDigestFromText(c.data(), c.size(), d);
                std::memcpy(c36 + 4, d, 32);
                hits.emplace_back(c36, c36 + 36);
            }
        }
        std::atomic<bool> stop{false}, busy{false};
        std::atomic<int> errs{0};
        struct Lat {
            std::mutex mu;
            std::vector<double> idle, during;
        };
        const char* names[4] = {"R01 get OMM+MPE", "R05 datasync OMM limit=100", "R11 index OMM page=1",
                                "R16 OMM count+nearest limit=200"};
        Lat lat[4];
        std::vector<std::thread> rd;
        for (int k = 0; k < 4; k++)
            rd.emplace_back([&, k] {
                for (int i = 0; !stop.load(); i++) {
                    const bool b0 = busy.load();
                    const uint64_t s = flatsql::ps::monoNs();
                    int32_t st = P4_OK;
                    std::string err;
                    if (k == 0) {
                        Result r = get(i % 2 ? "MPE" : "OMM", {hits[size_t(i) % hits.size()]});
                        st = r.status;
                        err = r.err;
                    } else if (k == 1) {
                        TlvW t;
                        t.text(1, "OMM").u8(2, 1).u64(3, 100);
                        Result r = call(P4_OPC_SCAN, t.b, co);
                        st = r.status;
                        err = r.err;
                    } else if (k == 2) {
                        TlvW t;
                        t.text(1, "OMM").u64(3, 50);
                        Result r = call(P4_OPC_INDEX_PAGE, t.b, co);
                        st = r.status;
                        err = r.err;
                    } else {
                        TlvW c, q;
                        c.text(1, "OMM").u8(30, 2).i64(31, 1789371001).u8(33, 1);
                        q.text(1, "OMM").u8(30, 2).i64(31, 1789371001).u64(3, 200);
                        Result r1 = call(P4_OPC_EPOCH, c.b, co);
                        Result r2 = call(P4_OPC_EPOCH, q.b, co);
                        st = r1.status != P4_OK ? r1.status : r2.status;
                        err = r1.err + r2.err;
                    }
                    const double ms = double(flatsql::ps::monoNs() - s) / 1e6;
                    if (st != P4_OK) {
                        errs++;
                        std::printf("  READ ERROR %s: %d %s\n", names[k], st, err.c_str());
                    }
                    {
                        std::lock_guard<std::mutex> g(lat[k].mu);
                        (b0 && busy.load() ? lat[k].during : lat[k].idle).push_back(ms);
                    }
                    const double rest = 50.0 - ms;
                    if (rest > 0) std::this_thread::sleep_for(std::chrono::microseconds(int64_t(rest * 1000)));
                }
            });
        std::atomic<int64_t> walMax{0};
        std::atomic<int64_t> walAt{0};
        const uint64_t t0 = flatsql::ps::monoNs();
        std::thread sampler([&] {
            while (!stop.load()) {
                int64_t files = 0;
                std::error_code ec;
                for (auto it = std::filesystem::recursive_directory_iterator(store + "/fsql4", ec);
                     !ec && it != std::filesystem::recursive_directory_iterator(); it.increment(ec)) {
                    const std::string pth = it->path().string();
                    if (pth.size() > 4 && pth.compare(pth.size() - 4, 4, "-wal") == 0) files += int64_t(it->file_size(ec));
                }
                if (files > walMax.load()) {
                    walMax = files;
                    walAt = int64_t((flatsql::ps::monoNs() - t0) / 1000000000ull);
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
        std::this_thread::sleep_for(std::chrono::seconds(argInt("idle-s", 5)));
        busy = true;
        const uint64_t ws = flatsql::ps::monoNs();
        CallOpts slow{0, 0, 0, 0, 0, 0, false, 3600000};
        auto supersede = [&](const std::string& keep) {
            TlvW s;
            s.text(1, "OMM").text(11, "space-data-network-02").text(12, "celestrak-gp").text(60, keep).u8(61, 1);
            return call(P4_OPC_SUPERSEDE, s.b, slow);
        };
        double w06 = 0;
        if (!argInt("no-w06", 0)) {
            const uint64_t s = flatsql::ps::monoNs();
            Result r = supersede("OMM-celestrak-gp-b053");
            w06 = double(flatsql::ps::monoNs() - s) / 1e6;
            CHECK_EQ(r.status, P4_OK, r.err);
            std::printf("  supersede keep b053: %.0f ms, wal max so far %.0f MiB (load %.1f)\n", w06,
                        double(walMax.load()) / 1048576.0, loadAvg());
        }
        std::vector<double> putMs, supMs;
        int cycles = 0;
        while (double(flatsql::ps::monoNs() - ws) / 1e9 < double(secs)) {
            const std::string batch = "OMM-celestrak-gp-m01-" + std::to_string(cycles);
            const uint64_t s = flatsql::ps::monoNs();
            for (int at = 0; at < 32015; at += 4096) {
                Batch b;
                b.type = "OMM";
                b.peer = peer;
                b.tags.push_back(Tag{"space-data-network-02", "celestrak-gp", "", batch, "", "", ""});
                b.at = 1790800000 + cycles;
                for (int i = at; i < 32015 && i < at + 4096; i++) {
                    In in;
                    in.frame = buildFrame(real, {Field::str("OBJECT_NAME", "M01 " + std::to_string(i)), Field::str("OBJECT_ID", "2026-001A"),
                                                 Field::u64("NORAD_CAT_ID", uint64_t(1 + i)),
                                                 Field::str("EPOCH", isoTime(unixOf(2026, 10, 1) + int64_t(cycles) * 600 + i % 600)),
                                                 Field::f64("MEAN_MOTION", 15.5 + double(cycles) * 1e-6), Field::f64("ECCENTRICITY", 0.0001),
                                                 Field::f64("INCLINATION", 51.6)});
                    in.ts = 1790800000 + cycles;
                    b.recs.push_back(std::move(in));
                }
                Result r = put(b);
                if (r.status != P4_OK) std::printf("  put: %d %s\n", r.status, r.err.c_str());
            }
            putMs.push_back(double(flatsql::ps::monoNs() - s) / 1e6);
            const uint64_t s2 = flatsql::ps::monoNs();
            Result r = supersede(batch);
            if (r.status != P4_OK) std::printf("  supersede: %d %s\n", r.status, r.err.c_str());
            supMs.push_back(double(flatsql::ps::monoNs() - s2) / 1e6);
            cycles++;
            if (cycles % 10 == 0)
                std::printf("  t=%.0fs cycles=%d wal max %.0f MiB (at %llds) load %.1f\n", double(flatsql::ps::monoNs() - ws) / 1e9,
                            cycles, double(walMax.load()) / 1048576.0, (long long)walAt.load(), loadAvg());
        }
        busy = false;
        stop = true;
        for (auto& t : rd) t.join();
        sampler.join();
        auto pct = [](std::vector<double> v, double q) {
            if (v.empty()) return 0.0;
            std::sort(v.begin(), v.end());
            return v[size_t(double(v.size() - 1) * q)];
        };
        std::printf("  m01: %d cycles in %.0f s; put p50 %.0f ms, supersede p50 %.0f ms (keep b053 %.0f ms); wal max %.1f MiB at %llds; read errors %d (load %.1f)\n",
                    cycles, double(flatsql::ps::monoNs() - ws) / 1e9, pct(putMs, 0.5), pct(supMs, 0.5), w06,
                    double(walMax.load()) / 1048576.0, (long long)walAt.load(), errs.load(), loadAvg());
        for (int k = 0; k < 4; k++) {
            int over = 0;
            for (double x : lat[k].during) over += x > 50.0;
            std::printf("  %-34s idle p50 %7.2f p99 %7.2f max %7.2f | writes p50 %7.2f p99 %7.2f max %7.2f, over 50 ms %d of %zu\n",
                        names[k], pct(lat[k].idle, 0.5), pct(lat[k].idle, 0.99), pct(lat[k].idle, 1.0), pct(lat[k].during, 0.5),
                        pct(lat[k].during, 0.99), pct(lat[k].during, 1.0), over, lat[k].during.size());
        }
        report("m01.wal_max", double(walMax.load()) / 1048576.0, "MiB");
        closeEngine(600000);
        return;
    }
#endif
    if (argInt("check-limit", 0)) {
        // EPOCH with a limit (and an offset) answers the no-limit answer's
        // first rows: OMM, MPE and CAT, every point profile.
        int bad = 0;
        for (const char* ty : {"OMM", "MPE", "CAT"})
            for (int prof : {2, 3, 4})
                for (uint64_t off : {uint64_t(0), uint64_t(137)}) {
                    TlvW all, lim;
                    all.text(1, ty).u8(30, uint8_t(prof)).i64(31, 1789371001);
                    lim.text(1, ty).u8(30, uint8_t(prof)).i64(31, 1789371001).u64(3, 200).u64(4, off);
                    Result ra = call(P4_OPC_EPOCH, all.b, co), rl = call(P4_OPC_EPOCH, lim.b, co);
                    bool same = ra.status == P4_OK && rl.status == P4_OK;
                    size_t want = ra.rows.size() > off ? std::min<size_t>(200, ra.rows.size() - off) : 0;
                    same = same && rl.rows.size() == want;
                    for (size_t i = 0; same && i < rl.rows.size(); i++) same = rl.s(i, "cid") == ra.s(i + off, "cid");
                    std::printf("  %s profile %d offset %llu: %zu of %zu rows %s\n", ty, prof, (unsigned long long)off,
                                rl.rows.size(), ra.rows.size(), same ? "equal" : "DIFFER");
                    if (!same) bad++;
                    if (off) continue;
                    for (int64_t win : {int64_t(0), int64_t(86400 * 3)}) {
                        TlvW cnt, rows;
                        cnt.text(1, ty).u8(30, uint8_t(prof)).i64(31, 1789371001).u8(33, 1);
                        rows.text(1, ty).u8(30, uint8_t(prof)).i64(31, 1789371001);
                        if (win) {
                            cnt.i64(32, win);
                            rows.i64(32, win);
                        }
                        Result rc = call(P4_OPC_EPOCH, cnt.b, co), rr = call(P4_OPC_EPOCH, rows.b, co);
                        const bool ok = rc.status == P4_OK && rc.rows.size() == 1 && rc.i(0, "n") == int64_t(rr.rows.size());
                        std::printf("  %s profile %d count (max delta %lld): %lld vs %zu rows %s\n", ty, prof, (long long)win,
                                    (long long)(rc.rows.empty() ? -1 : rc.i(0, "n")), rr.rows.size(), ok ? "equal" : "DIFFER");
                        if (!ok) bad++;
                    }
                }
        // A CID-ordered window under a lane filter = the lane's rows sorted by CID.
        for (uint64_t off : {uint64_t(0), uint64_t(1500)}) {
            TlvW all, win;
            all.text(1, "OMM").text(12, "celestrak-gp").text(13, "OMM-celestrak-gp-b052").u8(2, 1).u64(3, 1000000);
            win.text(1, "OMM").text(12, "celestrak-gp").text(13, "OMM-celestrak-gp-b052").u8(5, P4_ORDER_CID).u8(2, 1).u64(3, 1000).u64(4, off);
            Result ra = call(P4_OPC_SCAN, all.b, co), rw = call(P4_OPC_WINDOW, win.b, co);
            std::vector<std::string> cids;
            for (size_t i = 0; i < ra.rows.size(); i++) cids.push_back(ra.s(i, "cid"));
            std::sort(cids.begin(), cids.end());
            cids.erase(std::unique(cids.begin(), cids.end()), cids.end());
            bool same = ra.status == P4_OK && rw.status == P4_OK;
            const size_t want = cids.size() > off ? std::min<size_t>(1000, cids.size() - off) : 0;
            same = same && rw.rows.size() == want;
            for (size_t i = 0; same && i < rw.rows.size(); i++) same = rw.s(i, "cid") == cids[i + off];
            std::printf("  R13 OMM b052 cid window offset %llu: %zu rows of %zu lane CIDs %s\n", (unsigned long long)off,
                        rw.rows.size(), cids.size(), same ? "equal" : "DIFFER");
            if (!same) bad++;
        }
        // An unfiltered CID window at an offset = the walk from the start.
        for (auto tw : {std::make_pair("CAT", uint64_t(60000)), std::make_pair("IQC", uint64_t(100000))}) {
            TlvW all, win;
            all.text(1, tw.first).u8(5, P4_ORDER_CID).u8(2, 0).u64(3, tw.second + 200);
            win.text(1, tw.first).u8(5, P4_ORDER_CID).u8(2, 0).u64(3, 200).u64(4, tw.second);
            Result ra = call(P4_OPC_WINDOW, all.b, co), rw = call(P4_OPC_WINDOW, win.b, co);
            bool same = ra.status == P4_OK && rw.status == P4_OK && ra.rows.size() == tw.second + rw.rows.size() &&
                        rw.rows.size() == 200;
            for (size_t i = 0; same && i < rw.rows.size(); i++) same = rw.s(i, "cid") == ra.s(i + tw.second, "cid");
            std::printf("  R13 %s cid window offset %llu: %zu rows %s\n", tw.first, (unsigned long long)tw.second,
                        rw.rows.size(), same ? "equal" : "DIFFER");
            if (!same) bad++;
        }
        CHECK_EQ(bad, 0, "EPOCH limit/offset = the full answer's rows");
        closeEngine(600000);
        return;
    }
    if (const long rounds = argInt("burst", 0)) {
        // Open, then every shape at once from its own caller (M01's first
        // reads after open), --burst rounds: every answer must be P4_OK.
        int errors = 0;
        for (long round = 0; round < rounds; round++) {
            if (round > 0) REQUIRE(openEngine(store + "/fsql4", o) == P4_OK, "reopen");
            std::vector<std::thread> th;
            std::vector<Result> res(shapes.size());
            Result wr;
#if !defined(__wasm__)
            if (!argStr("bfbs", "").empty()) {
                // With a write starting at the same moment (M01's W01).
                th.emplace_back([&, round] {
                    TestType real = realType(argStr("bfbs", ""), "OMM");
                    real.schema = reflection::GetSchema(real.bfbs.data());
                    Batch b;
                    b.type = "OMM";
                    b.peer = argStr("peer", "12D3KooWBurstWriter");
                    b.tags.push_back(Tag{"space-data-network-02", "celestrak-gp", "", "OMM-celestrak-gp-burst", "", "", ""});
                    b.at = 1790700000;
                    for (int i = 0; i < 4096; i++) {
                        In in;
                        in.frame = buildFrame(real, {Field::str("OBJECT_NAME", "BURST " + std::to_string(i)), Field::str("OBJECT_ID", "2026-001A"),
                                                     Field::u64("NORAD_CAT_ID", uint64_t(1 + i)),
                                                     Field::str("EPOCH", isoTime(unixOf(2026, 10, 1) + round * 86400 + i)),
                                                     Field::f64("MEAN_MOTION", 15.5), Field::f64("ECCENTRICITY", 0.0001),
                                                     Field::f64("INCLINATION", 51.6)});
                        in.ts = 1790700000;
                        b.recs.push_back(std::move(in));
                    }
                    wr = put(b);
                });
            }
#endif
            for (size_t i = 0; i < shapes.size(); i++)
                th.emplace_back([&, i] { res[i] = call(shapes[i].op, shapes[i].t.b, co); });
            for (auto& t : th) t.join();
            if (!argStr("bfbs", "").empty() && wr.status != P4_OK) {
                errors++;
                std::printf("  round %ld: write -> %d %s\n", round, wr.status, wr.err.c_str());
            }
            for (size_t i = 0; i < shapes.size(); i++)
                if (res[i].status != P4_OK) {
                    errors++;
                    std::printf("  round %ld: %s -> %d %s\n", round, shapes[i].name.c_str(), res[i].status, res[i].err.c_str());
                }
            closeEngine(600000);
        }
        report("burst.rounds", double(rounds), "rounds");
        report("burst.errors", double(errors), "errors");
        CHECK_EQ(errors, 0, "every concurrent first read after open");
        return;
    }
    auto onlyMatch = [&](const std::string& name) {  // --only=<substring>[|<substring>...]
        if (only.empty()) return true;
        for (size_t b = 0; b <= only.size();) {
            size_t e = only.find('|', b);
            if (e == std::string::npos) e = only.size();
            if (e > b && name.find(only.substr(b, e - b)) != std::string::npos) return true;
            b = e + 1;
        }
        return false;
    };
    for (Shape& sh : shapes) {
        if (!onlyMatch(sh.name)) continue;
        std::vector<double> ms;
        size_t rows = 0;
        int32_t st = 0;
        uint64_t ex = 0;
        const int calls = int(argInt("reps", 4));
        const flatsql::FlatSqlIoVfsStats v0 = flatsql::flatSqlIoVfsStats();
        for (int i = 0; i < calls; i++) {
            const uint64_t s = flatsql::ps::monoNs();
            Result r = call(sh.op, sh.t.b, co);
            ms.push_back(double(flatsql::ps::monoNs() - s) / 1e6);
            rows = r.rows.size();
            st = r.status;
            ex = r.rowsExamined;
            if (sh.op == P4_OPC_HEAD && r.rows.size() == 1 && i == 0) rows = size_t(r.i(0, "n"));
        }
        if (!argStr("dump", "").empty() && sh.name.find(argStr("dump", "")) != std::string::npos) {
            Result r = call(sh.op, sh.t.b, co);
            for (size_t i = 0; i < r.rows.size(); i++)
            {
                std::string line;
                for (size_t k = 0; k < r.rows[i].size(); k++) {
                    const Cell& cl = r.rows[i][k];
                    line += cl.type == 1 ? std::to_string(cl.i) : cl.s;
                    line += '|';
                }
                std::printf("  DUMP %s %s\n", sh.name.c_str(), line.size() > 300 ? line.substr(0, 300).c_str() : line.c_str());
            }
        }
        std::vector<double> warm(ms.begin() + 1, ms.end());
        std::sort(warm.begin(), warm.end());
        const flatsql::FlatSqlIoVfsStats v1 = flatsql::flatSqlIoVfsStats();
        std::printf("  %-36s cold %9.3f ms  warm p50 %9.3f  max %9.3f  rows %zu  examined %llu  status %d (load %.1f)"
                    "  readahead/call %.1f MiB in %.0f reads, %.0f hits\n",
                    sh.name.c_str(), ms[0], warm[warm.size() / 2], warm.back(), rows, (unsigned long long)ex, st, loadAvg(),
                    double(v1.readaheadBytes - v0.readaheadBytes) / 1048576.0 / calls,
                    double(v1.readaheadReads - v0.readaheadReads) / calls, double(v1.readaheadHits - v0.readaheadHits) / calls);
    }
    closeEngine(600000);
}
