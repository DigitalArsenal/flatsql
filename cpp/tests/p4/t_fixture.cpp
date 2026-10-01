// G2 on the host-02-sized fixture (format 1, 4,364,873 copies of 3,945,845
// records): load it the way store-migrate does (migrate-mode PUTs keeping
// format 1's rowids and tag instances, then REBUILD 1), measure bytes per copy
// (< 800), and check answers against format 1 per shape. Native only (it
// reads format 1 with SQLite directly, as the oracle). Skips cleanly without
// --fixture.
//
//   flatsql_p4_test --test=g2_fixture --fixture=<control.flatsqldb>
//       --bfbs=<sdn-server/internal/sds/search-schemas> [--store=<dir>] [--keep=1]
#if !defined(__wasm__)
#include <sys/stat.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <map>
#include <set>
#include <chrono>
#include <thread>

#include "internal.h"
#include "p4/p4_test.h"

using namespace p4t;
namespace fp = flatsql::p4;

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
        t.rules = "bucket str:CAPTURE_START\n";
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
    CHECK(double(onDisk) / double(copiesF1) < 800.0, "under 800 B per copy");

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
