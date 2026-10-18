// Per-record write cost against feed size (flatsql-record-whole-map-copy-20261017).
//
// The weather node's sea lane is one WXF feed file of ~16 GB: tile batches of
// nine grid fields, ~190k batches, so ~190k instances in the feed's instance
// table and dictionary. Retention deletes it in pages of 500 CIDs (SCAN by
// seq, then DELETE), and a GFS lead arrives as 512 tile PUTs of nine records.
// A record must cost the same on that feed as on a small one.
//
// p4_record_scaling builds the large feed through the migration path
// (--bytes of stream, default 15 GB, --payload bytes a record) and a small
// feed of the same type and lane shape (--small-tiles), then on each times one
// lead's tile PUTs and --pages retention pages, then retention pages on the
// large feed while a lead writes to each feed. It checks every record a lead
// wrote (bytes, provenance), every record a page deleted (gone), the type's
// counts, the instance summary after a reopen against the one before, and
// REBUILD 8 (every file's counters, instances and tokens recounted from its
// rows and stream). It prints a digest of the serial phase's query results
// (the instance summary without the clock's restamps, the leads' records with
// their provenance) so two builds can be compared byte for byte on the same
// fixture.
// --accept=1 holds the targets: the large feed's median retention page within
// --page-ms (200 ms), alone and while both lanes write; its per-record PUT
// cost within --ratio (2x) of the small feed's; its DELETE page within
// --delete-ratio (4x) of the small feed's (a small feed's deleted rows share
// index pages, a large feed's each touch their own: a bounded difference,
// where a cost that grows with the feed is not).
// --store=<dir> keeps the fixture there; --reuse=1 measures an existing one
// (an APFS clone of a kept fixture: cp -c); --build-only=1 stops after it.
#include <sys/stat.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/idl.h>
#include <flatbuffers/reflection.h>

#include "flatsql/ps/platform.h"
#include "flatsql/ps/result_block.h"
#include "internal.h"
#include "p4/p4_test.h"
#include "p4/wxf_fixture.h"

using namespace p4t;
using namespace p4t::wxf;
namespace fp = flatsql::p4;
namespace rb1 = flatsql::ps::rb1;

namespace {

double secs(uint64_t a, uint64_t b) { return double(b - a) / 1e9; }

double median(std::vector<double> v) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}

std::string hexOf(const uint8_t* p, size_t n) {
    static const char* d = "0123456789abcdef";
    std::string s;
    for (size_t i = 0; i < n; i++) {
        s += d[p[i] >> 4];
        s += d[p[i] & 15];
    }
    return s;
}


// One lead's record as written: its CID, frame and tile batch.
struct Written {
    std::vector<uint8_t> cid, frame;
    std::string batch;
};

// What one lane's measured work cost.
struct Arm {
    std::vector<double> put;              // per tile PUT
    std::vector<double> scan, del, page;  // per retention page
    int64_t putRecs = 0, delRecs = 0;
    std::vector<Written> wrote;
    std::vector<std::vector<uint8_t>> deleted;
};

Result tilePut(const std::string& source, const std::string& batch, const std::string& lead, int64_t tile, int64_t ts,
               const std::vector<uint8_t>& values, std::vector<Written>* wrote) {
    Batch b = tileBatch(source, batch, lead, tile, ts, values);
    if (wrote)
        for (const In& in : b.recs) {
            Written w;
            uint8_t cid[36];
            cidOf(in.frame, cid);
            w.cid.assign(cid, cid + 36);
            w.frame = in.frame;
            w.batch = batch;
            wrote->push_back(std::move(w));
        }
    return put(b);
}

// A retention pass's pages on one lane (sdn's ExpireRecords on format 4: SCAN
// of the lane in seq order below the pass's high-water mark, 501 rows, then
// DELETE of the page's CIDs); with `writing`, up to `pages` while it is set.
void retentionPages(const std::string& source, int pages, int64_t before, Arm* a,
                    const std::atomic<int>* writing = nullptr) {
    TlvW hw;
    hw.text(1, "WXF");
    Result h = call(P4_OPC_HEAD, hw.b);
    REQUIRE(h.status == P4_OK && h.rows.size() == 1, "HEAD: " + h.err);
    const int64_t through = h.i(0, "through");
    int64_t after = 0;
    for (int p = 0; p < pages && (!writing || p == 0 || writing->load() > 0); p++) {
        CallOpts bulk;
        bulk.cls = P4_CLASS_BULK;
        const uint64_t t0 = flatsql::ps::monoNs();
        Result s = call(P4_OPC_SCAN, retentionScan(source, before, through, after), bulk);
        const uint64_t t1 = flatsql::ps::monoNs();
        REQUIRE(s.status == P4_OK, "SCAN page: " + s.err);
        if (s.rows.empty()) break;
        const size_t n = std::min<size_t>(s.rows.size(), 500);
        std::vector<std::vector<uint8_t>> cids;
        for (size_t i = 0; i < n; i++) {
            cids.push_back(cidOfText(s.s(i, "cid")));
            REQUIRE(cids.back().size() == 36, "page CID " + s.s(i, "cid"));
            after = std::max(after, s.i(i, "seq"));
        }
        TlvW dw;
        dw.text(1, "WXF");
        const std::vector<uint8_t> cl = cidList(cids, 0, cids.size());
        dw.raw(40, cl.data(), cl.size());
        Result d = call(P4_OPC_DELETE, dw.b);
        const uint64_t t2 = flatsql::ps::monoNs();
        REQUIRE(d.status == P4_OK && d.rows.size() == 1, "DELETE page: " + d.err);
        CHECK_EQ(d.i(0, "deleted"), int64_t(n), "a page deletes its CIDs (one copy each)");
        a->scan.push_back(secs(t0, t1));
        a->del.push_back(secs(t1, t2));
        a->page.push_back(secs(t0, t2));
        a->delRecs += int64_t(n);
        for (auto& c : cids) a->deleted.push_back(std::move(c));
        if (s.rows.size() <= 500) break;
    }
}

void lead(const std::string& source, const std::string& name, int64_t tiles, int64_t ts, const std::vector<uint8_t>& values,
          Arm* a) {
    for (int64_t tile = 0; tile < tiles; tile++) {
        const uint64_t t0 = flatsql::ps::monoNs();
        Result r = tilePut(source, name + "-tile-" + std::to_string(tile), name, tile, ts, values, &a->wrote);
        a->put.push_back(secs(t0, flatsql::ps::monoNs()));
        REQUIRE(r.status == P4_OK && r.rows.size() == 9, "tile PUT: " + r.err);
        for (size_t i = 0; i < r.rows.size(); i++) CHECK_EQ(r.i(i, "action"), int64_t(P4_ACT_NEW), "a lead record is new");
        a->putRecs += 9;
    }
}

// SUMMARY 3 (one row per feed instance) as canonical text: everything but
// the clock's restamp time.
std::vector<std::string> instances(int64_t* n) {
    TlvW w;
    w.u8(45, 3).text(1, "WXF");
    Result r = call(P4_OPC_SUMMARY, w.b);
    std::vector<std::string> out;
    *n = 0;
    if (r.status != P4_OK) return out;
    for (size_t i = 0; i < r.rows.size(); i++) {
        std::string s;
        for (const char* c : {"provider", "source", "batch", "content_key_id", "producer_peer", "producer_pubkey", "source_url"})
            s += r.s(i, c) + "|";
        for (const char* c : {"records", "bytes", "max_seq", "first", "min_w", "max_w"}) s += std::to_string(r.i(i, c)) + "|";
        *n += r.i(i, "records");
        out.push_back(std::move(s));
    }
    std::sort(out.begin(), out.end());
    return out;
}

int64_t records() {
    TlvW w;
    w.u8(45, 1).text(1, "WXF");
    Result r = call(P4_OPC_SUMMARY, w.b);
    return r.status == P4_OK && r.rows.size() == 1 ? r.i(0, "records") : -1;
}

void reportArm(const std::string& lane, const Arm& a) {
    double put = 0, page = 0;
    for (double x : a.put) put += x;
    for (double x : a.page) page += x;
    p4t::report(("scaling." + lane + ".lead.seconds").c_str(), put, "s");
    p4t::report(("scaling." + lane + ".lead.tile_median").c_str(), median(a.put), "s");
    p4t::report(("scaling." + lane + ".lead.tile_max").c_str(), a.put.empty() ? 0 : *std::max_element(a.put.begin(), a.put.end()), "s");
    p4t::report(("scaling." + lane + ".lead.per_record").c_str(), a.putRecs ? put / double(a.putRecs) : 0, "s");
    p4t::report(("scaling." + lane + ".page.median").c_str(), median(a.page), "s");
    p4t::report(("scaling." + lane + ".page.scan_median").c_str(), median(a.scan), "s");
    p4t::report(("scaling." + lane + ".page.delete_median").c_str(), median(a.del), "s");
    p4t::report(("scaling." + lane + ".page.max").c_str(), a.page.empty() ? 0 : *std::max_element(a.page.begin(), a.page.end()), "s");
    p4t::report(("scaling." + lane + ".delete.per_record").c_str(), a.delRecs ? page / double(a.delRecs) : 0, "s");
}

}  // namespace

P4_SLOW_TEST(p4_record_scaling) {
    const int64_t want = std::strtoll(argStr("bytes", "15000000000").c_str(), nullptr, 10);  // long is 32 bits on wasm32
    const size_t payload = size_t(argInt("payload", 8616));
    const int64_t smallTiles = argInt("small-tiles", 200);
    const int64_t leadTiles = argInt("lead-tiles", 512);
    const int pages = int(argInt("pages", 10));
    const std::string root = argStr("store", scratchDir("scaling")) + "/fsql4";
    const std::vector<uint8_t> values = payloadOf(payload);
    const int64_t frameBytes = int64_t(wxfFrame(std::string(kBig) + "/seed/tile-0", 0, "seed", values).size());
    const int64_t bigTiles = std::max<int64_t>(1, want / (frameBytes + 4) / 9);
    EngineOpts o;
    o.writeReqBytes = 16u << 20;
    o.writeSlots = 8;
    struct stat stStore;
    const bool reuse = argInt("reuse", 0) != 0 && ::stat((root + "/STORE").c_str(), &stStore) == 0;
    if (!reuse) {
        EngineOpts m = o;
        m.createMode = 2;
        m.gseqFloor = uint64_t((bigTiles + smallTiles) * 9) + 1 + (1u << 20);
        REQUIRE(openEngine(root, m) == P4_OK, "open the fixture");
        REQUIRE(registerType(wxfType()) == P4_OK, "register WXF");
        const uint64_t t0 = flatsql::ps::monoNs();
        int64_t seq = 1, bytes = 0;
        REQUIRE(migrateLane(kBig, bigTiles, &seq, values, 1024, &bytes), "the large feed");
        REQUIRE(migrateLane(kSmall, smallTiles, &seq, values, 1024, &bytes), "the small feed");
        p4t::report("scaling.fixture.records", double(seq - 1), "records");
        p4t::report("scaling.fixture.tiles", double(bigTiles + smallTiles), "batches");
        p4t::report("scaling.fixture.frame_bytes", double(bytes), "B");
        TlvW r1;
        r1.u32(63, 1);
        CallOpts slow;
        slow.timeoutMs = 3600000;
        CHECK_EQ(call(P4_OPC_REBUILD, r1.b, slow).status, P4_OK, "REBUILD 1");
        CHECK_EQ(flatsql_p4_activate(), P4_OK, "activate");
        closeEngine(600000);
        p4t::report("scaling.fixture.seconds", secs(t0, flatsql::ps::monoNs()), "s");
        if (argInt("build-only", 0)) return;
    }
    REQUIRE(openEngine(root, o) == P4_OK, "open");
    const int64_t before0 = records();
    REQUIRE(before0 > 0, "the fixture's records");
    p4t::report("scaling.large.tiles", double(bigTiles), "batches");
    // One tile on each feed first: the measured work starts with the feed's
    // writer, dictionary and instances loaded, as on a running node.
    const int64_t tLead = kT0 + 5 * 86400;
    Arm warm, big, small;
    lead(kBig, "warm", 1, tLead, values, &warm);
    lead(kSmall, "warm", 1, tLead, values, &warm);
    lead(kBig, "lead", leadTiles, tLead, values, &big);
    lead(kSmall, "lead", leadTiles, tLead, values, &small);
    const int64_t before = kT0 + 4 * 86400;  // the seeds are past the window; the leads are not
    retentionPages(kBig, pages, before, &big);
    retentionPages(kSmall, pages, before, &small);
    reportArm("large", big);
    reportArm("small", small);
    // The serial work's instance summary, for the digest (the concurrent
    // phase's seqs interleave as the threads run).
    int64_t serialN = 0;
    const std::vector<std::string> serialInst = instances(&serialN);
    const size_t serialWrote = warm.wrote.size() + big.wrote.size() + small.wrote.size();
    // Retention while the lanes write (the weather node's ten-minute ticks): a
    // lead into the large feed and one into the small feed (the imager, the
    // same type's other lane), each tile acknowledged before the next, while
    // pages delete from the large feed. The type has one writer: a page waits
    // for the rounds before it, and the tiles for the page.
    Arm cBig, cSmall, cDel;
    {
        std::atomic<int> writing{2};
        std::thread a([&] {
            lead(kBig, "conc", leadTiles, tLead, values, &cBig);
            writing--;
        });
        std::thread b([&] {
            lead(kSmall, "conc", leadTiles, tLead, values, &cSmall);
            writing--;
        });
        const uint64_t t0 = flatsql::ps::monoNs();
        retentionPages(kBig, int(argInt("concurrent-pages", 400)), before, &cDel, &writing);
        const double dt = secs(t0, flatsql::ps::monoNs());
        a.join();
        b.join();
        reportArm("concurrent.large", cBig);
        reportArm("concurrent.small", cSmall);
        reportArm("concurrent.retention", cDel);
        p4t::report("scaling.concurrent.retention.records_per_s", double(cDel.delRecs) / std::max(1e-9, dt), "records/s");
    }
    const double ingestRatio = (median(big.put) / 9.0) / std::max(1e-9, median(small.put) / 9.0);
    const double pageRatio = median(big.del) / std::max(1e-9, median(small.del));
    p4t::report("scaling.ratio.put_per_record", ingestRatio, "large/small");
    p4t::report("scaling.ratio.delete_page", pageRatio, "large/small");

    // Every record the leads wrote: its bytes and provenance; every record a
    // page deleted: gone.
    std::vector<Written> wrote = warm.wrote;
    wrote.insert(wrote.end(), big.wrote.begin(), big.wrote.end());
    wrote.insert(wrote.end(), small.wrote.begin(), small.wrote.end());
    wrote.insert(wrote.end(), cBig.wrote.begin(), cBig.wrote.end());
    wrote.insert(wrote.end(), cSmall.wrote.begin(), cSmall.wrote.end());
    uint8_t digest[32];
    std::string canon;
    int64_t bad = 0;
    for (size_t at = 0; at < wrote.size(); at += 256) {
        const size_t n = std::min<size_t>(256, wrote.size() - at);
        std::vector<std::vector<uint8_t>> cids;
        for (size_t i = at; i < at + n; i++) cids.push_back(wrote[i].cid);
        Result g = get("WXF", cids, true);
        REQUIRE(g.status == P4_OK, "GET leads: " + g.err);
        TlvW tw;
        tw.text(1, "WXF");
        const std::vector<uint8_t> cl = cidList(cids, 0, cids.size());
        tw.raw(40, cl.data(), cl.size());
        Result tg = call(P4_OPC_TAGS, tw.b);
        REQUIRE(tg.status == P4_OK, "TAGS leads: " + tg.err);
        std::map<std::string, size_t> row, tag;
        for (size_t i = 0; i < g.rows.size(); i++) row[g.s(i, "cid")] = i;
        for (size_t i = 0; i < tg.rows.size(); i++) tag[tg.s(i, "cid")] = i;
        CHECK_EQ(tg.rows.size(), n, "one tag a lead record");
        for (size_t i = at; i < at + n; i++) {
            const std::string cid = cidText(wrote[i].cid.data());
            auto it = row.find(cid);
            auto tt = tag.find(cid);
            if (it == row.end() || tt == tag.end()) {
                bad++;
                continue;
            }
            const size_t k = it->second, j = tt->second;
            const std::string data(wrote[i].frame.begin() + 4, wrote[i].frame.end());
            if (g.s(k, "data") != data || g.s(k, "peer") != kPeer || g.i(k, "ts") != tLead || tg.s(j, "provider") != kProvider ||
                tg.s(j, "batch") != wrote[i].batch || tg.s(j, "source_url") != kUrl || tg.i(j, "at") != tLead) {
                if (bad < 3)
                    std::fprintf(stderr, "  lead record %s: data %d provider %s batch %s/%s url %s peer %s ts %lld at %lld\n",
                                 cid.c_str(), int(g.s(k, "data") == data), tg.s(j, "provider").c_str(), tg.s(j, "batch").c_str(),
                                 wrote[i].batch.c_str(), tg.s(j, "source_url").c_str(), g.s(k, "peer").c_str(),
                                 (long long)g.i(k, "ts"), (long long)tg.i(j, "at"));
                bad++;
            }
            if (i < serialWrote)
                canon += cid + "|" + std::to_string(g.i(k, "seq")) + "|" + tg.s(j, "source") + "|" + tg.s(j, "batch") + "|" +
                         std::to_string(tg.i(j, "at")) + "\n";
        }
    }
    CHECK_EQ(bad, int64_t(0), "every lead record reads back with its bytes and provenance");
    std::vector<std::vector<uint8_t>> gone = big.deleted;
    gone.insert(gone.end(), small.deleted.begin(), small.deleted.end());
    gone.insert(gone.end(), cDel.deleted.begin(), cDel.deleted.end());
    int64_t still = 0;
    for (size_t at = 0; at < gone.size(); at += 500) {
        const size_t n = std::min<size_t>(500, gone.size() - at);
        std::vector<std::vector<uint8_t>> cids(gone.begin() + long(at), gone.begin() + long(at + n));
        Result g = get("WXF", cids, false);
        REQUIRE(g.status == P4_OK, "GET deleted: " + g.err);
        still += int64_t(g.rows.size());
    }
    CHECK_EQ(still, int64_t(0), "every deleted record is gone");
    const int64_t expect = before0 + int64_t(wrote.size()) - int64_t(gone.size());
    CHECK_EQ(records(), expect, "the type's records");
    int64_t instN = 0;
    const std::vector<std::string> inst = instances(&instN);
    CHECK_EQ(instN, expect, "one instance row a record (one tile batch each)");
    closeEngine(600000);

    // After a reopen: the instances the files hold are the ones the engine
    // kept, and REBUILD 8 recounts every file from its rows.
    REQUIRE(openEngine(root, o) == P4_OK, "reopen");
    int64_t instN2 = 0;
    const std::vector<std::string> inst2 = instances(&instN2);
    CHECK(inst == inst2, "the instance summary after a reopen");
    CHECK_EQ(records(), expect, "the type's records after a reopen");
    TlvW v8;
    v8.u32(63, 8);
    CallOpts slow;
    slow.timeoutMs = 3600000;
    const uint64_t tv = flatsql::ps::monoNs();
    Result rb = call(P4_OPC_REBUILD, v8.b, slow);
    p4t::report("scaling.verify.seconds", secs(tv, flatsql::ps::monoNs()), "s");
    REQUIRE(rb.status == P4_OK, "REBUILD 8: " + rb.err);
    for (size_t i = 0; i < rb.rows.size(); i++) CHECK_EQ(rb.i(i, "mismatches"), int64_t(0), "REBUILD 8 " + rb.s(i, "type"));
    closeEngine(600000);
    for (const std::string& s : serialInst) canon += s + "\n";
    flatsql::ps::sha256(reinterpret_cast<const uint8_t*>(canon.data()), canon.size(), digest);
    std::printf("  scaling.digest %s (the serial phase: %zu instance rows, %zu lead records)\n", hexOf(digest, 32).c_str(),
                serialInst.size(), serialWrote);

    if (argInt("accept", 0)) {
        const double pageS = double(argInt("page-ms", 200)) / 1000.0;
        const double ratio = double(argInt("ratio", 2)), delRatio = double(argInt("delete-ratio", 4));
        CHECK(median(big.page) <= pageS,
              "the large feed's retention page within " + std::to_string(pageS) + " s: " + std::to_string(median(big.page)));
        CHECK(ingestRatio <= ratio, "a large-feed PUT record within " + std::to_string(ratio) + "x of a small-feed one: " +
                                        std::to_string(ingestRatio));
        CHECK(median(cDel.page) <= pageS, "the large feed's retention page within " + std::to_string(pageS) +
                                              " s while both lanes write: " + std::to_string(median(cDel.page)));
        CHECK(pageRatio <= delRatio, "a large-feed DELETE page within " + std::to_string(delRatio) + "x of a small-feed one: " +
                                         std::to_string(pageRatio));
    }
}
