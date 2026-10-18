// The weather node's WXF sea lane as the format-4 tests build it: the type sdn
// registers, its tile frames, the migration path that builds a large feed and
// the retention page's SCAN (p4_record_scaling, p4_replay).
#ifndef FLATSQL_P4_WXF_FIXTURE_H
#define FLATSQL_P4_WXF_FIXTURE_H

#include <cstring>
#include <string>
#include <vector>

#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/idl.h>
#include <flatbuffers/reflection.h>

#include "flatsql/ps/result_block.h"
#include "internal.h"
#include "p4/p4_test.h"

namespace p4t {
namespace wxf {

inline const char* kWxfSchema = R"(
namespace sds;
table WXF {
  GRID_ID: string;
  FIELD: string;
  LEAD: string;
  VALUES: [ubyte];
}
root_type WXF;
file_identifier "$WXF";
)";

// The WXF registration sdn makes: no extraction rules, no full text.
inline TestType& wxfType() {
    static TestType t = [] {
        TestType x;
        x.name = "WXF";
        std::memcpy(x.fid, "$WXF", 4);
        flatbuffers::Parser parser;
        if (!parser.Parse(kWxfSchema)) {
            std::fprintf(stderr, "WXF schema: %s\n", parser.error_.c_str());
            std::abort();
        }
        parser.Serialize();
        x.bfbs.assign(parser.builder_.GetBufferPointer(), parser.builder_.GetBufferPointer() + parser.builder_.GetSize());
        x.fullText = false;
        return x;
    }();
    if (!t.schema) t.schema = reflection::GetSchema(t.bfbs.data());
    return t;
}

inline const char* kProvider = "noaa-ncep";
inline const char* kBig = "gfs-0p25-sea";
inline const char* kSmall = "gfs-0p25-small";
inline const char* kUrl = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod";
inline const char* kPeer = "12D3KooWScalingFixturePeer";
inline const char* kFields[9] = {"TMP", "UGRD", "VGRD", "PRMSL", "HGT", "RH", "APCP", "TCDC", "GUST"};
inline const int64_t kT0 = 1791763200;  // 2026-10-12T00:00:00Z, the fixture's first arrival

inline std::vector<uint8_t> wxfFrame(const std::string& grid, int field, const std::string& lead, const std::vector<uint8_t>& values) {
    return buildFrame(wxfType(), {Field::str("GRID_ID", grid), Field::str("FIELD", kFields[field]), Field::str("LEAD", lead),
                                  Field::raw("VALUES", values)});
}

inline std::vector<uint8_t> payloadOf(size_t n) {
    std::vector<uint8_t> v(n);
    uint64_t x = 0x9E3779B97F4A7C15ull;
    for (size_t i = 0; i < n; i++) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v[i] = uint8_t(x);
    }
    return v;
}

inline std::vector<uint8_t> cidList(const std::vector<std::vector<uint8_t>>& cids, size_t from, size_t n) {
    std::vector<uint8_t> c(4);
    flatsql::p4::st32(c.data(), uint32_t(n));
    for (size_t i = from; i < from + n; i++) c.insert(c.end(), cids[i].begin(), cids[i].end());
    return c;
}

inline std::vector<uint8_t> cidOfText(const std::string& text) {
    std::vector<uint8_t> c = {0x01, 0x55, 0x12, 0x20};
    uint8_t d[32];
    if (!flatsql::p4::cidDigestFromText(text.data(), text.size(), d)) return {};
    c.insert(c.end(), d, d + 32);
    return c;
}

// One tile of a lead: nine records (one per field) in one tile batch.
inline Batch tileBatch(const std::string& source, const std::string& batch, const std::string& lead, int64_t tile,
                       int64_t ts, const std::vector<uint8_t>& values) {
    Batch b;
    b.type = "WXF";
    b.peer = kPeer;
    b.at = ts;
    b.tags.push_back(Tag{kProvider, source, kUrl, batch, "", "", ""});
    for (int f = 0; f < 9; f++) {
        In in;
        in.frame = wxfFrame(source + "/" + lead + "/tile-" + std::to_string(tile), f, lead, values);
        in.ts = ts;
        b.recs.push_back(std::move(in));
    }
    return b;
}

// The migration path: the lane's tiles as historical records (explicit seqs,
// one tile batch each), many tiles a PUT.
inline bool migrateLane(const std::string& source, int64_t tiles, int64_t* seq, const std::vector<uint8_t>& values, size_t perPut,
                 int64_t* bytes) {
    Batch b;
    auto flush = [&]() {
        if (b.recs.empty()) return true;
        Result r = put(b);
        bool ok = r.status == P4_OK && r.rows.size() == b.recs.size();
        for (size_t i = 0; ok && i < r.rows.size(); i++) ok = r.i(i, "action") != P4_ACT_REJECTED;
        if (!ok) std::fprintf(stderr, "  migrate %s: PUT %d %s\n", source.c_str(), r.status, r.err.c_str());
        b.recs.clear();
        b.tags.clear();
        return ok;
    };
    for (int64_t tile = 0; tile < tiles; tile++) {
        if (b.recs.size() + 9 > perPut && !flush()) return false;
        if (b.recs.empty()) {
            b.type = "WXF";
            b.peer = kPeer;
            b.mode = 1;
        }
        const int64_t ts = kT0 + tile * 3;
        const uint16_t idx = uint16_t(b.tags.size());
        b.tags.push_back(Tag{kProvider, source, kUrl, "seed-tile-" + std::to_string(tile), "", "", ""});
        for (int f = 0; f < 9; f++) {
            In in;
            in.frame = wxfFrame(source + "/seed/tile-" + std::to_string(tile), f, "seed", values);
            in.ts = ts;
            in.seq = (*seq)++;
            in.tags.push_back({idx, ts});
            *bytes += int64_t(in.frame.size());
            b.recs.push_back(std::move(in));
        }
    }
    return flush();
}

// A retention page's SCAN (sdn's ExpireRecords on format 4): the lane in seq
// order below `before` (record ts), through the type's visible seq, after the
// previous page's last seq, 501 rows.
inline std::vector<uint8_t> retentionScan(const std::string& source, int64_t before, int64_t through, int64_t after) {
    std::vector<flatsql::ps::rb1::Cell> cells(1);
    cells[0].type = flatsql::ps::rb1::kInt;
    cells[0].i = before;
    std::vector<uint8_t> enc;
    flatsql::ps::rb1::encodeParams(cells, &enc);
    std::vector<uint8_t> pred = {uint8_t(P4_F_TS), uint8_t(P4_OP_LT), 1, 0};
    pred.insert(pred.end(), enc.begin() + 4, enc.end());
    TlvW sc;
    sc.text(1, "WXF").u64(3, 501).u8(5, P4_ORDER_SEQ_ASC);
    if (after) sc.i64(6, after);
    sc.i64(7, through).text(11, kProvider).text(12, source).raw(17, pred.data(), pred.size());
    return sc.b;
}

}  // namespace wxf
}  // namespace p4t

#endif
