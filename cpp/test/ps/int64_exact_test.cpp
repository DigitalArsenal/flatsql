// RB1 integers are exact (T2 acceptance #3; design §2: today every integer
// result reaches Go as a float64). -2^63, 2^53+1 and 2^63-1 round-trip
// through the C++ encoder, through SQL on a lane (parameters in, cells out),
// and match the golden byte vector T6's Go decoder is tested against
// (cpp/test/ps/vectors/rb1_int64.hex).
#include <cstdio>
#include <fstream>
#include <sstream>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {
const int64_t kVals[] = {INT64_MIN, (int64_t(1) << 53) + 1, INT64_MAX, -((int64_t(1) << 53) + 1), 0, -1};

std::string hexOf(const std::vector<uint8_t>& b) {
    static const char* d = "0123456789abcdef";
    std::string o;
    for (uint8_t x : b) {
        o.push_back(d[x >> 4]);
        o.push_back(d[x & 15]);
    }
    return o;
}

std::vector<uint8_t> goldenStream() {
    std::vector<uint8_t> out;
    rb1::Encoder e(&out);
    e.header({"min", "p53", "max", "n53", "zero", "neg1"});
    e.beginRow();
    for (int64_t v : kVals) e.i64(v);
    e.endRow();
    e.beginRow();
    e.real(9007199254740993.0);  // the float64 of 2^53+1 is 2^53: a REAL cell keeps what it holds
    e.text("t", 1);
    e.blob("\x00\x01", 2);
    e.null();
    e.i64(1);
    e.i64(2);
    e.endRow();
    e.end(0, 2, 0, 0);
    return out;
}
}  // namespace

PS_TEST(int64_rb1_encoder_decoder_exact_T2_3) {
    const std::vector<uint8_t> bytes = goldenStream();
    // Byte-split decoding (any chunking gives the same rows).
    for (size_t chunk : {size_t(1), size_t(3), size_t(7), bytes.size()}) {
        rb1::Decoder d;
        bool ok = true;
        for (size_t at = 0; at < bytes.size() && ok; at += chunk)
            ok = d.feed(bytes.data() + at, std::min(chunk, bytes.size() - at));
        CHECK(ok);
        CHECK(d.done());
        REQUIRE(d.rows().size() == 2);
        for (size_t i = 0; i < 6; i++) {
            CHECK_EQ(int(d.rows()[0][i].type), int(rb1::kInt));
            CHECK(d.rows()[0][i].i == kVals[i]);
        }
        CHECK(d.rows()[1][0].d == 9007199254740992.0);
        CHECK(d.rows()[1][2].s == std::string("\x00\x01", 2));
        CHECK_EQ(d.rowsReported(), uint64_t(2));
    }
    // The golden vector (T6: the Go decoder decodes exactly these bytes).
    std::ifstream f(std::string(PS_VECTOR_DIR) + "/rb1_int64.hex");
    std::stringstream ss;
    ss << f.rdbuf();
    std::string golden = ss.str();
    while (!golden.empty() && (golden.back() == '\n' || golden.back() == '\r')) golden.pop_back();
    if (golden != hexOf(bytes)) std::fprintf(stderr, "  encoder bytes: %s\n", hexOf(bytes).c_str());
    CHECK(golden == hexOf(bytes));
}

PS_TEST(int64_exact_through_sql_lane_T2_3) {
    Store s(false, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    Reader r(s, LaneClass::Interactive, 1);
    REQUIRE(r.inst);
    std::vector<Param> ps;
    for (int64_t v : kVals) ps.push_back(Param::i64(v));
    Rows q = r.q("SELECT ?1, ?2, ?3, ?4, ?5, ?6", ps);
    CHECK_EQ(q.status, 0);
    REQUIRE(q.rows.size() == 1);
    for (size_t i = 0; i < 6; i++) CHECK(q.rows[0][i].type == rb1::kInt && q.rows[0][i].i == kVals[i]);
    q = r.q("SELECT -9223372036854775808, 9007199254740993, 9223372036854775807, 9007199254740993 + 0");
    CHECK_EQ(q.status, 0);
    REQUIRE(q.rows.size() == 1);
    CHECK(q.i(0, 0) == INT64_MIN);
    CHECK(q.i(0, 1) == (int64_t(1) << 53) + 1);
    CHECK(q.i(0, 2) == INT64_MAX);
    CHECK(q.i(0, 3) == (int64_t(1) << 53) + 1);
    s.close();
}
