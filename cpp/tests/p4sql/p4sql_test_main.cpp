// p4sql tests: the framework, the harness over the fake reader, and main.
// Usage: flatsql_p4sql_fake_test [name-substring ...]
#ifdef FLATSQL_P4SQL_FAKE

#include <sqlite3.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <sstream>

#include "flatbuffers/idl.h"
#include "p4sql_test.h"

#if !defined(__wasi__)
#include <cstdlib>
#endif

namespace p4sqlt {

std::vector<Test>& registry() {
    static std::vector<Test> r;
    return r;
}

int gFailures = 0;

void failAt(const char* file, int line, const std::string& what) {
    gFailures++;
    std::fprintf(stderr, "    FAIL %s:%d: %s\n", file, line, what.c_str());
}

std::string show(int64_t v) { return std::to_string(v); }
std::string show(uint64_t v) { return std::to_string(v); }
std::string show(int v) { return std::to_string(v); }
std::string show(unsigned v) { return std::to_string(v); }
std::string show(size_t v) { return std::to_string(v); }
std::string show(const std::string& v) { return "\"" + v + "\""; }
std::string show(const char* v) { return v ? show(std::string(v)) : "null"; }
std::string show(bool v) { return v ? "true" : "false"; }

std::string env(const char* name) {
    const char* v = std::getenv(name);
    return v ? v : "";
}

void report(const char* key, double value, const char* unit) {
    double load[3] = {0, 0, 0};
#if !defined(__wasi__)
    if (getloadavg(load, 3) < 0) load[0] = -1;
#endif
    std::printf("    REPORT %s = %.3f %s (load %.2f)\n", key, value, unit, load[0]);
}

std::string vectorDir() {
    const std::string e = env("P4SQL_VECTOR_DIR");
    if (!e.empty()) return e;
#ifdef P4SQL_VECTOR_DIR
    return P4SQL_VECTOR_DIR;
#else
    return "vectors";
#endif
}

std::vector<uint8_t> readFile(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        std::fprintf(stderr, "cannot read %s\n", path.c_str());
        std::abort();
    }
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}

std::vector<uint8_t> buildRecord(const std::vector<uint8_t>& bfbs, const std::string& json, bool sizePrefixed) {
    flatbuffers::IDLOptions opts;
    opts.size_prefixed = sizePrefixed;
    flatbuffers::Parser parser(opts);
    if (!parser.Deserialize(bfbs.data(), bfbs.size()) || !parser.ParseJson(json.c_str())) {
        std::fprintf(stderr, "record build failed: %s\n", parser.error_.c_str());
        std::abort();
    }
    const uint8_t* p = parser.builder_.GetBufferPointer();
    return std::vector<uint8_t>(p, p + parser.builder_.GetSize());
}

rb1::Cell cInt(int64_t v) {
    rb1::Cell c;
    c.type = rb1::kInt;
    c.i = v;
    return c;
}
rb1::Cell cReal(double v) {
    rb1::Cell c;
    c.type = rb1::kReal;
    c.d = v;
    return c;
}
rb1::Cell cText(const std::string& v) {
    rb1::Cell c;
    c.type = rb1::kText;
    c.s = v;
    return c;
}
rb1::Cell cBlob(const std::vector<uint8_t>& v) {
    rb1::Cell c;
    c.type = rb1::kBlob;
    c.s.assign(v.begin(), v.end());
    return c;
}
rb1::Cell cNull() { return rb1::Cell(); }

std::string showCell(const rb1::Cell& c) {
    switch (c.type) {
        case rb1::kNull: return "NULL";
        case rb1::kInt: return std::to_string(c.i);
        case rb1::kReal: {
            std::ostringstream o;
            o.precision(17);
            o << c.d;
            return o.str();
        }
        case rb1::kText: return "'" + c.s + "'";
        default: return "blob[" + std::to_string(c.s.size()) + "]";
    }
}

std::string showRow(const std::vector<rb1::Cell>& r) {
    std::string o = "(";
    for (size_t i = 0; i < r.size(); i++) o += (i ? ", " : "") + showCell(r[i]);
    return o + ")";
}

// ---- harness ---------------------------------------------------------------------
Harness::Harness() {
    lane.engine = &engine;
    init_ = p4sql_lane_init(&lane);
}

Harness::~Harness() { p4sql_lane_free(&lane); }

p4fake::Type& Harness::addType(const std::string& name, const char fid[4], const std::vector<uint8_t>& bfbs,
                               uint64_t bound) {
    p4fake::Type& t = engine.types[name];
    t.name = name;
    std::memcpy(t.fid, fid, 4);
    t.bfbs = bfbs;
    t.bound = bound;
    return t;
}

Result Harness::finish(int32_t status, bool raw) {
    Result r;
    r.status = status;
    r.error = lane.err;
    r.raw = lane.out;
    r.rowsExamined = lane.rowsExamined;
    r.bytesRead = lane.bytesRead;
    r.opened = lane.opened;
    if (!raw) {
        rb1::Decoder dec;
        if (dec.feed(lane.out.data(), lane.out.size())) {
            r.names = dec.names();
            r.rows = dec.rows();
            r.ended = dec.done();
            r.endStatus = dec.status();
            r.endRows = dec.rowsReported();
        }
    }
    return r;
}

Result Harness::sql(const std::string& sql, const std::vector<rb1::Cell>& params, uint32_t flags, const Caps& caps) {
    lane.resetSlot();
    lane.maxRowsExamined = caps.maxRowsExamined;
    lane.maxBytesRead = caps.maxBytesRead;
    lane.maxResultBytes = caps.slotMaxResultBytes;
    lane.cancelAfterRows = caps.cancelAfterRows;
    std::vector<uint8_t> pb;
    rb1::encodeParams(params, &pb);
    P4SqlRequest req;
    std::memset(&req, 0, sizeof(req));
    req.sql = sql.data();
    req.sqlLen = uint32_t(sql.size());
    req.params = pb.data();
    req.paramsLen = uint32_t(pb.size());
    req.flags = flags;
    req.maxResultRows = caps.maxResultRows;
    req.maxResultBytes = caps.maxResultBytes;
    const int32_t st = p4sql_exec(&lane, &req);
    return finish(st, (flags & P4_SLOT_RAW) != 0);
}

Result Harness::surface() {
    lane.resetSlot();
    return finish(p4sql_surface(&lane), false);
}

}  // namespace p4sqlt

int main(int argc, char** argv) {
    if (p4sql_global_init() != P4_OK) {
        std::fprintf(stderr, "p4sql_global_init failed\n");
        return 2;
    }
    if (p4sqlt::env("P4SQL_MEMSTATUS") == "0") sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);
    sqlite3_initialize();
    int ran = 0, failedTests = 0;
    for (const p4sqlt::Test& t : p4sqlt::registry()) {
        bool want = argc <= 1;
        for (int i = 1; i < argc; i++)
            if (std::strstr(t.name, argv[i])) want = true;
        if (!want) continue;
        const int before = p4sqlt::gFailures;
        std::printf("RUN  %s\n", t.name);
        std::fflush(stdout);
        t.fn();
        ran++;
        if (p4sqlt::gFailures != before) {
            failedTests++;
            std::printf("FAIL %s\n", t.name);
        } else {
            std::printf("OK   %s\n", t.name);
        }
        std::fflush(stdout);
    }
    std::printf("%d tests, %d failed\n", ran, failedTests);
    return failedTests ? 1 : 0;
}

#endif  // FLATSQL_P4SQL_FAKE
