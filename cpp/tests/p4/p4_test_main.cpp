// Store format 4 tests: the framework, fixtures and the in-process client.
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/idl.h>
#include <flatbuffers/reflection.h>

#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>

#include "flatsql/ps/platform.h"
#include "internal.h"
#include "p4/p4_test.h"

namespace flatsql {
namespace p4 {
P4Engine* currentEngine();
const std::string& lastError();
}  // namespace p4
}  // namespace flatsql

namespace p4t {

using flatsql::p4::Engine;
using flatsql::p4::SlotHeader;

// ---- framework --------------------------------------------------------------------------------
std::vector<Test>& registry() {
    static std::vector<Test> r;
    return r;
}
int gFailures = 0;
static std::vector<std::string> gArgs;

long argInt(const char* name, long def) {
    const std::string pre = std::string("--") + name + "=";
    for (const auto& a : gArgs)
        if (a.rfind(pre, 0) == 0) return std::atol(a.c_str() + pre.size());
    return def;
}
std::string argStr(const char* name, const std::string& def) {
    const std::string pre = std::string("--") + name + "=";
    for (const auto& a : gArgs)
        if (a.rfind(pre, 0) == 0) return a.substr(pre.size());
    return def;
}
double loadAvg() {
    double l[3] = {0, 0, 0};
#if !defined(__wasm__)
    if (getloadavg(l, 3) < 1) return -1;
#else
    (void)l;
    return -1;
#endif
    return l[0];
}
void report(const char* key, double value, const char* unit) {
    std::printf("  MEASURE %s = %.3f %s (load %.1f)\n", key, value, unit, loadAvg());
    std::fflush(stdout);
}

// ---- bytes ----------------------------------------------------------------------------------------
std::string hex(const uint8_t* p, size_t n) {
    static const char* d = "0123456789abcdef";
    std::string s;
    for (size_t i = 0; i < n; i++) {
        s.push_back(d[p[i] >> 4]);
        s.push_back(d[p[i] & 15]);
    }
    return s;
}
std::vector<uint8_t> unhex(const std::string& s) {
    std::vector<uint8_t> b;
    auto v = [](char c) { return c <= '9' ? c - '0' : c - 'a' + 10; };
    for (size_t i = 0; i + 1 < s.size(); i += 2) b.push_back(uint8_t(v(s[i]) << 4 | v(s[i + 1])));
    return b;
}
TlvW& TlvW::raw(uint16_t tag, const void* p, size_t n) {
    flatsql::p4::tlvPut(b, tag, p, n);
    return *this;
}
TlvW& TlvW::u32(uint16_t tag, uint32_t v) {
    uint8_t x[4];
    flatsql::p4::st32(x, v);
    return raw(tag, x, 4);
}
TlvW& TlvW::u64(uint16_t tag, uint64_t v) {
    uint8_t x[8];
    flatsql::p4::st64(x, v);
    return raw(tag, x, 8);
}

// ---- types -------------------------------------------------------------------------------------------
namespace {
const char* kOmm = R"(
namespace sds;
table OMM {
  OBJECT_NAME: string;
  OBJECT_ID: string;
  NORAD_CAT_ID: uint;
  EPOCH: string;
  CREATION_DATE: string;
  MEAN_MOTION: double;
  ECCENTRICITY: double;
  INCLINATION: double;
  COMMENT: string;
}
root_type OMM;
file_identifier "$OMM";
)";
const char* kMpe = R"(
namespace sds;
table MPE {
  ENTITY_ID: string;
  EPOCH: double;
  MEAN_MOTION: double;
  COMMENT: string;
}
root_type MPE;
file_identifier "$MPE";
)";
const char* kCat = R"(
namespace sds;
enum objectType : byte { PAYLOAD = 0, ROCKET_BODY = 1, DEBRIS = 2, UNKNOWN = 3 }
enum opsStatusCode : byte { OPERATIONAL = 0, NONOPERATIONAL = 1, UNKNOWN = 2 }
table CAT {
  OBJECT_NAME: string;
  OBJECT_ID: string;
  NORAD_CAT_ID: uint;
  OBJECT_TYPE: objectType = UNKNOWN;
  OPS_STATUS_CODE: opsStatusCode = UNKNOWN;
  OWNER: string;
  CATALOG_URI: string;
  CATALOG_OBJECT_ID: string;
}
root_type CAT;
file_identifier "$CAT";
)";
const char* kIqc = R"(
namespace sds;
table IQC {
  ENTITY_ID: string;
  CAPTURE_START: string;
  SENSOR: string;
  RETRIEVED_AT: string;
  IMAGE: [ubyte];
}
root_type IQC;
file_identifier "$IQC";
)";
const char* kPnm = R"(
namespace sds;
table PNM {
  FILE_ID: string;
  NAME: string;
  BODY: [ubyte];
}
root_type PNM;
file_identifier "$PNM";
)";

TestType makeType(const char* schemaText, const char* name, const char* fid, const std::string& rules) {
    TestType t;
    t.name = name;
    std::memcpy(t.fid, fid, 4);
    t.rules = rules;
    flatbuffers::Parser parser;
    if (!parser.Parse(schemaText)) {
        std::fprintf(stderr, "schema parse failed: %s\n", parser.error_.c_str());
        std::abort();
    }
    parser.Serialize();
    t.bfbs.assign(parser.builder_.GetBufferPointer(), parser.builder_.GetBufferPointer() + parser.builder_.GetSize());
    t.schema = reflection::GetSchema(t.bfbs.data());
    return t;
}
}  // namespace

std::vector<uint8_t> TestType::spec() const {
    TlvW w;
    w.text(1, name + ".fbs").raw(2, fid, 4).raw(3, bfbs.data(), bfbs.size()).text(4, rules).u64(5, 16u << 20).u64(6, 4u << 20).u32(7, flags);
    w.u32(8, pageSize).u8(9, identity ? 1 : 0).u32(10, a18).u8(11, profile).u8(12, fullText ? 1 : 0);
    return w.b;
}

TestType& ommType() {
    static TestType t = [] {
        TestType x = makeType(kOmm, "OMM", "$OMM",
                              "epoch str:EPOCH|str:CREATION_DATE\ncol 0 u64pos:NORAD_CAT_ID\ncol 1 str:OBJECT_ID\nepoch_day 4\nobject 0,1\n");
        x.a18 = 400000;
        x.profile = 1;
        return x;
    }();
    return t;
}
TestType& mpeType() {
    static TestType t = [] {
        TestType x = makeType(kMpe, "MPE", "$MPE", "epoch f64floor:EPOCH\ncol 1 str:ENTITY_ID\nepoch_day 4\nobject 1\n");
        x.profile = 2;
        return x;
    }();
    return t;
}
TestType& catType() {
    static TestType t = [] {
        TestType x = makeType(kCat, "CAT", "$CAT",
                              "col 0 u64pos:NORAD_CAT_ID\ncol 1 str:OBJECT_ID\ncol 2 enum:OBJECT_TYPE\ncol 3 enum:OPS_STATUS_CODE\n"
                              "object 0,1\nsupersede pair:uri:CATALOG_URI,CATALOG_OBJECT_ID|u64:norad:NORAD_CAT_ID|str:object:OBJECT_ID\n");
        x.fullText = true;
        return x;
    }();
    return t;
}
TestType& iqcType() {
    static TestType t = [] {
        TestType x = makeType(kIqc, "IQC", "$IQC", "bucket str:CAPTURE_START\n");
        x.identity = true;
        x.pageSize = 16384;
        return x;
    }();
    return t;
}
TestType pnmLikeType(const std::string& name) {
    std::string text = kPnm;
    TestType x = makeType(text.c_str(), "PNM", "$PNM", "col 1 str:FILE_ID\n");
    x.name = name;
    return x;
}

std::vector<uint8_t> buildFrame(const TestType& t, const std::vector<Field>& fields) {
    const reflection::Object* root = t.schema->root_table();
    flatbuffers::FlatBufferBuilder b(512);
    std::vector<std::pair<uint16_t, uint32_t>> offs;
    for (const Field& f : fields) {
        const reflection::Field* rf = root->fields()->LookupByKey(f.name.c_str());
        if (!rf) {
            std::fprintf(stderr, "no field %s\n", f.name.c_str());
            std::abort();
        }
        if (f.kind == Field::kStr) offs.push_back({rf->offset(), b.CreateString(f.s).o});
        if (f.kind == Field::kBytes) offs.push_back({rf->offset(), b.CreateVector(f.bytes).o});
    }
    const auto start = b.StartTable();
    for (const Field& f : fields) {
        const reflection::Field* rf = root->fields()->LookupByKey(f.name.c_str());
        const uint16_t vo = rf->offset();
        switch (rf->type()->base_type()) {
            case reflection::UInt: b.AddElement<uint32_t>(vo, uint32_t(f.kind == Field::kUInt ? f.u : uint64_t(f.i)), 0); break;
            case reflection::Int: b.AddElement<int32_t>(vo, int32_t(f.i), 0); break;
            case reflection::ULong: b.AddElement<uint64_t>(vo, f.u, 0); break;
            case reflection::Long: b.AddElement<int64_t>(vo, f.i, 0); break;
            case reflection::Byte: b.AddElement<int8_t>(vo, int8_t(f.i), int8_t(rf->default_integer())); break;
            case reflection::UByte: b.AddElement<uint8_t>(vo, uint8_t(f.i), uint8_t(rf->default_integer())); break;
            case reflection::Double: b.AddElement<double>(vo, f.d, 0.0); break;
            default: break;
        }
    }
    for (const auto& o : offs) b.AddOffset(o.first, flatbuffers::Offset<void>(o.second));
    const auto end = b.EndTable(start);
    char fid[5] = {char(t.fid[0]), char(t.fid[1]), char(t.fid[2]), char(t.fid[3]), 0};
    b.FinishSizePrefixed(flatbuffers::Offset<flatbuffers::Table>(end), fid);
    return std::vector<uint8_t>(b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize());
}

std::vector<uint8_t> ommFrame(uint32_t norad, const std::string& objectId, const std::string& epoch, double mm, size_t pad) {
    std::vector<Field> f = {Field::str("OBJECT_NAME", "SAT-" + std::to_string(norad)), Field::str("OBJECT_ID", objectId),
                            Field::u64("NORAD_CAT_ID", norad), Field::str("EPOCH", epoch),
                            Field::str("CREATION_DATE", "2026-09-27T00:00:00"), Field::f64("MEAN_MOTION", mm),
                            Field::f64("ECCENTRICITY", 0.0001), Field::f64("INCLINATION", 51.6)};
    if (pad) f.push_back(Field::str("COMMENT", std::string(pad, 'x')));
    return buildFrame(ommType(), f);
}
std::vector<uint8_t> mpeFrame(const std::string& entity, double epoch, double x) {
    return buildFrame(mpeType(), {Field::str("ENTITY_ID", entity), Field::f64("EPOCH", epoch), Field::f64("MEAN_MOTION", x)});
}
std::vector<uint8_t> catFrame(uint32_t norad, const std::string& objectId, const std::string& name, int objectType) {
    return buildFrame(catType(), {Field::str("OBJECT_NAME", name), Field::str("OBJECT_ID", objectId),
                                  Field::u64("NORAD_CAT_ID", norad), Field::i64("OBJECT_TYPE", objectType),
                                  Field::i64("OPS_STATUS_CODE", 0), Field::str("OWNER", "US")});
}
std::vector<uint8_t> iqcFrame(const std::string& entity, const std::string& captureStart, size_t imageBytes, uint64_t seed) {
    std::vector<uint8_t> img(imageBytes);
    uint64_t x = seed * 0x9E3779B97F4A7C15ull + 1;
    for (size_t i = 0; i < imageBytes; i++) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        img[i] = uint8_t(x);
    }
    return buildFrame(iqcType(), {Field::str("ENTITY_ID", entity), Field::str("CAPTURE_START", captureStart),
                                  Field::str("SENSOR", "EO-1"), Field::str("RETRIEVED_AT", "2026-09-30T00:00:00Z"),
                                  Field::raw("IMAGE", std::move(img))});
}

int64_t unixOf(int y, int mo, int d, int h, int mi, int s) {
    y -= mo <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = unsigned(y - era * 400);
    const unsigned doy = (153 * (mo + (mo > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return (era * 146097 + int64_t(doe) - 719468) * 86400 + h * 3600 + mi * 60 + s;
}
std::string isoTime(int64_t sec) {
    const time_t t = time_t(sec);
    struct tm tmv;
    gmtime_r(&t, &tmv);
    char buf[32];
    std::strftime(buf, sizeof buf, "%Y-%m-%dT%H:%M:%S", &tmv);
    return buf;
}

// ---- PUT --------------------------------------------------------------------------------------------
void cidOf(const std::vector<uint8_t>& frame, uint8_t cid[36]) {
    cid[0] = 0x01;
    cid[1] = 0x55;
    cid[2] = 0x12;
    cid[3] = 0x20;
    flatsql::ps::sha256(frame.data() + 4, frame.size() - 4, cid + 4);
}
std::string cidText(const uint8_t cid[36]) {
    char t[60];
    flatsql::p4::cidTextFromDigest(cid + 4, t);
    return std::string(t, 59);
}

std::vector<uint8_t> encodePut(const Batch& b) {
    TlvW w;
    w.text(1, b.type).text(50, b.peer);
    for (const Tag& t : b.tags) {
        TlvW x;
        x.text(1, t.provider).text(2, t.source);
        if (!t.url.empty()) x.text(3, t.url);
        if (!t.batch.empty()) x.text(4, t.batch);
        if (!t.ckey.empty()) x.text(5, t.ckey);
        if (!t.ppeer.empty()) x.text(6, t.ppeer);
        if (!t.pkey.empty()) x.text(7, t.pkey);
        w.raw(51, x.b.data(), x.b.size());
    }
    w.u8(52, b.mode);
    std::vector<uint8_t> r(4);
    flatsql::p4::st32(r.data(), uint32_t(b.recs.size()));
    auto put16 = [&](uint16_t v) { r.push_back(uint8_t(v)); r.push_back(uint8_t(v >> 8)); };
    auto put32 = [&](uint32_t v) { for (int i = 0; i < 4; i++) r.push_back(uint8_t(v >> (8 * i))); };
    auto put64 = [&](uint64_t v) { for (int i = 0; i < 8; i++) r.push_back(uint8_t(v >> (8 * i))); };
    for (const In& in : b.recs) {
        uint16_t flags = 0;
        if (!in.sealed.empty()) flags |= 1;
        if (in.hasIdent) flags |= 2;
        if (in.seq) flags |= 4;
        if (!in.peer.empty()) flags |= 8;
        if (b.mode == 1) flags |= 16;
        put16(flags);
        put16(0);
        uint8_t cid[36];
        if (in.explicitCid) std::memcpy(cid, in.cid, 36);
        else cidOf(in.frame, cid);
        r.insert(r.end(), cid, cid + 36);
        put64(uint64_t(in.ts));
        put32(uint32_t(in.frame.size()));
        r.insert(r.end(), in.frame.begin(), in.frame.end());
        if (!in.sealed.empty()) {
            put32(uint32_t(in.sealed.size()));
            r.insert(r.end(), in.sealed.begin(), in.sealed.end());
        }
        put16(uint16_t(in.sig.size()));
        r.insert(r.end(), in.sig.begin(), in.sig.end());
        if (in.hasIdent) r.insert(r.end(), in.ident, in.ident + 32);
        if (in.seq) put64(uint64_t(in.seq));
        if (!in.peer.empty()) {
            put16(uint16_t(in.peer.size()));
            r.insert(r.end(), in.peer.begin(), in.peer.end());
        }
        if (b.mode == 1) {
            put16(uint16_t(in.tags.size()));
            for (auto& t : in.tags) {
                put16(t.first);
                put64(uint64_t(t.second));
            }
        }
    }
    w.raw(53, r.data(), r.size());
    if (b.at) w.i64(54, b.at);
    return w.b;
}

// ---- engine ----------------------------------------------------------------------------------------
std::string scratchDir(const std::string& name) {
    const std::string base = argStr("dir", std::string("/private/tmp/claude-501/-Users-tj-software-spacedatanetwork-stack/"
                                                          "fceff73f-656a-45b9-b7ad-9f96ac195cfa/scratchpad/p4test"));
#if defined(__wasm__)
    static unsigned n = 0;
    const std::string d = base + "/" + name + "-" + std::to_string(flatsql::ps::monoNs() % 1000000) + "-" + std::to_string(++n);
#else
    const std::string d = base + "/" + name + "-" + std::to_string(getpid());
#endif
    removeTree(d);
    std::filesystem::create_directories(d);
    return d;
}
void removeTree(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
}

int32_t openEngine(const std::string& root, const EngineOpts& o, bool start) {
    TlvW w;
    w.text(1, root).u8(2, o.createMode).u32(3, o.writers).u32(7, o.writeSlots).u32(8, o.readSlots).u32(9, o.writeReqBytes);
    w.u32(4, 2).u32(5, 1).u32(6, 1).u32(13, 8);
    if (o.gseqFloor) w.u64(12, o.gseqFloor);
    if (o.flushEntries) w.u32(42, o.flushEntries);
    if (o.groupRecords) w.u32(40, o.groupRecords);
    if (o.backlogCredit) w.u32(44, o.backlogCredit);
    if (o.seqBlock) w.u32(43, o.seqBlock);
    if (o.engineBytes) w.u64(20, o.engineBytes);
    if (o.readerConns) w.u32(23, o.readerConns);
    if (o.writerConns) w.u32(21, o.writerConns);
    if (o.softHeap) w.u64(26, o.softHeap);
    if (o.hardHeap) w.u64(27, o.hardHeap);
    w.b.insert(w.b.end(), o.extra.begin(), o.extra.end());
    int32_t rc = flatsql_p4_init(w.b.data(), int32_t(w.b.size()));
    if (rc != P4_OK) {
        std::fprintf(stderr, "    init: %d %s\n", rc, flatsql::p4::lastError().c_str());
        return rc;
    }
    if (start) {
        rc = flatsql_p4_start();
        if (rc < 0) return rc;
    }
    return P4_OK;
}
int32_t closeEngine(double deadlineMs) { return flatsql_p4_stop(deadlineMs); }
int32_t registerType(const TestType& t) {
    const auto s = t.spec();
    const int32_t rc = flatsql_p4_register_type(s.data(), int32_t(s.size()));
    if (rc != P4_OK) std::fprintf(stderr, "    register %s: %d %s\n", t.name.c_str(), rc, flatsql::p4::lastError().c_str());
    return rc;
}

std::vector<uint64_t> stats() {
    uint8_t buf[8 * 64];
    const int32_t n = flatsql_p4_stats(buf, sizeof buf);
    std::vector<uint64_t> v;
    for (int32_t i = 0; i + 8 <= n; i += 8) v.push_back(flatsql::p4::ld64(buf + i));
    return v;
}

// ---- the client ------------------------------------------------------------------------------------
uint32_t submit(uint32_t op, const std::vector<uint8_t>& req, const CallOpts& o) {
    Engine* e = flatsql::p4::currentEngine();
    const uint32_t cls = o.cls ? o.cls : (op < 10 ? P4_CLASS_WRITE : P4_CLASS_INTERACTIVE);
    const uint32_t pool = cls == P4_CLASS_WRITE ? 0 : 1;
    const uint32_t first = pool == 0 ? 0 : e->nSlots[0];
    const uint32_t n = e->nSlots[pool];
    for (;;) {
        for (uint32_t k = 0; k < n; k++) {
            const uint32_t i = first + k;
            SlotHeader* h = e->slot(i);
            uint32_t expect = P4_SLOT_FREE;
            if (!h->state.compare_exchange_strong(expect, P4_SLOT_CLAIMED)) continue;
            if (req.size() > e->reqBytes[pool]) {
                h->state.store(P4_SLOT_FREE);
                return UINT32_MAX;
            }
            std::memcpy(e->slotReq(i), req.data(), req.size());
            h->op = op;
            h->cls = cls;
            h->flags = o.flags;
            h->reqLen = uint32_t(req.size());
            h->reqId = i;
            h->maxRowsExamined = o.maxRowsExamined;
            h->maxBytesRead = o.maxBytesRead;
            h->maxResultRows = o.maxResultRows;
            h->maxResultBytes = o.maxResultBytes;
            h->ringHead.store(0);
            h->ringTail.store(0);
            h->cancel.store(0);
            h->status = 0;
            h->errLen = 0;
            h->rowsOut = h->rowsExamined = h->bytesRead = 0;
            h->submitNs = flatsql::ps::monoNs();
            h->state.store(P4_SLOT_QUEUED, std::memory_order_release);
            if (o.cancelAfterSubmit) h->cancel.store(1);
            while (!e->queues[cls - 1].push(i)) flatsql::ps::sleepNs(100000);
            // wake an idle thread of the class, else the first
            uint32_t pick = e->firstOfClass[cls];
            for (uint32_t t = 0; t < e->countOfClass[cls]; t++)
                if (e->bells[e->firstOfClass[cls] + t].state.load() == 0) {
                    pick = e->firstOfClass[cls] + t;
                    break;
                }
            flatsql_p4_wake(reinterpret_cast<uint32_t*>(&e->bells[pick].doorbell), 0);
            e->bells[pick].doorbell.fetch_add(1);
            flatsql_p4_wake(reinterpret_cast<uint32_t*>(&e->bells[pick].doorbell), 1);
            return i;
        }
        flatsql::ps::sleepNs(200000);
    }
}

Result wait(uint32_t slot, uint64_t timeoutMs) {
    Result res;
    Engine* e = flatsql::p4::currentEngine();
    SlotHeader* h = e->slot(slot);
    const uint8_t* ring = e->slotRing(slot);
    const uint64_t cap = e->ringBytes[e->poolOf(slot)];
    flatsql::ps::rb1::Decoder dec;
    std::vector<uint8_t> buf(64 << 10);
    const uint64_t until = flatsql::ps::monoNs() + timeoutMs * 1000000ull;
    for (;;) {
        const uint32_t seq = h->outSeq.load(std::memory_order_acquire);
        const uint32_t state = h->state.load(std::memory_order_acquire);
        const uint64_t tail = h->ringTail.load(std::memory_order_acquire);
        const uint64_t head = h->ringHead.load(std::memory_order_relaxed);
        if (tail > head) {
            const size_t k = size_t(std::min<uint64_t>(buf.size(), tail - head));
            const size_t at = size_t(head & (cap - 1));
            const size_t f1 = std::min(k, size_t(cap) - at);
            std::memcpy(buf.data(), ring + at, f1);
            if (k > f1) std::memcpy(buf.data() + f1, ring, k - f1);
            h->ringHead.store(head + k, std::memory_order_release);
            h->spaceSeq.fetch_add(1);
            const uint32_t th = h->thread;
            if (th < e->nThreads) {
                e->bells[th].doorbell.fetch_add(1);
                flatsql_p4_wake(reinterpret_cast<uint32_t*>(&e->bells[th].doorbell), 1);
            }
            if (!dec.feed(buf.data(), k)) {
                res.status = -999;
                res.err = "RB1 decode: " + dec.error();
            }
            continue;
        }
        if (state == P4_SLOT_DONE) break;
        if (flatsql::ps::monoNs() >= until) {
            res.status = -998;
            res.err = "timeout";
            return res;
        }
        flatsql::ps::waitU32(&h->outSeq, seq, 10ull * 1000 * 1000);
    }
    if (res.status == 0) res.status = h->status;
    res.err.assign(h->err, h->errLen);
    res.rowsExamined = h->rowsExamined;
    res.bytesRead = h->bytesRead;
    res.cols = dec.names();
    res.rows = std::move(dec.rows());
    if (dec.done() && dec.status() != h->status && res.status >= -14) {
        res.status = -997;
        res.err = "RB1E status " + std::to_string(dec.status()) + " != slot status " + std::to_string(h->status);
    }
    if (!dec.done() && res.status != -999) {
        res.status = -996;
        res.err = "no RB1E (" + res.err + ")";
    }
    h->state.store(P4_SLOT_FREE, std::memory_order_release);
    return res;
}

Result call(uint32_t op, const std::vector<uint8_t>& req, const CallOpts& o) {
    const uint32_t s = submit(op, req, o);
    if (s == UINT32_MAX) {
        Result r;
        r.status = -995;
        r.err = "request larger than the slot";
        return r;
    }
    return wait(s, o.timeoutMs);
}

Result put(const Batch& b) {
    for (int attempt = 0;; attempt++) {
        Result r = call(P4_OPC_PUT, encodePut(b));
        if (r.status != P4_E_BUSY || attempt > 2000) return r;
        flatsql::ps::sleepNs(1000000);
    }
}

Result get(const std::string& type, const std::vector<std::vector<uint8_t>>& cids, bool hydrate, bool every) {
    TlvW w;
    w.text(1, type).u8(2, hydrate ? 1 : 0);
    std::vector<uint8_t> c(4);
    flatsql::p4::st32(c.data(), uint32_t(cids.size()));
    for (auto& x : cids) c.insert(c.end(), x.begin(), x.end());
    w.raw(40, c.data(), c.size());
    if (every) w.u8(41, 1);
    return call(P4_OPC_GET, w.b);
}

int Result::col(const std::string& name) const {
    for (size_t i = 0; i < cols.size(); i++)
        if (cols[i] == name) return int(i);
    return -1;
}
int64_t Result::i(size_t row, const std::string& c) const {
    const int k = col(c);
    if (k < 0 || row >= rows.size()) return INT64_MIN;
    return rows[row][size_t(k)].i;
}
std::string Result::s(size_t row, const std::string& c) const {
    const int k = col(c);
    if (k < 0 || row >= rows.size()) return "<none>";
    return rows[row][size_t(k)].s;
}
bool Result::null(size_t row, const std::string& c) const {
    const int k = col(c);
    return k < 0 || row >= rows.size() || rows[row][size_t(k)].type == 0;
}

}  // namespace p4t

int main(int argc, char** argv) {
#if defined(__wasm__)
    // Grow the heap before any engine thread runs (a host may refresh a shared
    // memory's size lazily per thread): allocate and free one large block.
    if (void* p = std::malloc(size_t(1536) << 20)) std::free(p);
#endif
    for (int i = 1; i < argc; i++) p4t::gArgs.push_back(argv[i]);
    const std::string filter = p4t::argStr("test", "");
    const bool slow = p4t::argInt("slow", 0) != 0;
    for (const auto& a : p4t::gArgs)
        if (a == "--list") {
            for (const auto& t : p4t::registry()) std::printf("%s %s\n", t.name, t.slow ? "slow" : "fast");
            return 0;
        }
    int ran = 0;
    bool exact = false;
    for (const auto& t : p4t::registry()) exact = exact || filter == t.name;
    for (const auto& t : p4t::registry()) {
        if (!filter.empty() && (exact ? filter != t.name : std::string(t.name).find(filter) == std::string::npos)) continue;
        if (!filter.empty() && !exact && t.slow && !slow) continue;
        if (filter.empty() && t.slow && !slow) continue;
        const int before = p4t::gFailures;
        const auto t0 = std::chrono::steady_clock::now();
        std::printf("[ RUN  ] %s\n", t.name);
        std::fflush(stdout);
        t.fn();
        const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
        std::printf("[ %s ] %s (%.0f ms)\n", p4t::gFailures == before ? " OK " : "FAIL", t.name, ms);
        std::fflush(stdout);
        ran++;
    }
    std::printf("%d tests, %d failures\n", ran, p4t::gFailures);
    return p4t::gFailures ? 1 : 0;
}
