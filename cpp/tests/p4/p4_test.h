// Store format 4 test framework, fixtures and the in-process mailbox client.
//
// Tests assert computable outcomes only: exact rows, bytes, counters and
// sequences, and measured numbers reported with the machine's load. The
// client drives the engine exactly as a host does: it claims a slot, writes
// the request TLV, pushes the slot on its class's queue, rings a thread's
// doorbell and reads the slot's ring until DONE.
#ifndef FLATSQL_P4_TEST_H
#define FLATSQL_P4_TEST_H

#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "flatsql/p4/flatsql_p4.h"
#include "flatsql/ps/result_block.h"

namespace reflection {
struct Schema;
}

namespace p4t {

using Cell = flatsql::ps::rb1::Cell;

// ---- framework -------------------------------------------------------------------------------
struct Test {
    const char* name;
    void (*fn)();
    bool slow;
};
std::vector<Test>& registry();
struct Reg {
    Reg(const char* n, void (*f)(), bool slow = false) { registry().push_back({n, f, slow}); }
};
extern int gFailures;
long argInt(const char* name, long def);
std::string argStr(const char* name, const std::string& def);
void report(const char* key, double value, const char* unit);  // a measured number, with the load average
double loadAvg();

#define P4_TEST(name) \
    static void name(); \
    static ::p4t::Reg reg_##name(#name, name); \
    static void name()
#define P4_SLOW_TEST(name) \
    static void name(); \
    static ::p4t::Reg reg_##name(#name, name, true); \
    static void name()
#define CHECK(cond, msg)                                                                         \
    do {                                                                                         \
        if (!(cond)) {                                                                           \
            std::fprintf(stderr, "  FAIL %s:%d: %s [%s]\n", __FILE__, __LINE__, #cond,          \
                         std::string(msg).c_str());                                             \
            ::p4t::gFailures++;                                                                  \
        }                                                                                        \
    } while (0)
#define CHECK_EQ(a, b, msg)                                                                      \
    do {                                                                                         \
        const auto _a = (a);                                                                     \
        const auto _b = (b);                                                                     \
        if (!(_a == _b)) {                                                                       \
            std::fprintf(stderr, "  FAIL %s:%d: %s == %s (%lld vs %lld) [%s]\n", __FILE__, __LINE__, \
                         #a, #b, (long long)(_a), (long long)(_b), std::string(msg).c_str());   \
            ::p4t::gFailures++;                                                                  \
        }                                                                                        \
    } while (0)
#define REQUIRE(cond, msg)                                                                       \
    do {                                                                                         \
        if (!(cond)) {                                                                           \
            std::fprintf(stderr, "  FATAL %s:%d: %s [%s]\n", __FILE__, __LINE__, #cond,         \
                         std::string(msg).c_str());                                             \
            ::p4t::gFailures++;                                                                  \
            return;                                                                              \
        }                                                                                        \
    } while (0)

// ---- bytes ---------------------------------------------------------------------------------------
std::string hex(const uint8_t* p, size_t n);
std::vector<uint8_t> unhex(const std::string& s);
struct TlvW {
    std::vector<uint8_t> b;
    TlvW& raw(uint16_t tag, const void* p, size_t n);
    TlvW& text(uint16_t tag, const std::string& s) { return raw(tag, s.data(), s.size()); }
    TlvW& u8(uint16_t tag, uint8_t v) { return raw(tag, &v, 1); }
    TlvW& u32(uint16_t tag, uint32_t v);
    TlvW& u64(uint16_t tag, uint64_t v);
    TlvW& i64(uint16_t tag, int64_t v) { return u64(tag, uint64_t(v)); }
};

// ---- test types (schemas compiled at run time; SDS field names) ---------------------------------
struct TestType {
    std::string name;  // "OMM"
    uint8_t fid[4];
    std::vector<uint8_t> bfbs;
    std::string rules;
    const reflection::Schema* schema = nullptr;
    uint32_t pageSize = 4096;
    bool identity = false;
    uint32_t a18 = 10000;
    uint8_t profile = 0;
    bool fullText = false;
    uint32_t flags = 3;  // verify BFBS + CID
    std::vector<uint8_t> spec() const;  // the register_type TLV
};
TestType& ommType();
TestType& mpeType();
TestType& catType();
TestType& iqcType();
TestType pnmLikeType(const std::string& name);  // no epoch, no object: one flat file per partition

struct Field {
    std::string name;
    enum Kind { kStr, kInt, kUInt, kDouble, kBytes } kind = kStr;
    std::string s;
    int64_t i = 0;
    uint64_t u = 0;
    double d = 0;
    std::vector<uint8_t> bytes;
    static Field str(const std::string& n, const std::string& v) { Field f; f.name = n; f.kind = kStr; f.s = v; return f; }
    static Field i64(const std::string& n, int64_t v) { Field f; f.name = n; f.kind = kInt; f.i = v; return f; }
    static Field u64(const std::string& n, uint64_t v) { Field f; f.name = n; f.kind = kUInt; f.u = v; return f; }
    static Field f64(const std::string& n, double v) { Field f; f.name = n; f.kind = kDouble; f.d = v; return f; }
    static Field raw(const std::string& n, std::vector<uint8_t> v) { Field f; f.name = n; f.kind = kBytes; f.bytes = std::move(v); return f; }
};
// A frame: [u32 size][FlatBuffer with the type's file identifier].
std::vector<uint8_t> buildFrame(const TestType& t, const std::vector<Field>& fields);
std::vector<uint8_t> ommFrame(uint32_t norad, const std::string& objectId, const std::string& epoch, double mm = 15.5,
                              size_t pad = 0);
std::vector<uint8_t> mpeFrame(const std::string& entity, double epoch, double x = 1.0);
std::vector<uint8_t> catFrame(uint32_t norad, const std::string& objectId, const std::string& name, int objectType = 0);
std::vector<uint8_t> iqcFrame(const std::string& entity, const std::string& captureStart, size_t imageBytes,
                              uint64_t seed);
std::string isoTime(int64_t sec);
int64_t unixOf(int y, int mo, int d, int h = 0, int mi = 0, int s = 0);

// ---- PUT batches (CONTRACT §3.7) ------------------------------------------------------------------
struct Tag {
    std::string provider, source, url, batch, ckey, ppeer, pkey;
};
struct In {
    std::vector<uint8_t> frame;   // [u32 size][record]
    int64_t ts = 0;
    std::vector<uint8_t> sig;
    std::vector<uint8_t> sealed;  // stored instead of the plaintext
    bool hasIdent = false;
    uint8_t ident[32];
    int64_t seq = 0;              // migrate
    std::string peer;             // this record's peer (PEER flag)
    std::vector<std::pair<uint16_t, int64_t>> tags;  // migrate
    bool explicitCid = false;
    uint8_t cid[36];
};
struct Batch {
    std::string type, peer;
    std::vector<Tag> tags;
    int64_t at = 0;
    uint8_t mode = 0;
    std::vector<In> recs;
};
std::vector<uint8_t> encodePut(const Batch& b);
void cidOf(const std::vector<uint8_t>& frame, uint8_t cid[36]);  // CIDv1 raw sha2-256 of frame[4:]
std::string cidText(const uint8_t cid[36]);

// ---- the engine and its client ---------------------------------------------------------------------
struct Result {
    int32_t status = 0;
    std::string err;
    std::vector<std::string> cols;
    std::vector<std::vector<Cell>> rows;
    uint64_t rowsExamined = 0, bytesRead = 0;
    int col(const std::string& name) const;
    int64_t i(size_t row, const std::string& c) const;
    std::string s(size_t row, const std::string& c) const;
    bool null(size_t row, const std::string& c) const;
};
struct EngineOpts {
    uint8_t createMode = 1;
    uint32_t writers = 2;
    uint32_t writeSlots = 32, readSlots = 64;
    uint32_t writeReqBytes = 8u << 20;
    uint64_t gseqFloor = 0;
    uint32_t flushEntries = 0;
    uint32_t groupRecords = 0;
    uint32_t backlogCredit = 0;
    uint32_t seqBlock = 0;
    uint64_t engineBytes = 0;
    uint32_t readerConns = 0, writerConns = 0;
    uint64_t softHeap = 0, hardHeap = 0;
    std::vector<uint8_t> extra;
};
std::string scratchDir(const std::string& name);  // a fresh directory for one test
void removeTree(const std::string& path);
int32_t openEngine(const std::string& root, const EngineOpts& o = EngineOpts(), bool start = true);
int32_t closeEngine(double deadlineMs = 30000);
int32_t registerType(const TestType& t);

struct CallOpts {
    uint32_t cls = 0;  // 0: write for ops < 10, else interactive
    uint32_t flags = 0;
    uint64_t maxRowsExamined = 0, maxBytesRead = 0, maxResultRows = 0, maxResultBytes = 0;
    bool cancelAfterSubmit = false;
    uint64_t timeoutMs = 120000;
};
Result call(uint32_t op, const std::vector<uint8_t>& req, const CallOpts& o = CallOpts());
Result put(const Batch& b);
Result get(const std::string& type, const std::vector<std::vector<uint8_t>>& cids, bool hydrate = true, bool every = false);
std::vector<uint64_t> stats();
// Submits without waiting; wait() collects (async PUT calls for group commit).
uint32_t submit(uint32_t op, const std::vector<uint8_t>& req, const CallOpts& o = CallOpts());
Result wait(uint32_t slot, uint64_t timeoutMs = 120000);

}  // namespace p4t

#endif
