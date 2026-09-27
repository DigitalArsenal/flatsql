// Partition store test framework and fixtures.
//
// Tests assert computable outcomes only (tests-only-for-specific-outcomes):
// exact equalities over bytes, rows, counters and sequences, and measured
// numbers against the acceptance bounds of docs/PARTITION-STORE.md.
#ifndef FLATSQL_PS_TEST_H
#define FLATSQL_PS_TEST_H

#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "flatsql/ps/format.h"
#include "flatsql/ps/writer.h"
#include "ps/io_fault.h"

namespace reflection {
struct Schema;
}

namespace pst {

using namespace flatsql::ps;
using flatsql::ps::test::FaultFs;

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
void report(const char* key, double value, const char* unit);  // measured numbers

// ---- test types (schemas compiled at runtime; field names follow SDS) --------
struct TestType {
    std::string name;       // "OMM.fbs"
    uint8_t fid[4];
    std::vector<uint8_t> bfbs;
    std::string rules;
    std::vector<uint8_t> config;
    const reflection::Schema* schema = nullptr;
    uint64_t maxFrame = 16u << 20;
    uint64_t ringCap = 4u << 20;
};
TestType& ommType();
TestType& mpeType();
TestType& catType();
TestType& iqcType();
TestType& ctlType();   // control partition (no BFBS; CTL rows)
// The same table as a base type (0 OMM, 1 MPE, 2 CAT, 3 IQC) under another
// file identifier (crash harness: 64 types).
TestType makeTypeVariant(int base, const char* fid, const std::string& name);

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
// Size-prefixed frame [u32 size][FlatBuffer with file identifier].
std::vector<uint8_t> buildRecord(const TestType& t, const std::vector<Field>& fields);
std::vector<uint8_t> ommRecord(uint32_t norad, const std::string& objectId, const std::string& epoch,
                               double meanMotion, size_t pad = 0);
std::vector<uint8_t> mpeRecord(const std::string& entity, double epoch, double x, size_t pad = 0);
std::vector<uint8_t> catRecord(uint32_t norad, const std::string& objectId, const std::string& uri,
                               const std::string& catalogObjectId, const std::string& name,
                               int objectType = 1);
std::vector<uint8_t> iqcRecord(const std::string& entity, const std::string& epoch, size_t imageBytes,
                               uint64_t seed);
// CIDv1 raw sha2-256 of the bare FlatBuffer (the router's convention).
void frameCid(const std::vector<uint8_t>& frame, uint8_t cid[kCidLen]);

// ---- store fixture --------------------------------------------------------------
struct Store {
    std::unique_ptr<FaultFs> fs;
    EngineConfig cfg;
    std::unique_ptr<Engine> e;
    std::string root = "/mem/store";
    explicit Store(bool tracking = true, uint32_t writers = 1, bool threads = true);
    int32_t open(std::string* err = nullptr);
    void start() { e->start(); }
    void close();                       // clean stop
    void crash(FaultFs::CrashMode m, uint64_t seed);  // abandon + crash
    void registerTypes(const std::vector<TestType*>& types);
    uint32_t partition(const std::string& producer, const TestType& t);
};

struct Sent {
    uint32_t pid;
    uint64_t rseq;
    std::vector<uint8_t> frame;
    std::vector<uint8_t> attr;
    uint8_t cid[kCidLen];
};

// Enqueues one record and returns the rseq (0 on failure).
uint64_t send(Engine* e, Producer& prod, const std::vector<uint8_t>& frame, const std::vector<uint8_t>& attr,
              int64_t arrivalMs, bool wait = true);

// ---- independent on-disk inspector ------------------------------------------------
struct PartView {
    bool ok = false;
    PartitionHeadFixed head{};
    std::vector<RecRow> rows;           // pseq 1..pseqHi, index = pseq - 1
    std::vector<LaneCounter> lanes;
    std::map<uint32_t, uint64_t> segFirst;
    std::string err;
};
class Inspector {
public:
    Inspector(Io* io, const std::string& root) : io_(io), root_(root), ctx_(io, &stats_) {}
    PartView partition(uint32_t pid);
    std::vector<uint8_t> frame(uint32_t pid, const RecRow& r);
    std::vector<uint8_t> attr(uint32_t pid, const RecRow& r);
    struct TypeView {
        bool ok = false;
        TypeHeadFixed head{};
        std::vector<ArrivalEntry> arrivals;
        std::map<uint32_t, uint64_t> labeled;
    };
    TypeView type(const uint8_t fid[4]);
    IoStats& stats() { return stats_; }

private:
    Io* io_;
    std::string root_;
    IoStats stats_;
    IoCtx ctx_;
};

// Recount of partition counters from rows (head counters must equal it).
struct Recount {
    uint64_t total = 0, totalBytes = 0, live = 0, liveBytes = 0, tombs = 0;
    int64_t minEpoch = INT64_MAX, maxEpoch = INT64_MIN, latestArrival = INT64_MIN;
    std::map<uint32_t, std::pair<int64_t, int64_t>> lanes;  // lane -> (count, bytes)
    std::set<uint64_t> dead;
};
Recount recount(const PartView& v);

}  // namespace pst

#define PS_TEST(name)                                     \
    static void name();                                   \
    static pst::Reg reg_##name(#name, name, false);       \
    static void name()
#define PS_SLOW_TEST(name)                                \
    static void name();                                   \
    static pst::Reg reg_##name(#name, name, true);        \
    static void name()
#define CHECK(c)                                                                  \
    do {                                                                          \
        if (!(c)) {                                                               \
            std::fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);   \
            pst::gFailures++;                                                     \
        }                                                                         \
    } while (0)
#define CHECK_EQ(a, b)                                                                     \
    do {                                                                                   \
        const auto va_ = (a);                                                              \
        const auto vb_ = (b);                                                              \
        if (!(va_ == vb_)) {                                                               \
            std::fprintf(stderr, "  FAIL %s:%d: %s == %s (%lld vs %lld)\n", __FILE__,      \
                         __LINE__, #a, #b, (long long)va_, (long long)vb_);                \
            pst::gFailures++;                                                              \
        }                                                                                  \
    } while (0)
#define REQUIRE(c)                                                                \
    do {                                                                          \
        if (!(c)) {                                                               \
            std::fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);   \
            pst::gFailures++;                                                     \
            return;                                                               \
        }                                                                         \
    } while (0)

#endif
