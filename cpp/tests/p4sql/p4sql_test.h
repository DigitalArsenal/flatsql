// p4sql tests: a minimal framework and the harness (one fake engine, one
// lane with the SQL surface initialized). Tests assert computable outcomes
// only: exact rows, bytes, statuses and counters.
#ifndef FLATSQL_P4SQL_TEST_H
#define FLATSQL_P4SQL_TEST_H

#ifdef FLATSQL_P4SQL_FAKE

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "fake_reader.h"
#include "flatsql/p4sql/p4sql.h"
#include "flatsql/ps/result_block.h"

namespace p4sqlt {

namespace rb1 = flatsql::ps::rb1;

struct Test {
    const char* name;
    void (*fn)();
};
std::vector<Test>& registry();
struct Reg {
    Reg(const char* n, void (*f)()) { registry().push_back({n, f}); }
};
#define P4SQL_TEST(name)                                \
    static void name();                                 \
    static ::p4sqlt::Reg reg_##name(#name, name);       \
    static void name()

extern int gFailures;
void failAt(const char* file, int line, const std::string& what);
#define CHECK(c)                                                    \
    do {                                                            \
        if (!(c)) ::p4sqlt::failAt(__FILE__, __LINE__, #c);         \
    } while (0)
#define CHECK_EQ(a, b)                                                                                    \
    do {                                                                                                  \
        const auto va_ = (a);                                                                             \
        const auto vb_ = (b);                                                                             \
        if (!(va_ == vb_))                                                                                \
            ::p4sqlt::failAt(__FILE__, __LINE__,                                                          \
                             std::string(#a " == " #b " (") + ::p4sqlt::show(va_) + " vs " + ::p4sqlt::show(vb_) + ")"); \
    } while (0)

std::string show(int64_t v);
std::string show(uint64_t v);
std::string show(int v);
std::string show(unsigned v);
std::string show(size_t v) ;
std::string show(const std::string& v);
std::string show(const char* v);
std::string show(bool v);

// Measured numbers (with the load average at the time).
void report(const char* key, double value, const char* unit);
std::string env(const char* name);
std::string vectorDir();
std::vector<uint8_t> readFile(const std::string& path);

// A record of a type built from JSON with its BFBS (size-prefixed when asked).
std::vector<uint8_t> buildRecord(const std::vector<uint8_t>& bfbs, const std::string& json, bool sizePrefixed);

// One statement's outcome.
struct Result {
    int32_t status = 0;
    std::string error;                  // the lane's last error text
    std::vector<std::string> names;
    std::vector<std::vector<rb1::Cell>> rows;
    int32_t endStatus = 0;              // RB1E.status
    bool ended = false;                 // an RB1E was decoded
    uint64_t endRows = 0;
    std::vector<uint8_t> raw;           // the emitted bytes
    uint64_t rowsExamined = 0;          // what the fake reader examined
    uint64_t bytesRead = 0;
    std::vector<p4fake::OpenedSpec> opened;
};

struct Caps {
    uint64_t maxResultRows = 0, maxResultBytes = 0;   // P4SqlRequest
    uint64_t maxRowsExamined = 0, maxBytesRead = 0;   // the slot's reader caps
    uint64_t slotMaxResultBytes = 0;                  // the slot's ring-side result cap
    int64_t cancelAfterRows = -1;
};

class Harness {
public:
    Harness();
    ~Harness();
    Harness(const Harness&) = delete;
    Harness& operator=(const Harness&) = delete;
    // A type with its BFBS from vectors/<file> (or bytes).
    p4fake::Type& addType(const std::string& name, const char fid[4], const std::vector<uint8_t>& bfbs, uint64_t bound);
    void put(const std::string& type, p4fake::Rec r) { engine.put(type, std::move(r)); }
    Result sql(const std::string& sql, const std::vector<rb1::Cell>& params = {}, uint32_t flags = 0,
               const Caps& caps = Caps());
    Result surface();
    int32_t initStatus() const { return init_; }
    P4Engine engine;
    P4Lane lane;

private:
    Result finish(int32_t status, bool raw);
    int32_t init_ = 0;
};

rb1::Cell cInt(int64_t v);
rb1::Cell cReal(double v);
rb1::Cell cText(const std::string& v);
rb1::Cell cBlob(const std::vector<uint8_t>& v);
rb1::Cell cNull();
std::string showCell(const rb1::Cell& c);
std::string showRow(const std::vector<rb1::Cell>& r);

}  // namespace p4sqlt

#endif  // FLATSQL_P4SQL_FAKE
#endif
