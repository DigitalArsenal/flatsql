// Reader test helpers (T2): reader instances over a test store, statements
// decoded from RB1, and waits for committed (and labeled) visibility.
#ifndef FLATSQL_PS_READER_FIXTURES_H
#define FLATSQL_PS_READER_FIXTURES_H

#include <memory>
#include <string>
#include <vector>

#include "flatsql/ps/lane.h"
#include "flatsql/ps/result_block.h"
#include "ps/ps_test.h"

namespace pst {

using flatsql::ps::LaneClass;
using flatsql::ps::Outcome;
using flatsql::ps::Param;
using flatsql::ps::ReaderClient;
using flatsql::ps::ReaderConfig;
using flatsql::ps::ReaderInstance;
using flatsql::ps::Request;

struct Rows {
    int32_t status = 0;
    std::string error;
    std::vector<std::string> names;
    std::vector<std::vector<rb1::Cell>> rows;
    Outcome outcome;
    bool decodeOk = true;
    int64_t i(size_t r, size_t c) const { return rows[r][c].i; }
    const std::string& s(size_t r, size_t c) const { return rows[r][c].s; }
};

struct Reader {
    std::unique_ptr<ReaderInstance> inst;
    explicit Reader(Store& s, LaneClass cls = LaneClass::Bulk, uint32_t lanes = 2, uint64_t arenaBytes = 0,
                    uint32_t ringBytes = 256u << 10);
    Reader(Io* io, const std::string& root, LaneClass cls, uint32_t lanes, uint64_t arenaBytes = 0,
           uint32_t ringBytes = 256u << 10);
    explicit Reader(const ReaderConfig& cfg);
    ~Reader();
    Rows q(const std::string& sql, const std::vector<Param>& params = {}, uint32_t flags = 0,
           uint64_t readDelayNs = 0, size_t readChunk = 1u << 20);
    // Raw-stream mode: frames.
    std::vector<std::string> raw(const std::string& sql, const std::vector<Param>& params, Outcome* o);
};

// Waits until every partition of `pids` is visible at type level to a fresh
// reader (the type head's labeled_through covers each partition head).
bool waitTypeVisible(Io* io, const std::string& root, const uint8_t fid[4], const std::vector<uint32_t>& pids,
                     uint64_t timeoutNs);
// Waits until the engine's labels cover every durable row.
bool waitLabeledEngine(Engine* e, const std::vector<uint32_t>& pids, uint64_t timeoutNs);

std::string cidTextOf(const std::vector<uint8_t>& frame);

// Latency bounds are acceptance on a quiet Linux-8 box running a release
// build. latencyBoxQuiet() says whether this run is one: an optimized build,
// 8+ usable hardware threads (the affinity mask on Linux, so a cpuset-limited
// container counts what it may use) and a 1-minute load below a quarter of
// them. `why` names what disqualified it.
unsigned usableHardwareThreads();
bool latencyBoxQuiet(unsigned* threads, double* load1, std::string* why);

}  // namespace pst

#endif
