// Reader test helpers (see reader_fixtures.h).
#include "ps/reader_fixtures.h"

#include <stdlib.h>

#include <thread>
#if defined(__linux__)
#include <sched.h>
#endif

#include "flatsql/ps/platform.h"

namespace pst {

namespace {
ReaderConfig baseConfig(Io* io, const std::string& root, LaneClass cls, uint32_t lanes, uint64_t arenaBytes,
                        uint32_t ringBytes) {
    ReaderConfig c;
    c.root = root;
    c.io = io;
    c.cls = cls;
    c.lanes = lanes;
    c.arenaBytes = arenaBytes;
    c.ringBytes = ringBytes;
    return c;
}
}  // namespace

Reader::Reader(Store& s, LaneClass cls, uint32_t lanes, uint64_t arenaBytes, uint32_t ringBytes)
    : Reader(baseConfig(s.fs.get(), s.root, cls, lanes, arenaBytes, ringBytes)) {}

Reader::Reader(Io* io, const std::string& root, LaneClass cls, uint32_t lanes, uint64_t arenaBytes,
               uint32_t ringBytes)
    : Reader(baseConfig(io, root, cls, lanes, arenaBytes, ringBytes)) {}

Reader::Reader(const ReaderConfig& cfg) {
    std::string err;
    if (ReaderInstance::open(cfg, &inst, &err) < 0 || inst->start() < 0) {
        std::fprintf(stderr, "  reader open failed: %s\n", err.c_str());
        inst.reset();
    }
}

Reader::~Reader() {
    if (inst) inst->stop();
}

Rows Reader::q(const std::string& sql, const std::vector<Param>& params, uint32_t flags, uint64_t readDelayNs,
               size_t readChunk) {
    Rows r;
    if (!inst) {
        r.status = -1;
        return r;
    }
    ReaderClient c(inst.get());
    Request req;
    req.sql = sql;
    req.params = params;
    req.flags = flags;
    std::vector<uint8_t> out;
    r.outcome = c.run(req, &out, readDelayNs, readChunk);
    r.status = r.outcome.status;
    r.error = r.outcome.error;
    rb1::Decoder d;
    if (!d.feed(out.data(), out.size()) || !d.done()) {
        r.decodeOk = false;
        if (r.status == 0) r.error = "RB1 decode: " + d.error();
    }
    r.names = d.names();
    r.rows = std::move(d.rows());
    return r;
}

std::vector<std::string> Reader::raw(const std::string& sql, const std::vector<Param>& params, Outcome* o) {
    std::vector<std::string> frames;
    ReaderClient c(inst.get());
    Request req;
    req.sql = sql;
    req.params = params;
    req.flags = flatsql::ps::kReqRawStream;
    std::vector<uint8_t> out;
    *o = c.run(req, &out);
    if (!rb1::rawSplit(out.data(), out.size(), &frames)) o->status = -999;
    return frames;
}

bool waitTypeVisible(Io* io, const std::string& root, const uint8_t fid[4], const std::vector<uint32_t>& pids,
                     uint64_t timeoutNs) {
    LaneStoreConfig cfg;
    cfg.root = root;
    cfg.io = io;
    LaneStore st(cfg);
    const uint64_t until = monoNs() + timeoutNs;
    while (monoNs() < until) {
        bool ok = st.open() >= 0 && st.refreshRegistry() >= 0;
        TypeSnap t;
        if (ok) ok = st.loadType(fid, &t) >= 0;
        for (uint32_t pid : pids) {
            if (!ok) break;
            PartSnap p;
            if (st.loadPart(pid, &p, false) < 0) {
                ok = false;
                break;
            }
            if (t.labeledThrough(pid) < p.pseqHi()) ok = false;
        }
        if (ok) return true;
        sleepNs(1000000);
    }
    return false;
}

bool waitLabeledEngine(Engine* e, const std::vector<uint32_t>& pids, uint64_t timeoutNs) {
    const uint64_t deadline = monoNs() + timeoutNs;
    while (monoNs() < deadline) {
        bool all = true;
        for (uint32_t pid : pids) {
            Partition* p = e->partition(pid);
            if (p->labeledThrough.load() < p->durablePseqHi.load()) all = false;
        }
        if (all) return true;
        sleepNs(1000000);
    }
    return false;
}

std::string cidTextOf(const std::vector<uint8_t>& frame) {
    uint8_t cid[kCidLen];
    frameCid(frame, cid);
    return cidToText(cid);
}

unsigned usableHardwareThreads() {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == 0) return unsigned(CPU_COUNT(&set));
#endif
    return std::thread::hardware_concurrency();
}

bool latencyBoxQuiet(unsigned* threads, double* load1, std::string* why) {
    const unsigned hw = usableHardwareThreads();
#if defined(__wasm__)
    // WASI has no load average and no CPU count: under a wasm host the
    // latency bounds are reported, never enforced (they are native
    // acceptance numbers, docs/PARTITION-STORE-WASM.md).
    *threads = hw;
    *load1 = -1;
    *why = "wasm host (no load average or CPU count)";
    return false;
#endif
    double load[3] = {0, 0, 0};
#if !defined(__wasm__)
    getloadavg(load, 3);
#endif
    *threads = hw;
    *load1 = load[0];
    char b[160];
#ifndef NDEBUG
    std::snprintf(b, sizeof(b), "debug build (load %.1f on %u hardware threads)", load[0], hw);
    *why = b;
    return false;
#endif
    if (hw < 8 || load[0] >= double(hw) / 4) {
        std::snprintf(b, sizeof(b), "load %.1f on %u hardware threads", load[0], hw);
        *why = b;
        return false;
    }
    why->clear();
    return true;
}

}  // namespace pst
