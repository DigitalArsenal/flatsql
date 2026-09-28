// Host-level crash trials (T4, docs/PARTITION-STORE-WASM.md).
//
// The in-guest crash harness (crash_fault_test.cpp) injects faults in an
// in-memory host. These two commands drive the real host I/O imports instead,
// so a host's own fault layer decides what a crash takes away: under the Node
// host, the fault overlay (wasm/ps-node-fault.mjs) sits below the seven
// flatsql_io imports, and test/ps-host-fault.test.ts kills the guest mid-
// ingest, applies a crash (drop every unsynced page, drop a random subset, or
// keep the page cache) and runs the verifier.
//
//   host_crash_ingest --dir=D --round=R [--partitions=8] [--writers=2]
//                     [--journal=0|1] [--max-seconds=120]
//     produces OMM records into `partitions` partitions until killed; prints
//     "ACK <partition> <id>" (one write per line) once a record's durable ack
//     is observed. Record ids of round R start at R * 10^7, and a record's
//     bytes are a function of (partition, id).
//   host_crash_verify --dir=D --acks=FILE [--partitions=8] [--writers=2] [--journal=0|1]
//     reopens D and checks: every acked record is present with identical
//     bytes (its stored frame hashes to the CID of the frame rebuilt from its
//     id), pseqs are a gap-free prefix, head and lane counters equal a recount
//     from rows, and open read 0 bytes of any d-* file and parsed 0 frames.
//
// Both are PS_SLOW_TEST: they run only when named.
#include <atomic>
#include <cstring>
#include <deque>
#include <fstream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>
#include <unistd.h>

#include "flatsql/ps/platform.h"
#include "ps/ps_test.h"

using namespace pst;

namespace {

std::string hostEpoch(uint64_t id) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-09-%02dT%02d:%02d:%02dZ", int(1 + (id / 86400) % 28),
                  int((id / 3600) % 24), int((id / 60) % 60), int(id % 60));
    return b;
}

std::vector<uint8_t> hostFrame(uint32_t k, uint64_t id) {
    return ommRecord(uint32_t(id % 2000000000u) + 1, "H" + std::to_string(k) + "-" + std::to_string(id),
                     hostEpoch(id), 1.0 + double(k) + double(id % 1000) * 1e-4, 48);
}

struct HostStore {
    Store s{false, 2, true};
    std::vector<uint32_t> pids;
    explicit HostStore(const std::string& dir) {
        s.cfg.io = nullptr;  // the seven flatsql_io imports: the host's files
        s.cfg.root = dir;
        s.root = dir;
        s.cfg.writers = uint32_t(argInt("writers", 2));
        s.cfg.commitJournal = argInt("journal", 0) != 0;
        s.cfg.poolBytes = 64ull << 20;
    }
    int32_t open(uint32_t parts) {
        const int32_t rc = s.open();
        if (rc < 0) return rc;
        s.registerTypes({&ommType()});
        for (uint32_t k = 0; k < parts; k++) {
            const uint32_t pid = s.partition("host" + std::to_string(k), ommType());
            if (!pid) return -1;
            pids.push_back(pid);
        }
        return 0;
    }
};

std::mutex gOut;
void emitAck(uint32_t k, uint64_t id) {
    char line[64];
    const int n = std::snprintf(line, sizeof(line), "ACK %u %llu\n", k, (unsigned long long)id);
    std::lock_guard<std::mutex> lk(gOut);
    // One write per line: a kill cuts at most the last line, which the
    // verifier ignores.
    ssize_t done = 0;
    while (done < n) {
        const ssize_t w = ::write(1, line + done, size_t(n - done));
        if (w <= 0) break;
        done += w;
    }
}

}  // namespace

PS_SLOW_TEST(host_crash_ingest) {
    const std::string dir = argStr("dir", "");
    REQUIRE(!dir.empty());
    const uint32_t parts = uint32_t(argInt("partitions", 8));
    const uint64_t base = uint64_t(argInt("round", 0)) * 10000000ull;
    const uint64_t until = monoNs() + uint64_t(argInt("max-seconds", 120)) * 1000000000ull;
    HostStore h(dir);
    REQUIRE(h.open(parts) == 0);
    const auto attr = buildRecordAttr("host", "prov", "src", "b");
    std::vector<std::thread> ts;
    for (uint32_t k = 0; k < parts; k++) {
        ts.emplace_back([&, k] {
            Producer p(h.s.e.get(), h.pids[k]);
            std::deque<std::pair<uint64_t, uint64_t>> inflight;  // (rseq, id)
            for (uint64_t n = 0; monoNs() < until; n++) {
                const uint64_t id = base + n;
                const uint64_t rseq = send(h.s.e.get(), p, hostFrame(k, id), attr, int64_t(id));
                if (!rseq) break;  // the host is gone
                inflight.emplace_back(rseq, id);
                while (!inflight.empty() && (p.acked(inflight.front().first) || inflight.size() > 256)) {
                    if (!p.acked(inflight.front().first) &&
                        p.waitAcked(inflight.front().first, 30000000000ull) != 0)
                        return;
                    emitAck(k, inflight.front().second);
                    inflight.pop_front();
                }
            }
            for (auto& f : inflight) {
                if (p.waitAcked(f.first, 30000000000ull) != 0) return;
                emitAck(k, f.second);
            }
        });
    }
    for (auto& t : ts) t.join();
    h.s.close();
}

PS_SLOW_TEST(host_crash_verify) {
    const std::string dir = argStr("dir", "");
    const std::string acksPath = argStr("acks", "");
    REQUIRE(!dir.empty() && !acksPath.empty());
    const uint32_t parts = uint32_t(argInt("partitions", 8));
    std::map<uint32_t, std::set<uint64_t>> acked;
    {
        std::ifstream in(acksPath);
        REQUIRE(in.good());
        std::string line;
        while (std::getline(in, line)) {
            if (in.eof()) break;  // a line cut by the kill has no newline
            std::istringstream ls(line);
            std::string tag;
            unsigned k = 0;
            unsigned long long id = 0;
            if ((ls >> tag >> k >> id) && tag == "ACK" && k < parts) acked[k].insert(id);
        }
    }
    HostStore h(dir);
    REQUIRE(h.open(parts) == 0);
    const EngineStats st = h.s.e->stats();
    CHECK_EQ(st.openDataBytes, uint64_t(0));
    CHECK_EQ(st.framesParsedAtOpen, uint64_t(0));
    Inspector ins(importIo(), dir);
    uint64_t checked = 0, rows = 0;
    for (uint32_t k = 0; k < parts; k++) {
        PartView v = ins.partition(h.pids[k]);
        if (!v.ok || !v.err.empty()) {
            std::fprintf(stderr, "  partition %u: %s\n", k, v.err.c_str());
            gFailures++;
            continue;
        }
        rows += v.rows.size();
        for (size_t i = 0; i < v.rows.size(); i++) {
            if (v.rows[i].pseq != i + 1) {
                std::fprintf(stderr, "  partition %u: pseq %llu at %zu (not a gap-free prefix)\n", k,
                             (unsigned long long)v.rows[i].pseq, i);
                gFailures++;
                break;
            }
        }
        const Recount rc = recount(v);
        if (v.head.counters.totalCount != rc.total || v.head.counters.liveCount != rc.live ||
            v.head.counters.liveBytes != rc.liveBytes || v.head.counters.tombCount != rc.tombs ||
            v.head.counters.totalBytes != rc.totalBytes) {
            std::fprintf(stderr, "  partition %u: counters differ from recount\n", k);
            gFailures++;
        }
        std::map<uint32_t, std::pair<int64_t, int64_t>> head, rec;
        for (const auto& l : v.lanes)
            if (l.count) head[l.laneId] = {l.count, l.bytes};
        for (const auto& kv : rc.lanes)
            if (kv.second.first) rec[kv.first] = kv.second;
        if (head != rec) {
            std::fprintf(stderr, "  partition %u: lane counters differ from recount\n", k);
            gFailures++;
        }
        std::map<std::string, const RecRow*> puts;
        for (const auto& row : v.rows)
            if (row.kind == kRowPut) puts[std::string(reinterpret_cast<const char*>(row.cid), kCidLen)] = &row;
        int missing = 0, badBytes = 0;
        for (uint64_t id : acked[k]) {
            uint8_t cid[kCidLen];
            frameCid(hostFrame(k, id), cid);
            auto it = puts.find(std::string(reinterpret_cast<const char*>(cid), kCidLen));
            if (it == puts.end()) {
                missing++;
                continue;
            }
            const auto f = ins.frame(h.pids[k], *it->second);
            uint8_t c[kCidLen];
            if (f.size() < 12) {
                badBytes++;
                continue;
            }
            computeCid(f.data() + 4, f.size() - 4, c);
            if (std::memcmp(c, cid, kCidLen) != 0) badBytes++;
            checked++;
        }
        if (missing || badBytes) {
            std::fprintf(stderr, "  partition %u: %d acked records missing, %d with different bytes\n", k, missing,
                         badBytes);
            gFailures++;
        }
    }
    report("host_crash_acked_checked", double(checked), "records");
    report("host_crash_rows", double(rows), "rows");
    h.s.close();
}
