// The format-4 operations SDN nodes spend their FlatSQL time in, replayed
// (flatsql-p4-replay-10x-20261018).
//
// p4_replay runs one workload on a store and prints one REPLAY line per pass:
// its wall and CPU time and a hash of every answer, so the driver
// (cpp/tests/p4-replay.e2e.mjs) can run the same workload on two engine builds
// and on the same data, interleaved, and compare both. The requests are the
// ones sdn-server's storage/format4 daemon sends (format4_daemon_reads.go,
// format4_daemon_writes.go):
//
//   gp_ingest    a day of Space-Track gp_history as ingest PUTs of 4,096 OMMs
//                (format4PutChunk) into the gp_history feed
//   wxf_lead     a GFS lead on the sea feed: 512 tile PUTs of nine records
//   sync_norad   read_chunk with sync_filter "NORAD_CAT_ID = n AND EPOCH
//                BETWEEN a AND b": SCAN, w order, 100 a page, offset pages
//   sync_window  read_chunk with sync_filter "EPOCH BETWEEN a AND b" and the
//                row-id cursor: SCAN, seq order, 1,000 a page
//   latest       the OMM default profile: EPOCH nearest per object at now,
//                every object (the console's catalog)
//   retention    ExpireRecords: 20 pages of SCAN (501 rows) + DELETE (500)
//   explorer     the data explorer: 100 pages of 100 newest-first by the
//                seq cursor (FullTablePageWithCursor), hydrated
//   open         a restart: init (journal replay, WAL recovery), start, the
//                type registrations and each type's HEAD
//
// --phase=build builds the store: the sea feed through the migration path
// (--wxf-bytes of stream) and --gp (a TSV of real gp_history OMMs, one per
// line: NORAD, OBJECT_ID, OBJECT_NAME, EPOCH, CREATION_DATE, MEAN_MOTION,
// ECCENTRICITY, INCLINATION, TLE) as ingest PUTs, a day file per batch tag.
// --phase=run runs --workload on --store (--gp-day: the day gp_ingest
// writes; both may be comma lists, one store per workload, run in turn in the
// one process); --passes=N repeats a read workload in the same process (the
// first pass is the cold cache, the others warm); --lead-tiles (512) and
// --retention-pages (20) size wxf_lead and retention. Every answer goes into the hash: a build that
// answers differently hashes differently.
#include <fcntl.h>
#if defined(__wasm__)
#include <wasi/api.h>
#else
#include <sys/resource.h>
#endif
#if defined(__APPLE__)
#include <sys/clonefile.h>
#endif
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <sqlite3.h>

#include "flatsql/ps/platform.h"
#include "flatsql/ps/result_block.h"
#include "internal.h"
#include "p4/p4_test.h"
#include "p4/wxf_fixture.h"

namespace flatsql {
namespace p4 {
const std::string& lastError();
}  // namespace p4
}  // namespace flatsql

using namespace p4t;
using namespace p4t::wxf;
namespace rb1 = flatsql::ps::rb1;

namespace {

const char* kGpProvider = "space-track.org";
const char* kGpSource = "gp_history";
const char* kGpUrl = "https://www.space-track.org/basicspacedata/query/class/gp_history";
const char* kGpPeer = "12D3KooWReplayFixturePeer";

// A 128-bit multiply-mix hash over a canonical form of every answer, eight
// bytes a step (the client's own cost stays small beside the engine's).
struct Hash {
    uint64_t a = 0xcbf29ce484222325ull, b = 0x84222325cbf29ce4ull;
    static uint64_t rotl(uint64_t x, int r) { return (x << r) | (x >> (64 - r)); }
    void word(uint64_t w) {
        a = rotl((a ^ w) * 0x9e3779b97f4a7c15ull, 29);
        b = rotl((b + w) * 0xc2b2ae3d27d4eb4full, 31) ^ a;
    }
    void bytes(const void* p, size_t n) {
        const uint8_t* q = static_cast<const uint8_t*>(p);
        size_t i = 0;
        for (; i + 8 <= n; i += 8) {
            uint64_t w;
            std::memcpy(&w, q + i, 8);
            word(w);
        }
        uint64_t w = 0;
        std::memcpy(&w, q + i, n - i);
        word(w ^ (uint64_t(n - i) << 56));
    }
    void i64(int64_t v) { word(uint64_t(v)); }
    void str(const std::string& s) {
        i64(int64_t(s.size()));
        bytes(s.data(), s.size());
    }
    void result(const Result& r) {
        i64(r.status);
        for (const auto& c : r.cols) str(c);
        i64(int64_t(r.rows.size()));
        for (const auto& row : r.rows)
            for (const Cell& c : row) {
                i64(c.type);
                i64(c.i);
                bytes(&c.d, 8);
                str(c.s);
            }
    }
    std::string hex() const {
        char buf[40];
        std::snprintf(buf, sizeof buf, "%016llx%016llx", (unsigned long long)a, (unsigned long long)b);
        return buf;
    }
};

// The engine's durable syncs: stream syncs (kStJournalSyncs) and index
// commits (kStGroupCommits, each a WAL sync), the write workloads' bound.
uint64_t syncs() {
    const std::vector<uint64_t> v = stats();
    return v.size() > size_t(flatsql::p4::kStJournalSyncs)
               ? v[flatsql::p4::kStJournalSyncs] + v[flatsql::p4::kStGroupCommits]
               : 0;
}

struct Clock {
    uint64_t wall0 = 0, sync0 = 0;
    double cpu0 = 0;
    // The process's CPU seconds; under WASI the host's (the process clock
    // of the runtime that runs the command), -1 where the host has none.
    static double cpu() {
#if defined(__wasm__)
        __wasi_timestamp_t ns = 0;
        if (__wasi_clock_time_get(__WASI_CLOCKID_PROCESS_CPUTIME_ID, 1000, &ns) != 0) return -1;
        return double(ns) / 1e9;
#else
        struct rusage u;
        getrusage(RUSAGE_SELF, &u);
        return double(u.ru_utime.tv_sec + u.ru_stime.tv_sec) + double(u.ru_utime.tv_usec + u.ru_stime.tv_usec) / 1e6;
#endif
    }
    void start() {
        wall0 = flatsql::ps::monoNs();
        cpu0 = cpu();
        sync0 = syncs();
    }
    double wallMs() const { return double(flatsql::ps::monoNs() - wall0) / 1e6; }
    double cpuMs() const { return cpu0 < 0 ? -1 : (cpu() - cpu0) * 1e3; }
};

void emit(const std::string& workload, const char* pass, const Clock& c, const Hash& h, int64_t rows, int64_t ops) {
    std::printf("REPLAY {\"workload\":\"%s\",\"pass\":\"%s\",\"wall_ms\":%.3f,\"cpu_ms\":%.3f,\"hash\":\"%s\",\"rows\":%lld,"
                "\"ops\":%lld,\"syncs\":%llu,\"load\":%.2f}\n",
                workload.c_str(), pass, c.wallMs(), c.cpuMs(), h.hex().c_str(), (long long)rows, (long long)ops,
                (unsigned long long)(syncs() - c.sync0), loadAvg());
    std::fflush(stdout);
}

// ---- the GP archive -------------------------------------------------------------------------------
struct Gp {
    uint32_t norad = 0;
    std::string objectId, name, epoch, created, tle;
    double mm = 0, ecc = 0, inc = 0;
};

std::vector<Gp> readGp(const std::string& path) {
    std::vector<Gp> out;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        std::vector<std::string> f;
        std::stringstream ss(line);
        std::string x;
        while (std::getline(ss, x, '\t')) f.push_back(x);
        if (f.size() < 9) continue;
        Gp g;
        g.norad = uint32_t(std::strtoul(f[0].c_str(), nullptr, 10));
        g.objectId = f[1];
        g.name = f[2];
        g.epoch = f[3];
        g.created = f[4];
        g.mm = std::strtod(f[5].c_str(), nullptr);
        g.ecc = std::strtod(f[6].c_str(), nullptr);
        g.inc = std::strtod(f[7].c_str(), nullptr);
        g.tle = f[8];
        out.push_back(std::move(g));
    }
    return out;
}

std::vector<uint8_t> gpFrame(const Gp& g) {
    return buildFrame(ommType(), {Field::str("OBJECT_NAME", g.name), Field::str("OBJECT_ID", g.objectId),
                                  Field::u64("NORAD_CAT_ID", g.norad), Field::str("EPOCH", g.epoch),
                                  Field::str("CREATION_DATE", g.created), Field::f64("MEAN_MOTION", g.mm),
                                  Field::f64("ECCENTRICITY", g.ecc), Field::f64("INCLINATION", g.inc),
                                  Field::str("COMMENT", g.tle)});
}

int64_t isoSec(const std::string& s) {
    int y = 0, mo = 0, d = 0, h = 0, mi = 0, se = 0;
    if (std::sscanf(s.c_str(), "%d-%d-%dT%d:%d:%d", &y, &mo, &d, &h, &mi, &se) < 3) return 0;
    return unixOf(y, mo, d, h, mi, se);
}

// A day's records as ingest PUTs of 4,096 (sdn's format4PutChunk), tagged
// with the day as the batch, built before the clock starts.
std::vector<Batch> dayBatches(const std::vector<Gp>& recs, const std::string& day) {
    std::vector<Batch> out;
    const int64_t at = isoSec(day + "T23:59:59");
    for (size_t start = 0; start < recs.size(); start += 4096) {
        Batch b;
        b.type = "OMM";
        b.peer = kGpPeer;
        b.at = at;
        b.tags.push_back(Tag{kGpProvider, kGpSource, kGpUrl, day, "", "", ""});
        for (size_t i = start; i < std::min(recs.size(), start + 4096); i++) {
            In in;
            in.frame = gpFrame(recs[i]);
            in.ts = isoSec(recs[i].created);
            b.recs.push_back(std::move(in));
        }
        out.push_back(std::move(b));
    }
    return out;
}

// The PUTs, every outcome into the hash.
bool ingest(const std::vector<Batch>& batches, Hash* h, int64_t* ops) {
    for (const Batch& b : batches) {
        Result r = put(b);
        if (r.status != P4_OK || r.rows.size() != b.recs.size()) {
            std::fprintf(stderr, "  ingest: PUT %d %s\n", r.status, r.err.c_str());
            return false;
        }
        if (h) h->result(r);
        if (ops) (*ops)++;
    }
    return true;
}

// The day files in a seed TSV: consecutive runs of one CREATION_DATE day.
std::vector<std::pair<std::string, std::vector<Gp>>> byDay(std::vector<Gp> all) {
    std::vector<std::pair<std::string, std::vector<Gp>>> out;
    for (Gp& g : all) {
        const std::string day = g.created.substr(0, 10);
        if (out.empty() || out.back().first != day) out.push_back({day, {}});
        out.back().second.push_back(std::move(g));
    }
    return out;
}

// ---- the engine, as the node opens it -------------------------------------------------------------
// format4.Engine's config TLV: the root, the create mode, the cores, the
// node's connection pools; everything else the engine default (zero).
int32_t openNode(const std::string& root, uint8_t mode, uint64_t gseqFloor = 0) {
    TlvW w;
    w.text(1, root).u8(2, mode).u32(13, std::max(1u, std::thread::hardware_concurrency()));
    w.u32(21, 64).u32(23, 1024);
    if (gseqFloor) w.u64(12, gseqFloor);
    int32_t rc = flatsql_p4_init(w.b.data(), int32_t(w.b.size()));
    if (rc != P4_OK) {
        std::fprintf(stderr, "    init: %d %s\n", rc, flatsql::p4::lastError().c_str());
        return rc;
    }
    rc = flatsql_p4_start();
    return rc < 0 ? rc : P4_OK;
}

bool registerAll() { return registerType(wxfType()) == P4_OK && registerType(ommType()) == P4_OK; }

Result head(const std::string& type) {
    TlvW w;
    w.text(1, type);
    return call(P4_OPC_HEAD, w.b);
}

std::vector<uint8_t> predOf(uint8_t field, uint8_t op, const std::vector<int64_t>& vals) {
    std::vector<rb1::Cell> cells(vals.size());
    for (size_t i = 0; i < vals.size(); i++) {
        cells[i].type = rb1::kInt;
        cells[i].i = vals[i];
    }
    std::vector<uint8_t> enc;
    rb1::encodeParams(cells, &enc);
    std::vector<uint8_t> p = {field, op, uint8_t(vals.size()), uint8_t(vals.size() >> 8)};
    p.insert(p.end(), enc.begin() + 4, enc.end());
    return p;
}

// The seed's shape the read workloads ask about: its NORAD IDs and epochs.
struct Probe {
    std::vector<uint32_t> norads;  // a deterministic sample
    int64_t epochLo = 0, epochHi = 0;
    std::vector<std::pair<int64_t, int64_t>> windows;  // read_chunk epoch windows
};

Probe probeOf(const std::vector<Gp>& seed, int n) {
    Probe p;
    std::set<uint32_t> ids;
    std::vector<int64_t> ep;
    for (const Gp& g : seed) {
        ids.insert(g.norad);
        ep.push_back(isoSec(g.epoch));
    }
    std::sort(ep.begin(), ep.end());
    const std::vector<uint32_t> all(ids.begin(), ids.end());
    for (int i = 0; i < n && !all.empty(); i++) p.norads.push_back(all[(size_t(i) * 7919u) % all.size()]);
    if (!ep.empty()) {
        p.epochLo = ep[ep.size() / 20];
        p.epochHi = ep[ep.size() - 1 - ep.size() / 20];
        // three six-hour windows inside the seed's epochs
        for (int k = 1; k <= 3; k++) {
            const int64_t mid = ep[ep.size() * size_t(k) / 4];
            p.windows.push_back({mid - 3 * 3600, mid + 3 * 3600});
        }
    }
    return p;
}

// ---- workloads ---------------------------------------------------------------------------------------
using Work = void (*)(const Probe&, Hash*, int64_t* rows, int64_t* ops);

void wSyncNorad(const Probe& p, Hash* h, int64_t* rows, int64_t* ops) {
    for (uint32_t norad : p.norads) {
        const auto k = predOf(P4_F_COL0, P4_OP_EQ, {int64_t(norad)});
        const auto e = predOf(P4_F_EPOCH, P4_OP_BETWEEN, {p.epochLo, p.epochHi});
        for (uint64_t off = 0;; off += 100) {
            TlvW w;
            w.text(1, "OMM").u8(2, 1).u64(3, 100).u64(4, off).u8(5, P4_ORDER_W_ASC);
            w.raw(17, k.data(), k.size()).raw(17, e.data(), e.size());
            Result r = call(P4_OPC_SCAN, w.b);
            h->result(r);
            (*ops)++;
            *rows += int64_t(r.rows.size());
            if (r.status != P4_OK || r.rows.size() < 100) break;
        }
    }
}

void wSyncWindow(const Probe& p, Hash* h, int64_t* rows, int64_t* ops) {
    for (const auto& win : p.windows) {
        const auto e = predOf(P4_F_EPOCH, P4_OP_BETWEEN, {win.first, win.second});
        int64_t after = 0;
        for (;;) {
            TlvW w;
            w.text(1, "OMM").u8(2, 1).u64(3, 1000).u8(5, P4_ORDER_SEQ_ASC).i64(6, after);
            w.raw(17, e.data(), e.size());
            Result r = call(P4_OPC_SCAN, w.b);
            h->result(r);
            (*ops)++;
            *rows += int64_t(r.rows.size());
            if (r.status != P4_OK || r.rows.empty()) break;
            for (size_t i = 0; i < r.rows.size(); i++) after = std::max(after, r.i(i, "seq"));
            if (r.rows.size() < 1000) break;
        }
    }
}

void wLatest(const Probe& p, Hash* h, int64_t* rows, int64_t* ops) {
    TlvW w;
    w.text(1, "OMM").u8(2, 1).u64(3, 250000).u8(30, 2).i64(31, p.epochHi + 86400).i64(32, 0);
    CallOpts o;
    o.timeoutMs = 600000;
    Result r = call(P4_OPC_EPOCH, w.b, o);
    h->result(r);
    (*ops)++;
    *rows += int64_t(r.rows.size());
}

void wExplorer(const Probe&, Hash* h, int64_t* rows, int64_t* ops) {
    int64_t before = 0;
    for (int page = 0; page < 100; page++) {
        TlvW w;
        w.text(1, "OMM").u8(2, 1).u64(3, 100).u8(5, P4_ORDER_SEQ_DESC);
        if (before > 1) w.i64(7, before - 1);
        Result r = call(P4_OPC_SCAN, w.b);
        h->result(r);
        (*ops)++;
        *rows += int64_t(r.rows.size());
        if (r.status != P4_OK || r.rows.empty()) break;
        before = r.i(r.rows.size() - 1, "seq");
    }
}

void wRetention(const Probe&, Hash* h, int64_t* rows, int64_t* ops) {
    Result hd = head("WXF");
    h->result(hd);
    if (hd.status != P4_OK || hd.rows.size() != 1) return;
    const int64_t through = hd.i(0, "through");
    const int64_t before = kT0 + 4 * 86400;
    int64_t after = 0;
    const int pages = int(argInt("retention-pages", 20));
    for (int page = 0; page < pages; page++) {
        CallOpts bulk;
        bulk.cls = P4_CLASS_BULK;
        Result s = call(P4_OPC_SCAN, retentionScan(kBig, before, through, after), bulk);
        h->result(s);
        (*ops)++;
        if (s.status != P4_OK || s.rows.empty()) break;
        const size_t n = std::min<size_t>(s.rows.size(), 500);
        std::vector<std::vector<uint8_t>> cids;
        for (size_t i = 0; i < n; i++) {
            cids.push_back(cidOfText(s.s(i, "cid")));
            after = std::max(after, s.i(i, "seq"));
        }
        TlvW dw;
        dw.text(1, "WXF");
        const std::vector<uint8_t> cl = cidList(cids, 0, cids.size());
        dw.raw(40, cl.data(), cl.size());
        Result d = call(P4_OPC_DELETE, dw.b);
        h->result(d);
        (*ops)++;
        *rows += int64_t(n);
        if (s.rows.size() <= 500) break;
    }
}

void wLead(const Probe&, Hash* h, int64_t* rows, int64_t* ops) {
    const std::vector<uint8_t> values = payloadOf(size_t(argInt("payload", 8616)));
    const int64_t ts = kT0 + 5 * 86400;
    const int64_t tiles = argInt("lead-tiles", 512);
    for (int64_t tile = 0; tile < tiles; tile++) {
        Result r = put(tileBatch(kBig, "lead-tile-" + std::to_string(tile), "lead", tile, ts, values));
        h->result(r);
        (*ops)++;
        *rows += int64_t(r.rows.size());
        if (r.status != P4_OK) break;
    }
}

// ---- the speed of light ------------------------------------------------------------------------------
// This machine's bounds beside the store: sequential write and uncached read
// MB/s (1 MiB blocks), a durable append's latency (4 KiB then the engine's
// sync: F_FULLFSYNC on darwin, fdatasync elsewhere) and memcpy GB/s.
#if !defined(__wasm__)
int durableSync(int fd) {
#if defined(__APPLE__)
    if (::fcntl(fd, F_FULLFSYNC) == 0) return 0;
    return ::fsync(fd);
#else
    return ::fdatasync(fd);
#endif
}

void bounds(const std::string& dir) {
    const std::string path = dir + "/bounds.tmp";
    const size_t block = 1 << 20, total = size_t(argInt("bounds-mb", 2048)) << 20;
    std::vector<uint8_t> buf = payloadOf(block);
    int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0600);
    if (fd < 0) return;
    uint64_t t0 = flatsql::ps::monoNs();
    for (size_t off = 0; off < total; off += block)
        if (::pwrite(fd, buf.data(), block, off_t(off)) != ssize_t(block)) break;
    durableSync(fd);
    const double wMBs = double(total) / 1048576.0 / (double(flatsql::ps::monoNs() - t0) / 1e9);
    ::close(fd);
    // The read is cold: an APFS clone of the file shares its blocks but none
    // of its cached pages (the same as the replay's per-run store clones).
    const std::string cold = path + ".clone";
#if defined(__APPLE__)
    ::unlink(cold.c_str());
    if (::clonefile(path.c_str(), cold.c_str(), 0) != 0) return;
    fd = ::open(cold.c_str(), O_RDONLY);
    ::fcntl(fd, F_NOCACHE, 1);
#else
    fd = ::open(path.c_str(), O_RDONLY);
#endif
    t0 = flatsql::ps::monoNs();
    for (size_t off = 0; off < total; off += block)
        if (::pread(fd, buf.data(), block, off_t(off)) != ssize_t(block)) break;
    const double rMBs = double(total) / 1048576.0 / (double(flatsql::ps::monoNs() - t0) / 1e9);
    ::close(fd);
    ::unlink(cold.c_str());
    fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0600);
    std::vector<double> lat;
    for (int i = 0; i < 200; i++) {
        t0 = flatsql::ps::monoNs();
        if (::pwrite(fd, buf.data(), 4096, off_t(i) * 4096) != 4096) break;
        durableSync(fd);
        lat.push_back(double(flatsql::ps::monoNs() - t0) / 1e6);
    }
    ::close(fd);
    ::unlink(path.c_str());
    std::sort(lat.begin(), lat.end());
    std::vector<uint8_t> a(256u << 20, 1), b(256u << 20);
    t0 = flatsql::ps::monoNs();
    volatile uint64_t sink = 0;
    for (int i = 0; i < 8; i++) {
        a[size_t(i) * 4096] = uint8_t(i);
        std::memcpy(b.data(), a.data(), a.size());
        sink = sink + b[size_t(i) * 4096] + b[b.size() - 1 - size_t(i)];
    }
    const double cGBs = 8.0 * double(a.size()) / 1e9 / (double(flatsql::ps::monoNs() - t0) / 1e9) + double(sink & 0) * 0;
    std::printf("REPLAY_BOUNDS {\"seq_write_MBps\":%.1f,\"seq_read_MBps\":%.1f,\"sync_ms_median\":%.3f,"
                "\"sync_ms_p95\":%.3f,\"memcpy_GBps\":%.2f,\"load\":%.2f}\n",
                wMBs, rMBs, lat.empty() ? 0 : lat[lat.size() / 2], lat.empty() ? 0 : lat[lat.size() * 95 / 100], cGBs,
                loadAvg());
    std::fflush(stdout);
}
#endif

const std::map<std::string, Work>& reads() {
    static const std::map<std::string, Work> m = {
        {"sync_norad", wSyncNorad}, {"sync_window", wSyncWindow}, {"latest", wLatest}, {"explorer", wExplorer}};
    return m;
}

#if !defined(__wasm__)
// ---- the independent reference ---------------------------------------------------------------------
// Stock SQLite (this build's 3.53.4 amalgamation through its own unix VFS, no
// FlatSQL VFS and no format-4 engine) asked each read workload's question in
// SQL on the same feed index, with the same indexes, and each answer's frames
// read with one pread apiece in answer order: a yardstick, not an
// implementation. Staged rows (m=0) sit outside the partial indexes, so each
// query adds them through r_m. Prints REPLAY_STOCK lines: wall, CPU, rows and
// frame bytes (the rows must equal the engine's, or the yardstick asked
// something else).
struct StockDb {
    sqlite3* db = nullptr;
    int fd = -1;
    std::string err;
    explicit StockDb(const std::string& feed) {
        if (sqlite3_open_v2(("file:" + feed + ".db").c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, "unix") != SQLITE_OK)
            err = db ? sqlite3_errmsg(db) : "open";
        else
            sqlite3_exec(db, "PRAGMA query_only=1; PRAGMA mmap_size=0", nullptr, nullptr, nullptr);
        fd = ::open((feed + ".fsdata").c_str(), O_RDONLY);
        if (fd < 0) err = "open stream";
    }
    ~StockDb() {
        if (db) sqlite3_close_v2(db);
        if (fd >= 0) ::close(fd);
    }
    sqlite3_stmt* prep(const char* q) {
        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(db, q, -1, &st, nullptr) != SQLITE_OK) err = std::string(q) + ": " + sqlite3_errmsg(db);
        return st;
    }
    // Reads the frames of a statement's (off, len) columns 0 and 1.
    bool frames(sqlite3_stmt* st, int64_t* rows, int64_t* bytes, int64_t* lastCol2) {
        std::vector<uint8_t> buf;
        int rc;
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            const int64_t off = sqlite3_column_int64(st, 0), len = sqlite3_column_int64(st, 1);
            if (lastCol2) *lastCol2 = sqlite3_column_int64(st, 2);
            buf.resize(size_t(len) + 4);
            if (::pread(fd, buf.data(), buf.size(), off_t(off)) != ssize_t(buf.size())) return false;
            (*rows)++;
            *bytes += len;
        }
        sqlite3_reset(st);
        return rc == SQLITE_DONE;
    }
};

void stock(const std::string& root, const std::string& workload, const Probe& p, int passes) {
    for (int pass = 0; pass < passes; pass++) {
        StockDb s(root + "/P/OMM/space-track.org@gp_history");
        if (!s.err.empty()) {
            std::fprintf(stderr, "stock: %s\n", s.err.c_str());
            return;
        }
        Clock c;
        c.start();
        int64_t rows = 0, bytes = 0;
        bool ok = true;
        if (workload == "sync_norad") {
            sqlite3_stmt* kk = s.prep("SELECT id FROM okey WHERE k=?1");
            sqlite3_stmt* q = s.prep(
                "SELECT off, len, rid FROM (SELECT off, len, w, rid FROM r INDEXED BY r_ke WHERE kk=?1 AND m=1 AND e BETWEEN ?2 AND ?3"
                " UNION ALL SELECT off, len, w, rid FROM r INDEXED BY r_m WHERE m=0 AND kk=?1 AND e BETWEEN ?2 AND ?3)"
                " ORDER BY w, rid LIMIT 100 OFFSET ?4");
            for (uint32_t norad : p.norads) {
                if (!ok || !kk || !q) break;
                sqlite3_bind_int64(kk, 1, norad);
                const int64_t id = sqlite3_step(kk) == SQLITE_ROW ? sqlite3_column_int64(kk, 0) : -1;
                sqlite3_reset(kk);
                for (int64_t off = 0;; off += 100) {
                    const int64_t before = rows;
                    sqlite3_bind_int64(q, 1, id);
                    sqlite3_bind_int64(q, 2, p.epochLo);
                    sqlite3_bind_int64(q, 3, p.epochHi);
                    sqlite3_bind_int64(q, 4, off);
                    ok = s.frames(q, &rows, &bytes, nullptr);
                    if (!ok || rows - before < 100) break;
                }
            }
            sqlite3_finalize(kk);
            sqlite3_finalize(q);
        } else if (workload == "sync_window") {
            sqlite3_stmt* q = s.prep("SELECT off, len, seq FROM r INDEXED BY r_s WHERE seq>?3 AND e BETWEEN ?1 AND ?2 ORDER BY seq LIMIT 1000");
            for (const auto& win : p.windows) {
                int64_t after = 0;
                for (;;) {
                    if (!q) break;
                    const int64_t before = rows;
                    sqlite3_bind_int64(q, 1, win.first);
                    sqlite3_bind_int64(q, 2, win.second);
                    sqlite3_bind_int64(q, 3, after);
                    ok = s.frames(q, &rows, &bytes, &after);
                    if (!ok || rows - before < 1000) break;
                }
            }
            sqlite3_finalize(q);
        } else if (workload == "latest") {
            // Every object's greatest epoch (the probe instant follows every
            // epoch), the lowest CID among ties: one pass over the rows (a
            // seek per object would also walk the staged rows per object).
            sqlite3_stmt* q = s.prep(
                "SELECT off, len FROM (SELECT off, len, row_number() OVER (PARTITION BY kk ORDER BY e DESC, cid) AS n"
                " FROM r WHERE kk IS NOT NULL AND e IS NOT NULL) WHERE n=1");
            if (q) ok = s.frames(q, &rows, &bytes, nullptr);
            sqlite3_finalize(q);
        } else if (workload == "explorer") {
            sqlite3_stmt* q = s.prep("SELECT off, len, seq FROM r INDEXED BY r_s WHERE seq<?1 ORDER BY seq DESC LIMIT 100");
            int64_t before = INT64_MAX;
            for (int page = 0; page < 100 && q; page++) {
                const int64_t had = rows;
                sqlite3_bind_int64(q, 1, before);
                ok = s.frames(q, &rows, &bytes, &before);
                if (!ok || rows == had) break;
            }
            sqlite3_finalize(q);
        } else {
            return;
        }
        if (!s.err.empty() || !ok) {
            std::fprintf(stderr, "stock %s: %s\n", workload.c_str(), s.err.empty() ? "read failed" : s.err.c_str());
            return;
        }
        std::printf("REPLAY_STOCK {\"workload\":\"%s\",\"pass\":\"%s\",\"wall_ms\":%.3f,\"cpu_ms\":%.3f,\"rows\":%lld,\"bytes\":%lld}\n",
                    workload.c_str(), pass ? "warm" : "cold", c.wallMs(), c.cpuMs(), (long long)rows, (long long)bytes);
        std::fflush(stdout);
    }
}
#endif

}  // namespace

namespace {
void runOne(const std::string& root, const std::string& workload);
}  // namespace

P4_SLOW_TEST(p4_replay) {
    const std::string root = argStr("store", "") + "/fsql4";
    REQUIRE(root != "/fsql4", "--store");
    const std::string phase = argStr("phase", "run");
#if !defined(__wasm__)
    if (phase == "bounds") {
        bounds(argStr("store", ""));
        return;
    }
    if (phase == "stock") {
        stock(root, argStr("workload", ""), probeOf(readGp(argStr("gp", "")), int(argInt("probes", 200))), int(argInt("passes", 1)));
        return;
    }
#endif
    if (phase == "build") {
        // The sea feed: p4_record_scaling's fixture when the store holds it
        // (--build-only=1 on the same --store), else built here.
        const uint64_t t0 = flatsql::ps::monoNs();
        struct stat st;
        int64_t seq = 1, bytes = 0;
        if (::stat((root + "/STORE").c_str(), &st) != 0) {
            const int64_t want = std::strtoll(argStr("wxf-bytes", "1000000000").c_str(), nullptr, 10);
            const std::vector<uint8_t> values = payloadOf(size_t(argInt("payload", 8616)));
            const int64_t frameBytes = int64_t(wxfFrame(std::string(kBig) + "/seed/tile-0", 0, "seed", values).size());
            const int64_t tiles = std::max<int64_t>(1, want / (frameBytes + 4) / 9);
            REQUIRE(openNode(root, 2, uint64_t(tiles * 9) + 1 + (1u << 20)) == P4_OK, "open the fixture");
            REQUIRE(registerAll(), "register");
            REQUIRE(migrateLane(kBig, tiles, &seq, values, 1024, &bytes), "the sea feed");
            TlvW r1;
            r1.u32(63, 1);
            CallOpts slow;
            slow.timeoutMs = 3600000;
            CHECK_EQ(call(P4_OPC_REBUILD, r1.b, slow).status, P4_OK, "REBUILD 1");
            CHECK_EQ(flatsql_p4_activate(), P4_OK, "activate");
            closeEngine(600000);
        }
        REQUIRE(openNode(root, 1) == P4_OK, "reopen");
        REQUIRE(registerAll(), "register");
        int64_t n = 0;
        for (const auto& day : byDay(readGp(argStr("gp", "")))) {
            REQUIRE(ingest(dayBatches(day.second, day.first), nullptr, nullptr), "ingest " + day.first);
            n += int64_t(day.second.size());
        }
        closeEngine(600000);
        report("replay.fixture.wxf_records", double(seq - 1), "records");
        report("replay.fixture.wxf_bytes", double(bytes), "B");
        report("replay.fixture.omm_records", double(n), "records");
        report("replay.fixture.seconds", double(flatsql::ps::monoNs() - t0) / 1e9, "s");
        return;
    }
    // --workload and --store may be comma lists of the same length: the
    // workloads run in turn in this one process, each on its own store.
    std::vector<std::string> workloads, stores;
    for (auto* v : {&workloads, &stores}) {
        std::stringstream ss(argStr(v == &workloads ? "workload" : "store", ""));
        for (std::string x; std::getline(ss, x, ',');) v->push_back(x);
    }
    REQUIRE(workloads.size() == stores.size(), "--workload and --store: one store per workload");
    for (size_t i = 0; i < workloads.size(); i++) runOne(stores[i] + "/fsql4", workloads[i]);
}

namespace {
void runOne(const std::string& root, const std::string& workload) {
    Clock c;
    Hash h;
    int64_t rows = 0, ops = 0;
    c.start();
    REQUIRE(openNode(root, 1) == P4_OK, "open");
    REQUIRE(registerAll(), "register");
    for (const char* t : {"OMM", "WXF"}) h.result(head(t));
    if (workload == "open") {
        emit(workload, "cold", c, h, 0, 3);
        closeEngine(600000);
        return;
    }
    const Probe probe = probeOf(readGp(argStr("gp", "")), int(argInt("probes", 200)));
    auto it = reads().find(workload);
    if (it != reads().end()) {
        const int passes = std::max(1, int(argInt("passes", 1)));
        for (int pass = 0; pass < passes; pass++) {
            Hash ph;
            rows = ops = 0;
            c.start();
            it->second(probe, &ph, &rows, &ops);
            emit(workload, pass ? "warm" : "cold", c, ph, rows, ops);
        }
    } else if (workload == "gp_ingest" || workload == "wxf_lead" || workload == "retention") {
        Hash wh;
        std::vector<Batch> batches;
        if (workload == "gp_ingest") {
            const auto days = byDay(readGp(argStr("gp-day", "")));
            REQUIRE(days.size() == 1, "--gp-day: one day");
            batches = dayBatches(days[0].second, days[0].first);
            rows = int64_t(days[0].second.size());
        }
        c.start();
        if (workload == "gp_ingest") {
            REQUIRE(ingest(batches, &wh, &ops), "ingest");
        } else {
            (workload == "wxf_lead" ? wLead : wRetention)(probe, &wh, &rows, &ops);
        }
        emit(workload, "cold", c, wh, rows, ops);
        // What the writes left, read back: the same on both builds.
        Hash after;
        for (const char* t : {"OMM", "WXF"}) after.result(head(t));
        std::printf("REPLAY_AFTER {\"workload\":\"%s\",\"hash\":\"%s\"}\n", workload.c_str(), after.hex().c_str());
    } else {
        REQUIRE(false, "unknown --workload " + workload);
    }
    closeEngine(600000);
}
}  // namespace
