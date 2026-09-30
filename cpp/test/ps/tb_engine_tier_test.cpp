// Terabyte harness: the engine tier and the count-scaled tier (audit §4
// "Generating TB quickly" 1-2; the count-scaled tier is the coordinator's
// addition of 2026-09-29).
//
//   flatsql_ps_test --test=tb_engine_tier --tb-dir=<store dir> --tb-csv=<prefix> [knobs]
//
// A real engine on a real file system (the native host I/O, counted by
// CountingIo) fed by N writer threads from G generator threads:
//
//   --tb-mode=real   records cloned from the host-02 seed corpus
//                    (--tb-corpus=<corpus.tbc>, --tb-types-dir=<production
//                    .fsc configs>): identity, epoch and numeric fields mutate
//                    per clone, bytes and tags stay real. Byte-realistic.
//   --tb-mode=count  the smallest valid record of each type, small seals, a
//                    small fold cap: the audit's COUNT axes (records, segments
//                    per partition S, runs, catalog runs, partitions, handles)
//                    reach terabyte-equivalent values on tens of GB of disk.
//                    What it cannot show: real I/O at a full terabyte (write
//                    amplification on a full disk, compaction throughput,
//                    cold-cache reads over a terabyte working set).
//
// Workload (audit §4): --tb-types types (the real ones plus variants under
// other file identifiers), --tb-partitions producers spread by the type mix,
// OMM, CAT and IQC each with more than 128 of them once P >= 1,000, Zipf skew
// inside a type, and one dominant OMM partition held to --tb-dominant-pct of
// the bytes (a GP-archive-style import: its own generator keeps
// --tb-dominant-inflight fetches in flight; dominant_share reports what it
// actually got, which falls short when its merges cannot keep up). Churn: supersedes on types with a
// supersede rule (--tb-supersede-pct of their records reuse the previous
// clone's identity), type-level deletes (--tb-tomb-pct), retags
// (--tb-retag-pct), and a new batch id for every fetch of a partition
// (--tb-fetch records), which grows the lane tables.
//
// Every --tb-sample-s seconds one CSV row (<prefix>.samples.csv): records and
// bytes acknowledged, rates, seals and merges a second, fetch latency (first
// enqueue to ack) p50/p99, the engine's disk bytes, bytes the engine wrote
// (write amplification over frame bytes), heap, RSS, wasm pages (in the wasm
// command), committedBytes, accelerator and descriptor bytes, host handles
// (open, high water, refused), fds, the largest partition's sealed segments S,
// window p99 of writer maintenance steps and commit rounds, all maintenance
// time in the window over the merges in it, label lag, quarantined partitions,
// and the read p99s of the pass-criteria statements on a reader instance
// (GetRecord hit and miss, newest-first epoch window, tag-filtered window,
// arrivals window, partition counts, lanes).
//
// Reading the count-scaled tier: its unit is the segment. A segment holds 512
// records instead of about a million, so records a second there are seals a
// second times 512 and say nothing about production ingest; its disk bytes
// are dominated by per-segment overhead, not records. What it measures is the
// engine at terabyte COUNTS: S, runs, catalog runs, partitions written,
// handles, open time, memory, and per-operation latency (commit, maintenance,
// fetch, reads) as they grow. Throughput belongs to the real-record tier and
// sds-tb-gen. The generators keep --tb-gen-threads fetches in flight, each
// waiting for its ack, so throughput is also bounded by fetch latency.
//
// At every --tb-step-gb of store bytes a step row (<prefix>.steps.csv)
// adds a walk of the store (apparent and allocated bytes, files, partition
// runs, catalog runs), checks the file system (stops above --tb-max-used-pct)
// and, with --tb-reopen=1, stops and reopens the engine (open time, bytes
// read). Stops at --tb-max-gb, --tb-max-records or --tb-max-seconds, and
// whenever the file system's available bytes fall under --tb-min-free-gb
// (checked every sample) or would after one more step.
#include <algorithm>
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <thread>
#include <unordered_set>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"
#include "ps/tb_corpus.h"

#if !defined(__wasm__)
#include <filesystem>
#endif

using namespace pst;
using pst::tb::CountingIo;

namespace {

constexpr uint64_t kSec = 1000000000ull;
constexpr uint64_t kGB = 1000000000ull;

struct Knobs {
    std::string dir, csv, mode, corpus, typesDir, mix;
    uint32_t writers, genThreads, partitions, types, fetch, touch, sampleS, readEvery, readReps, handleCap, objects,
        domInflight;
    double zipf, dominantPct, supersedePct, tombPct, retagPct, maxUsedPct, stepGB, maxGB, minFreeGB;
    uint64_t maxRecords, maxSeconds, ackTimeoutS;
    bool reopen, crashReopen, keep, reads;
    // engine
    uint64_t sealBytes, sealRecords, compactSmall, compactMaxOut, foldMax, arrivalsSeg, typeMetaSeg, poolBytes, zeroFill;
    uint32_t compactMaxInputs;
};

Knobs readKnobs() {
    Knobs k;
    k.mode = argStr("tb-mode", "count");
    const bool count = k.mode == "count";
    k.dir = argStr("tb-dir", "");
    k.csv = argStr("tb-csv", "");
    k.corpus = argStr("tb-corpus", "");
    k.typesDir = argStr("tb-types-dir", "");
    k.mix = argStr("tb-mix", "OMM=0.5,CAT=0.15,IQC=0.2,MPE=0.05,*=0.1");
    k.writers = uint32_t(argInt("tb-writers", 8));
    k.genThreads = uint32_t(argInt("tb-gen-threads", 16));
    k.partitions = uint32_t(argInt("tb-partitions", 10000));
    k.types = uint32_t(argInt("tb-types", 30));
    k.fetch = uint32_t(argInt("tb-fetch", 1024));
    k.touch = uint32_t(argInt("tb-touch", 16));
    k.domInflight = uint32_t(argInt("tb-dominant-inflight", 8));  // the dominant's fetches in flight  // records written to every partition first (0: none)
    k.sampleS = uint32_t(argInt("tb-sample-s", 10));
    k.readEvery = uint32_t(argInt("tb-read-every-s", 10));
    k.readReps = uint32_t(argInt("tb-read-reps", 5));
    k.handleCap = uint32_t(argInt("tb-handle-cap", 0));
    k.objects = uint32_t(argInt("tb-objects", 60000));
    k.zipf = double(argInt("tb-zipf-x100", 110)) / 100.0;
    k.dominantPct = double(argInt("tb-dominant-pct", 25));
    k.supersedePct = double(argInt("tb-supersede-pct", 15));
    k.tombPct = double(argInt("tb-tomb-permille", 10)) / 10.0;
    k.retagPct = double(argInt("tb-retag-permille", 10)) / 10.0;
    k.maxUsedPct = double(argInt("tb-max-used-pct", 85));
    k.stepGB = double(argInt("tb-step-gb", 25));
    k.maxGB = double(argInt("tb-max-gb", 200));
    k.minFreeGB = double(argInt("tb-min-free-gb", 20));  // hard floor on the file system's available bytes
    k.maxRecords = uint64_t(argInt("tb-max-records", 0));
    k.maxSeconds = uint64_t(argInt("tb-max-seconds", 0));
    k.ackTimeoutS = uint64_t(argInt("tb-ack-timeout-s", 300));
    k.reopen = argInt("tb-reopen", 1) != 0;
    k.crashReopen = argInt("tb-crash-reopen", 0) != 0;
    k.keep = argInt("tb-keep", 0) != 0;
    k.reads = argInt("tb-reads", 1) != 0;
    // The count-scaled tier shrinks the unit every count axis is measured in.
    k.sealBytes = uint64_t(argInt("tb-seal-bytes", count ? 256 << 10 : 64 << 20));
    k.sealRecords = uint64_t(argInt("tb-seal-records", count ? 512 : 1000000));
    k.compactSmall = uint64_t(argInt("tb-compact-small", count ? 0 : 8 << 20));
    k.compactMaxOut = uint64_t(argInt("tb-compact-max-out", count ? 1 << 20 : 64 << 20));
    k.compactMaxInputs = uint32_t(argInt("tb-compact-max-inputs", 16));
    k.foldMax = uint64_t(argInt("tb-fold-max", count ? 20000 : 2000000));
    k.arrivalsSeg = uint64_t(argInt("tb-arrivals-seg", count ? 1 << 20 : 64 << 20));
    k.typeMetaSeg = uint64_t(argInt("tb-type-meta-seg", count ? 4 << 20 : 64 << 20));
    k.poolBytes = uint64_t(argInt("tb-pool-mib", 192)) << 20;
    // A8 zero-fill ahead: production fills 1 MiB ahead of 64 MiB segments. The
    // count tier scales it with the seal (1/64), or every tiny sealed segment
    // keeps a 1 MiB zero tail and disk bytes measure zero fill, not records.
    k.zeroFill = uint64_t(argInt("tb-zero-fill", count ? int64_t(std::max<uint64_t>(4096, k.sealBytes / 64)) : 1 << 20));
    return k;
}

// The legal ranges of the knobs the count-scaled tier shrinks. RecRow.off is
// a u32 offset into a data file (format.h), so a segment (seal plus the
// commit that crosses it plus one entry) and a compaction output must stay
// under 4 GiB (audit minor: capi_ps.cpp does not validate either).
bool checkKnobs(const Knobs& k, const EngineConfig& c, std::string* why) {
    const uint64_t u32 = 1ull << 32;
    if (k.sealBytes == 0 || k.sealBytes + c.commitBytes + c.maxEntryBytes >= u32) {
        *why = "sealBytes + commitBytes + maxEntryBytes must be > 0 and < 2^32 (u32 RecRow.off)";
        return false;
    }
    if (k.compactMaxOut == 0 || k.compactMaxOut + c.maxEntryBytes >= u32) {
        *why = "compactMaxOutputBytes must be > 0 and < 2^32 (u32 RecRow.off)";
        return false;
    }
    if (k.sealRecords == 0 || k.foldMax == 0 || k.compactMaxInputs < 2) {
        *why = "sealRecords, mergeFoldMaxEntries >= 1 and compactMaxInputs >= 2";
        return false;
    }
    if (k.arrivalsSeg < 64 * kArrivalBytes || k.typeMetaSeg < (64u << 10)) {
        *why = "arrivalsSegBytes >= 64 arrivals and typeMetaSegBytes >= 64 KiB";
        return false;
    }
    return true;
}

struct Part {
    uint32_t pid = 0;
    uint32_t type = 0;
    uint32_t idx = 0;  // within its type
    bool dominant = false;
    double weight = 0;
    std::string peer, provider, source;
    uint64_t batchNo = 0;
    std::unique_ptr<Producer> prod;
};

struct QueryStat {
    std::string name;
    std::vector<double> ms;  // this round
    double p50 = 0, p99 = 0, max = 0;
    uint64_t errors = 0, rows = 0;
};

struct Env {
    Knobs k;
    std::string root;
    std::unique_ptr<CountingIo> cio, rio;
    std::unique_ptr<Engine> e;
    EngineConfig cfg;
    std::vector<tb::TbType> types;
    tb::Corpus corpus;
    std::vector<tb::MinimalFrames> minimal;  // per base type
    std::vector<double> typeByteShare;
    std::vector<Part> parts;
    std::vector<std::atomic<uint64_t>> typeCounter;
    std::vector<uint32_t> partsOfOmm;
    int omm = -1;
    // control
    std::atomic<bool> stop{false}, pause{false};
    std::atomic<uint32_t> paused{0};
    std::mutex pauseMu;
    std::condition_variable pauseCv;
    // counters
    std::atomic<uint64_t> recs{0}, frameBytes{0}, fetches{0}, ackTimeouts{0}, enqueueFails{0}, retags{0},
        tombs{0}, supersedes{0}, domBytes{0}, domLastFetchUs{0}, domFetches{0}, domFetchUs{0}, domHeldUs{0};
    // Fetch latency (first enqueue to ack), this sample window, ms.
    std::mutex fetchMu;
    std::vector<double> fetchMs;
    // recent OMM CIDs (reads)
    std::mutex cidMu;
    std::vector<std::string> cids;
    size_t cidPos = 0;
    std::vector<std::string> ommSources;
    // reads
    std::mutex qMu;
    std::vector<QueryStat> q;
    std::atomic<uint64_t> readRounds{0};
    std::unique_ptr<Reader> reader;
    std::mutex eventsMu;
    std::vector<std::string> events;
    uint64_t t0 = 0;
    void event(const std::string& s) {
        const double t = double(monoNs() - t0) / 1e9;
        char b[64];
        std::snprintf(b, sizeof(b), "[%9.1fs] ", t);
        std::lock_guard<std::mutex> g(eventsMu);
        events.push_back(b + s);
        std::printf("  EVENT %s%s\n", b, s.c_str());
        std::fflush(stdout);
    }
};

// ---- setup ----------------------------------------------------------------------------------
bool setupTypes(Env& env, std::string* err) {
    const Knobs& k = env.k;
    if (!k.typesDir.empty()) {
        if (!tb::loadTypeConfigs(k.typesDir, &env.types, err)) return false;
    } else {
        env.types = tb::testTypes();
    }
    const size_t nBase = env.types.size();
    for (size_t i = 0; i < nBase; i++)
        if (env.types[i].name == "OMM") env.omm = int(i);
    if (env.omm < 0) {
        *err = "no OMM type";
        return false;
    }
    for (uint32_t v = 0; env.types.size() < k.types; v++) {
        const size_t b = v % nBase;
        char fid[5];
        std::snprintf(fid, sizeof(fid), "$%c%02u", env.types[b].name[0], unsigned(v % 100));
        tb::TbType t;
        char name[32];
        std::snprintf(name, sizeof(name), "%sV%02u", env.types[b].name.c_str(), unsigned(v));
        if (!tb::variantType(env.types[b], int(b), fid, name, &t, err)) return false;
        env.types.push_back(std::move(t));
    }
    if (k.mode == "real") {
        if (k.corpus.empty() || !env.corpus.load(k.corpus, env.types, err)) {
            if (err->empty()) *err = "--tb-mode=real needs --tb-corpus";
            return false;
        }
        for (size_t t = 0; t < env.types.size(); t++)
            if (env.corpus.ofType(uint32_t(t)).empty()) {
                *err = "corpus has no record of " + env.types[t].name;
                return false;
            }
    } else {
        env.minimal.resize(nBase);
        for (size_t i = 0; i < nBase; i++) {
            if (!env.minimal[i].init(env.types[i], err)) return false;
            std::printf("  CONFIG minimal %s\n", env.minimal[i].describe().c_str());
        }
    }
    // Byte shares: the mix names base types; "*" is split over the variants.
    std::vector<double> share(env.types.size(), 0);
    double star = 0;
    size_t start = 0;
    const std::string& m = k.mix;
    while (start <= m.size()) {
        const size_t end = std::min(m.find(',', start), m.size());
        const std::string kv = m.substr(start, end - start);
        const size_t eq = kv.find('=');
        if (eq != std::string::npos) {
            const std::string name = kv.substr(0, eq);
            const double w = std::atof(kv.c_str() + eq + 1);
            if (name == "*") star = w;
            for (size_t t = 0; t < nBase; t++)
                if (env.types[t].name == name) share[t] = w;
        }
        start = end + 1;
    }
    const size_t nVar = env.types.size() - nBase;
    for (size_t t = nBase; t < env.types.size(); t++) share[t] = nVar ? star / double(nVar) : 0;
    double sum = 0;
    for (double s : share) sum += s;
    for (auto& s : share) s /= sum;
    env.typeByteShare = share;
    return true;
}

const tb::TbType& baseOf(const Env& env, uint32_t t) {
    const tb::TbType& ty = env.types[t];
    return ty.base >= 0 ? env.types[size_t(ty.base)] : ty;
}

// Registers the types and partitions (idempotent: a reopen gets the same pids).
bool registerAll(Env& env, std::string* err) {
    for (const auto& t : env.types) {
        if (env.e->type(t.fid)) continue;
        if (env.e->registerType(t.config, err) < 0) return false;
    }
    std::atomic<uint32_t> next{0}, failed{0};
    std::vector<std::thread> ts;
    for (uint32_t i = 0; i < std::max(1u, env.k.genThreads); i++)
        ts.emplace_back([&] {
            for (uint32_t j; (j = next.fetch_add(1)) < env.parts.size();) {
                Part& p = env.parts[j];
                uint32_t pid = 0;
                const int32_t rc = env.e->registerPartition(reinterpret_cast<const uint8_t*>(p.peer.data()),
                                                            p.peer.size(), env.types[p.type].fid, &pid);
                if (rc < 0) {
                    if (failed.fetch_add(1) == 0)
                        env.event("registerPartition failed: " + std::to_string(rc) + " at partition " +
                                  std::to_string(j));
                    continue;
                }
                p.pid = pid;
            }
        });
    for (auto& t : ts) t.join();
    if (failed.load()) {
        *err = std::to_string(failed.load()) + " registrations failed";
        return false;
    }
    return true;
}

void planPartitions(Env& env) {
    const Knobs& k = env.k;
    const size_t nt = env.types.size();
    // Partitions per type follow the byte shares (each type at least one).
    std::vector<uint32_t> per(nt, 1);
    uint32_t used = uint32_t(nt);
    for (size_t t = 0; t < nt && used < k.partitions; t++) {
        const uint32_t want = uint32_t(std::floor(env.typeByteShare[t] * double(k.partitions)));
        const uint32_t add = std::min(want > 0 ? want - 1 : 0, k.partitions - used);
        per[t] += add;
        used += add;
    }
    for (size_t t = 0; used < k.partitions; t = (t + 1) % nt) {
        per[t]++;
        used++;
    }
    for (size_t t = 0; t < nt; t++) {
        double prev = 0;
        const tb::TbType& b = baseOf(env, uint32_t(t));
        for (uint32_t i = 0; i < per[t]; i++) {
            Part p;
            p.type = uint32_t(t);
            p.idx = i;
            // Zipf weight of rank i, normalized to the type's share below.
            const double w = 1.0 / std::pow(double(i + 1), k.zipf);
            p.weight = w;
            prev += w;
            p.peer = tb::peerId(uint32_t(env.parts.size()) * 2654435761u + 7);
            const std::string base = b.name == "IQC" ? "IQEngine" : b.name == "CAT" ? "celestrak-satcat" : "celestrak-gp";
            p.provider = "provider-" + std::to_string(i % 64);
            p.source = base + "-" + std::to_string(i % 16);
            env.parts.push_back(std::move(p));
        }
        // Normalize the type's weights to its byte share.
        for (size_t j = env.parts.size() - per[t]; j < env.parts.size(); j++)
            env.parts[j].weight = env.parts[j].weight / prev * env.typeByteShare[t];
    }
    // The dominant partition: OMM's first, taking dominantPct of all bytes.
    for (size_t j = 0; j < env.parts.size(); j++)
        if (int(env.parts[j].type) == env.omm) {
            if (env.partsOfOmm.empty() && k.dominantPct > 0) {
                env.parts[j].dominant = true;
                env.parts[j].weight = 0;  // its own thread
            }
            env.partsOfOmm.push_back(uint32_t(j));
        }
    for (int j = 0; j < 16; j++) env.ommSources.push_back("celestrak-gp-" + std::to_string(j));
}

int32_t openEngine(Env& env, std::string* err, double* openMs, EngineStats* st, uint64_t* openHandles) {
    const uint64_t opens0 = env.cio->opens();
    const uint64_t o0 = monoNs();
    int32_t rc = Engine::open(env.cfg, &env.e, err);
    if (rc < 0) return rc;
    *openMs = double(monoNs() - o0) / 1e6;
    *st = env.e->stats();
    *openHandles = env.cio->opens() - opens0;
    rc = env.e->start();
    if (rc < 0) return rc;
    if (env.reader)
        env.e->setReaderGate([](void* ctx) -> uint64_t { return static_cast<ReaderInstance*>(ctx)->oldestActiveStart(); },
                             env.reader->inst.get());
    return 0;
}

// ---- generators ----------------------------------------------------------------------------
void rememberCid(Env& env, const uint8_t* cid) {
    std::lock_guard<std::mutex> g(env.cidMu);
    std::string s(reinterpret_cast<const char*>(cid), kCidLen);
    if (env.cids.size() < 4096) env.cids.push_back(s);
    else env.cids[env.cidPos++ % env.cids.size()] = s;
}

void generator(Env& env, uint32_t id, std::vector<uint32_t> mine) {
    const Knobs& k = env.k;
    std::vector<double> cdf;
    double acc = 0;
    bool dominantOnly = false;
    for (uint32_t j : mine) {
        acc += env.parts[j].dominant ? 1.0 : env.parts[j].weight;
        cdf.push_back(acc);
        if (env.parts[j].dominant) dominantOnly = true;
    }
    if (mine.empty()) return;
    uint64_t rng = 0x7B0000 + id;
    std::vector<uint8_t> frame;
    uint8_t cid[kCidLen];
    std::vector<std::unique_ptr<std::atomic<int32_t>>> tickets;
    auto freeTicket = [&]() -> std::atomic<int32_t>* {
        for (auto& t : tickets)
            if (t->load() == 0) return t.get();
        tickets.emplace_back(new std::atomic<int32_t>(0));
        return tickets.back().get();
    };
    // One fetch: `count` records to p under a new batch id, then its ack.
    // One fetch: `count` records to p under a new batch id (send), then its
    // ack (complete). A regular partition has one fetch in flight; the
    // dominant one pipelines up to --tb-dominant-inflight (a bulk import).
    struct Sent {
        Part* p = nullptr;
        uint64_t last = 0, bytes = 0, n = 0, f0 = 0;
    };
    auto send = [&](Part& p, uint32_t count) -> Sent {
        Sent out;
        out.p = &p;
        if (!p.prod) p.prod.reset(new Producer(env.e.get(), p.pid));
        if (p.prod->ringDesc()->state.load() == kRingQuarantined) return out;
        p.batchNo++;
        const tb::TbType& ty = env.types[p.type];
        const uint32_t baseIdx = ty.base >= 0 ? uint32_t(ty.base) : p.type;
        const bool supersedes = ty.cfg->hasSupersede();
        const uint8_t* fidOv = ty.base >= 0 ? ty.fid : nullptr;
        const std::string batch = ty.name + "-" + p.source + "-b" + std::to_string(p.batchNo);
        const auto attr = buildRecordAttr(p.peer, p.provider, p.source, batch);
        uint64_t last = 0, bytes = 0, n = 0;
        const uint64_t f0 = monoNs();
        for (uint32_t i = 0; i < count && !env.stop.load(); i++) {
            const uint64_t r = tb::splitmix(&rng);
            const uint64_t kk = env.typeCounter[p.type].fetch_add(1);
            const bool keepId = supersedes && double(r % 10000) < k.supersedePct * 100.0;
            if (k.mode == "real") {
                env.corpus.frame(p.type, kk, keepId, fidOv, &frame);
            } else {
                // Identity: objects cycle on types with an epoch; otherwise every
                // record is its own object. A supersede takes the previous
                // record's identity with new bytes.
                const tb::MinimalFrames& mf = env.minimal[baseIdx];
                auto idOf = [&](uint64_t x) { return mf.hasEpoch() ? x % k.objects + 1 : x + 1; };
                mf.frame(kk, idOf(keepId && kk ? kk - 1 : kk), k.objects, fidOv, &frame);
            }
            if (keepId) env.supersedes.fetch_add(1);
            frameCid(frame, cid);
            uint64_t rseq = 0;
            const int32_t rc = p.prod->enqueue(kEntRecord, kEntCidPresent, int64_t(wallMs()), cid, attr.data(),
                                                uint32_t(attr.size()), frame.data(), uint32_t(frame.size()), &rseq,
                                                true);
            if (rc < 0) {
                env.enqueueFails.fetch_add(1);
                if (p.prod->ringDesc()->state.load() == kRingQuarantined)
                    env.event("partition " + std::to_string(p.pid) + " quarantined (enqueue " + std::to_string(rc) +
                              ")");
                break;
            }
            last = rseq;
            n++;
            bytes += frame.size() - 4;
            if (int(p.type) == env.omm && (r >> 20) % 64 == 0) rememberCid(env, cid);
            // A retag: the same record under another tag tuple (A2 RETAG row).
            if (double((r >> 32) % 100000) < k.retagPct * 1000.0) {
                const auto attr2 = buildRecordAttr(p.peer, p.provider + "-alt", p.source + "-alt", batch);
                if (p.prod->enqueue(kEntRecord, kEntCidPresent, int64_t(wallMs()), cid, attr2.data(),
                                    uint32_t(attr2.size()), frame.data(), uint32_t(frame.size()), &rseq, true) == 0) {
                    last = rseq;
                    env.retags.fetch_add(1);
                }
            }
            // A type-level delete of this record (A14 fan-out, TOMB rows).
            if (double((r >> 44) % 100000) < k.tombPct * 1000.0) {
                std::atomic<int32_t>* t = freeTicket();
                if (env.e->deleteCid(ty.fid, cid, t) == 0) env.tombs.fetch_add(1);
            }
        }
        out.last = last;
        out.bytes = bytes;
        out.n = n;
        out.f0 = f0;
        return out;
    };
    auto complete = [&](const Sent& f) {
        Part& p = *f.p;
        const uint64_t last = f.last, bytes = f.bytes, n = f.n, f0 = f.f0;
        if (!last) return;
        if (p.prod->waitAcked(last, k.ackTimeoutS * kSec) != 0) {
            if (env.ackTimeouts.fetch_add(1) < 20)
                env.event("ack timeout: partition " + std::to_string(p.pid) + " (" + env.types[p.type].name + ", " +
                          (p.dominant ? "dominant" : "regular") + ") after " + std::to_string(k.ackTimeoutS) + " s");
            return;
        }
        env.recs.fetch_add(n);
        env.frameBytes.fetch_add(bytes);
        env.fetches.fetch_add(1);
        const double fms = double(monoNs() - f0) / 1e6;
        if (p.dominant) {
            env.domBytes.fetch_add(bytes);
            env.domLastFetchUs.store(uint64_t(fms * 1000));
            env.domFetches.fetch_add(1);
            env.domFetchUs.fetch_add(uint64_t(fms * 1000));
        }
        {
            std::lock_guard<std::mutex> g(env.fetchMu);
            env.fetchMs.push_back(fms);
        }
    };
    auto fetch = [&](Part& p, uint32_t count) { complete(send(p, count)); };
    std::deque<Sent> inflight;  // the dominant partition's pipelined fetches
    auto pauseHere = [&] {
        for (; !inflight.empty(); inflight.pop_front()) complete(inflight.front());
        for (uint32_t j : mine) env.parts[j].prod.reset();
        std::unique_lock<std::mutex> lk(env.pauseMu);
        env.paused.fetch_add(1);
        env.pauseCv.notify_all();
        env.pauseCv.wait(lk, [&] { return !env.pause.load() || env.stop.load(); });
        env.paused.fetch_sub(1);
    };
    // Every partition is written once first (partitions written since boot:
    // audit B2), then the skewed stream.
    for (size_t i = 0; i < mine.size() && k.touch && !env.stop.load(); i++) {
        if (env.pause.load()) pauseHere();
        Part& p = env.parts[mine[i]];
        if (!p.pid || p.dominant) continue;
        fetch(p, k.touch);
        if ((i & 63) == 63)
            for (uint32_t j : mine) env.parts[j].prod.reset();  // producers are per fetch here
    }
    while (!env.stop.load()) {
        if (env.pause.load()) {
            pauseHere();
            continue;
        }
        const double u = double(tb::splitmix(&rng) >> 11) * (acc / 9007199254740992.0);
        const size_t pick = size_t(std::lower_bound(cdf.begin(), cdf.end(), u) - cdf.begin());
        Part& p = env.parts[mine[std::min(pick, mine.size() - 1)]];
        if (!p.pid) continue;
        if (dominantOnly) {
            // Hold the dominant partition to its share of the bytes.
            const uint64_t all = env.frameBytes.load() + 1;
            if (double(env.domBytes.load()) > k.dominantPct / 100.0 * double(all) && env.recs.load() > 20000) {
                sleepNs(2000000);
                env.domHeldUs.fetch_add(2000);
                continue;
            }
        }
        if (p.prod && p.prod->ringDesc()->state.load() == kRingQuarantined) {
            sleepNs(10000000);
            continue;
        }
        if (p.dominant) {
            inflight.push_back(send(p, k.fetch));
            while (inflight.size() >= std::max(1u, k.domInflight)) {
                complete(inflight.front());
                inflight.pop_front();
            }
        } else {
            fetch(p, k.fetch);
        }
    }
    for (; !inflight.empty(); inflight.pop_front()) complete(inflight.front());
    for (uint32_t j : mine) env.parts[j].prod.reset();
    // Tickets must outlive the deletes they track.
    for (auto& t : tickets)
        for (uint64_t w0 = monoNs(); t->load() > 0 && monoNs() - w0 < 30 * kSec;) sleepNs(1000000);
}

// ---- reads ----------------------------------------------------------------------------------
void reads(Env& env) {
    const Knobs& k = env.k;
    uint64_t rng = 0xBEAD;
    const std::string omm = env.types[size_t(env.omm)].name;
    struct Q {
        std::string name, sql;
        int param;  // 0 none, 1 hit cid, 2 miss cid, 3 source
    };
    const std::vector<Q> qs = {
        {"hit", "SELECT _cid_bin, _gseq, _pseq, _epoch, _data FROM " + omm + " WHERE _cid_bin = ?1", 1},
        {"miss", "SELECT _cid_bin, _gseq, _pseq, _epoch, _data FROM " + omm + " WHERE _cid_bin = ?1", 2},
        {"epoch", "SELECT _cid_bin, _epoch FROM " + omm + " ORDER BY _epoch DESC LIMIT 100", 0},
        {"tag", "SELECT _cid_bin, _epoch FROM " + omm + " WHERE _source_name = ?1 ORDER BY _epoch DESC LIMIT 100", 3},
        {"arrivals", "SELECT _gseq, _cid_bin FROM " + omm + " ORDER BY _gseq DESC LIMIT 100", 0},
        {"parts", "SELECT count(*), sum(live_count) FROM flatsql_partitions", 0},
        {"lanes", "SELECT count(*), sum(count) FROM flatsql_lanes", 0},
    };
    {
        std::lock_guard<std::mutex> g(env.qMu);
        env.q.clear();
        for (const auto& q : qs) env.q.push_back(QueryStat{q.name, {}, 0, 0, 0, 0, 0});
    }
    uint64_t next = monoNs();
    while (!env.stop.load()) {
        if (monoNs() < next || env.pause.load()) {
            sleepNs(100000000);
            continue;
        }
        next = monoNs() + uint64_t(k.readEvery) * kSec;
        std::vector<QueryStat> round;
        for (const auto& q : qs) {
            QueryStat s;
            s.name = q.name;
            for (uint32_t r = 0; r < k.readReps && !env.stop.load() && !env.pause.load(); r++) {
                std::vector<Param> ps;
                if (q.param == 1) {
                    std::lock_guard<std::mutex> g(env.cidMu);
                    if (env.cids.empty()) break;
                    ps.push_back(Param::blob(env.cids[tb::splitmix(&rng) % env.cids.size()]));
                } else if (q.param == 2) {
                    std::string c(kCidLen, '\0');
                    c[0] = 0x01, c[1] = 0x55, c[2] = 0x12, c[3] = 0x20;
                    for (size_t i = 4; i < kCidLen; i++) c[i] = char(tb::splitmix(&rng));
                    ps.push_back(Param::blob(c));
                } else if (q.param == 3) {
                    ps.push_back(Param::text(env.ommSources[tb::splitmix(&rng) % env.ommSources.size()]));
                }
                const uint64_t t0 = monoNs();
                const Rows rows = env.reader->q(q.sql, ps);
                const double ms = double(monoNs() - t0) / 1e6;
                if (rows.status != 0 || !rows.decodeOk) {
                    if (s.errors++ == 0 && env.readRounds.load() < 50)
                        env.event("read " + q.name + " failed: " + std::to_string(rows.status) + " " + rows.error);
                    continue;
                }
                s.rows += rows.rows.size();
                s.ms.push_back(ms);
            }
            if (!s.ms.empty()) {
                s.p50 = tb::pct(s.ms, 0.5);
                s.p99 = tb::pct(s.ms, 0.99);
                s.max = *std::max_element(s.ms.begin(), s.ms.end());
            }
            round.push_back(std::move(s));
        }
        std::lock_guard<std::mutex> g(env.qMu);
        env.q = std::move(round);
        env.readRounds.fetch_add(1);
    }
}

// ---- store walk -------------------------------------------------------------------------------
struct Walk {
    tb::DirUsage du;
    uint64_t partRuns = 0, partRunsMax = 0, catRuns = 0, catRunsMax = 0, dataFiles = 0;
};

Walk walkStore(const std::string& root) {
    Walk w;
    w.du = tb::dirUsage(root);
#if !defined(__wasm__)
    std::error_code ec;
    for (const char* sub : {"p", "t"}) {
        const std::string d = root + "/fsql2/" + sub;
        for (auto& e : std::filesystem::directory_iterator(d, ec)) {
            uint64_t runs = 0;
            std::error_code ec2;
            for (auto& f : std::filesystem::directory_iterator(e.path(), ec2)) {
                const std::string n = f.path().filename().string();
                if (n.compare(0, 2, "x-") == 0) runs++;
                if (sub[0] == 'p' && (n.compare(0, 2, "d-") == 0 || n.compare(0, 2, "c-") == 0) &&
                    n.size() > 4 && n.compare(n.size() - 4, 4, ".fsd") == 0)
                    w.dataFiles++;
            }
            if (sub[0] == 'p') {
                w.partRuns += runs;
                w.partRunsMax = std::max(w.partRunsMax, runs);
            } else {
                w.catRuns += runs;
                w.catRunsMax = std::max(w.catRunsMax, runs);
            }
        }
    }
#endif
    return w;
}

}  // namespace

PS_SLOW_TEST(tb_engine_tier) {
    auto envp = std::make_unique<Env>();
    Env& env = *envp;
    env.k = readKnobs();
    const Knobs& k = env.k;
    REQUIRE(!k.dir.empty());
    REQUIRE(k.mode == "real" || k.mode == "count");
    env.root = k.dir;
    const std::string csvPrefix = k.csv.empty() ? k.dir + "/../tb-" + k.mode : k.csv;
    std::string err;
    std::printf("  TB machine: %s\n", tb::machineLine().c_str());
    if (!setupTypes(env, &err)) {
        std::fprintf(stderr, "  setup: %s\n", err.c_str());
        REQUIRE(false);
    }
    env.typeCounter = std::vector<std::atomic<uint64_t>>(env.types.size());
    planPartitions(env);
    // Engine config: SDN's writer (format2/writer.go TLVs) plus the knobs.
    env.cio.reset(new CountingIo(importIo(), k.handleCap));
    env.rio.reset(new CountingIo(importIo(), 0));
    EngineConfig& c = env.cfg;
    c.root = k.dir;
    c.io = env.cio.get();
    c.writers = k.writers;
    c.poolBytes = k.poolBytes;
    c.lockStats = true;
    c.sealBytes = k.sealBytes;
    c.sealRecords = k.sealRecords;
    c.compactSmallBytes = k.compactSmall;
    c.compactMaxOutputBytes = k.compactMaxOut;
    c.compactMaxInputs = k.compactMaxInputs;
    c.mergeFoldMaxEntries = k.foldMax;
    c.arrivalsSegBytes = k.arrivalsSeg;
    c.typeMetaSegBytes = k.typeMetaSeg;
    c.zeroFillStep = k.zeroFill;
    std::string why;
    if (!checkKnobs(k, c, &why)) {
        std::fprintf(stderr, "  illegal knobs: %s\n", why.c_str());
        REQUIRE(false);
    }
    std::printf("  CONFIG mode=%s writers=%u gen_threads=%u partitions=%zu types=%zu fetch=%u seal_bytes=%llu "
                "seal_records=%llu compact_small=%llu compact_max_out=%llu fold_max=%llu arrivals_seg=%llu "
                "type_meta_seg=%llu zero_fill=%llu handle_cap=%u dominant_pct=%.0f zipf=%.2f\n",
                k.mode.c_str(), k.writers, k.genThreads, env.parts.size(), env.types.size(), k.fetch,
                (unsigned long long)k.sealBytes, (unsigned long long)k.sealRecords, (unsigned long long)k.compactSmall,
                (unsigned long long)k.compactMaxOut, (unsigned long long)k.foldMax, (unsigned long long)k.arrivalsSeg,
                (unsigned long long)k.typeMetaSeg, (unsigned long long)k.zeroFill, k.handleCap, k.dominantPct, k.zipf);
    for (size_t t = 0; t < env.types.size(); t++) {
        uint32_t n = 0;
        for (const auto& p : env.parts) n += p.type == t;
        std::printf("  CONFIG type %s fid=%.4s share=%.3f partitions=%u%s\n", env.types[t].name.c_str(),
                    reinterpret_cast<const char*>(env.types[t].fid), env.typeByteShare[t], n,
                    env.types[t].base >= 0 ? " (variant)" : "");
    }
    std::fflush(stdout);
#if !defined(__wasm__)
    std::error_code ec;
    std::filesystem::create_directories(k.dir, ec);
#endif
    const uint64_t fsTotal = tb::fsTotalBytes(k.dir);
    env.t0 = monoNs();
    double openMs = 0;
    EngineStats ost;
    uint64_t openHandles = 0;
    if (openEngine(env, &err, &openMs, &ost, &openHandles) < 0) {
        std::fprintf(stderr, "  open: %s\n", err.c_str());
        REQUIRE(false);
    }
    const uint64_t r0 = monoNs();
    if (!registerAll(env, &err)) {
        std::fprintf(stderr, "  register: %s\n", err.c_str());
        REQUIRE(false);
    }
    env.event("registered " + std::to_string(env.types.size()) + " types and " + std::to_string(env.parts.size()) +
              " partitions in " + std::to_string(double(monoNs() - r0) / 1e9) + " s");
    if (k.reads) {
        ReaderConfig rc;
        rc.root = k.dir;
        rc.io = env.rio.get();
        rc.cls = LaneClass::Interactive;
        rc.lanes = 2;
        env.reader.reset(new Reader(rc));
        REQUIRE(env.reader->inst != nullptr);
        env.e->setReaderGate([](void* ctx) -> uint64_t { return static_cast<ReaderInstance*>(ctx)->oldestActiveStart(); },
                             env.reader->inst.get());
    }
    // CSV
    tb::Csv samples, steps;
    const std::vector<std::string> sampleCols = {
        "t_s", "step", "records", "frame_bytes", "rec_s", "frame_mb_s", "fetches", "disk_bytes", "type_disk_bytes",
        "engine_written_bytes", "proc_write_bytes", "write_amp", "at_rest_ratio", "fs_used_pct", "fs_free_bytes",
        "heap_bytes", "rss_bytes", "wasm_pages", "committed_bytes", "accel_bytes", "descriptor_bytes",
        "pool_committed_bytes", "handles_open", "handles_hw", "handles_refused", "reader_handles_hw", "fds",
        "partitions", "partitions_written", "segs_max", "segs_total", "dominant_share", "merges", "seals", "seals_s",
        "merges_s", "fetch_p50_ms", "fetch_p99_ms", "dominant_fetch_ms", "dominant_fetches", "dominant_fetch_s_total",
        "dominant_held_s_total",
        "compactions", "type_commits", "l0_full_stalls", "maint_p99_ms", "maint_p999_ms", "maint_n",
        "commit_p99_ms", "commit_p999_ms", "commit_n", "maint_ms_per_merge", "label_lag_rows_max",
        "label_lag_rows_sum", "quarantined", "rejects", "dedupe_hits", "tombs", "retags", "supersedes",
        "ack_timeouts", "enqueue_fails", "read_rounds", "q_hit_p99_ms", "q_miss_p99_ms", "q_epoch_p99_ms",
        "q_tag_p99_ms", "q_arrivals_p99_ms", "q_parts_p99_ms", "q_lanes_p99_ms", "q_errors"};
    const std::vector<std::string> stepCols = {
        "step", "t_s", "records", "frame_bytes", "disk_bytes", "du_apparent", "du_allocated", "files", "data_files",
        "partitions", "partitions_written", "segs_max", "segs_total", "part_runs_total", "part_runs_max",
        "cat_runs_total", "cat_runs_max", "handles_hw", "heap_bytes", "rss_bytes", "committed_bytes", "accel_bytes",
        "descriptor_bytes", "rec_s_window", "maint_p99_ms", "commit_p99_ms", "open_ms", "open_read_bytes",
        "open_handles", "open_heap_bytes", "fs_used_pct"};
    REQUIRE(samples.open(csvPrefix + ".samples.csv", sampleCols));
    REQUIRE(steps.open(csvPrefix + ".steps.csv", stepCols));
    // Generators: the dominant partition has its own thread; the rest are
    // dealt round-robin within each type.
    std::vector<std::vector<uint32_t>> assign(std::max(2u, k.genThreads));
    uint32_t rr = 1;
    for (uint32_t j = 0; j < env.parts.size(); j++) {
        if (env.parts[j].dominant) assign[0].push_back(j);
        else assign[1 + (rr++ % (assign.size() - 1))].push_back(j);
    }
    std::vector<std::thread> gens;
    for (uint32_t t = 0; t < assign.size(); t++) gens.emplace_back(generator, std::ref(env), t, assign[t]);
    std::thread rd;
    if (k.reads) rd = std::thread(reads, std::ref(env));
    // Sampler.
    tb::HistWindow maint, commit;
    maint.take(env.e->maintHist());
    commit.take(env.e->commitHist());
    const uint64_t stepBytes = uint64_t(k.stepGB * double(kGB));
    uint64_t nextStep = stepBytes;
    uint32_t step = 0;
    uint64_t lastRecs = 0, lastBytes = 0, lastT = monoNs(), lastMerges = 0, lastSeals = 0, stepRecs = 0,
             stepT = monoNs();
    uint64_t procW0 = 0;
    bool crossedCap = false;
    auto procWrite = []() -> uint64_t {
#if defined(__linux__)
        FILE* f = std::fopen("/proc/self/io", "r");
        if (!f) return 0;
        char line[128];
        uint64_t v = 0;
        while (std::fgets(line, sizeof(line), f))
            if (std::strncmp(line, "write_bytes:", 12) == 0) v = std::strtoull(line + 12, nullptr, 10);
        std::fclose(f);
        return v;
#else
        return 0;
#endif
    };
    procW0 = procWrite();
    std::string stopWhy;
    uint64_t writtenBase = 0;  // engine write counters restart at a reopen
    for (;;) {
        sleepNs(uint64_t(k.sampleS) * kSec);
        const uint64_t now = monoNs();
        const EngineStats st = env.e->stats();
        IoStats io;
        env.e->totalIo(&io);
        uint64_t wrote = writtenBase;
        for (size_t i = 0; i < size_t(FileClass::Count); i++) wrote += io.writeBytes(FileClass(i));
        maint.take(env.e->maintHist());
        commit.take(env.e->commitHist());
        uint64_t segsMax = 0, segsTotal = 0, lagMax = 0, lagSum = 0, quar = 0, written = 0;
        for (const auto& p : env.parts) {
            Partition* pp = p.pid ? env.e->partition(p.pid) : nullptr;
            if (!pp) continue;
            size_t n;
            {
                std::lock_guard<std::mutex> g(pp->sumMu);
                n = pp->summary.size();
            }
            segsMax = std::max<uint64_t>(segsMax, n);
            segsTotal += n;
            const uint64_t hi = pp->durablePseqHi.load(), lt = pp->labeledThrough.load();
            if (hi) written++;
            if (hi > lt) {
                lagMax = std::max(lagMax, hi - lt);
                lagSum += hi - lt;
            }
            if (pp->ring->state.load() == kRingQuarantined) quar++;
        }
        const uint64_t recs = env.recs.load(), bytes = env.frameBytes.load();
        const double dt = double(now - lastT) / 1e9;
        const uint64_t disk = st.diskBytes + st.typeDiskBytes;
        // Engine counters restart at a reopen.
        const uint64_t dMerges = st.merges >= lastMerges ? st.merges - lastMerges : st.merges;
        const uint64_t dSeals = st.seals >= lastSeals ? st.seals - lastSeals : st.seals;
        std::vector<double> fl;
        {
            std::lock_guard<std::mutex> g(env.fetchMu);
            fl.swap(env.fetchMs);
        }
        std::vector<QueryStat> q;
        {
            std::lock_guard<std::mutex> g(env.qMu);
            q = env.q;
        }
        auto qp = [&](const char* n) {
            for (const auto& s : q)
                if (s.name == n) return s.p99;
            return 0.0;
        };
        uint64_t qerr = 0;
        for (const auto& s : q) qerr += s.errors;
        if (!crossedCap && env.cio->highWater() > 16384) {
            crossedCap = true;
            env.event("host handles passed 16,384 (SDN MaxHandles, audit B2) at " + std::to_string(recs) +
                      " records, " + std::to_string(written) + " partitions written");
        }
        const uint64_t pw = procWrite();
        samples.row({double(now - env.t0) / 1e9, double(step), double(recs), double(bytes),
                     double(recs - lastRecs) / dt, double(bytes - lastBytes) / dt / 1e6, double(env.fetches.load()),
                     double(st.diskBytes), double(st.typeDiskBytes), double(wrote), double(pw - procW0),
                     bytes ? double(wrote) / double(bytes) : 0, bytes ? double(disk) / double(bytes) : 0,
                     tb::fsUsedPct(k.dir), double(tb::fsFreeBytes(k.dir)), double(tb::heapBytes()),
                     double(tb::rssBytes()), double(tb::wasmPages()), double(st.committedBytes),
                     double(st.acceleratorBytes), double(st.descriptorBytes), double(st.poolCommittedBytes),
                     double(env.cio->openNow()), double(env.cio->highWater()), double(env.cio->refused()),
                     double(env.rio->highWater()), double(tb::openFds()), double(env.parts.size()), double(written),
                     double(segsMax), double(segsTotal),
                     bytes ? double(env.domBytes.load()) / double(bytes) : 0, double(st.merges), double(st.seals),
                     double(dSeals) / dt, double(dMerges) / dt, tb::pct(fl, 0.5), tb::pct(fl, 0.99),
                     double(env.domLastFetchUs.load()) / 1000, double(env.domFetches.load()),
                     double(env.domFetchUs.load()) / 1e6, double(env.domHeldUs.load()) / 1e6,
                     double(st.compactions), double(st.typeCommits), double(st.l0FullStalls),
                     double(maint.percentileNs(0.99)) / 1e6, double(maint.percentileNs(0.999)) / 1e6,
                     double(maint.count()), double(commit.percentileNs(0.99)) / 1e6,
                     double(commit.percentileNs(0.999)) / 1e6, double(commit.count()),
                     dMerges ? maint.sumNs() / 1e6 / double(dMerges) : 0, double(lagMax), double(lagSum),
                     double(quar), double(st.rejects), double(st.dedupeHits), double(env.tombs.load()),
                     double(env.retags.load()), double(env.supersedes.load()), double(env.ackTimeouts.load()),
                     double(env.enqueueFails.load()), double(env.readRounds.load()), qp("hit"), qp("miss"),
                     qp("epoch"), qp("tag"), qp("arrivals"), qp("parts"), qp("lanes"), double(qerr)});
        std::printf("  TB t=%.0fs recs=%llu (%.0f/s) frames=%.2fGB disk=%.2fGB amp=%.2f heap=%.0fMB committed=%.0fMB "
                    "accel=%.0fMB handles=%llu/%llu S_max=%llu segs=%llu maint_p99=%.2fms commit_p99=%.2fms lag=%llu "
                    "q_hit=%.1f q_epoch=%.1f q_tag=%.1f q_lanes=%.1f\n",
                    double(now - env.t0) / 1e9, (unsigned long long)recs, double(recs - lastRecs) / dt, bytes / 1e9,
                    disk / 1e9, bytes ? double(wrote) / double(bytes) : 0, tb::heapBytes() / 1e6,
                    st.committedBytes / 1e6, st.acceleratorBytes / 1e6, (unsigned long long)env.cio->openNow(),
                    (unsigned long long)env.cio->highWater(), (unsigned long long)segsMax,
                    (unsigned long long)segsTotal, double(maint.percentileNs(0.99)) / 1e6,
                    double(commit.percentileNs(0.99)) / 1e6, (unsigned long long)lagMax, qp("hit"), qp("epoch"),
                    qp("tag"), qp("lanes"));
        std::fflush(stdout);
        lastRecs = recs;
        lastBytes = bytes;
        lastT = now;
        lastMerges = st.merges;
        lastSeals = st.seals;
        // Stop conditions.
        if (k.maxRecords && recs >= k.maxRecords) stopWhy = "max records";
        if (k.maxSeconds && now - env.t0 >= k.maxSeconds * kSec) stopWhy = "max seconds";
        if (disk >= uint64_t(k.maxGB * double(kGB))) stopWhy = "max GB";
        if (tb::fsUsedPct(k.dir) >= k.maxUsedPct) stopWhy = "file system above max used pct";
        if (double(tb::fsFreeBytes(k.dir)) < k.minFreeGB * double(kGB)) stopWhy = "file system available below the floor";
        const bool atStep = disk >= nextStep || !stopWhy.empty();
        if (atStep) {
            const Walk w = walkStore(k.dir);
            double reopenMs = 0;
            EngineStats rst;
            uint64_t rHandles = 0, rHeap = 0;
            const double used = tb::fsUsedPct(k.dir);
            if (used >= k.maxUsedPct && stopWhy.empty()) stopWhy = "file system above max used pct";
            if (fsTotal && double(tb::fsFreeBytes(k.dir)) < double(stepBytes) * 1.2 + k.minFreeGB * double(kGB) &&
                stopWhy.empty())
                stopWhy = "not enough free space for another step above the floor";
            if (k.reopen || k.crashReopen) {
                env.pause.store(true);
                {
                    std::unique_lock<std::mutex> lk(env.pauseMu);
                    env.pauseCv.wait_for(lk, std::chrono::seconds(600),
                                         [&] { return env.paused.load() >= gens.size(); });
                }
                IoStats io2;
                env.e->totalIo(&io2);
                for (size_t i = 0; i < size_t(FileClass::Count); i++) writtenBase += io2.writeBytes(FileClass(i));
                if (k.crashReopen) env.e->abandon();
                else env.e->stop();
                env.e.reset();
                if (openEngine(env, &err, &reopenMs, &rst, &rHandles) < 0) {
                    env.event("reopen failed: " + err);
                    stopWhy = "reopen failed";
                }
                rHeap = tb::heapBytes();
                env.pause.store(false);
                env.pauseCv.notify_all();
            }
            const double wdt = double(now - stepT) / 1e9;
            steps.row({double(step), double(now - env.t0) / 1e9, double(recs), double(bytes), double(disk),
                       double(w.du.apparent), double(w.du.allocated), double(w.du.files), double(w.dataFiles),
                       double(env.parts.size()), double(written), double(segsMax), double(segsTotal),
                       double(w.partRuns), double(w.partRunsMax), double(w.catRuns), double(w.catRunsMax),
                       double(env.cio->highWater()), double(tb::heapBytes()), double(tb::rssBytes()),
                       double(st.committedBytes), double(st.acceleratorBytes), double(st.descriptorBytes),
                       double(recs - stepRecs) / wdt, double(maint.percentileNs(0.99)) / 1e6,
                       double(commit.percentileNs(0.99)) / 1e6, reopenMs, double(rst.openReadBytes),
                       double(rHandles), double(rHeap), used});
            std::printf("  STEP %u disk=%.1fGB du=%.1fGB(apparent %.1fGB) files=%llu records=%llu S_max=%llu "
                        "part_runs=%llu (max %llu) cat_runs=%llu (max %llu) handles_hw=%llu reopen=%.0fms "
                        "(%.1fMB read) used=%.1f%%\n",
                        step, disk / 1e9, w.du.allocated / 1e9, w.du.apparent / 1e9, (unsigned long long)w.du.files,
                        (unsigned long long)recs, (unsigned long long)segsMax, (unsigned long long)w.partRuns,
                        (unsigned long long)w.partRunsMax, (unsigned long long)w.catRuns,
                        (unsigned long long)w.catRunsMax, (unsigned long long)env.cio->highWater(), reopenMs,
                        rst.openReadBytes / 1e6, used);
            std::fflush(stdout);
            step++;
            stepRecs = recs;
            stepT = now;
            while (nextStep <= disk) nextStep += stepBytes;
        }
        if (!stopWhy.empty()) break;
    }
    env.event("stopping: " + stopWhy);
    env.stop.store(true);
    env.pauseCv.notify_all();
    for (auto& t : gens) t.join();
    if (rd.joinable()) rd.join();
    const EngineStats st = env.e->stats();
    report("records", double(env.recs.load()), "records");
    report("frame_bytes", double(env.frameBytes.load()), "B");
    report("disk_bytes", double(st.diskBytes + st.typeDiskBytes), "B");
    report("handles_high_water", double(env.cio->highWater()), "handles");
    report("ack_timeouts", double(env.ackTimeouts.load()), "n");
    report("seconds", double(monoNs() - env.t0) / 1e9, "s");
    env.e->setReaderGate(nullptr, nullptr);
    env.reader.reset();
    env.e->stop();
    env.e.reset();
    {
        std::lock_guard<std::mutex> g(env.eventsMu);
        for (const auto& ev : env.events) std::printf("  EVENTLOG %s\n", ev.c_str());
    }
#if !defined(__wasm__)
    if (!k.keep) std::filesystem::remove_all(k.dir, ec);
#endif
}

// Writes the corpus with each record's patch sites (TBC2) for the SDN
// end-to-end generator (sdn-server/cmd/sds-tb-gen), so both tiers clone
// records the same way without a FlatBuffers reflection library in Go:
//
//   flatsql_ps_test --test=tb_corpus_export --tb-corpus=<corpus.tbc>
//       --tb-types-dir=<types> --tb-out=<corpus.tbc2>
//
//   "TBC2" u32 version=2 u64 count
//   count x { fid[4] u32 len bytes[len] 5 x (u16 len, bytes): peer, provider,
//             source, batch, supersede key; u16 n; n x {u32 off, u8 kind,
//             u8 len, u8 identity, u8 pad} }
// Only records valid under the production schema are written (Corpus::load).
PS_SLOW_TEST(tb_corpus_export) {
    const std::string in = argStr("tb-corpus", ""), out = argStr("tb-out", ""), dir = argStr("tb-types-dir", "");
    REQUIRE(!in.empty() && !out.empty() && !dir.empty());
    std::vector<tb::TbType> types;
    std::string err;
    REQUIRE(tb::loadTypeConfigs(dir, &types, &err));
    tb::Corpus c;
    if (!c.load(in, types, &err)) {
        std::fprintf(stderr, "  corpus: %s\n", err.c_str());
        REQUIRE(false);
    }
    FILE* f = std::fopen(out.c_str(), "wb");
    REQUIRE(f != nullptr);
    uint8_t hdr[16] = {'T', 'B', 'C', '2'};
    putU32(hdr + 4, 2);
    putU64(hdr + 8, c.size());
    std::fwrite(hdr, 1, sizeof(hdr), f);
    std::vector<uint32_t> perType(types.size(), 0);
    for (const auto& r : c.records()) {
        uint8_t b[8];
        std::fwrite(types[r.type].fid, 1, 4, f);
        putU32(b, uint32_t(r.fb.size()));
        std::fwrite(b, 1, 4, f);
        std::fwrite(r.fb.data(), 1, r.fb.size(), f);
        for (const std::string* s : {&r.peer, &r.provider, &r.source, &r.batch, &r.supersedeKey}) {
            putU16(b, uint16_t(s->size()));
            std::fwrite(b, 1, 2, f);
            std::fwrite(s->data(), 1, s->size(), f);
        }
        putU16(b, uint16_t(r.patches.size()));
        std::fwrite(b, 1, 2, f);
        for (const auto& p : r.patches) {
            putU32(b, p.off);
            b[4] = p.kind;
            b[5] = p.len;
            b[6] = p.identity ? 1 : 0;
            b[7] = 0;
            std::fwrite(b, 1, 8, f);
        }
        perType[r.type]++;
    }
    std::fclose(f);
    for (size_t t = 0; t < types.size(); t++)
        std::printf("  EXPORT %s: %u records\n", types[t].name.c_str(), perType[t]);
    report("records", double(c.size()), "records");
}

// The clone generator's validity on the real corpus (the adversarial check of
// the real-record tier): every clone of every seed must verify under its
// production schema, have its own CID, and, when it has an epoch, keep its
// seed's identity bytes (the same object at a later epoch).
//
//   flatsql_ps_test --test=tb_clone_check --tb-corpus=<corpus.tbc> --tb-types-dir=<types> [--tb-clones=3]
PS_SLOW_TEST(tb_clone_check) {
    const std::string in = argStr("tb-corpus", ""), dir = argStr("tb-types-dir", "");
    const uint64_t clones = uint64_t(argInt("tb-clones", 3));
    REQUIRE(!in.empty() && !dir.empty());
    std::vector<tb::TbType> types;
    std::string err;
    REQUIRE(tb::loadTypeConfigs(dir, &types, &err));
    tb::Corpus c;
    REQUIRE(c.load(in, types, &err));
    std::vector<uint8_t> f;
    uint8_t cid[kCidLen];
    uint64_t total = 0, invalid = 0, dupes = 0, idMoved = 0, withEpoch = 0;
    for (uint32_t t = 0; t < types.size(); t++) {
        const size_t n = c.ofType(t).size();
        if (!n) continue;
        std::unordered_set<std::string> seen;
        seen.reserve(n * (clones + 1));
        uint64_t tInvalid = 0, tDupes = 0;
        for (uint64_t k = 0; k < n * (clones + 1); k++) {
            c.frame(t, k, false, nullptr, &f);
            total++;
            if (types[t].cfg->checkFrame(f.data(), f.size()) != 0) tInvalid++;
            frameCid(f, cid);
            if (!seen.insert(std::string(reinterpret_cast<char*>(cid), kCidLen)).second) tDupes++;
            const tb::SeedRecord& s = c.seedFor(t, k);
            bool epoch = false;
            for (const auto& p : s.patches)
                if (p.kind == tb::Patch::kEpochStr || p.kind == tb::Patch::kEpochF64) epoch = true;
            if (!epoch || k < n) continue;
            withEpoch++;
            for (const auto& p : s.patches)
                if (p.identity) {
                    const size_t w = p.kind == tb::Patch::kInt64 ? 8 : 4;
                    if (std::memcmp(f.data() + 4 + p.off, s.fb.data() + p.off, w) != 0) {
                        idMoved++;
                        break;
                    }
                }
        }
        std::printf("  CLONES %s: %zu seeds x %llu = %zu frames, %llu invalid, %llu duplicate CIDs\n",
                    types[t].name.c_str(), n, (unsigned long long)(clones + 1), size_t(n * (clones + 1)),
                    (unsigned long long)tInvalid, (unsigned long long)tDupes);
        invalid += tInvalid;
        dupes += tDupes;
    }
    report("clone_frames", double(total), "n");
    report("clone_invalid", double(invalid), "n");
    report("clone_duplicate_cids", double(dupes), "n");
    report("clone_epoch_records_identity_moved", double(idMoved), "n");
    report("clone_epoch_records_checked", double(withEpoch), "n");
    CHECK_EQ(invalid, 0u);
    CHECK_EQ(dupes, 0u);
    CHECK_EQ(idMoved, 0u);
}
