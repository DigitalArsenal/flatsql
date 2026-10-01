// Terabyte harness support (flatsql-ps-terabyte-harness; the TB audit at
// flatsql 0df6ef4, §4 "TB test plan").
//
// Shared by the three TB tiers and the unit gates:
//   - TbType: a type config, either a production one (t/<fid>/s-*.fsc copied
//     out of a migrated host-02 store: the real BFBS and rules) or a test type;
//     variants of one type under another file identifier (the audit's 30
//     types from the few real configs).
//   - Corpus: real records (scripts/tb/extract-corpus.py writes the file) and
//     the clone generator: clone c of record i shifts its epoch fields (61 s a
//     clone) and perturbs its numeric fields in place, so every clone has its
//     own CID while the bytes and tags stay the real ones; a record with an
//     epoch keeps its identity (the same object later), one without shifts it.
//     Nothing generated is stored twice.
//   - Minimal frames: the smallest valid record of a type (count-scaled tier).
//   - CountingIo: the host's virtual-handle table (the Go host allows
//     MaxHandles = 16,384 per instance; audit B2), counted and optionally capped.
//   - Process gauges (heap, RSS, wasm pages, fds, disk), window percentiles of
//     the engine's histograms, a CSV writer.
//   - headLabeledThrough: labeled_through as a host reads it from the durable
//     head and type log (audit B1).
#ifndef FLATSQL_PS_TB_CORPUS_H
#define FLATSQL_PS_TB_CORPUS_H

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "flatsql/typecfg/extract.h"
#include "flatsql/ps/io.h"
#include "flatsql/ps/writer.h"
#include "ps/ps_test.h"

namespace pst {
namespace tb {

// ---- types -------------------------------------------------------------------------
struct TbType {
    std::string name;             // "OMM" (the vtab name)
    uint8_t fid[4] = {0, 0, 0, 0};
    std::vector<uint8_t> config;  // serialized TypeConfig (registerType input)
    std::shared_ptr<TypeConfig> cfg;
    int base = -1;                // index of the type this one is a variant of (-1: none)
};
// Every <dir>/*.fsc (the files of a migrated store's t/<fid>/ directories,
// copied flat). Sorted by name.
bool loadTypeConfigs(const std::string& dir, std::vector<TbType>* out, std::string* err);
// The four test types (ps_fixtures: OMM, CAT, MPE, IQC).
std::vector<TbType> testTypes();
// `base` under file identifier `fid` and name `name`: the BFBS file_ident is
// rewritten in place (same length), rules and limits are kept.
bool variantType(const TbType& base, int baseIndex, const char fid[4], const std::string& name, TbType* out,
                 std::string* err);

// ---- corpus ----------------------------------------------------------------------------
struct Patch {
    enum Kind : uint8_t { kInt32, kInt64, kF64, kF32, kEpochStr, kEpochF64 };
    uint32_t off = 0;   // from the start of the bare FlatBuffer
    uint8_t kind = 0;
    uint8_t len = 0;    // kEpochStr: characters that parse (>= 19)
    bool identity = false;  // an identity-like integer (NORAD_CAT_ID, *_ID)
};

struct SeedRecord {
    uint32_t type = 0;               // index into the corpus' types
    std::vector<uint8_t> fb;         // bare FlatBuffer
    std::string peer, provider, source, batch, supersedeKey;
    std::vector<Patch> patches;
};

class Corpus {
public:
    // Loads a corpus file for `types` (records of other file identifiers are
    // skipped). Patch sites come from each type's BFBS.
    bool load(const std::string& path, const std::vector<TbType>& types, std::string* err);
    // Records of type t (indices into records()).
    const std::vector<uint32_t>& ofType(uint32_t t) const;
    const std::vector<SeedRecord>& records() const { return recs_; }
    size_t size() const { return recs_.size(); }
    // Frame ([u32 size][FlatBuffer]) of generated record k of type t: seed
    // record k % n, clone k / n (clone 0 is the seed itself). keepIdentity: on
    // a record without an epoch, identity fields keep clone (c - 1)'s values
    // (a supersede of that clone).
    // fidOverride: the frame's file identifier (type variants).
    const SeedRecord& seedFor(uint32_t t, uint64_t k) const;
    void frame(uint32_t t, uint64_t k, bool keepIdentity, const uint8_t* fidOverride,
               std::vector<uint8_t>* out) const;
    uint64_t bytesOfType(uint32_t t) const;

private:
    std::vector<SeedRecord> recs_;
    std::vector<std::vector<uint32_t>> byType_;
    std::vector<uint64_t> bytes_;
};

// Applies clone c's mutation to a bare FlatBuffer copy (see Corpus::frame).
void mutateClone(const std::vector<Patch>& patches, uint64_t c, bool keepIdentity, uint8_t* fb);
// Patch sites of a verified bare FlatBuffer of `cfg`'s root table.
std::vector<Patch> patchSites(const TypeConfig& cfg, const uint8_t* fb, size_t len);

// The smallest valid record of a type: its identity column, its epoch and
// nothing else. Record n with identity `id` (>= 1): the epoch advances a
// minute every `objects` records and carries n in its microseconds; a type
// without an epoch field carries n in one string field instead. Distinct n
// give distinct bytes whatever the identities.
class MinimalFrames {
public:
    bool init(const TbType& t, std::string* err);
    void frame(uint64_t n, uint64_t id, uint32_t objects, const uint8_t* fidOverride,
               std::vector<uint8_t>* out) const;
    bool hasEpoch() const { return voEpochStr_ || voEpochF64_; }
    const std::string& describe() const { return what_; }

private:
    uint8_t fid_[4] = {0, 0, 0, 0};
    uint16_t voId_ = 0, voEpochStr_ = 0, voEpochF64_ = 0, voEntity_ = 0, voAny_ = 0;
    uint8_t idWidth_ = 0;
    std::string what_;
};

// ---- host I/O ----------------------------------------------------------------------------
// Counts open handles (current, high water) and fails an open past `cap`
// (0: uncapped) the way the Go host's alloc_slot does (GENERIC).
class CountingIo final : public Io {
public:
    explicit CountingIo(Io* base, uint32_t cap = 0) : base_(base), cap_(cap) {}
    int32_t open(const char* path, int32_t pathLen, int32_t flags) override;
    int32_t read(int32_t h, void* dst, int32_t len, double off) override { return base_->read(h, dst, len, off); }
    int32_t write(int32_t h, const void* src, int32_t len, double off) override {
        return base_->write(h, src, len, off);
    }
    int32_t truncate(int32_t h, double size) override { return base_->truncate(h, size); }
    int32_t sync(int32_t h) override { return base_->sync(h); }
    double size(int32_t h) override { return base_->size(h); }
    int32_t close(int32_t h) override;
    uint64_t openNow() const { return cur_.load(); }
    uint64_t highWater() const { return hw_.load(); }
    uint64_t refused() const { return refused_.load(); }
    uint64_t opens() const { return opens_.load(); }
    uint32_t cap() const { return cap_; }
    void resetHighWater() { hw_.store(cur_.load()); }

private:
    Io* base_;
    uint32_t cap_;
    std::atomic<uint64_t> cur_{0}, hw_{0}, refused_{0}, opens_{0};
};

// ---- gauges ----------------------------------------------------------------------------------
uint64_t heapBytes();        // allocator bytes in use (0: unknown)
uint64_t rssBytes();         // resident set (0: unknown)
uint64_t wasmPages();        // linear memory pages (0 natively)
uint64_t openFds();          // process file descriptors (0: unknown)
uint64_t fsFreeBytes(const std::string& path);
uint64_t fsTotalBytes(const std::string& path);
double fsUsedPct(const std::string& path);
// Apparent and allocated bytes and files under dir (recursive).
struct DirUsage {
    uint64_t apparent = 0, allocated = 0, files = 0;
};
DirUsage dirUsage(const std::string& dir);
std::string machineLine();   // OS, arch, hardware threads, load average
uint64_t nowNs();

// Percentiles of the events a LockHist recorded between two snapshots.
struct HistSnap {
    std::vector<uint64_t> b;
    uint64_t count = 0;
    uint64_t maxNs = 0;
};
HistSnap snap(const LockHist& h);
double windowPercentileMs(const HistSnap& a, const HistSnap& b, double q);
uint64_t windowCount(const HistSnap& a, const HistSnap& b);

// ---- CSV -----------------------------------------------------------------------------------------
class Csv {
public:
    bool open(const std::string& path, const std::vector<std::string>& cols);
    void row(const std::vector<double>& v);
    void close();
    ~Csv() { close(); }

private:
    FILE* f_ = nullptr;
    size_t n_ = 0;
    std::mutex mu_;
};

// ---- B1 ------------------------------------------------------------------------------------------
// labeled_through of pid as a host reads it from durable files (SDN
// format2/heads.go, since 4e69a9db7): the type head's inline label table up to
// 128 partitions, and past that a fold of the type log from the head's label
// checkpoint through its (mSeg, mEnd). Returns false when the files do not
// say (unreadable head, a batch that does not parse, a missing segment).
bool headLabeledThrough(Io* io, const std::string& root, const uint8_t fid[4], uint32_t pid, uint64_t* out);
// The partition head's durable pseq_hi (0 when unwritten).
uint64_t headPseqHi(Io* io, const std::string& root, uint32_t pid);
// The newest valid slot of a head file (kind: kHeadPartition, kHeadType).
bool readDurableHead(Io* io, const PathBuf& path, uint16_t kind, std::vector<uint8_t>* out);

// ---- misc --------------------------------------------------------------------------------------------
// A peer id shaped like a libp2p one (53 characters) for producer i.
std::string peerId(uint32_t i);
// Deterministic 64-bit mixing (splitmix64).
inline uint64_t mix64(uint64_t x) {
    x += 0x9E3779B97F4A7C15ull;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
    return x ^ (x >> 31);
}
inline uint64_t splitmix(uint64_t* state) { return mix64((*state)++); }
// Zipf(s) sampler over [0, n) by inverse CDF (a table of n doubles).
class Zipf {
public:
    void init(uint32_t n, double s);
    uint32_t sample(uint64_t u64) const;

private:
    std::vector<double> cdf_;
};
// "2026-01-01T00:00:00" + n seconds, as an OMM EPOCH string.
std::string isoSecond(uint64_t n);
// q-quantile of a sample (sorted copy; nearest rank).
double pct(std::vector<double> v, double q);
uint64_t heapInUse();  // == heapBytes()

// ---- window percentiles over an engine LockHist ---------------------------------------------
// take() snapshots the histogram; the window is between the last two takes.
class HistWindow {
public:
    void take(const LockHist& h);
    uint64_t count() const { return windowCount(prev_, cur_); }
    uint64_t percentileNs(double q) const { return uint64_t(windowPercentileMs(prev_, cur_, q) * 1e6); }
    // Sum of the window's durations from bucket midpoints (within 25%).
    double sumNs() const;

private:
    HistSnap prev_, cur_;
};

// ---- unit gates ------------------------------------------------------------------------------------
// A gate is a slow test with its audit item and the status it has on the
// engine as built. Expected failures are declared here (fails = true) and
// cleared by the lane that fixes the blocker; scripts/tb/run-gates.sh runs
// each gate under a deadline and reports PASS, FAIL, HANG or TRAP against it.
struct Gate {
    const char* id;          // "B1"
    const char* audit;       // audit items it covers
    bool fails;              // expected to FAIL on the engine as built
    const char* why;         // the mechanism (audit file:line)
};
void gateBegin(const Gate& g);
void gateHang(const std::string& what);  // a bounded wait expired: counts as a failure
bool gateHangs();

}  // namespace tb
}  // namespace pst

// A unit gate: registered slow (never in the default suite or the wasm
// suite), announced with its expected status.
#define TB_GATE(name, gate)                                                  \
    static void name##_body();                                               \
    static void name() {                                                     \
        pst::tb::gateBegin(gate);                                            \
        name##_body();                                                       \
    }                                                                        \
    static pst::Reg reg_##name(#name, name, true);                           \
    static void name##_body()
#define TB_HANG(msg) pst::tb::gateHang(msg)

#endif
