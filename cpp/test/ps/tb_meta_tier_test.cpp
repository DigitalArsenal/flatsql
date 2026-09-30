// Terabyte harness: the metadata-only tier (audit §4 "Generating TB quickly"
// 4, and M11's "synthetic 1e9-record manifest" gate).
//
//   flatsql_ps_test --test=tb_meta_tier --tb-dir=<work dir> --tb-meta-tb=0.1,1,10 [knobs]
//
// A store of 10 TB (or any --tb-meta-tb list of sizes at rest) on almost no
// real disk: the real engine registers the types and partitions, then the
// tier writes, for every partition, its sealed and merged segments (sparse
// d/r/a files of their full logical length), one L1 run per segment and the
// manifest naming them, and a durable head; for every type, its catalog runs,
// catalog manifest, sealed arrivals segments (sparse), the arrivals fence
// index, the label checkpoint and the head. Run files are real headers, TOCs
// and footers with their fence and bloom regions left as holes (zeros, CRC
// computed over them): L1Run::load allocates exactly what it would for the
// real run, so memory and handle numbers are the engine's own. The kinds, the
// entries per record and per block, and the bloom bytes per entry of both run
// kinds come from a TEMPLATE store the real engine writes first (real host-02
// records with --tb-corpus, otherwise minimal ones), scaled by records.
//
// Then, on the real engine: open (time, bytes read, host opens, heap), each
// type's catalog warmed by one record (the catalog accelerators of audit B3),
// the dominant partition warmed and written (its runs, audit B5; commit and
// maintenance at its S, audit B4), handles after warm and after cool (B2), and
// a reopen. A type or partition whose accelerators the synthesized TOCs say
// exceed --tb-meta-warm-max-gb is not warmed (its estimate is reported).
//
// It cannot serve reads (every entry block is a hole), and no write path may
// read an old block: new records only, no deletes, retags or supersedes.
#include <algorithm>
#include <cmath>
#include <cstring>
#include <thread>

#include "flatsql/ps/compaction.h"
#include "flatsql/ps/index.h"
#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"
#include "ps/tb_corpus.h"

#if !defined(__wasm__)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>

using namespace pst;
using pst::tb::CountingIo;

namespace {

constexpr uint64_t kSec = 1000000000ull;
constexpr double kTB = 1e12;

struct KindT {
    uint16_t kind = 0;
    uint8_t keyType = 0, vlen = 0;
    double perRec = 0;        // entries per record
    double perBlock = 1;      // entries per 4 KiB block
    double bloomPerEntry = 0; // bloom bytes per entry (0: no bloom)
};

struct RunT {
    L1Header hdr{};
    std::vector<KindT> kinds;
    bool ok = false;
};

double gFixedHeap = 0;  // engine heap of an empty store (pool, arenas, threads)

struct TypeT {
    tb::TbType type;
    double share = 0, frameBytes = 0, restBytes = 0;  // byte share at rest, bytes per frame, per record at rest
    double attrPerRec = 200;
    RunT part, cat;
    tb::MinimalFrames mf;
};

bool readRunToc(const std::string& path, L1Header* h, std::vector<L1TocEntry>* toc) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    struct stat st;
    bool ok = fstat(fd, &st) == 0 && st.st_size >= int64_t(sizeof(L1Header) + sizeof(L1Footer));
    L1Footer ft{};
    ok = ok && pread(fd, h, sizeof(*h), 0) == ssize_t(sizeof(*h)) &&
         pread(fd, &ft, sizeof(ft), st.st_size - int64_t(sizeof(ft))) == ssize_t(sizeof(ft)) &&
         ft.magic == kMagicL1Footer && ft.tocLen % sizeof(L1TocEntry) == 0;
    if (ok) {
        toc->resize(ft.tocLen / sizeof(L1TocEntry));
        ok = pread(fd, toc->data(), ft.tocLen, off_t(ft.tocOff)) == ssize_t(ft.tocLen);
    }
    ::close(fd);
    return ok;
}

// Accumulates run TOCs into per-record ratios. `records`: what the runs cover.
struct RunAcc {
    L1Header hdr{};
    bool have = false;
    std::map<uint16_t, L1TocEntry> sum;
    void add(const std::string& path) {
        L1Header h;
        std::vector<L1TocEntry> toc;
        if (!readRunToc(path, &h, &toc)) return;
        if (!have) hdr = h;
        have = true;
        for (const auto& t : toc) {
            L1TocEntry& s = sum[t.kind];
            s.kind = t.kind;
            s.keyType = t.keyType;
            s.vlen = t.vlen;
            s.nBlocks += t.nBlocks;
            s.nEntries += t.nEntries;
            s.bloomBytes += t.bloomBytes;
        }
    }
    RunT ratios(double records) const {
        RunT r;
        r.hdr = hdr;
        r.ok = have && records > 0;
        for (const auto& kv : sum) {
            const L1TocEntry& s = kv.second;
            if (!s.nEntries) continue;
            KindT k;
            k.kind = s.kind;
            k.keyType = s.keyType;
            k.vlen = s.vlen;
            k.perRec = double(s.nEntries) / records;
            k.perBlock = s.nBlocks ? double(s.nEntries) / double(s.nBlocks) : 1;
            k.bloomPerEntry = double(s.bloomBytes) / double(s.nEntries);
            r.kinds.push_back(k);
        }
        return r;
    }
};

// Bytes L1Run::load keeps resident for a run of `records` (fences + blooms).
double runResident(const RunT& r, double records) {
    double b = 0;
    for (const auto& k : r.kinds) {
        const double e = std::max(0.0, std::round(records * k.perRec));
        if (e <= 0) continue;
        b += std::ceil(e / k.perBlock) * sizeof(L1Fence) + (k.bloomPerEntry > 0 ? std::ceil(e * k.bloomPerEntry / 8) * 8 : 0);
    }
    return b;
}

// ---- file writers --------------------------------------------------------------------------
bool mkdirs(const std::string& d) {
    std::error_code ec;
    std::filesystem::create_directories(d, ec);
    return !ec;
}

// A file of `len` logical bytes holding `chunks` (offset, bytes); the rest a hole.
bool writeSparse(const std::string& path, uint64_t len, const std::vector<std::pair<uint64_t, std::vector<uint8_t>>>& chunks) {
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;
    bool ok = ftruncate(fd, off_t(len)) == 0;
    for (const auto& c : chunks)
        ok = ok && pwrite(fd, c.second.data(), c.second.size(), off_t(c.first)) == ssize_t(c.second.size());
    ::close(fd);
    return ok;
}

uint32_t crcZeros(uint32_t crc, uint64_t n) {
    static const std::vector<uint8_t> z(1u << 20, 0);
    while (n) {
        const size_t k = size_t(std::min<uint64_t>(n, z.size()));
        crc = crc32c(crc, z.data(), k);
        n -= k;
    }
    return crc;
}

// A run of `records` in the template's shape: header, hole (entry blocks,
// fences, blooms), TOC, footer. Returns the file length (0 on failure) and
// the entries.
uint64_t writeRun(const std::string& path, const RunT& t, uint32_t seg, uint32_t gen, uint64_t first, uint64_t last,
                  double records, uint64_t* entriesOut, double* residentOut) {
    std::vector<L1TocEntry> toc;
    uint64_t blocks = 0, entries = 0;
    struct K {
        L1TocEntry e;
    };
    for (const auto& k : t.kinds) {
        const uint64_t e = uint64_t(std::max(0.0, std::round(records * k.perRec)));
        if (!e) continue;
        L1TocEntry te{};
        te.kind = k.kind;
        te.keyType = k.keyType;
        te.vlen = k.vlen;
        te.nEntries = e;
        te.nBlocks = uint32_t(std::ceil(double(e) / k.perBlock));
        te.bloomBytes = k.bloomPerEntry > 0 ? uint32_t(std::ceil(double(e) * k.bloomPerEntry / 8) * 8) : 0;
        blocks += te.nBlocks;
        entries += e;
        toc.push_back(te);
    }
    const uint64_t metaOff = sizeof(L1Header) + blocks * kL1BlockBytes;
    uint64_t off = metaOff;
    double resident = 0;
    for (auto& te : toc) {
        te.fenceOff = off;
        off += uint64_t(te.nBlocks) * sizeof(L1Fence);
        te.bloomOff = off;
        off += te.bloomBytes;
        off = (off + 7) & ~uint64_t(7);
        resident += double(te.nBlocks) * sizeof(L1Fence) + te.bloomBytes;
    }
    const uint64_t tocOff = off;
    const uint32_t tocLen = uint32_t(toc.size() * sizeof(L1TocEntry));
    std::vector<uint8_t> tail(tocLen + sizeof(L1Footer));
    std::memcpy(tail.data(), toc.data(), tocLen);
    L1Footer ft{};
    ft.tocOff = tocOff;
    ft.tocLen = tocLen;
    ft.tocCrc = crc32c(crcZeros(0, tocOff - metaOff), tail.data(), tocLen);
    ft.metaOff = metaOff;
    ft.magic = kMagicL1Footer;
    ft.crc = crc32c(&ft, 28);
    std::memcpy(tail.data() + tocLen, &ft, sizeof(ft));
    L1Header h = t.hdr;
    h.magic = kMagicL1;
    h.nKinds = uint16_t(toc.size());
    h.seg = seg;
    h.gen = gen;
    h.firstPseq = first;
    h.lastPseq = last;
    h.nEntries = entries;
    std::vector<uint8_t> hb(sizeof(h));
    std::memcpy(hb.data(), &h, sizeof(h));
    const uint64_t len = tocOff + tail.size();
    if (!writeSparse(path, len, {{0, hb}, {tocOff, tail}})) return 0;
    *entriesOut = entries;
    *residentOut = resident;
    return len;
}

std::vector<uint8_t> headSlot(const void* fixed, size_t fixedLen, const std::vector<uint8_t>& after, uint16_t kind,
                              uint32_t id) {
    std::vector<uint8_t> slot(2 * kHeadSlotBytes, 0);
    uint8_t* s = slot.data() + kHeadSlotBytes;  // gen 1 lives in slot 1
    std::memcpy(s, fixed, fixedLen);
    HeadPrefix hp{};
    hp.magic = kMagicHead;
    hp.format = kFormat;
    hp.kind = kind;
    hp.gen = 1;
    hp.ownerEpoch = 1;
    hp.id = id;
    hp.flags = kHeadDurableCkpt;
    std::memcpy(s, &hp, sizeof(hp));
    std::memcpy(s + fixedLen, after.data(), after.size());
    sealHeadSlot(s, uint32_t(fixedLen + after.size() + 4));
    return slot;
}

bool writeFile(const std::string& path, const std::vector<uint8_t>& b) { return writeSparse(path, b.size(), {{0, b}}); }

std::string P(const PathBuf& p) { return std::string(p.c_str(), p.len); }

// ---- the tier ---------------------------------------------------------------------------------
struct Knobs {
    std::string dir, csv, typesDir, corpus, sizes, mix;
    uint32_t partitions, writers, templateRecords, domRecords;
    double dominantPct, zipf, warmMaxGB, minFreeGB;
    uint64_t segBytes, foldMax, arrivalsSeg;
    bool keep;
};

Knobs readKnobs() {
    Knobs k;
    k.dir = argStr("tb-dir", "");
    k.csv = argStr("tb-csv", "");
    k.typesDir = argStr("tb-types-dir", "");
    k.corpus = argStr("tb-corpus", "");
    k.sizes = argStr("tb-meta-tb", "0.1,1,10");
    // name=byte share:frame bytes:bytes at rest per record (audit §4 workload and record counts)
    k.mix = argStr("tb-meta-mix", "OMM=0.6:616:1520,CAT=0.2:2048:3000,IQC=0.2:3072:4000");
    k.partitions = uint32_t(argInt("tb-partitions", 10000));
    k.writers = uint32_t(argInt("tb-writers", 8));
    k.templateRecords = uint32_t(argInt("tb-template-records", 200000));
    k.domRecords = uint32_t(argInt("tb-meta-dom-records", 65536));
    k.dominantPct = double(argInt("tb-dominant-pct", 25));
    k.zipf = double(argInt("tb-zipf-x100", 110)) / 100.0;
    k.warmMaxGB = double(argInt("tb-meta-warm-max-gb", 8));
    k.minFreeGB = double(argInt("tb-min-free-gb", 20));
    k.segBytes = uint64_t(argInt("tb-meta-seg-mib", 64)) << 20;
    k.foldMax = uint64_t(argInt("tb-fold-max", 2000000));
    k.arrivalsSeg = uint64_t(argInt("tb-arrivals-seg", 64 << 20));
    k.keep = argInt("tb-keep", 0) != 0;
    return k;
}

bool setupTypes(const Knobs& k, std::vector<TypeT>* out, std::string* err) {
    std::vector<tb::TbType> all;
    if (!k.typesDir.empty()) {
        if (!tb::loadTypeConfigs(k.typesDir, &all, err)) return false;
    } else {
        all = tb::testTypes();
    }
    size_t start = 0;
    while (start < k.mix.size()) {
        const size_t end = std::min(k.mix.find(',', start), k.mix.size());
        const std::string kv = k.mix.substr(start, end - start);
        start = end + 1;
        const size_t eq = kv.find('=');
        if (eq == std::string::npos) continue;
        TypeT t;
        const std::string name = kv.substr(0, eq);
        if (std::sscanf(kv.c_str() + eq + 1, "%lf:%lf:%lf", &t.share, &t.frameBytes, &t.restBytes) != 3) continue;
        bool found = false;
        for (const auto& ty : all)
            if (ty.name == name) {
                t.type = ty;
                found = true;
            }
        if (!found) {
            *err = "no type " + name;
            return false;
        }
        if (!t.mf.init(t.type, err)) return false;
        out->push_back(std::move(t));
    }
    double sum = 0;
    for (const auto& t : *out) sum += t.share;
    for (auto& t : *out) t.share /= sum;
    return !out->empty();
}

EngineConfig baseConfig(const std::string& root, Io* io, uint32_t writers) {
    EngineConfig c;
    c.root = root;
    c.io = io;
    c.writers = writers;
    c.lockStats = true;
    return c;
}

// The template: the real engine writes records of each type into one
// partition each; its runs give the per-record shape of partition and
// catalog runs.
bool buildTemplate(const Knobs& k, std::vector<TypeT>* types, std::string* err) {
    const std::string root = k.dir + "/template";
    std::filesystem::remove_all(root);
    mkdirs(root);
    std::vector<tb::TbType> tt;
    for (const auto& t : *types) tt.push_back(t.type);
    tb::Corpus corpus;
    const bool real = !k.corpus.empty() && corpus.load(k.corpus, tt, err);
    if (!k.corpus.empty() && !real) return false;
    // The engine's fixed memory (slab pool, arenas, threads): an empty
    // store opened with the tier's writer count.
    {
        const std::string empty = k.dir + "/empty";
        std::filesystem::remove_all(empty);
        std::unique_ptr<Engine> e0;
        EngineConfig c0 = baseConfig(empty, importIo(), k.writers);
        const uint64_t h0 = tb::heapBytes();
        if (Engine::open(c0, &e0, err) < 0 || e0->start() < 0) return false;
        gFixedHeap = double(tb::heapBytes()) - double(h0);
        e0->stop();
        e0.reset();
        std::filesystem::remove_all(empty);
        std::printf("  TEMPLATE engine fixed heap (empty store, %u writers): %.1f MB\n", k.writers, gFixedHeap / 1e6);
    }
    std::unique_ptr<Engine> e;
    EngineConfig c = baseConfig(root, importIo(), 4);
    if (Engine::open(c, &e, err) < 0 || e->start() < 0) return false;
    std::vector<uint32_t> pids;
    for (size_t i = 0; i < types->size(); i++) {
        const tb::TbType& ty = (*types)[i].type;
        if (e->registerType(ty.config, err) < 0) return false;
        const std::string peer = tb::peerId(900000 + uint32_t(i));
        uint32_t pid = 0;
        if (e->registerPartition(reinterpret_cast<const uint8_t*>(peer.data()), peer.size(), ty.fid, &pid) < 0) {
            *err = "template registration failed";
            return false;
        }
        pids.push_back(pid);
    }
    std::vector<std::thread> ts;
    std::atomic<uint32_t> bad{0};
    for (size_t i = 0; i < types->size(); i++)
        ts.emplace_back([&, i] {
            const TypeT& t = (*types)[i];
            Producer prod(e.get(), pids[i]);
            std::vector<uint8_t> frame;
            uint8_t cid[kCidLen];
            const auto attr = buildRecordAttr(tb::peerId(900000 + uint32_t(i)), "provider-0", "template-source",
                                              "template-batch");
            uint64_t last = 0;
            for (uint32_t n = 0; n < k.templateRecords; n++) {
                if (real) corpus.frame(uint32_t(i), n, false, nullptr, &frame);
                else t.mf.frame(n, t.mf.hasEpoch() ? n % 60000 + 1 : n + 1, 60000, nullptr, &frame);
                frameCid(frame, cid);
                if (prod.enqueue(kEntRecord, kEntCidPresent, int64_t(wallMs()), cid, attr.data(), uint32_t(attr.size()),
                                 frame.data(), uint32_t(frame.size()), &last, true) < 0) {
                    bad.fetch_add(1);
                    return;
                }
            }
            if (prod.waitAcked(last, 600 * kSec) != 0) bad.fetch_add(1);
        });
    for (auto& t : ts) t.join();
    if (bad.load() || !waitLabeledEngine(e.get(), pids, 600 * kSec)) {
        *err = "template writes did not complete";
        return false;
    }
    sleepNs(3 * kSec);  // merges and the type merges in flight
    e->stop();
    e.reset();
    // Partition runs: the manifest's segments and runs; records = merged rows.
    for (size_t i = 0; i < types->size(); i++) {
        TypeT& t = (*types)[i];
        PathBuf hp;
        pathPartition(&hp, root.c_str(), pids[i], "h.fsh");
        std::vector<uint8_t> head;
        if (!tb::readDurableHead(importIo(), hp, kHeadPartition, &head)) {
            *err = "template partition head unreadable";
            return false;
        }
        PartitionHeadFixed ph;
        std::memcpy(&ph, head.data(), sizeof(ph));
        PathBuf mp;
        pathPartitionManifest(&mp, root.c_str(), pids[i], ph.manifestGen);
        std::vector<uint8_t> man;
        {
            FILE* f = std::fopen(P(mp).c_str(), "rb");
            if (!f) {
                *err = "template has no merged run (manifest " + P(mp) + ")";
                return false;
            }
            std::fseek(f, 0, SEEK_END);
            man.resize(size_t(std::ftell(f)));
            std::fseek(f, 0, SEEK_SET);
            if (std::fread(man.data(), 1, man.size(), f) != man.size()) man.clear();
            std::fclose(f);
        }
        ManifestDesc md;
        if (!decodeManifest(man.data(), man.size(), &md)) {
            *err = "template manifest invalid";
            return false;
        }
        RunAcc acc;
        double rows = 0, aBytes = 0;
        for (const auto& s : md.segs) {
            rows += double(s.mergedEnd - s.firstPseq);
            aBytes += double(s.aLen);
            for (const auto& r : s.runs) {
                PathBuf rp;
                pathPartitionRun(&rp, root.c_str(), pids[i], s.seg, r.gen);
                acc.add(P(rp));
            }
        }
        t.part = acc.ratios(rows);
        t.attrPerRec = rows > 0 ? aBytes / rows : 200;
        // Catalog runs: the type manifest's runs; records = TypeCid entries.
        PathBuf th;
        pathType(&th, root.c_str(), t.type.fid, "h.fsh");
        if (!tb::readDurableHead(importIo(), th, kHeadType, &head)) {
            *err = "template type head unreadable";
            return false;
        }
        TypeHeadFixed tf;
        std::memcpy(&tf, head.data(), sizeof(tf));
        RunAcc cat;
        if (tf.manifestGen) {
            PathBuf tm;
            pathTypeManifest(&tm, root.c_str(), t.type.fid, tf.manifestGen);
            std::vector<uint8_t> tmb;
            FILE* f = std::fopen(P(tm).c_str(), "rb");
            if (f) {
                std::fseek(f, 0, SEEK_END);
                tmb.resize(size_t(std::ftell(f)));
                std::fseek(f, 0, SEEK_SET);
                if (std::fread(tmb.data(), 1, tmb.size(), f) != tmb.size()) tmb.clear();
                std::fclose(f);
            }
            if (tmb.size() >= 16) {
                const uint32_t n = getU32(tmb.data() + 4);
                for (uint32_t r = 0; r < n && 16 + size_t(r) * 16 + 16 <= tmb.size(); r++) {
                    PathBuf rp;
                    pathTypeRun(&rp, root.c_str(), t.type.fid, getU32(tmb.data() + 16 + size_t(r) * 16));
                    cat.add(P(rp));
                }
            }
        }
        double catRecs = 0;
        for (const auto& kv : cat.sum)
            if (kv.first == kIxTypeCid) catRecs = double(kv.second.nEntries);
        t.cat = cat.ratios(catRecs);
        std::printf("  TEMPLATE %s: %s records, %.0f merged rows, attr %.0f B/record; partition run kinds:",
                    t.type.name.c_str(), real ? "real" : "minimal", rows, t.attrPerRec);
        for (const auto& kk : t.part.kinds)
            std::printf(" %x(%.2f/rec, %.0f/blk, bloom %.2f B/e)", kk.kind, kk.perRec, kk.perBlock, kk.bloomPerEntry);
        std::printf("; catalog run kinds (per TypeCid entry):");
        for (const auto& kk : t.cat.kinds)
            std::printf(" %x(%.2f, %.0f/blk, bloom %.2f B/e)", kk.kind, kk.perRec, kk.perBlock, kk.bloomPerEntry);
        std::printf("\n");
        std::fflush(stdout);
        if (!t.part.ok || !t.cat.ok) {
            *err = "template for " + t.type.name + " has no merged partition or catalog run";
            return false;
        }
    }
    std::filesystem::remove_all(root);
    return true;
}

struct PartPlan {
    uint32_t pid = 0;
    size_t type = 0;
    bool dominant = false;
    uint64_t records = 0;
    uint32_t segs = 0;
    std::string peer;
};

struct Result {
    std::map<std::string, double> v;
    void set(const std::string& k, double x) { v[k] = x; }
};

void runTier(const Knobs& k, std::vector<TypeT>& types, double tbAtRest, tb::Csv* csv,
             const std::vector<std::string>& cols) {
    Result R;
    R.set("tier_tb", tbAtRest);
    const std::string root = k.dir + "/meta-" + std::to_string(int(tbAtRest * 1000)) + "gb";
    std::filesystem::remove_all(root);
    mkdirs(root);
    std::string err;
    const double freeGB = double(tb::fsFreeBytes(k.dir)) / 1e9;
    if (freeGB < k.minFreeGB + 5) {
        std::printf("  META %.3f TB: skipped, %.1f GB available (floor %.0f GB)\n", tbAtRest, freeGB, k.minFreeGB);
        return;
    }
    // Plan: records per type from the byte shares at rest; partitions per
    // type from the same shares; the dominant OMM partition takes
    // dominantPct of all bytes at rest; the rest Zipf inside each type.
    const uint64_t segBytes = k.segBytes;
    std::vector<PartPlan> plan;
    std::vector<uint64_t> typeRecs(types.size());
    for (size_t t = 0; t < types.size(); t++) {
        const TypeT& ty = types[t];
        const uint64_t n = uint64_t(ty.share * tbAtRest * kTB / ty.restBytes);
        typeRecs[t] = n;
        uint32_t np = std::max<uint32_t>(1, uint32_t(std::round(ty.share * k.partitions)));
        uint64_t domRecs = 0;
        if (t == 0 && k.dominantPct > 0) {
            domRecs = std::min<uint64_t>(n, uint64_t(k.dominantPct / 100.0 * tbAtRest * kTB / ty.restBytes));
            PartPlan d;
            d.type = t;
            d.dominant = true;
            d.records = domRecs;
            plan.push_back(d);
            np = np > 1 ? np - 1 : 1;
        }
        const uint64_t rest = n - domRecs;
        double h = 0;
        for (uint32_t i = 0; i < np; i++) h += 1.0 / std::pow(double(i + 1), k.zipf);
        for (uint32_t i = 0; i < np; i++) {
            PartPlan p;
            p.type = t;
            p.records = std::max<uint64_t>(1, uint64_t(double(rest) * (1.0 / std::pow(double(i + 1), k.zipf)) / h));
            plan.push_back(p);
        }
    }
    uint64_t segsTotal = 0, sMax = 0, recsTotal = 0;
    double framesTotal = 0;
    for (size_t j = 0; j < plan.size(); j++) {
        PartPlan& p = plan[j];
        const TypeT& ty = types[p.type];
        const uint64_t perSeg = std::max<uint64_t>(1, uint64_t(double(segBytes) / ty.frameBytes));
        uint64_t s = (p.records + perSeg - 1) / perSeg;
        if (s > 65535) {
            std::printf("  META M3: partition %zu needs %llu segments; a manifest names at most 65,535 (u16 nSegs)\n", j,
                        (unsigned long long)s);
            s = 65535;
            p.records = s * perSeg;
        }
        p.segs = uint32_t(s);
        segsTotal += s;
        sMax = std::max<uint64_t>(sMax, s);
        recsTotal += p.records;
        framesTotal += double(p.records) * ty.frameBytes;
        p.peer = tb::peerId(uint32_t(j) * 2654435761u + 11);
    }
    R.set("partitions", double(plan.size()));
    R.set("records_total", double(recsTotal));
    R.set("records_omm", double(typeRecs[0]));
    R.set("frame_bytes_total", framesTotal);
    R.set("segments_total", double(segsTotal));
    R.set("s_max", double(sMax));
    std::printf("  META %.3f TB at rest: %zu partitions, %llu records, %.2f TB of frames, %llu segments (S max %llu)\n",
                tbAtRest, plan.size(), (unsigned long long)recsTotal, framesTotal / kTB, (unsigned long long)segsTotal,
                (unsigned long long)sMax);
    std::fflush(stdout);
    // 1. The real engine registers types and partitions.
    const uint64_t b0 = monoNs();
    {
        std::unique_ptr<Engine> e;
        EngineConfig c = baseConfig(root, importIo(), k.writers);
        if (Engine::open(c, &e, &err) < 0 || e->start() < 0) {
            std::fprintf(stderr, "  open for registration: %s\n", err.c_str());
            gFailures++;
            return;
        }
        for (const auto& t : types)
            if (e->registerType(t.type.config, &err) < 0) {
                std::fprintf(stderr, "  registerType: %s\n", err.c_str());
                gFailures++;
                return;
            }
        std::atomic<size_t> next{0};
        std::atomic<uint32_t> bad{0};
        std::vector<std::thread> ts;
        for (int w = 0; w < 16; w++)
            ts.emplace_back([&] {
                for (size_t j; (j = next.fetch_add(1)) < plan.size();) {
                    uint32_t pid = 0;
                    if (e->registerPartition(reinterpret_cast<const uint8_t*>(plan[j].peer.data()), plan[j].peer.size(),
                                             types[plan[j].type].type.fid, &pid) < 0)
                        bad.fetch_add(1);
                    plan[j].pid = pid;
                }
            });
        for (auto& t : ts) t.join();
        e->stop();
        if (bad.load()) {
            std::fprintf(stderr, "  %u registrations failed\n", bad.load());
            gFailures++;
            return;
        }
    }
    R.set("register_s", double(monoNs() - b0) / 1e9);
    // 2. Partitions: segments, runs, manifest, head.
    const int64_t epoch0 = 1790000000000ll;
    std::atomic<size_t> next{0};
    std::atomic<uint64_t> partRuns{0}, files{0};
    std::atomic<uint32_t> bad{0};
    std::vector<double> domResident(1, 0);
    std::vector<std::thread> ts;
    for (int w = 0; w < 8; w++)
        ts.emplace_back([&] {
            for (size_t j; (j = next.fetch_add(1)) < plan.size();) {
                const PartPlan& p = plan[j];
                const TypeT& ty = types[p.type];
                const uint64_t perSeg = std::max<uint64_t>(1, uint64_t(double(segBytes) / ty.frameBytes));
                PathBuf dp;
                pathPartitionDir(&dp, root.c_str(), p.pid);
                mkdirs(P(dp));
                ManifestDesc md;
                md.gen = 2;
                md.pid = p.pid;
                uint64_t pseq = 1, diskBytes = 0;
                double resident = 0;
                for (uint32_t s = 0; s < p.segs; s++) {
                    const uint64_t n = std::min<uint64_t>(perSeg, p.records - (pseq - 1));
                    ManifestSegDesc sd;
                    sd.seg = s;
                    sd.lastSeg = s;
                    sd.sealed = true;
                    sd.firstPseq = pseq;
                    sd.endPseq = pseq + n;
                    sd.mergedEnd = pseq + n;
                    sd.dLen = uint64_t(double(n) * (ty.frameBytes + 4));
                    sd.rLen = n * sizeof(RecRow);
                    sd.aLen = uint64_t(double(n) * ty.attrPerRec);
                    sd.minEpoch = sd.minArrival = epoch0 + int64_t(s) * 3600000;
                    sd.maxEpoch = sd.maxArrival = epoch0 + int64_t(s + 1) * 3600000 - 1;
                    PathBuf fp;
                    bool ok = true;
                    pathPartitionSeg(&fp, root.c_str(), p.pid, 'd', s, "fsd");
                    ok = ok && writeSparse(P(fp), sd.dLen, {});
                    pathPartitionSeg(&fp, root.c_str(), p.pid, 'r', s, "fsr");
                    ok = ok && writeSparse(P(fp), sd.rLen, {});
                    pathPartitionSeg(&fp, root.c_str(), p.pid, 'a', s, "fsa");
                    ok = ok && writeSparse(P(fp), sd.aLen, {});
                    pathPartitionRun(&fp, root.c_str(), p.pid, s, 1);
                    uint64_t ents = 0;
                    double res = 0;
                    const uint64_t xl = writeRun(P(fp), ty.part, s, 1, pseq, pseq + n - 1, double(n), &ents, &res);
                    ok = ok && xl > 0;
                    if (!ok) {
                        bad.fetch_add(1);
                        break;
                    }
                    ManifestRun mr{};
                    mr.gen = 1;
                    mr.nEntries = ents;
                    mr.fileLen = xl;
                    sd.runs.push_back(mr);
                    md.segs.push_back(std::move(sd));
                    diskBytes += md.segs.back().dLen + md.segs.back().rLen + md.segs.back().aLen + xl;
                    resident += res;
                    pseq += n;
                }
                const std::vector<uint8_t> man = encodeManifest(md);
                PathBuf mp;
                pathPartitionManifest(&mp, root.c_str(), p.pid, 2);
                if (!writeFile(P(mp), man)) bad.fetch_add(1);
                diskBytes += man.size();
                PartitionHeadFixed h{};
                h.commitSeq = 1;
                h.pseqHi = p.records;
                h.mSeg = p.segs;
                h.incarnation = 1;
                h.dSeg = p.segs;
                h.nextSeg = p.segs + 1;
                h.mergedThrough = p.records;
                h.manifestGen = 2;
                h.firstLiveMSeg = p.segs;
                h.nextGen = 3;
                h.schemaFp = ty.type.cfg->fingerprint();
                h.counters.totalCount = h.counters.liveCount = p.records;
                h.counters.totalBytes = h.counters.liveBytes = uint64_t(double(p.records) * ty.frameBytes);
                h.counters.diskBytes = diskBytes;
                h.counters.minEpoch = epoch0;
                h.counters.maxEpoch = h.counters.latestArrival = epoch0 + int64_t(p.segs) * 3600000 - 1;
                h.nextLaneId = 1;
                h.segFirstPseq = p.records + 1;
                PathBuf hp;
                pathPartition(&hp, root.c_str(), p.pid, "h.fsh");
                if (!writeFile(P(hp), headSlot(&h, sizeof(h), {}, kHeadPartition, p.pid))) bad.fetch_add(1);
                partRuns.fetch_add(p.segs);
                files.fetch_add(4 * uint64_t(p.segs) + 2);
                if (p.dominant) domResident[0] = resident;
            }
        });
    for (auto& t : ts) t.join();
    // 3. Types: catalog runs, catalog manifest, arrivals, fence, labels, head.
    uint64_t gseqBase = 0, catRunsTotal = 0, catRunsOmm = 0, arrSegs = 0;
    std::vector<double> catResident(types.size(), 0);
    for (size_t t = 0; t < types.size() && !bad.load(); t++) {
        const TypeT& ty = types[t];
        const uint64_t N = [&] {
            uint64_t n = 0;
            for (const auto& p : plan)
                if (p.type == t) n += p.records;
            return n;
        }();
        PathBuf td;
        pathTypeDir(&td, root.c_str(), ty.type.fid);
        mkdirs(P(td));
        // Catalog runs of mergeFoldMaxEntries entries (the fold cap: audit B3).
        double entPerRec = 0;
        for (const auto& kk : ty.cat.kinds) entPerRec += kk.perRec;
        const double recsPerRun = double(k.foldMax) / std::max(entPerRec, 1e-9);
        const uint64_t nRuns = std::max<uint64_t>(1, uint64_t(std::ceil(double(N) / recsPerRun)));
        std::vector<std::pair<uint32_t, uint64_t>> runs;
        std::atomic<uint64_t> rnext{0};
        std::vector<std::pair<uint32_t, uint64_t>> runLens(nRuns);
        std::vector<double> runRes(nRuns, 0);
        std::vector<std::thread> rts;
        for (int w = 0; w < 8; w++)
            rts.emplace_back([&] {
                for (uint64_t r; (r = rnext.fetch_add(1)) < nRuns;) {
                    const double recs = std::min(recsPerRun, double(N) - double(r) * recsPerRun);
                    PathBuf rp;
                    pathTypeRun(&rp, root.c_str(), ty.type.fid, uint32_t(r + 1));
                    uint64_t ents = 0;
                    double res = 0;
                    const uint64_t len = writeRun(P(rp), ty.cat, 0, uint32_t(r + 1), 1, 1, recs, &ents, &res);
                    if (!len) bad.fetch_add(1);
                    runLens[r] = {uint32_t(r + 1), len};
                    runRes[r] = res;
                }
            });
        for (auto& th : rts) th.join();
        for (double x : runRes) catResident[t] += x;
        const uint32_t manGen = uint32_t(nRuns + 1);
        std::vector<uint8_t> man(16);
        putU32(man.data(), kMagicManifest);
        putU32(man.data() + 4, uint32_t(nRuns));
        putU32(man.data() + 8, manGen);
        for (const auto& rl : runLens) {
            const size_t at = man.size();
            man.resize(at + 16, 0);
            putU32(man.data() + at, rl.first);
            putU64(man.data() + at + 8, rl.second);
        }
        const size_t body = man.size();
        man.resize(body + 8, 0);
        putU32(man.data() + body, crc32c(man.data(), body));
        PathBuf mp;
        pathTypeManifest(&mp, root.c_str(), ty.type.fid, manGen);
        if (!writeFile(P(mp), man)) bad.fetch_add(1);
        catRunsTotal += nRuns;
        if (t == 0) catRunsOmm = nRuns;
        // Arrivals: sealed segments (holes) and the fence index.
        const uint64_t perG = k.arrivalsSeg / kArrivalBytes;
        const uint32_t gSeg = uint32_t(N / perG);
        std::vector<uint8_t> fence(size_t(gSeg) * sizeof(ArrivalFence));
        for (uint32_t g = 0; g < gSeg; g++) {
            PathBuf gp;
            pathTypeSeg(&gp, root.c_str(), ty.type.fid, 'g', g, "fsg");
            if (!writeSparse(P(gp), perG * kArrivalBytes, {})) bad.fetch_add(1);
            ArrivalFence f{};
            f.seg = g;
            f.firstGseq = gseqBase + uint64_t(g) * perG + 1;
            f.lastGseq = gseqBase + uint64_t(g + 1) * perG;
            f.count = perG;
            f.crc = crc32c(&f, offsetof(ArrivalFence, crc));
            std::memcpy(fence.data() + size_t(g) * sizeof(f), &f, sizeof(f));
        }
        arrSegs += gSeg;
        const uint64_t gLen = (N - uint64_t(gSeg) * perG) * kArrivalBytes;
        PathBuf gp;
        pathTypeSeg(&gp, root.c_str(), ty.type.fid, 'g', gSeg, "fsg");
        if (!writeSparse(P(gp), gLen, {})) bad.fetch_add(1);
        PathBuf fp;
        pathType(&fp, root.c_str(), ty.type.fid, kArrivalFenceName);
        if (!writeFile(P(fp), fence)) bad.fetch_add(1);
        // Labels: every partition labeled through its pseq_hi.
        std::vector<LabelEntry> labels;
        for (const auto& p : plan)
            if (p.type == t) {
                LabelEntry le{};
                le.pid = p.pid;
                le.labeledThrough = p.records;
                labels.push_back(le);
            }
        TypeHeadFixed h{};
        h.commitSeq = 1;
        h.gseqHi = gseqBase + N;
        h.gSeg = gSeg;
        h.incarnation = 1;
        h.gLen = gLen;
        h.mSeg = 0;
        h.nextSeg = 1;
        h.arrivalsCount = N;
        h.manifestGen = manGen;
        h.firstLiveCount = N;
        h.firstLiveBytes = uint64_t(double(N) * ty.frameBytes);
        h.nextGen = manGen + 1;
        h.gSegFirstGseq = gseqBase + uint64_t(gSeg) * perG + 1;
        h.tcsHi = 1;
        std::vector<uint8_t> after;
        if (labels.size() <= kMaxInlineLabels) {
            h.nLabels = uint16_t(labels.size());
            after.resize(labels.size() * sizeof(LabelEntry));
            std::memcpy(after.data(), labels.data(), after.size());
        } else {
            // A10: a FULL_LABELS batch at offset 0 of m-000000 (flags bit 0).
            h.nLabels = 0xffff;
            h.labelCkptOff = 0;
            h.labelCkptSeg = 0;
            const uint32_t len = uint32_t(sizeof(TypeBatchHeader) + labels.size() * sizeof(LabelEntry) + 8);
            std::vector<uint8_t> b(len, 0);
            TypeBatchHeader bh{};
            bh.magic = kMagicTypeBatch;
            bh.ver = 1;
            bh.flags = 1;
            bh.commitSeq = 1;
            bh.gSeg = gSeg;
            bh.gOff = gLen;
            bh.nLabel = uint32_t(labels.size());
            bh.gseqHi = h.gseqHi;
            bh.batchLen = len;
            bh.l0Off = len - 8;
            bh.incarnation = 1;
            bh.firstLiveCount = h.firstLiveCount;
            bh.firstLiveBytes = h.firstLiveBytes;
            bh.arrivalsCount = N;
            std::memcpy(b.data(), &bh, sizeof(bh));
            std::memcpy(b.data() + sizeof(bh), labels.data(), labels.size() * sizeof(LabelEntry));
            putU32(b.data() + len - 8, crc32c(b.data(), len - 8));
            PathBuf m0;
            pathTypeSeg(&m0, root.c_str(), ty.type.fid, 'm', 0, "fsl");
            if (!writeFile(P(m0), b)) bad.fetch_add(1);
            h.mEnd = len;
        }
        PathBuf hp;
        pathType(&hp, root.c_str(), ty.type.fid, "h.fsh");
        if (!writeFile(P(hp), headSlot(&h, sizeof(h), after, kHeadType, fidU32(ty.type.fid)))) bad.fetch_add(1);
        gseqBase += N;
    }
    R.set("build_s", double(monoNs() - b0) / 1e9);
    R.set("part_runs", double(partRuns.load()));
    R.set("cat_runs_total", double(catRunsTotal));
    R.set("cat_runs_omm", double(catRunsOmm));
    R.set("arrival_segs", double(arrSegs));
    R.set("cat_accel_est_omm", catResident[0]);
    double catAll = 0;
    for (double x : catResident) catAll += x;
    R.set("cat_accel_est_total", catAll);
    R.set("dom_accel_est", domResident[0]);
    if (bad.load()) {
        std::fprintf(stderr, "  META build failed (%u)\n", bad.load());
        gFailures++;
        return;
    }
    const tb::DirUsage du = tb::dirUsage(root);
    R.set("files", double(du.files));
    R.set("apparent_bytes", double(du.apparent));
    R.set("allocated_bytes", double(du.allocated));
    std::printf("  META built in %.1f s: %llu files, %.3f TB apparent, %.2f GB allocated (du), %llu partition runs, "
                "%llu catalog runs (OMM %llu), %llu arrivals segments; estimated resident accelerators: catalog %.2f GB "
                "(OMM %.2f GB), dominant partition %.2f GB\n",
                R.v["build_s"], (unsigned long long)du.files, double(du.apparent) / kTB, double(du.allocated) / 1e9,
                (unsigned long long)partRuns.load(), (unsigned long long)catRunsTotal,
                (unsigned long long)catRunsOmm, (unsigned long long)arrSegs, catAll / 1e9, catResident[0] / 1e9,
                domResident[0] / 1e9);
    std::fflush(stdout);
    // 4. Open on the real engine.
    CountingIo cio(importIo(), 0);
    EngineConfig c = baseConfig(root, &cio, k.writers);
    std::unique_ptr<Engine> e;
    const uint64_t heap0 = tb::heapBytes();
    const uint64_t o0 = monoNs();
    if (Engine::open(c, &e, &err) < 0) {
        std::fprintf(stderr, "  META open failed: %s\n", err.c_str());
        R.set("open_failed", 1);
        gFailures++;
        return;
    }
    R.set("open_ms", double(monoNs() - o0) / 1e6);
    if (e->start() < 0) {
        gFailures++;
        return;
    }
    EngineStats st = e->stats();
    R.set("open_read_bytes", double(st.openReadBytes));
    R.set("open_opens", double(cio.opens()));
    R.set("open_heap_delta", double(tb::heapBytes()) - double(heap0));
    R.set("open_heap_fixed", gFixedHeap);
    R.set("open_heap_store", double(tb::heapBytes()) - double(heap0) - gFixedHeap);
    R.set("descriptor_bytes", double(st.descriptorBytes));
    R.set("engine_disk_bytes", double(st.diskBytes + st.typeDiskBytes));
    R.set("rss_after_open", double(tb::rssBytes()));
    R.set("handles_after_open", double(cio.openNow()));
    std::printf("  META open %.0f ms, %.1f MB read (%.1f KB per partition), %llu host opens, heap +%.1f MB "
                "(%.1f MB over the empty store's), engine disk bytes %.3f TB, %llu handles open\n",
                R.v["open_ms"], st.openReadBytes / 1e6, st.openReadBytes / 1e3 / double(plan.size()),
                (unsigned long long)cio.opens(), R.v["open_heap_delta"] / 1e6, R.v["open_heap_store"] / 1e6,
                double(st.diskBytes + st.typeDiskBytes) / kTB, (unsigned long long)cio.openNow());
    std::fflush(stdout);
    // 5. Warm each type's catalog with one record in a non-dominant partition.
    double catWarmTotal = 0;
    for (size_t t = 0; t < types.size(); t++) {
        const std::string key = "cat_warm_heap_delta_" + types[t].type.name;
        if (catResident[t] > k.warmMaxGB * 1e9) {
            std::printf("  META %s catalog not warmed: estimated %.2f GB > --tb-meta-warm-max-gb\n",
                        types[t].type.name.c_str(), catResident[t] / 1e9);
            R.set(key, -1);
            continue;
        }
        const PartPlan* pp = nullptr;
        for (const auto& p : plan)
            if (p.type == t && !p.dominant) pp = &p;
        if (!pp) continue;
        const uint64_t h0 = tb::heapBytes(), w0 = monoNs();
        Producer prod(e.get(), pp->pid);
        std::vector<uint8_t> frame;
        uint8_t cid[kCidLen];
        types[t].mf.frame(1ull << 40, 1, 60000, nullptr, &frame);
        frameCid(frame, cid);
        const auto attr = buildRecordAttr(pp->peer, "provider-w", "warm", "warm-1");
        uint64_t rseq = 0;
        const bool ok = prod.enqueue(kEntRecord, kEntCidPresent, int64_t(wallMs()), cid, attr.data(),
                                     uint32_t(attr.size()), frame.data(), uint32_t(frame.size()), &rseq, true) == 0 &&
                        prod.waitAcked(rseq, 600 * kSec) == 0 && waitLabeledEngine(e.get(), {pp->pid}, 600 * kSec);
        const double d = double(tb::heapBytes()) - double(h0);
        R.set(key, ok ? d : -2);
        R.set("cat_warm_ms_" + types[t].type.name, double(monoNs() - w0) / 1e6);
        if (ok) catWarmTotal += d;
        std::printf("  META %s catalog warmed: %s, heap +%.1f MB (estimate %.1f MB) in %.0f ms, handles %llu\n",
                    types[t].type.name.c_str(), ok ? "ok" : "FAILED", d / 1e6, catResident[t] / 1e6,
                    double(monoNs() - w0) / 1e6, (unsigned long long)cio.openNow());
        std::fflush(stdout);
    }
    R.set("cat_warm_heap_delta_total", catWarmTotal);
    // 6. The dominant partition: warm, then write, measuring commits and
    //    maintenance at its S.
    const PartPlan* dom = nullptr;
    for (const auto& p : plan)
        if (p.dominant) dom = &p;
    if (dom && domResident[0] <= k.warmMaxGB * 1e9) {
        Producer prod(e.get(), dom->pid);
        std::vector<uint8_t> frame;
        uint8_t cid[kCidLen];
        const auto attr = buildRecordAttr(dom->peer, "provider-d", "dominant", "dominant-1");
        tb::HistWindow maint, commit;
        const uint64_t h0 = tb::heapBytes(), w0 = monoNs(), m0 = e->stats().merges;
        uint64_t last = 0, sent = 0;
        bool ok = true;
        double firstChunkMs = 0, restS = 0;
        // 256 records a commit: 16 commits fill the L0 directory's merge
        // trigger (mergeL0Blocks), so the partition merges every 4,096.
        const uint64_t chunk = 256;
        for (uint64_t n = 0; n < k.domRecords && ok; n++) {
            types[0].mf.frame((1ull << 41) + n, n % 60000 + 1, 60000, nullptr, &frame);
            frameCid(frame, cid);
            ok = prod.enqueue(kEntRecord, kEntCidPresent, int64_t(wallMs()), cid, attr.data(), uint32_t(attr.size()),
                              frame.data(), uint32_t(frame.size()), &last, true) == 0;
            sent++;
            if (sent % chunk == 0 || n + 1 == k.domRecords) {
                ok = ok && prod.waitAcked(last, 1200 * kSec) == 0;
                if (sent == chunk || n + 1 == k.domRecords) {
                    if (firstChunkMs == 0) {
                        firstChunkMs = double(monoNs() - w0) / 1e6;
                        R.set("dom_warm_heap_delta", double(tb::heapBytes()) - double(h0));
                        R.set("handles_after_dom_warm", double(cio.openNow()));
                        maint.take(e->maintHist());
                        commit.take(e->commitHist());
                        restS = double(monoNs()) / 1e9;
                    }
                }
            }
        }
        maint.take(e->maintHist());
        commit.take(e->commitHist());
        const double secs = double(monoNs()) / 1e9 - restS;
        const uint64_t dm = e->stats().merges - m0;
        R.set("dom_ok", ok ? 1 : 0);
        R.set("dom_first_chunk_ms", firstChunkMs);
        R.set("dom_rec_s", secs > 0 ? double(sent - chunk) / secs : 0);
        R.set("dom_merges", double(dm));
        R.set("dom_commit_p99_ms", double(commit.percentileNs(0.99)) / 1e6);
        R.set("dom_maint_p99_ms", double(maint.percentileNs(0.99)) / 1e6);
        R.set("dom_maint_per_merge_ms", dm ? maint.sumNs() / 1e6 / double(dm) : 0);
        std::printf("  META dominant partition (S=%u): %s, first %llu records (warm) %.0f ms, heap +%.1f MB (estimate "
                    "%.1f MB), then %.0f rec/s, commit p99 %.2f ms, maintenance p99 %.2f ms, %.2f ms maintenance per "
                    "merge, handles %llu\n",
                    dom->segs, ok ? "ok" : "FAILED", (unsigned long long)chunk, firstChunkMs,
                    R.v["dom_warm_heap_delta"] / 1e6, domResident[0] / 1e6, R.v["dom_rec_s"], R.v["dom_commit_p99_ms"],
                    R.v["dom_maint_p99_ms"], R.v["dom_maint_per_merge_ms"], (unsigned long long)cio.openNow());
        std::fflush(stdout);
    } else if (dom) {
        std::printf("  META dominant partition not warmed: estimated %.2f GB > --tb-meta-warm-max-gb\n",
                    domResident[0] / 1e9);
        R.set("dom_warm_heap_delta", -1);
    }
    R.set("handles_hw", double(cio.highWater()));
    R.set("heap_after_warm", double(tb::heapBytes()));
    R.set("rss_after_warm", double(tb::rssBytes()));
    // 7. Cool (idleCloseMs), then a reopen.
    sleepNs(uint64_t(c.idleCloseMs) * 1000000ull + 3 * kSec);
    R.set("handles_after_cool", double(cio.openNow()));
    e->stop();
    e.reset();
    const uint64_t o1 = monoNs();
    if (Engine::open(c, &e, &err) == 0) {
        R.set("reopen_ms", double(monoNs() - o1) / 1e6);
        R.set("reopen_read_bytes", double(e->stats().openReadBytes));
        e->stop();
        e.reset();
    } else {
        R.set("reopen_ms", -1);
    }
    std::printf("  META handles high water %.0f, after cool %.0f; reopen %.0f ms\n", R.v["handles_hw"],
                R.v["handles_after_cool"], R.v["reopen_ms"]);
    std::fflush(stdout);
    std::vector<double> row;
    for (const auto& col : cols) row.push_back(R.v.count(col) ? R.v[col] : 0);
    csv->row(row);
    if (!k.keep) std::filesystem::remove_all(root);
}

}  // namespace

PS_SLOW_TEST(tb_meta_tier) {
    const Knobs k = readKnobs();
    REQUIRE(!k.dir.empty());
    mkdirs(k.dir);
    std::printf("  TB machine: %s\n", tb::machineLine().c_str());
    std::string err;
    std::vector<TypeT> types;
    if (!setupTypes(k, &types, &err)) {
        std::fprintf(stderr, "  setup: %s\n", err.c_str());
        REQUIRE(false);
    }
    if (!buildTemplate(k, &types, &err)) {
        std::fprintf(stderr, "  template: %s\n", err.c_str());
        REQUIRE(false);
    }
    const std::vector<std::string> cols = {
        "tier_tb", "partitions", "records_total", "records_omm", "frame_bytes_total", "segments_total", "s_max",
        "part_runs", "cat_runs_total", "cat_runs_omm", "arrival_segs", "files", "apparent_bytes", "allocated_bytes",
        "engine_disk_bytes", "register_s", "build_s", "open_ms", "open_read_bytes", "open_opens", "open_heap_delta",
        "open_heap_fixed", "open_heap_store", "descriptor_bytes", "rss_after_open", "handles_after_open", "cat_accel_est_omm", "cat_accel_est_total",
        "cat_warm_heap_delta_OMM", "cat_warm_ms_OMM", "cat_warm_heap_delta_CAT", "cat_warm_heap_delta_IQC",
        "cat_warm_heap_delta_total", "dom_accel_est", "dom_warm_heap_delta", "dom_first_chunk_ms",
        "handles_after_dom_warm", "dom_ok", "dom_rec_s", "dom_commit_p99_ms", "dom_maint_p99_ms",
        "dom_maint_per_merge_ms", "dom_merges", "handles_hw", "heap_after_warm", "rss_after_warm", "handles_after_cool", "reopen_ms",
        "reopen_read_bytes", "open_failed"};
    tb::Csv csv;
    REQUIRE(csv.open((k.csv.empty() ? k.dir + "/tb-meta" : k.csv) + ".csv", cols));
    size_t start = 0;
    while (start < k.sizes.size()) {
        const size_t end = std::min(k.sizes.find(',', start), k.sizes.size());
        const double t = std::atof(k.sizes.substr(start, end - start).c_str());
        start = end + 1;
        if (t > 0) runTier(k, types, t, &csv, cols);
    }
}

#endif  // !__wasm__
