// Open tests: clean open cost (T1 #2), tail reads after drop-unsynced,
// resend dedupe after a crash (T1 #3), the -3 rebuild when both head slots
// are torn, and the MIGRATED gate (§10, A5).
#include <algorithm>
#include <atomic>
#include <filesystem>
#include <thread>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {
std::string ep(uint64_t i) {
    char b[40];
    std::snprintf(b, sizeof(b), "2026-07-%02dT%02d:%02d:%02dZ", int(1 + (i / 86400) % 28), int((i / 3600) % 24),
                  int((i / 60) % 60), int(i % 60));
    return b;
}

// Fills `parts` partitions (of 4 types) with `total` records using `threads`
// producers; returns the partition ids.
std::vector<uint32_t> fill(Store& s, uint32_t parts, uint64_t total, uint32_t threads) {
    s.registerTypes({&ommType(), &mpeType(), &catType(), &iqcType()});
    std::vector<uint32_t> pids;
    TestType* types[4] = {&ommType(), &mpeType(), &catType(), &ommType()};
    for (uint32_t i = 0; i < parts; i++) pids.push_back(s.partition("prod" + std::to_string(i), *types[i % 4]));
    std::vector<std::thread> ts;
    for (uint32_t t = 0; t < threads; t++) {
        ts.emplace_back([&, t] {
            const auto attr = buildRecordAttr("x", "prov", "src", "b1");
            std::vector<Producer> ps;
            std::vector<uint32_t> idx;
            for (uint32_t i = t; i < parts; i += threads) {
                ps.emplace_back(s.e.get(), pids[i]);
                idx.push_back(i);
            }
            std::vector<uint64_t> last(ps.size(), 0);
            const uint64_t perPart = total / parts;
            for (uint64_t n = 0; n < perPart; n++) {
                for (size_t k = 0; k < ps.size(); k++) {
                    const uint32_t i = idx[k];
                    const uint64_t id = uint64_t(i) * perPart + n;
                    std::vector<uint8_t> f;
                    switch (i % 4) {
                        case 1: f = mpeRecord("E" + std::to_string(id), 1.7e9 + double(n), double(i)); break;
                        case 2: f = catRecord(uint32_t(id + 1), "C" + std::to_string(id), "", "", "N" + std::to_string(id)); break;
                        default: f = ommRecord(uint32_t(id), "O", ep(n), double(i) + double(n) * 1e-4); break;
                    }
                    last[k] = send(s.e.get(), ps[k], f, attr, int64_t(n));
                }
            }
            for (size_t k = 0; k < ps.size(); k++) ps[k].waitAcked(last[k], 60000000000ull);
        });
    }
    for (auto& t : ts) t.join();
    return pids;
}

void openCost(uint64_t total, bool realFs) {
    Store s(false, 8, true);
    std::string dir;
    if (realFs) {
        dir = argStr("dir", "/tmp/flatsql-ps-open-test");
        std::filesystem::remove_all(dir);
        s.cfg.io = nullptr;  // the seven imports: POSIX + F_FULLFSYNC / fdatasync
        s.cfg.root = dir;
        s.root = dir;
    }
    s.cfg.poolBytes = 256ull << 20;
    REQUIRE(s.open() == 0);
    const uint64_t t0 = monoNs();
    fill(s, 256, total, 8);
    const double ingestS = double(monoNs() - t0) / 1e9;
    report(realFs ? "open_fixture_ingest_real_fs" : "open_fixture_ingest_mem", double(total) / ingestS, "records/s");
    s.close();
    const uint64_t o0 = monoNs();
    REQUIRE(s.open() == 0);
    const double openMs = double(monoNs() - o0) / 1e6;
    const EngineStats st = s.e->stats();
    report(realFs ? "open_clean_ms_real_fs" : "open_clean_ms_mem", openMs, "ms");
    report(realFs ? "open_clean_read_bytes_real_fs" : "open_clean_read_bytes_mem", double(st.openReadBytes), "bytes");
    report(realFs ? "open_clean_syncs_real_fs" : "open_clean_syncs_mem", double(st.openSyncs), "syncs");
    report(realFs ? "open_clean_write_bytes_real_fs" : "open_clean_write_bytes_mem", double(st.openWriteBytes), "bytes");
    // The 150 ms bound is a latency acceptance: enforced, like every latency
    // bound of this suite, on a quiet box (release build, 8+ hardware
    // threads, low load; latencyBoxQuiet). A debug build on a loaded 3-vCPU
    // CI runner measured 165-210 ms. What open reads, parses and writes is
    // enforced everywhere.
    {
        unsigned hw = 0;
        double load1 = 0;
        std::string why;
        if (latencyBoxQuiet(&hw, &load1, &why)) {
            CHECK(openMs <= 150.0);
        } else if (openMs > 150.0) {
            std::printf("  NOTE open %.1f ms reported, not enforced: %s\n", openMs, why.c_str());
        }
    }
    CHECK(st.openReadBytes <= (1u << 20));
    CHECK_EQ(st.openDataBytes, uint64_t(0));
    CHECK_EQ(st.framesParsedAtOpen, uint64_t(0));
    s.close();
    if (realFs) std::filesystem::remove_all(dir);
}
}  // namespace

PS_TEST(open_clean_256_partitions_T1_2) { openCost(uint64_t(argInt("open-records", 200000)), false); }
PS_SLOW_TEST(open_clean_1M_records_256_partitions_T1_2_full) { openCost(1000000, false); }
PS_SLOW_TEST(open_clean_1M_records_256_partitions_T1_2_real_fs) {
    openCost(uint64_t(argInt("open-real-records", 1000000)), true);
}

PS_TEST(open_after_drop_unsynced_reads_bounded_meta) {
    Store s(true, 4, true);
    REQUIRE(s.open() == 0);
    const auto pids = fill(s, 64, 64 * 300, 4);
    // Keep the tail short (small commits): trickle one record per partition.
    const auto attr = buildRecordAttr("x", "prov", "src", "b2");
    for (int round = 0; round < 20; round++) {
        for (size_t i = 0; i < pids.size(); i += 2) {
            Producer p(s.e.get(), pids[i]);
            const uint64_t r = send(s.e.get(), p, ommRecord(uint32_t(700000 + round * 100 + i), "T", ep(round), 9.0), attr, round);
            p.waitAcked(r, 10000000000ull);
        }
    }
    s.crash(FaultFs::kDropAll, 1);
    REQUIRE(s.open() == 0);
    const EngineStats st = s.e->stats();
    const double perPart = double(st.openMetaBytes) / double(pids.size());
    report("open_after_drop_unsynced_meta_bytes_per_partition", perPart, "bytes");
    report("open_after_drop_unsynced_adopted_batches", double(st.adoptedBatches), "batches");
    CHECK(perPart <= 65536.0);
    CHECK_EQ(st.openDataBytes, uint64_t(0));
    CHECK_EQ(st.framesParsedAtOpen, uint64_t(0));
    s.close();
}

PS_TEST(open_resend_after_crash_dedupes_T1_3) {
    Store s(true, 2, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    std::vector<uint32_t> pids;
    for (int i = 0; i < 16; i++) pids.push_back(s.partition("r" + std::to_string(i), ommType()));
    const auto attr = buildRecordAttr("x", "prov", "src", "b1");
    struct Acked {
        uint32_t pidx;
        std::vector<uint8_t> frame;
    };
    std::vector<Acked> acked;
    {
        std::vector<Producer> ps;
        for (uint32_t pid : pids) ps.emplace_back(s.e.get(), pid);
        std::vector<uint64_t> last(pids.size(), 0);
        for (int n = 0; n < 1500; n++)
            for (size_t k = 0; k < ps.size(); k++) {
                auto f = ommRecord(uint32_t(n * 16 + k), "R", ep(n), double(k) + n * 1e-3);
                last[k] = send(s.e.get(), ps[k], f, attr, n);
                acked.push_back({uint32_t(k), std::move(f)});
            }
        for (size_t k = 0; k < ps.size(); k++) CHECK_EQ(ps[k].waitAcked(last[k], 30000000000ull), 0);
    }
    s.crash(FaultFs::kDropAll, 7);
    REQUIRE(s.open() == 0);
    Inspector ins(s.fs.get(), s.root);
    uint64_t rowsBefore = 0;
    for (uint32_t pid : pids) rowsBefore += ins.partition(pid).rows.size();
    CHECK_EQ(rowsBefore, uint64_t(acked.size()));
    IoStats io0;
    s.e->totalIo(&io0);
    const uint64_t d0 = io0.readBytes(FileClass::Data);
    const uint64_t rows0 = s.e->stats().rowsAppended;
    {
        std::vector<Producer> ps;
        for (uint32_t pid : pids) ps.emplace_back(s.e.get(), pid);
        std::vector<uint64_t> last(pids.size(), 0);
        const size_t from = acked.size() > 10000 ? acked.size() - 10000 : 0;
        for (size_t i = from; i < acked.size(); i++)
            last[acked[i].pidx] = send(s.e.get(), ps[acked[i].pidx], acked[i].frame, attr, 99);
        for (size_t k = 0; k < ps.size(); k++)
            if (last[k]) CHECK_EQ(ps[k].waitAcked(last[k], 30000000000ull), 0);
    }
    IoStats io1;
    s.e->totalIo(&io1);
    report("resend_new_rows", double(s.e->stats().rowsAppended - rows0), "rows");
    report("resend_data_bytes_read", double(io1.readBytes(FileClass::Data) - d0), "bytes");
    CHECK_EQ(s.e->stats().rowsAppended - rows0, uint64_t(0));
    CHECK_EQ(io1.readBytes(FileClass::Data) - d0, uint64_t(0));
    s.close();
    REQUIRE(s.open() == 0);
    uint64_t rowsAfter = 0;
    for (uint32_t pid : pids) rowsAfter += ins.partition(pid).rows.size();
    CHECK_EQ(rowsAfter, rowsBefore);
    s.close();
}

PS_TEST(open_both_head_slots_torn_rebuilds_from_meta) {
    Store s(true, 1, true);
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("torn", ommType());
    Producer p(s.e.get(), pid);
    const auto attr = buildRecordAttr("x", "prov", "src", "b1");
    uint64_t last = 0;
    for (int i = 0; i < 300; i++) last = send(s.e.get(), p, ommRecord(uint32_t(i), "T", ep(i), 1.0 + i), attr, i);
    CHECK_EQ(p.waitAcked(last, 10000000000ull), 0);
    s.close();
    // Tear both slots.
    PathBuf hp;
    pathPartition(&hp, s.root.c_str(), pid, "h.fsh");
    const int32_t h = s.fs->open(hp.c_str(), int32_t(hp.len), FLATSQL_IO_READ | FLATSQL_IO_WRITE);
    REQUIRE(h >= 0);
    const uint8_t junk[16] = {0xde, 0xad, 0xbe, 0xef};
    s.fs->write(h, junk, 16, 40);
    s.fs->write(h, junk, 16, 4096 + 40);
    s.fs->sync(h);
    s.fs->close(h);
    REQUIRE(s.open() == 0);
    CHECK_EQ(s.e->partition(pid)->pseqHi, uint64_t(300));
    CHECK_EQ(s.e->stats().openDataBytes, uint64_t(0));
    s.close();
    Inspector ins(s.fs.get(), s.root);
    PartView v = ins.partition(pid);
    CHECK(v.err.empty());
    CHECK_EQ(v.rows.size(), size_t(300));
    CHECK_EQ(v.head.counters.liveCount, uint64_t(300));
}

PS_TEST(open_refuses_store_without_migrated_marker) {
    Store s(true, 1, true);
    s.cfg.freshMarksMigrated = false;
    s.cfg.requireMigrated = false;
    REQUIRE(s.open() == 0);
    s.close();
    s.cfg.requireMigrated = true;
    std::string err;
    CHECK_EQ(s.open(&err), int32_t(FLATSQL_IO_ERR_ACCESS));
    CHECK(err.find("MIGRATED") != std::string::npos);
}

// A partition with more than 32 live lanes keeps its lane table after an
// open whose first head comes from a batch without lane deltas (MERGE_DONE):
// the head must still point at the LANE_CKPT record (found by T4 under the
// Node host: "lane checkpoint out of bounds (seg 0 off 0 ...)").
// Liveness at the L0 directory caps: with a block trigger no directory ever
// reaches (mergeL0Blocks above kMaxL0Dir), one record per commit round makes
// every commit an L0 block. The type's directory fills first; its merge must
// run at the cap, or labeling stops, the partition's merges wait for labels
// and its writer stops at its own cap, and acks stop for good.
PS_TEST(open_l0_directory_caps_force_merges) {
    Store s(true, 1, true);
    s.cfg.mergeL0Blocks = 1000;
    s.cfg.mergeMinL0Bytes = 0;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("dircap", ommType());
    Producer p(s.e.get(), pid);
    const int n = int(4 * (kMaxL0Dir + kMaxTypeL0Dir));
    int acked = 0;
    for (int i = 0; i < n; i++) {
        const uint64_t r = send(s.e.get(), p, ommRecord(uint32_t(70000 + i), "D", "2026-09-01T00:00:00Z", 15.0),
                                buildRecordAttr("peer", "prov", "src", "b"), i);
        if (!r || p.waitAcked(r, 60000000000ull) != 0) break;  // one record per commit round
        acked++;
    }
    CHECK_EQ(acked, n);
    // Labels catch up with every durable row, and the partition merged.
    const Partition* part = s.e->partition(pid);
    bool labeled = false;
    for (const uint64_t t0 = monoNs(); !labeled && monoNs() - t0 < 60000000000ull; sleepNs(5000000))
        labeled = part->labeledThrough.load() >= part->durablePseqHi.load() && part->durablePseqHi.load() >= uint64_t(n);
    CHECK(labeled);
    CHECK(s.e->stats().merges > 0);
    s.close();
}

PS_TEST(open_lane_checkpoint_pointer_survives_merge_after_open) {
    Store s(true, 1, true);
    s.cfg.mergeL0Blocks = 1000;  // no merges in the first session
    s.cfg.mergeMinL0Bytes = 0;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("lanes", ommType());
    {
        Producer p(s.e.get(), pid);
        for (int i = 0; i < 400; i++) {
            const uint64_t r = send(s.e.get(), p, ommRecord(uint32_t(90000 + i), "L", "2026-09-01T00:00:00Z", 15.0),
                                    buildRecordAttr("peer", "prov", "src" + std::to_string(i % 40), "b"), i);
            if (i % 20 == 19) REQUIRE(p.waitAcked(r, 60000000000ull) == 0);  // many L0 blocks
        }
    }
    s.close();
    s.cfg.mergeL0Blocks = 2;  // the next open merges at once: MERGE_DONE carries no lane deltas
    REQUIRE(s.open() == 0);
    for (int i = 0; i < 6000 && s.e->partition(pid)->nL0 > 1; i++) sleepNs(10000000);
    s.close();
    REQUIRE(s.open() == 0);
    s.close();
    Inspector ins(s.fs.get(), s.root);
    PartView v = ins.partition(pid);
    if (!v.ok || !v.err.empty()) std::fprintf(stderr, "  inspector: %s\n", v.err.c_str());
    CHECK(v.ok && v.err.empty());
    size_t live = 0;
    for (const auto& l : v.lanes) live += l.count ? 1 : 0;
    CHECK_EQ(live, size_t(40));
}
