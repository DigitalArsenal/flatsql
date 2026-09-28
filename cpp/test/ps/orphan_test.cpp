// Crash points during compaction (T3 acceptance #4; A11, A12, minor 1).
//
// Stores run a kill-heavy workload with small segments, so compactions
// (requested and automatic), merges, meta-segment retirements and
// reclamations are always in flight; the fault host freezes at a random
// mutating I/O call and a random crash mode is applied (drop all unsynced
// writes, drop a random subset, tear the last write, reorder, kill -9). After
// every reopen:
//   - every acked record not killed by an acked tombstone is live, bytes as sent
//     (0 acked records lost), and every record an acked tombstone killed is
//     not (removed by a compaction, or dead);
//   - exactly one file set of every segment is named and on disk: a walk of
//     each partition directory finds 0 orphans and 0 missing files, and the
//     partition's disk_bytes equals the directory's total;
//   - head counters equal a recount of the rows the files hold.
// A trial counts as a crash point during compaction when a compaction was
// planned but not yet applied, and as one inside the grace window when
// retired files were waiting to be unlinked, at the moment of the crash.
#include <algorithm>
#include <map>
#include <random>
#include <set>

#include "flatsql/ps/platform.h"
#include "ps/reader_fixtures.h"

using namespace pst;

namespace {

struct Rec {
    std::vector<uint8_t> frame;
    uint64_t rseq = 0;         // in the current ring (0: from an earlier incarnation)
    bool known = false;        // state verified at a reopen: `live` says which
    bool live = false;
    bool killPending = false;  // a tombstone was enqueued in the current ring
    uint64_t killRseq = 0;
};

struct Part {
    uint32_t pid = 0;
    std::string token;
    std::vector<Rec> recs;
    uint64_t next = 0;
    uint64_t ackedTo = 0;      // highest rseq known acked
};

struct OrphanHarness {
    std::mt19937_64 rng;
    Store s{true, 2, true};
    std::vector<Part> parts;
    uint64_t salt = 0;

    explicit OrphanHarness(uint64_t seed) : rng(seed) {
        s.cfg.sealBytes = 48u << 10;          // many small segments: coalescing
        s.cfg.sealRecords = (seed & 1) ? 60 : 400;
        s.cfg.mergeL0Blocks = 3;
        s.cfg.mergeMinL0Bytes = 0;
        s.cfg.mergeL0Bytes = 128u << 10;
        s.cfg.mergeHelpers = (seed & 2) ? 0 : 1;
        s.cfg.zeroFillStep = (seed & 4) ? (16u << 10) : 0;
        s.cfg.ckptIntervalMs = 10;
        s.cfg.ckptMetaBytes = 8u << 10;
        s.cfg.sealAgeMs = 30;
        s.cfg.compactSliceBytes = 16u << 10;  // builders stop often: more crash points inside
        s.cfg.compactDeadRatio = 0.10;
        s.cfg.reclaimGraceMs = 15;            // crash points inside the grace window
        s.cfg.poolBytes = 48ull << 20;
        s.cfg.commitJournal = (seed & 8) != 0;
        s.cfg.journalCkptMs = 10;
        // The type meta log rotates often: its segments retire and unlink
        // with the catalog runs and manifests merges replace.
        s.cfg.typeMetaSegBytes = (seed & 16) ? (8u << 10) : (32u << 10);
    }

    bool setup() {
        if (s.open() != 0) return false;
        s.registerTypes({&ommType()});
        for (int i = 0; i < 3; i++) {
            Part p;
            p.token = "orph" + std::to_string(i);
            p.pid = s.partition(p.token, ommType());
            if (!p.pid) return false;
            parts.push_back(std::move(p));
        }
        return true;
    }

    // Enqueues records and tombstones and asks for compactions until the host
    // freezes or `ops` entries were attempted.
    void workload(int ops) {
        std::vector<std::unique_ptr<Producer>> prods;
        for (auto& p : parts) prods.emplace_back(new Producer(s.e.get(), p.pid));
        std::vector<SwapResult*> reqs;
        for (int op = 0; op < ops && !s.fs->frozen(); op++) {
            const size_t k = rng() % parts.size();
            Part& p = parts[k];
            Producer& prod = *prods[k];
            const uint32_t r = uint32_t(rng() % 100);
            if (r < 30 && !p.recs.empty()) {
                // Kill a record (by cid) that is not dead or dying.
                Rec& victim = p.recs[rng() % p.recs.size()];
                if (victim.killPending || (victim.known && !victim.live)) continue;
                uint8_t cid[kCidLen];
                frameCid(victim.frame, cid);
                uint64_t rs = 0;
                if (prod.enqueue(kEntTombCid, 0, 0, cid, nullptr, 0, nullptr, 0, &rs, false) == 0) {
                    victim.killPending = true;
                    victim.killRseq = rs;
                }
                continue;
            }
            if (r < 33) {
                auto* q = new SwapResult();
                if (s.e->swapSegment(p.pid, q) == 0) reqs.push_back(q);
                else delete q;
                continue;
            }
            Rec rec;
            const uint64_t id = ++p.next;
            rec.frame = ommRecord(uint32_t(k * 1000000 + id), p.token + "-" + std::to_string(id), "2026-06-01T00:00:00Z",
                                  double(id) + double(salt++), 96 + size_t(rng() % 400));
            rec.rseq = send(s.e.get(), prod, rec.frame, buildRecordAttr(p.token, "prov", "src", "b1"),
                            1780000000000ll + int64_t(id), false);
            if (!rec.rseq) continue;  // ring full or host frozen
            p.recs.push_back(std::move(rec));
        }
        // Let the engine work (maintenance, compactions, reclamation) until
        // the host freezes or a short while passes.
        const uint64_t until = monoNs() + 300000000ull;
        while (!s.fs->frozen() && monoNs() < until) sleepNs(1000000);
        for (size_t k = 0; k < parts.size(); k++) parts[k].ackedTo = prods[k]->ringDesc()->ackedRseq.load();
        // Requests may complete only after the crash: they are abandoned with
        // the engine (never dereferenced after it), then freed.
        pendingReqs.insert(pendingReqs.end(), reqs.begin(), reqs.end());
    }
    std::vector<SwapResult*> pendingReqs;

    void crash(FaultFs::CrashMode mode) {
        s.crash(mode, rng());
        for (auto* q : pendingReqs) delete q;
        pendingReqs.clear();
    }

    bool reopenAndVerify(const char* phase) {
        const int before = gFailures;
        std::string err;
        if (Engine::open(s.cfg, &s.e, &err) != 0) {
            std::fprintf(stderr, "  [%s] reopen failed: %s\n", phase, err.c_str());
            gFailures++;
            return false;
        }
        Inspector ins(s.fs.get(), s.root);
        for (auto& p : parts) {
            PartView v = ins.partition(p.pid);
            if (!v.ok || !v.err.empty()) {
                std::fprintf(stderr, "  [%s] pid %u: %s\n", phase, p.pid, v.err.c_str());
                gFailures++;
                continue;
            }
            const Recount rc = recount(v);
            if (v.head.counters.totalCount != rc.total || v.head.counters.liveCount != rc.live ||
                v.head.counters.liveBytes != rc.liveBytes || v.head.counters.tombCount != rc.tombs ||
                v.head.counters.totalBytes != rc.totalBytes) {
                std::fprintf(stderr, "  [%s] pid %u counters differ from recount\n", phase, p.pid);
                gFailures++;
            }
            const DirCheck dc = checkPartitionDir(s.fs.get(), s.fs.get(), s.root, p.pid);
            if (!dc.ok || dc.bytes != s.e->partitionDiskBytes(p.pid)) {
                std::fprintf(stderr, "  [%s] pid %u files: %s (dir %llu bytes, disk_bytes %llu)\n", phase, p.pid,
                             dc.ok ? "sizes differ" : dc.err.c_str(), (unsigned long long)dc.bytes,
                             (unsigned long long)s.e->partitionDiskBytes(p.pid));
                gFailures++;
            }
            // Live PUTs by cid.
            std::map<std::string, const RecRow*> live;
            for (const auto& row : v.rows)
                if (row.kind == kRowPut && !rc.dead.count(row.pseq))
                    live[std::string(reinterpret_cast<const char*>(row.cid), kCidLen)] = &row;
            int lost = 0, resurrected = 0, badBytes = 0;
            for (const Rec& r : p.recs) {
                uint8_t cid[kCidLen];
                frameCid(r.frame, cid);
                const auto it = live.find(std::string(reinterpret_cast<const char*>(cid), kCidLen));
                const bool present = r.known ? r.live : (r.rseq && r.rseq <= p.ackedTo);
                const bool killAcked = r.killPending && r.killRseq <= p.ackedTo;
                const bool dead = (r.known && !r.live) || killAcked;
                if (present && !dead) {
                    if (it == live.end()) {
                        if (!r.killPending) lost++;  // an unacked kill may have applied
                        continue;
                    }
                    if (ins.frame(p.pid, *it->second) != r.frame) badBytes++;
                } else if (dead && it != live.end()) {
                    resurrected++;
                }
            }
            if (lost || resurrected || badBytes) {
                std::fprintf(stderr, "  [%s] pid %u: %d acked records lost, %d killed records live, %d with different bytes\n",
                             phase, p.pid, lost, resurrected, badBytes);
                gFailures++;
            }
            // What survives is the oracle from here on: unacked records that
            // did not survive are forgotten; unacked kills that applied are
            // treated as acked, those that did not as never sent.
            std::vector<Rec> keep;
            for (Rec& r : p.recs) {
                uint8_t cid[kCidLen];
                frameCid(r.frame, cid);
                const bool isLive = live.count(std::string(reinterpret_cast<const char*>(cid), kCidLen)) != 0;
                const bool present = r.known || (r.rseq && r.rseq <= p.ackedTo);
                if (!present && !isLive) continue;  // maybe never committed: forget it
                r.known = true;
                r.live = isLive;
                r.rseq = 0;
                r.killPending = false;
                r.killRseq = 0;
                keep.push_back(std::move(r));
            }
            p.recs.swap(keep);
            p.ackedTo = 0;
        }
        // Type logs: the head's segments, the manifest's runs, nothing else.
        const DirCheck tc = checkTypeDir(s.fs.get(), s.fs.get(), s.root, ommType().fid);
        if (tc.ok && tc.bytes != s.e->typeDiskBytesOf(ommType().fid)) {
            std::fprintf(stderr, "  [%s] type dir %llu bytes, type disk bytes %llu\n", phase,
                         (unsigned long long)tc.bytes, (unsigned long long)s.e->typeDiskBytesOf(ommType().fid));
            gFailures++;
        }
        if (!tc.ok) {
            std::fprintf(stderr, "  [%s] type files: %s\n", phase, tc.err.c_str());
            {
                Inspector ti(s.fs.get(), s.root);
                const auto tv = ti.type(ommType().fid);
                std::fprintf(stderr, "    head manifestGen %x nextGen %x mSeg %u firstLiveMSeg %u commitSeq %llu\n",
                             tv.head.manifestGen, tv.head.nextGen, tv.head.mSeg, tv.head.firstLiveMSeg,
                             (unsigned long long)tv.head.commitSeq);
                for (const std::string& path : s.fs->list(s.root + "/fsql2/t/"))
                    std::fprintf(stderr, "    %s\n", path.c_str());
            }
            gFailures++;
        }
        return gFailures == before;
    }

    // Type level, once labels caught up: the catalog agrees with the files
    // (every cid is unique, so first_live_count is the number of live
    // records), and a sample of cids looked up through the catalog is found
    // exactly when live.
    bool typeVerify(const char* phase) {
        const int before = gFailures;
        std::vector<uint32_t> pids;
        uint64_t live = 0;
        std::vector<const Rec*> liveRecs, deadRecs;
        for (auto& p : parts) {
            pids.push_back(p.pid);
            for (const Rec& r : p.recs) {
                if (r.known && r.live) {
                    live++;
                    liveRecs.push_back(&r);
                } else if (r.known) {
                    deadRecs.push_back(&r);
                }
            }
        }
        if (!waitLabeledEngine(s.e.get(), pids, 60000000000ull) ||
            !waitTypeVisible(s.fs.get(), s.root, ommType().fid, pids, 60000000000ull)) {
            std::fprintf(stderr, "  [%s] labels did not catch up\n", phase);
            gFailures++;
            return false;
        }
        Reader rd(s, LaneClass::Interactive, 1);
        const Rows t = rd.q("SELECT first_live_count FROM flatsql_types WHERE type = 'OMM'");
        if (t.status != 0 || t.rows.size() != 1 || uint64_t(t.i(0, 0)) != live) {
            std::fprintf(stderr, "  [%s] first_live_count %lld, %llu live records\n", phase,
                         t.rows.empty() ? -1ll : (long long)t.i(0, 0), (unsigned long long)live);
            gFailures++;
        }
        int wrong = 0;
        for (int i = 0; i < 16; i++) {
            const bool wantLive = (i & 1) == 0;
            const auto& pool = wantLive ? liveRecs : deadRecs;
            if (pool.empty()) continue;
            const Rec* r = pool[rng() % pool.size()];
            const Rows c = rd.q("SELECT count(*) FROM OMM WHERE _cid = ?", {Param::text(cidTextOf(r->frame))});
            if (c.status != 0 || c.rows.size() != 1 || c.i(0, 0) != (wantLive ? 1 : 0)) wrong++;
        }
        if (wrong) {
            std::fprintf(stderr, "  [%s] %d catalog lookups wrong\n", phase, wrong);
            gFailures++;
        }
        return gFailures == before;
    }
};

void runOrphanTrials(int wantDuring, int maxTrials, uint64_t seed0) {
    int trials = 0, during = 0, inGrace = 0;
    std::map<int, int> modes;
    uint64_t compactions = 0, unlinked = 0, metaRetired = 0, catalogDropped = 0;
    uint64_t seed = seed0;
    while ((during < wantDuring) && trials < maxTrials) {
        OrphanHarness h(seed++);
        if (!h.setup()) {
            gFailures++;
            return;
        }
        for (int ph = 0; ph < 12 && during < wantDuring && trials < maxTrials; ph++) {
            h.s.fs->armCrashAtOp(h.s.fs->mutatingOps() + 20 + h.rng() % 2500);
            h.workload(200 + int(h.rng() % 600));
            h.s.fs->freeze();
            const EngineStats st = h.s.e->stats();
            const bool compacting = st.compactInFlight > 0;
            const bool grace = st.retiredFiles > st.unlinkedFiles;
            compactions += st.compactions;
            unlinked += st.unlinkedFiles;
            metaRetired += st.metaSegsRetired;
            catalogDropped += st.catalogEntriesDropped;
            const auto mode = FaultFs::CrashMode(h.rng() % FaultFs::kModeCount);
            modes[mode]++;
            h.crash(mode);
            trials++;
            if (compacting) during++;
            if (grace) inGrace++;
            if (!h.reopenAndVerify("crash")) {
                std::fprintf(stderr, "  trial %d (seed %llu, mode %d, compacting %d, grace %d) failed\n", trials,
                             (unsigned long long)(seed - 1), int(mode), int(compacting), int(grace));
                return;
            }
            h.s.e->start();
            if (!h.typeVerify("crash")) {
                std::fprintf(stderr, "  trial %d (seed %llu) failed at type level\n", trials,
                             (unsigned long long)(seed - 1));
                return;
            }
        }
        h.s.close();
    }
    report("orphan_trials", double(trials), "trials");
    report("orphan_crash_points_during_compaction", double(during), "trials");
    report("orphan_crash_points_in_grace_window", double(inGrace), "trials");
    report("orphan_compactions_applied", double(compactions), "compactions");
    report("orphan_files_unlinked", double(unlinked), "files");
    report("orphan_meta_segments_retired", double(metaRetired), "segments");
    report("orphan_catalog_entries_dropped", double(catalogDropped), "entries");
    const char* names[] = {"drop_all", "drop_subset", "tear_last_512", "reorder", "kill9_keep_all"};
    for (const auto& kv : modes) {
        char key[64];
        std::snprintf(key, sizeof(key), "orphan_mode_%s", names[kv.first]);
        report(key, double(kv.second), "trials");
    }
    CHECK(during >= wantDuring);
}

}  // namespace

PS_TEST(orphan_crash_points_during_compaction_T3_4) {
    runOrphanTrials(int(argInt("during", 25)), int(argInt("orphan-trials", 400)), uint64_t(argInt("orphan-seed", 1)));
}
PS_SLOW_TEST(orphan_crash_points_during_compaction_T3_4_full) {
    runOrphanTrials(int(argInt("during", 1000)), int(argInt("orphan-trials", 20000)), uint64_t(argInt("orphan-seed", 1000)));
}

// Outputs of a type merge a crash cut short sit at generations after the
// manifest's; the head's next_gen can lag them (heads are not written every
// commit). Open sweeps past next_gen and past every leftover it finds.
PS_TEST(orphan_type_merge_outputs_past_next_gen_go_at_open) {
    Store s(true, 1, true);
    s.cfg.mergeL0Blocks = 2;
    s.cfg.mergeMinL0Bytes = 0;
    REQUIRE(s.open() == 0);
    s.registerTypes({&ommType()});
    const uint32_t pid = s.partition("sweep", ommType());
    {
        Producer p(s.e.get(), pid);
        for (int i = 0; i < 200; i++) {
            const uint64_t r = send(s.e.get(), p, ommRecord(uint32_t(80000 + i), "S", "2026-09-01T00:00:00Z", 15.0),
                                    buildRecordAttr("peer", "prov", "src", "b"), i);
            if (i % 10 == 9) REQUIRE(p.waitAcked(r, 60000000000ull) == 0);
        }
    }
    REQUIRE(waitLabeledEngine(s.e.get(), {pid}, 60000000000ull));
    s.close();
    Inspector ins(s.fs.get(), s.root);
    const auto tv = ins.type(ommType().fid);
    REQUIRE(tv.ok && tv.head.manifestGen > 0);
    // Leftovers of merges past next_gen: one just past it, one 40 further
    // (found by the sweep's reach from the first), and a run without its
    // manifest.
    const uint32_t gens[3] = {tv.head.nextGen + 2, tv.head.nextGen + 42, tv.head.nextGen + 50};
    IoStats st;
    IoCtx ctx(s.fs.get(), &st);
    for (int k = 0; k < 3; k++) {
        for (const char letter : {'x', 'f'}) {
            if (k == 2 && letter == 'f') continue;
            PathBuf path;
            typeRetirePath(&path, s.root.c_str(), ommType().fid, retireItem(letter, 0, gens[k], 0));
            FileRef f;
            REQUIRE(ctx.open(path.c_str(), path.len,
                             FLATSQL_IO_READ | FLATSQL_IO_WRITE | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS,
                             FileClass::Index, &f) == 0);
            const uint8_t junk[64] = {1, 2, 3};
            REQUIRE(ctx.write(f, junk, sizeof(junk), 0) == 0);
            REQUIRE(ctx.sync(f) == 0);
            ctx.close(&f);
        }
    }
    CHECK(!checkTypeDir(s.fs.get(), s.fs.get(), s.root, ommType().fid).ok);
    std::string err;
    REQUIRE(Engine::open(s.cfg, &s.e, &err) == 0);
    const DirCheck dc = checkTypeDir(s.fs.get(), s.fs.get(), s.root, ommType().fid);
    if (!dc.ok) std::fprintf(stderr, "  type dir: %s\n", dc.err.c_str());
    CHECK(dc.ok);
    CHECK_EQ(dc.bytes, s.e->typeDiskBytesOf(ommType().fid));
    s.e->start();
    s.close();
}
