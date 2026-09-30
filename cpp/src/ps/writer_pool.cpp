// FlatSQL partition store: writer threads, commit rounds, ownership, the
// sync pool and the native producer (design §5.2, §6.4, §7, A8, A24, A25,
// A26).
#include <algorithm>
#include <cstdlib>

#include "flatsql/ps/flatsql_attr_generated.h"
#include "flatsql/ps/lane_arena.h"
#include "internal.h"

#if !defined(__wasm__)
#  include <pthread.h>
#  if defined(__APPLE__)
#    include <mach/mach.h>
#  elif defined(__linux__)
#    include <sys/syscall.h>
#    include <unistd.h>
#  endif
#endif

namespace flatsql {
namespace ps {

namespace {
uint32_t osThreadId() {
#if defined(__APPLE__)
    uint64_t tid = 0;
    pthread_threadid_np(nullptr, &tid);
    return uint32_t(tid);
#elif defined(__linux__)
    return uint32_t(syscall(SYS_gettid));
#else
    static std::atomic<uint32_t> next{1};
    thread_local uint32_t id = next.fetch_add(1);
    return id;
#endif
}
}  // namespace

// ---------------------------------------------------------------------------
// Small building blocks
// ---------------------------------------------------------------------------
void LockHist::record(uint64_t ns) {
    buckets[bucketOf(ns)].fetch_add(1, std::memory_order_relaxed);
    count.fetch_add(1, std::memory_order_relaxed);
    uint64_t m = maxNs.load(std::memory_order_relaxed);
    while (ns > m && !maxNs.compare_exchange_weak(m, ns)) {}
}

uint64_t LockHist::percentileNs(double q) const {
    const uint64_t total = count.load();
    if (!total) return 0;
    const uint64_t target = uint64_t(q * double(total));
    uint64_t acc = 0;
    for (int b = 0; b < kBuckets; b++) {
        acc += buckets[b].load();
        if (acc > target) return upperNs(b);  // upper bound of the bucket
    }
    return maxNs.load();
}

bool Arena::init(size_t bytes) {
    base_ = static_cast<uint8_t*>(std::malloc(bytes));
    cap_ = base_ ? bytes : 0;
    used_ = 0;
    return base_ != nullptr;
}

Arena::~Arena() { std::free(base_); }

void* Arena::alloc(size_t n, size_t align) {
    size_t at = used_;
    if (align > 1) at = (at + align - 1) & ~(align - 1);
    if (at + n > cap_) return nullptr;
    used_ = at + n;
    if (used_ > high_.load(std::memory_order_relaxed)) high_.store(used_, std::memory_order_relaxed);
    return base_ + at;
}

void* SlabChain::alloc(SlabPool& pool, size_t n, uint64_t* pos) {
    slabBytes_ = pool.slabBytes();
    if (n > slabBytes_) return nullptr;
    if (tail_ == head_ || used_ + n > slabBytes_) {
        if (tail_ - head_ >= kMaxSlabs) return nullptr;
        const uint32_t s = pool.alloc();
        if (s == kNoSlab) return nullptr;
        ids_[tail_ % kMaxSlabs] = s;
        tail_++;
        used_ = 0;
    }
    uint8_t* p = pool.ptr(ids_[(tail_ - 1) % kMaxSlabs]) + used_;
    *pos = ((tail_ - 1) << 32) | used_ | 1;  // never 0
    used_ += uint32_t((n + 7) & ~size_t(7));
    return p;
}

void SlabChain::freeBefore(SlabPool& pool, uint64_t pos) {
    const uint64_t slab = pos >> 32;
    while (head_ < slab && head_ < tail_) {
        pool.free(ids_[head_ % kMaxSlabs]);
        head_++;
    }
}

void SlabChain::freeAll(SlabPool& pool) {
    while (head_ < tail_) {
        pool.free(ids_[head_ % kMaxSlabs]);
        head_++;
    }
    used_ = 0;
}

CmdQueue::CmdQueue(uint32_t capPow2) : cells_(new Cell[capPow2]), mask_(capPow2 - 1) {
    for (uint32_t i = 0; i < capPow2; i++) cells_[i].seq.store(i, std::memory_order_relaxed);
}

bool CmdQueue::push(const Cmd& c) {
    uint64_t pos = enq_.load(std::memory_order_relaxed);
    for (;;) {
        Cell& cell = cells_[pos & mask_];
        const uint64_t seq = cell.seq.load(std::memory_order_acquire);
        const int64_t dif = int64_t(seq) - int64_t(pos);
        if (dif == 0) {
            if (enq_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                cell.cmd = c;
                cell.seq.store(pos + 1, std::memory_order_release);
                return true;
            }
        } else if (dif < 0) {
            return false;
        } else {
            pos = enq_.load(std::memory_order_relaxed);
        }
    }
}

bool CmdQueue::pop(Cmd* c) {
    uint64_t pos = deq_.load(std::memory_order_relaxed);
    for (;;) {
        Cell& cell = cells_[pos & mask_];
        const uint64_t seq = cell.seq.load(std::memory_order_acquire);
        const int64_t dif = int64_t(seq) - int64_t(pos + 1);
        if (dif == 0) {
            if (deq_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                *c = cell.cmd;
                cell.seq.store(pos + mask_ + 1, std::memory_order_release);
                return true;
            }
        } else if (dif < 0) {
            return false;
        } else {
            pos = deq_.load(std::memory_order_relaxed);
        }
    }
}

bool CmdQueue::empty() const {
    return enq_.load(std::memory_order_acquire) == deq_.load(std::memory_order_acquire);
}

// ---------------------------------------------------------------------------
// Sync pool (A8): concurrent sync rounds; the submitting writer helps, so a
// round never depends on pool threads alone.
// ---------------------------------------------------------------------------
SyncPool::~SyncPool() { stop(); }

void SyncPool::start(uint32_t threads) {
    stop_.store(false);
    for (uint32_t i = 0; i < threads; i++) {
        threads_.emplace_back([this, i] {
            uint32_t rr = i;
            while (!stop_.load(std::memory_order_acquire)) {
                const uint32_t w = work_.load(std::memory_order_acquire);
                bool progress = false;
                for (uint32_t k = 0; k < kMaxWriters; k++) {
                    Slot& s = slots_[(rr + k) % kMaxWriters];
                    if (helpOne(s)) {
                        progress = true;
                        rr = (rr + k + 1) % kMaxWriters;
                        break;
                    }
                }
                if (!progress) waitU32(&work_, w, 5000000);
            }
        });
    }
}

void SyncPool::stop() {
    if (threads_.empty()) return;
    stop_.store(true, std::memory_order_release);
    work_.fetch_add(1, std::memory_order_release);
    wakeU32(&work_, -1);
    for (auto& t : threads_) t.join();
    threads_.clear();
}

bool SyncPool::helpOne(Slot& s) {
    uint64_t c = s.claim.load(std::memory_order_acquire);
    for (;;) {
        const uint32_t idx = uint32_t(c);
        const uint32_t n = s.n.load(std::memory_order_acquire);
        if (idx >= n) return false;
        SyncJob* jobs = s.jobs.load(std::memory_order_relaxed);
        if (!s.claim.compare_exchange_weak(c, c + 1, std::memory_order_acq_rel)) continue;
        SyncJob& job = jobs[idx];
        const int saved = tHotPathDepth;
        tHotPathDepth = 0;  // host call
        *job.result = job.io->sync(job.handle);
        tHotPathDepth = saved;
        if (s.remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) wakeU32(&s.remaining, -1);
        return true;
    }
}

void SyncPool::runAll(uint32_t writer, SyncJob* jobs, size_t n) {
    if (n == 0) return;
    if (threads_.empty() || n == 1 || writer >= kMaxWriters) {
        const int saved = tHotPathDepth;
        tHotPathDepth = 0;  // host calls
        for (size_t i = 0; i < n; i++) *jobs[i].result = jobs[i].io->sync(jobs[i].handle);
        tHotPathDepth = saved;
        return;
    }
    Slot& s = slots_[writer];
    // Publish the round: n first, then a new generation with next = 0.
    s.n.store(0, std::memory_order_release);
    const uint64_t gen = (s.claim.load(std::memory_order_relaxed) >> 32) + 1;
    s.claim.store(gen << 32, std::memory_order_release);
    s.jobs.store(jobs, std::memory_order_relaxed);
    s.remaining.store(uint32_t(n), std::memory_order_release);
    s.n.store(uint32_t(n), std::memory_order_release);
    work_.fetch_add(1, std::memory_order_release);
    wakeU32(&work_, int(n - 1));
    while (helpOne(s)) {}
    for (;;) {
        const uint32_t r = s.remaining.load(std::memory_order_acquire);
        if (r == 0) break;
        waitU32(&s.remaining, r, 1000000);
    }
    s.n.store(0, std::memory_order_release);
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------
Writer::Writer(Engine* eng, uint8_t id) : eng_(eng), id_(id), io_(nullptr, nullptr) {
    io_ = IoCtx(eng->config().io ? eng->config().io : importIo(), &ioStats_);
    sc_ = new StageScratch();
    sc_->init(eng->config());
    prepScratch_.reset(new PrepResult());
    arena_.init(size_t(eng->config().arenaBytes / 2));
    framesArena_.init(size_t(eng->config().arenaBytes / 2));
    lookupScratch_.resize(kL1BlockBytes * 2);
    jobs_.reserve(4096);
    jobResults_.reserve(4096);
    jobOwners_.reserve(4096);
    headSyncs_.reserve(4096);
    dirty_.reserve(4096);
    dirtyTypes_.reserve(1024);
}

Writer::~Writer() {
    if (sc_) {
        sc_->freeAll();
        delete sc_;
    }
}

void Writer::ring() {
    seq_.fetch_add(1, std::memory_order_seq_cst);
    if (sleeping_.load(std::memory_order_seq_cst)) wakeU32(&seq_, 1);
}

void Writer::queueSync(const FileRef& f, void* owner, uint8_t ownerKind) {
    SyncJob j;
    j.io = io_.io();
    j.handle = f.handle;
    jobs_.push_back(j);
    jobOwners_.push_back({owner, ownerKind});
    ioStats_.c[size_t(f.cls)].syncs.fetch_add(1, std::memory_order_relaxed);
}

void Writer::runJobs() {
    if (jobs_.empty()) return;
    jobResults_.assign(jobs_.size(), 0);
    for (size_t i = 0; i < jobs_.size(); i++) jobs_[i].result = &jobResults_[i];
    if (eng_->config().cooperative) {
        const int saved = tHotPathDepth;
        tHotPathDepth = 0;  // host calls
        for (auto& j : jobs_) *j.result = j.io->sync(j.handle);
        tHotPathDepth = saved;
    } else {
        eng_->syncPool().runAll(id_, jobs_.data(), jobs_.size());
    }
    syncRounds_.fetch_add(1, std::memory_order_relaxed);
    // A failed sync is never retried (fsyncgate): the owner is quarantined.
    for (size_t i = 0; i < jobs_.size(); i++) {
        if (jobResults_[i] >= 0) {
            if (jobOwners_[i].kind == 3) {  // a DURABLE_CKPT head is on disk (A9)
                Partition* p = static_cast<Partition*>(jobOwners_[i].owner);
                p->lastDurableHeadNs = p->pendingDurableHeadNs;
            }
            continue;
        }
        if (jobOwners_[i].kind == 1) {
            Partition* p = static_cast<Partition*>(jobOwners_[i].owner);
            if (p->st) p->st->err = jobResults_[i];
            p->quarantined = true;
            p->ring->state.store(kRingQuarantined, std::memory_order_release);
        } else if (jobOwners_[i].kind == 2) {
            TypeOwner* t = static_cast<TypeOwner*>(jobOwners_[i].owner);
            if (t->st) t->st->err = jobResults_[i];
        } else if (jobOwners_[i].kind == 5) {
            // A8 journal: the whole round failed (never retried).
            jRoundFailed_ = true;
            jFailed_ = true;
        }
    }
    jobs_.clear();
    jobOwners_.clear();
}

void Writer::commitRound() {
    const EngineConfig& cfg = eng_->config();
    const uint64_t t0 = monoNs();
    HotPathScope hot;
    // A8 fallback: with the commit journal, the round's only fsync is the
    // journal's; the files are synced by the journal checkpoint.
    const bool journal = cfg.commitJournal;
    // Checkpoint heads written by the previous round are synced in this one.
    for (const auto& hs : headSyncs_) {
        if (hs.second == 3) queueSync(static_cast<Partition*>(hs.first)->h, hs.first, 3);
        else queueSync(static_cast<TypeOwner*>(hs.first)->h, hs.first, 4);
    }
    headSyncs_.clear();
    // Phase A: frames (d), lane frames (l), arrivals (g), merge outputs.
    for (Partition* p : dirty_) {
        Staged* st = p->st;
        st->commitStartNs = t0;
        if (!st->batch) continue;
        if (st->dBytes) {
            framedCommits_.fetch_add(1, std::memory_order_relaxed);
            int32_t rc = ensureExtent(&io_, p->d, &p->dExtent, st->dOff + st->dBytes, cfg.zeroFillStep);
            if (rc >= 0) rc = io_.write(p->d, st->frames, st->dBytes, st->dOff);
            if (rc < 0) {
                st->err = rc;
                continue;
            }
            if (!journal) queueSync(p->d, p, 1);
        }
        if (st->laneFrameBytes) {
            const int32_t rc = io_.write(p->l, st->laneFrames, st->laneFrameBytes, st->lOff);
            if (rc < 0) {
                st->err = rc;
                continue;
            }
            if (!journal) queueSync(p->l, p, 1);
        }
        // T3: the manifest a SWAP commits is durable before the batch is.
        if (st->swapCommit)
            if (const FileRef* mf = partitionCompactManifestFile(p)) queueSync(*mf, p, 1);
    }
    for (TypeOwner* t : dirtyTypes_) {
        StagedType* st = t->st;
        if (st->nArrivals) {
            FileRef& g = st->gSeal ? t->gNext : t->g;
            const int32_t rc = io_.write(g, st->arrivals, size_t(st->nArrivals) * kArrivalBytes, st->gOff);
            if (rc < 0) {
                st->err = rc;
                continue;
            }
            uint64_t& ext = st->gSeal ? t->gNextExtent : t->gExtent;
            ext = std::max<uint64_t>(ext, st->gOff + uint64_t(st->nArrivals) * kArrivalBytes);
            if (!journal) queueSync(g, t, 2);
        }
        if (st->gSeal) {
            // A15: the sealed segment's fence entry, durable before the
            // switching batch (round 2).
            const int32_t rc = io_.write(t->gFence, &st->fence, sizeof(st->fence), st->fenceOff);
            if (rc < 0) {
                st->err = rc;
                continue;
            }
            t->fenceExtent = std::max<uint64_t>(t->fenceExtent, st->fenceOff + sizeof(st->fence));
            if (!journal) queueSync(t->gFence, t, 2);
        }
    }
    commitSyncRounds_.fetch_add(jobs_.empty() ? 0 : 1, std::memory_order_relaxed);
    runJobs();  // sync round 1
    // Phase B: meta batches (partition m, type m).
    for (Partition* p : dirty_) {
        Staged* st = p->st;
        if (!st->batch || st->err) continue;
        int32_t rc = ensureExtent(&io_, p->m, &p->mExtent, st->mOff + st->batchLen, cfg.zeroFillStep);
        if (rc >= 0) rc = io_.write(p->m, st->batch, st->batchLen, st->mOff);
        if (rc < 0) {
            st->err = rc;
            continue;
        }
        if (!journal) queueSync(p->m, p, 1);
        batches_.fetch_add(1, std::memory_order_relaxed);
    }
    for (TypeOwner* t : dirtyTypes_) {
        StagedType* st = t->st;
        if (st->err) continue;
        int32_t rc = ensureExtent(&io_, t->m, &t->mExtent, st->mOff + st->batchLen, cfg.zeroFillStep);
        if (rc >= 0) rc = io_.write(t->m, st->batch, st->batchLen, st->mOff);
        if (rc < 0) {
            st->err = rc;
            continue;
        }
        t->mExtent = std::max<uint64_t>(t->mExtent, st->mOff + st->batchLen);
        if (!journal) queueSync(t->m, t, 2);
    }
    if (journal) journalAppendRound();
    commitSyncRounds_.fetch_add(jobs_.empty() ? 0 : 1, std::memory_order_relaxed);
    runJobs();  // sync round 2 (journal mode: the only one)
    if (journal) {
        if (jRoundFailed_) {
            for (Partition* p : dirty_) {
                Staged* st = p->st;
                if (!st->batch) continue;
                if (!st->err) st->err = FLATSQL_IO_ERR_IO;
                p->quarantined = true;
                p->ring->state.store(kRingQuarantined, std::memory_order_release);
            }
            for (TypeOwner* t : dirtyTypes_)
                if (!t->st->err) t->st->err = FLATSQL_IO_ERR_IO;
        } else {
            journalTrack();
        }
    }
    // Publish: heads, then acks (§6.4 steps 5-6); checkpoint heads are synced.
    const uint64_t nowNsV = monoNs();
    for (Partition* p : dirty_) {
        Staged* st = p->st;
        if (st->err) {
            if (st->err == FLATSQL_IO_ERR_NOSPACE) eng_->signalNoSpace();  // A13
            partitionRollback(this, p, st);
            if (!p->quarantined) {
                // Write errors (ENOSPC): pause the ring briefly; entries retry.
                p->ring->state.store(kRingPaused, std::memory_order_release);
            }
            continue;
        }
        const uint64_t firstPseq = st->firstPseq;
        const uint64_t lastPseq = st->nextPseq - 1;
        const bool hadBatch = st->batch != nullptr;
        const uint64_t startNs = st->commitStartNs;
        if (hadBatch) {
            // Journal mode: heads become durable at the journal checkpoint.
            const bool due = !journal && ((nowNsV - p->lastCkptNs) >= uint64_t(cfg.ckptIntervalMs) * 1000000ull ||
                                          p->metaSinceCkpt + st->batchLen >= cfg.ckptMetaBytes);
            // State first (includes a segment switch), then the head, then acks.
            Staged copy = *st;
            copy.consumed = false;
            copy.nTickets = 0;
            partitionPublish(this, p, &copy);
            // Queue consumption (kills, TOMB_RANGE cursor) happens once.
            st->killsTaken = 0;
            st->rangeStep = false;
            const int32_t hrc = partitionWriteHead(this, p, due);
            if (hrc == FLATSQL_IO_ERR_NOSPACE) {
                // A13: the batch is durable; the next commit writes the head.
                eng_->signalNoSpace();
            } else if (hrc < 0) {
                p->quarantined = true;
                p->ring->state.store(kRingQuarantined, std::memory_order_release);
            } else if (due) {
                headSyncs_.push_back({p, 3});  // synced in the next round (no third round)
                p->lastCkptNs = nowNsV;
                p->metaSinceCkpt = 0;
            }
            // Acks and tickets.
            Staged acks = *st;
            acks.batch = nullptr;
            acks.nextPseq = acks.firstPseq;
            acks.sealAfter = false;
            partitionPublish(this, p, &acks);
            if (st->sealAfter) {
                const int32_t frc = partitionEnsureFiles(this, p);
                if (frc == FLATSQL_IO_ERR_NOSPACE) {
                    eng_->signalNoSpace();  // A13: created before the next staging
                } else if (frc < 0) {
                    p->quarantined = true;
                    p->ring->state.store(kRingQuarantined, std::memory_order_release);
                }
            }
        } else {
            partitionPublish(this, p, st);
        }
        if (eng_->config().audit && hadBatch && lastPseq >= firstPseq) {
            AuditRecord ar{};
            ar.pid = p->pid;
            ar.writer = id_;
            ar.epoch = p->ownerEpoch;
            ar.osTid = osTid_;
            ar.firstPseq = firstPseq;
            ar.lastPseq = lastPseq;
            ar.startNs = startNs;
            ar.endNs = monoNs();
            eng_->auditAppend(ar);
        }
        commits_.fetch_add(1, std::memory_order_relaxed);
    }
    for (TypeOwner* t : dirtyTypes_) {
        StagedType* st = t->st;
        if (st->err) {
            typeRollback(this, t, st);
            continue;
        }
        const bool due = !journal && ((nowNsV - t->lastCkptNs) >= uint64_t(cfg.ckptIntervalMs) * 1000000ull ||
                                      t->metaSinceCkpt + st->batchLen >= cfg.ckptMetaBytes);
        typePublish(this, t, st);
        if (typeWriteHead(this, t, due) >= 0 && due) {
            headSyncs_.push_back({t, 4});
            t->lastCkptNs = nowNsV;
            t->metaSinceCkpt = 0;
        }
    }
    lastCommitNs_ = monoNs();
    iterCommit_.fetch_add(1, std::memory_order_relaxed);
    dirty_.clear();
    dirtyTypes_.clear();
}

bool Writer::iterate(bool mayWait) {
    heartbeat_.fetch_add(1, std::memory_order_relaxed);
    if (eng_->stopping()) return false;
    const uint32_t seqBefore = seq_.load(std::memory_order_acquire);
    processMailbox();
    arena_.reset();
    framesArena_.reset();
    bool any = false;
    const size_t n = owned_.size();
    const uint32_t reserve = eng_->reserveSlabs();
    for (size_t k = 0; k < n; k++) {
        Partition* p = owned_[(rr_ + k) % n];
        if (p->quarantined) continue;
        RingDesc* r = p->ring;
        if (r->state.load(std::memory_order_acquire) == kRingPaused) {
            if (monoNs() - p->lastActivityNs > 100000000ull) r->state.store(kRingActive);
            else continue;
        }
        const bool backlog = r->tail.load(std::memory_order_acquire) != r->head.load(std::memory_order_acquire);
        // Pages only for a ring with traffic or a producer asking: an idle
        // partition holds zero slabs (§17).
        if (backlog || r->wantPage.load(std::memory_order_acquire)) {
            // T3b: a split partition's producer runs ahead of the owner by the
            // whole ring, so helpers always have entries to prepare.
            const uint32_t ahead = p->hot.load(std::memory_order_relaxed)
                                       ? uint32_t((r->cap + r->maxEntry) / r->slabBytes)
                                       : (backlog ? p->mapAheadPages : 1);
            ringMapAhead(r, eng_->pool(), reserve, ahead);
        }
        if (!backlog && !p->rec.active && p->kills.empty() && p->ranges.empty() && !p->sealPending &&
            !p->nPendingCtl && p->mergePhase != kMergeOutputsWritten && !p->retireDirty && p->retiring.empty() &&
            p->unlinked.empty() && !p->forceLaneCkpt)
            continue;
        if (!p->warm && partitionWarm(this, p) < 0) {
            p->quarantined = true;
            r->state.store(kRingQuarantined, std::memory_order_release);
            continue;
        }
        if (arena_.remaining() < (1u << 20) || framesArena_.remaining() < eng_->config().maxEntryBytes + 4096)
            break;
        // A13: segment files a full disk kept from being created after a seal.
        if ((!p->m.valid() || !p->d.valid()) && partitionEnsureFiles(this, p) < 0) continue;
        if (partitionStage(this, p, sc_, &framesArena_, &arena_)) {
            dirty_.push_back(p);
            any = true;
        }
    }
    rr_++;
    for (TypeOwner* t : types_) {
        if (arena_.remaining() < (4u << 20)) break;
        if (typeStage(this, t, sc_, &framesArena_, &arena_)) {
            dirtyTypes_.push_back(t);
            any = true;
        }
    }
    if (!dirty_.empty() || !dirtyTypes_.empty()) {
        const uint64_t c0 = monoNs();
        commitRound();
        lastWorkNs_ = monoNs();
        if (eng_->config().lockStats) eng_->commitHist().record(lastWorkNs_ - c0);
    }
    const uint64_t m0 = eng_->config().lockStats ? monoNs() : 0;
    maintenance();
    if (m0) eng_->maintHist().record(monoNs() - m0);
    // T3b (§12): an idle writer prepares entries of split partitions.
    if (!any && eng_->prepHelp(this, eng_->config().prepHelpBudget) > 0) any = true;
    if (!any && mayWait && !eng_->stopping()) {
        const uint64_t idleFor = monoNs() - lastWorkNs_;
        const uint64_t timeoutNs = idleFor > 1000000000ull ? uint64_t(eng_->config().idleWaitUs) * 1000
                                                          : uint64_t(eng_->config().activeWaitUs) * 1000;
        sleeping_.store(1, std::memory_order_seq_cst);
        if (seq_.load(std::memory_order_seq_cst) == seqBefore && mailbox_.empty())
            waitU32(&seq_, seqBefore, timeoutNs);
        sleeping_.store(0, std::memory_order_relaxed);
    }
    return true;
}

void Writer::threadMain() {
    osTid_ = osThreadId();
    setThreadClass(ThreadClass::Writer);  // lock-set instrumentation (T2 #1)
    while (iterate(true)) {}
}

void Writer::processMailbox() {
    Cmd c;
    int budget = 256;
    while (budget-- > 0 && mailbox_.pop(&c)) {
        switch (c.kind) {
            case kCmdAdoptPartition: {
                Partition* p = eng_->partition(uint32_t(c.a));
                if (!p) break;
                RingDesc* r = p->ring;
                uint64_t w = r->ownerWordV.load(std::memory_order_acquire);
                if (c.b != 0) {
                    // HANDOFF -> self (A26): the previous owner has finished its
                    // last commit and released the partition.
                    if (ownerState(w) != kOwnHandoff) break;
                    const uint64_t next = ownerWord(ownerEpoch(w) + 1, id_, kOwnOwned);
                    if (!r->ownerWordV.compare_exchange_strong(w, next, std::memory_order_acq_rel)) break;
                    p->ownerEpoch = ownerEpoch(w) + 1;
                } else {
                    p->ownerEpoch = ownerEpoch(w);
                }
                p->ownerWriter.store(id_, std::memory_order_release);
                p->handoffTarget.store(0xff, std::memory_order_release);
                r->handoffTo.store(0xff, std::memory_order_release);
                owned_.push_back(p);
                ownedCount_.fetch_add(1, std::memory_order_relaxed);
                break;
            }
            case kCmdRebalance: {
                Partition* p = eng_->partition(uint32_t(c.a));
                if (!p || p->ownerWriter.load() != id_ || uint8_t(c.b) == id_) break;
                // Only while this writer still owns it: a partition released
                // here (HANDOFF) and not yet adopted also reads as ours.
                if (std::find(owned_.begin(), owned_.end(), p) == owned_.end()) break;
                if (uint32_t(c.b) >= eng_->writerCount()) break;
                releaseOwnership(p, uint8_t(c.b));
                break;
            }
            case kCmdAdoptType: {
                TypeOwner* t = static_cast<TypeOwner*>(c.ptr);
                t->ownerWriter.store(id_, std::memory_order_release);
                types_.push_back(t);
                break;
            }
            case kCmdKillCid: {
                Partition* p = eng_->partition(uint32_t(c.a));
                if (!p) {
                    if (c.ticket && c.ticket->fetch_sub(1) == 1)
                        wakeU32(reinterpret_cast<std::atomic<uint32_t>*>(c.ticket), -1);
                    break;
                }
                if (p->ownerWriter.load(std::memory_order_acquire) != id_) {
                    // The partition moved: forward to its owner.
                    Writer* o = eng_->writer(p->ownerWriter.load());
                    while (!o->mailbox().push(c)) cpuRelax();
                    o->ring();
                    break;
                }
                PendingKill k;
                std::memcpy(k.cid, c.data, kCidLen);
                k.remaining = c.ticket;
                p->kills.push_back(k);
                break;
            }
            case kCmdTombRange: {
                Partition* p = eng_->partition(uint32_t(c.a));
                if (!p) {
                    if (c.ticket && c.ticket->fetch_sub(1) == 1)
                        wakeU32(reinterpret_cast<std::atomic<uint32_t>*>(c.ticket), -1);
                    break;
                }
                if (p->ownerWriter.load(std::memory_order_acquire) != id_) {
                    Writer* o = eng_->writer(p->ownerWriter.load());
                    while (!o->mailbox().push(c)) cpuRelax();
                    o->ring();
                    break;
                }
                TombRange tr;
                tr.seg = uint32_t(c.b);
                std::memcpy(&tr.beforeMs, c.data, 8);
                std::memcpy(&tr.firstPseq, c.data + 8, 8);
                std::memcpy(&tr.endPseq, c.data + 16, 8);
                tr.remaining = c.ticket;
                p->ranges.push_back(tr);
                break;
            }
            case kCmdSwap: {
                Partition* p = eng_->partition(uint32_t(c.a));
                SwapResult* r = static_cast<SwapResult*>(c.ptr);
                if (!p) {
                    r->status = FLATSQL_IO_ERR_NOENT;
                    if (r->remaining.fetch_sub(1) == 1)
                        wakeU32(reinterpret_cast<std::atomic<uint32_t>*>(&r->remaining), -1);
                    break;
                }
                if (p->ownerWriter.load(std::memory_order_acquire) != id_) {
                    Writer* o = eng_->writer(p->ownerWriter.load());
                    while (!o->mailbox().push(c)) cpuRelax();
                    o->ring();
                    break;
                }
                p->swaps.push_back(r);
                break;
            }
            case kCmdHotSplit: {
                Partition* p = eng_->partition(uint32_t(c.a));
                if (!p) break;
                if (p->ownerWriter.load(std::memory_order_acquire) != id_) {
                    Writer* o = eng_->writer(p->ownerWriter.load());
                    while (!o->mailbox().push(c)) cpuRelax();
                    o->ring();
                    break;
                }
                p->hotForce = c.b ? 1 : 0;
                break;
            }
            case kCmdTypeDelete: {
                TypeOwner* t = static_cast<TypeOwner*>(c.ptr);
                TypeOwner::Delete d;
                std::memcpy(d.cid, c.data, kCidLen);
                d.remaining = c.ticket;
                t->deletes.push_back(d);
                t->dirty.store(1);
                break;
            }
            default:
                break;
        }
    }
}

void Writer::flushHeadSyncs() {
    for (const auto& hs : headSyncs_) {
        if (hs.second == 3) queueSync(static_cast<Partition*>(hs.first)->h, hs.first, 3);
        else queueSync(static_cast<TypeOwner*>(hs.first)->h, hs.first, 4);
    }
    headSyncs_.clear();
    runJobs();
}

void Writer::releaseOwnership(Partition* p, uint8_t target) {
    // At an iteration boundary: the partition has no staged batch (A26).
    // HANDOFF aborts in-flight maintenance; its INTENT is discarded as at open.
    partitionMergeAbort(this, p);
    partitionCompactAbort(this, p);
    flushHeadSyncs();
    RingDesc* r = p->ring;
    const uint64_t w = r->ownerWordV.load(std::memory_order_acquire);
    p->handoffTarget.store(target, std::memory_order_release);
    r->handoffTo.store(target, std::memory_order_release);
    r->ownerWordV.store(ownerWord(ownerEpoch(w), id_, kOwnHandoff), std::memory_order_release);
    partitionCool(this, p);
    // The head handle goes too: the new owner opens its own.
    io_.close(&p->h);
    owned_.erase(std::remove(owned_.begin(), owned_.end(), p), owned_.end());
    ownedCount_.fetch_sub(1, std::memory_order_relaxed);
    pinned_.fetch_sub(1, std::memory_order_relaxed);
    Writer* t = eng_->writer(target);
    t->pinned_.fetch_add(1, std::memory_order_relaxed);
    Cmd c;
    c.kind = kCmdAdoptPartition;
    c.a = p->pid;
    c.b = 1;
    while (!t->mailbox().push(c)) cpuRelax();
    t->ring();
}

void Writer::maintenance() {
    const EngineConfig& cfg = eng_->config();
    if (cfg.commitJournal) journalMaintenance(false);
    const int64_t nowMsV = eng_->nowMs();
    const uint64_t nowNsV = monoNs();
    const size_t n = owned_.size();
    // A13: in a space emergency a compaction the quota planner did not request
    // does not start (one in flight finishes). Its outputs would spend the
    // ballast's room, and a build that fails NOSPACE starts over with a new
    // intent in the meta log each time: T3 #6 filled the device that way and
    // stayed in its first emergency for good. Merges are gated likewise
    // (partitionWantsMerge).
    const bool emergency = eng_->spaceEmergency();
    // Merge pipeline steps (bounded: a few output builds per iteration).
    int outputs = 0;
    for (size_t k = 0; k < n && outputs < 4; k++) {
        Partition* p = owned_[(maintRr_ + k) % n];
        int32_t rc = partitionMergeStep(this, p);
        if (rc >= 0 && (!emergency || p->compactPhase != kCompactIdle || !p->swaps.empty())) {
            const int32_t crc = partitionCompactStep(this, p);
            rc = crc < 0 ? crc : rc + crc;
        }
        if (rc >= 0) {
            const int32_t rrc = partitionReclaimStep(this, p);
            if (rrc < 0) rc = rrc;
        }
        if (rc >= 0) partitionRetireMetaStep(this, p);
        if (rc == FLATSQL_IO_ERR_NOSPACE) {
            // A13: a merge or compaction that found no room is abandoned and
            // retried; the engine frees space first.
            eng_->signalNoSpace();
        } else if (rc < 0) {
            p->quarantined = true;
            p->ring->state.store(kRingQuarantined, std::memory_order_release);
        } else if (rc > 0) {
            outputs++;
        }
    }
    maintRr_++;
    for (TypeOwner* t : types_) {
        typeMergeStep(this, t);
        typeReclaimStep(this, t);
    }
    if (id_ == 0) quotaStep(this);  // §13: the planner runs on writer 0
    // Checkpoint heads waiting for a round that is not coming: sync now.
    if (!headSyncs_.empty() && nowNsV - lastCommitNs_ > 2000000ull) flushHeadSyncs();
    for (Partition* p : owned_) {
        hotSplitStep(this, p, nowNsV);  // T3b (§12)
        if (p->quarantined) continue;
        // Seal by age (§4.2) and next-segment pre-creation at 50%.
        if (!p->sealPending && (p->dLen > 0 || p->segRecords > 0) && cfg.sealAgeMs > 0 && p->segOpenedMs &&
            nowMsV - p->segOpenedMs >= cfg.sealAgeMs)
            p->sealPending = true;
        if (!p->precreated && p->warm && p->dLen >= cfg.sealBytes / 2) partitionPrecreate(this, p);
        // Checkpoint heads that went idle before their interval elapsed.
        if (p->metaSinceCkpt && p->h.valid() &&
            nowNsV - p->lastCkptNs >= uint64_t(cfg.ckptIntervalMs) * 1000000ull) {
            if (partitionWriteHead(this, p, true) >= 0) {
                queueSync(p->h, p, 3);
                p->lastCkptNs = nowNsV;
                p->metaSinceCkpt = 0;
            }
        }
        // Idle partitions give back ring slabs, then handles and accelerators.
        const uint64_t idle = nowNsV - p->lastActivityNs;
        if (idle >= uint64_t(cfg.idleReclaimMs) * 1000000ull) ringReclaimIdle(p->ring, eng_->pool());
        if (p->warm && !p->rec.active && p->mergePhase == kMergeIdle && !p->nPendingCtl &&
            !partitionCompactBusy(p) && p->retired.empty() && idle >= uint64_t(cfg.idleCloseMs) * 1000000ull &&
            p->ring->tail.load() == p->ring->head.load())
            partitionCool(this, p);
    }
    for (TypeOwner* t : types_) {
        if (t->metaSinceCkpt && t->h.valid() &&
            nowNsV - t->lastCkptNs >= uint64_t(cfg.ckptIntervalMs) * 1000000ull) {
            if (typeWriteHead(this, t, true) >= 0) {
                queueSync(t->h, t, 4);
                t->lastCkptNs = nowNsV;
                t->metaSinceCkpt = 0;
            }
        }
    }
    runJobs();
}

// ---------------------------------------------------------------------------
// Engine (runtime half; open lives in open.cpp)
// ---------------------------------------------------------------------------
int64_t Engine::wallMsNow() { return wallMs(); }

Engine::~Engine() {
    if (!closed_) stop(0);
    for (auto& p : partStore_) {
        if (!p) continue;
        ringDestroy(p->ring);
        p->ring = nullptr;
    }
    for (auto& t : typeStore_)
        for (uint32_t c = 0; c < TypeOwner::kMaxChunks; c++) delete[] t->partChunks[c].load();
}

uint8_t Engine::leastLoadedWriter() const {
    uint8_t best = 0;
    uint32_t bestLoad = UINT32_MAX;
    for (size_t i = 0; i < writers_.size(); i++) {
        const uint32_t l = writers_[i]->pinned_.load(std::memory_order_relaxed);
        if (l < bestLoad) {
            bestLoad = l;
            best = uint8_t(i);
        }
    }
    return best;
}

void Engine::typeAddPartition(TypeOwner* t, Partition* p) {
    const uint32_t i = t->nParts.load(std::memory_order_relaxed);
    const uint32_t c = i / TypeOwner::kChunk;
    if (c >= TypeOwner::kMaxChunks) return;
    Partition** chunk = t->partChunks[c].load(std::memory_order_relaxed);
    if (!chunk) {
        chunk = new Partition*[TypeOwner::kChunk]();
        t->partChunks[c].store(chunk, std::memory_order_release);
    }
    chunk[i % TypeOwner::kChunk] = p;
    t->nParts.store(i + 1, std::memory_order_release);
}

void Engine::submitMaintenance(bool urgent, std::function<void(IoCtx*)> job) {
    if (urgent && urgentThread_.joinable()) {
        {
            std::lock_guard<std::mutex> g(urgentMu_);
            urgentJobs_.push_back(std::move(job));
        }
        urgentCv_.notify_one();
        return;
    }
    {
        std::lock_guard<std::mutex> g(helperMu_);
        if (urgent) helperJobs_.push_front(std::move(job));
        else helperJobs_.push_back(std::move(job));
    }
    helperCv_.notify_one();
}

void Engine::stopUrgentThread() {
    if (!urgentThread_.joinable()) return;
    {
        std::lock_guard<std::mutex> g(urgentMu_);
        urgentStop_ = true;
    }
    urgentCv_.notify_all();
    urgentThread_.join();  // drains the queue first
}

void Engine::submitCompaction(std::function<void(IoCtx*)> job) {
    {
        std::lock_guard<std::mutex> g(compactMu_);
        compactJobs_.push_back(std::move(job));
    }
    compactCv_.notify_one();
}

void Engine::stopCompactThreads() {
    // Plans in flight stop at their next slice.
    for (auto& w : writers_)
        for (Partition* p : w->owned_)
            partitionCompactSignalAbort(p);
    {
        std::lock_guard<std::mutex> g(compactMu_);
        compactStop_ = true;
    }
    compactCv_.notify_all();
    for (auto& t : compactThreads_)
        if (t.joinable()) t.join();  // drains the queue first
    compactThreads_.clear();
}

// Returns once no gate call can still use the previous (fn, ctx): the caller
// may then destroy what ctx points at (a reader instance going away). A gate
// call that started before the swap finishes first (they are short: a scan of
// the lanes' announce words); one that starts after sees the new pair.
void Engine::setReaderGate(uint64_t (*fn)(void*), void* ctx) {
    std::lock_guard<std::mutex> g(readerGateMu_);
    readerGate_.store(nullptr, std::memory_order_seq_cst);
    while (readerGateCalls_.load(std::memory_order_seq_cst) != 0) cpuRelax();
    readerGateCtx_.store(ctx, std::memory_order_seq_cst);
    readerGate_.store(fn, std::memory_order_seq_cst);
}

uint64_t Engine::readerGateNs() const {
    readerGateCalls_.fetch_add(1, std::memory_order_seq_cst);
    uint64_t (*fn)(void*) = readerGate_.load(std::memory_order_seq_cst);
    uint64_t v;
    if (fn) {
        v = fn(readerGateCtx_.load(std::memory_order_seq_cst));
    } else if (!hostGateSet_.load(std::memory_order_acquire)) {
        readerGateCalls_.fetch_sub(1, std::memory_order_seq_cst);
        return UINT64_MAX;
    } else {
        v = hostGate_.load(std::memory_order_acquire);
    }
    readerGateCalls_.fetch_sub(1, std::memory_order_seq_cst);
    // Nothing running: every retirement so far is past the gate.
    return v == UINT64_MAX ? monoNs() : v;
}

uint64_t Engine::partitionDiskBytes(uint32_t pid) const {
    const Partition* p = partition(pid);
    return p ? p->diskBytesPub.load(std::memory_order_relaxed) : 0;
}

uint64_t Engine::typeDiskBytes(uint64_t* retired) const {
    // typeStore_ grows under the registration lock (maintenance, never a
    // record path).
    std::lock_guard<std::mutex> g(const_cast<std::mutex&>(regMutex_));
    uint64_t b = 0, r = 0;
    for (const auto& t : typeStore_) {
        b += t->diskBytesPub.load(std::memory_order_relaxed);
        r += t->retiredBytesPub.load(std::memory_order_relaxed);
    }
    if (retired) *retired = r;
    return b;
}

uint64_t Engine::typeDiskBytesOf(const uint8_t fid[4]) const {
    const TypeOwner* t = type(fid);
    return t ? t->diskBytesPub.load(std::memory_order_relaxed) : 0;
}

uint64_t Engine::journalSafeNs() const {
    uint64_t safe = UINT64_MAX;
    for (const auto& w : writers_) {
        const uint64_t s = w->jSafeNs_.load(std::memory_order_acquire);
        if (s < safe) safe = s;
    }
    return safe == UINT64_MAX ? monoNs() : safe;
}

bool Engine::submitCheckpoint(std::function<void(IoCtx*)> job) {
    if (!ckptActive_.load(std::memory_order_acquire)) return false;
    {
        std::lock_guard<std::mutex> g(ckptMu_);
        ckptJobs_.push_back(std::move(job));
    }
    ckptCv_.notify_one();
    return true;
}

void Engine::stopCheckpointThread() {
    if (!ckptThread_.joinable()) return;
    {
        std::lock_guard<std::mutex> g(ckptMu_);
        ckptStop_ = true;
    }
    ckptCv_.notify_all();
    ckptThread_.join();  // drains the queue first
    ckptActive_.store(false, std::memory_order_release);
}

int32_t Engine::start() {
    if (cfg_.cooperative || started_) return 0;
    helperStop_ = false;
    if (cfg_.mergeHelpers) {
        urgentStop_ = false;
        if (!urgentIo_) urgentIo_.reset(new IoCtx(cfg_.io ? cfg_.io : importIo(), &urgentIoStats_));
        urgentThread_ = std::thread([this] {
            for (;;) {
                std::function<void(IoCtx*)> job;
                {
                    std::unique_lock<std::mutex> lk(urgentMu_);
                    urgentCv_.wait(lk, [this] { return urgentStop_ || !urgentJobs_.empty(); });
                    if (urgentJobs_.empty()) return;  // stopping, queue drained
                    job = std::move(urgentJobs_.front());
                    urgentJobs_.pop_front();
                }
                job(urgentIo_.get());
            }
        });
    }
    if (cfg_.commitJournal) {
        ckptStop_ = false;
        ckptActive_.store(true, std::memory_order_release);
        ckptThread_ = std::thread([this] {
            for (;;) {
                std::function<void(IoCtx*)> job;
                {
                    std::unique_lock<std::mutex> lk(ckptMu_);
                    ckptCv_.wait(lk, [this] { return ckptStop_ || !ckptJobs_.empty(); });
                    if (ckptJobs_.empty()) return;  // stopping, queue drained
                    job = std::move(ckptJobs_.front());
                    ckptJobs_.pop_front();
                }
                job(helperIo_.get());
            }
        });
    }
    compactStop_ = false;
    for (uint32_t i = 0; i < cfg_.compactThreads; i++) {
        compactIo_.emplace_back(new IoCtx(cfg_.io ? cfg_.io : importIo(), &compactIoStats_));
        IoCtx* cio = compactIo_.back().get();
        compactThreads_.emplace_back([this, cio] {
            for (;;) {
                std::function<void(IoCtx*)> job;
                {
                    std::unique_lock<std::mutex> lk(compactMu_);
                    compactCv_.wait(lk, [this] { return compactStop_ || !compactJobs_.empty(); });
                    if (compactJobs_.empty()) return;  // stopping, queue drained
                    job = std::move(compactJobs_.front());
                    compactJobs_.pop_front();
                }
                job(cio);
            }
        });
    }
    for (uint32_t i = 0; i < cfg_.mergeHelpers; i++) {
        helpers_.emplace_back([this] {
            for (;;) {
                std::function<void(IoCtx*)> job;
                {
                    std::unique_lock<std::mutex> lk(helperMu_);
                    helperCv_.wait(lk, [this] { return helperStop_ || !helperJobs_.empty(); });
                    if (helperJobs_.empty()) return;  // stopping, queue drained
                    job = std::move(helperJobs_.front());
                    helperJobs_.pop_front();
                }
                job(helperIo_.get());
            }
        });
    }
    const uint32_t syncThreads =
        cfg_.syncThreads ? cfg_.syncThreads : std::min<uint32_t>(8, std::max<uint32_t>(4, cfg_.writers));
    syncPool_.start(syncThreads);
    for (auto& w : writers_) {
        Writer* wp = w.get();
        wp->thread_ = std::thread([wp] { wp->threadMain(); });
    }
    started_ = true;
    return 0;
}

int32_t Engine::stop(uint64_t deadlineMs) {
    (void)deadlineMs;
    if (closed_) return 0;
    closed_ = true;
    stop_.store(true, std::memory_order_release);
    for (auto& w : writers_) w->ring();
    for (auto& w : writers_)
        if (w->thread_.joinable()) w->thread_.join();
    {
        std::lock_guard<std::mutex> g(helperMu_);
        helperStop_ = true;
    }
    helperCv_.notify_all();
    for (auto& h : helpers_)
        if (h.joinable()) h.join();
    helpers_.clear();
    stopCompactThreads();
    stopUrgentThread();
    stopCheckpointThread();
    syncPool_.stop();
    started_ = false;
    // Merges and compactions still in flight are abandoned: their outputs go
    // now, and open discards any INTENT without MERGE_DONE or SWAP.
    for (auto& w : writers_) {
        for (Partition* p : w->owned_) {
            partitionMergeAbort(w.get(), p);
            partitionCompactAbort(w.get(), p);
        }
        for (TypeOwner* t : w->types_) typeMergeStop(w.get(), t);
    }
    if (!writers_.empty()) quotaClose(this, &writers_[0]->io());
    // Clean shutdown: durable heads, so the next open reads no tail.
    for (auto& w : writers_) w->flushHeadSyncs();
    for (auto& w : writers_) {
        for (Partition* p : w->owned_) {
            if (p->metaSinceCkpt && p->h.valid() && !p->quarantined) {
                if (partitionWriteHead(w.get(), p, true) >= 0) w->io().sync(p->h);
                p->metaSinceCkpt = 0;
            }
        }
        for (TypeOwner* t : w->types_) {
            if (t->metaSinceCkpt && t->h.valid()) {
                if (typeWriteHead(w.get(), t, true) >= 0) w->io().sync(t->h);
                t->metaSinceCkpt = 0;
            }
        }
    }
    // A8 journal: every journaled byte into durable files, journals emptied,
    // so the next open replays nothing.
    for (auto& w : writers_) w->journalMaintenance(true);
    hotClose();  // T3b: helpers are stopped; their run handles close here
    for (auto& w : writers_) {
        w->io().close(&w->jf_[0]);
        w->io().close(&w->jf_[1]);
    }
    for (auto& w : writers_) {
        for (Partition* p : w->owned_) {
            partitionCool(w.get(), p);
            w->io().close(&p->h);
        }
        for (TypeOwner* t : w->types_) {
            w->io().close(&t->h);
            w->io().close(&t->m);
            w->io().close(&t->g);
            w->io().close(&t->gNext);
            w->io().close(&t->gFence);
            for (auto& kv : t->partM) w->io().close(&kv.second);
            t->partM.clear();
            for (auto& r : t->runs) w->io().close(&r.file);
            t->chain.freeAll(pool_);
        }
    }
    registry_.close();
    return 0;
}

void Engine::abandon() {
    if (closed_) return;
    closed_ = true;
    stop_.store(true, std::memory_order_release);
    for (auto& w : writers_) w->ring();
    for (auto& w : writers_)
        if (w->thread_.joinable()) w->thread_.join();
    {
        std::lock_guard<std::mutex> g(helperMu_);
        helperStop_ = true;
    }
    helperCv_.notify_all();
    for (auto& h : helpers_)
        if (h.joinable()) h.join();
    helpers_.clear();
    stopCompactThreads();
    stopUrgentThread();
    stopCheckpointThread();
    syncPool_.stop();
    started_ = false;
    hotClose();  // T3b: helpers are stopped; their run handles close here
    for (auto& w : writers_) {
        w->io().close(&w->jf_[0]);
        w->io().close(&w->jf_[1]);
    }
    for (auto& w : writers_) {
        for (Partition* p : w->owned_) {
            partitionCool(w.get(), p);
            w->io().close(&p->h);
        }
        for (TypeOwner* t : w->types_) {
            w->io().close(&t->h);
            w->io().close(&t->m);
            w->io().close(&t->g);
            w->io().close(&t->gNext);
            w->io().close(&t->gFence);
            for (auto& kv : t->partM) w->io().close(&kv.second);
            t->partM.clear();
            for (auto& r : t->runs) w->io().close(&r.file);
        }
    }
    registry_.close();
}

int32_t Engine::pump(uint64_t budgetUs) {
    const uint64_t deadline = monoNs() + budgetUs * 1000;
    do {
        for (auto& w : writers_) w->iterate(false);
    } while (monoNs() < deadline && budgetUs > 0 && false);
    return 0;
}

Partition* Engine::partition(uint32_t pid) const {
    if (pid == 0 || pid >= partsCap_) return nullptr;
    return parts_[pid].load(std::memory_order_acquire);
}

RingDesc* Engine::ring(uint32_t pid) const {
    Partition* p = partition(pid);
    return p ? p->ring : nullptr;
}

TypeOwner* Engine::type(const uint8_t fid[4]) const {
    auto it = typeByFid_.find(fidU32(fid));
    return it == typeByFid_.end() ? nullptr : it->second;
}

void Engine::ringOwner(uint32_t pid) {
    Partition* p = partition(pid);
    if (!p) return;
    writers_[p->ownerWriter.load(std::memory_order_acquire) % writers_.size()]->ring();
    const uint32_t h = p->handoffTarget.load(std::memory_order_acquire);
    if (h != 0xff && h < writers_.size()) writers_[h]->ring();  // A24: ring both during HANDOFF
}

int32_t Engine::rebalance(uint32_t pid, uint8_t to) {
    Partition* p = partition(pid);
    if (!p || to >= writers_.size()) return FLATSQL_IO_ERR_GENERIC;
    Cmd c;
    c.kind = kCmdRebalance;
    c.a = pid;
    c.b = to;
    Writer* w = writers_[p->ownerWriter.load() % writers_.size()].get();
    if (!w->mailbox().push(c)) return FLATSQL_IO_ERR_BUSY;
    w->ring();
    return 0;
}

int32_t Engine::tombRange(uint32_t pid, uint32_t seg, int64_t beforeMs, std::atomic<int32_t>* remaining,
                          uint64_t firstPseq, uint64_t endPseq) {
    Partition* p = partition(pid);
    if (!p) return FLATSQL_IO_ERR_NOENT;
    if (remaining) remaining->store(1, std::memory_order_release);
    Cmd c;
    c.kind = kCmdTombRange;
    c.a = pid;
    c.b = seg;
    c.ticket = remaining;
    std::memcpy(c.data, &beforeMs, 8);
    std::memcpy(c.data + 8, &firstPseq, 8);
    std::memcpy(c.data + 16, &endPseq, 8);
    Writer* w = writers_[p->ownerWriter.load() % writers_.size()].get();
    if (!w->mailbox().push(c)) return FLATSQL_IO_ERR_BUSY;
    w->ring();
    return 0;
}

int32_t Engine::swapSegment(uint32_t pid, SwapResult* r) {
    Partition* p = partition(pid);
    if (!p || !r) return FLATSQL_IO_ERR_NOENT;
    r->remaining.store(1, std::memory_order_release);
    r->status = 0;
    Cmd c;
    c.kind = kCmdSwap;
    c.a = pid;
    c.ptr = r;
    Writer* w = writers_[p->ownerWriter.load() % writers_.size()].get();
    if (!w->mailbox().push(c)) return FLATSQL_IO_ERR_BUSY;
    w->ring();
    return 0;
}

int32_t Engine::deleteCid(const uint8_t fid[4], const uint8_t cid[kCidLen], std::atomic<int32_t>* remaining) {
    TypeOwner* t = type(fid);
    if (!t) return FLATSQL_IO_ERR_NOENT;
    if (remaining) remaining->store(1, std::memory_order_release);
    Cmd c;
    c.kind = kCmdTypeDelete;
    c.ptr = t;
    c.ticket = remaining;
    std::memcpy(c.data, cid, kCidLen);
    Writer* w = writers_[t->ownerWriter.load() % writers_.size()].get();
    if (!w->mailbox().push(c)) return FLATSQL_IO_ERR_BUSY;
    w->ring();
    return 0;
}

EngineStats Engine::stats() const {
    EngineStats s;
    for (const auto& w : writers_) {
        s.commits += w->commits();
        s.syncRounds += w->syncRounds();
        s.iterationsWithCommit += w->iterationsWithCommit();
        s.commitSyncRounds += w->commitSyncRounds();
        s.partitionCommitsWithFrames += w->partitionCommitsWithFrames();
        s.partitionBatches += w->partitionBatches();
        s.arenaHighWater += w->arena_.highWater() + w->framesArena_.highWater();
    }
    s.rowsAppended = cRows.load();
    s.dedupeHits = cDedupe.load();
    s.retags = cRetags.load();
    s.tombs = cTombs.load();
    s.rejects = cRejects.load();
    s.merges = cMerges.load();
    s.seals = cSeals.load();
    s.typeCommits = cTypeCommits.load();
    s.firstLabels = cFirst.load();
    s.repeatLabels = cRepeat.load();
    s.promotions = cPromotions.load();
    s.migratedGseqs = cMigratedGseq.load();
    s.migratedGseqFallbacks = cMigratedGseqFallback.load();
    s.mergeNotOwner = cMergeNotOwner.load();
    s.helperStalls = cHelperStalls.load();
    s.handoffHelperWaits = cHandoffHelperWaits.load();
    for (const auto& w : writers_) {
        s.journalRecords += w->jRecords_.load();
        s.journalBytes += w->jBytes_.load();
        s.journalCheckpoints += w->jCheckpoints_.load();
    }
    s.journalReplayRecords = journalReplayRecords;
    s.openJournalBytes = openIoStats_.readBytes(FileClass::Journal);
    {
        // typeStore_ grows under the registration lock.
        std::lock_guard<std::mutex> g(const_cast<std::mutex&>(regMutex_));
        for (const auto& t : typeStore_) {
            s.noticesDropped += t->noticesDropped.load();
            s.typeDiskBytes += t->diskBytesPub.load(std::memory_order_relaxed);
        }
    }
    s.framesParsedAtOpen = framesParsedAtOpen;
    s.openReadBytes = openIoStats_.totalReadBytes();
    s.openDataBytes = openIoStats_.readBytes(FileClass::Data);
    s.openMetaBytes = openIoStats_.readBytes(FileClass::Meta);
    s.openSyncs = openIoStats_.totalSyncs();
    for (size_t k = 0; k < size_t(FileClass::Count); k++)
        s.openWriteBytes += openIoStats_.writeBytes(FileClass(k));
    s.adoptedBatches = adoptedBatches;
    s.poolSlabsInUse = pool_.inUse();
    s.poolSlabsPeak = pool_.peakInUse();
    s.poolCommittedBytes = pool_.committedBytes();
    // Only published values: writers own the partition structures.
    uint64_t desc = 0, acc = 0;
    const uint32_t maxPid = maxPidPub_.load(std::memory_order_acquire);
    for (uint32_t pid = 1; pid <= maxPid && pid < partsCap_; pid++) {
        const Partition* p = parts_[pid].load(std::memory_order_acquire);
        if (!p) continue;
        desc += sizeof(Partition) + ringDescBytes(p->ring->nSlots) +
                uint64_t(p->laneCount.load(std::memory_order_relaxed)) * sizeof(Lane) +
                p->laneCutBytes.load(std::memory_order_relaxed);
        acc += p->accelBytes.load(std::memory_order_relaxed);
    }
    s.descriptorBytes = desc;
    s.acceleratorBytes = acc;
    for (uint32_t pid = 1; pid <= maxPid && pid < partsCap_; pid++) {
        const Partition* p = parts_[pid].load(std::memory_order_acquire);
        if (p) s.diskBytes += p->diskBytesPub.load(std::memory_order_relaxed);
    }
    s.compactions = cCompactions.load();
    s.compactAborts = cCompactAborts.load();
    s.compactBytesIn = cCompactBytesIn.load();
    s.compactBytesOut = cCompactBytesOut.load();
    s.retiredFiles = cRetired.load();
    s.unlinkedFiles = cUnlinked.load();
    s.unlinkBusy = cUnlinkBusy.load();
    s.metaSegsRetired = cMetaRetired.load();
    s.catalogEntriesDropped = cCatalogDropped.load();
    s.compactInFlight = cCompactInFlight.load();
    s.splits = cSplits.load();
    s.unsplits = cUnsplits.load();
    s.prepHelperStalls = cPrepStalls.load();
    s.l0FullStalls = cL0Full.load();
    s.arrivalSegsCompacted = cArrSegsCompacted.load();
    s.arrivalEntriesDropped = cArrDropped.load();
    uint64_t prepBytes = 0;
    for (uint32_t pid = 1; pid <= maxPid && pid < partsCap_; pid++) {
        const Partition* p = parts_[pid].load(std::memory_order_acquire);
        const PrepWindow* pw = p ? p->prepPub.load(std::memory_order_acquire) : nullptr;
        if (!pw) continue;
        prepBytes += sizeof(PrepWindow);
        s.prepPrepared += pw->prepared.load(std::memory_order_relaxed);
        s.prepUsed += pw->used.load(std::memory_order_relaxed);
        s.prepStolen += pw->stolen.load(std::memory_order_relaxed);
        s.prepWasted += pw->wasted.load(std::memory_order_relaxed);
        s.prepHinted += pw->hinted.load(std::memory_order_relaxed);
    }
    // Engine-accounted committed memory of the writer instance: touched pool
    // slabs, arenas and scratch, descriptors and L1 accelerators.
    uint64_t scratch = 0;
    for (const auto& w : writers_) {
        scratch += w->arena_.capacity() + w->framesArena_.capacity();
        const StageScratch* sc = w->sc_;
        scratch += uint64_t(sc->capRows) * sizeof(RecRow) + sc->capAttrs +
                   uint64_t(sc->capEntries) * (sizeof(StagedEntry) + 2 * sizeof(void*) + sizeof(SortKey)) + sc->capKeys +
                   sc->capPlain + sc->capExtract + sc->capSection;
    }
    s.committedBytes = s.poolCommittedBytes + scratch + s.descriptorBytes + s.acceleratorBytes +
                       uint64_t(partsCap_) * sizeof(void*) + prepBytes;
    return s;
}

void Engine::totalIo(IoStats* out) const {
    IoStats& total = *out;
    total.reset();
    auto add = [&](const IoStats& s) {
        for (size_t i = 0; i < size_t(FileClass::Count); i++) {
            total.c[i].readBytes += s.c[i].readBytes.load();
            total.c[i].readCalls += s.c[i].readCalls.load();
            total.c[i].writeBytes += s.c[i].writeBytes.load();
            total.c[i].writeCalls += s.c[i].writeCalls.load();
            total.c[i].syncs += s.c[i].syncs.load();
            total.c[i].opens += s.c[i].opens.load();
            total.c[i].truncates += s.c[i].truncates.load();
        }
    };
    add(openIoStats_);
    add(helperIoStats_);
    add(urgentIoStats_);
    for (const auto& w : writers_) add(w->ioStats_);
}

std::vector<AuditRecord> Engine::auditLog() const {
    std::lock_guard<std::mutex> g(auditMutex_);
    return audit_;
}

void Engine::auditAppend(const AuditRecord& r) {
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;  // test instrumentation, not the record path
    std::lock_guard<std::mutex> g(auditMutex_);
    if (audit_.size() < cfg_.auditCapacity) audit_.push_back(r);
    tHotPathDepth = saved;
}

// ---------------------------------------------------------------------------
// Producer (the router's half)
// ---------------------------------------------------------------------------
Producer::Producer(Engine* eng, uint32_t pid) : eng_(eng), pid_(pid), ring_(eng->ring(pid)) {}

uint64_t Producer::credits() const {
    if (!ring_) return 0;
    if (ring_->state.load(std::memory_order_acquire) != kRingActive) return 0;
    const uint64_t used = ring_->used();
    const uint64_t ringFree = used >= ring_->cap ? 0 : ring_->cap - used;
    const uint64_t poolFree = uint64_t(eng_->pool().freeCount() > eng_->reserveSlabs()
                                           ? eng_->pool().freeCount() - eng_->reserveSlabs()
                                           : 0) *
                              ring_->slabBytes;
    // Mapped-but-unused pages of this ring are credit too.
    return std::min(ringFree, poolFree + uint64_t(ring_->mappedPages.load()) * ring_->slabBytes);
}

int32_t Producer::enqueue(uint16_t kind, uint16_t flags, int64_t arrivalMs, const uint8_t* cid,
                          const uint8_t* attr, uint32_t attrLen, const uint8_t* frame,
                          uint32_t frameLen, uint64_t* rseq, bool wait, const uint8_t* sealed,
                          uint32_t sealedLen) {
    if (!ring_) return FLATSQL_IO_ERR_BADHANDLE;
    RingDesc* r = ring_;
    const uint64_t raw = sizeof(EntryHeader) + uint64_t(attrLen) + frameLen +
                         ((flags & kEntSealed) ? 4 + uint64_t(sealedLen) : 0);
    const uint64_t len = (raw + 7) & ~uint64_t(7);
    if (len > r->maxEntry) return FLATSQL_IO_ERR_GENERIC;
    const uint32_t S = r->slabBytes;
    for (;;) {
        const uint32_t st = r->state.load(std::memory_order_acquire);
        if (st == kRingQuarantined) return FLATSQL_IO_ERR_ACCESS;
        r->prodBusy.fetch_add(1, std::memory_order_seq_cst);
        if (r->reclaim.load(std::memory_order_seq_cst)) {
            r->prodBusy.fetch_sub(1, std::memory_order_seq_cst);
            cpuRelax();
            continue;
        }
        const uint64_t tail = r->tail.load(std::memory_order_relaxed);
        const uint64_t head = r->head.load(std::memory_order_acquire);
        const uint64_t used = tail - head;
        bool ok = st == kRingActive && (used == 0 || used + len <= r->cap);
        bool mapped = true;
        if (ok) {
            for (uint64_t p = tail / S; p <= (tail + len - 1) / S; p++) {
                const uint64_t v = r->pages()[p % r->nSlots].load(std::memory_order_acquire);
                if ((v >> 32) != p + 1) {
                    mapped = false;
                    r->wantPage.store(uint32_t((tail + len - 1) / S + 1), std::memory_order_release);
                    break;
                }
            }
        }
        if (ok && mapped) {
            EntryHeader h{};
            h.entryLen = uint32_t(len);
            h.kind = kind;
            h.flags = flags;
            h.rseq = r->nextRseq.fetch_add(1, std::memory_order_relaxed);
            h.arrivalMs = arrivalMs;
            if (cid) std::memcpy(h.cid, cid, kCidLen);
            h.attrLen = attrLen;
            h.frameLen = frameLen;
            const SlabPool& pool = eng_->pool();
            uint64_t pos = tail;
            ringWrite(r, pool, pos, &h, sizeof(h));
            pos += sizeof(h);
            if (attrLen) ringWrite(r, pool, pos, attr, attrLen);
            pos += attrLen;
            if (frameLen) ringWrite(r, pool, pos, frame, frameLen);
            pos += frameLen;
            if (flags & kEntSealed) {
                uint8_t b[4];
                putU32(b, sealedLen);
                ringWrite(r, pool, pos, b, 4);
                pos += 4;
                if (sealedLen) ringWrite(r, pool, pos, sealed, sealedLen);
                pos += sealedLen;
            }
            static const uint8_t zeros[8] = {0};
            if (tail + len > pos) ringWrite(r, pool, pos, zeros, size_t(tail + len - pos));
            r->tail.store(tail + len, std::memory_order_release);
            r->prodBusy.fetch_sub(1, std::memory_order_seq_cst);
            eng_->ringOwner(pid_);
            if (rseq) *rseq = h.rseq;
            return 0;
        }
        r->prodBusy.fetch_sub(1, std::memory_order_seq_cst);
        eng_->ringOwner(pid_);
        creditWaits_++;
        if (!wait) return FLATSQL_IO_ERR_BUSY;
        // Zero credits: wait for an ack (space) or a mapping (pages).
        r->prodWaiting.store(1, std::memory_order_release);
        const uint32_t g = mapped ? r->ackGen.load(std::memory_order_acquire)
                                  : r->mapGen.load(std::memory_order_acquire);
        if (eng_->config().cooperative) {
            eng_->pump(0);
            continue;
        }
        waitU32(mapped ? &r->ackGen : &r->mapGen, g, 1000000);
    }
}

bool Producer::acked(uint64_t rseq) const {
    return ring_ && ring_->ackedRseq.load(std::memory_order_acquire) >= rseq;
}

int32_t Producer::waitAcked(uint64_t rseq, uint64_t timeoutNs) {
    const uint64_t deadline = monoNs() + timeoutNs;
    RingDesc* r = ring_;
    for (;;) {
        if (r->ackedRseq.load(std::memory_order_acquire) >= rseq) return 0;
        if (r->state.load(std::memory_order_acquire) == kRingQuarantined) return FLATSQL_IO_ERR_ACCESS;
        if (eng_->config().cooperative) {
            eng_->pump(0);
            if (monoNs() > deadline) return FLATSQL_IO_ERR_BUSY;
            continue;
        }
        r->prodWaiting.store(1, std::memory_order_release);
        const uint32_t g = r->ackGen.load(std::memory_order_acquire);
        if (r->ackedRseq.load(std::memory_order_acquire) >= rseq) return 0;
        const uint64_t now = monoNs();
        if (now >= deadline) return FLATSQL_IO_ERR_BUSY;
        const uint64_t left = deadline - now;
        waitU32(&r->ackGen, g, left < 10000000 ? left : 10000000);
    }
}

int32_t Producer::rejectCode(uint64_t rseq) {
    RingDesc* r = ring_;
    uint64_t t = r->rejectTail.load(std::memory_order_relaxed);
    const uint64_t h = r->rejectHead.load(std::memory_order_acquire);
    while (t < h) {
        const RejectEntry& e = r->rejects[t % kRejectSlots];
        rejects_[e.rseq] = e.code;
        t++;
    }
    r->rejectTail.store(t, std::memory_order_release);
    auto it = rejects_.find(rseq);
    return it == rejects_.end() ? 0 : it->second;
}

std::vector<uint8_t> buildRecordAttr(const std::string& peerId, const std::string& provider,
                                     const std::string& source, const std::string& batch,
                                     const std::string& contentKey, const std::string& producerPeer,
                                     const std::string& producerKey, const std::string& supersedeKey,
                                     int64_t sourceTimestamp, const std::string& licenceKey,
                                     uint64_t migratedGseq) {
    flatbuffers::FlatBufferBuilder b(256);
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<fb::SourceTag>>> tags = 0;
    const bool hasTag = !provider.empty() || !source.empty() || !batch.empty() || !producerPeer.empty() ||
                        !producerKey.empty() || !contentKey.empty();
    if (hasTag) {
        // One CreateString per statement, in field order: argument evaluation
        // order is unspecified in C++ (GCC on x86_64 builds nested calls in the
        // opposite order from clang), and the bytes must not depend on it
        // (22.3a-6 golden vectors).
        const auto providerS = b.CreateString(provider);
        const auto sourceS = b.CreateString(source);
        const auto batchS = b.CreateString(batch);
        const auto contentKeyS = b.CreateString(contentKey);
        const auto producerPeerS = b.CreateString(producerPeer);
        const auto producerKeyS = b.CreateString(producerKey);
        const auto tag = fb::CreateSourceTag(b, providerS, sourceS, 0, batchS, contentKeyS, producerPeerS, producerKeyS);
        tags = b.CreateVector(&tag, 1);
    }
    auto peer = b.CreateVector(reinterpret_cast<const uint8_t*>(peerId.data()), peerId.size());
    flatbuffers::Offset<flatbuffers::String> sk = 0, lk = 0;
    if (!supersedeKey.empty()) sk = b.CreateString(supersedeKey);
    if (!licenceKey.empty()) lk = b.CreateString(licenceKey);
    auto ra = fb::CreateRecordAttr(b, peer, 0, sk, sourceTimestamp, lk, tags, migratedGseq);
    fb::FinishRecordAttrBuffer(b, ra);
    return std::vector<uint8_t>(b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize());
}

}  // namespace ps
}  // namespace flatsql
