// FlatSQL partition store: writer threads, commit rounds, ownership, the
// sync pool and the native producer (design §5.2, §6.4, §7, A8, A24, A25,
// A26).
#include <algorithm>
#include <cstdlib>

#include "flatsql/ps/flatsql_attr_generated.h"
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
    int b = ns ? 63 - __builtin_clzll(ns) : 0;
    if (b > 47) b = 47;
    buckets[b].fetch_add(1, std::memory_order_relaxed);
    count.fetch_add(1, std::memory_order_relaxed);
    uint64_t m = maxNs.load(std::memory_order_relaxed);
    while (ns > m && !maxNs.compare_exchange_weak(m, ns)) {}
}

uint64_t LockHist::percentileNs(double q) const {
    const uint64_t total = count.load();
    if (!total) return 0;
    const uint64_t target = uint64_t(q * double(total));
    uint64_t acc = 0;
    for (int b = 0; b < 48; b++) {
        acc += buckets[b].load();
        if (acc > target) return (uint64_t(1) << (b + 1)) - 1;  // upper bound of the bucket
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
    if (used_ > high_) high_ = used_;
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
    slots_.reset(new Slot[kCap]);
    for (uint32_t i = 0; i < kCap; i++) slots_[i].seq.store(i, std::memory_order_relaxed);
    stop_.store(false);
    for (uint32_t i = 0; i < threads; i++) {
        threads_.emplace_back([this] {
            while (!stop_.load(std::memory_order_acquire)) {
                if (popAndRun()) continue;
                const uint32_t w = work_.load(std::memory_order_acquire);
                if (popAndRun()) continue;
                waitU32(&work_, w, 5000000);
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

bool SyncPool::popAndRun() {
    if (!slots_) return false;
    uint64_t pos = deq_.load(std::memory_order_relaxed);
    for (;;) {
        Slot& s = slots_[pos & (kCap - 1)];
        const uint64_t seq = s.seq.load(std::memory_order_acquire);
        const int64_t dif = int64_t(seq) - int64_t(pos + 1);
        if (dif == 0) {
            if (deq_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                SyncJob job = s.job;
                s.seq.store(pos + kCap, std::memory_order_release);
                *job.result = job.io->sync(job.handle);
                if (job.remaining->fetch_sub(1, std::memory_order_acq_rel) == 1)
                    wakeU32(job.remaining, -1);
                return true;
            }
        } else if (dif < 0) {
            return false;
        } else {
            pos = deq_.load(std::memory_order_relaxed);
        }
    }
}

void SyncPool::runAll(SyncJob* jobs, size_t n) {
    if (n == 0) return;
    if (threads_.empty() || n == 1) {
        for (size_t i = 0; i < n; i++) *jobs[i].result = jobs[i].io->sync(jobs[i].handle);
        return;
    }
    std::atomic<uint32_t> remaining{uint32_t(n)};
    size_t pushed = 0;
    while (pushed < n) {
        uint64_t pos = enq_.load(std::memory_order_relaxed);
        Slot& s = slots_[pos & (kCap - 1)];
        const uint64_t seq = s.seq.load(std::memory_order_acquire);
        if (int64_t(seq) - int64_t(pos) == 0 &&
            enq_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
            s.job = jobs[pushed];
            s.job.remaining = &remaining;
            s.seq.store(pos + 1, std::memory_order_release);
            pushed++;
        } else if (int64_t(seq) - int64_t(pos) < 0) {
            popAndRun();  // queue full: help
        }
    }
    work_.fetch_add(1, std::memory_order_release);
    wakeU32(&work_, int(n));
    while (true) {
        const uint32_t r = remaining.load(std::memory_order_acquire);
        if (r == 0) break;
        if (!popAndRun()) waitU32(&remaining, r, 1000000);
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------
Writer::Writer(Engine* eng, uint8_t id) : eng_(eng), id_(id), io_(nullptr, nullptr) {
    io_ = IoCtx(eng->config().io ? eng->config().io : importIo(), &ioStats_);
    sc_ = new StageScratch();
    sc_->init(eng->config());
    arena_.init(size_t(eng->config().arenaBytes / 2));
    framesArena_.init(size_t(eng->config().arenaBytes / 2));
    lookupScratch_.resize(kL1BlockBytes * 2);
    jobs_.reserve(4096);
    jobResults_.reserve(4096);
    jobOwners_.reserve(4096);
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
        for (auto& j : jobs_) *j.result = j.io->sync(j.handle);
    } else {
        eng_->syncPool().runAll(jobs_.data(), jobs_.size());
    }
    syncRounds_.fetch_add(1, std::memory_order_relaxed);
    // A failed sync is never retried (fsyncgate): the owner is quarantined.
    for (size_t i = 0; i < jobs_.size(); i++) {
        if (jobResults_[i] >= 0) continue;
        if (jobOwners_[i].kind == 1) {
            Partition* p = static_cast<Partition*>(jobOwners_[i].owner);
            if (p->st) p->st->err = jobResults_[i];
            p->quarantined = true;
            p->ring->state.store(kRingQuarantined, std::memory_order_release);
        } else if (jobOwners_[i].kind == 2) {
            TypeOwner* t = static_cast<TypeOwner*>(jobOwners_[i].owner);
            if (t->st) t->st->err = jobResults_[i];
        }
    }
    jobs_.clear();
    jobOwners_.clear();
}

void Writer::commitRound() {
    const EngineConfig& cfg = eng_->config();
    const uint64_t t0 = monoNs();
    HotPathScope hot;
    // Phase A: frames (d), lane frames (l), arrivals (g).
    for (Partition* p : dirty_) {
        Staged* st = p->st;
        st->commitStartNs = t0;
        if (!st->batch) continue;
        if (st->dBytes) {
            int32_t rc = ensureExtent(&io_, p->d, &p->dExtent, st->dOff + st->dBytes, cfg.zeroFillStep);
            if (rc >= 0) rc = io_.write(p->d, st->frames, st->dBytes, st->dOff);
            if (rc < 0) {
                st->err = rc;
                continue;
            }
            queueSync(p->d, p, 1);
        }
        if (st->laneFrameBytes) {
            const int32_t rc = io_.write(p->l, st->laneFrames, st->laneFrameBytes, st->lOff);
            if (rc < 0) {
                st->err = rc;
                continue;
            }
            queueSync(p->l, p, 1);
        }
    }
    for (TypeOwner* t : dirtyTypes_) {
        StagedType* st = t->st;
        if (st->nArrivals) {
            const int32_t rc = io_.write(t->g, st->arrivals, size_t(st->nArrivals) * kArrivalBytes, st->gOff);
            if (rc < 0) {
                st->err = rc;
                continue;
            }
            queueSync(t->g, t, 2);
        }
    }
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
        queueSync(p->m, p, 1);
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
        queueSync(t->m, t, 2);
    }
    runJobs();  // sync round 2
    // Publish: heads, then acks (§6.4 steps 5-6); checkpoint heads are synced.
    const uint64_t nowNsV = monoNs();
    for (Partition* p : dirty_) {
        Staged* st = p->st;
        if (st->err) {
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
            const bool due = (nowNsV - p->lastCkptNs) >= uint64_t(cfg.ckptIntervalMs) * 1000000ull ||
                             p->metaSinceCkpt + st->batchLen >= cfg.ckptMetaBytes;
            // State first (includes a segment switch), then the head, then acks.
            Staged copy = *st;
            copy.consumed = false;
            copy.nTickets = 0;
            partitionPublish(this, p, &copy);
            const int32_t hrc = partitionWriteHead(this, p, due);
            if (hrc < 0) {
                p->quarantined = true;
                p->ring->state.store(kRingQuarantined, std::memory_order_release);
            } else if (due) {
                queueSync(p->h, p, 3);
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
                if (frc < 0) {
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
        const bool due = (nowNsV - t->lastCkptNs) >= uint64_t(cfg.ckptIntervalMs) * 1000000ull ||
                         t->metaSinceCkpt + st->batchLen >= cfg.ckptMetaBytes;
        typePublish(this, t, st);
        if (typeWriteHead(this, t, due) >= 0 && due) {
            queueSync(t->h, t, 4);
            t->lastCkptNs = nowNsV;
        }
    }
    runJobs();  // checkpoint heads (when due)
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
        ringMapAhead(r, eng_->pool(), reserve, 2);
        const bool backlog = r->tail.load(std::memory_order_acquire) != r->head.load(std::memory_order_acquire);
        if (!backlog && !p->rec.active && p->kills.empty() && !p->sealPending) continue;
        if (!p->warm && partitionWarm(this, p) < 0) {
            p->quarantined = true;
            r->state.store(kRingQuarantined, std::memory_order_release);
            continue;
        }
        if (arena_.remaining() < (1u << 20) || framesArena_.remaining() < eng_->config().maxEntryBytes + 4096)
            break;
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
        commitRound();
        lastWorkNs_ = monoNs();
    }
    maintenance();
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

void Writer::releaseOwnership(Partition* p, uint8_t target) {
    // At an iteration boundary: the partition has no staged batch (A26).
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
    const int64_t nowMsV = eng_->nowMs();
    const uint64_t nowNsV = monoNs();
    const size_t n = owned_.size();
    // One merge per iteration (bounded maintenance step).
    for (size_t k = 0; k < n; k++) {
        Partition* p = owned_[(maintRr_ + k) % n];
        if (!partitionWantsMerge(eng_, p)) continue;
        if (partitionMerge(this, p) < 0) {
            p->quarantined = true;
            p->ring->state.store(kRingQuarantined, std::memory_order_release);
        }
        break;
    }
    maintRr_++;
    for (TypeOwner* t : types_) {
        if (t->nL0 >= cfg.mergeL0Blocks) {
            typeMerge(this, t);
            break;
        }
    }
    for (Partition* p : owned_) {
        if (p->quarantined) continue;
        // Seal by age (§4.2) and next-segment pre-creation at 50%.
        if (!p->sealPending && p->dLen > 0 && cfg.sealAgeMs > 0 && p->segOpenedMs &&
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
        if (p->warm && !p->rec.active && idle >= uint64_t(cfg.idleCloseMs) * 1000000ull &&
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
    if (started_) stop(0);
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

int32_t Engine::start() {
    if (cfg_.cooperative || started_) return 0;
    syncPool_.start(cfg_.syncThreads);
    for (auto& w : writers_) {
        Writer* wp = w.get();
        wp->thread_ = std::thread([wp] { wp->threadMain(); });
    }
    started_ = true;
    return 0;
}

int32_t Engine::stop(uint64_t deadlineMs) {
    (void)deadlineMs;
    stop_.store(true, std::memory_order_release);
    for (auto& w : writers_) w->ring();
    for (auto& w : writers_)
        if (w->thread_.joinable()) w->thread_.join();
    syncPool_.stop();
    started_ = false;
    // Clean shutdown: durable heads, so the next open reads no tail.
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
    for (auto& w : writers_) {
        for (Partition* p : w->owned_) {
            partitionCool(w.get(), p);
            w->io().close(&p->h);
        }
        for (TypeOwner* t : w->types_) {
            w->io().close(&t->h);
            w->io().close(&t->m);
            w->io().close(&t->g);
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
    stop_.store(true, std::memory_order_release);
    for (auto& w : writers_) w->ring();
    for (auto& w : writers_)
        if (w->thread_.joinable()) w->thread_.join();
    syncPool_.stop();
    started_ = false;
    for (auto& w : writers_) {
        for (Partition* p : w->owned_) {
            partitionCool(w.get(), p);
            w->io().close(&p->h);
        }
        for (TypeOwner* t : w->types_) {
            w->io().close(&t->h);
            w->io().close(&t->m);
            w->io().close(&t->g);
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
    for (const auto& t : typeStore_) s.noticesDropped += t->noticesDropped.load();
    s.framesParsedAtOpen = framesParsedAtOpen;
    s.openReadBytes = openIoStats_.totalReadBytes();
    s.openDataBytes = openIoStats_.readBytes(FileClass::Data);
    s.openMetaBytes = openIoStats_.readBytes(FileClass::Meta);
    s.adoptedBatches = adoptedBatches;
    s.poolSlabsInUse = pool_.inUse();
    s.poolSlabsPeak = pool_.peakInUse();
    s.poolCommittedBytes = pool_.committedBytes();
    uint64_t desc = 0;
    for (const auto& p : partStore_) {
        if (!p) continue;
        desc += sizeof(Partition) + ringDescBytes(p->ring->nSlots) + p->lanes.size() * sizeof(Lane);
    }
    s.descriptorBytes = desc;
    uint64_t acc = 0;
    for (const auto& p : partStore_) {
        if (!p) continue;
        for (const auto& si : p->segs)
            for (const auto& r : si.runs)
                if (r.run) acc += r.run->memoryBytes();
    }
    s.acceleratorBytes = acc;
    // Engine-accounted committed memory of the writer instance: touched pool
    // slabs, arenas and scratch, descriptors and L1 accelerators.
    uint64_t scratch = 0;
    for (const auto& w : writers_) {
        scratch += w->arena_.capacity() + w->framesArena_.capacity();
        const StageScratch* sc = w->sc_;
        scratch += uint64_t(sc->capRows) * sizeof(RecRow) + sc->capAttrs +
                   uint64_t(sc->capEntries) * (sizeof(StagedEntry) + sizeof(void*)) + sc->capKeys +
                   sc->capPlain + sc->capExtract + sc->capSection;
    }
    s.committedBytes = s.poolCommittedBytes + scratch + s.descriptorBytes + s.acceleratorBytes +
                       uint64_t(partsCap_) * sizeof(void*);
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
                                     int64_t sourceTimestamp, const std::string& licenceKey) {
    flatbuffers::FlatBufferBuilder b(256);
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<fb::SourceTag>>> tags = 0;
    const bool hasTag = !provider.empty() || !source.empty() || !batch.empty() || !producerPeer.empty() ||
                        !producerKey.empty() || !contentKey.empty();
    if (hasTag) {
        auto tag = fb::CreateSourceTag(b, b.CreateString(provider), b.CreateString(source), 0,
                                       b.CreateString(batch), b.CreateString(contentKey),
                                       b.CreateString(producerPeer), b.CreateString(producerKey));
        tags = b.CreateVector(&tag, 1);
    }
    auto peer = b.CreateVector(reinterpret_cast<const uint8_t*>(peerId.data()), peerId.size());
    flatbuffers::Offset<flatbuffers::String> sk = 0, lk = 0;
    if (!supersedeKey.empty()) sk = b.CreateString(supersedeKey);
    if (!licenceKey.empty()) lk = b.CreateString(licenceKey);
    auto ra = fb::CreateRecordAttr(b, peer, 0, sk, sourceTimestamp, lk, tags);
    fb::FinishRecordAttrBuffer(b, ra);
    return std::vector<uint8_t>(b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize());
}

}  // namespace ps
}  // namespace flatsql
