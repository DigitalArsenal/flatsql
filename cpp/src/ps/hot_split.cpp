// FlatSQL partition store: hot-partition splitting (design §12, A26;
// docs/PARTITION-STORE.md §32).
//
// One appender, always: the owner. A split partition's owner publishes its
// upcoming ring entries and idle writers prepare them (stage1.cpp); the
// owner takes them in ring order. Sealed segments' merges and compactions
// already build off the owner (T1 deviation 1, T3 deviation 1), so the split
// adds only stage 1. Splitting changes nothing on disk.
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace ps {

namespace {

bool dedicated(Writer* w, const std::vector<Partition*>& owned, const Partition* p) {
    // The owner's worker serves this partition alone: no other owned
    // partition has a backlog (§12 trigger).
    for (const Partition* o : owned)
        if (o != p && o->ring->tail.load(std::memory_order_acquire) != o->ring->head.load(std::memory_order_acquire))
            return false;
    return true;
}

void split(Writer* w, Partition* p) {
    Engine* e = w->engine();
    if (!p->prep) {
        const int saved = tHotPathDepth;
        tHotPathDepth = 0;  // once per partition and run
        p->prep.reset(new PrepWindow());
        p->prepPub.store(p->prep.get(), std::memory_order_release);
        tHotPathDepth = saved;
    }
    PrepWindow& pw = *p->prep;
    // At an iteration boundary nothing is staged: the next entry to stage is
    // at the ring head. Ordinals continue from the last session (never reused).
    const uint64_t head = p->ring->head.load(std::memory_order_acquire);
    const uint64_t ord = std::max(pw.published.load(std::memory_order_relaxed), pw.takeOrd);
    pw.takeOrd = ord;
    pw.takePos = head;
    pw.pubPos = head;
    pw.claim.store(ord, std::memory_order_relaxed);
    pw.published.store(ord, std::memory_order_release);
    pw.on = true;
    p->hot.store(1, std::memory_order_seq_cst);
    e->hotAdd(p);
    e->cSplits.fetch_add(1, std::memory_order_relaxed);
}

void unsplit(Writer* w, Partition* p) {
    Engine* e = w->engine();
    p->hotManual = false;
    p->hot.store(0, std::memory_order_seq_cst);
    e->hotRemove(p);
    e->cUnsplits.fetch_add(1, std::memory_order_relaxed);
    // pw.on stays until no helper is inside the window (hotSplitStep).
}

}  // namespace

void hotSplitStep(Writer* w, Partition* p, uint64_t nowNs) {
    Engine* e = w->engine();
    const EngineConfig& cfg = e->config();
    const bool hot = p->hot.load(std::memory_order_relaxed) != 0;
    // Merged back: the owner leaves the window once no helper is in it (a
    // helper counts itself in before it reads `hot`).
    if (!hot && p->prep && p->prep->on && p->prep->helpers.load(std::memory_order_seq_cst) == 0)
        p->prep->on = false;
    if (p->hotForce >= 0) {
        // Forced (tests, hosts): kept until forced off, whatever the backlog.
        const bool want = p->hotForce == 1;
        p->hotForce = -1;
        if (want && !hot && e->writerCount() > 1 && !(p->prep && p->prep->on)) {
            split(w, p);
            p->hotManual = true;
        } else if (!want && hot) {
            unsplit(w, p);
        }
        p->hotSinceNs = p->coolSinceNs = 0;
        return;
    }
    if (p->quarantined && hot) {
        unsplit(w, p);
        return;
    }
    if (!cfg.hotSplit || e->writerCount() < 2 || (hot && p->hotManual)) return;
    RingDesc* r = p->ring;
    const uint64_t used = r->tail.load(std::memory_order_acquire) - r->head.load(std::memory_order_acquire);
    if (!hot) {
        p->coolSinceNs = 0;
        if (used * 100 > r->cap * cfg.hotSplitBacklogPct && dedicated(w, w->ownedPartitions(), p)) {
            if (!p->hotSinceNs) p->hotSinceNs = nowNs;
            else if (nowNs - p->hotSinceNs >= uint64_t(cfg.hotSplitAfterMs) * 1000000ull &&
                     !(p->prep && p->prep->on)) {
                split(w, p);
                p->hotSinceNs = 0;
            }
        } else {
            p->hotSinceNs = 0;
        }
    } else {
        p->hotSinceNs = 0;
        if (used * 100 < r->cap * cfg.hotUnsplitPct) {
            if (!p->coolSinceNs) p->coolSinceNs = nowNs;
            else if (nowNs - p->coolSinceNs >= uint64_t(cfg.hotUnsplitAfterMs) * 1000000ull) {
                unsplit(w, p);
                p->coolSinceNs = 0;
            }
        } else {
            p->coolSinceNs = 0;
        }
    }
}

// ---------------------------------------------------------------------------
// Engine side
// ---------------------------------------------------------------------------
void Engine::hotAdd(Partition* p) {
    std::lock_guard<std::mutex> g(hotMu_);
    for (uint32_t i = 0; i < kMaxHot; i++)
        if (hot_[i].load(std::memory_order_relaxed) == p) return;
    for (uint32_t i = 0; i < kMaxHot; i++) {
        if (!hot_[i].load(std::memory_order_relaxed)) {
            hot_[i].store(p, std::memory_order_release);
            nHot_.fetch_add(1, std::memory_order_release);
            return;
        }
    }
    // More split partitions than helper slots: this one is not offered to
    // helpers (its owner stages everything, as unsplit).
}

void Engine::hotRemove(Partition* p) {
    std::lock_guard<std::mutex> g(hotMu_);
    for (uint32_t i = 0; i < kMaxHot; i++)
        if (hot_[i].load(std::memory_order_relaxed) == p) {
            hot_[i].store(nullptr, std::memory_order_release);
            nHot_.fetch_sub(1, std::memory_order_release);
        }
}

int32_t Engine::setHotSplit(uint32_t pid, bool on) {
    Partition* p = partition(pid);
    if (!p) return FLATSQL_IO_ERR_NOENT;
    Cmd c;
    c.kind = kCmdHotSplit;
    c.a = pid;
    c.b = on ? 1 : 0;
    Writer* o = writer(p->ownerWriter.load(std::memory_order_acquire));
    while (!o->mailbox().push(c)) cpuRelax();
    o->ring();
    return 0;
}

void Engine::hotClose() {
    for (uint32_t i = 0; i < kMaxHot; i++) hot_[i].store(nullptr, std::memory_order_release);
    nHot_.store(0, std::memory_order_release);
    const uint32_t maxPid = maxPidPub_.load(std::memory_order_acquire);
    for (uint32_t pid = 1; pid <= maxPid && pid < partsCap_; pid++) {
        Partition* p = parts_[pid].load(std::memory_order_acquire);
        if (!p || !p->prep) continue;
        p->hot.store(0, std::memory_order_release);
        std::atomic_store(&p->prep->helperView, std::shared_ptr<const PrepHelperView>());
    }
}

bool Engine::isHot(uint32_t pid) const {
    const Partition* p = partition(pid);
    return p && p->hot.load(std::memory_order_acquire) != 0;
}

}  // namespace ps
}  // namespace flatsql
