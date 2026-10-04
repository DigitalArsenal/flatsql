// Store format 4: the mailbox (CONTRACT §3.4) and the service threads.
//
// Slots live in two pools (write: a large request area; read: a small one).
// The host claims a FREE slot, writes the request TLV, stores QUEUED, pushes
// the slot index on its class's Vyukov MPMC queue and rings a thread's
// doorbell. Writer threads route write slots to the owner of the partition
// (its backlog, bounded by a record credit: full = P4_E_BUSY, nothing done);
// lanes run read ops and SQL; the maintenance thread runs QUOTA_GC, REBUILD,
// flushes, checkpoints, closes and unlinks. Output streams through the slot's
// ring; a full ring makes the producer wait on its own doorbell (1 ms) without
// holding a read transaction or a writer lock.
#include <algorithm>
#include <cstdlib>

#include <chrono>

#include "internal.h"
#include "sql_bridge.h"

namespace flatsql {
namespace p4 {

thread_local uint32_t tThread = 0;

// ---- queues -------------------------------------------------------------------------------------
bool Queue::push(uint32_t v) {
    uint64_t pos = w->enq.load(std::memory_order_relaxed);
    for (;;) {
        QCell& c = cells[pos & mask];
        const uint64_t seq = c.seq.load(std::memory_order_acquire);
        const int64_t dif = int64_t(seq) - int64_t(pos);
        if (dif == 0) {
            if (w->enq.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                c.value = v;
                c.seq.store(pos + 1, std::memory_order_release);
                return true;
            }
        } else if (dif < 0) {
            return false;
        } else {
            pos = w->enq.load(std::memory_order_relaxed);
        }
    }
}

bool Queue::pop(uint32_t* v) {
    uint64_t pos = w->deq.load(std::memory_order_relaxed);
    for (;;) {
        QCell& c = cells[pos & mask];
        const uint64_t seq = c.seq.load(std::memory_order_acquire);
        const int64_t dif = int64_t(seq) - int64_t(pos + 1);
        if (dif == 0) {
            if (w->deq.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                *v = c.value;
                c.seq.store(pos + mask + 1, std::memory_order_release);
                return true;
            }
        } else if (dif < 0) {
            return false;
        } else {
            pos = w->deq.load(std::memory_order_relaxed);
        }
    }
}

namespace {
uint32_t pow2At(uint32_t n) {
    uint32_t c = 2;
    while (c < n) c <<= 1;
    return c;
}
size_t align64(size_t n) { return (n + 63) & ~size_t(63); }
}  // namespace

// ---- memory and layout ----------------------------------------------------------------------------
int32_t mailboxInit(Engine* e, std::string* err) {
    const Config& c = e->cfg;
    e->nSlots[0] = c.writeSlots;
    e->nSlots[1] = c.readSlots;
    e->reqBytes[0] = c.writeReqBytes;
    e->reqBytes[1] = c.readReqBytes;
    e->ringBytes[0] = e->ringBytes[1] = c.ringBytes;
    for (int p = 0; p < 2; p++)
        e->slotStride[p] = uint32_t(align64(size_t(FLATSQL_P4_SLOT_HEADER) + e->reqBytes[p] + e->ringBytes[p]));
    // classes: 1 write, 2 interactive, 3 bulk, 4 sandbox, 5 maintenance
    const uint32_t per[6] = {0, c.writers, c.interactive, c.bulk, c.sandbox, 1};
    uint32_t at = 0;
    for (int k = 1; k <= 5; k++) {
        e->firstOfClass[k] = at;
        e->countOfClass[k] = per[k];
        for (uint32_t i = 0; i < per[k]; i++) e->threadClass[at++] = uint32_t(k);
    }
    e->nThreads = at;
    e->maintThread = e->firstOfClass[5];
    const uint32_t cells[4] = {pow2At(c.writeSlots), pow2At(c.readSlots), pow2At(c.readSlots), pow2At(c.readSlots)};
    size_t bytes = 64;                                    // stop word
    bytes += 4 * sizeof(QueueWords);                      // queue counters
    for (uint32_t n : cells) bytes += size_t(n) * sizeof(QCell);
    bytes = align64(bytes);
    bytes += 64 * sizeof(Bell);
    bytes = align64(bytes);
    const size_t slotsAt = bytes;
    bytes += size_t(e->nSlots[0]) * e->slotStride[0] + size_t(e->nSlots[1]) * e->slotStride[1];
    if (bytes > e->cfg.engineBytes) {
        *err = "mailbox: the slots need more than the engine bytes (tag 20)";
        return P4_E_ARG;
    }
    uint8_t* raw = static_cast<uint8_t*>(std::calloc(1, bytes + 64));
    if (!raw) {
        *err = "mailbox: out of memory";
        return P4_E_NOMEM;
    }
    e->mem = raw;
    e->memBytes = bytes + 64;
    uint8_t* base = reinterpret_cast<uint8_t*>((reinterpret_cast<uintptr_t>(raw) + 63) & ~uintptr_t(63));
    e->stopWord = new (base) std::atomic<uint32_t>(0);
    uint8_t* q = base + 64;
    QueueWords* words = reinterpret_cast<QueueWords*>(q);
    q += 4 * sizeof(QueueWords);
    for (int k = 0; k < 4; k++) {
        new (&words[k]) QueueWords();
        words[k].enq.store(0);
        words[k].deq.store(0);
        e->queues[k].w = &words[k];
        e->queues[k].cells = reinterpret_cast<QCell*>(q);
        e->queues[k].mask = cells[k] - 1;
        for (uint32_t i = 0; i < cells[k]; i++) {
            new (&e->queues[k].cells[i]) QCell();
            e->queues[k].cells[i].seq.store(i);
        }
        q += size_t(cells[k]) * sizeof(QCell);
    }
    uint8_t* bellsAt = base + align64(size_t(q - base));
    e->bells = reinterpret_cast<Bell*>(bellsAt);
    for (int i = 0; i < 64; i++) {
        new (&e->bells[i]) Bell();
        e->bells[i].doorbell.store(0);
        e->bells[i].state.store(1);
    }
    e->slotBase[0] = base + slotsAt;
    e->slotBase[1] = e->slotBase[0] + size_t(e->nSlots[0]) * e->slotStride[0];
    for (uint32_t i = 0; i < e->nSlots[0] + e->nSlots[1]; i++) {
        SlotHeader* h = new (e->slot(i)) SlotHeader();
        h->state.store(P4_SLOT_FREE);
        h->cancel.store(0);
        h->outSeq.store(0);
        h->spaceSeq.store(0);
        h->ringHead.store(0);
        h->ringTail.store(0);
    }
    for (uint32_t w = 0; w < c.writers; w++) e->writers.push_back(std::make_unique<WriterState>());
    return P4_OK;
}

void mailboxLayout(Engine* e, FlatsqlP4Layout* out) {
    std::memset(out, 0, sizeof *out);
    auto addr = [](const void* p) { return uint32_t(uintptr_t(p)); };
    out->version = FLATSQL_P4_LAYOUT_VERSION;
    out->size = sizeof(FlatsqlP4Layout);
    out->headerSize = FLATSQL_P4_SLOT_HEADER;
    out->stopWord = addr(e->stopWord);
    for (int p = 0; p < 2; p++) {
        out->nSlots[p] = e->nSlots[p];
        out->slotBase[p] = addr(e->slotBase[p]);
        out->slotStride[p] = e->slotStride[p];
        out->reqBytes[p] = e->reqBytes[p];
        out->ringBytes[p] = e->ringBytes[p];
    }
    for (int k = 0; k < 4; k++) {
        out->queueCells[k] = addr(e->queues[k].cells);
        out->queueMask[k] = e->queues[k].mask;
        out->queueEnq[k] = addr(&e->queues[k].w->enq);
        out->queueDeq[k] = addr(&e->queues[k].w->deq);
    }
    out->nThreads = e->nThreads;
    for (uint32_t i = 0; i < e->nThreads; i++) {
        out->threadClass[i] = e->threadClass[i];
        out->doorbell[i] = addr(&e->bells[i].doorbell);
    }
}

}  // namespace p4
}  // namespace flatsql

void P4Engine::wake(uint32_t thread) {
    if (thread >= nThreads) return;
    bells[thread].doorbell.fetch_add(1, std::memory_order_release);
    flatsql::ps::wakeU32(&bells[thread].doorbell, 1);
}

void P4Engine::kickMaintenance() { wake(maintThread); }

P4Lane::~P4Lane() {
}

namespace flatsql {
namespace p4 {

// ---- slot output ----------------------------------------------------------------------------------
namespace {
int32_t ringWrite(Engine* e, uint32_t slot, uint32_t thread, const uint8_t* p, size_t n, bool honourCancel) {
    SlotHeader* h = e->slot(slot);
    uint8_t* ring = e->slotRing(slot);
    const uint64_t cap = e->ringBytes[e->poolOf(slot)];
    while (n > 0) {
        if (honourCancel && h->cancel.load(std::memory_order_acquire)) return P4_E_CANCELLED;
        const uint64_t head = h->ringHead.load(std::memory_order_acquire);
        const uint64_t tail = h->ringTail.load(std::memory_order_relaxed);
        const uint64_t space = cap - (tail - head);
        if (space == 0) {
            if (e->stopWord->load(std::memory_order_acquire) && e->stopping.load()) {
                // The host stopped reading: give up rather than hang the stop.
                return P4_E_STOPPED;
            }
            Bell& b = e->bells[thread < e->nThreads ? thread : 0];
            const uint32_t seq = b.doorbell.load(std::memory_order_acquire);
            if (h->ringHead.load(std::memory_order_acquire) == head)
                ps::waitU32(&b.doorbell, seq, 1000ull * 1000);
            continue;
        }
        const size_t k = size_t(std::min<uint64_t>(space, n));
        const size_t at = size_t(tail & (cap - 1));
        const size_t first = std::min(k, size_t(cap) - at);
        std::memcpy(ring + at, p, first);
        if (k > first) std::memcpy(ring, p + first, k - first);
        h->ringTail.store(tail + k, std::memory_order_release);
        h->outSeq.fetch_add(1, std::memory_order_release);
        ps::wakeU32(&h->outSeq, -1);
        p += k;
        n -= k;
    }
    return P4_OK;
}
}  // namespace

int32_t emitBytes(P4Lane* L, const uint8_t* p, size_t n) {
    if (L->trip) return L->trip;
    // The result-bytes cap: the first chunk (it carries the RB1 header) always
    // goes, so a capped stream stays header, blocks, RB1E.
    if (L->maxResultBytes && L->outBytes > 0 && L->outBytes + n > L->maxResultBytes) {
        L->trip = P4_E_BUDGET;
        return P4_E_BUDGET;
    }
    L->outBytes += n;
    const int32_t rc = ringWrite(L->e, L->slot, L->thread, p, n, true);
    if (rc != P4_OK) L->trip = rc;
    return rc;
}

int32_t flushOut(P4Lane* L) {
    if (L->out.empty()) return L->trip;
    const int32_t rc = emitBytes(L, L->out.data(), L->out.size());
    L->out.clear();
    return rc;
}

// The op's last bytes (the RB1E, and the header of an op that never ran):
// written whatever tripped, cancel and caps included.
int32_t flushFinal(P4Lane* L) {
    if (L->out.empty()) return P4_OK;
    L->outBytes += L->out.size();
    const int32_t rc = ringWrite(L->e, L->slot, L->thread, L->out.data(), L->out.size(), false);
    L->out.clear();
    return rc;
}

void slotDone(Engine* e, uint32_t slot, int32_t status, const std::string& err, uint64_t rows) {
    SlotHeader* h = e->slot(slot);
    h->status = status;
    const size_t n = std::min(err.size(), sizeof(h->err) - 1);
    std::memcpy(h->err, err.data(), n);
    h->err[n] = 0;
    h->errLen = uint32_t(n);
    h->rowsOut = rows;
    h->endNs = monoNs();
    h->state.store(P4_SLOT_DONE, std::memory_order_release);
    h->outSeq.fetch_add(1, std::memory_order_release);
    ps::wakeU32(&h->outSeq, -1);
}

void slotDoneLane(P4Lane* L, int32_t status, const std::string& err) {
    SlotHeader* h = L->h;
    h->rowsExamined = L->rowsExamined;
    h->bytesRead = L->bytesRead;
    slotDone(L->e, L->slot, status, err, L->rowsOut);
}

void respondEmpty(P4Lane* L, const std::vector<std::string>& cols, int32_t status, const std::string& err) {
    L->out.clear();
    ps::rb1::Encoder enc(&L->out);
    enc.header(cols);
    enc.end(status, 0, L->rowsExamined, L->bytesRead);
    flushFinal(L);
    slotDoneLane(L, status, err);
}

int32_t SlotOut::flush() {
    if (buf.empty()) return P4_OK;
    const int32_t rc = ringWrite(e, slot, tThread, buf.data(), buf.size(), false);
    buf.clear();
    return rc;
}

int32_t SlotOut::end(int32_t status, const std::string& err) {
    enc.end(status, rows, 0, 0);
    e->slot(slot)->thread = tThread;
    flush();
    slotDone(e, slot, status, err, rows);
    return status;
}

// ---- write routing (any writer thread) -----------------------------------------------------------
namespace {

void doneStatus(Engine* e, uint32_t slot, const std::vector<std::string>& cols, int32_t status, const std::string& err) {
    SlotOut out(e, slot);
    out.enc.header(cols);
    out.end(status, err);
}

const std::vector<std::string>& colsOfWrite(uint32_t op) {
    static const std::vector<std::string> put = {"i", "action", "seq", "reject"};
    static const std::vector<std::string> sup = {"tags_deleted", "records_deleted", "files_deleted"};
    static const std::vector<std::string> del = {"deleted"};
    static const std::vector<std::string> quota = {"files_dropped", "records_dropped", "bytes_freed"};
    static const std::vector<std::string> reb = {"type", "entries", "mismatches"};
    static const std::vector<std::string> none = {};
    switch (op) {
        case P4_OPC_PUT: return put;
        case P4_OPC_SUPERSEDE: return sup;
        case P4_OPC_DELETE: return del;
        case P4_OPC_QUOTA_GC: return quota;
        case P4_OPC_REBUILD: return reb;
        default: return none;
    }
}

}  // namespace

void pushTask(Engine* e, Type* t, WriteTask* wt) {
    WriterState& ws = *e->writers[t->owner];
    {
        std::lock_guard<std::mutex> g(ws.mu);
        t->backlog.push_back(wt);
        t->backlogRecords += wt->records;
        if (!t->ready) {
            t->ready = true;
            ws.ready.push_back(t);
        }
    }
    e->wake(e->firstOfClass[1] + t->owner);
}

namespace {

void routeWrite(Engine* e, uint32_t wi, uint32_t slot) {
    SlotHeader* h = e->slot(slot);
    uint32_t expect = P4_SLOT_QUEUED;
    if (h->cancel.load(std::memory_order_acquire)) {
        if (h->state.compare_exchange_strong(expect, P4_SLOT_RUNNING)) {
            h->thread = tThread;
            doneStatus(e, slot, colsOfWrite(h->op), P4_E_CANCELLED, "cancelled");
        }
        return;
    }
    if (!h->state.compare_exchange_strong(expect, P4_SLOT_RUNNING)) return;
    h->startNs = monoNs();
    h->thread = tThread;
    const uint32_t op = h->op;
    if (e->poolOf(slot) != 0 || h->cls != P4_CLASS_WRITE) {
        doneStatus(e, slot, colsOfWrite(op), P4_E_ARG, "write ops use the write pool and class 1");
        return;
    }
    if (e->stopping.load()) {
        doneStatus(e, slot, colsOfWrite(op), P4_E_STOPPED, "stopping");
        return;
    }
    if (op == P4_OPC_QUOTA_GC || op == P4_OPC_REBUILD) {
        {
            std::lock_guard<std::mutex> g(e->maintMu);
            MaintTask mt;
            mt.kind = MaintTask::kSlot;
            mt.slot = slot;
            e->maintQ.push_back(mt);
        }
        e->kickMaintenance();
        return;
    }
    std::vector<Tlv> v;
    if (h->reqLen > e->reqBytes[0] || !tlvParse(e->slotReq(slot), h->reqLen, &v)) {
        doneStatus(e, slot, colsOfWrite(op), P4_E_ARG, "malformed request");
        return;
    }
    std::string typeName;
    tlvText(v, 1, &typeName);
    Type* t = e->findType(typeName);
    if (op != P4_OPC_PUT && op != P4_OPC_SUPERSEDE && op != P4_OPC_DELETE) {
        doneStatus(e, slot, colsOfWrite(op), P4_E_ARG, "not a write op");
        return;
    }
    if (!t) {
        doneStatus(e, slot, colsOfWrite(op), P4_E_NOTYPE, "type not registered: " + typeName);
        return;
    }
    uint64_t records = 0;
    uint8_t mode = 0;
    if (op == P4_OPC_PUT) {
        bool bad = false;
        tlvU8(v, 52, &mode, &bad);
        const Tlv* recs = tlvFind(v, 53);
        if (bad || !recs || recs->n < 4) {
            doneStatus(e, slot, colsOfWrite(op), P4_E_ARG, "PUT needs records (tag 53)");
            return;
        }
        bool over;
        {
            std::lock_guard<std::mutex> g(t->mu);
            over = t->overQuota;
        }
        if (over) {
            doneStatus(e, slot, colsOfWrite(op), P4_E_NOSPACE, "over quota");
            return;
        }
        records = ld32(recs->v);
        WriterState& ws = *e->writers[t->owner];
        bool busy;
        {
            std::lock_guard<std::mutex> g(ws.mu);
            busy = t->backlogRecords > 0 && t->backlogRecords + records > e->cfg.backlogCredit;
        }
        // Backpressure: the type's backlog over its credit, or the WALs over
        // twice their total (checkpoints cannot keep up).
        const char* why = busy ? "type backlog full" : nullptr;
        if (!why && e->stat[kStWalBytes].load(std::memory_order_relaxed) > 2 * e->cfg.walTotal) why = "WAL checkpoints behind";
        if (why) {
            e->bump(kStBusy);
            e->kickMaintenance();
            doneStatus(e, slot, colsOfWrite(op), P4_E_BUSY, why);
            return;
        }
    }
    WriteTask* wt = new (std::nothrow) WriteTask();
    if (!wt) {
        doneStatus(e, slot, colsOfWrite(op), P4_E_NOMEM, "out of memory");
        return;
    }
    wt->slot = slot;
    wt->op = int(op);
    wt->mode = mode;
    wt->records = records;
    wt->type = t;
    (void)wi;
    pushTask(e, t, wt);
}

// Takes the next work of a type's backlog: consecutive PUT calls of one mode
// up to group-commit records, or one other task.
bool takeGroup(Engine* e, uint32_t wi, Type** tp, std::vector<WriteTask*>* out) {
    WriterState& ws = *e->writers[wi];
    std::lock_guard<std::mutex> g(ws.mu);
    while (!ws.ready.empty()) {
        Type* t = ws.ready.front();
        ws.ready.pop_front();
        t->ready = false;
        if (t->backlog.empty()) continue;
        uint64_t recs = 0;
        WriteTask* first = t->backlog.front();
        do {
            WriteTask* wt = t->backlog.front();
            if (!out->empty() && (wt->op != P4_OPC_PUT || first->op != P4_OPC_PUT || wt->mode != first->mode)) break;
            if (!out->empty() && recs + wt->records > e->cfg.groupRecords) break;
            t->backlog.pop_front();
            t->backlogRecords -= wt->records;
            recs += wt->records;
            out->push_back(wt);
        } while (!t->backlog.empty() && first->op == P4_OPC_PUT);
        if (!t->backlog.empty()) {
            t->ready = true;
            ws.ready.push_back(t);
        }
        *tp = t;
        return true;
    }
    return false;
}

// ---- the PUT pipeline -------------------------------------------------------------------------------
// The writer plans groups and appends their frames; its indexer commits them
// in rounds: each round takes every queued unit and gives each feed file it
// touches ONE transaction (stream synced first; the rows staged), then acks
// the round's calls. Units are committed and answered in plan order. A group
// is planned on the state the type's pending units leave its records in
// (their seeds). Between rounds the indexer merges the feeds' staged rows
// into their indexes (staged.cpp), one feed's transaction at a time.

constexpr size_t kMaxPending = 16;  // planned but not yet committed, per writer

// The pending units at the front that are done (block: wait for the front
// one). A failed one stops the collection: the writer recovers first.
void collect(WriterState& ws, bool block) {
    while (!ws.pending.empty()) {
        PutUnit* p = ws.pending.front().get();
        if (!p->done.load(std::memory_order_acquire)) {
            if (!block) return;
            std::unique_lock<std::mutex> g(ws.imu);
            ws.dcv.wait(g, [&] { return p->done.load(std::memory_order_acquire); });
        }
        if (p->status != P4_OK) {
            ws.failing = true;
            return;
        }
        ws.pending.pop_front();
        block = false;
    }
}

// After a failed round: every pending unit done (the indexer fails each one
// after the failure: poison), the streams they appended to cut back to their
// committed marks, then the indexer applies again.
void recover(WriterState& ws) {
    for (auto& u : ws.pending) {
        std::unique_lock<std::mutex> g(ws.imu);
        ws.dcv.wait(g, [&] { return u->done.load(std::memory_order_acquire); });
    }
    for (auto& u : ws.pending)
        if (u->status != P4_OK) u->resetStreams();
    ws.pending.clear();
    std::lock_guard<std::mutex> g(ws.imu);
    ws.poison = P4_OK;
    ws.failing = false;
}

// Every pending unit committed and no merge running: the writer's own index
// work runs next (release() lets the indexer merge again).
void drain(WriterState& ws) {
    while (!ws.pending.empty() && !ws.failing) collect(ws, true);
    if (ws.failing) recover(ws);
    std::unique_lock<std::mutex> g(ws.imu);
    ws.hold = true;
    ws.dcv.wait(g, [&] { return !ws.merging; });
}
void release(WriterState& ws) {
    std::lock_guard<std::mutex> g(ws.imu);
    ws.hold = false;
}

void runPut(Engine* e, uint32_t wi, Type* t, std::vector<WriteTask*>& tasks) {
    WriterState& ws = *e->writers[wi];
    collect(ws, false);
    if (ws.failing) recover(ws);
    std::vector<const PutUnit*> prevs;
    for (auto& u : ws.pending) prevs.push_back(u.get());
    std::unique_ptr<PutUnit> u = putPlan(e, t, tasks, prevs);
    PutUnit* raw = u.get();
    ws.pending.push_back(std::move(u));
    {
        std::lock_guard<std::mutex> g(ws.imu);
        ws.iq.push_back(raw);
    }
    ws.icv.notify_one();
    while (ws.pending.size() > kMaxPending && !ws.failing) collect(ws, true);
}

void indexerLoop(Engine* e, uint32_t wi) {
    tThread = e->firstOfClass[1] + wi;
    WriterState& ws = *e->writers[wi];
    for (;;) {
        std::vector<PutUnit*> round;
        int32_t poison;
        {
            // Units, or (staged rows waiting) a look at the merges every 250 ms.
            std::unique_lock<std::mutex> g(ws.imu);
            ws.icv.wait_for(g, std::chrono::milliseconds(250), [&] { return !ws.iq.empty() || ws.istop; });
            if (ws.iq.empty() && ws.istop) break;
            round.assign(ws.iq.begin(), ws.iq.end());
            ws.iq.clear();
            poison = ws.poison;
        }
        if (round.empty()) {
            mergeStep(e, wi);
            continue;
        }
        int32_t rc = poison;
        std::string why = "an earlier write on this writer failed before this one could commit: nothing stored";
        if (rc == P4_OK) {
            std::vector<WriteCtx*> ctxs;
            for (PutUnit* u : round)
                if (WriteCtx* w = u->applyCtx()) ctxs.push_back(w);
            if (!ctxs.empty()) rc = applyRound(e, ctxs, &why);
        }
        for (PutUnit* u : round) {
            u->status = rc;
            u->finish(rc, why);
        }
        {
            std::lock_guard<std::mutex> g(ws.imu);
            if (rc != P4_OK && ws.poison == P4_OK) ws.poison = rc;
            for (PutUnit* u : round) u->done.store(true, std::memory_order_release);
        }
        ws.dcv.notify_all();
        if (rc == P4_OK) mergeStep(e, wi);
    }
}

void writerLoop(Engine* e, uint32_t wi) {
    tThread = e->firstOfClass[1] + wi;
    Bell& b = e->bells[tThread];
    WriterState& ws = *e->writers[wi];
    for (;;) {
        const uint32_t seq = b.doorbell.load(std::memory_order_acquire);
        bool did = false;
        uint32_t slot;
        // Route everything queued before taking a group: a group is then every
        // call that arrived while the previous commit ran (group commit).
        for (uint32_t k = 0; k < e->nSlots[0] && e->queues[0].pop(&slot); k++) {
            routeWrite(e, wi, slot);
            did = true;
        }
        Type* t = nullptr;
        std::vector<WriteTask*> tasks;
        if (takeGroup(e, wi, &t, &tasks)) {
            did = true;
            const int op = tasks[0]->op;
            for (WriteTask* wt : tasks)
                if (!wt->internal) e->slot(wt->slot)->thread = tThread;
            if (op == P4_OPC_PUT && !tasks[0]->internal) {
                runPut(e, wi, t, tasks);
            } else {
                // Everything else reads and writes the index files itself:
                // the indexer is idle first (no round, no merge).
                drain(ws);
                if (tasks[0]->internal) {
                    Internal* in = tasks[0]->internal;
                    if (op == P4_OPC_QUOTA_GC) quotaWork(e, t, in);
                    else if (op == kOpCompact) compactWork(e, t, in);
                    else rebuildWork(e, t, in);
                    in->done.store(true, std::memory_order_release);
                } else if (op == P4_OPC_SUPERSEDE) {
                    supersedeOp(e, t, tasks[0]);
                } else {
                    deleteOp(e, t, tasks[0]);
                }
                release(ws);
            }
            for (WriteTask* wt : tasks) delete wt;
        }
        if (did) continue;
        bool idle;
        {
            std::lock_guard<std::mutex> g(e->writers[wi]->mu);
            idle = e->writers[wi]->ready.empty();
        }
        if (idle && !ws.pending.empty()) {
            // Nothing to plan: the pending units, as their rounds are done
            // (a short wait, then new calls are routed again).
            collect(ws, false);
            if (ws.failing) recover(ws);
            if (!ws.pending.empty()) {
                std::unique_lock<std::mutex> g(ws.imu);
                PutUnit* front = ws.pending.front().get();
                ws.dcv.wait_for(g, std::chrono::milliseconds(1), [&] { return front->done.load(std::memory_order_acquire); });
            }
            continue;
        }
        if (idle && e->queues[0].empty() && e->stopping.load()) break;
        b.state.store(0, std::memory_order_seq_cst);
        if (idle && e->queues[0].empty() && b.doorbell.load(std::memory_order_acquire) == seq)
            ps::waitU32(&b.doorbell, seq, 50ull * 1000 * 1000);
        b.state.store(1, std::memory_order_seq_cst);
    }
    {
        std::lock_guard<std::mutex> g(ws.imu);
        ws.istop = true;
    }
    ws.icv.notify_all();
    if (ws.indexer.joinable()) ws.indexer.join();
    b.state.store(2);
}

// ---- lanes ------------------------------------------------------------------------------------------
void runSlot(P4Lane* L, uint32_t slot) {
    Engine* e = L->e;
    SlotHeader* h = e->slot(slot);
    L->slot = slot;
    L->h = h;
    L->req = e->slotReq(slot);
    L->reqLen = h->reqLen;
    L->ring = e->slotRing(slot);
    L->ringBytes = e->ringBytes[e->poolOf(slot)];
    L->rowsExamined = L->bytesRead = L->rowsOut = L->outBytes = 0;
    L->trip = 0;
    L->err.clear();
    L->out.clear();
    uint32_t expect = P4_SLOT_QUEUED;
    if (h->cancel.load(std::memory_order_acquire)) {
        if (h->state.compare_exchange_strong(expect, P4_SLOT_RUNNING)) {
            h->thread = L->thread;
            respondEmpty(L, {}, P4_E_CANCELLED, "cancelled");
        }
        return;
    }
    if (!h->state.compare_exchange_strong(expect, P4_SLOT_RUNNING)) return;
    h->startNs = monoNs();
    h->thread = L->thread;
    const bool sandbox = L->cls == P4_CLASS_SANDBOX || (h->flags & P4_SLOT_SANDBOX);
    L->maxRows = h->maxRowsExamined ? h->maxRowsExamined : (sandbox ? e->cfg.sandboxRows : 0);
    L->maxBytes = h->maxBytesRead ? h->maxBytesRead : (sandbox ? e->cfg.sandboxBytes : 0);
    L->maxResultRows = h->maxResultRows;
    L->maxResultBytes = h->maxResultBytes;
    L->heapCap = e->cfg.sandboxHeap;
    if (e->poolOf(slot) != 1 || h->cls != L->cls || h->reqLen > e->reqBytes[1]) {
        respondEmpty(L, {}, P4_E_ARG, "read ops use the read pool and the lane's class");
        return;
    }
    if (e->stopping.load()) {
        respondEmpty(L, {}, P4_E_STOPPED, "stopping");
        return;
    }
    const uint32_t op = h->op;
    int32_t status;
    if (op >= P4_OPC_GET && op <= P4_OPC_SUMMARY) {
        status = runRead(L, op);
        (void)status;
        return;  // runRead finishes the slot
    }
    if (op == P4_OPC_SQL || op == P4_OPC_SURFACE) {
        e->bump(kStSqlStatements);
        if (op == P4_OPC_SQL) {
            std::vector<Tlv> v;
            if (!tlvParse(L->req, L->reqLen, &v)) {
                respondEmpty(L, {}, P4_E_ARG, "malformed request");
                return;
            }
            const Tlv* sql = tlvFind(v, 70);
            const Tlv* params = tlvFind(v, 71);
            P4SqlRequest req{};
            req.sql = sql ? reinterpret_cast<const char*>(sql->v) : "";
            req.sqlLen = sql ? sql->n : 0;
            req.params = params ? params->v : nullptr;
            req.paramsLen = params ? params->n : 0;
            req.flags = h->flags & (P4_SLOT_RAW | P4_SLOT_SANDBOX);
            req.maxResultRows = h->maxResultRows;
            req.maxResultBytes = h->maxResultBytes;
            status = p4sql_exec(L, &req);
        } else {
            status = p4sql_surface(L);
        }
        if (status == P4_E_UNSUPPORTED && L->outBytes == 0 && L->out.empty()) {
            respondEmpty(L, {}, status, L->err.empty() ? "the SQL surface is not in this build" : L->err);
            return;
        }
        if (status == P4_E_BUDGET) e->bump(kStSqlBudgetTrips);
        // C-28: in RB1 mode the engine ends the stream, tripped or not, with
        // the returned status, the slot's counters and C-29's rows (a stream
        // that never started gets an empty header first). RAW is unchanged.
        if (!(h->flags & P4_SLOT_RAW)) {
            const bool started = L->outBytes > 0 || !L->out.empty();
            ps::rb1::Encoder enc(&L->out);
            if (!started) enc.header({});
            enc.end(status, L->rowsOut, L->rowsExamined, L->bytesRead);
        }
        flushFinal(L);
        slotDoneLane(L, status, !L->err.empty() ? L->err : status == P4_OK ? std::string() : std::string("SQL op failed"));
        return;
    }
    respondEmpty(L, {}, P4_E_ARG, "unknown op");
}

void laneLoop(Engine* e, uint32_t ti, uint32_t cls) {
    tThread = ti;
    Bell& b = e->bells[ti];
    P4Lane L;
    L.e = e;
    L.thread = ti;
    L.cls = cls;
    p4sql_lane_init(&L);
    Queue& q = e->queues[cls - 1];
    for (;;) {
        const uint32_t seq = b.doorbell.load(std::memory_order_acquire);
        uint32_t slot;
        if (q.pop(&slot)) {
            runSlot(&L, slot);
            continue;
        }
        if (e->stopping.load()) break;
        b.state.store(0, std::memory_order_seq_cst);
        if (q.empty() && b.doorbell.load(std::memory_order_acquire) == seq)
            ps::waitU32(&b.doorbell, seq, 50ull * 1000 * 1000);
        b.state.store(1, std::memory_order_seq_cst);
    }
    p4sql_lane_free(&L);
    b.state.store(2);
}

}  // namespace

int32_t startThreads(Engine* e) {
    if (e->started) return P4_E_ARG;
    // Each writer's indexer first (a writer hands it units from its start).
    for (uint32_t w = 0; w < e->writers.size(); w++) {
        e->writers[w]->istop = false;
        e->writers[w]->indexer = std::thread(indexerLoop, e, w);
    }
    for (uint32_t i = 0; i < e->nThreads; i++) {
        const uint32_t cls = e->threadClass[i];
        if (cls == P4_CLASS_WRITE) e->threads.emplace_back(writerLoop, e, i - e->firstOfClass[1]);
        else if (cls == P4_CLASS_MAINTENANCE) e->threads.emplace_back(maintenanceLoop, e, i);
        else e->threads.emplace_back(laneLoop, e, i, cls);
    }
    e->slowThread = std::thread(slowLoop, e);
    e->started = true;
    return int32_t(e->nThreads);
}

// ---- stop -------------------------------------------------------------------------------------------
int32_t engineStop(Engine* e, double deadlineMs) {
    e->stopping.store(true);
    if (e->stopWord) e->stopWord->store(1, std::memory_order_release);
    if (e->bells)
        for (uint32_t i = 0; i < e->nThreads; i++) e->wake(i);
    const uint64_t until = monoNs() + uint64_t(deadlineMs > 0 ? deadlineMs : 0) * 1000000ull;
    if (e->started) {
        {
            std::lock_guard<std::mutex> g(e->slowMu);
            e->slowStop = true;
            e->slowCv.notify_all();
        }
        if (e->slowThread.joinable()) e->slowThread.join();
        for (;;) {
            bool all = true;
            for (uint32_t i = 0; i < e->nThreads; i++) all = all && e->bells[i].state.load() == 2;
            if (all) break;
            if (deadlineMs > 0 && monoNs() >= until) return P4_E_BUSY;
            for (uint32_t i = 0; i < e->nThreads; i++) e->wake(i);
            ps::sleepNs(1000000);
        }
        for (auto& th : e->threads)
            if (th.joinable()) th.join();
        e->threads.clear();
        e->started = false;
    }
    // Slots that never ran.
    for (int k = 0; k < 4 && e->queues[k].w; k++) {
        uint32_t slot;
        while (e->queues[k].pop(&slot)) {
            SlotHeader* h = e->slot(slot);
            uint32_t expect = P4_SLOT_QUEUED;
            if (h->state.compare_exchange_strong(expect, P4_SLOT_RUNNING)) slotDone(e, slot, P4_E_STOPPED, "stopping", 0);
        }
    }
    std::vector<Type*> types;
    {
        std::lock_guard<std::mutex> g(e->typesMu);
        for (auto& t : e->types) types.push_back(t.get());
    }
    // Close every writer connection with a TRUNCATE checkpoint.
    std::vector<Conn*> conns;
    {
        std::lock_guard<std::mutex> g(e->wconnMu);
        for (Feed* f : e->wlru) {
            conns.push_back(f->w);
            f->w = nullptr;
            f->inLru = false;
        }
        e->wlru.clear();
        e->nWConn = 0;
    }
    {
        std::lock_guard<std::mutex> g(e->maintMu);
        for (auto& mt : e->maintQ)
            if (mt.kind == MaintTask::kClose && mt.conn) conns.push_back(mt.conn);
        e->maintQ.clear();
    }
    for (Conn* c : conns) {
        sqlite3_wal_checkpoint_v2(c->db, nullptr, SQLITE_CHECKPOINT_TRUNCATE, nullptr, nullptr);
        e->bump(kStTruncate);
        delete c;
    }
    e->rpool.closeAll();
    for (Type* t : types) {
        t->hasFiles.store(false, std::memory_order_release);  // closed: nothing reads or flushes them again
        if (t->idx) {
            sqlite3_wal_checkpoint_v2(t->idx->db, nullptr, SQLITE_CHECKPOINT_TRUNCATE, nullptr, nullptr);
            delete t->idx;
            t->idx = nullptr;
        }
        {
            // The streams (a generation a compaction replaced is unlinked now).
            std::vector<Feed*> fs;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (auto& f : t->feeds) fs.push_back(f.get());
            }
            for (Feed* f : fs) streamClose(f);
        }
        if (t->fts) {
            delete t->fts;
            t->fts = nullptr;
        }
    }
    return P4_OK;
}

}  // namespace p4
}  // namespace flatsql
