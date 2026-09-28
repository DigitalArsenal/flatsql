// FlatSQL partition store: reader instances, lanes, the mailbox ABI and the
// native client (see ps/lane.h; design §5.3, §8, §9, A12, A21, A28, A30).
#include "flatsql/ps/lane.h"

#include <sqlite3.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <new>

#if !defined(__wasm__)
#include <sys/mman.h>
#include <sys/resource.h>
#include <unistd.h>
#if defined(__linux__)
#include <sys/syscall.h>
#endif
#if defined(__APPLE__)
#include <pthread/qos.h>
#endif
#endif

#include "admission.h"
#include "flatsql/ps/platform.h"
#include "flatsql/ps/result_block.h"
#include "flatsql/ps/vtab.h"

namespace flatsql {
namespace ps {

namespace {
constexpr uint64_t kCanary = 0x43414e4152594c4eull;  // "NLYRANAC"
inline size_t align64(size_t n) { return (n + 63) & ~size_t(63); }
std::string lowerOf(const char* s) {
    std::string o;
    for (; s && *s; s++) o.push_back(char(std::tolower(uint8_t(*s))));
    return o;
}
}  // namespace

// ---------------------------------------------------------------------------
// Active statements
// ---------------------------------------------------------------------------
struct ReaderLane::Active {
    uint32_t slot = 0;
    SlotHeader* h = nullptr;
    StmtCtx ctx;
    sqlite3_stmt* stmt = nullptr;
    std::vector<uint8_t> pending;
    size_t pendingAt = 0;
    bool raw = false;
    bool finished = false;   // SQL done; draining pending bytes
    bool parked = false;
    int32_t status = 0;
    std::string msg;
    uint64_t rows = 0;
    uint64_t outBytes = 0;
    int ncols = 0;
    uint64_t vmSteps = 0;
};

// ---------------------------------------------------------------------------
// ReaderLane
// ---------------------------------------------------------------------------
ReaderLane::ReaderLane(ReaderInstance* inst, uint32_t id) : inst_(inst), id_(id) {
    const ReaderConfig& cfg = inst->config();
    LaneStoreConfig sc;
    sc.root = cfg.root;
    sc.io = cfg.io;
    sc.maxHandles = cfg.maxHandlesPerLane;
    sc.cacheBytes = cfg.laneCacheBytes;
    sc.shared = inst->cache_.get();
    sc.verifyFrameCrc = cfg.verifyFrameCrc;
    store_.reset(new LaneStore(sc));
}

ReaderLane::~ReaderLane() {
#if !defined(__wasm__)
    if (stack_) munmap(stack_, stackBytes_);
#endif
}

LaneClass ReaderLane::cls() const { return inst_->config().cls; }
const ReaderConfig& ReaderLane::config() const { return inst_->config(); }

bool ReaderLane::hasTable(const char* name) const { return tables_.count(lowerOf(name)) != 0; }
void ReaderLane::noteTable(const std::string& name) { tables_.insert(lowerOf(name.c_str())); }

uint64_t ReaderLane::hotWindow(const std::string& typeName) const {
    const ReaderConfig& cfg = config();
    for (const auto& kv : cfg.hotWindow)
        if (kv.first.size() == typeName.size() &&
            std::equal(kv.first.begin(), kv.first.end(), typeName.begin(),
                       [](char a, char b) { return std::tolower(uint8_t(a)) == std::tolower(uint8_t(b)); }))
            return kv.second;
    return cfg.genericHotWindow;
}

int32_t ReaderLane::part(StmtCtx* s, uint32_t pid, PartSnap** out) {
    auto it = s->parts.find(pid);
    if (it != s->parts.end()) {
        *out = it->second.snap.get();
        return 0;
    }
    auto snap = std::make_shared<PartSnap>();
    const int32_t rc = store_->loadPart(pid, snap.get());
    if (rc < 0) return rc;
    StmtCtx::PartEntry e;
    e.seq = ++s->snapSeq;
    e.snap = snap;
    s->parts.emplace(pid, e);
    *out = snap.get();
    return 0;
}

int32_t ReaderLane::partCounters(StmtCtx* s, uint32_t pid, PartSnap** out) {
    auto it = s->parts.find(pid);
    if (it != s->parts.end()) {
        *out = it->second.snap.get();
        return 0;
    }
    auto ct = s->counterParts.find(pid);
    if (ct != s->counterParts.end()) {
        *out = ct->second.snap.get();
        return 0;
    }
    auto snap = std::make_shared<PartSnap>();
    const int32_t rc = store_->loadPart(pid, snap.get(), false);
    if (rc < 0) return rc;
    StmtCtx::PartEntry e;
    e.seq = ++s->snapSeq;
    e.snap = snap;
    s->counterParts.emplace(pid, e);
    *out = snap.get();
    return 0;
}

int32_t ReaderLane::type(StmtCtx* s, const uint8_t fid[4], TypeSnap** out) {
    const uint32_t key = fidU32(fid);
    auto it = s->types.find(key);
    if (it != s->types.end()) {
        *out = it->second.snap.get();
        return 0;
    }
    auto snap = std::make_shared<TypeSnap>();
    const int32_t rc = store_->loadType(fid, snap.get());
    if (rc < 0) return rc;
    StmtCtx::TypeEntry e;
    e.seq = ++s->snapSeq;
    e.snap = snap;
    s->types.emplace(key, e);
    *out = snap.get();
    return 0;
}

int32_t ReaderLane::partForType(StmtCtx* s, uint32_t pid, const uint8_t fid[4], PartSnap** out) {
    TypeSnap* ts = nullptr;
    int32_t rc = type(s, fid, &ts);
    if (rc < 0) return rc;
    auto it = s->typeParts.find(pid);
    if (it != s->typeParts.end()) {
        *out = it->second.snap.get();
        return 0;
    }
    // Taken after the type snapshot. A partition head is written right after
    // the commit its type owner may already have labeled; a head older than
    // labeled_through is re-read (bounded) so V_p never hides a promotion.
    auto snap = std::make_shared<PartSnap>();
    const uint64_t labeled = ts->labeledThrough(pid);
    for (int attempt = 0;; attempt++) {
        rc = store_->loadPart(pid, snap.get());
        if (rc < 0) return rc;
        if (snap->pseqHi() >= labeled) break;
        if (attempt >= 200) return kRsRetryable;
        sleepNs(20000);
    }
    StmtCtx::PartEntry e;
    e.seq = ++s->snapSeq;
    e.snap = snap;
    s->typeParts.emplace(pid, e);
    *out = snap.get();
    return 0;
}

void ReaderLane::dropSnapshots(StmtCtx* s) {
    // Only before the first row (§8.8). Cursors hold raw snapshot pointers,
    // so the old snapshots stay alive until the statement ends.
    for (auto& kv : s->parts) retired_.push_back(kv.second.snap);
    for (auto& kv : s->typeParts) retired_.push_back(kv.second.snap);
    for (auto& kv : s->types) retiredTypes_.push_back(kv.second.snap);
    s->parts.clear();
    s->typeParts.clear();
    s->types.clear();
}

int32_t ReaderLane::initConnection(std::string* err) {
    const ReaderConfig& cfg = config();
    const int rc = sqlite3_open_v2(":memory:", &db_,
                                   SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_MEMORY,
                                   kNullVfsName);
    if (rc != SQLITE_OK) {
        if (err) *err = std::string("lane open: ") + (db_ ? sqlite3_errmsg(db_) : sqlite3_errstr(rc));
        return rc == SQLITE_NOMEM ? kRsNoMem : kRsSqlError;
    }
    // Lookaside from the lane arena (pre-sized).
    if (cfg.lookasideSlots && cfg.lookasideSlotBytes) {
        void* buf = arena_.alloc(size_t(cfg.lookasideSlots) * cfg.lookasideSlotBytes);
        if (buf) sqlite3_db_config(db_, SQLITE_DBCONFIG_LOOKASIDE, buf, int(cfg.lookasideSlotBytes), int(cfg.lookasideSlots));
    }
    sqlite3_exec(db_, "PRAGMA temp_store=MEMORY", nullptr, nullptr, nullptr);
    sqlite3_limit(db_, SQLITE_LIMIT_ATTACHED, 0);
    const int vr = vtabRegister(db_, this);
    if (vr != SQLITE_OK) {
        if (err) *err = "lane: module registration failed";
        return kRsSqlError;
    }
    sqlite3_progress_handler(
        db_, 4096,
        [](void* p) -> int {
            ReaderLane* l = static_cast<ReaderLane*>(p);
            if (l->inst_->stopWord().load(std::memory_order_relaxed)) return 1;
            StmtCtx* s = l->cur_;
            if (!s) return 0;
            if (s->guard.cancel && s->guard.cancel->load(std::memory_order_relaxed)) return 1;
            if (s->guard.poll() < 0) return 1;
            if (s->sandbox()) {
                s->vmSteps += 4096;
                if (l->config().sandboxMaxVmSteps && s->vmSteps > l->config().sandboxMaxVmSteps) {
                    if (!s->vtabStatus) {
                        s->vtabStatus = kRsTimeout;
                        s->vtabMessage = "work budget exhausted (VM steps)";
                    }
                    return 1;
                }
            }
            return 0;
        },
        this);
    return 0;
}

void ReaderLane::closeConnection() {
    for (auto& a : active_)
        if (a->stmt) sqlite3_finalize(a->stmt);
    active_.clear();
    if (db_) sqlite3_close_v2(db_);
    db_ = nullptr;
    tables_.clear();
}

void ReaderLane::updateAnnounce() {
    uint64_t oldest = 0;
    for (const auto& a : active_)
        if (!a->finished && (oldest == 0 || a->ctx.startNs < oldest)) oldest = a->ctx.startNs;
    inst_->laneShared(id_).announce.store(oldest, std::memory_order_release);
}

namespace {
// Name after "no such table: " (schema prefix dropped).
bool missingTable(const char* msg, std::string* name) {
    static const char* kPrefix = "no such table: ";
    const char* at = msg ? std::strstr(msg, kPrefix) : nullptr;
    if (!at) return false;
    std::string n = at + std::strlen(kPrefix);
    const size_t dot = n.find('.');
    if (dot != std::string::npos && (n.compare(0, dot, "main") == 0 || n.compare(0, dot, "temp") == 0))
        n = n.substr(dot + 1);
    *name = n;
    return !n.empty();
}

int32_t mapSqlite(int rc, StmtCtx& ctx, const std::atomic<uint32_t>& stop) {
    if (ctx.vtabStatus) return ctx.vtabStatus;
    if (rc == SQLITE_NOMEM) return kRsNoMem;
    if (rc == SQLITE_INTERRUPT) {
        if (stop.load()) return kRsStopped;
        if (ctx.guard.cancel && ctx.guard.cancel->load()) return kRsCancelled;
        if (ctx.guard.poll() == kRsTimeout) return kRsTimeout;
        return kRsCancelled;
    }
    if (rc == SQLITE_AUTH) return kRsNotAuthorized;
    return kRsSqlError;
}
}  // namespace

int32_t ReaderLane::prepare(Active* a, const char* sql, size_t n, std::string* msg) {
    const bool sandbox = a->ctx.sandbox();
    SandboxAuth auth;
    auth.lane = this;
    auth.db = db_;
    for (int attempt = 0; attempt < 64; attempt++) {
        if (sandbox) sqlite3_set_authorizer(db_, sandboxAuthorizer, &auth);
        const char* tail = nullptr;
        sqlite3_stmt* st = nullptr;
        const int rc = sqlite3_prepare_v3(db_, sql, int(n), SQLITE_PREPARE_PERSISTENT, &st, &tail);
        if (sandbox) sqlite3_set_authorizer(db_, nullptr, nullptr);
        if (rc == SQLITE_OK) {
            if (!st) {
                *msg = "empty statement";
                return kRsSqlError;
            }
            if (sandbox) {
                for (const char* p = tail; p && p < sql + n && *p; p++)
                    if (!std::isspace(uint8_t(*p))) {
                        sqlite3_finalize(st);
                        *msg = "sandbox: multi-statement: exactly one SELECT statement is allowed";
                        return kRsNotAuthorized;
                    }
                if (!sqlite3_stmt_readonly(st)) {
                    sqlite3_finalize(st);
                    *msg = "sandbox: read-only: statement could modify the database";
                    return kRsNotAuthorized;
                }
                if (sqlite3_column_count(st) <= 0) {
                    sqlite3_finalize(st);
                    *msg = "sandbox: not-select: statement returns no result columns";
                    return kRsNotAuthorized;
                }
            }
            a->stmt = st;
            return 0;
        }
        const std::string err = sqlite3_errmsg(db_);
        if (st) sqlite3_finalize(st);
        if (sandbox && !auth.violation.empty()) {
            *msg = "sandbox: not-authorized: " + auth.violation;
            return kRsNotAuthorized;
        }
        std::string table;
        if (rc != SQLITE_NOMEM && missingTable(err.c_str(), &table)) {
            std::string e2;
            const int made = vtabEnsure(this, table, sandbox, &e2);
            if (made < 0) {
                *msg = e2;
                return made;
            }
            if (made == 1) continue;  // retry with the table in place
        }
        *msg = err;
        if (rc == SQLITE_NOMEM) return kRsNoMem;
        return kRsSqlError;
    }
    *msg = "too many tables";
    return kRsSqlError;
}

void ReaderLane::finishStatement(Active* a, int32_t status, const std::string& msg) {
    if (a->finished) return;
    a->finished = true;
    a->status = status;
    a->msg = msg;
    if (a->stmt) {
        sqlite3_finalize(a->stmt);  // releases cursors, snapshots and arena memory now
        a->stmt = nullptr;
    }
    if (!a->raw) {
        rb1::Encoder enc(&a->pending);
        enc.end(status, a->rows, a->ctx.stats.rowsExamined, a->ctx.stats.bytesRead);
    }
    if (status == kRsNeedsBulk) inst_->cNeedsBulk.fetch_add(1, std::memory_order_relaxed);
    else if (status == kRsNoMem) inst_->cNoMem.fetch_add(1, std::memory_order_relaxed);
    else if (status == kRsSnapshotGone) inst_->cSnapshotGone.fetch_add(1, std::memory_order_relaxed);
    else if (status == kRsCancelled) inst_->cCancelled.fetch_add(1, std::memory_order_relaxed);
    else if (status == kRsTimeout) inst_->cTimeouts.fetch_add(1, std::memory_order_relaxed);
    else if (status < 0) inst_->cErrors.fetch_add(1, std::memory_order_relaxed);
    inst_->cStatements.fetch_add(1, std::memory_order_relaxed);
    a->ctx.parts.clear();
    a->ctx.typeParts.clear();
    a->ctx.counterParts.clear();
    a->ctx.types.clear();
    retired_.clear();
    retiredTypes_.clear();
    updateAnnounce();
}

bool ReaderLane::flushPending(Active* a) {
    SlotHeader* h = a->h;
    uint8_t* ring = inst_->slotRing(a->slot);
    const uint64_t cap = h->ringCap;
    while (a->pendingAt < a->pending.size()) {
        const uint64_t head = h->ringHead.load(std::memory_order_acquire);
        const uint64_t tail = h->ringTail.load(std::memory_order_relaxed);
        const uint64_t space = cap - (tail - head);
        if (space == 0) return false;
        const size_t want = size_t(std::min<uint64_t>(space, a->pending.size() - a->pendingAt));
        const size_t at = size_t(tail % cap);
        const size_t first = std::min(want, size_t(cap) - at);
        std::memcpy(ring + at, a->pending.data() + a->pendingAt, first);
        if (want > first) std::memcpy(ring, a->pending.data() + a->pendingAt + first, want - first);
        h->ringTail.store(tail + want, std::memory_order_release);
        a->pendingAt += want;
        a->outBytes += want;
        h->outSeq.fetch_add(1, std::memory_order_release);
        wakeU32(&h->outSeq, -1);
    }
    a->pending.clear();
    a->pendingAt = 0;
    return true;
}

void ReaderLane::runStatement(Active* a) {
    a->parked = false;
    SlotHeader* h = a->h;
    cur_ = &a->ctx;
    store_->beginStatement(&a->ctx.stats, &a->ctx.guard);
    const std::atomic<uint32_t>& stop = inst_->stopWord();
    auto park = [&] {
        a->parked = true;
        h->state.store(kSlotParked, std::memory_order_release);
        inst_->cParks.fetch_add(1, std::memory_order_relaxed);
    };
    if (!flushPending(a)) {
        if (h->cancel.load(std::memory_order_acquire) && !a->finished) {
            a->pending.clear();
            a->pendingAt = 0;
            finishStatement(a, kRsCancelled, "cancelled");
        } else {
            park();
            cur_ = nullptr;
            store_->endStatement();
            return;
        }
    }
    h->state.store(kSlotRunning, std::memory_order_release);
    while (!a->finished) {
        const int rc = sqlite3_step(a->stmt);
        if (rc == SQLITE_ROW) {
            a->rows++;
            if (a->raw) {
                for (int i = 0; i < a->ncols; i++) {
                    if (sqlite3_column_type(a->stmt, i) != SQLITE_BLOB) {
                        finishStatement(a, kRsSqlError,
                                        "not-a-record-stream: raw stream queries must return only BLOB cells");
                        break;
                    }
                    const void* b = sqlite3_column_blob(a->stmt, i);
                    const int n = sqlite3_column_bytes(a->stmt, i);
                    rb1::rawFrame(b, size_t(n), &a->pending);
                }
                if (a->finished) break;
            } else {
                rb1::Encoder enc(&a->pending);
                enc.beginRow();
                for (int i = 0; i < a->ncols; i++) {
                    switch (sqlite3_column_type(a->stmt, i)) {
                        case SQLITE_INTEGER: enc.i64(sqlite3_column_int64(a->stmt, i)); break;
                        case SQLITE_FLOAT: enc.real(sqlite3_column_double(a->stmt, i)); break;
                        case SQLITE_TEXT:
                            enc.text(sqlite3_column_text(a->stmt, i), size_t(sqlite3_column_bytes(a->stmt, i)));
                            break;
                        case SQLITE_BLOB:
                            enc.blob(sqlite3_column_blob(a->stmt, i), size_t(sqlite3_column_bytes(a->stmt, i)));
                            break;
                        default: enc.null(); break;
                    }
                }
                enc.endRow();
                enc.flushBlock();
            }
            if (a->ctx.vtabStatus) {
                finishStatement(a, a->ctx.vtabStatus, a->ctx.vtabMessage);
                break;
            }
            if (h->maxResultRows && a->rows > h->maxResultRows) {
                a->pending.clear();
                a->pendingAt = 0;
                finishStatement(a, kRsTimeout, "row-cap: result exceeds the row limit");
                break;
            }
            if (h->maxResultBytes && a->outBytes + a->pending.size() > h->maxResultBytes) {
                a->pending.clear();
                a->pendingAt = 0;
                finishStatement(a, kRsTimeout, "byte-cap: result exceeds the byte limit");
                break;
            }
            if (a->pending.size() - a->pendingAt >= (64u << 10) && !flushPending(a)) {
                park();  // A28: never block on output
                cur_ = nullptr;
                store_->endStatement();
                return;
            }
            continue;
        }
        if (rc == SQLITE_DONE) {
            finishStatement(a, a->ctx.vtabStatus ? a->ctx.vtabStatus : 0,
                            a->ctx.vtabStatus ? a->ctx.vtabMessage : std::string());
            break;
        }
        const int32_t status = mapSqlite(rc, a->ctx, stop);
        std::string msg = a->ctx.vtabStatus ? a->ctx.vtabMessage : std::string(sqlite3_errmsg(db_));
        if (status == kRsTimeout && msg.find("budget") == std::string::npos)
            msg = "timeout: statement exceeded its work budget";
        finishStatement(a, status, msg);
        break;
    }
    // Drain what remains of the output.
    if (!flushPending(a)) {
        park();
    } else {
        h->status = a->status;
        const size_t ml = std::min(a->msg.size(), sizeof(h->err) - 1);
        std::memcpy(h->err, a->msg.data(), ml);
        h->err[ml] = 0;
        h->errLen = uint32_t(ml);
        h->rowsOut = a->rows;
        h->rowsExamined = a->ctx.stats.rowsExamined;
        h->bytesRead = a->ctx.stats.bytesRead;
        h->indexEntries = a->ctx.stats.indexEntries;
        h->fenceReads = a->ctx.stats.fenceReads;
        h->endNs = monoNs();
        h->state.store(kSlotDone, std::memory_order_release);
        h->outSeq.fetch_add(1, std::memory_order_release);
        wakeU32(&h->outSeq, -1);
        a->slot = UINT32_MAX;  // done: removed by the loop
    }
    cur_ = nullptr;
    store_->endStatement();
}

bool ReaderLane::startStatement(uint32_t slot) {
    std::unique_ptr<Active> a(new Active());
    a->slot = slot;
    a->h = inst_->slot(slot);
    SlotHeader* h = a->h;
    const ReaderConfig& cfg = config();
    h->lane = id_;
    h->startNs = monoNs();
    h->state.store(kSlotRunning, std::memory_order_release);
    StmtCtx& ctx = a->ctx;
    ctx.slot = slot;
    ctx.flags = h->flags;
    ctx.cls = cfg.cls;
    ctx.startNs = h->startNs;
    ctx.guard.cancel = &h->cancel;
    ctx.guard.stop = &inst_->stopWord();
    ctx.guard.stats = &ctx.stats;
    if (ctx.sandbox()) {
        ctx.guard.maxRowsExamined = h->maxRowsExamined ? h->maxRowsExamined : cfg.sandboxMaxRowsExamined;
        ctx.guard.maxBytesRead = h->maxBytesRead ? h->maxBytesRead : cfg.sandboxMaxBytesRead;
    } else {
        ctx.guard.maxRowsExamined = h->maxRowsExamined;
        ctx.guard.maxBytesRead = h->maxBytesRead;
    }
    a->raw = (h->flags & kReqRawStream) != 0;
    Active* ap = a.get();
    active_.push_back(std::move(a));
    updateAnnounce();
    if (cfg.testStallNs) sleepNs(cfg.testStallNs);
    cur_ = &ctx;
    store_->beginStatement(&ctx.stats, &ctx.guard);
    auto early = [&](int32_t status, const std::string& msg) {
        finishStatement(ap, status, msg);
        cur_ = nullptr;
        store_->endStatement();
        runStatement(ap);  // drains the end marker and publishes DONE
    };
    // §8 step 1: the registry view (first statement: STORE and MIGRATED).
    int32_t rc = storeOpened_ ? store_->refreshRegistry() : store_->open();
    if (rc >= 0) storeOpened_ = true;
    if (rc < 0) {
        early(rc == FLATSQL_IO_ERR_NOENT ? kRsSqlError : rc, "store unavailable");
        return true;
    }
    ctx.reg = store_->registry();
    const uint8_t* req = inst_->slotReq(slot);
    if (uint64_t(h->sqlLen) + h->paramsLen > cfg.reqBytes) {
        early(kRsTooLarge, "request larger than its slot");
        return true;
    }
    std::vector<rb1::Cell> params;
    if (!rb1::decodeParams(req + h->sqlLen, h->paramsLen, &params)) {
        early(kRsSqlError, "malformed parameters");
        return true;
    }
    if (ctx.sandbox() && cfg.cls == LaneClass::Bulk) {
        // A28: untrusted SQL never runs on the shared bulk lane.
        early(kRsNeedsBulk, "needs-bulk: untrusted SQL never runs on the shared bulk lane");
        return true;
    }
    std::string msg;
    rc = prepare(ap, reinterpret_cast<const char*>(req), h->sqlLen, &msg);
    if (rc < 0) {
        early(rc, msg);
        return true;
    }
    // Admission (§9): interactive lanes (and sandboxed statements on them)
    // take index-bounded plans only; the dedicated sandbox lane admits any
    // plan under its work budget.
    const bool admit = cfg.cls == LaneClass::Interactive;
    if (admit) {
        bool bounded = true;
        std::string table;
        rc = planBounded(ap->stmt, &bounded, &table);
        if (rc < 0) {
            early(rc, "admission failed");
            return true;
        }
        if (!bounded) {
            early(kRsNeedsBulk, "needs-bulk: the plan is not index-bounded" + (table.empty() ? "" : " (" + table + ")"));
            return true;
        }
    }
    const int np = sqlite3_bind_parameter_count(ap->stmt);
    if (np != int(params.size())) {
        early(kRsSqlError, "parameter count mismatch: statement expects " + std::to_string(np));
        return true;
    }
    for (size_t i = 0; i < params.size(); i++) {
        const rb1::Cell& c = params[i];
        const int idx = int(i + 1);
        int brc;
        switch (c.type) {
            case rb1::kInt: brc = sqlite3_bind_int64(ap->stmt, idx, c.i); break;
            case rb1::kReal: brc = sqlite3_bind_double(ap->stmt, idx, c.d); break;
            case rb1::kText: brc = sqlite3_bind_text(ap->stmt, idx, c.s.data(), int(c.s.size()), SQLITE_TRANSIENT); break;
            case rb1::kBlob: brc = sqlite3_bind_blob(ap->stmt, idx, c.s.data(), int(c.s.size()), SQLITE_TRANSIENT); break;
            default: brc = sqlite3_bind_null(ap->stmt, idx); break;
        }
        if (brc != SQLITE_OK) {
            early(brc == SQLITE_NOMEM ? kRsNoMem : kRsSqlError, sqlite3_errmsg(db_));
            return true;
        }
    }
    ap->ncols = sqlite3_column_count(ap->stmt);
    if (!ap->raw) {
        std::vector<std::string> names;
        for (int i = 0; i < ap->ncols; i++) {
            const char* n = sqlite3_column_name(ap->stmt, i);
            names.emplace_back(n ? n : "");
        }
        rb1::Encoder enc(&ap->pending);
        enc.header(names);
    }
    cur_ = nullptr;
    store_->endStatement();
    runStatement(ap);
    return true;
}

void ReaderLane::threadMain() {
    setThreadClass(ThreadClass::Lane);
    laneArenaBind(&arena_);
    LaneShared& sh = inst_->laneShared(id_);
    const ReaderConfig& cfg = config();
#if !defined(__wasm__)
    if (cfg.cls == LaneClass::Bulk) {
#if defined(__linux__)
        setpriority(PRIO_PROCESS, int(syscall(SYS_gettid)), 10);  // §5.3: bulk lanes nice +10
#elif defined(__APPLE__)
        pthread_set_qos_class_self_np(QOS_CLASS_UTILITY, 0);
#endif
    }
#endif
    std::string err;
    if (initConnection(&err) < 0) {
        sh.state.store(2, std::memory_order_release);
        laneArenaBind(nullptr);
        return;
    }
    for (;;) {
        sh.heartbeat.fetch_add(1, std::memory_order_relaxed);
        if (inst_->stopWord().load(std::memory_order_acquire)) break;
        if (canary_ && *canary_ != kCanary) {
            // A30: the stack canary is gone: stop this lane (a trap).
            sh.canary.fetch_add(1, std::memory_order_relaxed);
            break;
        }
        bool did = false;
        for (size_t i = 0; i < active_.size(); i++) {
            Active* a = active_[i].get();
            if (!a->parked) continue;
            SlotHeader* h = a->h;
            const uint64_t used = h->ringTail.load(std::memory_order_relaxed) - h->ringHead.load(std::memory_order_acquire);
            if (used < h->ringCap || h->cancel.load(std::memory_order_acquire)) {
                runStatement(a);
                did = true;
            }
        }
        active_.erase(std::remove_if(active_.begin(), active_.end(),
                                     [](const std::unique_ptr<Active>& a) { return a->slot == UINT32_MAX; }),
                      active_.end());
        if (active_.size() <= cfg.maxParked) {
            uint32_t slot;
            if (inst_->dequeue(&slot)) {
                startStatement(slot);
                did = true;
                active_.erase(std::remove_if(active_.begin(), active_.end(),
                                             [](const std::unique_ptr<Active>& a) { return a->slot == UINT32_MAX; }),
                              active_.end());
            }
        }
        if (did) continue;
        const uint32_t seq = sh.doorbell.load(std::memory_order_acquire);
        sh.state.store(active_.empty() ? 0u : 3u, std::memory_order_seq_cst);
        bool ready = active_.size() <= cfg.maxParked && inst_->queued();
        for (const auto& a : active_) {
            const uint64_t used =
                a->h->ringTail.load(std::memory_order_relaxed) - a->h->ringHead.load(std::memory_order_acquire);
            if (used < a->h->ringCap || a->h->cancel.load(std::memory_order_acquire)) ready = true;
        }
        if (!ready && !inst_->stopWord().load(std::memory_order_acquire))
            waitU32(&sh.doorbell, seq, 20ull * 1000 * 1000);
        sh.state.store(1, std::memory_order_seq_cst);
    }
    // Stop: every running or parked statement ends with STOPPED.
    for (auto& a : active_) {
        if (!a->finished) finishStatement(a.get(), kRsStopped, "stopped");
        a->pending.clear();
        a->pendingAt = 0;
        SlotHeader* h = a->h;
        h->status = a->status;
        h->state.store(kSlotDone, std::memory_order_release);
        h->outSeq.fetch_add(1, std::memory_order_release);
        wakeU32(&h->outSeq, -1);
    }
    closeConnection();
    laneArenaBind(nullptr);
    sh.state.store(2, std::memory_order_release);
}

// ---------------------------------------------------------------------------
// ReaderInstance
// ---------------------------------------------------------------------------
int32_t ReaderInstance::open(const ReaderConfig& cfgIn, std::unique_ptr<ReaderInstance>* out, std::string* err) {
    int32_t rc = readerSqliteInit(err);
    if (rc < 0) return rc;
    std::unique_ptr<ReaderInstance> inst(new ReaderInstance());
    ReaderConfig& cfg = inst->cfg_;
    cfg = cfgIn;
    if (!cfg.io) cfg.io = importIo();
    if (cfg.lanes == 0) cfg.lanes = 1;
    if (cfg.lanes > 64) cfg.lanes = 64;
    if (!cfg.arenaBytes) cfg.arenaBytes = cfg.cls == LaneClass::Bulk ? (128ull << 20) : (8ull << 20);
    if (cfg.stackBytes < (1ull << 20)) cfg.stackBytes = 1ull << 20;  // A30
    if (!cfg.slots) cfg.slots = cfg.lanes * (cfg.maxParked + 1) + 16;
    if (cfg.reqBytes < 4096) cfg.reqBytes = 4096;
    if (cfg.ringBytes < 4096) cfg.ringBytes = 4096;
    inst->nSlots_ = cfg.slots;
    inst->slotStride_ = align64(sizeof(SlotHeader) + cfg.reqBytes + cfg.ringBytes);
    inst->slotBytes_ = inst->slotStride_ * cfg.slots;
#if !defined(__wasm__)
    void* mem = mmap(nullptr, inst->slotBytes_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (mem == MAP_FAILED) {
        if (err) *err = "slot memory";
        return kRsNoMem;
    }
#else
    void* mem = std::aligned_alloc(64, inst->slotBytes_);
    if (!mem) return kRsNoMem;
    std::memset(mem, 0, inst->slotBytes_);
#endif
    inst->slotBase_ = static_cast<uint8_t*>(mem);
    for (uint32_t i = 0; i < cfg.slots; i++) {
        SlotHeader* h = new (inst->slot(i)) SlotHeader();
        h->state.store(kSlotFree);
        h->cancel.store(0);
        h->outSeq.store(0);
        h->spaceSeq.store(0);
        h->ringHead.store(0);
        h->ringTail.store(0);
        h->reqCap = cfg.reqBytes;
        h->ringCap = cfg.ringBytes;
    }
    static_assert(sizeof(QCell) == 16, "the C ABI documents 16-byte queue cells");
    uint64_t qcap = 1;
    while (qcap < uint64_t(cfg.slots) * 2) qcap <<= 1;
    inst->q_.reset(new QCell[qcap]);
    for (uint64_t i = 0; i < qcap; i++) inst->q_[i].seq.store(i);
    inst->qMask_ = qcap - 1;
    inst->shared_.reset(new LaneShared[cfg.lanes]);
    inst->cache_.reset(new ReaderCache(cfg.cacheBytes));
    for (uint32_t i = 0; i < cfg.lanes; i++) {
        inst->lanes_.emplace_back(new ReaderLane(inst.get(), i));
        if (!inst->lanes_.back()->arena_.init(cfg.arenaBytes)) {
            if (err) *err = "lane arena reservation failed";
            return kRsNoMem;
        }
    }
    *out = std::move(inst);
    return 0;
}

ReaderInstance::~ReaderInstance() {
    stop();
    lanes_.clear();
#if !defined(__wasm__)
    if (slotBase_) munmap(slotBase_, slotBytes_);
#else
    std::free(slotBase_);
#endif
}

namespace {
void* laneEntry(void* p) {
    static_cast<ReaderLane*>(p)->runThread();
    return nullptr;
}
}  // namespace

void ReaderLane::runThread() { threadMain(); }

int32_t ReaderInstance::start() {
    if (started_) return 0;
    for (auto& l : lanes_) {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
#if !defined(__wasm__)
        // A30: explicit lane stacks with a guard page and a canary word.
        const size_t page = size_t(sysconf(_SC_PAGESIZE));
        const size_t bytes = (size_t(cfg_.stackBytes) + page - 1) / page * page + page;
        void* st = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
        if (st == MAP_FAILED) return kRsNoMem;
        mprotect(st, page, PROT_NONE);
        l->stack_ = st;
        l->stackBytes_ = bytes;
        l->canary_ = reinterpret_cast<volatile uint64_t*>(static_cast<uint8_t*>(st) + page);
        *l->canary_ = kCanary;
        pthread_attr_setstack(&attr, static_cast<uint8_t*>(st) + page, bytes - page);
#else
        pthread_attr_setstacksize(&attr, size_t(cfg_.stackBytes));
#endif
        const int rc = pthread_create(&l->thread_, &attr, laneEntry, l.get());
        pthread_attr_destroy(&attr);
        if (rc != 0) return kRsNoMem;
        l->started_ = true;
    }
    started_ = true;
    return 0;
}

int32_t ReaderInstance::stop(uint64_t deadlineMs) {
    if (!started_) return 0;
    stop_.store(1, std::memory_order_release);
    for (uint32_t i = 0; i < lanes_.size(); i++) wakeLane(i);
    const uint64_t until = monoNs() + deadlineMs * 1000000ull;
    bool all = true;
    for (uint32_t i = 0; i < lanes_.size(); i++) {
        while (shared_[i].state.load(std::memory_order_acquire) != 2 && monoNs() < until) {
            wakeLane(i);
            sleepNs(1000000);
        }
        if (shared_[i].state.load() != 2) all = false;
    }
    for (auto& l : lanes_) {
        if (!l->started_) continue;
        if (all) pthread_join(l->thread_, nullptr);
        else pthread_detach(l->thread_);
        l->started_ = false;
    }
    started_ = false;
    return all ? 0 : kRsStopped;
}

int32_t ReaderInstance::enqueue(uint32_t s) {
    uint64_t pos = qEnq_.load(std::memory_order_relaxed);
    for (;;) {
        QCell& c = q_[pos & qMask_];
        const uint64_t seq = c.seq.load(std::memory_order_acquire);
        const int64_t dif = int64_t(seq) - int64_t(pos);
        if (dif == 0) {
            if (qEnq_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                c.value = s;
                c.seq.store(pos + 1, std::memory_order_release);
                break;
            }
        } else if (dif < 0) {
            return kRsBusy;
        } else {
            pos = qEnq_.load(std::memory_order_relaxed);
        }
    }
    wakeIdleLane();
    return 0;
}

bool ReaderInstance::dequeue(uint32_t* s) {
    uint64_t pos = qDeq_.load(std::memory_order_relaxed);
    for (;;) {
        QCell& c = q_[pos & qMask_];
        const uint64_t seq = c.seq.load(std::memory_order_acquire);
        const int64_t dif = int64_t(seq) - int64_t(pos + 1);
        if (dif == 0) {
            if (qDeq_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                *s = c.value;
                c.seq.store(pos + qMask_ + 1, std::memory_order_release);
                return true;
            }
        } else if (dif < 0) {
            return false;
        } else {
            pos = qDeq_.load(std::memory_order_relaxed);
        }
    }
}

bool ReaderInstance::queued() const {
    return qEnq_.load(std::memory_order_acquire) != qDeq_.load(std::memory_order_acquire);
}

void ReaderInstance::wakeLane(uint32_t i) {
    shared_[i].doorbell.fetch_add(1, std::memory_order_release);
    wakeU32(&shared_[i].doorbell, -1);
}

void ReaderInstance::wakeIdleLane() {
    int pick = -1;
    for (uint32_t i = 0; i < lanes_.size(); i++) {
        const uint32_t s = shared_[i].state.load(std::memory_order_seq_cst);
        if (s == 0) {
            pick = int(i);
            break;
        }
        if (s == 3 && pick < 0) pick = int(i);
    }
    if (pick >= 0) wakeLane(uint32_t(pick));
}

uint64_t ReaderInstance::oldestActiveStart() const {
    uint64_t oldest = UINT64_MAX;
    for (uint32_t i = 0; i < lanes_.size(); i++) {
        const uint64_t a = shared_[i].announce.load(std::memory_order_acquire);
        if (a && a < oldest) oldest = a;
    }
    return oldest;
}

ReaderStats ReaderInstance::stats() const {
    ReaderStats s;
    s.statements = cStatements.load();
    s.parks = cParks.load();
    s.needsBulk = cNeedsBulk.load();
    s.noMem = cNoMem.load();
    s.snapshotGone = cSnapshotGone.load();
    s.cancelled = cCancelled.load();
    s.timeouts = cTimeouts.load();
    s.errors = cErrors.load();
    return s;
}

// ---------------------------------------------------------------------------
// ReaderClient
// ---------------------------------------------------------------------------
int32_t ReaderClient::submit(const Request& req, uint32_t* slotOut, uint64_t waitNs) {
    std::vector<uint8_t> params;
    std::vector<rb1::Cell> cells;
    for (const Param& p : req.params) {
        rb1::Cell c;
        c.type = p.type == Param::kInt ? rb1::kInt : p.type == Param::kReal ? rb1::kReal
               : p.type == Param::kText ? rb1::kText
               : p.type == Param::kBlob ? rb1::kBlob
                                        : rb1::kNull;
        c.i = p.i;
        c.d = p.d;
        c.s = p.s;
        cells.push_back(std::move(c));
    }
    if (!cells.empty()) rb1::encodeParams(cells, &params);
    const ReaderConfig& cfg = inst_->config();
    if (req.sql.size() + params.size() > cfg.reqBytes) return kRsTooLarge;
    const uint64_t until = monoNs() + waitNs;
    const uint32_t n = inst_->slotCount();
    for (;;) {
        const uint32_t start = inst_->claimHint_.fetch_add(1, std::memory_order_relaxed);
        for (uint32_t k = 0; k < n; k++) {
            const uint32_t i = (start + k) % n;
            SlotHeader* h = inst_->slot(i);
            uint32_t expect = kSlotFree;
            if (!h->state.compare_exchange_strong(expect, kSlotClaimed, std::memory_order_acq_rel)) continue;
            uint8_t* rq = inst_->slotReq(i);
            std::memcpy(rq, req.sql.data(), req.sql.size());
            if (!params.empty()) std::memcpy(rq + req.sql.size(), params.data(), params.size());
            h->flags = req.flags;
            h->sqlLen = uint32_t(req.sql.size());
            h->paramsLen = uint32_t(params.size());
            h->maxRowsExamined = req.maxRowsExamined;
            h->maxBytesRead = req.maxBytesRead;
            h->maxResultRows = req.maxResultRows;
            h->maxResultBytes = req.maxResultBytes;
            h->ringHead.store(0, std::memory_order_relaxed);
            h->ringTail.store(0, std::memory_order_relaxed);
            h->cancel.store(0, std::memory_order_relaxed);
            h->status = 0;
            h->errLen = 0;
            h->err[0] = 0;
            h->rowsOut = h->rowsExamined = h->bytesRead = h->indexEntries = h->fenceReads = 0;
            h->submitNs = monoNs();
            h->startNs = h->endNs = 0;
            h->state.store(kSlotQueued, std::memory_order_release);
            const int32_t rc = inst_->enqueue(i);
            if (rc < 0) {
                h->state.store(kSlotFree, std::memory_order_release);
                return rc;
            }
            *slotOut = i;
            return 0;
        }
        if (monoNs() >= until) return kRsBusy;
        sleepNs(50000);
    }
}

int64_t ReaderClient::read(uint32_t slot, uint8_t* dst, size_t cap, uint64_t waitNs) {
    SlotHeader* h = inst_->slot(slot);
    const uint8_t* ring = inst_->slotRing(slot);
    const uint64_t until = monoNs() + waitNs;
    for (;;) {
        const uint32_t seq = h->outSeq.load(std::memory_order_acquire);
        const uint32_t state = h->state.load(std::memory_order_acquire);
        const uint64_t tail = h->ringTail.load(std::memory_order_acquire);
        const uint64_t head = h->ringHead.load(std::memory_order_relaxed);
        if (tail > head) {
            const uint64_t ringCap = h->ringCap;
            const size_t n = size_t(std::min<uint64_t>(cap, tail - head));
            const size_t at = size_t(head % ringCap);
            const size_t first = std::min(n, size_t(ringCap) - at);
            std::memcpy(dst, ring + at, first);
            if (n > first) std::memcpy(dst + first, ring, n - first);
            h->ringHead.store(head + n, std::memory_order_release);
            h->spaceSeq.fetch_add(1, std::memory_order_release);
            if (h->state.load(std::memory_order_acquire) == kSlotParked) inst_->wakeLane(h->lane);
            return int64_t(n);
        }
        if (state == kSlotDone) return 0;
        const uint64_t now = monoNs();
        if (now >= until) return kRsBusy;
        waitU32(&h->outSeq, seq, std::min<uint64_t>(until - now, 20ull * 1000 * 1000));
    }
}

void ReaderClient::cancel(uint32_t slot) {
    SlotHeader* h = inst_->slot(slot);
    h->cancel.store(1, std::memory_order_release);
    if (h->state.load(std::memory_order_acquire) == kSlotParked) inst_->wakeLane(h->lane);
}

Outcome ReaderClient::finish(uint32_t slot) {
    SlotHeader* h = inst_->slot(slot);
    Outcome o;
    o.status = h->status;
    o.error.assign(h->err, h->errLen);
    o.rowsOut = h->rowsOut;
    o.rowsExamined = h->rowsExamined;
    o.bytesRead = h->bytesRead;
    o.indexEntries = h->indexEntries;
    o.fenceReads = h->fenceReads;
    o.queueNs = h->startNs > h->submitNs ? h->startNs - h->submitNs : 0;
    o.runNs = h->endNs > h->startNs ? h->endNs - h->startNs : 0;
    h->state.store(kSlotFree, std::memory_order_release);
    return o;
}

Outcome ReaderClient::run(const Request& req, std::vector<uint8_t>* out, uint64_t readDelayNs, size_t readChunk) {
    uint32_t slot;
    const int32_t rc = submit(req, &slot);
    if (rc < 0) {
        Outcome o;
        o.status = rc;
        o.error = "submit failed";
        return o;
    }
    std::vector<uint8_t> buf(readChunk);
    for (;;) {
        const int64_t n = read(slot, buf.data(), buf.size(), 60ull * 1000 * 1000 * 1000);
        if (n == 0) break;
        if (n < 0) continue;  // keep waiting (bounded per call)
        if (out) out->insert(out->end(), buf.begin(), buf.begin() + n);
        if (readDelayNs) sleepNs(readDelayNs);
    }
    return finish(slot);
}

// ---------------------------------------------------------------------------
// CounterReader (A28: counters without a lane)
// ---------------------------------------------------------------------------
namespace {
LaneStoreConfig counterConfig(const std::string& root, Io* io) {
    LaneStoreConfig c;
    c.root = root;
    c.io = io;
    c.maxHandles = 4096;
    c.cacheBytes = 4u << 20;
    return c;
}
}  // namespace

CounterReader::CounterReader(const std::string& root, Io* io) : store_(counterConfig(root, io)) {}

int32_t CounterReader::partitions(std::vector<PartitionCounters>* out) {
    out->clear();
    int32_t rc = store_.open();
    if (rc < 0) return rc;
    rc = store_.refreshRegistry();
    if (rc < 0) return rc;
    auto reg = store_.registry();
    for (const PartInfo& pi : reg->parts) {
        if (!pi.pid || pi.dropped) continue;
        PartSnap s;
        rc = store_.loadPart(pi.pid, &s, false);
        if (rc < 0) return rc;
        PartitionCounters pc;
        pc.pid = pi.pid;
        pc.token = pi.token;
        pc.sqlName = pi.sqlName;
        if (const TypeInfo* t = reg->typeByFid(pi.fid)) pc.typeName = t->typeName;
        pc.empty = s.empty;
        if (!s.empty) {
            pc.pseqHi = s.head.pseqHi;
            pc.commitSeq = s.head.commitSeq;
            pc.c = s.head.counters;
            pc.quarantined = (s.head.p.flags & kHeadQuarantined) != 0;
        }
        pc.quarantined = pc.quarantined || pi.quarantined;
        out->push_back(std::move(pc));
    }
    return 0;
}

}  // namespace ps
}  // namespace flatsql
