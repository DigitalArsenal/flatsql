// Store format 4: staged rows and their merge (BRIEF4 ruling (B); owner,
// 2026-10-03: "stream them directly while indices and btrees (SQLite
// database metadata) were created in a separate thread in the data being
// streamed in").
//
// The ack commits a feed's new rows staged: in table r with m=0, which keeps
// them out of the feed's random-keyed indexes (r_c, r_ke, r_w are partial
// indexes WHERE m=1) and lists them in r_m; the ordered indexes take them at
// once. So the ack's transaction writes the stream's frames (synced before)
// and appended pages only. The writer's indexer thread later merges a feed's
// staged rows into those indexes in one large transaction (m=1): a batch of
// many rows touches each index page once instead of once per ack.
//
// A feed's in-memory view (Staged) holds what the reads that use those
// indexes need of each staged row. A view is immutable; each commit and
// each merge publishes a new one; a read pins one before its SQL runs. A row
// merged after a read's pin is in both the view and the index: the same row,
// so the same key and seq, and the read's per-record collapse takes it once.
// A row merged before the pin is in the index only; a staged one in the view
// only. Reads never wait on a merge.
#include <algorithm>
#include <cstdio>
#include <cstdlib>

#include "internal.h"

namespace flatsql {
namespace p4 {

int kCmp(const KVal& a, const KVal& b) {
    if (a.type != b.type) return a.type < b.type ? -1 : 1;
    if (a.type == 1) return a.i < b.i ? -1 : a.i > b.i;
    if (a.type == 3) {
        const int c = std::memcmp(a.s.data(), b.s.data(), std::min(a.s.size(), b.s.size()));
        if (c) return c < 0 ? -1 : 1;
        return a.s.size() < b.s.size() ? -1 : a.s.size() > b.s.size();
    }
    return 0;
}

namespace {

bool ridLess(const StRow* a, const StRow* b) { return a->rid < b->rid; }
bool cidLess(const StRow* a, const StRow* b) {
    const int c = std::memcmp(a->cid, b->cid, 32);
    return c ? c < 0 : a->rid < b->rid;
}
// SQLite's order of r_ke(k, e): k, then e (none first), then the rowid.
bool keLess(const StRow* a, const StRow* b) {
    const int c = kCmp(a->k, b->k);
    if (c) return c < 0;
    if (a->hasE != b->hasE) return !a->hasE;
    if (a->hasE && a->e != b->e) return a->e < b->e;
    return a->rid < b->rid;
}
bool wLess(const StRow* a, const StRow* b) { return a->w != b->w ? a->w < b->w : a->rid < b->rid; }

size_t runSize(const StRun& r) { return r.byRid.size(); }

std::shared_ptr<StRun> runOf(std::shared_ptr<const std::vector<StRow>> chunk) {
    auto r = std::make_shared<StRun>();
    for (const StRow& x : *chunk) r->byRid.push_back(&x);
    r->chunks.push_back(std::move(chunk));
    r->byCid = r->byKe = r->byW = r->byRid;
    std::sort(r->byRid.begin(), r->byRid.end(), ridLess);
    std::sort(r->byCid.begin(), r->byCid.end(), cidLess);
    std::sort(r->byKe.begin(), r->byKe.end(), keLess);
    std::sort(r->byW.begin(), r->byW.end(), wLess);
    return r;
}

std::shared_ptr<StRun> runMerge(const StRun& a, const StRun& b) {
    auto r = std::make_shared<StRun>();
    r->chunks = a.chunks;
    r->chunks.insert(r->chunks.end(), b.chunks.begin(), b.chunks.end());
    auto m = [](const std::vector<const StRow*>& x, const std::vector<const StRow*>& y, std::vector<const StRow*>* out,
                bool (*less)(const StRow*, const StRow*)) {
        out->resize(x.size() + y.size());
        std::merge(x.begin(), x.end(), y.begin(), y.end(), out->begin(), less);
    };
    m(a.byRid, b.byRid, &r->byRid, ridLess);
    m(a.byCid, b.byCid, &r->byCid, cidLess);
    m(a.byKe, b.byKe, &r->byKe, keLess);
    m(a.byW, b.byW, &r->byW, wLess);
    return r;
}

// A run without the rows of `gone` (sorted rids); the same run when it holds none.
std::shared_ptr<const StRun> runWithout(const std::shared_ptr<const StRun>& r, const std::vector<int64_t>& gone) {
    bool any = false;
    for (int64_t rid : gone) {
        StRow probe;
        probe.rid = rid;
        auto it = std::lower_bound(r->byRid.begin(), r->byRid.end(), &probe, ridLess);
        if (it != r->byRid.end() && (*it)->rid == rid) {
            any = true;
            break;
        }
    }
    if (!any) return r;
    auto keep = [&](const StRow* x) { return !std::binary_search(gone.begin(), gone.end(), x->rid); };
    auto n = std::make_shared<StRun>();
    n->chunks = r->chunks;
    for (auto* v : {&r->byRid, &r->byCid, &r->byKe, &r->byW}) {
        std::vector<const StRow*>& out = v == &r->byRid ? n->byRid : v == &r->byCid ? n->byCid : v == &r->byKe ? n->byKe : n->byW;
        out.reserve(v->size());
        for (const StRow* x : *v)
            if (keep(x)) out.push_back(x);
    }
    if (n->byRid.empty()) return nullptr;
    return n;
}

}  // namespace

std::shared_ptr<const Staged> Staged::make(const Staged* base, std::vector<StRow> add, const std::vector<int64_t>& drop,
                                           const std::vector<std::pair<IdentKey, int64_t>>& ids, bool dropIdents) {
    auto v = std::make_shared<Staged>();
    if (base) {
        v->runs = base->runs;
        if (!dropIdents) v->idents = base->idents;
    }
    if (!drop.empty() && !v->runs.empty()) {
        std::vector<int64_t> gone = drop;
        std::sort(gone.begin(), gone.end());
        std::vector<std::shared_ptr<const StRun>> runs;
        for (auto& r : v->runs)
            if (auto k = runWithout(r, gone)) runs.push_back(std::move(k));
        v->runs.swap(runs);
    }
    if (!add.empty()) {
        v->runs.push_back(runOf(std::make_shared<const std::vector<StRow>>(std::move(add))));
        // Runs merge as they grow (a run is merged into the one before it
        // while that one is at most twice its size): few runs, and each row
        // copied O(log n) times.
        while (v->runs.size() >= 2 && runSize(*v->runs[v->runs.size() - 2]) <= 2 * runSize(*v->runs.back())) {
            auto m = runMerge(*v->runs[v->runs.size() - 2], *v->runs.back());
            v->runs.pop_back();
            v->runs.back() = std::move(m);
        }
    }
    if (!ids.empty()) {
        std::map<IdentKey, int64_t> m(v->idents.begin(), v->idents.end());
        for (auto& kv : ids) m[kv.first] = kv.second;  // later wins (INSERT OR REPLACE in order)
        v->idents.assign(m.begin(), m.end());
    }
    for (auto& r : v->runs) v->rows += runSize(*r);
    if (v->rows == 0 && v->idents.empty()) return nullptr;
    return v;
}

void Staged::all(std::vector<const StRow*>* out) const {
    out->clear();
    for (auto& r : runs) out->insert(out->end(), r->byRid.begin(), r->byRid.end());
    std::sort(out->begin(), out->end(), ridLess);
}

void Staged::ofCid(const uint8_t key[32], std::vector<const StRow*>* out) const {
    out->clear();
    for (auto& r : runs) {
        auto it = std::lower_bound(r->byCid.begin(), r->byCid.end(), key,
                                   [](const StRow* x, const uint8_t* k) { return std::memcmp(x->cid, k, 32) < 0; });
        for (; it != r->byCid.end() && std::memcmp((*it)->cid, key, 32) == 0; ++it) out->push_back(*it);
    }
    std::sort(out->begin(), out->end(), ridLess);
}

void Staged::ofK(const KVal& k, std::vector<const StRow*>* out) const {
    out->clear();
    for (auto& r : runs) {
        auto it = std::lower_bound(r->byKe.begin(), r->byKe.end(), &k, [](const StRow* x, const KVal* kv) { return kCmp(x->k, *kv) < 0; });
        for (; it != r->byKe.end() && kCmp((*it)->k, k) == 0; ++it) out->push_back(*it);
    }
    std::sort(out->begin(), out->end(), keLess);
}

int64_t Staged::identSeq(const uint8_t h[32]) const {
    IdentKey key;
    std::memcpy(key.data(), h, 32);
    auto it = std::lower_bound(idents.begin(), idents.end(), key,
                               [](const std::pair<IdentKey, int64_t>& a, const IdentKey& b) { return a.first < b; });
    return it != idents.end() && it->first == key ? it->second : 0;
}

void Staged::wRange(bool desc, int64_t from, bool fromIncl, int64_t to, int64_t ridLo, int64_t ridHi, size_t limit,
                    std::vector<const StRow*>* out, bool* more) const {
    out->clear();
    *more = false;
    // Each run's position, then the runs merged in w order.
    struct Pos {
        const std::vector<const StRow*>* v;
        long at, end, step;
    };
    std::vector<Pos> ps;
    for (auto& r : runs) {
        const std::vector<const StRow*>& v = r->byW;
        auto wl = [](const StRow* x, int64_t w) { return x->w < w; };
        auto wu = [](int64_t w, const StRow* x) { return w < x->w; };
        if (!desc) {
            const long a = long((fromIncl ? std::lower_bound(v.begin(), v.end(), from, wl) : std::upper_bound(v.begin(), v.end(), from, wu)) -
                                v.begin());
            const long b = long(std::upper_bound(v.begin(), v.end(), to, wu) - v.begin());
            if (a < b) ps.push_back(Pos{&v, a, b, 1});
        } else {
            const long a = long((fromIncl ? std::upper_bound(v.begin(), v.end(), from, wu) : std::lower_bound(v.begin(), v.end(), from, wl)) -
                                v.begin()) - 1;
            const long b = long(std::lower_bound(v.begin(), v.end(), to, wl) - v.begin()) - 1;
            if (a > b) ps.push_back(Pos{&v, a, b, -1});
        }
    }
    bool haveLast = false;
    int64_t last = 0;
    for (;;) {
        int best = -1;
        for (size_t i = 0; i < ps.size(); i++) {
            if (ps[i].at == ps[i].end) continue;
            const StRow* x = (*ps[i].v)[size_t(ps[i].at)];
            if (best < 0) {
                best = int(i);
                continue;
            }
            const StRow* y = (*ps[size_t(best)].v)[size_t(ps[size_t(best)].at)];
            if (desc ? x->w > y->w : x->w < y->w) best = int(i);
        }
        if (best < 0) return;
        Pos& p = ps[size_t(best)];
        const StRow* x = (*p.v)[size_t(p.at)];
        if (out->size() >= limit && (!haveLast || x->w != last)) {
            *more = true;
            return;
        }
        p.at += p.step;
        if (x->rid < ridLo || x->rid > ridHi) continue;
        out->push_back(x);
        haveLast = true;
        last = x->w;
    }
}

void Staged::cidRange(const std::string& aCid, int64_t aRid, const uint8_t* bCid, int64_t bRid, int64_t ridLo, int64_t ridHi,
                      size_t limit, std::vector<const StRow*>* out, bool* more) const {
    out->clear();
    *more = false;
    StRow lo, hi;
    if (!aCid.empty()) std::memcpy(lo.cid, aCid.data(), std::min<size_t>(32, aCid.size()));
    lo.rid = aRid;
    if (bCid) {
        std::memcpy(hi.cid, bCid, 32);
        hi.rid = bRid;
    }
    struct Pos {
        const std::vector<const StRow*>* v;
        size_t at, end;
    };
    std::vector<Pos> ps;
    for (auto& r : runs) {
        const std::vector<const StRow*>& v = r->byCid;
        const size_t a = aCid.empty() ? 0 : size_t(std::upper_bound(v.begin(), v.end(), &lo, cidLess) - v.begin());
        const size_t b = bCid ? size_t(std::upper_bound(v.begin(), v.end(), &hi, cidLess) - v.begin()) : v.size();
        if (a < b) ps.push_back(Pos{&v, a, b});
    }
    for (;;) {
        int best = -1;
        for (size_t i = 0; i < ps.size(); i++) {
            if (ps[i].at == ps[i].end) continue;
            if (best < 0 || cidLess((*ps[i].v)[ps[i].at], (*ps[size_t(best)].v)[ps[size_t(best)].at])) best = int(i);
        }
        if (best < 0) return;
        if (out->size() >= limit) {
            *more = true;
            return;
        }
        const StRow* x = (*ps[size_t(best)].v)[ps[size_t(best)].at++];
        if (x->rid < ridLo || x->rid > ridHi) continue;
        out->push_back(x);
    }
}

void Staged::byObject(std::vector<std::pair<KVal, std::vector<const StRow*>>>* out) const {
    out->clear();
    std::vector<const StRow*> v;
    for (auto& r : runs) v.insert(v.end(), r->byKe.begin(), r->byKe.end());
    std::sort(v.begin(), v.end(), keLess);
    for (const StRow* x : v) {
        if (out->empty() || kCmp(out->back().first, x->k) != 0) out->push_back({x->k, {}});
        out->back().second.push_back(x);
    }
}

// ---- the view's lifecycle --------------------------------------------------------------------------
void stagedPublish(Engine* e, Feed* f, std::shared_ptr<const Staged> v, bool added) {
    const int64_t before = f->staged ? int64_t(f->staged->rows) : 0, after = v ? int64_t(v->rows) : 0;
    f->staged = std::move(v);
    if (added) f->stagedAt = monoNs();
    if (after != before) e->stagedRows.fetch_add(after - before, std::memory_order_relaxed);
}

namespace {
// A feed file's staged rows (r_m) and identities (idst), as its view holds them.
int32_t readStaged(Conn* c, std::vector<StRow>* out, std::vector<std::pair<IdentKey, int64_t>>* ids) {
    std::vector<StRow>& rows = *out;
    sqlite3_stmt* q = c->sql("SELECT rid, cid, e, k, ts, at FROM r INDEXED BY r_m WHERE m=0");
    if (!q) return P4_E_FORMAT;
    int r;
    while ((r = sqlite3_step(q)) == SQLITE_ROW) {
        if (sqlite3_column_bytes(q, 1) != 32) continue;
        StRow x;
        x.rid = sqlite3_column_int64(q, 0);
        std::memcpy(x.cid, sqlite3_column_blob(q, 1), 32);
        x.hasE = sqlite3_column_type(q, 2) != SQLITE_NULL;
        x.e = sqlite3_column_int64(q, 2);
        x.k.from(q, 3);
        x.ts = sqlite3_column_int64(q, 4);
        x.at = sqlite3_column_int64(q, 5);
        x.w = x.hasE ? x.e : x.ts;
        rows.push_back(std::move(x));
    }
    sqlite3_reset(q);
    if (r != SQLITE_DONE) return statusOfSqlite(r);
    q = c->sql("SELECT h, seq FROM idst ORDER BY rowid");
    if (!q) return P4_E_FORMAT;
    while ((r = sqlite3_step(q)) == SQLITE_ROW) {
        if (sqlite3_column_bytes(q, 0) != 32) continue;
        IdentKey h;
        std::memcpy(h.data(), sqlite3_column_blob(q, 0), 32);
        ids->push_back({h, sqlite3_column_int64(q, 1)});
    }
    sqlite3_reset(q);
    return r == SQLITE_DONE ? P4_OK : statusOfSqlite(r);
}
}  // namespace

int32_t stagedLoad(Engine* e, Feed* f, Conn* c, std::string* err) {
    std::vector<StRow> rows;
    std::vector<std::pair<IdentKey, int64_t>> ids;
    const int32_t st = readStaged(c, &rows, &ids);
    if (st == P4_E_FORMAT && err) *err = f->path + ": a feed index without staged rows (format 4 from an older engine)";
    if (st != P4_OK) return st;
    Type* t = f->type;
    std::lock_guard<std::mutex> g(t->mu);
    stagedPublish(e, f, Staged::make(nullptr, std::move(rows), {}, ids, true), true);
    return P4_OK;
}

int64_t stagedMismatches(Feed* f, Conn* c) {
    std::vector<StRow> rows;
    std::vector<std::pair<IdentKey, int64_t>> ids;
    if (readStaged(c, &rows, &ids) != P4_OK) return 1;
    std::shared_ptr<const Staged> disk = Staged::make(nullptr, std::move(rows), {}, ids, true), mem;
    {
        std::lock_guard<std::mutex> g(f->type->mu);
        mem = f->staged;
    }
    std::vector<const StRow*> a, b;
    if (disk) disk->all(&a);
    if (mem) mem->all(&b);
    int64_t bad = a.size() == b.size() ? 0 : 1;
    for (size_t i = 0; !bad && i < a.size(); i++) {
        const StRow& x = *a[i];
        const StRow& y = *b[i];
        if (x.rid != y.rid || x.w != y.w || x.hasE != y.hasE || (x.hasE && x.e != y.e) || x.at != y.at || x.ts != y.ts || !(x.k == y.k) ||
            std::memcmp(x.cid, y.cid, 32) != 0)
            bad = 1;
    }
    const std::vector<std::pair<IdentKey, int64_t>> none;
    if ((disk ? disk->idents : none) != (mem ? mem->idents : none)) bad++;
    return bad;
}

// ---- the merge ------------------------------------------------------------------------------------------
namespace {
// Native test builds: P4_MERGE_DEBUG=1 logs each merge's start and end on
// stderr (the kill-during-merge test times its kills by them).
void mergeLog(const char* what, const Feed* f, size_t rows, int rc) {
#if !defined(__wasm__)
    if (!std::getenv("P4_MERGE_DEBUG")) return;
    std::fprintf(stderr, "%s %s rows %zu rc %d\n", what, f->path.c_str(), rows, rc);
    std::fflush(stderr);
#else
    (void)what; (void)f; (void)rows; (void)rc;
#endif
}
}  // namespace

int32_t mergeFeed(Engine* e, Type* t, Feed* f, std::string* err) {
    std::shared_ptr<const Staged> v;
    {
        std::lock_guard<std::mutex> g(t->mu);
        if (!f->created || f->quarantined) return P4_OK;
        v = f->staged;
    }
    if (!v) return P4_OK;
    // CID order: the CID index's pages are then filled one after the other
    // (the largest random-keyed index; a page that left the cache is not
    // touched again by this transaction).
    std::vector<const StRow*> rows;
    v->all(&rows);
    std::sort(rows.begin(), rows.end(), [](const StRow* a, const StRow* b) {
        const int c = std::memcmp(a->cid, b->cid, 32);
        return c ? c < 0 : a->rid < b->rid;
    });
    const size_t cap = size_t(std::max<uint32_t>(e->cfg.flushEntries, 1024)) * 2;
    if (rows.size() > cap) rows.resize(cap);
    int32_t st = P4_OK;
    std::string er;
    Conn* c = writerPin(e, f, &st, &er);
    if (!c) {
        if (err) *err = "merge open " + f->path + ": " + er;
        return st;
    }
    // The transaction's pages stay in memory up to its budget (a quarter of
    // the hard heap, at most 256 MiB): each is written once, at the commit.
    const uint64_t budgetKiB = std::min<uint64_t>(256ull << 10, (e->cfg.hardHeap >> 10) / 4);
    char sql[96];
    std::snprintf(sql, sizeof sql, "PRAGMA cache_spill=-%llu", (unsigned long long)std::max<uint64_t>(budgetKiB, kWriterSpillKiB));
    c->exec(sql);
    int rc = c->exec("BEGIN IMMEDIATE");
    mergeLog("merge-begin", f, rows.size(), rc);
    sqlite3_stmt* q = rc == SQLITE_OK ? c->sql("UPDATE r SET m=1 WHERE rid=?1 AND m=0") : nullptr;
    if (rc == SQLITE_OK && !q) rc = SQLITE_ERROR;
    std::vector<int64_t> merged;
    merged.reserve(rows.size());
    for (const StRow* x : rows) {
        if (rc != SQLITE_OK) break;
        sqlite3_bind_int64(q, 1, x->rid);
        const int r = sqlite3_step(q);
        sqlite3_reset(q);
        if (r != SQLITE_DONE) rc = r;
        else if (sqlite3_changes(c->db) != 1) rc = SQLITE_CORRUPT;  // the view names a row the file does not stage
        merged.push_back(x->rid);
    }
    const bool ids = !v->idents.empty();
    if (rc == SQLITE_OK && ids) rc = c->exec("INSERT OR REPLACE INTO ident(h, seq) SELECT h, seq FROM idst ORDER BY rowid; DELETE FROM idst");
    if (rc == SQLITE_OK) rc = c->exec("COMMIT");
    mergeLog("merge-end", f, rows.size(), rc);
    if (rc != SQLITE_OK) {
        if (err) *err = "merge " + f->path + ": " + sqlite3_errmsg(c->db) + " (" + std::to_string(rc) + ")";
        c->exec("ROLLBACK");
    }
    std::snprintf(sql, sizeof sql, "PRAGMA cache_spill=-%u", kWriterSpillKiB);
    c->exec(sql);
    int64_t freeBytes = 0;
    const int64_t dbBytes = rc == SQLITE_OK ? dbBytesOf(c, &freeBytes) : -1;
    writerUnpin(e, f);
    if (rc != SQLITE_OK) return statusOfSqlite(rc);
    e->bump(kStIndexFlushes);
    e->bump(kStIndexFlushEntries, merged.size());
    std::lock_guard<std::mutex> g(t->mu);
    if (dbBytes >= 0) {
        f->dbBytes = dbBytes;
        f->freeBytes = freeBytes;
    }
    stagedPublish(e, f, Staged::make(f->staged.get(), {}, merged, {}, ids), false);
    return P4_OK;
}

int32_t mergeAll(Engine* e, Type* t, std::string* err) {
    std::vector<Feed*> feeds;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& f : t->feeds)
            if (f->staged && f->created && !f->quarantined) feeds.push_back(f.get());
    }
    for (Feed* f : feeds)
        for (;;) {
            size_t before;
            {
                std::lock_guard<std::mutex> g(t->mu);
                before = f->staged ? f->staged->rows + f->staged->idents.size() : 0;
            }
            if (!before) break;
            const int32_t rc = mergeFeed(e, t, f, err);
            if (rc != P4_OK) return rc;
        }
    return P4_OK;
}

// A feed is due when it holds flushEntries staged rows, when it has had no
// new rows for 2 s (a quiet feed ends fully indexed), or, while every feed's
// staged rows together pass 4 x flushEntries, when it holds the most. The
// due feeds are merged one transaction after the other, until calls are
// waiting for their ack; past 8 x flushEntries staged rows the merges go on
// first (the acks wait: the views' memory stays bounded).
void mergeStep(Engine* e, uint32_t writer) {
    WriterState& ws = *e->writers[writer];
    for (bool first = true;; first = false) {
        const int64_t flush = int64_t(std::max<uint32_t>(e->cfg.flushEntries, 1));
        const int64_t staged = e->stagedRows.load(std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> g(ws.imu);
            if (ws.hold || ws.istop || (!first && !ws.iq.empty() && staged < 8 * flush)) return;
            ws.merging = true;
        }
        const uint64_t now = monoNs();
        const bool over = staged >= 4 * flush;
        Feed* pick = nullptr;
        int64_t pickRows = 0;
        bool pickDue = false;
        {
            std::lock_guard<std::mutex> g(e->typesMu);
            for (auto& tp : e->types) {
                Type* t = tp.get();
                if (t->owner != writer || !t->hasFiles.load(std::memory_order_acquire)) continue;
                std::lock_guard<std::mutex> g2(t->mu);
                for (auto& fp : t->feeds) {
                    Feed* f = fp.get();
                    if (!f->staged || !f->created || f->quarantined || now < f->mergeAfter) continue;
                    const int64_t n = int64_t(f->staged->rows) + int64_t(f->staged->idents.size());
                    const bool due = n >= flush || now - f->stagedAt >= 2000000000ull;
                    if ((due && !pickDue) || ((due == pickDue) && n > pickRows)) {
                        pick = f;
                        pickRows = n;
                        pickDue = due;
                    }
                }
            }
        }
        const bool run = pick && (pickDue || over);
        if (run) {
            std::string err;
            const int32_t mrc = mergeFeed(e, pick->type, pick, &err);
            if (mrc != P4_OK) {
                // Nothing changed: the rows stay staged and are merged later.
                std::lock_guard<std::mutex> g(pick->type->mu);
                pick->mergeAfter = monoNs() + 2000000000ull;
            }
        }
        {
            std::lock_guard<std::mutex> g(ws.imu);
            ws.merging = false;
        }
        ws.dcv.notify_all();
        if (!run) return;
    }
}

}  // namespace p4
}  // namespace flatsql
