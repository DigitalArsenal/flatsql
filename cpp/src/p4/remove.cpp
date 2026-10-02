// Store format 4: removals and repair on a type's writer (CONTRACT C-37
// (7), (9); design §4 W-f/W-g/W-i; §3.5 REBUILD).
//   SUPERSEDE  one feed file: its rows whose batch is not the kept one, a
//              chunk of 32,768 rows per transaction; a record left with no
//              row anywhere is gone (the record keeps its other feeds' rows,
//              which hold every one of its copies).
//   DELETE     every row of the CIDs, in every feed file.
//   QUOTA      the type's oldest records by arrival (the type index's seq order).
//   REBUILD    1 the feed files' secondary indexes (after a migration's bulk
//              append); 2 the counters and the type index recounted from the
//              rows; 8 the same comparison changing nothing, plus
//              PRAGMA integrity_check on every file (C-27).
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>

#include "internal.h"

namespace flatsql {
namespace p4 {

namespace {
// The record's distinct copies over its live rows: token -> smallest length.
std::map<uint32_t, int64_t> copiesLive(const RecState* r) {
    std::map<uint32_t, int64_t> m;
    for (const RowR& x : r->rows) {
        if (!x.live()) continue;
        auto it = m.find(x.tok);
        if (it == m.end() || x.len < it->second) m[x.tok] = x.len;
    }
    return m;
}
bool anyLive(const RecState* r) {
    for (const RowR& x : r->rows)
        if (x.live()) return true;
    return false;
}
}  // namespace

// ---- SUPERSEDE ---------------------------------------------------------------------------------------
void supersedeOp(Engine* e, Type* t, WriteTask* task) {
    SlotOut out(e, task->slot);
    out.enc.header({"tags_deleted", "records_deleted", "files_deleted"});
    std::vector<Tlv> v;
    SlotHeader* h = e->slot(task->slot);
    std::string provider, source, keep;
    uint8_t apply = 0;
    bool bad = h->reqLen > e->reqBytes[0] || !tlvParse(e->slotReq(task->slot), h->reqLen, &v);
    if (!bad) {
        tlvText(v, 11, &provider);
        tlvText(v, 12, &source);
        tlvText(v, 60, &keep);
        tlvU8(v, 61, &apply, &bad);
    }
    if (bad || provider.empty() || source.empty()) {
        out.end(P4_E_ARG, "SUPERSEDE needs provider and source");
        return;
    }
    int64_t tags = 0, records = 0;
    int32_t status = P4_OK;
    std::string err;
    Feed* f = nullptr;
    bool created = false, indexed = true;
    if (t->hasFiles.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> g(t->mu);
        f = feedFor(t, provider, source, false);
        created = f && f->created && !f->quarantined;
        indexed = f && f->indexed;
        if (f && f->quarantined) {
            status = P4_E_CORRUPT;
            err = "feed file quarantined: " + f->path;
        }
    }
    std::vector<uint32_t> drop;  // the batch ids whose batch is not kept
    std::string in;
    if (created && status == P4_OK) {
        int32_t st = P4_OK;
        std::string er;
        Conn* c = writerPin(e, f, &st, &er);
        if (!c) {
            status = st;
            err = "open " + er;
        } else {
            if (!dictEnsure(f, c)) {
                status = P4_E_IO;
                err = "dictionary of " + f->path;
            } else {
                // reload: a newer snapshot may hold batches the cache lacks
                std::lock_guard<std::mutex> g(f->dictMu);
                for (size_t i = 0; i < f->dict.batch.size(); i++)
                    if (f->dict.batch[i].batch != keep) drop.push_back(uint32_t(i + 1));
            }
            writerUnpin(e, f);
        }
        for (uint32_t id : drop) in += (in.empty() ? "" : ",") + std::to_string(id);
    }
    if (!drop.empty() && status == P4_OK && !apply) {
        // Count only: the dropped instances' records, and the records that
        // would be left with no row.
        {
            std::lock_guard<std::mutex> g(t->mu);
            for (auto& kv : f->inst)
                if (std::find(drop.begin(), drop.end(), kv.first.first) != drop.end()) tags += kv.second.n;
        }
        WriteCtx w(e, t);
        int32_t st = P4_OK;
        std::string er;
        Conn* c = writerPin(e, f, &st, &er);
        std::vector<int64_t> seqs;
        if (!c) {
            status = st;
        } else {
            sqlite3_stmt* q = c->sql(std::string("SELECT DISTINCT seq FROM r") + (indexed ? " INDEXED BY r_b" : "") +
                                     " WHERE b IN (" + in + ")");
            int r = SQLITE_ERROR;
            if (q) {
                while ((r = sqlite3_step(q)) == SQLITE_ROW) seqs.push_back(sqlite3_column_int64(q, 0));
                sqlite3_reset(q);
            }
            writerUnpin(e, f);
            if (r != SQLITE_DONE) status = statusOfSqlite(r);
        }
        for (size_t i = 0; i < seqs.size() && status == P4_OK; i++) {
            RecState* r = w.bySeq(seqs[i], &st);
            if (!r) {
                status = st;
                break;
            }
            bool keeps = false;
            for (const RowR& x : r->rows)
                keeps = keeps || (x.live() && (x.fid != f->fid || std::find(drop.begin(), drop.end(), x.bId) == drop.end()));
            if (!keeps) records++;
        }
    }
    while (!drop.empty() && status == P4_OK && apply) {
        // A chunk: up to 32,768 dropped rows, their records whole in this file.
        int32_t st = P4_OK;
        std::string er;
        Conn* c = writerPin(e, f, &st, &er);
        if (!c) {
            status = st;
            err = "open " + er;
            break;
        }
        std::vector<int64_t> seqs;
        sqlite3_stmt* q = c->sql(std::string("SELECT seq FROM r") + (indexed ? " INDEXED BY r_b" : "") + " WHERE b IN (" + in +
                                 ") LIMIT 32768");
        int r = SQLITE_ERROR;
        size_t got = 0;
        if (q) {
            while ((r = sqlite3_step(q)) == SQLITE_ROW) {
                got++;
                seqs.push_back(sqlite3_column_int64(q, 0));
            }
            sqlite3_reset(q);
        }
        writerUnpin(e, f);
        if (r != SQLITE_DONE) {
            status = statusOfSqlite(r);
            break;
        }
        if (seqs.empty()) break;
        std::sort(seqs.begin(), seqs.end());
        seqs.erase(std::unique(seqs.begin(), seqs.end()), seqs.end());
        WriteCtx w(e, t);
        int64_t chunkTags = 0, chunkRecs = 0;
        for (int64_t s : seqs) {
            RecState* rs = w.bySeq(s, &st);
            if (!rs) {
                status = st;
                err = w.err;
                break;
            }
            std::vector<const RowR*> gone;
            for (RowR& x : rs->rows)
                if (x.live() && x.fid == f->fid && std::find(drop.begin(), drop.end(), x.bId) != drop.end()) {
                    bool seen = false;
                    for (const RowR* y : gone) seen = seen || (y->bId == x.bId && y->cId == x.cId);
                    if (!seen) chunkTags++;
                    gone.push_back(&x);
                    x.del = true;
                    rs->touched.insert(x.fid);
                }
            if (!anyLive(rs)) chunkRecs++;
        }
        if (status != P4_OK) break;
        status = w.commit(true);
        if (status != P4_OK) {
            err = "supersede commit failed: " + w.err;
            break;
        }
        tags += chunkTags;
        records += chunkRecs;
        e->bump(kStSupersedeTags, uint64_t(chunkTags));
        e->bump(kStSupersedeRecords, uint64_t(chunkRecs));
        if (got < 32768) break;
    }
    if (status == P4_OK) {
        out.enc.beginRow();
        out.enc.i64(tags);
        out.enc.i64(records);
        out.enc.i64(0);  // files_deleted: a feed keeps its file
        out.enc.endRow();
        out.rows = 1;
    }
    out.end(status, err);
}

// ---- DELETE ------------------------------------------------------------------------------------------
void deleteOp(Engine* e, Type* t, WriteTask* task) {
    SlotOut out(e, task->slot);
    out.enc.header({"deleted"});
    std::vector<Tlv> v;
    SlotHeader* h = e->slot(task->slot);
    const Tlv* cids = nullptr;
    if (h->reqLen <= e->reqBytes[0] && tlvParse(e->slotReq(task->slot), h->reqLen, &v)) cids = tlvFind(v, 40);
    if (!cids || cids->n < 4 || (cids->n - 4) != size_t(ld32(cids->v)) * kCidBin) {
        out.end(P4_E_ARG, "DELETE needs CIDs (tag 40)");
        return;
    }
    int64_t deleted = 0;
    int32_t status = P4_OK;
    std::string err;
    if (t->hasFiles.load(std::memory_order_acquire)) {
        WriteCtx w(e, t);
        const uint32_t n = ld32(cids->v);
        for (uint32_t i = 0; i < n && status == P4_OK; i++) {
            const uint8_t* c36 = cids->v + 4 + size_t(i) * kCidBin;
            if (!cidBinValid(c36)) continue;
            uint8_t key[32];
            cidKeyFromDigest(c36 + 4, key);
            RecState* r = w.byKey(key, &status);
            if (!r) {
                err = w.err;
                break;
            }
            if (!r->existed) continue;
            deleted += int64_t(copiesLive(r).size());
            w.dropAll(r);
        }
        if (status == P4_OK) {
            status = w.commit(true);
            if (status != P4_OK) err = "delete commit failed: " + w.err;
        }
        if (status == P4_OK) e->bump(kStDeletes, uint64_t(deleted));
    }
    if (status == P4_OK) {
        out.enc.beginRow();
        out.enc.i64(deleted);
        out.enc.endRow();
        out.rows = 1;
    }
    out.end(status, err);
}

// ---- QUOTA -------------------------------------------------------------------------------------------
void quotaWork(Engine* e, Type* t, Internal* in) {
    for (size_t at = 0; at < in->seqs.size() && in->status == P4_OK; at += 32768) {
        WriteCtx w(e, t);
        int64_t recs = 0, bytes = 0;
        for (size_t i = at; i < in->seqs.size() && i < at + 32768; i++) {
            int32_t st = P4_OK;
            RecState* r = w.bySeq(in->seqs[i], &st);
            if (!r) {
                in->status = st;
                in->err = w.err;
                break;
            }
            if (!anyLive(r)) continue;
            recs++;
            for (auto& kv : copiesLive(r)) bytes += kv.second;
            w.dropAll(r);
        }
        if (in->status != P4_OK) break;
        const int32_t rc = w.commit(true);
        if (rc != P4_OK) {
            in->status = rc;
            in->err = "quota commit failed: " + w.err;
            break;
        }
        in->a += recs;
        in->b += bytes;
    }
}

// ---- REBUILD -----------------------------------------------------------------------------------------
namespace {
void why(const char* what, const std::string& where, int64_t a, int64_t b) {
#if !defined(__wasm__)
    static const bool on = std::getenv("P4_VERIFY_DEBUG") != nullptr;
    if (on) std::fprintf(stderr, "verify: %s %s (%lld vs %lld)\n", what, where.c_str(), (long long)a, (long long)b);
#else
    (void)what; (void)where; (void)a; (void)b;
#endif
}

bool fileIntact(const std::string& path) {
    Conn* c = nullptr;
    if (openConn(path, OpenKind::Maint, 4096, 0, &c, nullptr) != SQLITE_OK) return false;
    sqlite3_stmt* s = c->sql("PRAGMA integrity_check");
    bool ok = false;
    if (s) {
        const int rc = sqlite3_step(s);
        ok = rc == SQLITE_ROW && std::strcmp(reinterpret_cast<const char*>(sqlite3_column_text(s, 0)), "ok") == 0 &&
             sqlite3_step(s) == SQLITE_DONE;
        sqlite3_reset(s);
    }
    delete c;
    return ok;
}

// One feed file's rows, recounted: its counters, its instances, and each
// record's entry (seq, len, w, k, e, copies), in seq order. Also whether
// each record's rows are its copies x its instances.
struct FileCount {
    Counters k;
    std::map<InstId, InstCount> inst;
    int64_t u2bad = 0;
};
int32_t countFile(Type* t, Feed* f, Conn* c, FileCount* fc, const std::function<int32_t(const XEnt&)>& entry) {
    std::shared_ptr<const Spec> sp = t->spec();
    if (!dictEnsure(f, c)) return P4_E_IO;
    sqlite3_stmt* q = c->sql("SELECT rid, seq, n, b, c, at, cid, e, k, ts, length(d) FROM r ORDER BY rid");
    if (!q) return P4_E_INTERNAL;
    struct RowC {
        uint32_t tok, n, b, cc;
        int64_t len, at;
    };
    XEnt cur;
    bool have = false;
    std::vector<RowC> rows;
    int64_t ts = 0;
    int32_t st = P4_OK;
    auto flush = [&]() -> int32_t {
        if (!have) return P4_OK;
        Counters& k = fc->k;
        int64_t bytes = 0;
        std::map<uint32_t, int64_t> cp;
        std::map<InstId, int64_t> instLen;
        std::map<InstId, std::pair<int64_t, int64_t>> instAt;
        std::set<uint32_t> nodes;
        bool localRows = false;
        for (const RowC& r : rows) {
            bytes += r.len;
            auto it = cp.find(r.tok);
            if (it == cp.end() || r.len < it->second) cp[r.tok] = r.len;
            nodes.insert(r.tok);
            if (!r.b) {
                localRows = true;
                continue;
            }
            const InstId id(r.b, r.cc);
            auto il = instLen.find(id);
            if (il == instLen.end() || r.len < il->second) instLen[id] = r.len;
            auto& a = instAt[id];
            if (il == instLen.end()) a = {r.at, r.at};
            a.first = std::min(a.first, r.at);
            a.second = std::max(a.second, r.at);
        }
        k.rows += int64_t(rows.size());
        k.bytes += bytes;
        k.recs++;
        if (sp->hasEpochRule && !cur.hasE) k.nnull++;
        k.minseq = std::min(k.minseq, cur.seq);
        k.maxseq = std::max(k.maxseq, cur.seq);
        k.minw = std::min(k.minw, cur.w);
        k.maxw = std::max(k.maxw, cur.w);
        k.mints = std::min(k.mints, ts);
        k.maxts = std::max(k.maxts, ts);
        if (cur.hasE) {
            k.mine = std::min(k.mine, cur.e);
            k.maxe = std::max(k.maxe, cur.e);
        }
        for (auto& kv : instLen) {
            InstCount& ic = fc->inst[kv.first];
            ic.n++;
            ic.bytes += kv.second;
        }
        // every copy with every instance; local rows only without one
        const size_t want = instLen.empty() ? cp.size() : cp.size() * instLen.size();
        if (rows.size() != want || (localRows && !instLen.empty())) fc->u2bad++;
        cur.len = INT64_MAX;
        cur.cp.clear();
        for (auto& kv : cp) {
            cur.cp.push_back(CopyLen{kv.first, kv.second});
            cur.len = std::min(cur.len, kv.second);
        }
        cur.fid = f->fid;
        const int32_t r = entry(cur);
        rows.clear();
        have = false;
        return r;
    };
    int r;
    while ((r = sqlite3_step(q)) == SQLITE_ROW) {
        const int64_t seq = sqlite3_column_int64(q, 1);
        if (have && seq != cur.seq) {
            st = flush();
            if (st != P4_OK) break;
        }
        if (!have) {
            cur = XEnt();
            cur.seq = seq;
            if (sqlite3_column_bytes(q, 6) == 32) std::memcpy(cur.key, sqlite3_column_blob(q, 6), 32);
            cur.hasE = sqlite3_column_type(q, 7) != SQLITE_NULL;
            cur.e = sqlite3_column_int64(q, 7);
            cur.k.from(q, 8);
            ts = sqlite3_column_int64(q, 9);
            cur.w = cur.hasE ? cur.e : ts;
            have = true;
        }
        RowC x;
        x.n = uint32_t(sqlite3_column_int64(q, 2));
        x.b = uint32_t(sqlite3_column_int64(q, 3));
        x.cc = uint32_t(sqlite3_column_int64(q, 4));
        x.at = sqlite3_column_int64(q, 5);
        x.len = sqlite3_column_int64(q, 10);
        NodeDef nd;
        if (!dictNode(f, c, x.n, &nd)) {
            st = P4_E_CORRUPT;
            break;
        }
        {
            std::lock_guard<std::mutex> g(t->mu);
            x.tok = tokFor(t, nd.producer, nd.peer, false);
        }
        rows.push_back(x);
    }
    sqlite3_reset(q);
    if (st == P4_OK && r != SQLITE_DONE && r != SQLITE_ROW) st = statusOfSqlite(r);
    if (st == P4_OK) st = flush();
    if (st != P4_OK) return st;
    // The instances' strings from the dictionary.
    for (auto& kv : fc->inst) {
        BatchDef bd;
        if (dictBatch(f, c, kv.first.first, &bd)) {
            kv.second.batch = bd.batch;
            kv.second.ppeer = bd.ppeer;
            kv.second.pkey = bd.pkey;
        }
        kv.second.ckey = dictCkey(c, kv.first.second);
    }
    if (fc->k.rows == 0) {
        const int64_t keep = fc->k.maxseq;
        fc->k = Counters();
        fc->k.maxseq = keep;
    }
    return P4_OK;
}

bool sameEnt(const XEnt& a, const XEnt& b) {
    if (a.seq != b.seq || a.fid != b.fid || std::memcmp(a.key, b.key, 32) != 0 || a.len != b.len || a.w != b.w ||
        !(a.k == b.k) || a.hasE != b.hasE || (a.hasE && a.e != b.e) || a.cp.size() != b.cp.size())
        return false;
    for (size_t i = 0; i < a.cp.size(); i++)
        if (a.cp[i].tok != b.cp[i].tok || a.cp[i].len != b.cp[i].len) return false;
    return true;
}

// The type's counters from its index entries (a walk in seq order).
int32_t countIndex(Conn* x, TypeCounts* tc, int64_t* entries, int64_t* u2bad) {
    sqlite3_stmt* q = x->sql("SELECT seq, fid, cid, len, w, k, e, cp FROM x ORDER BY seq, fid");
    if (!q) return P4_E_INTERNAL;
    std::vector<XEnt> cur;
    int32_t st = P4_OK;
    auto flush = [&]() {
        if (cur.empty()) return;
        XChange c;
        c.after = cur;
        xCount(c, tc);
        // every entry of a record names the same copies
        for (size_t i = 1; i < cur.size(); i++) {
            bool same = cur[i].cp.size() == cur[0].cp.size();
            for (size_t j = 0; same && j < cur[i].cp.size(); j++) same = cur[i].cp[j].tok == cur[0].cp[j].tok;
            if (!same) (*u2bad)++;
        }
        cur.clear();
    };
    int r;
    while ((r = sqlite3_step(q)) == SQLITE_ROW) {
        XEnt e;
        e.seq = sqlite3_column_int64(q, 0);
        e.fid = uint32_t(sqlite3_column_int64(q, 1));
        if (sqlite3_column_bytes(q, 2) == 32) std::memcpy(e.key, sqlite3_column_blob(q, 2), 32);
        e.len = sqlite3_column_int64(q, 3);
        e.w = sqlite3_column_int64(q, 4);
        e.k.from(q, 5);
        e.hasE = sqlite3_column_type(q, 6) != SQLITE_NULL;
        e.e = sqlite3_column_int64(q, 6);
        cpDecode(sqlite3_column_blob(q, 7), size_t(sqlite3_column_bytes(q, 7)), &e.cp);
        if (!cur.empty() && cur[0].seq != e.seq) flush();
        cur.push_back(std::move(e));
        (*entries)++;
    }
    sqlite3_reset(q);
    if (r != SQLITE_DONE) st = statusOfSqlite(r);
    flush();
    return st;
}

bool sameCounts(const Counters& a, const Counters& b) {
    return a.rows == b.rows && a.recs == b.recs && a.bytes == b.bytes && a.nnull == b.nnull;
}
bool sameInst(const std::map<InstId, InstCount>& a, const std::map<InstId, InstCount>& b) {
    if (a.size() != b.size()) return false;
    for (auto& kv : a) {
        auto it = b.find(kv.first);
        if (it == b.end() || it->second.n != kv.second.n || it->second.bytes != kv.second.bytes ||
            it->second.batch != kv.second.batch || it->second.ppeer != kv.second.ppeer || it->second.pkey != kv.second.pkey ||
            it->second.ckey != kv.second.ckey)
            return false;
    }
    return true;
}
}  // namespace

void rebuildWork(Engine* e, Type* t, Internal* in) {
    if (!t->hasFiles.load(std::memory_order_acquire)) return;
    std::vector<Feed*> feeds;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (auto& f : t->feeds)
            if (f->created && !f->quarantined) feeds.push_back(f.get());
    }
    if (in->what & 1) {
        // Feed files' secondary indexes, after a migration's bulk append.
        for (Feed* f : feeds) {
            bool indexed;
            {
                std::lock_guard<std::mutex> g(t->mu);
                indexed = f->indexed;
            }
            in->a++;
            if (indexed) continue;
            int32_t st = P4_OK;
            std::string er;
            Conn* c = writerPin(e, f, &st, &er);
            if (!c) {
                in->status = st;
                in->err = er;
                return;
            }
            st = fileCreateIndexes(t, c);
            writerUnpin(e, f);
            if (st != P4_OK) {
                in->status = st;
                in->err = "indexes of " + f->path;
                return;
            }
            FeedSnap s;
            {
                std::lock_guard<std::mutex> g(t->mu);
                f->indexed = true;
                s = feedSnapOf(f);
            }
            int r = t->idx->exec("BEGIN IMMEDIATE");
            if (r == SQLITE_OK) r = indexPutFeed(t->idx, s);
            if (r == SQLITE_OK) r = t->idx->exec("COMMIT");
            if (r != SQLITE_OK) {
                t->idx->exec("ROLLBACK");
                in->status = statusOfSqlite(r);
                in->err = "type index";
                return;
            }
        }
        e->bump(kStRebuilds);
    }
    if (in->what & (2 | 8)) {
        const bool fix = (in->what & 2) != 0;
        Conn* x = t->idx;
        int64_t entries = 0, mismatches = 0, present = 0;
        // 1. Each feed file: its counters and instances from its rows; each
        //    record's entry against the index.
        if (fix && x->exec("BEGIN IMMEDIATE") != SQLITE_OK) {
            in->status = P4_E_IO;
            in->err = "type index busy";
            return;
        }
        int32_t st = P4_OK;
        auto fail = [&](int32_t s, const std::string& what) {
            if (fix) x->exec("ROLLBACK");
            in->status = s;
            in->err = what;
        };
        for (Feed* f : feeds) {
            Conn* c = nullptr;
            if (openConn(f->path, OpenKind::Maint, 4096, 0, &c, nullptr) != SQLITE_OK) {
                fail(P4_E_IO, "open " + f->path);
                return;
            }
            c->exec("BEGIN");
            FileCount fc;
            std::vector<XEnt> xs;
            st = countFile(t, f, c, &fc, [&](const XEnt& en) -> int32_t {
                entries++;
                const int32_t rc = xOfSeq(x, en.seq, &xs);
                if (rc != P4_OK) return rc;
                const XEnt* have = nullptr;
                for (const XEnt& y : xs)
                    if (y.fid == en.fid) have = &y;
                if (have) present++;
                if (have && sameEnt(*have, en)) {
                } else {
                    mismatches++;
                    why("entry", f->path, en.seq, have ? 1 : 0);
                    if (fix) {
                        XChange ch;
                        if (have) ch.before.push_back(*have);
                        ch.after.push_back(en);
                        const int r = xWrite(x, ch);
                        if (r != SQLITE_OK) return statusOfSqlite(r);
                    }
                }
                return P4_OK;
            });
            Counters stored;
            bool indexed = true;
            std::map<InstId, InstCount> storedInst;
            if (st == P4_OK && (readMeta(c, &stored, &indexed) != SQLITE_OK || readInst(f, c, &storedInst) != SQLITE_OK))
                st = P4_E_IO;
            c->exec("COMMIT");
            delete c;
            if (st != P4_OK) {
                fail(st, "count " + f->path);
                return;
            }
            Counters mem;
            std::map<InstId, InstCount> memInst;
            {
                std::lock_guard<std::mutex> g(t->mu);
                mem = f->k;
                memInst = f->inst;
            }
            if (!sameCounts(fc.k, stored) || !sameCounts(fc.k, mem)) {
                mismatches++;
                why("file counters rows", f->path, stored.rows, fc.k.rows);
                why("file counters recs", f->path, stored.recs, fc.k.recs);
            }
            if (!sameInst(fc.inst, storedInst) || !sameInst(fc.inst, memInst)) {
                mismatches++;
                why("instance counters", f->path, int64_t(storedInst.size()), int64_t(fc.inst.size()));
            }
            if (fc.u2bad) {
                mismatches += fc.u2bad;
                why("rows not copies x instances", f->path, fc.u2bad, 0);
            }
            if (fix && (!sameCounts(fc.k, stored) || !sameInst(fc.inst, storedInst))) {
                // The file's counters rewritten from its rows (its writer).
                int32_t ps = P4_OK;
                std::string er;
                Conn* w = writerPin(e, f, &ps, &er);
                if (!w) {
                    fail(ps, er);
                    return;
                }
                int r = w->exec("BEGIN IMMEDIATE");
                if (r == SQLITE_OK) r = w->exec("DELETE FROM inst");
                for (auto& kv : fc.inst) {
                    if (r != SQLITE_OK) break;
                    auto old = storedInst.find(kv.first);
                    InstCount ic = kv.second;
                    if (old != storedInst.end()) {
                        ic.first = old->second.first;
                        ic.updated = old->second.updated;
                        ic.maxat = old->second.maxat;
                        ic.maxts = old->second.maxts;
                        ic.url = old->second.url;
                        ic.minw = old->second.minw;
                        ic.maxw = old->second.maxw;
                        ic.minseq = old->second.minseq;
                        ic.maxseq = old->second.maxseq;
                    }
                    kv.second = ic;
                    sqlite3_stmt* q = w->get(S_INST_PUT);
                    sqlite3_bind_int64(q, 1, kv.first.first);
                    sqlite3_bind_int64(q, 2, kv.first.second);
                    sqlite3_bind_int64(q, 3, ic.n);
                    sqlite3_bind_int64(q, 4, ic.bytes);
                    if (ic.minw != INT64_MAX) sqlite3_bind_int64(q, 5, ic.minw); else sqlite3_bind_null(q, 5);
                    if (ic.maxw != INT64_MIN) sqlite3_bind_int64(q, 6, ic.maxw); else sqlite3_bind_null(q, 6);
                    if (ic.minseq != INT64_MAX) sqlite3_bind_int64(q, 7, ic.minseq); else sqlite3_bind_null(q, 7);
                    sqlite3_bind_int64(q, 8, ic.maxseq);
                    sqlite3_bind_int64(q, 9, ic.first);
                    sqlite3_bind_int64(q, 10, ic.updated);
                    sqlite3_bind_int64(q, 11, ic.maxat);
                    sqlite3_bind_int64(q, 12, ic.maxts);
                    sqlite3_bind_text(q, 13, ic.url.data(), int(ic.url.size()), SQLITE_STATIC);
                    r = sqlite3_step(q);
                    sqlite3_reset(q);
                    if (r == SQLITE_DONE) r = SQLITE_OK;
                }
                fc.k.maxseq = std::max(fc.k.maxseq, stored.maxseq);
                if (r == SQLITE_OK) r = writeMeta(w, fc.k, nowSec());
                if (r == SQLITE_OK) r = w->exec("COMMIT");
                if (r != SQLITE_OK) w->exec("ROLLBACK");
                writerUnpin(e, f);
                if (r != SQLITE_OK) {
                    fail(statusOfSqlite(r), "repair " + f->path);
                    return;
                }
                std::lock_guard<std::mutex> g(t->mu);
                f->k = fc.k;
                f->inst = fc.inst;
            }
            if (!fileIntact(f->path)) {
                mismatches++;
                if (in->firstBad.empty()) in->firstBad = f->path;
            }
        }
        // 2. Entries the files do not hold (dangling), and the type's
        //    counters from the entries.
        int64_t total = 0, u2bad = 0;
        TypeCounts tc;
        {
            std::lock_guard<std::mutex> g(t->mu);
            tc.toks = t->toks;
            for (TokDef& d : tc.toks) {
                d.n = d.bytes = d.maxts = d.maxseq = 0;
                d.mints = INT64_MAX;
            }
        }
        {
            // Entries no file holds: the index's count against the files'
            // (one walk); when they differ, each entry probed in its file.
            sqlite3_stmt* q = x->sql("SELECT count(*) FROM x");
            int64_t n = 0;
            if (q && sqlite3_step(q) == SQLITE_ROW) n = sqlite3_column_int64(q, 0);
            if (q) sqlite3_reset(q);
            const int64_t held = fix ? entries : present;
            if (n != held) {
                std::vector<std::pair<int64_t, uint32_t>> dangling;
                sqlite3_stmt* w = x->sql("SELECT seq, fid FROM x ORDER BY fid, seq");
                std::map<uint32_t, Conn*> conns;
                int r;
                while (w && (r = sqlite3_step(w)) == SQLITE_ROW) {
                    const int64_t seq = sqlite3_column_int64(w, 0);
                    const uint32_t fid = uint32_t(sqlite3_column_int64(w, 1));
                    Feed* f;
                    {
                        std::lock_guard<std::mutex> g(t->mu);
                        f = t->feedById(fid);
                    }
                    bool ok = false;
                    if (f && f->created) {
                        Conn*& c = conns[fid];
                        if (!c && openConn(f->path, OpenKind::Maint, 1024, 0, &c, nullptr) != SQLITE_OK) c = nullptr;
                        sqlite3_stmt* p = c ? c->get(S_R_MAXRID) : nullptr;
                        if (p) {
                            sqlite3_bind_int64(p, 1, seq << 16);
                            sqlite3_bind_int64(p, 2, (seq << 16) | 0xffff);
                            ok = sqlite3_step(p) == SQLITE_ROW && sqlite3_column_type(p, 0) != SQLITE_NULL;
                            sqlite3_reset(p);
                        }
                    }
                    if (!ok) dangling.push_back({seq, fid});
                }
                if (w) sqlite3_reset(w);
                for (auto& kv : conns) delete kv.second;
                mismatches += int64_t(dangling.size());
                why("dangling entries", t->name, n, held);
                for (auto& d : dangling) {
                    if (!fix) break;
                    sqlite3_stmt* del = x->get(S_X_DEL);
                    sqlite3_bind_int64(del, 1, d.first);
                    sqlite3_bind_int64(del, 2, d.second);
                    const int dr = sqlite3_step(del);
                    sqlite3_reset(del);
                    if (dr != SQLITE_DONE) {
                        fail(statusOfSqlite(dr), "type index");
                        return;
                    }
                }
            }
        }
        st = countIndex(x, &tc, &total, &u2bad);
        if (st != P4_OK) {
            fail(st, "type index walk");
            return;
        }
        mismatches += u2bad;
        TypeCounts mem;
        {
            std::lock_guard<std::mutex> g(t->mu);
            mem = typeCountsOf(t);
        }
        if (tc.uniq != mem.uniq || tc.uniqBytes != mem.uniqBytes || tc.copies != mem.copies || tc.copyBytes != mem.copyBytes) {
            mismatches++;
            why("type counters uniq", t->name, mem.uniq, tc.uniq);
            why("type counters copies", t->name, mem.copies, tc.copies);
        }
        for (size_t i = 0; i < tc.toks.size() && i < mem.toks.size(); i++)
            if (tc.toks[i].n != mem.toks[i].n || tc.toks[i].bytes != mem.toks[i].bytes) {
                mismatches++;
                why("token counters", tc.toks[i].token, mem.toks[i].n, tc.toks[i].n);
            }
        if (fix) {
            // Bounds kept from before (they only widen); counts from the entries.
            tc.mine = std::min(tc.mine, mem.mine);
            tc.maxe = std::max(tc.maxe, mem.maxe);
            tc.mints = std::min(tc.mints, mem.mints);
            tc.maxts = std::max(tc.maxts, mem.maxts);
            tc.maxseq = std::max(tc.maxseq, mem.maxseq);
            tc.nextSeq = mem.nextSeq;
            for (size_t i = 0; i < tc.toks.size(); i++) tc.dirtyToks.insert(uint32_t(i + 1));
            std::vector<FeedSnap> snaps;
            {
                std::lock_guard<std::mutex> g(t->mu);
                for (Feed* f : feeds) snaps.push_back(feedSnapOf(f));
            }
            int r = indexPutCounts(x, tc);
            for (const FeedSnap& s : snaps)
                if (r == SQLITE_OK) r = indexPutFeed(x, s);
            if (r == SQLITE_OK) r = x->exec("COMMIT");
            if (r != SQLITE_OK) {
                fail(statusOfSqlite(r), "type index commit");
                return;
            }
            std::lock_guard<std::mutex> g(t->mu);
            typeCountsTo(t, tc);
        }
        // C-27: the type index and the journal too.
        for (const std::string* p : {&t->pIdx, &t->pJnl})
            if (!fileIntact(*p)) {
                mismatches++;
                if (in->firstBad.empty()) in->firstBad = *p;
            }
        in->a = entries;
        in->b = mismatches;
        e->bump(kStRebuilds);
    }
}

}  // namespace p4
}  // namespace flatsql
