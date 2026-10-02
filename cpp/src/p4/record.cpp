// Store format 4: a record's rows across its feed files (CONTRACT C-37), and
// how a write changes and commits them.
//
// A record's rows are its copies x its instances: every copy (producer token)
// appears with every instance (feed, batch, content key, producer peer and
// key) in that instance's feed file; a record with no instance has one row
// per copy in the type's local file. A write loads the records it touches
// (the type index names their feed files; their rows are a rid range of each
// file, rid = seq << 16 | n), changes their rows, then commits:
//   1. the journal (synchronous=FULL): the feed files and records it touches
//      (J_TOUCH), new feed / token ids and ingest identities;
//   2. each feed file in one transaction (rows, the file's counters and its
//      instances' counters), feeds before local, so a cut leaves rows that
//      only add;
//   3. the type index in one transaction: each record's entries set from its
//      rows, the type's counters, the feeds' mirrors.
// Open replays the journal tail through the same steps (journal.cpp).
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace p4 {

WriteCtx::WriteCtx(Engine* e_, Type* t_) : e(e_), t(t_) {
    sp = t->spec();
    now = nowSec();
}

WriteCtx::~WriteCtx() {}

RecState* WriteCtx::known(int64_t seq) {
    auto it = bySeq_.find(seq);
    return it == bySeq_.end() ? nullptr : it->second;
}

int32_t WriteCtx::loadRows(RecState* r, uint32_t fid) {
    r->loadedFids.insert(fid);
    Feed* f;
    bool created, quarantined;
    {
        std::lock_guard<std::mutex> g(t->mu);
        f = t->feedById(fid);
        created = f && f->created;
        quarantined = f && f->quarantined;
    }
    if (!f || !created) return P4_OK;
    if (quarantined) {
        err = "feed file quarantined: " + f->path;
        return P4_E_CORRUPT;
    }
    int32_t st = P4_OK;
    std::string er;
    Conn* c = writerPin(e, f, &st, &er);
    if (!c) {
        err = "writer open: " + er;
        return st;
    }
    if (!dictEnsure(f, c)) {
        writerUnpin(e, f);
        err = "dictionary of " + f->path;
        return P4_E_IO;
    }
    sqlite3_stmt* q = c->get(S_R_SEQ);
    if (!q) {
        writerUnpin(e, f);
        return P4_E_INTERNAL;
    }
    const int64_t base = r->seq << 16;
    sqlite3_bind_int64(q, 1, base);
    sqlite3_bind_int64(q, 2, base | 0xffff);
    int rc;
    bool haveKey = r->existed || !r->rows.empty();
    struct Pending {
        RowR row;
        NodeDef node;
    };
    std::vector<RowR> got;
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) {
        RowR x;
        x.fid = fid;
        x.loaded = true;
        x.rid = sqlite3_column_int64(q, 0);
        x.nId = uint32_t(sqlite3_column_int64(q, 1));
        x.bId = uint32_t(sqlite3_column_int64(q, 2));
        x.cId = uint32_t(sqlite3_column_int64(q, 3));
        x.uId = uint32_t(sqlite3_column_int64(q, 4));
        x.at = sqlite3_column_int64(q, 5);
        if (sqlite3_column_bytes(q, 6) != 32) {
            rc = SQLITE_CORRUPT;
            break;
        }
        const uint8_t* cid = static_cast<const uint8_t*>(sqlite3_column_blob(q, 6));
        if (!haveKey) {
            std::memcpy(r->key, cid, 32);
            haveKey = true;
        } else if (std::memcmp(r->key, cid, 32) != 0) {
            rc = SQLITE_CORRUPT;  // one seq is one CID (§3.8.2)
            break;
        }
        const bool hasE = sqlite3_column_type(q, 7) != SQLITE_NULL;
        const int64_t ev = sqlite3_column_int64(q, 7);
        KVal kv;
        kv.from(q, 8);
        x.ts = sqlite3_column_int64(q, 9);
        x.sealed = sqlite3_column_type(q, 10) != SQLITE_NULL;
        if (x.sealed) x.fcols.assign(static_cast<const char*>(sqlite3_column_blob(q, 10)), size_t(sqlite3_column_bytes(q, 10)));
        if (sqlite3_column_type(q, 11) != SQLITE_NULL)
            x.sig.assign(static_cast<const char*>(sqlite3_column_blob(q, 11)), size_t(sqlite3_column_bytes(q, 11)));
        x.len = sqlite3_column_int64(q, 12);
        if (r->rows.empty() && got.empty() && !r->existed) {
            r->hasE = hasE;
            r->e = ev;
            r->k = kv;
            r->ts = x.ts;
            r->w = hasE ? ev : x.ts;
        } else if (r->rows.empty() && got.empty()) {
            r->ts = x.ts;
        }
        NodeDef nd;
        if (!dictNode(f, c, x.nId, &nd)) {
            rc = SQLITE_CORRUPT;
            break;
        }
        x.peer = nd.peer;
        {
            std::lock_guard<std::mutex> g(t->mu);
            x.tok = tokFor(t, nd.producer, nd.peer, true);
        }
        if (x.bId) {
            BatchDef bd;
            if (!dictBatch(f, c, x.bId, &bd)) {
                rc = SQLITE_CORRUPT;
                break;
            }
            x.inst = true;
            x.batch = bd.batch;
            x.ppeer = bd.ppeer;
            x.pkey = bd.pkey;
        }
        got.push_back(std::move(x));
    }
    sqlite3_reset(q);
    // Content keys and urls (looked up in the file, not cached).
    for (RowR& x : got) {
        if (x.cId) x.ckey = dictCkey(c, x.cId);
        if (x.uId) x.url = dictUrl(c, x.uId);
    }
    writerUnpin(e, f);
    if (rc != SQLITE_DONE) {
        err = "rows of seq " + std::to_string(r->seq) + " in " + f->path + ": " + std::to_string(rc);
        if ((rc & 0xff) == SQLITE_CORRUPT) return P4_E_CORRUPT;
        return statusOfSqlite(rc);
    }
    if (!got.empty()) r->fids.insert(fid);
    for (RowR& x : got) r->rows.push_back(std::move(x));
    return P4_OK;
}

RecState* WriteCtx::byKey(const uint8_t key[32], int32_t* rc) {
    *rc = P4_OK;
    const std::string k(reinterpret_cast<const char*>(key), 32);
    auto it = byKey_.find(k);
    if (it != byKey_.end()) return it->second;
    std::vector<XEnt> xs;
    if (t->idx) {
        *rc = xOfCid(t->idx, key, &xs);
        if (*rc != P4_OK) return nullptr;
    }
    if (!xs.empty()) {
        // One seq per CID: the entries of its (lowest) seq.
        int64_t s0 = xs[0].seq;
        for (const XEnt& x : xs) s0 = std::min(s0, x.seq);
        std::vector<XEnt> keep;
        for (XEnt& x : xs)
            if (x.seq == s0) keep.push_back(std::move(x));
        xs.swap(keep);
        auto bs = bySeq_.find(s0);
        if (bs != bySeq_.end()) {
            byKey_[k] = bs->second;
            return bs->second;
        }
    }
    store_.emplace_back();
    RecState* r = &store_.back();
    std::memcpy(r->key, key, 32);
    if (!xs.empty()) {
        r->seq = xs[0].seq;
        r->existed = true;
        r->hasE = xs[0].hasE;
        r->e = xs[0].e;
        r->k = xs[0].k;
        r->w = xs[0].w;
        for (const XEnt& x : xs) r->fids.insert(x.fid);
        r->before = std::move(xs);
        const std::set<uint32_t> fids = r->fids;
        for (uint32_t fid : fids) {
            *rc = loadRows(r, fid);
            if (*rc != P4_OK) return nullptr;
        }
        bySeq_[r->seq] = r;
    }
    byKey_[k] = r;
    order_.push_back(r);
    return r;
}

RecState* WriteCtx::bySeq(int64_t seq, int32_t* rc, const std::set<uint32_t>* more) {
    *rc = P4_OK;
    auto it = bySeq_.find(seq);
    if (it != bySeq_.end()) {
        RecState* r = it->second;
        if (more)
            for (uint32_t fid : *more)
                if (!r->loadedFids.count(fid)) {
                    *rc = loadRows(r, fid);
                    if (*rc != P4_OK) return nullptr;
                }
        return r;
    }
    std::vector<XEnt> xs;
    if (t->idx) {
        *rc = xOfSeq(t->idx, seq, &xs);
        if (*rc != P4_OK) return nullptr;
    }
    store_.emplace_back();
    RecState* r = &store_.back();
    r->seq = seq;
    std::set<uint32_t> fids;
    for (const XEnt& x : xs) fids.insert(x.fid);
    if (more) fids.insert(more->begin(), more->end());
    if (!xs.empty()) {
        std::memcpy(r->key, xs[0].key, 32);
        r->existed = true;
        r->hasE = xs[0].hasE;
        r->e = xs[0].e;
        r->k = xs[0].k;
        r->w = xs[0].w;
        for (const XEnt& x : xs) r->fids.insert(x.fid);
        r->before = std::move(xs);
    }
    for (uint32_t fid : fids) {
        *rc = loadRows(r, fid);
        if (*rc != P4_OK) return nullptr;
    }
    if (!r->existed && !r->rows.empty()) r->existed = true;
    bySeq_[seq] = r;
    if (r->existed) byKey_[std::string(reinterpret_cast<const char*>(r->key), 32)] = r;
    order_.push_back(r);
    return r;
}

int32_t WriteCtx::loadD(RowR& x) {
    if (x.hasD) return P4_OK;
    const uint32_t fid = x.loaded ? x.fid : x.srcFid;
    const int64_t rid = x.loaded ? x.rid : x.srcRid;
    Feed* f;
    {
        std::lock_guard<std::mutex> g(t->mu);
        f = t->feedById(fid);
    }
    if (!f) return P4_E_INTERNAL;
    int32_t st = P4_OK;
    std::string er;
    Conn* c = writerPin(e, f, &st, &er);
    if (!c) {
        err = "writer open: " + er;
        return st;
    }
    sqlite3_stmt* q = c->get(S_R_D);
    int r = SQLITE_ERROR;
    if (q) {
        sqlite3_bind_int64(q, 1, rid);
        r = sqlite3_step(q);
        if (r == SQLITE_ROW) {
            x.d.assign(static_cast<const char*>(sqlite3_column_blob(q, 0)), size_t(sqlite3_column_bytes(q, 0)));
            x.hasD = true;
        }
        sqlite3_reset(q);
    }
    writerUnpin(e, f);
    if (r == SQLITE_ROW) return P4_OK;
    err = "bytes of row " + std::to_string(rid) + " in " + f->path;
    return r == SQLITE_DONE ? P4_E_CORRUPT : statusOfSqlite(r);
}

// ---- changes ------------------------------------------------------------------------------------
namespace {
bool sameInst(const RowR& a, const WriteCtx::Inst& i) {
    return a.inst && a.fid == i.fid && a.batch == i.batch && a.ppeer == i.ppeer && a.pkey == i.pkey && a.ckey == i.ckey;
}
}  // namespace

// Every copy with every instance; local rows only without one.
void WriteCtx::fill(RecState* r) {
    std::vector<size_t> copies;  // a representative live row per copy (token)
    std::vector<size_t> insts;   // a representative live row per instance
    for (size_t i = 0; i < r->rows.size(); i++) {
        const RowR& x = r->rows[i];
        if (!x.live()) continue;
        bool seen = false;
        for (size_t c : copies) seen = seen || r->rows[c].tok == x.tok;
        if (!seen) copies.push_back(i);
        if (!x.inst) continue;
        seen = false;
        for (size_t c : insts) seen = seen || r->rows[c].sameInst(x);
        if (!seen) insts.push_back(i);
    }
    // An instance's at is the earliest of its rows; its url is the one its
    // rows carry (kept equal by deliver).
    auto instAt = [&](size_t ir) {
        int64_t at = r->rows[ir].at;
        for (const RowR& x : r->rows)
            if (x.live() && x.sameInst(r->rows[ir])) at = std::min(at, x.at);
        return at;
    };
    std::vector<RowR> add;
    if (!insts.empty()) {
        for (size_t ci : copies)
            for (size_t ii : insts) {
                bool have = false;
                for (const RowR& x : r->rows) have = have || (x.live() && x.tok == r->rows[ci].tok && x.sameInst(r->rows[ii]));
                if (have) continue;
                const RowR& c = r->rows[ci];
                const RowR& in = r->rows[ii];
                RowR n;
                n.fid = in.fid;
                n.tok = c.tok;
                n.peer = c.peer;
                n.inst = true;
                n.batch = in.batch;
                n.ppeer = in.ppeer;
                n.pkey = in.pkey;
                n.ckey = in.ckey;
                n.url = in.url;
                n.at = instAt(ii);
                n.ts = r->ts;
                n.len = c.len;
                n.sig = c.sig;
                n.fcols = c.fcols;
                n.sealed = c.sealed;
                if (c.hasD) {
                    n.hasD = true;
                    n.d = c.d;
                } else {
                    n.srcFid = c.loaded ? c.fid : c.srcFid;
                    n.srcRid = c.loaded ? c.rid : c.srcRid;
                }
                add.push_back(std::move(n));
            }
        for (RowR& x : r->rows)
            if (x.live() && !x.inst) {
                x.del = true;
                r->touched.insert(x.fid);
            }
    } else {
        uint32_t local;
        {
            std::lock_guard<std::mutex> g(t->mu);
            local = feedFor(t, "", "", true)->fid;
        }
        for (size_t ci : copies) {
            bool have = false;
            for (const RowR& x : r->rows) have = have || (x.live() && x.tok == r->rows[ci].tok && !x.inst && x.fid == local);
            if (have) continue;
            const RowR& c = r->rows[ci];
            RowR n = c;
            n.fid = local;
            n.inst = false;
            n.batch.clear();
            n.ppeer.clear();
            n.pkey.clear();
            n.ckey.clear();
            n.url.clear();
            n.ts = r->ts;
            n.rid = 0;
            n.loaded = false;
            n.del = false;
            n.urlSet = false;
            n.nId = n.bId = n.cId = n.uId = 0;
            if (!c.hasD) {
                n.srcFid = c.loaded ? c.fid : c.srcFid;
                n.srcRid = c.loaded ? c.rid : c.srcRid;
            }
            add.push_back(std::move(n));
        }
    }
    for (RowR& n : add) {
        r->fids.insert(n.fid);
        r->touched.insert(n.fid);
        r->rows.push_back(std::move(n));
    }
    // New rows that are gone again (a seed replaced, a superseded record of
    // this write) are dropped.
    r->rows.erase(std::remove_if(r->rows.begin(), r->rows.end(), [](const RowR& x) { return !x.loaded && x.del; }),
                  r->rows.end());
}

void WriteCtx::deliver(RecState* r, const RowR& copy, const std::vector<Inst>& insts, Out* out) {
    bool copyAt = false;
    for (const RowR& x : r->rows) copyAt = copyAt || (x.live() && x.tok == copy.tok);
    if (!copyAt) out->copyNew = true;
    // A representative row of the copy: the delivered data when it is new.
    RowR rep = copy;
    if (copyAt)
        for (const RowR& x : r->rows)
            if (x.live() && x.tok == copy.tok) {
                rep = x;
                break;
            }
    auto newRow = [&](uint32_t fid, const Inst* in) {
        RowR n;
        n.fid = fid;
        n.tok = rep.tok;
        n.peer = rep.peer;
        n.ts = r->ts;
        n.len = rep.len;
        n.sig = rep.sig;
        n.fcols = rep.fcols;
        n.sealed = rep.sealed;
        if (rep.hasD) {
            n.hasD = true;
            n.d = rep.d;
        } else {
            n.srcFid = rep.loaded ? rep.fid : rep.srcFid;
            n.srcRid = rep.loaded ? rep.rid : rep.srcRid;
        }
        if (in) {
            n.inst = true;
            n.batch = in->batch;
            n.ppeer = in->ppeer;
            n.pkey = in->pkey;
            n.ckey = in->ckey;
            n.url = in->url;
            n.at = in->at;
        }
        r->fids.insert(fid);
        r->touched.insert(fid);
        r->rows.push_back(std::move(n));
    };
    for (const Inst& in : insts) {
        bool held = false;
        for (RowR& x : r->rows) {
            if (!x.live() || !sameInst(x, in)) continue;
            held = true;
            // DUP: source_url follows the latest write (C-3); at is unchanged.
            if (x.url != in.url) {
                x.url = in.url;
                if (x.loaded) x.urlSet = true;
                r->touched.insert(x.fid);
                out->urlChanged = true;
            }
        }
        if (!held) out->instNew = true;
        bool row = false;
        for (const RowR& x : r->rows) row = row || (x.live() && x.tok == rep.tok && sameInst(x, in));
        if (!row) newRow(in.fid, &in);
    }
    if (insts.empty() && !copyAt) {
        // A copy without instances: a seed row (local); fill puts it with
        // every instance the record has, or keeps it local.
        uint32_t local;
        {
            std::lock_guard<std::mutex> g(t->mu);
            local = feedFor(t, "", "", true)->fid;
        }
        newRow(local, nullptr);
    }
    fill(r);
}

void WriteCtx::normalize(RecState* r) { fill(r); }

void WriteCtx::dropFid(RecState* r, uint32_t fid) {
    for (RowR& x : r->rows)
        if (x.live() && x.fid == fid) {
            x.del = true;
            r->touched.insert(fid);
        }
    r->rows.erase(std::remove_if(r->rows.begin(), r->rows.end(), [](const RowR& x) { return !x.loaded && x.del; }),
                  r->rows.end());
}

void WriteCtx::dropAll(RecState* r) {
    for (RowR& x : r->rows)
        if (x.live()) {
            x.del = true;
            r->touched.insert(x.fid);
        }
    r->rows.erase(std::remove_if(r->rows.begin(), r->rows.end(), [](const RowR& x) { return !x.loaded && x.del; }),
                  r->rows.end());
}

void WriteCtx::touchAll(RecState* r) {
    for (uint32_t fid : r->fids) r->touched.insert(fid);
    for (uint32_t fid : r->loadedFids) r->touched.insert(fid);
    for (const XEnt& x : r->before) r->touched.insert(x.fid);
}

bool entryOf(const RecState& r, uint32_t fid, XEnt* out) {
    std::map<uint32_t, int64_t> cp;
    int64_t len = INT64_MAX;
    for (const RowR& x : r.rows) {
        if (!x.live() || x.fid != fid) continue;
        auto it = cp.find(x.tok);
        if (it == cp.end() || x.len < it->second) cp[x.tok] = x.len;
        len = std::min(len, x.len);
    }
    if (cp.empty()) return false;
    *out = XEnt();
    out->seq = r.seq;
    out->fid = fid;
    std::memcpy(out->key, r.key, 32);
    out->len = len;
    out->w = r.w;
    out->k = r.k;
    out->hasE = r.hasE;
    out->e = r.e;
    for (auto& kv : cp) out->cp.push_back(CopyLen{kv.first, kv.second});
    return true;
}

// ---- commit ---------------------------------------------------------------------------------------
int32_t WriteCtx::journalWrite() {
    Conn* j = t->jdb;
    if (!j) return P4_E_INTERNAL;
    // Ids this write uses that no durable record names yet.
    std::vector<std::pair<uint32_t, std::string>> feeds;
    std::vector<std::pair<uint32_t, std::string>> toks;
    int64_t reserve = 0;
    {
        std::lock_guard<std::mutex> g(t->mu);
        std::set<uint32_t> fids, tokIds;
        for (RecState* r : order_) {
            for (uint32_t fid : r->touched) fids.insert(fid);
            for (const RowR& x : r->rows) tokIds.insert(x.tok);
        }
        for (uint32_t fid : fids) {
            Feed* f = t->feedById(fid);
            if (f && !f->registered) feeds.push_back({fid, f->provider + '\x1f' + f->source + '\x1f' + f->name});
        }
        for (uint32_t id : tokIds) {
            TokDef* d = t->tokById(id);
            if (d && !d->registered) toks.push_back({id, d->token + '\x1f' + d->peer});
        }
        if (t->nextSeq - 1 > t->seqReserved) reserve = t->nextSeq - 1 + int64_t(e->cfg.seqBlock);
    }
    int rc = j->exec("BEGIN IMMEDIATE");
    int64_t rows = 0;
    auto add = [&](int op, int64_t fid, int64_t seq, const void* k, size_t kn, const std::string* s, int64_t v) {
        if (rc != SQLITE_OK) return;
        sqlite3_stmt* q = j->get(S_J_INS);
        if (!q) {
            rc = SQLITE_ERROR;
            return;
        }
        sqlite3_bind_int(q, 1, op);
        sqlite3_bind_int64(q, 2, fid);
        sqlite3_bind_int64(q, 3, seq);
        if (k) sqlite3_bind_blob(q, 4, k, int(kn), SQLITE_STATIC);
        else sqlite3_bind_null(q, 4);
        if (s) sqlite3_bind_text(q, 5, s->data(), int(s->size()), SQLITE_STATIC);
        else sqlite3_bind_null(q, 5);
        sqlite3_bind_int64(q, 6, v);
        const int r = sqlite3_step(q);
        sqlite3_reset(q);
        if (r != SQLITE_DONE) rc = r;
        rows++;
    };
    // The rows of earlier writes are applied (their index commits are durable).
    if (rc == SQLITE_OK && t->jcut > 0) {
        sqlite3_stmt* q = j->get(S_J_DEL);
        if (!q) rc = SQLITE_ERROR;
        else {
            sqlite3_bind_int64(q, 1, t->jcut);
            const int r = sqlite3_step(q);
            sqlite3_reset(q);
            if (r != SQLITE_DONE) rc = r;
        }
    }
    for (auto& f : feeds) add(J_FEED, f.first, 0, nullptr, 0, &f.second, 0);
    for (auto& d : toks) add(J_TOK, 0, 0, nullptr, 0, &d.second, d.first);
    for (RecState* r : order_)
        if (r->seq)
            for (uint32_t fid : r->touched) add(J_TOUCH, fid, r->seq, r->key, 32, nullptr, 0);
    for (const IdentNew& id : idents) {
        if (!id.rec || !id.rec->seq) continue;
        uint8_t k[64];
        std::memcpy(k, id.h, 32);
        std::memcpy(k + 32, id.rec->key, 32);
        add(J_IDENT, 0, id.rec->seq, k, 64, nullptr, int64_t(id.src));
    }
    if (rc == SQLITE_OK && reserve) {
        sqlite3_stmt* q = j->get(S_JM_SET);
        if (!q) rc = SQLITE_ERROR;
        else {
            sqlite3_bind_text(q, 1, "seq_reserved", -1, SQLITE_STATIC);
            sqlite3_bind_int64(q, 2, reserve);
            const int r = sqlite3_step(q);
            sqlite3_reset(q);
            if (r != SQLITE_DONE) rc = r;
        }
    }
    if (rc == SQLITE_OK) rc = j->exec("COMMIT");
    if (rc != SQLITE_OK) {
        err = std::string("journal: ") + sqlite3_errmsg(j->db);
        j->exec("ROLLBACK");
        return statusOfSqlite(rc);
    }
    e->bump(kStJournalSyncs);
    jlast = rows ? sqlite3_last_insert_rowid(j->db) : 0;
    std::lock_guard<std::mutex> g(t->mu);
    if (t->jcut > 0) t->jcut = 0;  // deleted above
    if (reserve) t->seqReserved = reserve;
    for (auto& f : feeds)
        if (Feed* x = t->feedById(f.first)) x->registered = true;
    for (auto& d : toks)
        if (TokDef* x = t->tokById(d.first)) x->registered = true;
    return P4_OK;
}

namespace {
struct InstAgg {
    bool b = false, a = false;
    int64_t lenB = INT64_MAX, lenA = INT64_MAX, maxat = 0, minat = INT64_MAX;
    bool changed = false;  // new rows or a url set in this file
    const RowR* any = nullptr;
    std::string url;
};

uint32_t internNode(Dict& d, Conn* c, const std::string& producer, const std::string& peer, int* rc) {
    const std::string k = nodeKey(producer, peer);
    auto it = d.nodeId.find(k);
    if (it != d.nodeId.end()) return it->second;
    const uint32_t id = uint32_t(d.node.size() + 1);
    sqlite3_stmt* q = c->get(S_NODE_INS);
    if (!q) {
        *rc = SQLITE_ERROR;
        return 0;
    }
    sqlite3_bind_int64(q, 1, id);
    sqlite3_bind_text(q, 2, producer.data(), int(producer.size()), SQLITE_STATIC);
    sqlite3_bind_text(q, 3, peer.data(), int(peer.size()), SQLITE_STATIC);
    const int r = sqlite3_step(q);
    sqlite3_reset(q);
    if (r != SQLITE_DONE) {
        *rc = r;
        return 0;
    }
    d.node.push_back(NodeDef{producer, peer});
    d.nodeId[k] = id;
    return id;
}

uint32_t internBatch(Dict& d, Conn* c, const RowR& x, int* rc) {
    const std::string k = batchKey(x.batch, x.ppeer, x.pkey);
    auto it = d.batchId.find(k);
    if (it != d.batchId.end()) return it->second;
    const uint32_t id = uint32_t(d.batch.size() + 1);
    sqlite3_stmt* q = c->get(S_BATCH_INS);
    if (!q) {
        *rc = SQLITE_ERROR;
        return 0;
    }
    sqlite3_bind_int64(q, 1, id);
    sqlite3_bind_text(q, 2, x.batch.data(), int(x.batch.size()), SQLITE_STATIC);
    sqlite3_bind_text(q, 3, x.ppeer.data(), int(x.ppeer.size()), SQLITE_STATIC);
    sqlite3_bind_text(q, 4, x.pkey.data(), int(x.pkey.size()), SQLITE_STATIC);
    const int r = sqlite3_step(q);
    sqlite3_reset(q);
    if (r != SQLITE_DONE) {
        *rc = r;
        return 0;
    }
    d.batch.push_back(BatchDef{x.batch, x.ppeer, x.pkey});
    d.batchId[k] = id;
    return id;
}

// A content key's or url's id in the file (0 for "").
uint32_t internText(Conn* c, StmtId get, StmtId ins, const std::string& s, int* rc) {
    if (s.empty()) return 0;
    sqlite3_stmt* q = c->get(get);
    if (!q) {
        *rc = SQLITE_ERROR;
        return 0;
    }
    sqlite3_bind_text(q, 1, s.data(), int(s.size()), SQLITE_STATIC);
    int r = sqlite3_step(q);
    uint32_t id = 0;
    if (r == SQLITE_ROW) id = uint32_t(sqlite3_column_int64(q, 0));
    sqlite3_reset(q);
    if (r == SQLITE_ROW) return id;
    if (r != SQLITE_DONE) {
        *rc = r;
        return 0;
    }
    q = c->get(ins);
    if (!q) {
        *rc = SQLITE_ERROR;
        return 0;
    }
    sqlite3_bind_text(q, 1, s.data(), int(s.size()), SQLITE_STATIC);
    r = sqlite3_step(q);
    sqlite3_reset(q);
    if (r != SQLITE_DONE) {
        *rc = r;
        return 0;
    }
    return uint32_t(sqlite3_last_insert_rowid(c->db));
}
}  // namespace

int32_t WriteCtx::commitFile(Feed* f, const std::vector<RecState*>& recs) {
    bool change = false;
    for (RecState* r : recs)
        for (const RowR& x : r->rows)
            if (x.fid == f->fid && ((!x.loaded && !x.del) || (x.loaded && x.del) || (x.loaded && !x.del && x.urlSet))) change = true;
    if (!change) return P4_OK;
    // The bytes new rows copy from other rows, before the transaction.
    for (RecState* r : recs)
        for (RowR& x : r->rows)
            if (x.fid == f->fid && !x.loaded && !x.del && !x.hasD) {
                // Its source: a loaded row of the same record.
                RowR* src = nullptr;
                for (RowR& y : r->rows)
                    if (y.loaded && y.fid == x.srcFid && y.rid == x.srcRid) src = &y;
                if (src) {
                    const int32_t st = loadD(*src);
                    if (st != P4_OK) return st;
                    x.d = src->d;
                    x.hasD = true;
                } else {
                    RowR tmp;
                    tmp.loaded = true;
                    tmp.fid = x.srcFid;
                    tmp.rid = x.srcRid;
                    const int32_t st = loadD(tmp);
                    if (st != P4_OK) return st;
                    x.d.swap(tmp.d);
                    x.hasD = true;
                }
            }
    {
        std::lock_guard<std::mutex> g(t->mu);
        if (!f->created && migrate) f->indexed = false;  // a migration: REBUILD 1 adds the secondary indexes
    }
    int32_t status = P4_OK;
    std::string er;
    Conn* c = writerPin(e, f, &status, &er);
    if (!c) {
        err = "writer open: " + er;
        return status;
    }
    if (!dictEnsure(f, c)) {
        writerUnpin(e, f);
        err = "dictionary of " + f->path;
        return P4_E_IO;
    }
    Dict dict;
    {
        std::lock_guard<std::mutex> g(f->dictMu);
        dict = f->dict;
    }
    Counters k;
    std::map<InstId, InstCount> inst;
    bool indexed;
    {
        std::lock_guard<std::mutex> g(t->mu);
        k = f->k;
        inst = f->inst;
        indexed = f->indexed;
    }
    std::set<InstId> changedInst;
    bool deleted = false;
    int rc = c->exec("BEGIN IMMEDIATE");
    auto bad = [&](int r) {
        if (rc == SQLITE_OK && r != SQLITE_OK && r != SQLITE_DONE && r != SQLITE_ROW) rc = r;
    };
    std::vector<RecState*> sorted = recs;
    std::sort(sorted.begin(), sorted.end(), [](RecState* a, RecState* b) { return a->seq < b->seq; });
    for (RecState* r : sorted) {
        if (rc != SQLITE_OK) break;
        const int64_t base = r->seq << 16;
        int64_t next = -1;
        for (RowR& x : r->rows) {
            if (rc != SQLITE_OK) break;
            if (x.fid != f->fid) continue;
            if (x.loaded && x.del) {
                sqlite3_stmt* q = c->get(S_R_DEL);
                sqlite3_bind_int64(q, 1, x.rid);
                bad(sqlite3_step(q));
                sqlite3_reset(q);
                deleted = true;
            } else if (x.loaded && x.urlSet) {
                x.uId = internText(c, S_URL_GET, S_URL_INS, x.url, &rc);
                sqlite3_stmt* q = c->get(S_R_URL);
                sqlite3_bind_int64(q, 1, x.rid);
                sqlite3_bind_int64(q, 2, x.uId);
                bad(sqlite3_step(q));
                sqlite3_reset(q);
            } else if (!x.loaded && !x.del) {
                if (next < 0) {
                    sqlite3_stmt* q = c->get(S_R_MAXRID);
                    sqlite3_bind_int64(q, 1, base);
                    sqlite3_bind_int64(q, 2, base | 0xffff);
                    const int sr = sqlite3_step(q);
                    next = sr == SQLITE_ROW && sqlite3_column_type(q, 0) != SQLITE_NULL ? sqlite3_column_int64(q, 0) + 1 : base;
                    sqlite3_reset(q);
                    if (sr != SQLITE_ROW) bad(sr);
                }
                if (next > (base | 0xffff)) {
                    rc = SQLITE_FULL;  // 65,536 rows of one record in one feed
                    break;
                }
                std::string producer;
                {
                    std::lock_guard<std::mutex> g(t->mu);
                    TokDef* d = t->tokById(x.tok);
                    producer = d ? d->token : std::string();
                }
                x.nId = internNode(dict, c, producer, x.peer, &rc);
                x.bId = x.inst ? internBatch(dict, c, x, &rc) : 0;
                x.cId = x.inst ? internText(c, S_CKEY_GET, S_CKEY_INS, x.ckey, &rc) : 0;
                x.uId = x.inst ? internText(c, S_URL_GET, S_URL_INS, x.url, &rc) : 0;
                if (rc != SQLITE_OK) break;
                sqlite3_stmt* q = c->get(S_R_INS);
                const int64_t rid = next++;
                sqlite3_bind_int64(q, 1, rid);
                sqlite3_bind_int64(q, 2, r->seq);
                sqlite3_bind_int64(q, 3, x.nId);
                sqlite3_bind_int64(q, 4, x.bId);
                sqlite3_bind_int64(q, 5, x.cId);
                sqlite3_bind_int64(q, 6, x.uId);
                sqlite3_bind_int64(q, 7, x.at);
                sqlite3_bind_blob(q, 8, r->key, 32, SQLITE_STATIC);
                if (r->hasE) sqlite3_bind_int64(q, 9, r->e);
                else sqlite3_bind_null(q, 9);
                r->k.bind(q, 10);
                sqlite3_bind_int64(q, 11, x.ts);
                if (x.sealed) sqlite3_bind_blob(q, 12, x.fcols.data(), int(x.fcols.size()), SQLITE_STATIC);
                else sqlite3_bind_null(q, 12);
                if (!x.sig.empty()) sqlite3_bind_blob(q, 13, x.sig.data(), int(x.sig.size()), SQLITE_STATIC);
                else sqlite3_bind_null(q, 13);
                sqlite3_bind_blob(q, 14, x.d.data(), int(x.d.size()), SQLITE_STATIC);
                bad(sqlite3_step(q));
                sqlite3_reset(q);
                x.rid = rid;
            }
        }
        if (rc != SQLITE_OK) break;
        // The file's and its instances' counters: this record's rows before
        // (as loaded) and after (live).
        int64_t nB = 0, nA = 0, bytesB = 0, bytesA = 0;
        std::map<InstId, InstAgg> agg;
        for (const RowR& x : r->rows) {
            if (x.fid != f->fid) continue;
            if (x.loaded) {
                nB++;
                bytesB += x.len;
                if (x.bId) {
                    InstAgg& a = agg[InstId(x.bId, x.cId)];
                    a.b = true;
                    a.lenB = std::min(a.lenB, x.len);
                    a.any = &x;
                }
            }
            if (!x.del) {
                nA++;
                bytesA += x.len;
                if (x.bId) {
                    InstAgg& a = agg[InstId(x.bId, x.cId)];
                    a.a = true;
                    a.lenA = std::min(a.lenA, x.len);
                    a.maxat = std::max(a.maxat, x.at);
                    a.minat = std::min(a.minat, x.at);
                    if (!x.loaded || x.urlSet) {
                        a.changed = true;
                        a.url = x.url;
                    }
                    a.any = &x;
                }
            }
        }
        k.rows += nA - nB;
        k.bytes += bytesA - bytesB;
        const int64_t had = nB > 0, has = nA > 0;
        k.recs += has - had;
        if (sp->hasEpochRule && !r->hasE) k.nnull += has - had;
        if (has) {
            k.minseq = std::min(k.minseq, r->seq);
            k.maxseq = std::max(k.maxseq, r->seq);
            k.minw = std::min(k.minw, r->w);
            k.maxw = std::max(k.maxw, r->w);
            k.mints = std::min(k.mints, r->ts);
            k.maxts = std::max(k.maxts, r->ts);
            if (r->hasE) {
                k.mine = std::min(k.mine, r->e);
                k.maxe = std::max(k.maxe, r->e);
            }
        }
        for (auto& kv : agg) {
            InstAgg& a = kv.second;
            if (a.a == a.b && !a.changed && a.lenA == a.lenB) continue;
            changedInst.insert(kv.first);
            InstCount& ic = inst[kv.first];
            if (ic.n == 0 && a.any) {
                ic.batch = a.any->batch;
                ic.ppeer = a.any->ppeer;
                ic.pkey = a.any->pkey;
                ic.ckey = a.any->ckey;
            }
            ic.n += int64_t(a.a) - int64_t(a.b);
            ic.bytes += (a.a ? a.lenA : 0) - (a.b ? a.lenB : 0);
            if (a.a && (a.changed || !a.b)) {
                if (a.changed) ic.url = a.url;
                if (migrate) {
                    ic.updated = std::max(ic.updated, a.maxat);
                    ic.first = ic.first ? std::min(ic.first, a.minat) : a.minat;
                } else {
                    ic.updated = std::max(ic.updated, now);
                    if (!ic.first) ic.first = a.minat;
                }
                ic.maxat = std::max(ic.maxat, a.maxat);
                ic.maxts = std::max(ic.maxts, r->ts);
                ic.minw = std::min(ic.minw, r->w);
                ic.maxw = std::max(ic.maxw, r->w);
                ic.minseq = std::min(ic.minseq, r->seq);
                ic.maxseq = std::max(ic.maxseq, r->seq);
            }
        }
    }
    for (const InstId& id : changedInst) {
        if (rc != SQLITE_OK) break;
        auto it = inst.find(id);
        if (it == inst.end() || it->second.n <= 0) {
            if (it != inst.end()) inst.erase(it);
            sqlite3_stmt* q = c->get(S_INST_DEL);
            sqlite3_bind_int64(q, 1, id.first);
            sqlite3_bind_int64(q, 2, id.second);
            bad(sqlite3_step(q));
            sqlite3_reset(q);
            continue;
        }
        const InstCount& ic = it->second;
        sqlite3_stmt* q = c->get(S_INST_PUT);
        sqlite3_bind_int64(q, 1, id.first);
        sqlite3_bind_int64(q, 2, id.second);
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
        bad(sqlite3_step(q));
        sqlite3_reset(q);
    }
    if (rc == SQLITE_OK && deleted) {
        // The bounds again (the ends of the seq and w indexes: O(log n)); a
        // file without the indexes (a migration before REBUILD 1) walks.
        if (k.rows <= 0) {
            const int64_t keep = k.maxseq;
            k = Counters();
            k.maxseq = keep;  // seqs never go back
        } else {
            sqlite3_stmt* s = c->sql("SELECT min(seq), max(seq) FROM r");
            if (s && sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) {
                k.minseq = sqlite3_column_int64(s, 0);
                k.maxseq = std::max(k.maxseq, sqlite3_column_int64(s, 1));
            }
            if (s) sqlite3_reset(s);
            s = c->sql(indexed ? "SELECT min(w) FROM r INDEXED BY r_w" : "SELECT min(w) FROM r");
            if (s && sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) k.minw = sqlite3_column_int64(s, 0);
            if (s) sqlite3_reset(s);
            s = c->sql(indexed ? "SELECT max(w) FROM r INDEXED BY r_w" : "SELECT max(w) FROM r");
            if (s && sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) k.maxw = sqlite3_column_int64(s, 0);
            if (s) sqlite3_reset(s);
        }
    }
    if (rc == SQLITE_OK) rc = writeMeta(c, k, now);
    if (rc == SQLITE_OK) rc = c->exec("COMMIT");
    if (rc != SQLITE_OK) {
        err = std::string(sqlite3_errmsg(c->db)) + " (" + std::to_string(rc) + ") " + f->path;
        c->exec("ROLLBACK");
        writerUnpin(e, f);
        if ((rc & 0xff) == SQLITE_CORRUPT || (rc & 0xff) == SQLITE_NOTADB) {
            std::lock_guard<std::mutex> g(t->mu);
            f->quarantined = true;
        }
        // The in-memory rows keep their ids; the context is discarded.
        return statusOfSqlite(rc);
    }
    int64_t freeBytes = 0;
    const int64_t dbBytes = dbBytesOf(c, &freeBytes);
    writerUnpin(e, f);
    e->bump(kStGroupCommits);
    {
        std::lock_guard<std::mutex> g(f->dictMu);
        if (dict.node.size() >= f->dict.node.size() && dict.batch.size() >= f->dict.batch.size()) f->dict = std::move(dict);
    }
    std::lock_guard<std::mutex> g(t->mu);
    if (dbBytes >= 0) {
        f->dbBytes = dbBytes;
        f->freeBytes = freeBytes;
    }
    f->k = k;
    f->inst = std::move(inst);
    f->created = true;
    return P4_OK;
}

int32_t WriteCtx::indexCommit() {
    Conn* x = t->idx;
    if (!x) return P4_E_INTERNAL;
    TypeCounts tc;
    std::vector<FeedSnap> snaps;
    {
        std::lock_guard<std::mutex> g(t->mu);
        tc = typeCountsOf(t);
        tc.nextSeq = t->nextSeq;
        std::set<uint32_t> fids(committedFids.begin(), committedFids.end());
        for (auto& f : t->feeds)
            if (f->registered && !f->created && f->provider.rfind("\x1f#", 0) != 0) fids.insert(f->fid);
        for (uint32_t fid : fids)
            if (Feed* f = t->feedById(fid)) snaps.push_back(feedSnapOf(f));
    }
    int rc = x->exec("BEGIN IMMEDIATE");
    int64_t entries = 0;
    std::vector<int64_t> gone;  // records left with no row anywhere
    for (RecState* r : order_) {
        if (rc != SQLITE_OK) break;
        if (r->touched.empty() || !r->seq) continue;
        XChange xc;
        xc.before = r->before;
        xc.ts = r->ts;
        std::set<uint32_t> all;
        for (const XEnt& b : r->before) all.insert(b.fid);
        for (uint32_t fid : r->fids) all.insert(fid);
        for (uint32_t fid : r->touched) all.insert(fid);
        for (uint32_t fid : all) {
            if (r->touched.count(fid) && committedFids.count(fid)) {
                XEnt en;
                if (entryOf(*r, fid, &en)) xc.after.push_back(std::move(en));
            } else {
                for (const XEnt& b : r->before)
                    if (b.fid == fid) xc.after.push_back(b);
            }
        }
        xCount(xc, &tc);
        rc = xWrite(x, xc);
        entries += int64_t(xc.after.size());
        if (!xc.before.empty() && xc.after.empty()) gone.push_back(r->seq);
    }
    for (const IdentNew& id : idents) {
        if (rc != SQLITE_OK) break;
        if (!id.rec || !id.rec->seq) continue;
        bool live = false;
        for (const RowR& rr : id.rec->rows) live = live || (rr.live() && committedFids.count(rr.fid));
        if (!live) continue;
        sqlite3_stmt* q = x->get(S_IDENT_INS);
        if (!q) {
            rc = SQLITE_ERROR;
            break;
        }
        sqlite3_bind_int64(q, 1, int64_t(id.src));
        sqlite3_bind_blob(q, 2, id.h, 32, SQLITE_STATIC);
        sqlite3_bind_int64(q, 3, id.rec->seq);
        sqlite3_bind_blob(q, 4, id.rec->key, 32, SQLITE_STATIC);
        const int r = sqlite3_step(q);
        sqlite3_reset(q);
        if (r != SQLITE_DONE) rc = r;
    }
    for (const FeedSnap& s : snaps) {
        if (rc != SQLITE_OK) break;
        rc = indexPutFeed(x, s);
    }
    {
        // Tokens this write registered, with their counters.
        std::lock_guard<std::mutex> g(t->mu);
        for (size_t i = 0; i < t->toks.size() && i < tc.toks.size(); i++) {
            tc.toks[i].token = t->toks[i].token;
            tc.toks[i].peer = t->toks[i].peer;
        }
        for (size_t i = tc.toks.size(); i < t->toks.size(); i++) tc.toks.push_back(t->toks[i]);
        for (RecState* r : order_)
            for (const RowR& rr : r->rows)
                if (rr.tok && rr.tok <= tc.toks.size()) tc.dirtyToks.insert(rr.tok);
    }
    if (rc == SQLITE_OK) rc = indexPutCounts(x, tc);
    if (rc == SQLITE_OK) rc = x->exec("COMMIT");
    if (rc != SQLITE_OK) {
        err = std::string("type index: ") + sqlite3_errmsg(x->db) + " (" + std::to_string(rc) + ")";
        x->exec("ROLLBACK");
        return statusOfSqlite(rc);
    }
    e->bump(kStIndexFlushes);
    e->bump(kStIndexFlushEntries, uint64_t(entries));
    if (!gone.empty() && sp->fullText) {
        std::lock_guard<std::mutex> g(t->ftsGoneMu);
        t->ftsGone.insert(t->ftsGone.end(), gone.begin(), gone.end());
    }
    std::lock_guard<std::mutex> g(t->mu);
    typeCountsTo(t, tc);
    if (jlast > 0) t->jcut = jlast;
    return P4_OK;
}

int32_t WriteCtx::commit(bool journal) {
    bool any = false;
    for (RecState* r : order_) {
        if (!r->seq) r->touched.clear();  // a new record this write refused: never stored
        any = any || !r->touched.empty();
    }
    if (!any) return P4_OK;
    if (journal) {
        const int32_t rc = journalWrite();
        if (rc != P4_OK) return rc;
    }
    // Feed files, then local files (a cut between them leaves rows that only add).
    std::set<uint32_t> fids;
    for (RecState* r : order_)
        for (uint32_t fid : r->touched) fids.insert(fid);
    std::vector<uint32_t> order;
    std::vector<uint32_t> locals;
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (uint32_t fid : fids) {
            Feed* f = t->feedById(fid);
            if (f && f->local) locals.push_back(fid);
            else order.push_back(fid);
        }
    }
    order.insert(order.end(), locals.begin(), locals.end());
    int32_t first = P4_OK;
    for (uint32_t fid : order) {
        Feed* f;
        {
            std::lock_guard<std::mutex> g(t->mu);
            f = t->feedById(fid);
        }
        if (!f) continue;
        if (first != P4_OK) {
            failedFids.insert(fid);
            continue;
        }
        std::vector<RecState*> recs;
        for (RecState* r : order_)
            if (r->touched.count(fid) && r->seq) recs.push_back(r);
        const int32_t rc = commitFile(f, recs);
        if (rc == P4_OK) {
            committedFids.insert(fid);
        } else {
            failedFids.insert(fid);
            first = rc;
        }
    }
    const int32_t irc = indexCommit();
    if (irc != P4_OK) {
        // The index lags the files: the journal (not cut) brings it in line.
        const std::string why = err;
        std::string rerr;
        const int32_t rr = journal ? journalReplay(t, &rerr) : irc;
        if (rr != P4_OK) {
            std::lock_guard<std::mutex> g(t->mu);
            t->broken = true;
            t->brokenWhy = why + (rerr.empty() ? "" : "; replay: " + rerr);
        }
        err = why;
        return irc;
    }
    return first;
}

}  // namespace p4
}  // namespace flatsql
