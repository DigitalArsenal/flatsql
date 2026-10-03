// Store format 4: a record's rows in one feed file (CONTRACT C-37, C-38),
// and how a write changes and commits them.
//
// A record of a feed file is found by its CID (the file's one CID index) or
// its seq; its rows are one rid range of that file (rid = seq << 16 | n):
// every copy (producer token) appears with every instance (batch, content
// key, producer peer and key) of the feed; a local record (no source) has one
// row per copy. A write loads the records it touches, changes their rows,
// then commits:
//   1. the journal (synchronous=FULL): the feed files it touches (J_TOUCH),
//      new feed / token ids, the seq reservation;
//   2. each feed file in one transaction (rows, the file's counters, its
//      instances' and tokens' counters, its ingest identities), feeds before
//      local;
//   3. the type index in one transaction: the touched files' mirrors, new
//      tokens, the next seq.
// Open replays the journal tail (journal.cpp).
#include <algorithm>

#include "internal.h"

namespace flatsql {
namespace p4 {

WriteCtx::WriteCtx(Engine* e_, Type* t_) : e(e_), t(t_) {
    sp = t->spec();
    now = nowSec();
}

WriteCtx::~WriteCtx() {}

RecState* WriteCtx::known(uint32_t fid, int64_t seq) {
    auto it = bySeq_.find({fid, seq});
    return it == bySeq_.end() ? nullptr : it->second;
}

RecState* WriteCtx::make(uint32_t fid, int64_t seq, const uint8_t* key) {
    store_.emplace_back();
    RecState* r = &store_.back();
    r->fid = fid;
    r->seq = seq;
    if (key) {
        std::memcpy(r->key, key, 32);
        r->keyed = true;
    }
    order_.push_back(r);
    return r;
}

int32_t WriteCtx::loadRows(RecState* r) {
    Feed* f;
    bool created, quarantined;
    {
        std::lock_guard<std::mutex> g(t->mu);
        f = t->feedById(r->fid);
        created = f && f->created;
        quarantined = f && f->quarantined;
    }
    if (!f || !created || !r->seq) return P4_OK;
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
    bool haveKey = false;
    std::vector<RowR> got;
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) {
        RowR x;
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
            r->keyed = true;
            haveKey = true;
            r->hasE = sqlite3_column_type(q, 7) != SQLITE_NULL;
            r->e = sqlite3_column_int64(q, 7);
            r->k.from(q, 8);
            r->ts = sqlite3_column_int64(q, 9);
            r->w = r->hasE ? r->e : r->ts;
        } else if (std::memcmp(r->key, cid, 32) != 0) {
            rc = SQLITE_CORRUPT;  // one seq is one CID in a file
            break;
        }
        x.ts = sqlite3_column_int64(q, 9);
        x.sealed = sqlite3_column_type(q, 10) != SQLITE_NULL;
        if (x.sealed) x.fcols.assign(static_cast<const char*>(sqlite3_column_blob(q, 10)), size_t(sqlite3_column_bytes(q, 10)));
        if (sqlite3_column_type(q, 11) != SQLITE_NULL)
            x.sig.assign(static_cast<const char*>(sqlite3_column_blob(q, 11)), size_t(sqlite3_column_bytes(q, 11)));
        x.len = sqlite3_column_int64(q, 12);
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
    r->existed = !got.empty();
    r->rows = std::move(got);
    return P4_OK;
}

RecState* WriteCtx::byKey(uint32_t fid, const uint8_t key[32], int32_t* rc) {
    *rc = P4_OK;
    const auto k = std::make_pair(fid, std::string(reinterpret_cast<const char*>(key), 32));
    auto it = byKey_.find(k);
    if (it != byKey_.end()) return it->second;
    Feed* f;
    bool created, indexed;
    {
        std::lock_guard<std::mutex> g(t->mu);
        f = t->feedById(fid);
        created = f && f->created;
        indexed = f && f->indexed;
    }
    int64_t seq = 0;
    if (created) {
        std::string er;
        Conn* c = writerPin(e, f, rc, &er);
        if (!c) {
            err = "writer open: " + er;
            return nullptr;
        }
        *rc = seqOfCid(c, indexed, key, &seq);
        writerUnpin(e, f);
        if (*rc != P4_OK) {
            err = "CID lookup in " + f->path;
            return nullptr;
        }
    }
    if (seq) {
        RecState* r = known(fid, seq);
        if (!r) {
            r = make(fid, seq, key);
            *rc = loadRows(r);
            if (*rc != P4_OK) return nullptr;
            bySeq_[{fid, seq}] = r;
        }
        byKey_[k] = r;
        return r;
    }
    RecState* r = make(fid, 0, key);
    byKey_[k] = r;
    return r;
}

RecState* WriteCtx::bySeq(uint32_t fid, int64_t seq, int32_t* rc, const uint8_t* keyIfNew) {
    *rc = P4_OK;
    RecState* r = known(fid, seq);
    if (!r) {
        r = make(fid, seq, nullptr);
        *rc = loadRows(r);
        if (*rc != P4_OK) return nullptr;
        bySeq_[{fid, seq}] = r;
        if (r->existed) byKey_[{fid, std::string(reinterpret_cast<const char*>(r->key), 32)}] = r;
    }
    // A seq the file does not hold is a new record of the file once a caller
    // names its CID. A probe that asked without one (is the seq here?) keeps
    // the state keyless; the caller that names the CID keys it, whenever it
    // asks (C-39 B1: a cached keyless state wrote a zero CID).
    if (!r->keyed && keyIfNew) {
        std::memcpy(r->key, keyIfNew, 32);
        r->keyed = true;
        byKey_.emplace(std::make_pair(fid, std::string(reinterpret_cast<const char*>(r->key), 32)), r);
    }
    return r;
}

int32_t WriteCtx::loadD(uint32_t fid, RowR& x) {
    if (x.hasD) return P4_OK;
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
        sqlite3_bind_int64(q, 1, x.rid);
        r = sqlite3_step(q);
        if (r == SQLITE_ROW) {
            x.d.assign(static_cast<const char*>(sqlite3_column_blob(q, 0)), size_t(sqlite3_column_bytes(q, 0)));
            x.hasD = true;
        }
        sqlite3_reset(q);
    }
    writerUnpin(e, f);
    if (r == SQLITE_ROW) return P4_OK;
    err = "bytes of row " + std::to_string(x.rid) + " in " + f->path;
    return r == SQLITE_DONE ? P4_E_CORRUPT : statusOfSqlite(r);
}

// ---- changes ------------------------------------------------------------------------------------
namespace {
bool sameInst(const RowR& a, const WriteCtx::Inst& i) {
    return a.inst && a.batch == i.batch && a.ppeer == i.ppeer && a.pkey == i.pkey && a.ckey == i.ckey;
}
void sweep(RecState* r) {
    // New rows that are gone again (a superseded record of this write) are dropped.
    r->rows.erase(std::remove_if(r->rows.begin(), r->rows.end(), [](const RowR& x) { return !x.loaded && x.del; }),
                  r->rows.end());
}
}  // namespace

// Every copy with every instance of the feed (a local record: one row per copy).
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
    if (insts.empty()) return;  // local rows: one per copy, as delivered
    // An instance's at is the earliest of its rows; its url the one its rows
    // carry (kept equal by deliver).
    auto instAt = [&](size_t ir) {
        int64_t at = r->rows[ir].at;
        for (const RowR& x : r->rows)
            if (x.live() && x.sameInst(r->rows[ir])) at = std::min(at, x.at);
        return at;
    };
    std::vector<RowR> add;
    for (size_t ci : copies)
        for (size_t ii : insts) {
            bool have = false;
            for (const RowR& x : r->rows) have = have || (x.live() && x.tok == r->rows[ci].tok && x.sameInst(r->rows[ii]));
            if (have) continue;
            const RowR& c = r->rows[ci];
            const RowR& in = r->rows[ii];
            RowR n;
            n.tok = c.tok;
            n.peer = c.peer;
            n.inst = true;
            n.batch = in.batch;
            n.ppeer = in.ppeer;
            n.pkey = in.pkey;
            n.ckey = in.ckey;
            n.url = in.url;
            n.at = instAt(ii);
            n.ts = c.ts;
            n.len = c.len;
            n.sig = c.sig;
            n.fcols = c.fcols;
            n.sealed = c.sealed;
            if (c.hasD) {
                n.hasD = true;
                n.d = c.d;
            } else if (c.loaded) {
                n.srcFid = r->fid;
                n.srcRid = c.rid;
            } else {
                n.srcFid = c.srcFid;
                n.srcRid = c.srcRid;
            }
            add.push_back(std::move(n));
        }
    for (RowR& n : add) r->rows.push_back(std::move(n));
    if (!add.empty()) r->touched = true;
    sweep(r);
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
    auto newRow = [&](const Inst* in, bool stamp) {
        RowR n;
        n.tok = rep.tok;
        n.peer = rep.peer;
        n.ts = rep.ts;
        n.len = rep.len;
        n.sig = rep.sig;
        n.fcols = rep.fcols;
        n.sealed = rep.sealed;
        if (rep.hasD) {
            n.hasD = true;
            n.d = rep.d;
        } else if (rep.loaded) {
            n.srcFid = r->fid;
            n.srcRid = rep.rid;
        } else {
            n.srcFid = rep.srcFid;
            n.srcRid = rep.srcRid;
        }
        if (in) {
            n.inst = true;
            n.batch = in->batch;
            n.ppeer = in->ppeer;
            n.pkey = in->pkey;
            n.ckey = in->ckey;
            n.url = in->url;
            n.at = in->at;
            n.stamp = stamp;
        }
        r->touched = true;
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
                r->touched = true;
                out->urlChanged = true;
            }
        }
        if (!held) out->instNew = true;
        bool row = false;
        for (const RowR& x : r->rows) row = row || (x.live() && x.tok == rep.tok && sameInst(x, in));
        if (!row) newRow(&in, true);
    }
    // A new copy without new instances: a row with each instance the record
    // has in this file, or one local row.
    if (!copyAt && insts.empty()) {
        std::vector<Inst> have;
        for (const RowR& x : r->rows) {
            if (!x.live() || !x.inst) continue;
            bool seen = false;
            for (Inst& h : have)
                if (sameInst(x, h)) {
                    seen = true;
                    h.at = std::min(h.at, x.at);
                }
            if (!seen) have.push_back(Inst{x.batch, x.ppeer, x.pkey, x.ckey, x.url, x.at});
        }
        if (have.empty()) newRow(nullptr, false);
        for (const Inst& in : have) newRow(&in, false);
    }
    fill(r);
}

void WriteCtx::dropAll(RecState* r) {
    for (RowR& x : r->rows)
        if (x.live()) {
            x.del = true;
            r->touched = true;
        }
    sweep(r);
}

// ---- commit ---------------------------------------------------------------------------------------
int32_t WriteCtx::journalWrite() {
    Conn* j = t->jdb;
    if (!j) return P4_E_INTERNAL;
    // Ids this write uses that no durable record names yet.
    std::vector<std::pair<uint32_t, std::string>> feeds;
    std::vector<std::pair<uint32_t, std::string>> toks;
    std::set<uint32_t> fids;
    int64_t reserve = 0;
    {
        std::lock_guard<std::mutex> g(t->mu);
        std::set<uint32_t> tokIds;
        for (RecState* r : order_) {
            if (!r->touched) continue;
            fids.insert(r->fid);
            for (const RowR& x : r->rows) tokIds.insert(x.tok);
        }
        for (uint32_t fid : fids) {
            Feed* f = t->feedById(fid);
            if (f && !f->registered) feeds.push_back({fid, unitJoin({&f->provider, &f->source, &f->name})});
        }
        for (uint32_t id : tokIds) {
            TokDef* d = t->tokById(id);
            if (d && !d->registered) toks.push_back({id, unitJoin({&d->token, &d->peer})});
        }
        if (t->nextSeq - 1 > t->seqReserved) reserve = t->nextSeq - 1 + int64_t(e->cfg.seqBlock);
    }
    int rc = j->exec("BEGIN IMMEDIATE");
    int64_t rows = 0;
    auto add = [&](int op, int64_t fid, const std::string* s, int64_t v) {
        if (rc != SQLITE_OK) return;
        sqlite3_stmt* q = j->get(S_J_INS);
        if (!q) {
            rc = SQLITE_ERROR;
            return;
        }
        sqlite3_bind_int(q, 1, op);
        sqlite3_bind_int64(q, 2, fid);
        sqlite3_bind_int64(q, 3, 0);
        sqlite3_bind_null(q, 4);
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
    for (auto& f : feeds) add(J_FEED, f.first, &f.second, 0);
    for (auto& d : toks) add(J_TOK, 0, &d.second, d.first);
    for (uint32_t fid : fids) add(J_TOUCH, fid, nullptr, 0);
    // Moves: a record whose rows leave one file while its seq gains rows in
    // another file of this write (a local record taken into a feed).
    for (RecState* r : order_) {
        if (rc != SQLITE_OK) break;
        if (!r->touched || !r->existed || !r->seq) continue;
        bool live = false;
        for (const RowR& x : r->rows) live = live || x.live();
        if (live) continue;
        bool gains = false;
        for (RecState* o : order_) {
            if (o == r || o->seq != r->seq || o->fid == r->fid) continue;
            for (const RowR& x : o->rows) gains = gains || (x.live() && !x.loaded);
        }
        if (!gains) continue;
        sqlite3_stmt* q = j->get(S_J_INS);
        if (!q) {
            rc = SQLITE_ERROR;
            break;
        }
        sqlite3_bind_int(q, 1, J_MOVE);
        sqlite3_bind_int64(q, 2, r->fid);
        sqlite3_bind_int64(q, 3, r->seq);
        sqlite3_bind_null(q, 4);
        sqlite3_bind_null(q, 5);
        sqlite3_bind_int64(q, 6, 0);
        const int x = sqlite3_step(q);
        sqlite3_reset(q);
        if (x != SQLITE_DONE) rc = x;
        rows++;
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

// A record's copies over its rows (loaded = before, live = after): token ->
// its smallest length.
std::map<uint32_t, int64_t> copiesOf(const RecState* r, bool after) {
    std::map<uint32_t, int64_t> m;
    for (const RowR& x : r->rows) {
        if (after ? x.del : !x.loaded) continue;
        auto it = m.find(x.tok);
        if (it == m.end() || x.len < it->second) m[x.tok] = x.len;
    }
    return m;
}
}  // namespace

int32_t WriteCtx::commitFile(Feed* f, const std::vector<RecState*>& recs) {
    bool change = false;
    for (RecState* r : recs)
        for (const RowR& x : r->rows)
            if ((!x.loaded && !x.del) || (x.loaded && x.del) || (x.loaded && !x.del && x.urlSet)) change = true;
    bool identWrites = false;
    for (const IdentNew& id : idents) identWrites = identWrites || (id.rec && id.rec->fid == f->fid && id.rec->seq);
    if (!change && !identWrites) return P4_OK;
    // A new row is stored under its record's CID: a record still without one
    // is a bug, refused before anything is written (C-39 B1).
    for (RecState* r : recs) {
        bool adds = false;
        for (const RowR& x : r->rows) adds = adds || (!x.loaded && !x.del);
        if (adds && !r->keyed) {
            err = "a new row of seq " + std::to_string(r->seq) + " has no CID: " + f->path;
            return P4_E_INTERNAL;
        }
    }
    // The bytes new rows copy from other rows, before the transaction.
    for (RecState* r : recs)
        for (RowR& x : r->rows)
            if (!x.loaded && !x.del && !x.hasD) {
                RowR* src = nullptr;
                if (x.srcFid == r->fid)
                    for (RowR& y : r->rows)
                        if (y.loaded && y.rid == x.srcRid) src = &y;
                if (src) {
                    const int32_t st = loadD(r->fid, *src);
                    if (st != P4_OK) return st;
                    x.d = src->d;
                    x.hasD = true;
                } else {
                    RowR tmp;
                    tmp.loaded = true;
                    tmp.rid = x.srcRid;
                    const int32_t st = loadD(x.srcFid, tmp);
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
    std::map<uint32_t, TokCount> tokc;
    bool indexed;
    {
        std::lock_guard<std::mutex> g(t->mu);
        k = f->k;
        inst = f->inst;
        tokc = f->tokc;
        indexed = f->indexed;
    }
    const std::map<uint32_t, TokCount> tokcBefore = tokc;
    std::set<InstId> changedInst;
    std::vector<int64_t> gone;  // records that leave the file
    bool deleted = false;
    bool epochEdge = false;     // a record at the file's epoch bound left it
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
        // The file's, its instances' and its tokens' counters: this record's
        // rows before (as loaded) and after (live).
        int64_t nB = 0, nA = 0, bytesB = 0, bytesA = 0;
        std::map<InstId, InstAgg> agg;
        for (const RowR& x : r->rows) {
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
                    if ((!x.loaded && x.stamp) || x.urlSet) {
                        a.changed = true;
                        a.url = x.url;
                    }
                    a.any = &x;
                }
            }
        }
        const std::map<uint32_t, int64_t> cpB = copiesOf(r, false), cpA = copiesOf(r, true);
        // The live rows' ts and w (a copy may keep its own ts, C-39 E6), and
        // each copy's.
        int64_t tsLo = INT64_MAX, tsHi = INT64_MIN, wLo = INT64_MAX, wHi = INT64_MIN;
        std::map<uint32_t, std::pair<int64_t, int64_t>> tokTs;
        for (const RowR& x : r->rows) {
            if (x.del) continue;
            const int64_t xw = r->hasE ? r->e : x.ts;
            tsLo = std::min(tsLo, x.ts);
            tsHi = std::max(tsHi, x.ts);
            wLo = std::min(wLo, xw);
            wHi = std::max(wHi, xw);
            auto it = tokTs.find(x.tok);
            if (it == tokTs.end()) tokTs[x.tok] = {x.ts, x.ts};
            else it->second = {std::min(it->second.first, x.ts), std::max(it->second.second, x.ts)};
        }
        auto minLen = [](const std::map<uint32_t, int64_t>& m) {
            int64_t v = INT64_MAX;
            for (auto& kv : m) v = std::min(v, kv.second);
            return m.empty() ? int64_t(0) : v;
        };
        auto sumLen = [](const std::map<uint32_t, int64_t>& m) {
            int64_t v = 0;
            for (auto& kv : m) v += kv.second;
            return v;
        };
        k.rows += nA - nB;
        k.bytes += bytesA - bytesB;
        const int64_t had = nB > 0, has = nA > 0;
        k.recs += has - had;
        k.rbytes += minLen(cpA) - minLen(cpB);
        k.copies += int64_t(cpA.size()) - int64_t(cpB.size());
        k.cbytes += sumLen(cpA) - sumLen(cpB);
        if (sp->hasEpochRule && !r->hasE) k.nnull += has - had;
        if (had && !has) {
            gone.push_back(r->seq);
            if (r->hasE && (r->e <= k.mine || r->e >= k.maxe)) epochEdge = true;
        }
        if (has) {
            k.minseq = std::min(k.minseq, r->seq);
            k.maxseq = std::max(k.maxseq, r->seq);
            k.minw = std::min(k.minw, wLo);
            k.maxw = std::max(k.maxw, wHi);
            k.mints = std::min(k.mints, tsLo);
            k.maxts = std::max(k.maxts, tsHi);
            if (r->hasE) {
                k.mine = std::min(k.mine, r->e);
                k.maxe = std::max(k.maxe, r->e);
            }
        }
        std::set<uint32_t> toks;
        for (auto& kv : cpA) toks.insert(kv.first);
        for (auto& kv : cpB) toks.insert(kv.first);
        for (uint32_t tok : toks) {
            auto ia = cpA.find(tok), ib = cpB.find(tok);
            const bool inA = ia != cpA.end(), inB = ib != cpB.end();
            if (inA == inB && (!inA || ia->second == ib->second)) continue;
            TokCount& d = tokc[tok];
            d.n += int64_t(inA) - int64_t(inB);
            d.bytes += (inA ? ia->second : 0) - (inB ? ib->second : 0);
            if (inA) {
                const auto& tt = tokTs[tok];
                d.mints = std::min(d.mints, tt.first);
                d.maxts = std::max(d.maxts, tt.second);
                d.maxseq = std::max(d.maxseq, r->seq);
            }
        }
        for (auto& kv : agg) {
            InstAgg& a = kv.second;
            // A DELETE or CAT supersede restamps the instances its record
            // leaves (format 1's decrementSourceSummary, C-39 E5).
            const bool restamp = r->restamp && a.b && !a.a;
            if (a.a == a.b && !a.changed && a.lenA == a.lenB && !restamp) continue;
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
                ic.maxts = std::max(ic.maxts, tsHi);
                ic.minw = std::min(ic.minw, wLo);
                ic.maxw = std::max(ic.maxw, wHi);
                ic.minseq = std::min(ic.minseq, r->seq);
                ic.maxseq = std::max(ic.maxseq, r->seq);
            }
            if (restamp) ic.updated = now;
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
    for (auto it = tokc.begin(); it != tokc.end();) {
        if (it->second.n <= 0) it = tokc.erase(it);
        else ++it;
    }
    if (rc == SQLITE_OK) rc = writeTokc(t, c, tokcBefore, tokc);
    // Ingest identities of this file's records that hold rows after the write.
    for (const IdentNew& id : idents) {
        if (rc != SQLITE_OK) break;
        if (!id.rec || id.rec->fid != f->fid || !id.rec->seq) continue;
        bool live = false;
        for (const RowR& rr : id.rec->rows) live = live || rr.live();
        if (!live) continue;
        sqlite3_stmt* q = c->get(S_IDENT_INS);
        if (!q) {
            rc = SQLITE_ERROR;
            break;
        }
        sqlite3_bind_blob(q, 1, id.h, 32, SQLITE_STATIC);
        sqlite3_bind_int64(q, 2, id.rec->seq);
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
            // The epoch range follows a delete at its edge (the type's
            // epoch range, SchemaDateRanges): among rows with an epoch w is
            // the epoch, so each end of r_w to its first such row.
            if (epochEdge) {
                k.mine = INT64_MAX;
                k.maxe = INT64_MIN;
                s = c->sql(indexed ? "SELECT e FROM r INDEXED BY r_w WHERE e IS NOT NULL ORDER BY w ASC LIMIT 1"
                                   : "SELECT min(e) FROM r");
                if (s && sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) k.mine = sqlite3_column_int64(s, 0);
                if (s) sqlite3_reset(s);
                s = c->sql(indexed ? "SELECT e FROM r INDEXED BY r_w WHERE e IS NOT NULL ORDER BY w DESC LIMIT 1"
                                   : "SELECT max(e) FROM r");
                if (s && sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) k.maxe = sqlite3_column_int64(s, 0);
                if (s) sqlite3_reset(s);
            }
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
    if (!gone.empty() && sp->fullText) {
        // A seq that moved to another file in this write (a record's local
        // rows taken into a feed) keeps its full-text row.
        std::vector<int64_t> drop;
        for (int64_t seq : gone) {
            bool elsewhere = false;
            for (RecState* o : order_) {
                if (o->seq != seq || o->fid == f->fid) continue;
                for (const RowR& x : o->rows) elsewhere = elsewhere || x.live();
            }
            if (!elsewhere) drop.push_back(seq);
        }
        std::lock_guard<std::mutex> g(t->ftsGoneMu);
        t->ftsGone.insert(t->ftsGone.end(), drop.begin(), drop.end());
    }
    std::lock_guard<std::mutex> g(t->mu);
    if (dbBytes >= 0) {
        f->dbBytes = dbBytes;
        f->freeBytes = freeBytes;
    }
    f->k = k;
    f->inst = std::move(inst);
    f->tokc = std::move(tokc);
    f->created = true;
    return P4_OK;
}

int32_t WriteCtx::indexCommit() {
    Conn* x = t->idx;
    if (!x) return P4_E_INTERNAL;
    std::vector<FeedSnap> snaps;
    std::vector<std::pair<uint32_t, TokDef>> toks;
    int64_t nextSeq;
    {
        std::lock_guard<std::mutex> g(t->mu);
        nextSeq = t->nextSeq;
        std::set<uint32_t> fids(committedFids.begin(), committedFids.end());
        for (auto& f : t->feeds)
            if (f->registered && !f->created && f->provider.rfind("\x1f#", 0) != 0) fids.insert(f->fid);
        for (uint32_t fid : fids)
            if (Feed* f = t->feedById(fid)) snaps.push_back(feedSnapOf(f));
        std::set<uint32_t> ids;
        for (RecState* r : order_)
            for (const RowR& rr : r->rows) ids.insert(rr.tok);
        for (uint32_t id : ids)
            if (TokDef* d = t->tokById(id)) toks.push_back({id, *d});
    }
    int rc = x->exec("BEGIN IMMEDIATE");
    for (auto& d : toks)
        if (rc == SQLITE_OK) rc = indexPutTok(x, d.first, d.second);
    for (const FeedSnap& s : snaps)
        if (rc == SQLITE_OK) rc = indexPutFeed(x, s);
    if (rc == SQLITE_OK) rc = indexPutNextSeq(x, nextSeq);
    if (rc == SQLITE_OK) rc = x->exec("COMMIT");
    if (rc != SQLITE_OK) {
        err = std::string("type index: ") + sqlite3_errmsg(x->db) + " (" + std::to_string(rc) + ")";
        x->exec("ROLLBACK");
        return statusOfSqlite(rc);
    }
    e->bump(kStIndexFlushes);
    e->bump(kStIndexFlushEntries, uint64_t(snaps.size()));
    std::lock_guard<std::mutex> g(t->mu);
    if (jlast > 0) t->jcut = jlast;
    return P4_OK;
}

int32_t WriteCtx::commit() {
    bool any = false;
    for (RecState* r : order_) {
        if (!r->seq) r->touched = false;  // a new record this write refused: never stored
        any = any || r->touched;
    }
    for (const IdentNew& id : idents)
        if (id.rec && id.rec->seq) {
            id.rec->touched = true;
            any = true;
        }
    if (!any) return P4_OK;
    if (!replaying) {
        const int32_t jrc = journalWrite();
        if (jrc != P4_OK) return jrc;
    }
    // Feed files, then local files.
    std::set<uint32_t> fids;
    for (RecState* r : order_)
        if (r->touched) fids.insert(r->fid);
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
            if (r->touched && r->fid == fid && r->seq) recs.push_back(r);
        const int32_t rc = commitFile(f, recs);
        if (rc == P4_OK) {
            committedFids.insert(fid);
        } else {
            failedFids.insert(fid);
            first = rc;
        }
    }
    const int32_t irc = indexCommit();
    if (irc != P4_OK && replaying) return irc;
    if (irc != P4_OK) {
        // The index lags the files: the journal (not cut) brings it in line.
        const std::string why = err;
        std::string rerr;
        const int32_t rr = journalReplay(t, &rerr);
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
