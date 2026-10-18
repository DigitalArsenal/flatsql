// Store format 5: the stream commits each record's rows and provenance in
// sibling SDS metadata frames. SQLite is a derived, staged index. Planning
// assigns seqs and rids; the indexer appends each complete round, syncs its
// touched streams, commits the indexes at NORMAL, then publishes the views.
// Local-to-feed moves persist MOVED_IN first and MOVED_OUT in local second.
#include <algorithm>
#include <future>

#include "internal.h"
#include "checkpoint.h"

namespace flatsql {
namespace p4 {

WriteCtx::WriteCtx(Engine* e_, Type* t_) : e(e_), t(t_) {
    sp = t->spec();
    now = nowSec();
}

WriteCtx::~WriteCtx() {}

Conn* planAcquire(Engine* e, Feed* f, int32_t* rc, std::string* err) {
    int orc = 0;
    Conn* c = e->rpool.acquire(f->path, OpenKind::Reader, &orc, err);
    if (!c) *rc = statusOfSqlite(orc);
    return c;
}
void planRelease(Engine* e, Conn* c) { e->rpool.release(c); }

RecState* WriteCtx::known(uint32_t fid, int64_t seq) {
    auto it = bySeq_.find({fid, seq});
    if (it != bySeq_.end()) return it->second;
    // A record a pending unit of the type touched: its state after that unit.
    for (const WriteCtx* p : seeds_) {
        auto s = p->postSeq_.find({fid, seq});
        if (s != p->postSeq_.end()) return materialize(p->post_[s->second]);
    }
    return nullptr;
}

// A pending unit's record, copied into this write (once).
RecState* WriteCtx::materialize(const RecState& s) {
    store_.push_back(s);
    RecState* r = &store_.back();
    order_.push_back(r);
    bySeq_[{r->fid, r->seq}] = r;
    if (r->keyed) byKey_[{r->fid, std::string(reinterpret_cast<const char*>(r->key), 32)}] = r;
    return r;
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
    Conn* c = planAcquire(e, f, &st, &er);
    if (!c) {
        err = "read open: " + er;
        return st;
    }
    if (!dictEnsure(f, c)) {
        planRelease(e, c);
        err = "dictionary of " + f->path;
        return P4_E_IO;
    }
    sqlite3_stmt* q = c->get(S_R_SEQ);
    if (!q) {
        planRelease(e, c);
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
        x.off = sqlite3_column_int64(q, 13);
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
    planRelease(e, c);
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
    for (const WriteCtx* p : seeds_) {
        auto sk = p->postKey_.find(k);
        if (sk != p->postKey_.end()) {
            // The latest state of that record's seq (a newer unit may hold it).
            const RecState& s0 = p->post_[sk->second];
            return known(s0.fid, s0.seq);
        }
    }
    Feed* f;
    bool created, indexed;
    std::shared_ptr<const Staged> st;
    {
        std::lock_guard<std::mutex> g(t->mu);
        f = t->feedById(fid);
        created = f && f->created;
        indexed = f && f->indexed;
        if (f) st = f->staged;  // pinned before the read below
    }
    int64_t seq = 0;
    if (created) {
        std::string er;
        Conn* c = planAcquire(e, f, rc, &er);
        if (!c) {
            err = "read open: " + er;
            return nullptr;
        }
        *rc = seqOfCid(c, indexed, st.get(), key, &seq);
        planRelease(e, c);
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
    int64_t off = x.off, len = x.len;
    if (off < 0) {
        // The row's frame from the index (a row this write did not load).
        std::string er;
        Conn* c = planAcquire(e, f, &st, &er);
        if (!c) {
            err = "read open: " + er;
            return st;
        }
        sqlite3_stmt* q = c->get(S_R_FRAME);
        int r = SQLITE_ERROR;
        if (q) {
            sqlite3_bind_int64(q, 1, x.rid);
            r = sqlite3_step(q);
            if (r == SQLITE_ROW) {
                off = sqlite3_column_int64(q, 0);
                len = sqlite3_column_int64(q, 1);
            }
            sqlite3_reset(q);
        }
        planRelease(e, c);
        if (r != SQLITE_ROW) {
            err = "frame of row " + std::to_string(x.rid) + " in " + f->path;
            return r == SQLITE_DONE ? P4_E_CORRUPT : statusOfSqlite(r);
        }
    }
    std::shared_ptr<Stream> stream = streamCur(f, &st);
    if (!stream) {
        err = "stream of " + f->path;
        return st;
    }
    st = streamRead(stream.get(), off, len, &x.d);
    if (st != P4_OK) {
        err = "frame at " + std::to_string(off) + " in " + stream->path;
        return st;
    }
    x.hasD = true;
    return P4_OK;
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
            n.commitFlags = FmbCopy;
            n.deliveryGroup = c.deliveryGroup;
            n.off = c.off;  // a frame already in this file (a loaded row's), or -1
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
        n.commitFlags = r->existed ? ((!copyAt ? FmbCopy : 0) | (stamp ? FmbRetag : 0)) : 0;
        n.deliveryGroup = copy.deliveryGroup;
        n.off = rep.off;  // a frame already in this file, or -1
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

void WriteCtx::dropAll(RecState* r, uint8_t reason) {
    for (RowR& x : r->rows)
        if (x.live()) {
            x.del = true;
            x.removeReason = reason;
            r->touched = true;
        }
    sweep(r);
}

// ---- commit ---------------------------------------------------------------------------------------
// Cache new feed and token ids while planning. Their durable definitions
// live in genesis and FMB dictionary frames; this registry uses NORMAL.
int32_t WriteCtx::registryWrite() {
    std::vector<Feed*> feeds;
    std::vector<std::pair<uint32_t, TokDef>> toks;
    {
        std::lock_guard<std::mutex> g(t->mu);
        std::set<uint32_t> fids, tokIds;
        for (RecState* r : order_) {
            if (!r->touched) continue;
            fids.insert(r->fid);
            for (const RowR& x : r->rows) tokIds.insert(x.tok);
        }
        for (uint32_t fid : fids) {
            Feed* f = t->feedById(fid);
            if (f && !f->registered) feeds.push_back(f);
        }
        for (uint32_t id : tokIds) {
            TokDef* d = t->tokById(id);
            if (d && !d->registered) toks.push_back({id, *d});
        }
    }
    if (feeds.empty() && toks.empty()) return P4_OK;
    Conn* x = t->idx;
    if (!x) return P4_E_INTERNAL;
    int rc = x->exec("BEGIN IMMEDIATE");
    for (Feed* f : feeds)
        if (rc == SQLITE_OK) rc = indexPutFeed(x, *f, f->gen);
    for (auto& d : toks)
        if (rc == SQLITE_OK) rc = indexPutTok(x, d.first, d.second);
    if (rc == SQLITE_OK) rc = x->exec("COMMIT");
    if (rc != SQLITE_OK) {
        err = std::string("type index: ") + sqlite3_errmsg(x->db) + " (" + std::to_string(rc) + ")";
        x->exec("ROLLBACK");
        return statusOfSqlite(rc);
    }
    std::lock_guard<std::mutex> g(t->mu);
    for (Feed* f : feeds) {
        f->registered = true;
        f->regGen = f->gen;
    }
    for (auto& d : toks)
        if (TokDef* td = t->tokById(d.first)) td->registered = true;
    return P4_OK;
}

// Every new live row's frame in the file's stream: a frame already there
// whose bytes are the row's (a loaded row of the same record it copies or
// matches, or another new row of this write), else a new frame appended at
// `end`: [u32 LE len][bytes], exactly the bytes the record arrived with.
// *out: the new frames, back to back.
int32_t WriteCtx::frames(Feed* f, const std::vector<RecState*>& recs, std::string* out, int64_t end) {
    const bool verified = (sp->tc.flags() & ps::TypeConfig::kVerifyCid) != 0;
    std::shared_ptr<Stream> stream;  // opened when a framed record's bytes must be compared
    for (RecState* r : recs) {
        struct Fr {
            int64_t off, len;
            bool sealed;
            int row;           // the new row holding the bytes, or -1
            std::string read;  // the bytes read from the stream (row -1)
            bool have;
        };
        std::vector<Fr> framed;
        auto knownAdd = [&](const RowR& y, int row) {
            for (const Fr& k : framed)
                if (k.off == y.off) return;
            framed.push_back(Fr{y.off, y.len, y.sealed, row, std::string(), row >= 0});
        };
        for (const RowR& y : r->rows)
            if (y.loaded && y.off >= 0) knownAdd(y, -1);
        for (size_t i = 0; i < r->rows.size(); i++) {
            RowR& x = r->rows[i];
            if (x.loaded || x.del) continue;
            if (x.off < 0 && !x.hasD && x.srcFid == r->fid)
                for (const RowR& y : r->rows)
                    if (y.loaded && y.rid == x.srcRid) {
                        x.off = y.off;
                        x.len = y.len;
                        break;
                    }
            if (x.off >= 0) {
                knownAdd(x, -1);
                continue;
            }
            if (!x.hasD) {
                // Another file's bytes (a local record's taken into a feed):
                // its frame where this write knows the row (a row of a unit
                // still being applied is not in the index yet), else from
                // that file's index.
                RowR src;
                src.loaded = true;
                src.rid = x.srcRid;
                if (RecState* sr = known(x.srcFid, x.srcRid >> 16))
                    for (const RowR& y : sr->rows)
                        if (y.loaded && y.rid == x.srcRid) {
                            src.off = y.off;
                            src.len = y.len;
                            break;
                        }
                const int32_t st = loadD(x.srcFid, src);
                if (st != P4_OK) return st;
                x.d.swap(src.d);
                x.hasD = true;
            }
            x.len = int64_t(x.d.size());
            bool reuse = false;
            for (Fr& k : framed) {
                if (k.len != x.len) continue;
                bool same;
                if (verified && !k.sealed && !x.sealed) {
                    same = true;  // one CID, verified over both: the same bytes
                } else {
                    if (!k.have) {
                        int32_t st = P4_OK;
                        if (!stream) stream = streamCur(f, &st);
                        if (!stream) return st;
                        st = streamRead(stream.get(), k.off, k.len, &k.read);
                        if (st != P4_OK) {
                            err = "frame at " + std::to_string(k.off) + " in " + stream->path;
                            return st;
                        }
                        k.have = true;
                    }
                    same = (k.row >= 0 ? r->rows[size_t(k.row)].d : k.read) == x.d;
                }
                if (same) {
                    x.off = k.off;
                    reuse = true;
                    break;
                }
            }
            if (reuse) continue;
            x.off = end + int64_t(out->size());
            x.newFrame = true;
            uint8_t p[4];
            st32(p, uint32_t(x.len));
            out->append(reinterpret_cast<const char*>(p), 4);
            out->append(x.d);
            framed.push_back(Fr{x.off, x.len, x.sealed, int(i), std::string(), true});
        }
    }
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

// A node's or batch's id in the feed's dictionary (Feed::dictMu), else in the
// transaction's additions (0: none).
uint32_t dictId(Feed* f, const DictTxn& d, bool node, const std::string& k) {
    {
        std::lock_guard<std::mutex> g(f->dictMu);
        const auto& m = node ? f->dict.nodeId : f->dict.batchId;
        auto it = m.find(k);
        if (it != m.end()) return it->second;
    }
    const auto& m = node ? d.add.nodeId : d.add.batchId;
    auto it = m.find(k);
    return it != m.end() ? it->second : 0;
}

uint32_t internNode(DictTxn& dt, Feed* f, Conn* c, const std::string& producer, const std::string& peer, int* rc) {
    const std::string k = nodeKey(producer, peer);
    if (const uint32_t known = dictId(f, dt, true, k)) return known;
    Dict& d = dt.add;
    const uint32_t id = dt.node0 + uint32_t(d.node.size() + 1);
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

uint32_t internBatch(DictTxn& dt, Feed* f, Conn* c, const RowR& x, int* rc) {
    const std::string k = batchKey(x.batch, x.ppeer, x.pkey);
    if (const uint32_t known = dictId(f, dt, false, k)) return known;
    Dict& d = dt.add;
    const uint32_t id = dt.batch0 + uint32_t(d.batch.size() + 1);
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
uint32_t internText(Conn* c, StmtId get, StmtId ins, const std::string& s, int* rc,
                    FmbCommit* metadata = nullptr, uint8_t kind = 0) {
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
    id = uint32_t(sqlite3_last_insert_rowid(c->db));
    if (metadata) metadata->dictionary.push_back(FmbEntry{kind, id, {s}});
    return id;
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

// ---- a transaction's instances and dictionary additions --------------------------------------------------
InstCount& InstTxn::touch(Type* t, const Feed* f, const InstId& id) {
    auto it = changed.find(id);
    if (it != changed.end()) return it->second;
    InstCount base;
    {
        std::lock_guard<std::mutex> g(t->mu);
        auto b = f->inst.find(id);
        if (b != f->inst.end()) base = b->second;
    }
    return changed.emplace(id, std::move(base)).first->second;
}

void InstTxn::publish(Feed* f) {
    for (auto& kv : changed) {
        if (kv.second.n <= 0) f->inst.erase(kv.first);
        else f->inst[kv.first] = std::move(kv.second);
    }
    changed.clear();
}

void DictTxn::begin(const Dict& d) {
    node0 = uint32_t(d.node.size());
    batch0 = uint32_t(d.batch.size());
    add = Dict();
}

// Ids only grow, densely from 1: an addition a reader already loaded from the
// committed file is there; a gap (the feed's dictionary behind the file) is
// left to the next load.
void DictTxn::publish(Dict& d) {
    for (size_t i = 0; i < add.node.size(); i++) {
        const uint32_t id = node0 + uint32_t(i + 1);
        if (id <= d.node.size()) continue;
        if (id != d.node.size() + 1) {
            d.loaded = false;
            break;
        }
        d.nodeId[nodeKey(add.node[i].producer, add.node[i].peer)] = id;
        d.node.push_back(std::move(add.node[i]));
    }
    for (size_t i = 0; i < add.batch.size(); i++) {
        const uint32_t id = batch0 + uint32_t(i + 1);
        if (id <= d.batch.size()) continue;
        if (id != d.batch.size() + 1) {
            d.loaded = false;
            break;
        }
        d.batchId[batchKey(add.batch[i].batch, add.batch[i].ppeer, add.batch[i].pkey)] = id;
        d.batch.push_back(std::move(add.batch[i]));
    }
    add = Dict();
}

// ---- a feed's index transaction in a commit round -------------------------------------------------------
// Whether this write changes the feed file fid (rows, its moves, identities).
bool WriteCtx::fileChanges(uint32_t fid) const {
    if (movesIn_.count(fid)) return true;
    for (const RecState* r : order_) {
        if (!r->touched || r->fid != fid || !r->seq) continue;
        for (const RowR& x : r->rows)
            if ((!x.loaded && !x.del) || (x.loaded && x.del) || (x.loaded && !x.del && x.urlSet)) return true;
    }
    for (const IdentNew& id : idents)
        if (id.rec && id.rec->fid == fid && id.rec->seq) return true;
    return false;
}

int32_t WriteCtx::fileBegin(FileTxn& x, Feed* f) {
    x.f = f;
    if (migrate) {
        std::lock_guard<std::mutex> g(t->mu);
        if (!f->created) f->indexed = false;
    }
    int32_t status = P4_OK;
    std::string er;
    x.c = writerPin(e, f, &status, &er);
    if (!x.c) {
        err = "writer open: " + er;
        return status;
    }
    if (!dictEnsure(f, x.c)) {
        writerUnpin(e, f);
        x.c = nullptr;
        err = "dictionary of " + f->path;
        return P4_E_IO;
    }
    {
        std::lock_guard<std::mutex> g(f->dictMu);
        x.dict.begin(f->dict);
    }
    // A checkpoint prepared off the writer goes in at this round boundary:
    // this round's sync makes it durable and its commit publishes it.
    checkpointInstallReady(f);
    {
        std::lock_guard<std::mutex> g(t->mu);
        x.k = f->k;
        x.tokc = f->tokc;
        x.indexed = f->indexed;
        x.newEnd = f->mark;
        x.previous = f->pendingCheckpoint ? f->pendingCheckpoint->offset : f->lastCommit;
        if (f->pendingCheckpoint) x.metadataBytes = int64_t(f->pendingCheckpoint->end) - f->mark;
        x.stBase = f->staged;
    }
    x.tokcBefore = x.tokc;
    x.rc = x.c->exec("BEGIN IMMEDIATE");
    x.open = true;
    return x.rc == SQLITE_OK ? P4_OK : statusOfSqlite(x.rc);
}

// This write's rows of the feed file x.f, in x's transaction (after the
// round's earlier writes): rows, the file's, its instances' and its tokens'
// counters, its moves and ingest identities.
int32_t WriteCtx::fileApply(FileTxn& x) {
    Feed* f = x.f;
    Conn* c = x.c;
    std::vector<RecState*> recs;
    for (RecState* r : order_)
        if (r->touched && r->fid == f->fid && r->seq) recs.push_back(r);
    // A new row is stored under its record's CID: a record still without one
    // is a bug, refused before anything is written (C-39 B1).
    for (RecState* r : recs) {
        bool adds = false;
        for (const RowR& y : r->rows) adds = adds || (!y.loaded && !y.del);
        if (adds && !r->keyed) {
            err = "a new row of seq " + std::to_string(r->seq) + " has no CID: " + f->path;
            return P4_E_INTERNAL;
        }
    }
    // The indexer alone owns stream appends. Planning keeps bytes for rows
    // whose predecessor has not yet assigned a frame; resolve those stable
    // rids now that preceding units have applied to this transaction.
    for (RecState* r : recs)
        for (RowR& y : r->rows) {
            if (!y.loaded || y.off >= 0) continue;
            sqlite3_stmt* q = c->get(S_R_FRAME);
            if (!q) return P4_E_INTERNAL;
            sqlite3_bind_int64(q, 1, y.rid);
            const int rc = sqlite3_step(q);
            if (rc == SQLITE_ROW) {
                y.off = sqlite3_column_int64(q, 0);
                y.len = sqlite3_column_int64(q, 1);
            }
            sqlite3_reset(q);
            if (rc != SQLITE_ROW) return rc == SQLITE_DONE ? P4_E_CORRUPT : statusOfSqlite(rc);
        }
    int32_t frameStatus = P4_OK;
    std::shared_ptr<Stream> stream = streamCur(f, &frameStatus);
    if (!stream) return frameStatus;
    const int64_t oldEnd = f->end;
    if (x.recordStart < 0) x.recordStart = oldEnd;
    newEnd_[f->fid] = oldEnd;
    x.previous = f->pendingCheckpoint ? f->pendingCheckpoint->offset : f->lastCommit;
    std::string framed;
    frameStatus = frames(f, recs, &framed, oldEnd);
    if (frameStatus == P4_OK && !framed.empty())
        frameStatus = streamWrite(stream.get(), oldEnd, reinterpret_cast<const uint8_t*>(framed.data()), framed.size());
    if (frameStatus != P4_OK) return frameStatus;
    f->end = oldEnd + int64_t(framed.size());
    newEnd_[f->fid] = f->end;
    x.newEnd = f->end;
    x.metadata.committedAt = now;
    {
        std::lock_guard<std::mutex> g(t->mu);
        x.metadata.typeSeqHigh = uint64_t(t->nextSeq - 1);
    }
    int& rc = x.rc;
    Counters& k = x.k;
    auto bad = [&](int r) {
        if (rc == SQLITE_OK && r != SQLITE_OK && r != SQLITE_DONE && r != SQLITE_ROW) rc = r;
    };
    auto mv = movesIn_.find(f->fid);
    // Rows an earlier write of the round or a previous unit added (this
    // write's seed) carry no file ids yet: their instance's ids are looked up.
    for (RecState* r : recs)
        for (RowR& y : r->rows) {
            if (rc != SQLITE_OK) break;
            if (!y.loaded || !y.inst || y.bId) continue;
            y.bId = internBatch(x.dict, f, c, y, &rc);
            y.cId = internText(c, S_CKEY_GET, S_CKEY_INS, y.ckey, &rc, &x.metadata, 3);
        }
    std::vector<RecState*> sorted = recs;
    std::sort(sorted.begin(), sorted.end(), [](RecState* a, RecState* b) { return a->seq < b->seq; });
    std::vector<int64_t> gone;  // this write's records that leave the file
    for (RecState* r : sorted) {
        if (rc != SQLITE_OK) break;
        const int64_t base = r->seq << 16;
        for (RowR& y : r->rows) {
            if (rc != SQLITE_OK) break;
            if (y.loaded && y.del) {
                auto& pending = x.metadata.rows;
                pending.erase(std::remove_if(pending.begin(), pending.end(), [&](const FmbRow& row) {
                    return row.seq == uint64_t(r->seq) && row.rowJ == uint16_t(y.rid & 0xffff);
                }), pending.end());
                auto& urls = x.metadata.urls;
                urls.erase(std::remove_if(urls.begin(), urls.end(), [&](const FmbUrl& url) {
                    return url.seq == uint64_t(r->seq) && url.rowJ == uint32_t(y.rid & 0xffff);
                }), urls.end());
                x.metadata.removals.push_back(FmbRemove{uint64_t(r->seq), uint32_t(y.rid & 0xffff), y.removeReason});
                sqlite3_stmt* q = c->get(S_R_DEL);
                sqlite3_bind_int64(q, 1, y.rid);
                bad(sqlite3_step(q));
                sqlite3_reset(q);
                x.deleted = true;
                // A staged row leaves the view (one this round staged, or the base's).
                if (!x.stAdd.erase(y.rid)) x.stDrop.push_back(y.rid);
            } else if (y.loaded && y.urlSet) {
                y.uId = internText(c, S_URL_GET, S_URL_INS, y.url, &rc, &x.metadata, 4);
                x.metadata.urls.push_back(FmbUrl{uint64_t(r->seq), uint32_t(y.rid & 0xffff), y.uId});
                sqlite3_stmt* q = c->get(S_R_URL);
                sqlite3_bind_int64(q, 1, y.rid);
                sqlite3_bind_int64(q, 2, y.uId);
                bad(sqlite3_step(q));
                sqlite3_reset(q);
            } else if (!y.loaded && !y.del) {
                // prepare() gave the row its rid (after the record's live rows)
                if (y.rid < base || y.rid > (base | 0xffff)) {
                    rc = SQLITE_FULL;  // 65,536 rows of one record in one feed
                    break;
                }
                std::string producer;
                {
                    std::lock_guard<std::mutex> g(t->mu);
                    TokDef* d = t->tokById(y.tok);
                    producer = d ? d->token : std::string();
                }
                y.nId = internNode(x.dict, f, c, producer, y.peer, &rc);
                y.bId = y.inst ? internBatch(x.dict, f, c, y, &rc) : 0;
                y.cId = y.inst ? internText(c, S_CKEY_GET, S_CKEY_INS, y.ckey, &rc, &x.metadata, 3) : 0;
                y.uId = y.inst ? internText(c, S_URL_GET, S_URL_INS, y.url, &rc, &x.metadata, 4) : 0;
                if (rc != SQLITE_OK) break;
                if (y.off < 0) {
                    rc = SQLITE_INTERNAL;  // frames() gives every new row its frame
                    break;
                }
                if (r->k.type != 0) {  // the object key's id (okey), once per key
                    sqlite3_stmt* ok = c->get(S_OKEY_INS);
                    r->k.bind(ok, 1);
                    bad(sqlite3_step(ok));
                    sqlite3_reset(ok);
                }
                // A row of an indexed file is staged (m=0); a migration's
                // append (no secondary indexes until REBUILD 1) is not.
                sqlite3_stmt* q = c->get(x.indexed ? S_R_STAGE : S_R_INS);
                sqlite3_bind_int64(q, 1, y.rid);
                sqlite3_bind_int64(q, 2, r->seq);
                sqlite3_bind_int64(q, 3, y.nId);
                sqlite3_bind_int64(q, 4, y.bId);
                sqlite3_bind_int64(q, 5, y.cId);
                sqlite3_bind_int64(q, 6, y.uId);
                sqlite3_bind_int64(q, 7, y.at);
                sqlite3_bind_blob(q, 8, r->key, 32, SQLITE_STATIC);
                if (r->hasE) sqlite3_bind_int64(q, 9, r->e);
                else sqlite3_bind_null(q, 9);
                r->k.bind(q, 10);
                sqlite3_bind_int64(q, 11, y.ts);
                if (y.sealed) sqlite3_bind_blob(q, 12, y.fcols.data(), int(y.fcols.size()), SQLITE_STATIC);
                else sqlite3_bind_null(q, 12);
                if (!y.sig.empty()) sqlite3_bind_blob(q, 13, y.sig.data(), int(y.sig.size()), SQLITE_STATIC);
                else sqlite3_bind_null(q, 13);
                sqlite3_bind_int64(q, 14, y.off);
                sqlite3_bind_int64(q, 15, y.len);
                bad(sqlite3_step(q));
                sqlite3_reset(q);
                if (x.indexed) {
                    StRow& sr = x.stAdd[y.rid];
                    sr.rid = y.rid;
                    sr.hasE = r->hasE;
                    sr.e = r->e;
                    sr.ts = y.ts;
                    sr.at = y.at;
                    sr.w = r->hasE ? r->e : y.ts;
                    sr.k = r->k;
                    std::memcpy(sr.cid, r->key, 32);
                }
            }
        }
        if (rc != SQLITE_OK) break;
        // The file's, its instances' and its tokens' counters: this record's
        // rows before (as loaded) and after (live).
        int64_t nB = 0, nA = 0, bytesB = 0, bytesA = 0;
        std::map<InstId, InstAgg> agg;
        for (const RowR& y : r->rows) {
            if (y.loaded) {
                nB++;
                bytesB += y.len;
                if (y.bId) {
                    InstAgg& a = agg[InstId(y.bId, y.cId)];
                    a.b = true;
                    a.lenB = std::min(a.lenB, y.len);
                    a.any = &y;
                }
            }
            if (!y.del) {
                nA++;
                bytesA += y.len;
                if (y.bId) {
                    InstAgg& a = agg[InstId(y.bId, y.cId)];
                    a.a = true;
                    a.lenA = std::min(a.lenA, y.len);
                    a.maxat = std::max(a.maxat, y.at);
                    a.minat = std::min(a.minat, y.at);
                    if ((!y.loaded && y.stamp) || y.urlSet) {
                        a.changed = true;
                        a.url = y.url;
                    }
                    a.any = &y;
                }
            }
        }
        const std::map<uint32_t, int64_t> cpB = copiesOf(r, false), cpA = copiesOf(r, true);
        // The live rows' ts and w (a copy may keep its own ts, C-39 E6), and
        // each copy's.
        int64_t tsLo = INT64_MAX, tsHi = INT64_MIN, wLo = INT64_MAX, wHi = INT64_MIN;
        std::map<uint32_t, std::pair<int64_t, int64_t>> tokTs;
        for (const RowR& y : r->rows) {
            if (y.del) continue;
            const int64_t xw = r->hasE ? r->e : y.ts;
            tsLo = std::min(tsLo, y.ts);
            tsHi = std::max(tsHi, y.ts);
            wLo = std::min(wLo, xw);
            wHi = std::max(wHi, xw);
            auto it = tokTs.find(y.tok);
            if (it == tokTs.end()) tokTs[y.tok] = {y.ts, y.ts};
            else it->second = {std::min(it->second.first, y.ts), std::max(it->second.second, y.ts)};
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
        // The stream's live frames: the record's distinct frames before
        // (its loaded rows) and after (its live rows).
        {
            std::map<int64_t, int64_t> frB, frA;
            for (const RowR& y : r->rows) {
                if (y.loaded && y.off >= 0) frB[y.off] = y.len + 4;
                if (!y.del && y.off >= 0) frA[y.off] = y.len + 4;
            }
            for (auto& kv : frA) k.fbytes += kv.second;
            for (auto& kv : frB) k.fbytes -= kv.second;
        }
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
            if (r->hasE && (r->e <= k.mine || r->e >= k.maxe)) x.epochEdge = true;
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
            TokCount& d = x.tokc[tok];
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
            InstCount& ic = x.inst.touch(t, f, kv.first);
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
    // Ingest identities of this file's records that hold rows after the write.
    for (const IdentNew& id : idents) {
        if (rc != SQLITE_OK) break;
        if (!id.rec || id.rec->fid != f->fid || !id.rec->seq) continue;
        bool live = false;
        for (const RowR& rr : id.rec->rows) live = live || rr.live();
        if (!live) continue;
        // Staged (idst, appended; the merge moves it into ident) in an indexed file.
        sqlite3_stmt* q = c->get(x.indexed ? S_IDST_INS : S_IDENT_INS);
        if (!q) {
            rc = SQLITE_ERROR;
            break;
        }
        sqlite3_bind_blob(q, 1, id.h, 32, SQLITE_STATIC);
        sqlite3_bind_int64(q, 2, id.rec->seq);
        bad(sqlite3_step(q));
        sqlite3_reset(q);
        if (x.indexed) {
            IdentKey h;
            std::memcpy(h.data(), id.h, 32);
            x.idAdd.push_back({h, id.rec->seq});
        }
    }
    // The full-text rows of records that left the file go after the commit;
    // a seq that moved to another file in this write (a record's local rows
    // taken into a feed) keeps its row.
    if (!gone.empty() && sp->fullText)
        for (int64_t seq : gone) {
            bool elsewhere = false;
            for (RecState* o : order_) {
                if (o->seq != seq || o->fid == f->fid) continue;
                for (const RowR& y : o->rows) elsewhere = elsewhere || y.live();
            }
            if (!elsewhere) x.ftsDrop.push_back(seq);
        }
    if (rc != SQLITE_OK) {
        err = std::string(sqlite3_errmsg(c->db)) + " (" + std::to_string(rc) + ") " + f->path;
        return statusOfSqlite(rc);
    }
    // Persist the semantic rows, in input plan order. The codec owns the SDS
    // byte layout; this layer owns the ids and the row semantics.
    for (RecState* r : recs) {
        for (RowR& y : r->rows) {
            if (y.loaded || y.del) continue;
            const auto groupKey = std::make_tuple(y.deliveryGroup, y.at, y.nId, y.bId, y.cId, y.uId);
            auto gi = x.groups.find(groupKey);
            if (gi == x.groups.end()) {
                const uint32_t id = uint32_t(x.metadata.groups.size());
                x.metadata.groups.push_back(FmbGroup{y.at, y.nId, y.bId, y.cId, y.uId});
                gi = x.groups.emplace(groupKey, id).first;
            }
            FmbRow row;
            row.offset = uint64_t(y.off);
            row.seq = uint64_t(r->seq);
            row.sourceTs = y.ts;
            row.length = uint32_t(y.len);
            row.group = gi->second;
            row.rowJ = uint16_t(y.rid & 0xffff);
            row.flags = y.commitFlags | (y.newFrame ? FmbNewFrame : 0) | (y.sealed ? FmbSealed : 0) |
                        (y.len > 1048576 ? FmbLarge : 0);
            if (mv != movesIn_.end() && std::find(mv->second.begin(), mv->second.end(), r->seq) != mv->second.end())
                row.flags |= FmbMovedIn;
            cidDigestFromKey(r->key, row.digest.data());
            row.signature = y.sig;
            if (y.sealed) {
                row.columns = y.fcols;
                row.epoch = r->hasE ? r->e : INT64_MIN;
                if (r->k.type) {
                    sqlite3_stmt* q = c->sql("SELECT id FROM okey WHERE k=?1");
                    if (!q) return P4_E_INTERNAL;
                    r->k.bind(q, 1);
                    const int qr = sqlite3_step(q);
                    if (qr == SQLITE_ROW) row.objectKey = uint32_t(sqlite3_column_int64(q, 0));
                    sqlite3_reset(q);
                    if (qr != SQLITE_ROW) return P4_E_CORRUPT;
                    if (x.objectKeys.insert(row.objectKey).second) {
                        std::lock_guard<std::mutex> g(t->mu);
                        if (!f->streamObjects.count(row.objectKey)) {
                            const std::string text = (r->k.type == 1 ? "i" : "s") + r->k.text();
                            x.metadata.dictionary.push_back(FmbEntry{5, row.objectKey, {text}});
                        }
                    }
                }
                if (!y.hasD) {
                    const int32_t st = streamRead(stream.get(), y.off, y.len, &y.d);
                    if (st != P4_OK) return st;
                    y.hasD = true;
                }
                ps::sha256(reinterpret_cast<const uint8_t*>(y.d.data()), y.d.size(), row.leafDigest.data());
            }
            for (const IdentNew& identity : idents)
                if (identity.rec == r) row.identity.assign(reinterpret_cast<const char*>(identity.h), 32);
            x.metadata.rows.push_back(std::move(row));
            if (x.tokens.insert(y.tok).second) {
                std::lock_guard<std::mutex> g(t->mu);
                if (!f->streamTokens.count(y.tok))
                    if (const TokDef* td = t->tokById(y.tok))
                        x.metadata.dictionary.push_back(FmbEntry{0, y.tok, {td->token, td->peer}});
            }
        }
    }
    return P4_OK;
}

namespace {
// Large rounds must interleave their record chunks and metadata: a later
// FMB cannot claim NEW_FRAME bytes before the preceding commit point. The
// same rewrite puts removal-only metadata before a round's replacement rows.
// Only the unsynced tail is rewritten; acknowledged ranges never move.
int32_t appendChunkedMetadata(FileTxn& x, Stream* stream) {
    FmbCommit& metadata = x.metadata;
    std::map<uint64_t, std::string> frames;
    for (const FmbRow& row : metadata.rows) {
        if (row.offset < uint64_t(x.recordStart) || frames.count(row.offset)) continue;
        std::string bytes;
        const int32_t st = streamRead(stream, int64_t(row.offset), row.length, &bytes, true);
        if (st != P4_OK) return st;
        frames.emplace(row.offset, std::move(bytes));
    }
    x.newEnd = x.recordStart;
    bool dictionaryWritten = false;
    if (!metadata.removals.empty()) {
        FmbCommit removals;
        removals.committedAt = metadata.committedAt;
        removals.typeSeqHigh = metadata.typeSeqHigh;
        removals.dictionary = metadata.dictionary;
        removals.removals = metadata.removals;
        const int64_t before = x.newEnd;
        const int32_t st = streamAppendCommit(x.f, stream, removals, &x.newEnd, &x.previous);
        if (st != P4_OK) return st;
        x.metadataBytes += x.newEnd - before;
        dictionaryWritten = true;
    }
    std::map<uint64_t, uint64_t> offsets;
    for (size_t at = 0; at < metadata.rows.size(); at += 65536) {
        FmbCommit part;
        part.committedAt = metadata.committedAt;
        part.typeSeqHigh = metadata.typeSeqHigh;
        if (!dictionaryWritten) {
            part.dictionary = metadata.dictionary;
            dictionaryWritten = true;
        }
        const size_t count = std::min<size_t>(65536, metadata.rows.size() - at);
        std::map<uint32_t, uint32_t> groups;
        for (size_t j = 0; j < count; ++j) {
            FmbRow row = metadata.rows[at + j];
            const uint64_t oldOffset = row.offset;
            row.flags &= ~FmbNewFrame;
            if (oldOffset >= uint64_t(x.recordStart)) {
                auto oi = offsets.find(oldOffset);
                if (oi == offsets.end()) {
                    oi = offsets.emplace(oldOffset, uint64_t(x.newEnd)).first;
                    const std::string& bytes = frames.at(oldOffset);
                    const int32_t st = streamWrite(stream, x.newEnd, reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
                    if (st != P4_OK) return st;
                    x.newEnd += int64_t(bytes.size());
                    row.flags |= FmbNewFrame;
                }
                row.offset = oi->second;
            }
            auto gi = groups.find(row.group);
            if (gi == groups.end()) {
                gi = groups.emplace(row.group, uint32_t(part.groups.size())).first;
                part.groups.push_back(metadata.groups.at(row.group));
            }
            row.group = gi->second;
            // Rids stay fixed, including in pending units' immutable plans.
            sqlite3_stmt* q = x.c->sql("UPDATE r SET off=?1 WHERE rid=?2");
            if (!q) return P4_E_INTERNAL;
            sqlite3_bind_int64(q, 1, int64_t(row.offset));
            sqlite3_bind_int64(q, 2, int64_t(row.seq << 16) | row.rowJ);
            const int rc = sqlite3_step(q);
            sqlite3_reset(q);
            if (rc != SQLITE_DONE) return statusOfSqlite(rc);
            part.rows.push_back(std::move(row));
        }
        if (at + count == metadata.rows.size()) part.urls = metadata.urls;
        const int64_t before = x.newEnd;
        const int32_t st = streamAppendCommit(x.f, stream, part, &x.newEnd, &x.previous);
        if (st != P4_OK) return st;
        x.metadataBytes += x.newEnd - before;
    }
    return streamTruncate(stream, x.newEnd, false);
}

int32_t appendMetadata(FileTxn& x, std::string* err) {
    if (x.metadataAppended) return P4_OK;
    for (size_t i = 0; i < x.dict.add.node.size(); ++i) {
        const NodeDef& d = x.dict.add.node[i];
        x.metadata.dictionary.push_back(FmbEntry{1, x.dict.node0 + uint32_t(i + 1), {d.producer, d.peer}});
    }
    for (size_t i = 0; i < x.dict.add.batch.size(); ++i) {
        const BatchDef& d = x.dict.add.batch[i];
        x.metadata.dictionary.push_back(FmbEntry{2, x.dict.batch0 + uint32_t(i + 1), {d.batch, d.ppeer, d.pkey}});
    }
    int32_t st = P4_OK;
    std::shared_ptr<Stream> stream = streamCur(x.f, &st);
    if (stream) {
        if (x.metadata.rows.size() > 65536 || (!x.metadata.removals.empty() && !x.metadata.rows.empty()))
            st = appendChunkedMetadata(x, stream.get());
        else {
            const int64_t before = x.newEnd;
            st = streamAppendCommit(x.f, stream.get(), x.metadata, &x.newEnd, &x.previous);
            if (st == P4_OK) x.metadataBytes += x.newEnd - before;
        }
    }
    if (st != P4_OK) {
        *err = "metadata append " + streamPath(x.f, x.f->gen);
        return st;
    }
    x.f->end = x.newEnd;
    x.metadataAppended = true;
    return P4_OK;
}

int32_t syncTransaction(Engine* e, FileTxn& x, std::string* err) {
    if (x.streamSynced) return P4_OK;
    int32_t st = appendMetadata(x, err);
    if (st != P4_OK) return st;
    std::shared_ptr<Stream> stream = streamCur(x.f, &st);
    if (stream) st = streamSync(stream.get());
    if (st != P4_OK) {
        *err = "stream sync " + streamPath(x.f, x.f->gen);
        return st;
    }
    e->bump(kStJournalSyncs);
    x.streamSynced = true;
    return P4_OK;
}
}  // namespace

// The round's transaction of x.f: the stream synced first (the frames the
// round's writes appended), then the instances, tokens, bounds and meta
// (with the stream's new mark), then COMMIT; the counters published.
int32_t fileCommit(Engine* e, Type* t, FileTxn& x, std::string* err) {
    Feed* f = x.f;
    Conn* c = x.c;
    int& rc = x.rc;
    auto bad = [&](int r) {
        if (rc == SQLITE_OK && r != SQLITE_OK && r != SQLITE_DONE && r != SQLITE_ROW) rc = r;
    };
    if (rc == SQLITE_OK) {
        const int32_t st = syncTransaction(e, x, err);
        if (st != P4_OK) {
            c->exec("ROLLBACK");
            writerUnpin(e, f);
            x.open = false;
            return st;
        }
    }
    Counters& k = x.k;
    for (const auto& kv : x.inst.changed) {
        if (rc != SQLITE_OK) break;
        const InstId& id = kv.first;
        const InstCount& ic = kv.second;
        if (ic.n <= 0) {
            sqlite3_stmt* q = c->get(S_INST_DEL);
            sqlite3_bind_int64(q, 1, id.first);
            sqlite3_bind_int64(q, 2, id.second);
            bad(sqlite3_step(q));
            sqlite3_reset(q);
            continue;
        }
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
    for (auto it = x.tokc.begin(); it != x.tokc.end();) {
        if (it->second.n <= 0) it = x.tokc.erase(it);
        else ++it;
    }
    if (rc == SQLITE_OK) rc = writeTokc(t, c, x.tokcBefore, x.tokc);
    // The feed's staged rows after this transaction.
    const bool stChanged = !x.stAdd.empty() || !x.stDrop.empty() || !x.idAdd.empty();
    if (stChanged) {
        std::vector<StRow> add;
        add.reserve(x.stAdd.size());
        for (auto& kv : x.stAdd) add.push_back(std::move(kv.second));
        x.stNew = Staged::make(x.stBase.get(), std::move(add), x.stDrop, x.idAdd, false);
    } else {
        x.stNew = x.stBase;
    }
    if (rc == SQLITE_OK && x.deleted) {
        // The bounds again (the ends of the rowid and w indexes: O(log n)); a
        // file without the indexes (a migration before REBUILD 1) walks for w.
        // A row's rid is its seq << 16 plus its index in the record, so the
        // first and last rows by rid hold the least and greatest seq (one
        // SELECT of both min() and max() would scan every row).
        if (k.rows <= 0) {
            const int64_t keep = k.maxseq;
            k = Counters();
            k.maxseq = keep;  // seqs never go back
        } else {
            sqlite3_stmt* s = c->sql("SELECT seq FROM r ORDER BY rid LIMIT 1");
            if (s && sqlite3_step(s) == SQLITE_ROW) k.minseq = sqlite3_column_int64(s, 0);
            if (s) sqlite3_reset(s);
            s = c->sql("SELECT seq FROM r ORDER BY rid DESC LIMIT 1");
            if (s && sqlite3_step(s) == SQLITE_ROW) k.maxseq = std::max<int64_t>(k.maxseq, sqlite3_column_int64(s, 0));
            if (s) sqlite3_reset(s);
            // r_w holds the merged rows, the view the staged ones.
            k.minw = INT64_MAX;
            k.maxw = INT64_MIN;
            s = c->sql(x.indexed ? "SELECT min(w) FROM r INDEXED BY r_w WHERE m=1" : "SELECT min(w) FROM r");
            if (s && sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) k.minw = sqlite3_column_int64(s, 0);
            if (s) sqlite3_reset(s);
            s = c->sql(x.indexed ? "SELECT max(w) FROM r INDEXED BY r_w WHERE m=1" : "SELECT max(w) FROM r");
            if (s && sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) k.maxw = sqlite3_column_int64(s, 0);
            if (s) sqlite3_reset(s);
            std::vector<const StRow*> staged;
            if (x.indexed && x.stNew) x.stNew->all(&staged);
            for (const StRow* y : staged) {
                k.minw = std::min(k.minw, y->w);
                k.maxw = std::max(k.maxw, y->w);
            }
            // The epoch range follows a delete at its edge (the type's
            // epoch range, SchemaDateRanges): among rows with an epoch w is
            // the epoch, so each end of r_w to its first such row.
            if (x.epochEdge) {
                k.mine = INT64_MAX;
                k.maxe = INT64_MIN;
                s = c->sql(x.indexed ? "SELECT e FROM r INDEXED BY r_w WHERE m=1 AND e IS NOT NULL ORDER BY w ASC LIMIT 1"
                                     : "SELECT min(e) FROM r");
                if (s && sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) k.mine = sqlite3_column_int64(s, 0);
                if (s) sqlite3_reset(s);
                s = c->sql(x.indexed ? "SELECT e FROM r INDEXED BY r_w WHERE m=1 AND e IS NOT NULL ORDER BY w DESC LIMIT 1"
                                     : "SELECT max(e) FROM r");
                if (s && sqlite3_step(s) == SQLITE_ROW && sqlite3_column_type(s, 0) != SQLITE_NULL) k.maxe = sqlite3_column_int64(s, 0);
                if (s) sqlite3_reset(s);
                for (const StRow* y : staged)
                    if (y->hasE) {
                        k.mine = std::min(k.mine, y->e);
                        k.maxe = std::max(k.maxe, y->e);
                    }
            }
        }
    }
    if (rc == SQLITE_OK) rc = writeMeta(c, k, nowSec(), x.newEnd, f->gen);
    if (rc == SQLITE_OK) {
        sqlite3_stmt* q = c->get(S_META_SET);
        if (!q) rc = SQLITE_ERROR;
        else {
            sqlite3_bind_text(q, 1, "commit", -1, SQLITE_STATIC);
            sqlite3_bind_int64(q, 2, int64_t(x.previous));
            const int qr = sqlite3_step(q);
            sqlite3_reset(q);
            if (qr != SQLITE_DONE) rc = qr;
        }
    }
    std::set<uint32_t> cacheTokens, cacheObjects;
    std::set<int64_t> cacheMoves;
    uint64_t cacheHigh = 0;
    {
        std::lock_guard<std::mutex> lock(t->mu);
        cacheTokens = f->streamTokens; cacheObjects = f->streamObjects; cacheMoves = f->streamMoves;
        cacheHigh = std::max(f->streamHigh, x.metadata.typeSeqHigh);
    }
    cacheTokens.insert(x.tokens.begin(), x.tokens.end());
    cacheObjects.insert(x.objectKeys.begin(), x.objectKeys.end());
    for (const auto& row : x.metadata.rows) if (row.flags & FmbMovedIn) cacheMoves.insert(int64_t(row.seq));
    if (rc == SQLITE_OK)
        rc = writeStreamCache(c, f, x.newEnd, x.previous, f->metadataBytes + x.metadataBytes,
                              cacheHigh, cacheTokens, cacheObjects, cacheMoves);
    if (rc == SQLITE_OK) rc = c->exec("COMMIT");
    x.open = false;
    if (rc != SQLITE_OK) {
        *err = std::string(sqlite3_errmsg(c->db)) + " (" + std::to_string(rc) + ") " + f->path;
        c->exec("ROLLBACK");
        writerUnpin(e, f);
        // The frames past the committed mark were never acknowledged: the
        // writer cuts them (resetStreams).
        if ((rc & 0xff) == SQLITE_CORRUPT || (rc & 0xff) == SQLITE_NOTADB) {
            std::lock_guard<std::mutex> g(t->mu);
            f->quarantined = true;
            stagedPublish(e, f, nullptr, false);  // never read or merged: its view goes
        }
        return statusOfSqlite(rc);
    }
    int64_t freeBytes = 0;
    const int64_t dbBytes = dbBytesOf(c, &freeBytes);
    writerUnpin(e, f);
    e->bump(kStGroupCommits);
    {
        std::lock_guard<std::mutex> g(f->dictMu);
        x.dict.publish(f->dict);
    }
    if (!x.ftsDrop.empty()) {
        std::lock_guard<std::mutex> g(t->ftsGoneMu);
        t->ftsGone.insert(t->ftsGone.end(), x.ftsDrop.begin(), x.ftsDrop.end());
    }
    std::lock_guard<std::mutex> g(t->mu);
    if (dbBytes >= 0) {
        f->dbBytes = dbBytes;
        f->freeBytes = freeBytes;
    }
    if (f->pendingCheckpoint) { f->checkpoint = std::move(f->pendingCheckpoint); f->checkpointRows = 0; }
    f->checkpointRows += x.metadata.rows.size();
    f->checkpointAt = monoNs();
    f->mark = x.newEnd;
    f->lastCommit = x.previous;
    f->streamHigh = cacheHigh;
    f->metadataBytes += x.metadataBytes;
    f->streamTokens = std::move(cacheTokens);
    f->streamObjects = std::move(cacheObjects);
    f->streamMoves = std::move(cacheMoves);
    f->streamBytes = x.newEnd;
    if (stChanged) stagedPublish(e, f, x.stNew, !x.stAdd.empty() || !x.idAdd.empty());
    f->k = k;
    x.inst.publish(f);
    f->tokc = std::move(x.tokc);
    f->created = true;
    return P4_OK;
}

// A commit round: every feed file the writes touch in ONE transaction each
// (the writes' rows in plan order), source feeds before local files, each
// stream synced before its transaction commits (the writes registered their
// new feeds and tokens while planned). A failure fails the round; the files
// committed before it stay (as a write cut between two files).
int32_t applyRound(Engine* e, const std::vector<WriteCtx*>& units, std::string* err) {
    std::map<Feed*, FileTxn> txns;
    std::vector<Feed*> feeds, locals;
    int32_t first = P4_OK;
    const int64_t roundNow = nowSec();
    for (WriteCtx* w : units) {
        if (first != P4_OK) break;
        // FMB carries one clock for the complete round. Use it for every
        // instance update and removal restamp in the derived index too.
        w->now = roundNow;
        for (uint32_t fid : w->files()) {
            if (!w->fileChanges(fid)) continue;
            Feed* f;
            {
                std::lock_guard<std::mutex> g(w->t->mu);
                f = w->t->feedById(fid);
            }
            if (!f) continue;
            FileTxn& x = txns[f];
            if (!x.open && !x.f) {
                (f->local ? locals : feeds).push_back(f);
                first = w->fileBegin(x, f);
                if (first != P4_OK) {
                    *err = w->err;
                    break;
                }
            }
            first = w->fileApply(x);
            if (first != P4_OK) {
                *err = w->err;
                break;
            }
        }
    }
    std::set<Feed*> committed;
    // Different streams sync concurrently. A move's destination streams
    // are durable before the local removal is even appended.
    auto syncFeeds = [&](const std::vector<Feed*>& fs) {
        if (first != P4_OK) return;
        if (fs.size() == 1) {
            first = syncTransaction(e, txns[fs.front()], err);
            return;
        }
        std::vector<std::future<std::pair<int32_t, std::string>>> waits;
        for (Feed* f : fs)
            waits.push_back(std::async(std::launch::async, [&, f] {
                std::string why;
                const int32_t st = syncTransaction(e, txns.at(f), &why);
                return std::make_pair(st, std::move(why));
            }));
        for (auto& wait : waits) {
            auto result = wait.get();
            if (first == P4_OK && result.first != P4_OK) {
                first = result.first;
                *err = std::move(result.second);
            }
        }
    };
    syncFeeds(feeds);
    syncFeeds(locals);
    feeds.insert(feeds.end(), locals.begin(), locals.end());
    for (Feed* f : feeds) {
        FileTxn& x = txns[f];
        if (first != P4_OK) {
            if (x.open) {
                x.c->exec("ROLLBACK");
                writerUnpin(e, f);
                x.open = false;
            }
            continue;
        }
        first = fileCommit(e, f->type, x, err);
        if (first == P4_OK) committed.insert(f);
    }
    return first;
}

// ---- prepare (the writer) ------------------------------------------------------------------------------
// What this write commits, decided on the writer: the records it touches,
// each new row's rid (after the record's live rows) and frame, the frames
// appended to each feed's stream (not yet synced), the moves; then the state
// its records are in after it (the next unit's seed).
int32_t WriteCtx::prepare() {
    any_ = false;
    for (RecState* r : order_) {
        if (!r->seq) r->touched = false;  // a new record this write refused: never stored
        any_ = any_ || r->touched;
    }
    for (const IdentNew& id : idents)
        if (id.rec && id.rec->seq) {
            id.rec->touched = true;
            any_ = true;
        }
    if (!any_) {
        snapshot();
        return P4_OK;
    }
    // Feed files, then local files.
    std::set<uint32_t> fids;
    for (RecState* r : order_)
        if (r->touched) fids.insert(r->fid);
    std::vector<uint32_t> locals;
    fileOrder_.clear();
    {
        std::lock_guard<std::mutex> g(t->mu);
        for (uint32_t fid : fids) {
            Feed* f = t->feedById(fid);
            if (!f) continue;
            if (f->local) locals.push_back(fid);
            else fileOrder_.push_back(fid);
        }
    }
    locals_ = locals;
    fileOrder_.insert(fileOrder_.end(), locals.begin(), locals.end());
    // Moves: a local record whose rows leave local while its seq gains rows
    // in a feed file of this write; the feed's transaction records the seq.
    movesIn_.clear();
    for (RecState* r : order_) {
        if (!r->touched || !r->existed || !r->seq) continue;
        if (std::find(locals.begin(), locals.end(), r->fid) == locals.end()) continue;
        bool live = false;
        for (const RowR& x : r->rows) live = live || x.live();
        if (live) continue;
        for (RecState* o : order_) {
            if (o == r || o->seq != r->seq || o->fid == r->fid) continue;
            bool gains = false;
            for (const RowR& x : o->rows) gains = gains || (x.live() && !x.loaded);
            if (gains) movesIn_[o->fid].push_back(r->seq);
        }
    }
    // Rids: a record's new rows after its live rows (deleted rows go first in
    // the same transaction).
    for (RecState* r : order_) {
        if (!r->touched || !r->seq) continue;
        const int64_t base = r->seq << 16;
        int64_t next = base;
        for (const RowR& x : r->rows)
            if (x.loaded && !x.del) next = std::max(next, x.rid + 1);
        for (RowR& x : r->rows)
            if (!x.loaded && !x.del) x.rid = next++;
    }
    // Planning never writes the stream: the indexer appends a complete round
    // and its metadata as one contiguous commit unit.
    newEnd_.clear();
    if (const int32_t rc = registryWrite()) return rc;
    snapshot();
    return P4_OK;
}

// The state this write leaves its records in, for the next unit of the type
// while this one is applied: every touched record with its live rows (new
// rows as rows of the file with their rid and frame; their file ids are
// looked up by the unit that needs them), and the ingest identities it adds.
void WriteCtx::snapshot() {
    post_.clear();
    postSeq_.clear();
    postKey_.clear();
    postK_.clear();
    postIdent_.clear();
    postFids_.clear();
    std::map<const RecState*, size_t> at;
    for (RecState* r : order_) {
        if (!r->touched || !r->seq) continue;
        RecState s;
        s.fid = r->fid;
        std::memcpy(s.key, r->key, 32);
        s.seq = r->seq;
        s.keyed = r->keyed;
        s.hasE = r->hasE;
        s.e = r->e;
        s.ts = r->ts;
        s.w = r->w;
        s.k = r->k;
        for (const RowR& x : r->rows) {
            if (x.del) continue;
            RowR y = x;
            if (!x.loaded) y.nId = y.bId = y.cId = y.uId = 0;
            y.loaded = true;
            if (y.off >= 0) {
                y.hasD = false;
                y.d.clear();
                y.srcFid = 0;
                y.srcRid = 0;
            }
            y.newFrame = false;
            y.urlSet = false;
            y.stamp = false;
            s.rows.push_back(std::move(y));
        }
        s.existed = !s.rows.empty();
        at[r] = post_.size();
        post_.push_back(std::move(s));
    }
    for (const IdentNew& id : idents) {
        auto it = id.rec ? at.find(id.rec) : at.end();
        if (it == at.end()) continue;
        postIdent_[{post_[it->second].fid, std::string(reinterpret_cast<const char*>(id.h), 32)}] = it->second;
    }
    // The lookups a later unit's planning makes into this state.
    for (size_t i = 0; i < post_.size(); i++) {
        const RecState& r = post_[i];
        postSeq_[{r.fid, r.seq}] = i;
        if (r.keyed) postKey_[{r.fid, std::string(reinterpret_cast<const char*>(r.key), 32)}] = i;
        if (r.k.type) postK_[{r.fid, std::to_string(r.k.type) + ":" + r.k.text()}].push_back(i);
        postFids_.insert(r.fid);
    }
    seeds_.clear();  // planned: nothing of the pending units is used after this
}

void WriteCtx::seed(const std::vector<const WriteCtx*>& prevs) {
    // Newest first: a record a later unit touched is in its later state.
    // Nothing is copied until this write asks for a record.
    seeds_.assign(prevs.rbegin(), prevs.rend());
}

bool WriteCtx::seededFeed(uint32_t fid) const {
    for (const WriteCtx* p : seeds_)
        if (p->postFids_.count(fid)) return true;
    return false;
}

void WriteCtx::seededWithK(uint32_t fid, const KVal& k, std::vector<RecState*>* out) {
    out->clear();
    const auto key = std::make_pair(fid, std::to_string(k.type) + ":" + k.text());
    std::set<int64_t> seen;
    for (const WriteCtx* p : seeds_) {
        auto it = p->postK_.find(key);
        if (it == p->postK_.end()) continue;
        for (size_t at : it->second) {
            const int64_t seq = p->post_[at].seq;
            if (!seen.insert(seq).second) continue;
            if (RecState* r = known(fid, seq)) out->push_back(r);
        }
    }
}

RecState* WriteCtx::seededIdent(uint32_t fid, const uint8_t h[32]) {
    const auto key = std::make_pair(fid, std::string(reinterpret_cast<const char*>(h), 32));
    for (const WriteCtx* p : seeds_) {
        auto it = p->postIdent_.find(key);
        if (it != p->postIdent_.end()) return known(fid, p->post_[it->second].seq);
    }
    return nullptr;
}

// After a failed apply: every feed this write appended to goes back to its
// committed mark (the frames past it were never acknowledged).
void WriteCtx::resetStreams() {
    for (auto& kv : newEnd_) {
        Feed* f;
        int64_t mark;
        {
            std::lock_guard<std::mutex> g(t->mu);
            f = t->feedById(kv.first);
            mark = f ? f->mark : 0;
        }
        if (!f) continue;
        int32_t st = P4_OK;
        std::shared_ptr<Stream> stream = streamCur(f, &st);
        if (stream && streamSize(stream.get()) > mark) streamTruncate(stream.get(), mark, false);
        { std::lock_guard<std::mutex> g(t->mu); f->pendingCheckpoint.reset(); f->end = mark; }
    }
}

// ---- apply (the indexer, or the writer for its own synchronous work) -------------------------------------
int32_t WriteCtx::apply() {
    if (!any_) return P4_OK;
    std::string e2;
    const int32_t rc = applyRound(e, {this}, &e2);
    if (rc != P4_OK && err.empty()) err = e2;
    return rc;
}

// The synchronous write (DELETE, SUPERSEDE, QUOTA, REBUILD, open's repairs):
// the writer prepares and applies it itself, its indexer idle.
int32_t WriteCtx::commit() {
    int32_t rc = prepare();
    if (rc == P4_OK) rc = apply();
    if (rc != P4_OK) resetStreams();
    return rc;
}

}  // namespace p4
}  // namespace flatsql
