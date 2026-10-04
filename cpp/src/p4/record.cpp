// Store format 4: a record's rows in one feed file (CONTRACT C-37, C-38),
// and how a write changes and commits them (BRIEF4: the bytes in the feed's
// stream, the rows in its index).
//
// A record of a feed file is found by its CID (the file's one CID index) or
// its seq; its rows are one rid range of that file (rid = seq << 16 | n):
// every copy (producer token) appears with every instance (batch, content
// key, producer peer and key) of the feed; a local record (no source) has one
// row per copy. Every row points at its bytes' frame in the feed's stream;
// the rows of a record whose bytes are the same share one frame. A write
// loads the records it touches, changes their rows, then commits:
//   1. the type index (synchronous=FULL), only when the write uses a feed or
//      a producer token the registry does not hold yet (the writer, before
//      any file of a new feed exists);
//   2. each feed, feeds before local: its new frames appended to its stream
//      (a new feed's index file made before its first frame) and synced,
//      then its index file in one transaction (rows, the file's
//      counters, its instances' and tokens' counters, its ingest identities,
//      the moves it takes in, and the stream's new mark).
// There is no ingest journal: a feed file's transaction is the whole truth
// about that feed. Its new rows are staged (m=0: not in r_c, r_ke, r_w until
// the indexer merges them, staged.cpp) and its ingest identities go to idst,
// so the transaction writes appended pages only; the feed's view of staged
// rows is published after the commit, before the ack. The one write that spans two files with one record is a
// move (a local record taken into a feed): the feed's transaction records the
// seq in its `moved` table, and open finishes a move a crash cut (the local
// rows of the seq go once the feed holds it).
#include <algorithm>

#include "internal.h"

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

void WriteCtx::dropAll(RecState* r) {
    for (RowR& x : r->rows)
        if (x.live()) {
            x.del = true;
            r->touched = true;
        }
    sweep(r);
}

// ---- commit ---------------------------------------------------------------------------------------
// New feeds and producer tokens go into the type index while the writer
// plans, before any file of a new feed exists: every feed file on disk then
// belongs to a registered feed, which a reopen opens and recovers.
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
    e->bump(kStIndexFlushes);
    e->bump(kStIndexFlushEntries, uint64_t(feeds.size() + toks.size()));
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

// A feed's first frame: its index file comes first (the feed is registered
// already). writerPin creates it durably, directory entry included, and
// commits its schema with mark 0. A stream is then never without its index
// unless the index was lost, and open cuts the frames past the index's mark
// (never acknowledged) instead of indexing them as records with blank
// provenance (GATES-stream-r1 B1).
int32_t WriteCtx::fileCreate(Feed* f) {
    {
        std::lock_guard<std::mutex> g(t->mu);
        if (f->created) return P4_OK;
        if (migrate) f->indexed = false;  // a migration: REBUILD 1 adds the secondary indexes
    }
    int32_t st = P4_OK;
    std::string er;
    if (!writerPin(e, f, &st, &er)) {
        err = "create " + f->path + ": " + er;
        return st;
    }
    writerUnpin(e, f);
    return P4_OK;
}

int32_t WriteCtx::fileBegin(FileTxn& x, Feed* f) {
    x.f = f;
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
        x.dict = f->dict;
    }
    {
        std::lock_guard<std::mutex> g(t->mu);
        x.k = f->k;
        x.inst = f->inst;
        x.tokc = f->tokc;
        x.indexed = f->indexed;
        x.clearMoved = f->movedRows && f->movedDone;
        x.newEnd = f->mark;
        x.stBase = f->staged;
    }
    x.tokcBefore = x.tokc;
    x.rc = x.c->exec("BEGIN IMMEDIATE");
    x.open = true;
    // The finished moves go; the round's are recorded with its rows.
    if (x.rc == SQLITE_OK && x.clearMoved) x.rc = x.c->exec("DELETE FROM moved");
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
    auto ne = newEnd_.find(f->fid);
    if (ne != newEnd_.end()) x.newEnd = std::max(x.newEnd, ne->second);
    int& rc = x.rc;
    Dict& dict = x.dict;
    Counters& k = x.k;
    auto bad = [&](int r) {
        if (rc == SQLITE_OK && r != SQLITE_OK && r != SQLITE_DONE && r != SQLITE_ROW) rc = r;
    };
    auto mv = movesIn_.find(f->fid);
    if (mv != movesIn_.end()) {
        x.moves = true;
        for (int64_t seq : mv->second) {
            if (rc != SQLITE_OK) break;
            sqlite3_stmt* q = c->get(S_MOVED_INS);
            if (!q) {
                rc = SQLITE_ERROR;
                break;
            }
            sqlite3_bind_int64(q, 1, seq);
            bad(sqlite3_step(q));
            sqlite3_reset(q);
        }
    }
    // Rows an earlier write of the round or a previous unit added (this
    // write's seed) carry no file ids yet: their instance's ids are looked up.
    for (RecState* r : recs)
        for (RowR& y : r->rows) {
            if (rc != SQLITE_OK) break;
            if (!y.loaded || !y.inst || y.bId) continue;
            y.bId = internBatch(dict, c, y, &rc);
            y.cId = internText(c, S_CKEY_GET, S_CKEY_INS, y.ckey, &rc);
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
                sqlite3_stmt* q = c->get(S_R_DEL);
                sqlite3_bind_int64(q, 1, y.rid);
                bad(sqlite3_step(q));
                sqlite3_reset(q);
                x.deleted = true;
                // A staged row leaves the view (one this round staged, or the base's).
                if (!x.stAdd.erase(y.rid)) x.stDrop.push_back(y.rid);
            } else if (y.loaded && y.urlSet) {
                y.uId = internText(c, S_URL_GET, S_URL_INS, y.url, &rc);
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
                y.nId = internNode(dict, c, producer, y.peer, &rc);
                y.bId = y.inst ? internBatch(dict, c, y, &rc) : 0;
                y.cId = y.inst ? internText(c, S_CKEY_GET, S_CKEY_INS, y.ckey, &rc) : 0;
                y.uId = y.inst ? internText(c, S_URL_GET, S_URL_INS, y.url, &rc) : 0;
                if (rc != SQLITE_OK) break;
                if (y.off < 0) {
                    rc = SQLITE_INTERNAL;  // frames() gives every new row its frame
                    break;
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
            x.changedInst.insert(kv.first);
            InstCount& ic = x.inst[kv.first];
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
    return P4_OK;
}

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
    int64_t oldMark;
    {
        std::lock_guard<std::mutex> g(t->mu);
        oldMark = f->mark;
    }
    if (rc == SQLITE_OK && x.newEnd > oldMark) {
        int32_t st = P4_OK;
        std::shared_ptr<Stream> stream = streamCur(f, &st);
        if (stream) st = streamSync(stream.get());
        if (st != P4_OK) {
            *err = "stream sync " + streamPath(f, f->gen);
            c->exec("ROLLBACK");
            writerUnpin(e, f);
            x.open = false;
            return st;
        }
        e->bump(kStJournalSyncs);
    }
    Counters& k = x.k;
    for (const InstId& id : x.changedInst) {
        if (rc != SQLITE_OK) break;
        auto it = x.inst.find(id);
        if (it == x.inst.end() || it->second.n <= 0) {
            if (it != x.inst.end()) x.inst.erase(it);
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
        }
        return statusOfSqlite(rc);
    }
    int64_t freeBytes = 0;
    const int64_t dbBytes = dbBytesOf(c, &freeBytes);
    writerUnpin(e, f);
    e->bump(kStGroupCommits);
    {
        std::lock_guard<std::mutex> g(f->dictMu);
        if (x.dict.node.size() >= f->dict.node.size() && x.dict.batch.size() >= f->dict.batch.size()) f->dict = std::move(x.dict);
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
    f->mark = x.newEnd;
    f->streamBytes = x.newEnd;
    if (stChanged) stagedPublish(e, f, x.stNew, !x.stAdd.empty() || !x.idAdd.empty());
    f->k = k;
    f->inst = std::move(x.inst);
    f->tokc = std::move(x.tokc);
    f->created = true;
    if (x.clearMoved) f->movedRows = false;
    if (x.moves) {
        f->movedRows = true;
        f->movedDone = false;  // until the local file's commit takes the moved rows
    }
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
    for (WriteCtx* w : units) {
        if (first != P4_OK) break;
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
    // Every local file committed: the moves into the feeds are finished (the
    // feeds' next commits drop their `moved` rows; open finishes the others).
    bool localsOk = true;
    for (Feed* f : locals) localsOk = localsOk && committed.count(f);
    if (localsOk)
        for (auto& kv : txns)
            if (kv.second.moves && committed.count(kv.first)) {
                std::lock_guard<std::mutex> g(kv.first->type->mu);
                kv.first->movedDone = true;
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
    // New feeds and tokens registered, then frames appended to each feed's
    // stream at its end (the writer's offset, past the frames of a unit still
    // being applied); the apply syncs them.
    newEnd_.clear();
    oldEnd_.clear();
    if (const int32_t rc = registryWrite()) return rc;
    for (uint32_t fid : fileOrder_) {
        Feed* f;
        {
            std::lock_guard<std::mutex> g(t->mu);
            f = t->feedById(fid);
        }
        std::vector<RecState*> recs;
        for (RecState* r : order_)
            if (r->touched && r->fid == fid && r->seq) recs.push_back(r);
        const int64_t oldEnd = f->end;
        std::string fr;
        int32_t st = frames(f, recs, &fr, oldEnd);
        if (st == P4_OK && !fr.empty()) st = fileCreate(f);
        if (st == P4_OK && !fr.empty()) {
            std::shared_ptr<Stream> stream = streamCur(f, &st);
            if (stream) st = streamWrite(stream.get(), oldEnd, reinterpret_cast<const uint8_t*>(fr.data()), fr.size());
            if (st != P4_OK) {
                err = "stream append " + streamPath(f, f->gen);
                if (stream) streamTruncate(stream.get(), oldEnd, false);
            }
        }
        if (st != P4_OK) {
            // This write's frames so far go (a pending unit's stay below them).
            for (auto& kv : oldEnd_) {
                Feed* g2;
                {
                    std::lock_guard<std::mutex> g(t->mu);
                    g2 = t->feedById(kv.first);
                }
                int32_t s2 = P4_OK;
                std::shared_ptr<Stream> stream = g2 ? streamCur(g2, &s2) : nullptr;
                if (stream) streamTruncate(stream.get(), kv.second, false);
                if (g2) g2->end = kv.second;
            }
            newEnd_.clear();
            return st;
        }
        oldEnd_[fid] = oldEnd;
        f->end = oldEnd + int64_t(fr.size());
        newEnd_[fid] = f->end;
    }
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
            y.hasD = false;
            y.d.clear();
            y.srcFid = 0;
            y.srcRid = 0;
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
        f->end = mark;
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
