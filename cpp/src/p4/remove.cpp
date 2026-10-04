// Store format 4: removals and repair on a type's writer (CONTRACT C-37
// (7), (9), C-38; design §4 W-f/W-g/W-i; §3.5 REBUILD; BRIEF4).
//   SUPERSEDE  one feed file: its rows whose batch is not the kept one, a
//              chunk of 32,768 rows per transaction; a record left with no
//              row in the file leaves the feed.
//   DELETE     every row of the CIDs, in every feed file (each file's CID index).
//   QUOTA      the type's oldest record row sets by arrival (the feed files
//              merged by seq).
//   These remove index rows; the stream keeps the frames until a compaction
//   writes the live ones into the next generation (COMPACT).
//   REBUILD    1 the feed files' secondary indexes (after a migration's bulk
//              append); 2 the feed files' counters recounted from their rows;
//              8 the same comparison changing nothing, every row's frame
//              checked in its stream, plus PRAGMA integrity_check on every
//              index file and the type index (C-27).
//   An index that is missing or damaged at open is rebuilt from its stream
//   (rebuildFeed).
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>

#include "flatsql/flatsql_io.h"
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
        // would be left with no row in the file.
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
            RecState* r = w.bySeq(f->fid, seqs[i], &st);
            if (!r) {
                status = st;
                break;
            }
            bool keeps = false;
            for (const RowR& x : r->rows) keeps = keeps || (x.live() && std::find(drop.begin(), drop.end(), x.bId) == drop.end());
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
            RecState* rs = w.bySeq(f->fid, s, &st);
            if (!rs) {
                status = st;
                err = w.err;
                break;
            }
            std::vector<const RowR*> gone;
            for (RowR& x : rs->rows)
                if (x.live() && std::find(drop.begin(), drop.end(), x.bId) != drop.end()) {
                    bool seen = false;
                    for (const RowR* y : gone) seen = seen || (y->bId == x.bId && y->cId == x.cId);
                    if (!seen) chunkTags++;
                    gone.push_back(&x);
                    x.del = true;
                    rs->touched = true;
                }
            if (!anyLive(rs)) chunkRecs++;
        }
        if (status != P4_OK) break;
        status = w.commit();
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
// Every feed file holding a CID gives up its rows of it (each file's CID
// index), and the instances it leaves are restamped with the clock (format
// 1's decrementSourceSummary, C-39 E5). `deleted` counts the CID's distinct
// copies (producer tokens) over the files, format 1's count of producer rows.
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
        std::vector<uint32_t> fids;
        {
            std::lock_guard<std::mutex> g(t->mu);
            for (auto& f : t->feeds) {
                if (!f->created || f->k.recs <= 0) continue;
                if (f->quarantined) {
                    status = P4_E_CORRUPT;
                    err = "feed file quarantined: " + f->path;
                }
                fids.push_back(f->fid);
            }
        }
        WriteCtx w(e, t);
        const uint32_t n = ld32(cids->v);
        for (uint32_t i = 0; i < n && status == P4_OK; i++) {
            const uint8_t* c36 = cids->v + 4 + size_t(i) * kCidBin;
            if (!cidBinValid(c36)) continue;
            uint8_t key[32];
            cidKeyFromDigest(c36 + 4, key);
            std::set<uint32_t> toks;
            for (uint32_t fid : fids) {
                RecState* r = w.byKey(fid, key, &status);
                if (!r) {
                    err = w.err;
                    break;
                }
                if (!r->existed) continue;
                for (auto& kv : copiesLive(r)) toks.insert(kv.first);
                r->restamp = true;
                w.dropAll(r);
            }
            deleted += int64_t(toks.size());
        }
        if (status == P4_OK) {
            status = w.commit();
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
            RecState* r = w.bySeq(in->seqs[i].first, in->seqs[i].second, &st);
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
        const int32_t rc = w.commit();
        if (rc != P4_OK) {
            in->status = rc;
            in->err = "quota commit failed: " + w.err;
            break;
        }
        in->a += recs;
        in->b += bytes;
    }
}

// ---- REBUILD ----------------------------------------------------------------------------------------
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

// One feed file's rows, recounted: its counters, its instances and its
// tokens' copies, record by record in rid order. Also whether each record's
// rows are its copies x its instances (local records: one row per copy) and
// one CID, and whether every frame a row names is in the stream below the
// mark with its size prefix.
struct FileCount {
    Counters k;
    std::map<InstId, InstCount> inst;
    std::map<uint32_t, TokCount> tokc;
    int64_t records = 0, u2bad = 0, framesBad = 0;
};
int32_t countFile(Type* t, Feed* f, Conn* c, Stream* stream, int64_t mark, FileCount* fc) {
    std::shared_ptr<const Spec> sp = t->spec();
    if (!dictEnsure(f, c)) return P4_E_IO;
    sqlite3_stmt* q = c->sql("SELECT rid, seq, n, b, c, at, cid, e, k, ts, len, off FROM r ORDER BY rid");
    if (!q) return P4_E_INTERNAL;
    struct RowC {
        uint32_t tok, b, cc;
        int64_t len, ts, off;
        std::string key;
    };
    bool have = false;
    int64_t seq = 0, ev = 0;
    bool hasE = false;
    std::vector<RowC> rows;
    int32_t st = P4_OK;
    auto frameOk = [&](int64_t off, int64_t len) {
        if (off < 0 || off + 4 + len > mark || !stream) return false;
        uint8_t p[4];
        if (flatsql_io_read(stream->h, p, 4, double(off)) != 4) return false;
        return ld32(p) == uint32_t(len);
    };
    auto flush = [&]() {
        if (!have) return;
        Counters& k = fc->k;
        int64_t bytes = 0;
        std::map<uint32_t, int64_t> cp;
        std::map<InstId, int64_t> instLen;
        std::map<int64_t, int64_t> frames;
        bool localRows = false, oneCid = true;
        for (const RowC& r : rows) {
            bytes += r.len;
            oneCid = oneCid && r.key == rows[0].key;
            auto fit = frames.find(r.off);
            if (fit == frames.end()) frames[r.off] = r.len;
            else if (fit->second != r.len) fc->framesBad++;  // one frame, two lengths
            auto it = cp.find(r.tok);
            if (it == cp.end() || r.len < it->second) cp[r.tok] = r.len;
            if (!r.b) {
                localRows = true;
                continue;
            }
            const InstId id(r.b, r.cc);
            auto il = instLen.find(id);
            if (il == instLen.end() || r.len < il->second) instLen[id] = r.len;
        }
        for (auto& kv : frames) {
            k.fbytes += kv.second + 4;
            if (!frameOk(kv.first, kv.second)) fc->framesBad++;
        }
        int64_t minLen = INT64_MAX, sumLen = 0;
        for (auto& kv : cp) {
            minLen = std::min(minLen, kv.second);
            sumLen += kv.second;
        }
        k.rows += int64_t(rows.size());
        k.bytes += bytes;
        k.recs++;
        k.rbytes += minLen;
        k.copies += int64_t(cp.size());
        k.cbytes += sumLen;
        if (sp->hasEpochRule && !hasE) k.nnull++;
        k.minseq = std::min(k.minseq, seq);
        k.maxseq = std::max(k.maxseq, seq);
        for (const RowC& r : rows) {  // a copy may keep its own ts (C-39 E6)
            const int64_t w = hasE ? ev : r.ts;
            k.minw = std::min(k.minw, w);
            k.maxw = std::max(k.maxw, w);
            k.mints = std::min(k.mints, r.ts);
            k.maxts = std::max(k.maxts, r.ts);
        }
        if (hasE) {
            k.mine = std::min(k.mine, ev);
            k.maxe = std::max(k.maxe, ev);
        }
        for (auto& kv : instLen) {
            InstCount& ic = fc->inst[kv.first];
            ic.n++;
            ic.bytes += kv.second;
        }
        for (auto& kv : cp) {
            TokCount& tc = fc->tokc[kv.first];
            tc.n++;
            tc.bytes += kv.second;
        }
        // every copy with every instance; local rows only without one; one CID
        const size_t want = instLen.empty() ? cp.size() : cp.size() * instLen.size();
        if (rows.size() != want || (localRows && !instLen.empty()) || (f->local != instLen.empty()) || !oneCid) fc->u2bad++;
        fc->records++;
        rows.clear();
        have = false;
    };
    int r;
    while ((r = sqlite3_step(q)) == SQLITE_ROW) {
        const int64_t s = sqlite3_column_int64(q, 1);
        if (have && s != seq) flush();
        if (!have) {
            seq = s;
            hasE = sqlite3_column_type(q, 7) != SQLITE_NULL;
            ev = sqlite3_column_int64(q, 7);
            have = true;
        }
        RowC x;
        x.ts = sqlite3_column_int64(q, 9);
        const uint32_t n = uint32_t(sqlite3_column_int64(q, 2));
        x.b = uint32_t(sqlite3_column_int64(q, 3));
        x.cc = uint32_t(sqlite3_column_int64(q, 4));
        x.len = sqlite3_column_int64(q, 10);
        x.off = sqlite3_column_int64(q, 11);
        if (sqlite3_column_bytes(q, 6) == 32) x.key.assign(static_cast<const char*>(sqlite3_column_blob(q, 6)), 32);
        NodeDef nd;
        if (!dictNode(f, c, n, &nd)) {
            st = P4_E_CORRUPT;
            break;
        }
        {
            std::lock_guard<std::mutex> g(t->mu);
            x.tok = tokFor(t, nd.producer, nd.peer, true);
        }
        rows.push_back(std::move(x));
    }
    sqlite3_reset(q);
    if (st == P4_OK && r != SQLITE_DONE && r != SQLITE_ROW) st = statusOfSqlite(r);
    if (st == P4_OK) flush();
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

bool sameCounts(const Counters& a, const Counters& b) {
    return a.rows == b.rows && a.recs == b.recs && a.bytes == b.bytes && a.nnull == b.nnull && a.rbytes == b.rbytes &&
           a.copies == b.copies && a.cbytes == b.cbytes && a.fbytes == b.fbytes;
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
bool sameTokc(const std::map<uint32_t, TokCount>& a, const std::map<uint32_t, TokCount>& b) {
    if (a.size() != b.size()) return false;
    for (auto& kv : a) {
        auto it = b.find(kv.first);
        if (it == b.end() || it->second.n != kv.second.n || it->second.bytes != kv.second.bytes) return false;
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
            st = fileCreateIndexes(t, c, f->local);
            writerUnpin(e, f);
            if (st != P4_OK) {
                in->status = st;
                in->err = "indexes of " + f->path;
                return;
            }
            std::lock_guard<std::mutex> g(t->mu);
            f->indexed = true;
        }
        e->bump(kStRebuilds);
    }
    if (in->what & (2 | 8)) {
        const bool fix = (in->what & 2) != 0;
        int64_t entries = 0, mismatches = 0;
        for (Feed* f : feeds) {
            // 1. The file's counters, instances and tokens from its rows (and
            //    every row's frame in the stream), against its meta and the
            //    engine's. This runs on the type's writer: the stream's end and
            //    generation are the committed ones.
            int32_t st = P4_OK;
            std::shared_ptr<Stream> stream;
            int64_t mark;
            {
                std::lock_guard<std::mutex> g(t->mu);
                mark = f->mark;
            }
            if (mark > 0) {
                stream = streamCur(f, &st);
                if (!stream) {
                    in->status = st;
                    in->err = "stream of " + f->path;
                    return;
                }
            }
            Conn* c = nullptr;
            if (openConn(f->path, OpenKind::Maint, 4096, 0, &c, nullptr) != SQLITE_OK) {
                in->status = P4_E_IO;
                in->err = "open " + f->path;
                return;
            }
            c->exec("BEGIN");
            FileCount fc;
            st = countFile(t, f, c, stream.get(), mark, &fc);
            FileMeta stored;
            std::map<InstId, InstCount> storedInst;
            std::map<uint32_t, TokCount> storedTokc;
            if (st == P4_OK && (readMeta(c, &stored) != SQLITE_OK || readInst(f, c, &storedInst) != SQLITE_OK ||
                                readTokc(t, c, &storedTokc) != SQLITE_OK))
                st = P4_E_IO;
            c->exec("COMMIT");
            delete c;
            if (st != P4_OK) {
                in->status = st;
                in->err = "count " + f->path;
                return;
            }
            entries += fc.records;
            Counters mem;
            std::map<InstId, InstCount> memInst;
            std::map<uint32_t, TokCount> memTokc;
            {
                std::lock_guard<std::mutex> g(t->mu);
                mem = f->k;
                memInst = f->inst;
                memTokc = f->tokc;
            }
            const bool fileBad = !sameCounts(fc.k, stored.k) || !sameInst(fc.inst, storedInst) || !sameTokc(fc.tokc, storedTokc);
            const bool memBad = !sameCounts(fc.k, mem) || !sameInst(fc.inst, memInst) || !sameTokc(fc.tokc, memTokc);
            const bool markBad = stored.mark != mark || stored.gen != f->gen || f->end != mark ||
                                 (stream && streamSize(stream.get()) != mark);
            if (fileBad) {
                mismatches++;
                why("file counters rows", f->path, stored.k.rows, fc.k.rows);
                why("file counters recs", f->path, stored.k.recs, fc.k.recs);
                why("file counters fbytes", f->path, stored.k.fbytes, fc.k.fbytes);
            }
            if (memBad) {
                mismatches++;
                why("engine counters recs", f->path, mem.recs, fc.k.recs);
            }
            if (markBad) {
                mismatches++;
                why("stream mark", f->path, stored.mark, f->end);
            }
            if (fc.u2bad) {
                mismatches += fc.u2bad;
                why("rows not copies x instances", f->path, fc.u2bad, 0);
            }
            if (fc.framesBad) {
                mismatches += fc.framesBad;
                why("rows naming no frame of the stream", f->path, fc.framesBad, 0);
            }
            if (fix && (fileBad || memBad)) {
                // The file's counters rewritten from its rows (its writer),
                // keeping the times and bounds the rows cannot give back.
                int32_t ps = P4_OK;
                std::string er;
                Conn* w = writerPin(e, f, &ps, &er);
                if (!w) {
                    in->status = ps;
                    in->err = er;
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
                for (auto& kv : fc.tokc) {
                    auto old = storedTokc.find(kv.first);
                    if (old != storedTokc.end()) {
                        kv.second.mints = old->second.mints;
                        kv.second.maxts = old->second.maxts;
                        kv.second.maxseq = old->second.maxseq;
                    }
                }
                if (r == SQLITE_OK) r = w->exec("DELETE FROM tokc");
                if (r == SQLITE_OK) r = writeTokc(t, w, {}, fc.tokc);
                fc.k.maxseq = std::max(fc.k.maxseq, stored.k.maxseq);
                if (r == SQLITE_OK) r = writeMeta(w, fc.k, nowSec(), mark, f->gen);
                if (r == SQLITE_OK) r = w->exec("COMMIT");
                if (r != SQLITE_OK) w->exec("ROLLBACK");
                writerUnpin(e, f);
                if (r != SQLITE_OK) {
                    in->status = statusOfSqlite(r);
                    in->err = "repair " + f->path;
                    return;
                }
                std::lock_guard<std::mutex> g(t->mu);
                f->k = fc.k;
                f->inst = fc.inst;
                f->tokc = fc.tokc;
            }
            if (!fileIntact(f->path)) {
                mismatches++;
                if (in->firstBad.empty()) in->firstBad = f->path;
            }
        }
        // C-27: the type index too.
        if (!fileIntact(t->pIdx)) {
            mismatches++;
            if (in->firstBad.empty()) in->firstBad = t->pIdx;
        }
        in->a = entries;
        in->b = mismatches;
        e->bump(kStRebuilds);
    }
}

// ---- COMPACT -----------------------------------------------------------------------------------------
// A stream is compacted once more than half of it is dead (frames no live row
// names) and the dead part is at least 1 MiB.
bool compactDue(const Feed* f) {
    const int64_t dead = f->mark - f->k.fbytes;
    return f->created && !f->quarantined && f->mark > 0 && dead >= (int64_t(1) << 20) && dead * 2 > f->mark;
}

void compactWork(Engine* e, Type* t, Internal* in) {
    Feed* f;
    bool due;
    {
        std::lock_guard<std::mutex> g(t->mu);
        f = t->feedById(in->what);
        due = f && compactDue(f);
    }
    if (!due) return;
    in->a = f->end;  // the writer's own work, its indexer idle: the end is the mark
    int32_t st = P4_OK;
    std::string er;
    Conn* c = writerPin(e, f, &st, &er);
    if (!c) {
        in->status = st;
        in->err = er;
        return;
    }
    // 1. The live frames, in stream order (arrival), and their new offsets.
    std::vector<std::pair<int64_t, int64_t>> rows;  // (rid, off)
    std::map<int64_t, int64_t> live;                // off -> len
    {
        sqlite3_stmt* q = c->sql("SELECT rid, off, len FROM r");
        int r = q ? SQLITE_OK : SQLITE_ERROR;
        while (q && (r = sqlite3_step(q)) == SQLITE_ROW) {
            rows.push_back({sqlite3_column_int64(q, 0), sqlite3_column_int64(q, 1)});
            live[sqlite3_column_int64(q, 1)] = sqlite3_column_int64(q, 2);
        }
        if (q) sqlite3_reset(q);
        if (r != SQLITE_DONE) {
            writerUnpin(e, f);
            in->status = statusOfSqlite(r);
            in->err = "rows of " + f->path;
            return;
        }
    }
    std::shared_ptr<Stream> old = streamCur(f, &st);
    const uint32_t gen = f->gen + 1;
    std::shared_ptr<Stream> next;
    if (old) {
        // The next generation from scratch (a leftover of a cut compaction is cut to 0).
        const std::string path = streamPath(f, gen);
        const int32_t h = flatsql_io_open(path.data(), int32_t(path.size()),
                                          FLATSQL_IO_READ | FLATSQL_IO_WRITE | FLATSQL_IO_CREATE | FLATSQL_IO_TRUNC |
                                              FLATSQL_IO_CREATE_PARENTS);
        if (h < 0) {
            st = h == FLATSQL_IO_ERR_NOSPACE ? P4_E_NOSPACE : P4_E_IO;
        } else {
            next = std::make_shared<Stream>();
            next->path = path;
            next->gen = gen;
            next->h = h;
        }
    }
    std::map<int64_t, int64_t> moved;  // old off -> new off
    int64_t at = 0;
    std::string buf;
    for (auto it = live.begin(); st == P4_OK && it != live.end(); ++it) {
        std::string bytes;
        st = streamRead(old.get(), it->first, it->second, &bytes);
        if (st != P4_OK) break;
        moved[it->first] = at + int64_t(buf.size());
        uint8_t p[4];
        st32(p, uint32_t(bytes.size()));
        buf.append(reinterpret_cast<const char*>(p), 4);
        buf.append(bytes);
        if (buf.size() >= (4u << 20)) {
            st = streamWrite(next.get(), at, reinterpret_cast<const uint8_t*>(buf.data()), buf.size());
            at += int64_t(buf.size());
            buf.clear();
        }
    }
    if (st == P4_OK && !buf.empty()) {
        st = streamWrite(next.get(), at, reinterpret_cast<const uint8_t*>(buf.data()), buf.size());
        at += int64_t(buf.size());
    }
    if (st == P4_OK) st = streamSync(next.get());
    if (st != P4_OK) {
        writerUnpin(e, f);
        if (next) next->drop.store(true);
        in->status = st;
        in->err = "compact " + f->path;
        return;
    }
    // 2. The swap: readers find the new generation as soon as the commit
    //    shows it to them, and the old one while their snapshot names it.
    {
        std::lock_guard<std::mutex> g(f->smu);
        f->retired = old;
        f->retiredBytes = in->a;
        f->retireAt = monoNs() + 30ull * 1000 * 1000 * 1000;
        f->stream = next;
        f->gen = gen;
    }
    // 3. One transaction: every row repointed, the new generation and mark.
    Counters k;
    {
        std::lock_guard<std::mutex> g(t->mu);
        k = f->k;
    }
    int r = c->exec("BEGIN IMMEDIATE");
    sqlite3_stmt* q = r == SQLITE_OK ? c->get(S_R_OFF) : nullptr;
    if (r == SQLITE_OK && !q) r = SQLITE_ERROR;
    for (size_t i = 0; r == SQLITE_OK && i < rows.size(); i++) {
        sqlite3_bind_int64(q, 1, rows[i].first);
        sqlite3_bind_int64(q, 2, moved[rows[i].second]);
        r = sqlite3_step(q);
        sqlite3_reset(q);
        if (r == SQLITE_DONE) r = SQLITE_OK;
    }
    if (r == SQLITE_OK) r = writeMeta(c, k, nowSec(), at, gen);
    if (r == SQLITE_OK) r = c->exec("COMMIT");
    if (r != SQLITE_OK) {
        c->exec("ROLLBACK");
        writerUnpin(e, f);
        {
            std::lock_guard<std::mutex> g(f->smu);
            f->stream = old;
            f->retired.reset();
            f->retiredBytes = 0;
            f->gen = gen - 1;
        }
        next->drop.store(true);
        in->status = statusOfSqlite(r);
        in->err = "compact commit " + f->path;
        return;
    }
    writerUnpin(e, f);
    old->drop.store(true);  // unlinked when the last reader holding it lets go
    f->end = at;
    {
        std::lock_guard<std::mutex> g(t->mu);
        f->mark = at;
        f->streamBytes = at;
    }
    // The registry follows (a missing index's rebuild reads it; open also
    // tries the generation after the registered one).
    if (t->idx) {
        int x = t->idx->exec("BEGIN IMMEDIATE");
        if (x == SQLITE_OK) x = indexPutFeed(t->idx, *f, gen);
        if (x == SQLITE_OK) x = t->idx->exec("COMMIT");
        if (x != SQLITE_OK) t->idx->exec("ROLLBACK");
        else {
            std::lock_guard<std::mutex> g(t->mu);
            f->regGen = gen;
        }
    }
    in->b = at;
    e->bump(kStRebuilds);
}

// ---- an index rebuilt from its stream ----------------------------------------------------------------
// The feed's index is missing or damaged at open: the damaged file goes, and
// a new one is built from the stream's frames. Keys come from the bytes (the
// CID is the sha256 of the frame's FlatBuffer, the epoch and object key are
// extracted). What only the index held is gone: each record gets a fresh seq,
// the open's clock as its ts and delivery time, an empty publishing node, and
// (a source feed) one instance with an empty batch. A frame whose CID the
// feed already indexed, a sealed frame (its CID is the plaintext's), or a
// frame that does not parse stays in the stream unindexed. The stream is cut
// after its last whole frame.
int32_t rebuildFeed(Engine* e, Type* t, Feed* f, int64_t* indexed, std::string* err) {
    *indexed = 0;
    writerDrop(e, f);
    e->rpool.dropPath(f->path);
    for (const char* sfx : {"", "-wal", "-shm", "-journal"}) ioUnlink(f->path + sfx);
    {
        std::lock_guard<std::mutex> g(t->mu);
        f->created = false;
        f->indexed = true;
        f->k = Counters();
        f->inst.clear();
        f->tokc.clear();
        f->quarantined = false;
    }
    {
        std::lock_guard<std::mutex> g(f->dictMu);
        f->dict = Dict();
    }
    int32_t st = P4_OK;
    std::shared_ptr<Stream> stream = streamCur(f, &st);
    if (!stream) {
        if (err) *err = "open " + streamPath(f, f->gen);
        return st;
    }
    const int64_t size = streamSize(stream.get());
    if (size < 0) return P4_E_IO;
    std::shared_ptr<const Spec> sp = t->spec();
    const ps::TypeConfig& tc = sp->tc;
    // The whole frames.
    std::vector<std::pair<int64_t, uint32_t>> frames;
    int64_t at = 0;
    while (at + 4 <= size) {
        uint8_t p[4];
        if (flatsql_io_read(stream->h, p, 4, double(at)) != 4) break;
        const uint32_t n = ld32(p);
        if (at + 4 + int64_t(n) > size) break;
        frames.push_back({at, n});
        at += 4 + int64_t(n);
    }
    if (at < size) {
        st = streamTruncate(stream.get(), at, true);
        if (st != P4_OK) return st;
    }
    f->end = at;
    {
        std::lock_guard<std::mutex> g(t->mu);
        f->mark = 0;  // the new index commits the whole stream
    }
    uint32_t tok;
    {
        std::lock_guard<std::mutex> g(t->mu);
        tok = tokFor(t, std::string(), std::string(), true);
    }
    std::unordered_set<std::string> seen;
    const int64_t now = nowSec();
    for (size_t i = 0; i < frames.size();) {
        WriteCtx w(e, t);
        std::vector<RecState*> fresh;
        for (; i < frames.size() && fresh.size() < 4096; i++) {
            std::string bytes;
            if (streamRead(stream.get(), frames[i].first, frames[i].second, &bytes) != P4_OK) continue;
            const uint8_t* d = reinterpret_cast<const uint8_t*>(bytes.data());
            if (ps::sealedEnvelopeValid(d, bytes.size())) continue;
            std::string frame(bytes.size() + 4, '\0');
            st32(reinterpret_cast<uint8_t*>(&frame[0]), uint32_t(bytes.size()));
            std::memcpy(&frame[4], bytes.data(), bytes.size());
            if (tc.checkFrame(reinterpret_cast<const uint8_t*>(frame.data()), uint32_t(frame.size())) != 0) continue;
            uint8_t dg[32], key[32];
            ps::sha256(d, bytes.size(), dg);
            cidKeyFromDigest(dg, key);
            if (!seen.insert(std::string(reinterpret_cast<const char*>(key), 32)).second) continue;
            ps::Extracted x;
            uint8_t scratch[2048];
            tc.extract(reinterpret_cast<const uint8_t*>(frame.data()), frame.size(), &x, scratch, sizeof scratch);
            int32_t rc = P4_OK;
            RecState* rs = w.byKey(f->fid, key, &rc);
            if (!rs) return rc;
            rs->hasE = x.hasEpoch;
            rs->e = x.epochSec;
            rs->ts = now;
            rs->w = rs->hasE ? rs->e : now;
            if (x.objectCol >= 0 && x.objectCol < int(ps::kMaxCols) && x.cols[x.objectCol].present) {
                const ps::ColValue& cv = x.cols[x.objectCol];
                if (cv.isU64) {
                    rs->k.type = 1;
                    rs->k.i = int64_t(cv.u);
                } else {
                    rs->k.type = 3;
                    rs->k.s.assign(reinterpret_cast<const char*>(cv.s), cv.n);
                }
            }
            RowR copy;
            copy.tok = tok;
            copy.ts = now;
            copy.len = int64_t(bytes.size());
            copy.off = frames[i].first;
            std::vector<WriteCtx::Inst> insts;
            if (!f->local) insts.push_back(WriteCtx::Inst{std::string(), std::string(), std::string(), std::string(), std::string(), now});
            WriteCtx::Out out;
            w.deliver(rs, copy, insts, &out);
            fresh.push_back(rs);
        }
        {
            std::lock_guard<std::mutex> g(t->mu);
            for (RecState* rs : fresh) rs->seq = t->nextSeq++;
        }
        st = w.commit();
        if (st != P4_OK) {
            if (err) *err = "rebuild " + f->path + ": " + w.err;
            return st;
        }
        *indexed += int64_t(fresh.size());
    }
    if (frames.empty()) {
        // Nothing to index: an empty index file, so the feed opens as created.
        Conn* c = writerPin(e, f, &st, nullptr);
        if (!c) return st;
        writerUnpin(e, f);
    }
    e->bump(kStRebuilds);
    return P4_OK;
}

}  // namespace p4
}  // namespace flatsql
