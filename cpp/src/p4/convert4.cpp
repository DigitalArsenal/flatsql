// Offline format-4 reader and verified format-5 rewrite of a PRIVATE copy.
// The feed reader below is from FlatSQL 3.7.2,
// 1b3ae18c00bb7a8917acc3db32b903d6ae8affea:cpp/src/p4/partition.cpp.
// It reads the real format-4 SQLite metadata and record stream. The shared
// readMeta/readInst/readTokc/streamRead codecs retain that on-disk contract.
// Only status names and the stream's initial mark are adapted here.
// Normal opens never accept format 4. The host retains the source and publishes
// this copy only after conversion, verification, and activation all succeed.
#include "internal.h"
#include "dag.h"
#include <cstdio>
#include <map>

namespace flatsql { namespace p4 {
namespace {
bool damaged(int rc) { return (rc & 0xff) == SQLITE_CORRUPT || (rc & 0xff) == SQLITE_NOTADB; }
}
static int32_t format4FeedOpen(Type* t, Feed* f, std::vector<int64_t>* moved, std::string* err) {
    Engine* e = t->e;
    moved->clear();
    if (!ioExists(f->path)) {
        // No index: a feed never written, or an index that is gone. A feed's
        // index file is made before its first frame (WriteCtx::fileCreate),
        // so a stream without one lost its index: that stream (the registered
        // generation, or the next one a compaction committed after the
        // registry last named it) is indexed again from its frames
        // (rebuildFeed, after every feed is open).
        std::lock_guard<std::mutex> g(t->mu);
        f->created = false;
        for (uint32_t gen : {f->regGen + 1, f->regGen})
            if (ioExists(streamPath(f, gen))) {
                f->gen = gen;
                return FLATSQL_E_CORRUPT;  // the caller rebuilds
            }
        return FLATSQL_OK;
    }
    {
        std::lock_guard<std::mutex> g(t->mu);
        f->created = true;  // the writer open completes a schema a crash cut before its commit
    }
    int32_t st = FLATSQL_OK;
    std::string er;
    Conn* c = writerPin(e, f, &st, &er);
    if (!c) {
        if (st == FLATSQL_E_CORRUPT) return FLATSQL_E_CORRUPT;  // a damaged index: rebuilt from the stream
        if (err) *err = "open " + f->path + ": " + er;
        return st;
    }
    // The current layout (C-45: staged rows, the keys' okey ids, the CID
    // prefix); an index an earlier format-4 build wrote lacks some of it and
    // is refused as it is (C-46 (6)).
    if (!c->sql("SELECT r.m, r.kk, r.cp, okey.k, idst.h FROM r, okey, idst LIMIT 0")) {
        const int pr = sqlite3_errcode(c->db);
        writerUnpin(e, f);
        if (damaged(pr)) return FLATSQL_E_CORRUPT;
        if (err) *err = f->path + (pr == SQLITE_ERROR ? ": a feed index an earlier format-4 build wrote" : ": unreadable");
        return pr == SQLITE_ERROR ? FLATSQL_E_FORMAT : statusOfSqlite(pr);
    }
    FileMeta m;
    std::map<InstId, InstCount> inst;
    std::map<uint32_t, TokCount> tokc;
    int r = readMeta(c, &m);
    if (r == SQLITE_OK) r = readInst(f, c, &inst);
    if (r == SQLITE_OK) r = readTokc(t, c, &tokc);
    if (r == SQLITE_OK) {
        sqlite3_stmt* q = c->sql("SELECT seq FROM moved");
        if (!q) r = SQLITE_ERROR;
        while (q && (r = sqlite3_step(q)) == SQLITE_ROW) moved->push_back(sqlite3_column_int64(q, 0));
        if (q) sqlite3_reset(q);
        if (r == SQLITE_DONE) r = SQLITE_OK;
    }
    // The staged rows and identities: the view the reads take them from
    // until the indexer merges them.
    int32_t sst = FLATSQL_OK;
    if (r == SQLITE_OK && m.indexed) sst = stagedLoad(e, f, c, err);
    writerUnpin(e, f);
    if (r != SQLITE_OK) {
        if (damaged(r)) return FLATSQL_E_CORRUPT;
        if (err) *err = "read " + f->path + ": " + std::to_string(r);
        return statusOfSqlite(r);
    }
    if (sst != FLATSQL_OK) return sst == FLATSQL_E_CORRUPT ? FLATSQL_E_CORRUPT : sst;
    // The stream: cut back to the committed mark (bytes past it were never
    // acknowledged). A stream shorter than its mark lost acknowledged frames:
    // the feed is quarantined (FLATSQL_E_CORRUPT, named), never silently cut.
    f->gen = m.gen;
    f->mark = m.mark;
    bool quarantine = false;
    if (m.mark > 0 || ioExists(streamPath(f, m.gen))) {
        std::shared_ptr<Stream> sp = streamCur(f, &st);
        if (!sp) {
            if (err) *err = "open " + streamPath(f, m.gen);
            return st;
        }
        const int64_t size = streamSize(sp.get());
        if (size < 0) {
            if (err) *err = "size " + sp->path;
            return FLATSQL_E_IO;
        }
        if (size > m.mark) {
            st = streamTruncate(sp.get(), m.mark, true);
            if (st != FLATSQL_OK) {
                if (err) *err = "cut " + sp->path;
                return st;
            }
        } else if (size < m.mark) {
            quarantine = true;
        }
    }
    // A compaction cut by a crash: the next generation it was writing, or the
    // one it replaced and had not unlinked yet.
    ioUnlink(streamPath(f, m.gen + 1));
    if (m.gen > 0) ioUnlink(streamPath(f, m.gen - 1));
    std::lock_guard<std::mutex> g(t->mu);
    f->k = m.k;
    f->indexed = m.indexed;
    f->inst = std::move(inst);
    f->tokc = std::move(tokc);
    f->end = m.mark;
    f->mark = m.mark;
    f->streamBytes = m.mark;
    f->movedRows = !moved->empty();
    f->movedDone = moved->empty();
    if (quarantine) {
        f->quarantined = true;
        stagedPublish(e, f, nullptr, false);  // never read or merged: its view goes
    }
    if (m.k.maxseq >= t->nextSeq) t->nextSeq = m.k.maxseq + 1;
    return FLATSQL_OK;
}


namespace {
using Digest = std::array<uint8_t,32>;
// Hash every scalar with its SQLite type and byte length. Physical stream
// offsets and staged-index flags are rebuilt; all record/provenance values
// and historical dictionaries/counters must survive an index-loss replay.
int32_t digestQuery(Conn* c, const char* sql, Digest* out) {
    auto* q=c->sql(sql); if(!q) return FLATSQL_E_IO;
    Digest chain{}; int rc;
    while((rc=sqlite3_step(q))==SQLITE_ROW) {
        std::vector<uint8_t> row(chain.begin(),chain.end());
        for(int i=0;i<sqlite3_column_count(q);i++) {
            const int type=sqlite3_column_type(q,i);
            const auto* bytes=static_cast<const uint8_t*>(sqlite3_column_blob(q,i));
            const int n=sqlite3_column_bytes(q,i);
            row.push_back(uint8_t(type));
            uint8_t length[8]; st64(length,uint64_t(n));
            row.insert(row.end(),length,length+8);
            if(n) row.insert(row.end(),bytes,bytes+n);
        }
        ps::sha256(row.data(),row.size(),chain.data());
    }
    sqlite3_reset(q); if(rc!=SQLITE_DONE) return statusOfSqlite(rc);
    *out=chain; return FLATSQL_OK;
}
const char* checks[]={
    "SELECT rid,seq,n,b,c,u,at,cid,e,k,ts,f,x,len,kk FROM r ORDER BY rid",
    "SELECT id,producer,peer FROM node ORDER BY id",
    "SELECT id,batch,ppeer,pkey FROM batch ORDER BY id",
    "SELECT id,ckey FROM ckey ORDER BY id",
    "SELECT id,url FROM url ORDER BY id",
    "SELECT id,k FROM okey ORDER BY id",
    "SELECT * FROM inst ORDER BY b,c",
    "SELECT * FROM tokc ORDER BY producer",
    "SELECT h,seq FROM ident UNION SELECT h,seq FROM idst ORDER BY h,seq",
    "SELECT k,v FROM meta WHERE k IN ('rows','recs','bytes','nnull','rbytes','copies','cbytes','fbytes',"
    "'minseq','maxseq','minw','maxw','mints','maxts','mine','maxe','provider','source','type','fid','layout') ORDER BY k"
};
int32_t snapshot(Engine* e,Feed* f,std::vector<Digest>* out,std::string* err) {
    int32_t rc=FLATSQL_OK; Conn* c=writerPin(e,f,&rc,err); if(!c) return rc;
    out->clear();
    for(const char* query:checks) {
        Digest digest; rc=digestQuery(c,query,&digest);
        if(rc!=FLATSQL_OK) { if(err) *err=std::string("conversion verification query: ")+query; break; }
        out->push_back(digest);
    }
    if(rc==FLATSQL_OK) {
        // Compaction preserves live frame order. Compare the actual record
        // bytes as well as their stored CIDs; checkpoint-assisted recovery
        // can trust committed digests without rehashing every payload.
        auto stream=streamCur(f,&rc);
        auto* q=c->sql("SELECT off,len FROM r GROUP BY off,len ORDER BY off,len");
        if(!stream || !q) rc=FLATSQL_E_IO;
        Digest chain{};
        int sr=SQLITE_DONE;
        while(rc==FLATSQL_OK && (sr=sqlite3_step(q))==SQLITE_ROW) {
            std::string bytes;
            rc=streamRead(stream.get(),sqlite3_column_int64(q,0),sqlite3_column_int64(q,1),&bytes);
            if(rc!=FLATSQL_OK) break;
            Digest payload; ps::sha256(reinterpret_cast<const uint8_t*>(bytes.data()),bytes.size(),payload.data());
            uint8_t joined[72]; std::memcpy(joined,chain.data(),32); std::memcpy(joined+32,payload.data(),32);
            st64(joined+64,bytes.size()); ps::sha256(joined,sizeof joined,chain.data());
        }
        if(q) sqlite3_reset(q);
        if(rc==FLATSQL_OK && sr!=SQLITE_DONE) rc=statusOfSqlite(sr);
        if(rc==FLATSQL_OK) out->push_back(chain);
    }
    writerUnpin(e,f); return rc;
}
int32_t preflight(Engine* e,Feed* f,std::string* err) {
    int32_t rc=FLATSQL_OK; Conn* c=writerPin(e,f,&rc,err); if(!c) return rc;
    const char* sql[]={
        "SELECT v FROM meta WHERE k='format'",
        "SELECT count(*) FROM moved",
        "SELECT count(*) FROM (SELECT h,seq FROM ident UNION SELECT h,seq FROM idst) i "
            "WHERE NOT EXISTS (SELECT 1 FROM r WHERE r.seq=i.seq)",
        "SELECT count(*) FROM (SELECT seq FROM (SELECT h,seq FROM ident UNION SELECT h,seq FROM idst) "
            "GROUP BY seq HAVING count(*)>1)"
    };
    for(size_t i=0;i<4;i++) {
        auto* q=c->sql(sql[i]); const int sr=q?sqlite3_step(q):SQLITE_ERROR;
        if(sr!=SQLITE_ROW || sqlite3_column_int64(q,0)!=(i==0?4:0)) {
            if(err) *err="conversion refuses incomplete moves, unsupported identity history, or a non-format-4 feed: "+f->path;
            rc=FLATSQL_E_FORMAT;
        }
        if(q) sqlite3_reset(q);
        if(rc!=FLATSQL_OK) break;
    }
    // Consolidation looks up identity by sequence for each retained row.
    // These temporary acceleration indexes disappear with the disposable
    // candidate index during the mandatory stream-only verification.
    if(rc==FLATSQL_OK) rc=statusOfSqlite(c->exec(
        "CREATE INDEX IF NOT EXISTS migration_ident_seq ON ident(seq);"
        "CREATE INDEX IF NOT EXISTS migration_idst_seq ON idst(seq);"
        "CREATE INDEX IF NOT EXISTS migration_frame_off ON r(off,len)"));
    writerUnpin(e,f); return rc;
}
}
int32_t convertFormat4Type(Type* t,std::string* err) {
    Engine* e=t->e; std::vector<Feed*> feeds;
    for(auto& f:t->feeds) if(f->provider.rfind("\x1f#",0)!=0) feeds.push_back(f.get());
    // Load every feed before rewriting one: the type sequence high includes
    // every feed, including a higher sequence in a later feed.
    for(Feed* f:feeds) {
        if(!ioExists(f->path)) {
            std::vector<int64_t> moved;
            const int32_t rc=format4FeedOpen(t,f,&moved,err);
            if(rc!=FLATSQL_OK) { if(err) *err="conversion requires the format-4 index for its stream: "+f->path; return rc; }
            continue; // a registered feed never written has no file or stream
        }
        f->created=true;
        int32_t rc=preflight(e,f,err); if(rc!=FLATSQL_OK) return rc;
        std::vector<int64_t> moved;
        rc=format4FeedOpen(t,f,&moved,err); if(rc!=FLATSQL_OK) return rc;
        if(f->quarantined) { if(err) *err="conversion refuses a truncated stream: "+f->path; return FLATSQL_E_CORRUPT; }
    }
    for(Feed* f:feeds) {
        if(!f->created) continue;
        std::fprintf(stderr,"store conversion 4 -> 5: %s/%s rows=%lld\n",t->name.c_str(),f->name.c_str(),(long long)f->k.rows);
        std::fflush(stderr);
        std::vector<Digest> before,after;
        int32_t rc=snapshot(e,f,&before,err); if(rc!=FLATSQL_OK) return rc;
        std::fprintf(stderr,"store conversion 4 -> 5: rewriting %s/%s\n",t->name.c_str(),f->name.c_str());
        std::fflush(stderr);
        Internal work; work.force=true; work.what=f->fid;
        compactWork(e,t,&work);
        if(work.status!=FLATSQL_OK) { if(err) *err=work.err; return work.status; }
        // Conversion has no published ranges or concurrent readers. The
        // committed checkpoint retains its root; release this feed's derived
        // DAG inventory before replay and before converting the next feed.
        feedDagReset(f);
        // Recreate the disposable index entirely from the new stream. This
        // proves the new metadata is authoritative, not a relabeled cache.
        std::fprintf(stderr,"store conversion 4 -> 5: replaying %s/%s\n",t->name.c_str(),f->name.c_str());
        std::fflush(stderr);
        int64_t indexed=0;
        rc=replayRebuildFeed(e,t,f,&indexed,err); if(rc!=FLATSQL_OK) return rc;
        std::fprintf(stderr,"store conversion 4 -> 5: comparing %s/%s\n",t->name.c_str(),f->name.c_str());
        std::fflush(stderr);
        rc=snapshot(e,f,&after,err); if(rc!=FLATSQL_OK) return rc;
        if(before!=after) {
            size_t i=0; while(i<before.size() && i<after.size() && before[i]==after[i]) i++;
            if(err) *err="conversion preservation mismatch in "+f->path+" check "+std::to_string(i);
            return FLATSQL_E_CORRUPT;
        }
        std::fprintf(stderr,"store conversion 4 -> 5: verified %s/%s\n",t->name.c_str(),f->name.c_str());
        std::fflush(stderr);
    }
    return FLATSQL_OK;
}
} }
