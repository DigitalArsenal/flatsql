// Checkpoint preparation off the writer (FORMAT.md 5, 6). One thread folds a
// due feed from one SQLite read snapshot and the stream prefix it names; the
// feed's appender installs the plan as a short step at a round boundary, so
// no PUT waits for a fold. A plan whose base changed is dropped, and the feed
// is folded again while it stays due.
#include "checkpoint.h"
#include "dag.h"

namespace flatsql { namespace p4 {
std::function<void(Feed*)> checkpointPreparedHook;

namespace {
int readCommit(Conn* c,uint64_t* previous) {
    auto* q=c->sql("SELECT v FROM meta WHERE k='commit'"); if(!q) return SQLITE_ERROR;
    const int rc=sqlite3_step(q);
    if(rc==SQLITE_ROW) *previous=uint64_t(sqlite3_column_int64(q,0));
    sqlite3_reset(q); return rc==SQLITE_ROW?SQLITE_OK:rc==SQLITE_DONE?SQLITE_CORRUPT:rc;
}
int32_t prepare(Feed* f,std::shared_ptr<CheckpointPlan>* out,std::string* err) {
    Engine* e=f->type->e; auto plan=std::make_shared<CheckpointPlan>();
    { std::lock_guard<std::mutex> g(f->type->mu); if(!f->created || f->quarantined) return P4_E_BUSY; plan->base=f->checkpoint; }
    int32_t rc=P4_OK; plan->stream=streamCur(f,&rc); if(!plan->stream) return rc;
    int sql=SQLITE_OK; Conn* c=e->rpool.acquire(f->path,OpenKind::Reader,&sql,err); if(!c) return statusOfSqlite(sql);
    FileMeta meta; uint64_t previous=0; std::string commitsRoot;
    {
        // With dagMu held no DAG build passes the mark this snapshot reads.
        std::lock_guard<std::mutex> dag(f->dagMu);
        sql=c->exec("BEGIN");
        if(sql==SQLITE_OK) sql=readMeta(c,&meta);
        if(sql==SQLITE_OK) sql=readCommit(c,&previous);
        rc=sql==SQLITE_OK?P4_OK:statusOfSqlite(sql);
        if(rc==P4_OK && (meta.gen!=plan->stream->gen || meta.mark<12 ||
           (plan->base && uint64_t(meta.mark)<=plan->base->end))) rc=P4_E_BUSY;  // compacted, or nothing new
        if(rc==P4_OK) rc=feedDagCommitRootHeld(f,plan->stream.get(),previous,uint64_t(meta.mark),&commitsRoot,err);
    }
    if(rc==P4_OK && checkpointPreparedHook) checkpointPreparedHook(f);
    if(rc==P4_OK) rc=checkpointPrepare(f,c,plan->stream.get(),previous,uint64_t(meta.mark),meta.k,
                                       plan->base?*plan->base:CheckpointState(),false,commitsRoot,plan.get(),err);
    c->exec("ROLLBACK"); e->rpool.release(c);
    if(rc==P4_OK) *out=std::move(plan);
    return rc;
}
} // namespace

void checkpointSchedule(Feed* f) {
    Engine* e=f->type->e;
    { std::lock_guard<std::mutex> g(f->type->mu);
      if(f->checkpointPreparing || f->checkpointPlan || !f->created || f->quarantined || monoNs()<f->checkpointRetryAt) return;
      f->checkpointPreparing=true; }
    bool queued;
    { std::lock_guard<std::mutex> g(e->ckMu); queued=!e->ckStop; if(queued) e->ckQ.push_back(f); }
    if(queued) e->ckCv.notify_one();
    else { std::lock_guard<std::mutex> g(f->type->mu); f->checkpointPreparing=false; }
}

void fsgVerifySchedule(Feed* f) {
    Engine* e=f->type->e;
    { std::lock_guard<std::mutex> g(f->fsgMu); if(f->fsgVerifying) return; f->fsgVerifying=true; }
    bool queued;
    { std::lock_guard<std::mutex> g(e->ckMu); queued=!e->ckStop; if(queued) e->fsgQ.push_back(f); }
    if(queued) e->ckCv.notify_one();
    else { std::lock_guard<std::mutex> g(f->fsgMu); f->fsgVerifying=false; }
}

namespace {
// The feed's current checkpoint's row-order segments, each hashed once in
// this process (checkpointVerifyPairs); a failure leaves the readers on the
// SQLite index and is found again by the next reader's schedule.
void verifyFeed(Feed* f) {
    std::shared_ptr<const CheckpointState> ck;
    { std::lock_guard<std::mutex> g(f->type->mu); ck=f->checkpoint; }
    int32_t st=P4_OK; auto stream=ck?streamCur(f,&st):nullptr;
    if(stream && st==P4_OK)
        for(const auto& d:ck->segments) if(d.kind>=kFsgSeq) { std::shared_ptr<const FsgVerified> v; if(checkpointVerifyPairs(f,stream.get(),d,&v)!=P4_OK) break; }
    std::lock_guard<std::mutex> g(f->fsgMu); f->fsgVerifying=false;
}
}

void checkpointLoop(Engine* e) {
    for(;;) {
        Feed* f; bool verify=false;
        { std::unique_lock<std::mutex> g(e->ckMu);
          e->ckCv.wait(g,[&]{ return e->ckStop || !e->ckQ.empty() || !e->fsgQ.empty(); });
          if(e->ckStop) break;
          // A checkpoint to prepare goes first.
          if(e->ckQ.empty()) { f=e->fsgQ.front(); e->fsgQ.pop_front(); verify=true; }
          else { f=e->ckQ.front(); e->ckQ.pop_front(); } }
        if(verify) { verifyFeed(f); continue; }
        std::shared_ptr<CheckpointPlan> plan; std::string err;
        const int32_t rc=prepare(f,&plan,&err);
        { std::lock_guard<std::mutex> g(f->type->mu);
          f->checkpointPreparing=false;
          if(rc==P4_OK) f->checkpointPlan=std::move(plan); else f->checkpointRetryAt=monoNs()+1000000000ull; }
        if(rc==P4_OK) e->wake(e->firstOfClass[1]+f->type->owner);
    }
}

void checkpointWorkerStop(Engine* e) {
    std::deque<Feed*> left;
    std::deque<Feed*> verifying;
    { std::lock_guard<std::mutex> g(e->ckMu); e->ckStop=true; left.swap(e->ckQ); verifying.swap(e->fsgQ); }
    e->ckCv.notify_all();
    if(e->ckThread.joinable()) e->ckThread.join();
    for(Feed* f:left) { std::lock_guard<std::mutex> g(f->type->mu); f->checkpointPreparing=false; }
    for(Feed* f:verifying) { std::lock_guard<std::mutex> g(f->fsgMu); f->fsgVerifying=false; }
}

int32_t checkpointInstallReady(Feed* f) {
    std::shared_ptr<CheckpointPlan> plan; int64_t end; uint64_t previous;
    { std::lock_guard<std::mutex> g(f->type->mu);
      if(!f->checkpointPlan) return P4_OK;
      plan=std::move(f->checkpointPlan);
      if(f->quarantined || f->pendingCheckpoint || f->checkpoint!=plan->base || f->end!=f->mark) return P4_OK;  // stale: dropped
      end=f->end; previous=f->lastCommit; }
    int32_t rc=P4_OK; std::shared_ptr<Stream> stream=streamCur(f,&rc); if(!stream) return rc;
    if(stream!=plan->stream || uint64_t(end)<plan->at) return P4_OK;
    CheckpointState state; const int64_t start=end;
    rc=checkpointInstall(f,stream.get(),*plan,&end,&previous,&state);
    if(rc!=P4_OK) { streamTruncate(stream.get(),start,false); return rc; }
    std::lock_guard<std::mutex> g(f->type->mu);
    f->pendingCheckpoint=std::make_shared<CheckpointState>(std::move(state)); f->end=end;
    return P4_OK;
}
} }
