// FlatSQL partition store: the per-writer commit journal (A8 fallback,
// §22.4 ruling 5). With EngineConfig::commitJournal a commit round writes its
// d/l/m/g/fence/type-m bytes to their files as usual, appends one record with
// the same bytes to the writer's journal, and fsyncs only the journal: one
// fdatasync per writer iteration, whatever the number of dirty partitions.
//
// Checkpoint: the writer switches to its other journal file and a maintenance
// helper fsyncs every file the retired file's records touched (plus their
// heads), then truncates the retired file. Open replays both files of every
// writer (records in seq order) into the files, fsyncs them, truncates the
// journals, and only then runs the normal durable-tail open (A4).
//
// A failed journal write or sync is never retried (fsyncgate): the round's
// partitions are quarantined and the writer commits nothing more.
#include <algorithm>
#include <cstring>
#include <map>
#include <set>

#include "internal.h"

namespace flatsql {
namespace ps {

namespace {

constexpr int32_t kOpenRW = FLATSQL_IO_READ | FLATSQL_IO_WRITE;
constexpr uint32_t kJournalOwnerKind = 5;

size_t pad8z(size_t n) { return (n + 7) & ~size_t(7); }

void journalTargetPath(PathBuf* out, const char* root, uint8_t file, uint32_t id, uint32_t seg) {
    uint8_t fid[4];
    std::memcpy(fid, &id, 4);
    switch (file) {
        case kJrnData: pathPartitionSeg(out, root, id, 'd', seg, "fsd"); break;
        case kJrnMeta: pathPartitionSeg(out, root, id, 'm', seg, "fsl"); break;
        case kJrnLanes: pathPartition(out, root, id, "l.fsl"); break;
        case kJrnArrivals: pathTypeSeg(out, root, fid, 'g', seg, "fsg"); break;
        case kJrnFence: pathType(out, root, fid, kArrivalFenceName); break;
        case kJrnTypeMeta: pathTypeSeg(out, root, fid, 'm', seg, "fsl"); break;
        default: out->len = 0; out->buf[0] = 0; break;
    }
}

FileClass journalTargetClass(uint8_t file) {
    switch (file) {
        case kJrnData: return FileClass::Data;
        case kJrnMeta: return FileClass::Meta;
        case kJrnLanes: return FileClass::Lanes;
        case kJrnArrivals:
        case kJrnFence: return FileClass::Arrivals;
        default: return FileClass::TypeMeta;
    }
}

uint32_t fidId(const uint8_t fid[4]) {
    uint32_t id;
    std::memcpy(&id, fid, 4);
    return id;
}

}  // namespace

// ---------------------------------------------------------------------------
// Writer side
// ---------------------------------------------------------------------------
int32_t Writer::journalOpen() {
    if (jf_[0].valid() && jf_[1].valid()) return 0;
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;  // once per writer
    int32_t rc = 0;
    for (int f = 0; f < 2 && rc >= 0; f++) {
        if (jf_[f].valid()) continue;
        PathBuf jp;
        pathJournal(&jp, eng_root(), id_, f);
        rc = io_.open(jp.c_str(), jp.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS,
                      FileClass::Journal, &jf_[f]);
        if (rc >= 0) {
            // Open replayed and truncated every journal; a non-empty file here
            // belongs to this run (never the case at first open).
            const int64_t sz = io_.size(jf_[f]);
            jExtent_[f] = sz > 0 ? uint64_t(sz) : 0;
            jEnd_[f] = 0;
        }
    }
    if (jStage_.capacity() < (64u << 10)) jStage_.reserve(64u << 10);
    if (jParts_.capacity() < 8192) jParts_.reserve(8192);
    if (jTypes_.capacity() < 1024) jTypes_.reserve(1024);
    tHotPathDepth = saved;
    return rc;
}

void Writer::journalAppendRound() {
    jRoundFailed_ = false;
    if (jFailed_ || journalOpen() < 0) {
        jFailed_ = true;
        jRoundFailed_ = true;
        return;
    }
    // Size the record.
    uint32_t nParts = 0;
    size_t bytes = sizeof(JournalRecHeader) + sizeof(JournalTrailer);
    auto count = [&](size_t len) {
        nParts++;
        bytes += sizeof(JournalPart) + pad8z(len);
    };
    for (Partition* p : dirty_) {
        Staged* st = p->st;
        if (!st->batch || st->err) continue;
        if (st->dBytes) count(st->dBytes);
        if (st->laneFrameBytes) count(st->laneFrameBytes);
        count(st->batchLen);
    }
    for (TypeOwner* t : dirtyTypes_) {
        StagedType* st = t->st;
        if (st->err) continue;
        if (st->nArrivals) count(size_t(st->nArrivals) * kArrivalBytes);
        if (st->gSeal) count(sizeof(ArrivalFence));
        count(st->batchLen);
    }
    if (nParts == 0) return;
    const EngineConfig& cfg = eng_->config();
    FileRef& jf = jf_[jcur_];
    const uint64_t start = jEnd_[jcur_];
    int32_t rc = ensureExtent(&io_, jf, &jExtent_[jcur_], start + bytes, cfg.zeroFillStep);
    uint64_t off = start;
    uint32_t crc = 0;
    jStage_.clear();
    uint64_t stageOff = off;
    auto flush = [&]() {
        if (rc >= 0 && !jStage_.empty()) rc = io_.write(jf, jStage_.data(), jStage_.size(), stageOff);
        stageOff += jStage_.size();
        jStage_.clear();
    };
    auto put = [&](const void* src, size_t n, bool inCrc) {
        if (inCrc) crc = crc32c(crc, src, n);
        if (n > 4096) {
            flush();
            if (rc >= 0) rc = io_.write(jf, src, n, stageOff);
            stageOff += n;
        } else {
            if (jStage_.size() + n > jStage_.capacity()) flush();
            const uint8_t* b = static_cast<const uint8_t*>(src);
            jStage_.insert(jStage_.end(), b, b + n);
        }
        off += n;
    };
    static const uint8_t kZeros[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    auto part = [&](uint8_t file, uint32_t id, uint32_t seg, uint64_t at, const void* src, size_t len) {
        JournalPart jp{};
        jp.file = file;
        jp.len = uint32_t(len);
        jp.id = id;
        jp.seg = seg;
        jp.off = at;
        put(&jp, sizeof(jp), true);
        put(src, len, true);
        if (pad8z(len) != len) put(kZeros, pad8z(len) - len, true);
    };
    JournalRecHeader h{};
    h.magic = kMagicJournal;
    h.ver = 1;
    h.len = uint32_t(bytes);
    h.nParts = nParts;
    h.seq = ++jSeq_;
    h.incarnation = eng_->incarnation();
    h.writer = id_;
    put(&h, sizeof(h), true);
    for (Partition* p : dirty_) {
        Staged* st = p->st;
        if (!st->batch || st->err) continue;
        if (st->dBytes) part(kJrnData, p->pid, st->dSeg, st->dOff, st->frames, st->dBytes);
        if (st->laneFrameBytes) part(kJrnLanes, p->pid, 0, st->lOff, st->laneFrames, st->laneFrameBytes);
        part(kJrnMeta, p->pid, st->mSeg, st->mOff, st->batch, st->batchLen);
    }
    for (TypeOwner* t : dirtyTypes_) {
        StagedType* st = t->st;
        if (st->err) continue;
        const uint32_t id = fidId(t->fid);
        if (st->nArrivals)
            part(kJrnArrivals, id, st->gSeg, st->gOff, st->arrivals, size_t(st->nArrivals) * kArrivalBytes);
        if (st->gSeal) part(kJrnFence, id, 0, st->fenceOff, &st->fence, sizeof(st->fence));
        part(kJrnTypeMeta, id, st->mSeg, st->mOff, st->batch, st->batchLen);
    }
    JournalTrailer tr{};
    tr.crc = crc;
    tr.magic = kMagicJournalEnd;
    put(&tr, sizeof(tr), false);
    flush();
    if (rc < 0) {
        jFailed_ = true;
        jRoundFailed_ = true;
        return;
    }
    jEnd_[jcur_] = start + bytes;
    jRecords_.fetch_add(1, std::memory_order_relaxed);
    jBytes_.fetch_add(bytes, std::memory_order_relaxed);
    queueSync(jf, this, kJournalOwnerKind);
}

void Writer::journalTrack() {
    // Files this round touched join the current checkpoint epoch's list.
    const uint64_t tag = (uint64_t(id_) << 32) | jEpoch_;
    for (Partition* p : dirty_) {
        Staged* st = p->st;
        if (!st->batch || st->err) continue;
        const uint32_t lo = std::min(st->dSeg, st->mSeg);
        const uint32_t hi = std::max(st->dSeg, st->mSeg);
        if (p->jCkptTag == tag && p->jCkptIdx < jParts_.size() && jParts_[p->jCkptIdx].pid == p->pid) {
            JEntry& e = jParts_[p->jCkptIdx];
            e.lo = std::min(e.lo, lo);
            e.hi = std::max(e.hi, hi);
            continue;
        }
        p->jCkptTag = tag;
        p->jCkptIdx = uint32_t(jParts_.size());
        JEntry e;
        e.pid = p->pid;
        e.lo = lo;
        e.hi = hi;
        jParts_.push_back(e);
    }
    for (TypeOwner* t : dirtyTypes_) {
        StagedType* st = t->st;
        if (st->err) continue;
        const uint32_t glo = st->gSeal ? st->gSeg - 1 : st->gSeg;
        if (t->jCkptTag == tag && t->jCkptIdx < jTypes_.size() &&
            std::memcmp(jTypes_[t->jCkptIdx].fid, t->fid, 4) == 0) {
            JEntry& e = jTypes_[t->jCkptIdx];
            e.lo = std::min(e.lo, glo);
            e.hi = std::max(e.hi, st->gSeg);
            e.mLo = std::min(e.mLo, st->mSeg);
            e.mHi = std::max(e.mHi, st->mSeg);
            continue;
        }
        t->jCkptTag = tag;
        t->jCkptIdx = uint32_t(jTypes_.size());
        JEntry e;
        std::memcpy(e.fid, t->fid, 4);
        e.lo = glo;
        e.hi = st->gSeg;
        e.mLo = e.mHi = st->mSeg;
        jTypes_.push_back(e);
    }
}

void Writer::journalCheckpointPaths(std::vector<std::string>* paths) {
    const char* root = eng_root();
    PathBuf pb;
    auto add = [&]() { paths->emplace_back(pb.c_str(), pb.len); };
    for (const JEntry& e : jParts_) {
        for (uint32_t seg = e.lo; seg <= e.hi; seg++) {
            pathPartitionSeg(&pb, root, e.pid, 'd', seg, "fsd");
            add();
            pathPartitionSeg(&pb, root, e.pid, 'm', seg, "fsl");
            add();
        }
        pathPartition(&pb, root, e.pid, "l.fsl");
        add();
        pathPartition(&pb, root, e.pid, "h.fsh");
        add();
    }
    for (const JEntry& e : jTypes_) {
        for (uint32_t seg = e.lo; seg <= e.hi; seg++) {
            pathTypeSeg(&pb, root, e.fid, 'g', seg, "fsg");
            add();
        }
        pathType(&pb, root, e.fid, kArrivalFenceName);
        add();
        for (uint32_t seg = e.mLo; seg <= e.mHi; seg++) {
            pathTypeSeg(&pb, root, e.fid, 'm', seg, "fsl");
            add();
        }
        pathType(&pb, root, e.fid, "h.fsh");
        add();
    }
    jParts_.clear();
    jTypes_.clear();
    jEpoch_++;
}

void Writer::journalMaintenance(bool final) {
    const EngineConfig& cfg = eng_->config();
    if (!jf_[0].valid()) return;
    auto settle = [&](bool wait) {
        if (!jCkptInFlight_) return;
        int32_t r = jCkptResult_->load(std::memory_order_acquire);
        while (r == 0 && wait) {
            sleepNs(1000000);
            r = jCkptResult_->load(std::memory_order_acquire);
        }
        if (r == 0) return;
        jCkptInFlight_ = false;
        if (r < 0) {
            jFailed_ = true;  // the retired file stays; open replays it
            return;
        }
        jEnd_[jCkptRetire_] = 0;
        jExtent_[jCkptRetire_] = 0;
        jCheckpoints_.fetch_add(1, std::memory_order_relaxed);
    };
    settle(final);
    if (jCkptInFlight_ || jFailed_) return;
    const uint64_t now = monoNs();
    const bool pending = jEnd_[jcur_] > 0 || !jParts_.empty() || !jTypes_.empty();
    const bool due = final ? pending
                           : (jEnd_[jcur_] >= cfg.journalCkptBytes ||
                              (pending && now - jLastCkptNs_ >= uint64_t(cfg.journalCkptMs) * 1000000ull));
    if (!due) return;
    std::vector<std::string> paths;
    journalCheckpointPaths(&paths);
    jCkptRetire_ = jcur_;
    jcur_ = 1 - jcur_;
    jEnd_[jcur_] = 0;
    PathBuf jp;
    pathJournal(&jp, eng_root(), id_, jCkptRetire_);
    std::string jpath(jp.c_str(), jp.len);
    auto result = std::make_shared<std::atomic<int32_t>>(0);
    jCkptResult_ = result;
    jCkptInFlight_ = true;
    jLastCkptNs_ = now;
    auto job = [paths = std::move(paths), jpath, result](IoCtx* io) {
        int32_t rc = 0;
        for (const std::string& p : paths) {
            FileRef f;
            const int32_t o = io->open(p.c_str(), p.size(), FLATSQL_IO_READ | FLATSQL_IO_WRITE, FileClass::Directory, &f);
            if (o == FLATSQL_IO_ERR_NOENT) continue;
            if (o < 0) {
                rc = o;
                break;
            }
            const int32_t s = io->sync(f);
            io->close(&f);
            if (s < 0) {
                rc = s;
                break;
            }
        }
        if (rc >= 0) {
            FileRef jf;
            rc = io->open(jpath.c_str(), jpath.size(), FLATSQL_IO_READ | FLATSQL_IO_WRITE, FileClass::Journal, &jf);
            if (rc >= 0) {
                rc = io->truncate(jf, 0);
                if (rc >= 0) rc = io->sync(jf);
                io->close(&jf);
            }
        }
        result->store(rc < 0 ? rc : 1, std::memory_order_release);
    };
    const int saved = tHotPathDepth;
    tHotPathDepth = 0;
    if (final || cfg.cooperative || cfg.mergeHelpers == 0) {
        job(&io_);
        settle(true);
    } else {
        eng_->submitMaintenance(std::move(job));
    }
    tHotPathDepth = saved;
    if (final) {
        // The other file may hold this epoch's tail too: checkpoint it as well.
        if (jEnd_[jcur_] > 0 || !jParts_.empty() || !jTypes_.empty()) journalMaintenance(true);
    }
}

// ---------------------------------------------------------------------------
// Open: replay every writer's journals into the files (A4 order: the files
// are durable before any head is read).
// ---------------------------------------------------------------------------
int32_t Engine::replayJournals(std::string* err) {
    IoCtx* io = openIo_;
    const char* root = cfg_.root.c_str();
    std::map<std::string, FileRef> touched;
    uint64_t records = 0;
    int32_t rc = 0;
    std::vector<std::pair<std::string, FileRef>> journals;
    for (uint32_t w = 0; w < 256 && rc >= 0; w++) {
        struct Rec {
            uint64_t seq;
            int file;
            size_t off;
            uint32_t len;
        };
        std::vector<uint8_t> data[2];
        std::vector<Rec> recs;
        bool any = false;
        for (int f = 0; f < 2; f++) {
            PathBuf jp;
            pathJournal(&jp, root, w, f);
            if (io->probe(jp.c_str(), jp.len) != 0) continue;
            any = true;
            FileRef jf;
            rc = io->open(jp.c_str(), jp.len, FLATSQL_IO_READ | FLATSQL_IO_WRITE, FileClass::Journal, &jf);
            if (rc < 0) break;
            const int64_t size = io->size(jf);
            if (size > 0) {
                data[f].resize(size_t(size));
                if (io->read(jf, data[f].data(), data[f].size(), 0) != size) {
                    io->close(&jf);
                    rc = FLATSQL_IO_ERR_IO;
                    break;
                }
            }
            journals.emplace_back(std::string(jp.c_str(), jp.len), jf);
            size_t off = 0;
            const std::vector<uint8_t>& d = data[f];
            while (off + sizeof(JournalRecHeader) + sizeof(JournalTrailer) <= d.size()) {
                JournalRecHeader h;
                std::memcpy(&h, d.data() + off, sizeof(h));
                if (h.magic != kMagicJournal || h.ver != 1 || (h.len & 7) ||
                    h.len < sizeof(h) + sizeof(JournalTrailer) || off + h.len > d.size())
                    break;
                JournalTrailer tr;
                std::memcpy(&tr, d.data() + off + h.len - sizeof(tr), sizeof(tr));
                if (tr.magic != kMagicJournalEnd || tr.crc != crc32c(d.data() + off, h.len - sizeof(tr))) break;
                recs.push_back({h.seq, f, off, h.len});
                off += h.len;
            }
        }
        if (rc < 0) break;
        // A writer that never committed has no journal; the writer count may
        // also have shrunk since the files were written: probe every id.
        if (!any) continue;
        std::sort(recs.begin(), recs.end(), [](const Rec& a, const Rec& b) { return a.seq < b.seq; });
        for (const Rec& r : recs) {
            const uint8_t* b = data[r.file].data() + r.off;
            JournalRecHeader h;
            std::memcpy(&h, b, sizeof(h));
            size_t at = sizeof(h);
            for (uint32_t i = 0; i < h.nParts && rc >= 0; i++) {
                JournalPart jp;
                if (at + sizeof(jp) > r.len) {
                    rc = FLATSQL_IO_ERR_IO;
                    break;
                }
                std::memcpy(&jp, b + at, sizeof(jp));
                at += sizeof(jp);
                if (at + pad8z(jp.len) > r.len - sizeof(JournalTrailer)) {
                    rc = FLATSQL_IO_ERR_IO;
                    break;
                }
                PathBuf tp;
                journalTargetPath(&tp, root, jp.file, jp.id, jp.seg);
                if (!tp.len) {
                    rc = FLATSQL_IO_ERR_IO;
                    break;
                }
                const std::string key(tp.c_str(), tp.len);
                auto it = touched.find(key);
                if (it == touched.end()) {
                    FileRef f;
                    rc = io->open(tp.c_str(), tp.len, kOpenRW | FLATSQL_IO_CREATE | FLATSQL_IO_CREATE_PARENTS,
                                  journalTargetClass(jp.file), &f);
                    if (rc < 0) break;
                    it = touched.emplace(key, f).first;
                }
                rc = io->write(it->second, b + at, jp.len, jp.off);
                if (jp.file == kJrnMeta || jp.file == kJrnTypeMeta) journalMeta_.emplace(jp.file, jp.id, jp.seg, jp.off);
                at += pad8z(jp.len);
            }
            if (rc < 0) break;
            records++;
        }
    }
    // The replayed bytes are durable before the journals are emptied.
    for (auto& kv : touched) {
        if (rc >= 0) rc = io->sync(kv.second);
        io->close(&kv.second);
    }
    for (auto& jf : journals) {
        if (rc >= 0) rc = io->truncate(jf.second, 0);
        if (rc >= 0) rc = io->sync(jf.second);
        io->close(&jf.second);
    }
    if (rc < 0) {
        if (err) *err = "commit journal replay failed";
        return rc;
    }
    journalReplayRecords = records;
    return 0;
}

bool Engine::journalReplayed(uint8_t file, uint32_t id, uint32_t seg, uint64_t off) const {
    return journalMeta_.count(std::make_tuple(file, id, seg, off)) != 0;
}

}  // namespace ps
}  // namespace flatsql
