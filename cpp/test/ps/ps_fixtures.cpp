// Partition store test fixtures: test types (schemas compiled at run time by
// the flatbuffers parser), a reflection-driven record builder, the store
// fixture and an independent on-disk inspector. Shared by the tests and the
// benchmark driver.
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/idl.h>
#include <flatbuffers/reflection.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>

#include "flatsql/ps/platform.h"
#include "ps/ps_test.h"

namespace pst {

// ---------------------------------------------------------------------------
// Test types
// ---------------------------------------------------------------------------
namespace {

const char* kOmmSchema = R"(
namespace sds;
table OMM {
  OBJECT_NAME: string;
  OBJECT_ID: string;
  NORAD_CAT_ID: uint;
  EPOCH: string;
  CREATION_DATE: string;
  MEAN_MOTION: double;
  ECCENTRICITY: double;
  INCLINATION: double;
  RA_OF_ASC_NODE: double;
  ARG_OF_PERICENTER: double;
  MEAN_ANOMALY: double;
  BSTAR: double;
  COMMENT: string;
}
root_type OMM;
file_identifier "$OMM";
)";

const char* kMpeSchema = R"(
namespace sds;
table MPE {
  ENTITY_ID: string;
  EPOCH: double;
  MEAN_MOTION: double;
  ECCENTRICITY: double;
  INCLINATION: double;
  RA_OF_ASC_NODE: double;
  ARG_OF_PERICENTER: double;
  MEAN_ANOMALY: double;
  BSTAR: double;
  COMMENT: string;
}
root_type MPE;
file_identifier "$MPE";
)";

const char* kCatSchema = R"(
namespace sds;
enum objectType : byte { PAYLOAD = 0, ROCKET_BODY = 1, DEBRIS = 2, UNKNOWN = 3 }
enum opsStatusCode : byte { OPERATIONAL = 0, NONOPERATIONAL = 1, UNKNOWN = 2 }
table CAT {
  OBJECT_NAME: string;
  OBJECT_ID: string;
  NORAD_CAT_ID: uint;
  OBJECT_TYPE: objectType = UNKNOWN;
  OPS_STATUS_CODE: opsStatusCode = UNKNOWN;
  OWNER: string;
  LAUNCH_DATE: string;
  MASS: double;
  CATALOG_URI: string;
  CATALOG_OBJECT_ID: string;
}
root_type CAT;
file_identifier "$CAT";
)";

const char* kIqcSchema = R"(
namespace sds;
table IQC {
  ENTITY_ID: string;
  EPOCH: string;
  SENSOR: string;
  IMAGE: [ubyte];
}
root_type IQC;
file_identifier "$IQC";
)";

TestType makeType(const char* schemaText, const char* name, const char* fid, const std::string& rules,
                  uint64_t maxFrame, uint64_t ringCap) {
    TestType t;
    t.name = name;
    std::memcpy(t.fid, fid, 4);
    t.rules = rules;
    t.maxFrame = maxFrame;
    t.ringCap = ringCap;
    if (schemaText) {
        flatbuffers::Parser parser;
        if (!parser.Parse(schemaText)) {
            std::fprintf(stderr, "schema parse failed: %s\n", parser.error_.c_str());
            std::abort();
        }
        parser.Serialize();
        t.bfbs.assign(parser.builder_.GetBufferPointer(),
                      parser.builder_.GetBufferPointer() + parser.builder_.GetSize());
    }
    const uint32_t flags = schemaText ? (TypeConfig::kVerifyBfbs | TypeConfig::kVerifyCid)
                                      : uint32_t(TypeConfig::kControl);
    t.config = TypeConfig::build(t.name, t.fid, t.bfbs, t.rules, t.maxFrame, t.ringCap, flags);
    if (!t.bfbs.empty()) t.schema = reflection::GetSchema(t.bfbs.data());
    return t;
}

}  // namespace

TestType& ommType() {
    static TestType t = makeType(kOmmSchema, "OMM.fbs", "$OMM",
                                 "epoch str:EPOCH|str:CREATION_DATE\n"
                                 "col 0 u64pos:NORAD_CAT_ID\n"
                                 "col 1 str:OBJECT_ID\n"
                                 "epoch_day 4\n"
                                 "object 0,1\n",
                                 16u << 20, 4u << 20);
    return t;
}
TestType& mpeType() {
    static TestType t = makeType(kMpeSchema, "MPE.fbs", "$MPE",
                                 "epoch f64floor:EPOCH\n"
                                 "col 1 str:ENTITY_ID\n"
                                 "epoch_day 4\n"
                                 "object 1\n",
                                 16u << 20, 4u << 20);
    return t;
}
TestType& catType() {
    static TestType t = makeType(kCatSchema, "CAT.fbs", "$CAT",
                                 "col 0 u64pos:NORAD_CAT_ID\n"
                                 "col 1 str:OBJECT_ID\n"
                                 "col 2 enum:OBJECT_TYPE\n"
                                 "col 3 enum:OPS_STATUS_CODE\n"
                                 "supersede pair:uri:CATALOG_URI,CATALOG_OBJECT_ID|u64:norad:NORAD_CAT_ID|str:object:OBJECT_ID\n"
                                 "object 0,1\n",
                                 16u << 20, 4u << 20);
    return t;
}
TestType& iqcType() {
    static TestType t = makeType(kIqcSchema, "IQC.fbs", "$IQC",
                                 "epoch str:EPOCH\n"
                                 "col 1 str:ENTITY_ID\n"
                                 "object 1\n",
                                 64u << 20, 64u << 20);
    return t;
}
TestType& ctlType() {
    static TestType t = makeType(nullptr, "CTL", "$CTL", "", 1u << 20, 4u << 20);
    return t;
}

TestType makeTypeVariant(int base, const char* fid, const std::string& name) {
    const char* texts[4] = {kOmmSchema, kMpeSchema, kCatSchema, kIqcSchema};
    const char* ids[4] = {"$OMM", "$MPE", "$CAT", "$IQC"};
    const TestType* bases[4] = {&ommType(), &mpeType(), &catType(), &iqcType()};
    std::string text = texts[base];
    const std::string from = std::string("file_identifier \"") + ids[base] + "\"";
    const std::string to = std::string("file_identifier \"") + std::string(fid, 4) + "\"";
    text.replace(text.find(from), from.size(), to);
    return makeType(text.c_str(), name.c_str(), fid, bases[base]->rules, bases[base]->maxFrame,
                    bases[base]->ringCap);
}

std::vector<uint8_t> buildRecord(const TestType& t, const std::vector<Field>& fields) {
    const reflection::Object* root = t.schema->root_table();
    flatbuffers::FlatBufferBuilder b(512);
    std::vector<std::pair<uint16_t, uint32_t>> offs;
    for (const Field& f : fields) {
        const reflection::Field* rf = root->fields()->LookupByKey(f.name.c_str());
        if (!rf) {
            std::fprintf(stderr, "no field %s\n", f.name.c_str());
            std::abort();
        }
        if (f.kind == Field::kStr) offs.push_back({rf->offset(), b.CreateString(f.s).o});
        if (f.kind == Field::kBytes) offs.push_back({rf->offset(), b.CreateVector(f.bytes).o});
    }
    const auto start = b.StartTable();
    for (const Field& f : fields) {
        const reflection::Field* rf = root->fields()->LookupByKey(f.name.c_str());
        const uint16_t vo = rf->offset();
        switch (rf->type()->base_type()) {
            case reflection::UInt: b.AddElement<uint32_t>(vo, uint32_t(f.kind == Field::kUInt ? f.u : uint64_t(f.i)), 0); break;
            case reflection::Int: b.AddElement<int32_t>(vo, int32_t(f.i), 0); break;
            case reflection::ULong: b.AddElement<uint64_t>(vo, f.u, 0); break;
            case reflection::Long: b.AddElement<int64_t>(vo, f.i, 0); break;
            case reflection::Byte: b.AddElement<int8_t>(vo, int8_t(f.i), int8_t(rf->default_integer())); break;
            case reflection::UByte: b.AddElement<uint8_t>(vo, uint8_t(f.i), uint8_t(rf->default_integer())); break;
            case reflection::Double: b.AddElement<double>(vo, f.d, 0.0); break;
            case reflection::Float: b.AddElement<float>(vo, float(f.d), 0.0f); break;
            default: break;
        }
    }
    for (const auto& o : offs) b.AddOffset(o.first, flatbuffers::Offset<void>(o.second));
    const auto end = b.EndTable(start);
    b.FinishSizePrefixed(flatbuffers::Offset<flatbuffers::Table>(end), reinterpret_cast<const char*>(t.fid));
    return std::vector<uint8_t>(b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize());
}

std::vector<uint8_t> ommRecord(uint32_t norad, const std::string& objectId, const std::string& epoch,
                               double meanMotion, size_t pad) {
    std::vector<Field> f = {Field::str("OBJECT_NAME", "SAT-" + std::to_string(norad)),
                            Field::str("OBJECT_ID", objectId),
                            Field::u64("NORAD_CAT_ID", norad),
                            Field::str("EPOCH", epoch),
                            Field::str("CREATION_DATE", "2026-09-27T00:00:00"),
                            Field::f64("MEAN_MOTION", meanMotion),
                            Field::f64("ECCENTRICITY", 0.0001 + meanMotion * 1e-6),
                            Field::f64("INCLINATION", 51.6),
                            Field::f64("RA_OF_ASC_NODE", 120.5),
                            Field::f64("ARG_OF_PERICENTER", 90.1),
                            Field::f64("MEAN_ANOMALY", 270.2),
                            Field::f64("BSTAR", 3.1e-5)};
    if (pad) f.push_back(Field::str("COMMENT", std::string(pad, 'x')));
    return buildRecord(ommType(), f);
}

std::vector<uint8_t> mpeRecord(const std::string& entity, double epoch, double x, size_t pad) {
    std::vector<Field> f = {Field::str("ENTITY_ID", entity), Field::f64("EPOCH", epoch),
                            Field::f64("MEAN_MOTION", x),    Field::f64("ECCENTRICITY", 0.001),
                            Field::f64("INCLINATION", 97.4), Field::f64("RA_OF_ASC_NODE", 10.0),
                            Field::f64("ARG_OF_PERICENTER", 20.0), Field::f64("MEAN_ANOMALY", 30.0),
                            Field::f64("BSTAR", 1e-5)};
    if (pad) f.push_back(Field::str("COMMENT", std::string(pad, 'm')));
    return buildRecord(mpeType(), f);
}

std::vector<uint8_t> catRecord(uint32_t norad, const std::string& objectId, const std::string& uri,
                               const std::string& catalogObjectId, const std::string& name, int objectType) {
    std::vector<Field> f = {Field::str("OBJECT_NAME", name), Field::str("OBJECT_ID", objectId),
                            Field::u64("NORAD_CAT_ID", norad), Field::i64("OBJECT_TYPE", objectType),
                            Field::i64("OPS_STATUS_CODE", 0), Field::str("OWNER", "US"),
                            Field::str("LAUNCH_DATE", "1998-11-20"), Field::f64("MASS", 419725.0)};
    if (!uri.empty()) f.push_back(Field::str("CATALOG_URI", uri));
    if (!catalogObjectId.empty()) f.push_back(Field::str("CATALOG_OBJECT_ID", catalogObjectId));
    return buildRecord(catType(), f);
}

std::vector<uint8_t> iqcRecord(const std::string& entity, const std::string& epoch, size_t imageBytes,
                               uint64_t seed) {
    std::vector<uint8_t> img(imageBytes);
    uint64_t x = seed * 0x9E3779B97F4A7C15ull + 1;
    for (size_t i = 0; i < imageBytes; i++) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        img[i] = uint8_t(x);
    }
    return buildRecord(iqcType(), {Field::str("ENTITY_ID", entity), Field::str("EPOCH", epoch),
                                   Field::str("SENSOR", "EO-1"), Field::raw("IMAGE", std::move(img))});
}

void frameCid(const std::vector<uint8_t>& frame, uint8_t cid[kCidLen]) {
    computeCid(frame.data() + 4, frame.size() - 4, cid);
}

// ---------------------------------------------------------------------------
// Store fixture
// ---------------------------------------------------------------------------
Store::Store(bool tracking, uint32_t writers, bool threads) : fs(new FaultFs(tracking)) {
    cfg.root = root;
    cfg.io = fs.get();
    cfg.writers = writers;
    cfg.cooperative = !threads;
    cfg.syncThreads = threads ? 2 : 0;
    cfg.poolBytes = 64ull << 20;
    cfg.arenaBytes = 16ull << 20;
    cfg.zeroFillStep = 64u << 10;
    cfg.reserveBytes = 2ull << 20;
}

int32_t Store::open(std::string* err) {
    std::string e2;
    const int32_t rc = Engine::open(cfg, &e, err ? err : &e2);
    if (rc < 0) {
        if (!err) std::fprintf(stderr, "  open failed: %s (%d)\n", e2.c_str(), rc);
        return rc;
    }
    if (!cfg.cooperative) e->start();
    return 0;
}

void Store::close() {
    if (e) e->stop();
    e.reset();
}

void Store::crash(FaultFs::CrashMode m, uint64_t seed) {
    fs->freeze();
    if (e) e->abandon();
    e.reset();
    fs->crash(m, seed);
}

void Store::registerTypes(const std::vector<TestType*>& types) {
    for (TestType* t : types) {
        std::string err;
        if (e->registerType(t->config, &err) < 0) {
            std::fprintf(stderr, "registerType %s: %s\n", t->name.c_str(), err.c_str());
            std::abort();
        }
    }
}

uint32_t Store::partition(const std::string& producer, const TestType& t) {
    uint32_t pid = 0;
    const int32_t rc = e->registerPartition(reinterpret_cast<const uint8_t*>(producer.data()),
                                            producer.size(), t.fid, &pid);
    if (rc < 0) {
        std::fprintf(stderr, "registerPartition failed %d\n", rc);
        return 0;
    }
    return pid;
}

uint64_t send(Engine* e, Producer& prod, const std::vector<uint8_t>& frame, const std::vector<uint8_t>& attr,
              int64_t arrivalMs, bool wait) {
    uint8_t cid[kCidLen];
    frameCid(frame, cid);
    uint64_t rseq = 0;
    const int32_t rc = prod.enqueue(kEntRecord, kEntCidPresent, arrivalMs, cid, attr.data(),
                                    uint32_t(attr.size()), frame.data(), uint32_t(frame.size()), &rseq, wait);
    (void)e;
    return rc < 0 ? 0 : rseq;
}

// ---------------------------------------------------------------------------
// Inspector: an independent reader of the committed on-disk state
// ---------------------------------------------------------------------------
namespace {
bool readFile(IoCtx& io, const PathBuf& p, FileClass cls, std::vector<uint8_t>* out) {
    FileRef f;
    if (io.open(p.c_str(), p.len, FLATSQL_IO_READ, cls, &f) < 0) return false;
    const int64_t n = io.size(f);
    out->resize(n > 0 ? size_t(n) : 0);
    const bool ok = n <= 0 || io.read(f, out->data(), out->size(), 0) == n;
    io.close(&f);
    return ok;
}
}  // namespace

PartView Inspector::partition(uint32_t pid) {
    PartView v;
    PathBuf hp;
    pathPartition(&hp, root_.c_str(), pid, "h.fsh");
    std::vector<uint8_t> h;
    if (!readFile(ctx_, hp, FileClass::Head, &h)) {
        // §4.7 intent rule: a registered pid with no head file is an empty
        // partition (the crash fell between PARTITION_ADD and the head).
        v.ok = true;
        return v;
    }
    int best = -1;
    uint64_t bestGen = 0;
    for (int s = 0; s < 2; s++) {
        if (h.size() < size_t(s) * kHeadSlotBytes + 64) continue;
        const uint8_t* slot = h.data() + size_t(s) * kHeadSlotBytes;
        const size_t avail = std::min<size_t>(kHeadSlotBytes, h.size() - size_t(s) * kHeadSlotBytes);
        if (!validHeadSlot(slot, avail, kHeadPartition)) continue;
        HeadPrefix hp2;
        std::memcpy(&hp2, slot, sizeof(hp2));
        if (best < 0 || hp2.gen > bestGen) {
            best = s;
            bestGen = hp2.gen;
        }
    }
    if (best < 0) {
        // Registered, never committed: an empty partition.
        v.ok = true;
        return v;
    }
    const uint8_t* slot = h.data() + size_t(best) * kHeadSlotBytes;
    std::memcpy(&v.head, slot, sizeof(v.head));
    const PartitionHeadFixed& hd = v.head;
    v.rows.assign(hd.pseqHi, RecRow{});
    std::vector<bool> have(hd.pseqHi, false);
    // Merged rows (manifest).
    if (hd.manifestGen) {
        PathBuf mp;
        pathPartitionManifest(&mp, root_.c_str(), pid, hd.manifestGen);
        std::vector<uint8_t> man;
        if (!readFile(ctx_, mp, FileClass::Manifest, &man) || man.size() < 32) {
            v.err = "manifest unreadable";
            return v;
        }
        const uint16_t nSegs = getU16(man.data() + 6);
        size_t off = 24;
        for (uint16_t i = 0; i < nSegs; i++) {
            const uint32_t seg = getU32(man.data() + off);
            const uint64_t first = getU64(man.data() + off + 8);
            const uint64_t mergedEnd = getU64(man.data() + off + 24);
            const uint32_t nRuns = getU32(man.data() + off + 72);
            off += 80 + size_t(nRuns) * 24;
            v.segFirst[seg] = first;
            if (mergedEnd <= first) continue;
            PathBuf rp;
            pathPartitionSeg(&rp, root_.c_str(), pid, 'r', seg, "fsr");
            std::vector<uint8_t> r;
            if (!readFile(ctx_, rp, FileClass::Rows, &r)) {
                v.err = "r file unreadable";
                return v;
            }
            for (uint64_t ps = first; ps < mergedEnd; ps++) {
                const size_t at = size_t(ps - first) * sizeof(RecRow);
                if (at + sizeof(RecRow) > r.size() || ps > hd.pseqHi) {
                    v.err = "r file short";
                    return v;
                }
                RecRow row;
                std::memcpy(&row, r.data() + at, sizeof(row));
                if (row.pseq != ps) {
                    v.err = "r row pseq mismatch";
                    return v;
                }
                v.rows[ps - 1] = row;
                have[ps - 1] = true;
            }
        }
    }
    // Unmerged rows (L0 directory).
    const L0DirEntry* l0 = reinterpret_cast<const L0DirEntry*>(slot + sizeof(PartitionHeadFixed));
    std::map<uint32_t, std::vector<uint8_t>> mfiles;
    for (uint16_t i = 0; i < hd.nL0; i++) {
        L0DirEntry e;
        std::memcpy(&e, reinterpret_cast<const uint8_t*>(l0) + size_t(i) * sizeof(L0DirEntry), sizeof(e));
        if (!mfiles.count(e.mSeg)) {
            PathBuf mp;
            pathPartitionSeg(&mp, root_.c_str(), pid, 'm', e.mSeg, "fsl");
            if (!readFile(ctx_, mp, FileClass::Meta, &mfiles[e.mSeg])) {
                v.err = "m file unreadable";
                return v;
            }
        }
        const std::vector<uint8_t>& m = mfiles[e.mSeg];
        if (e.mOff + e.batchLen > m.size()) {
            v.err = "batch beyond m";
            return v;
        }
        const uint8_t* b = m.data() + e.mOff;
        BatchTrailer tr;
        std::memcpy(&tr, b + e.batchLen - sizeof(tr), sizeof(tr));
        if (crc32c(b, e.batchLen - sizeof(tr) + offsetof(BatchTrailer, crc)) != tr.crc) {
            v.err = "batch crc";
            return v;
        }
        for (uint32_t r = 0; r < e.nRows; r++) {
            RecRow row;
            std::memcpy(&row, b + sizeof(BatchHeader) + size_t(r) * sizeof(RecRow), sizeof(row));
            if (row.pseq != e.firstPseq + r || row.pseq == 0 || row.pseq > hd.pseqHi) {
                v.err = "l0 row pseq mismatch";
                return v;
            }
            if (have[row.pseq - 1]) {
                v.err = "pseq present twice";
                return v;
            }
            v.rows[row.pseq - 1] = row;
            have[row.pseq - 1] = true;
        }
    }
    for (uint64_t i = 0; i < hd.pseqHi; i++)
        if (!have[i]) {
            v.err = "pseq gap at " + std::to_string(i + 1);
            return v;
        }
    // Lanes.
    const uint8_t* lp = slot + sizeof(PartitionHeadFixed) + size_t(hd.nL0) * sizeof(L0DirEntry);
    if (hd.nLanes != 0xffff) {
        for (uint16_t i = 0; i < hd.nLanes; i++) {
            LaneCounter c;
            std::memcpy(&c, lp + size_t(i) * sizeof(c), sizeof(c));
            v.lanes.push_back(c);
        }
    } else {
        std::vector<uint8_t> m;
        PathBuf mp;
        pathPartitionSeg(&mp, root_.c_str(), pid, 'm', hd.lanesOverflowSeg, "fsl");
        if (readFile(ctx_, mp, FileClass::Meta, &m) && hd.lanesOverflowOff + 4 <= m.size()) {
            const uint32_t n = getU32(m.data() + hd.lanesOverflowOff);
            for (uint32_t i = 0; i < n; i++) {
                LaneCounter c;
                std::memcpy(&c, m.data() + hd.lanesOverflowOff + 4 + size_t(i) * sizeof(c), sizeof(c));
                v.lanes.push_back(c);
            }
        }
    }
    v.ok = true;
    return v;
}

std::vector<uint8_t> Inspector::frame(uint32_t pid, const RecRow& r) {
    PathBuf dp;
    pathPartitionSeg(&dp, root_.c_str(), pid, 'd', r.seg, "fsd");
    FileRef f;
    std::vector<uint8_t> out;
    if (ctx_.open(dp.c_str(), dp.len, FLATSQL_IO_READ, FileClass::Data, &f) < 0) return out;
    out.resize(r.len);
    if (ctx_.read(f, out.data(), r.len, r.off) != int64_t(r.len)) out.clear();
    ctx_.close(&f);
    return out;
}

std::vector<uint8_t> Inspector::attr(uint32_t pid, const RecRow& r) {
    std::vector<uint8_t> out;
    if (!(r.flags & kRowHasAttr)) return out;
    PathBuf p;
    if (r.flags & kRowAttrInM) pathPartitionSeg(&p, root_.c_str(), pid, 'm', r.seg, "fsl");
    else pathPartitionSeg(&p, root_.c_str(), pid, 'a', r.seg, "fsa");
    FileRef f;
    if (ctx_.open(p.c_str(), p.len, FLATSQL_IO_READ, FileClass::Attrs, &f) < 0) return out;
    out.resize(r.attrLen);
    if (ctx_.read(f, out.data(), r.attrLen, r.attrOff) != int64_t(r.attrLen)) out.clear();
    ctx_.close(&f);
    return out;
}

Inspector::TypeView Inspector::type(const uint8_t fid[4]) {
    TypeView v;
    PathBuf hp;
    pathType(&hp, root_.c_str(), fid, "h.fsh");
    std::vector<uint8_t> h;
    if (!readFile(ctx_, hp, FileClass::TypeHead, &h)) return v;
    int best = -1;
    uint64_t bestGen = 0;
    for (int s = 0; s < 2; s++) {
        if (h.size() < size_t(s) * kHeadSlotBytes + 64) continue;
        const uint8_t* slot = h.data() + size_t(s) * kHeadSlotBytes;
        const size_t avail = std::min<size_t>(kHeadSlotBytes, h.size() - size_t(s) * kHeadSlotBytes);
        if (!validHeadSlot(slot, avail, kHeadType)) continue;
        HeadPrefix hp2;
        std::memcpy(&hp2, slot, sizeof(hp2));
        if (best < 0 || hp2.gen > bestGen) {
            best = s;
            bestGen = hp2.gen;
        }
    }
    if (best < 0) return v;
    const uint8_t* slot = h.data() + size_t(best) * kHeadSlotBytes;
    std::memcpy(&v.head, slot, sizeof(v.head));
    const size_t labelsAt = sizeof(TypeHeadFixed) + size_t(v.head.nL0) * sizeof(TypeL0DirEntry);
    if (v.head.nLabels != 0xffff)
        for (uint16_t i = 0; i < v.head.nLabels; i++) {
            LabelEntry le;
            std::memcpy(&le, slot + labelsAt + size_t(i) * sizeof(le), sizeof(le));
            v.labeled[le.pid] = le.labeledThrough;
        }
    // A15: sealed segments by the fence index, then the active one.
    PathBuf fp;
    pathType(&fp, root_.c_str(), fid, kArrivalFenceName);
    std::vector<uint8_t> fb;
    if (v.head.gSeg && !readFile(ctx_, fp, FileClass::Arrivals, &fb)) v.fenceErr = "fence missing";
    for (uint32_t s = 0; s < v.head.gSeg && v.fenceErr.empty(); s++) {
        ArrivalFence f;
        if ((size_t(s) + 1) * sizeof(f) > fb.size()) {
            v.fenceErr = "fence short";
            break;
        }
        std::memcpy(&f, fb.data() + size_t(s) * sizeof(f), sizeof(f));
        if (f.seg != s || f.crc != crc32c(&f, offsetof(ArrivalFence, crc))) {
            v.fenceErr = "fence entry invalid";
            break;
        }
        v.fence.push_back(f);
    }
    for (uint32_t s = 0; s <= v.head.gSeg; s++) {
        const uint64_t len = s < v.head.gSeg ? (s < v.fence.size() ? v.fence[s].count * kArrivalBytes : 0)
                                             : v.head.gLen;
        PathBuf gp;
        pathTypeSeg(&gp, root_.c_str(), fid, 'g', s, "fsg");
        std::vector<uint8_t> g;
        if (!readFile(ctx_, gp, FileClass::Arrivals, &g)) {
            if (len && v.fenceErr.empty()) v.fenceErr = "segment missing";
            continue;
        }
        if (g.size() < len && v.fenceErr.empty()) v.fenceErr = "segment short";
        for (uint64_t off = 0; off + kArrivalBytes <= len && off + kArrivalBytes <= g.size(); off += kArrivalBytes) {
            ArrivalEntry a;
            std::memcpy(&a, g.data() + off, sizeof(a));
            if (s < v.fence.size() && v.fenceErr.empty()) {
                if (off == 0 && a.gseq != v.fence[s].firstGseq) v.fenceErr = "fence first gseq";
                if (off + kArrivalBytes == len && a.gseq != v.fence[s].lastGseq) v.fenceErr = "fence last gseq";
            }
            if (s == v.head.gSeg && off == 0 && s > 0 && a.gseq != v.head.gSegFirstGseq && v.fenceErr.empty())
                v.fenceErr = "head first gseq";
            v.arrivals.push_back(a);
        }
    }
    for (size_t i = 1; i < v.arrivals.size() && v.fenceErr.empty(); i++)
        if (v.arrivals[i].gseq <= v.arrivals[i - 1].gseq) v.fenceErr = "arrivals gseq not increasing";
    v.ok = true;
    return v;
}

Recount recount(const PartView& v) {
    Recount r;
    std::set<uint64_t> tagDead;
    for (const RecRow& row : v.rows) {
        if (row.kind == kRowTomb || row.kind == kRowCtlTomb) r.dead.insert(row.targetPseq);
        if (row.kind == kRowTagTomb) tagDead.insert(row.targetPseq);
    }
    for (const RecRow& row : v.rows) {
        if (row.kind == kRowPut) {
            r.total++;
            r.totalBytes += row.len - 4;
            if (!r.dead.count(row.pseq)) {
                r.live++;
                r.liveBytes += row.len - 4;
            }
            r.minEpoch = std::min(r.minEpoch, row.epochMs);
            r.maxEpoch = std::max(r.maxEpoch, row.epochMs);
            r.latestArrival = std::max(r.latestArrival, row.arrivalMs);
        }
        if (row.kind == kRowTomb || row.kind == kRowCtlTomb) r.tombs++;
        const bool instance = (row.kind == kRowPut && row.laneId) || row.kind == kRowRetag;
        if (instance) {
            const uint64_t put = row.kind == kRowPut ? row.pseq : row.targetPseq;
            if (!tagDead.count(row.pseq) && !r.dead.count(put)) {
                auto& lc = r.lanes[row.laneId];
                lc.first += 1;
                lc.second += int64_t(row.len) - 4;
            }
        }
    }
    return r;
}

}  // namespace pst

