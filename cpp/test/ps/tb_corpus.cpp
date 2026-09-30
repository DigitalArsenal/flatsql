// Terabyte harness support (see tb_corpus.h).
#include "ps/tb_corpus.h"

#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/reflection.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <thread>

#include "flatsql/ps/format.h"
#include "flatsql/ps/platform.h"

#if !defined(__wasm__)
#include <dirent.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/utsname.h>
#include <unistd.h>
#include <filesystem>
#endif
#if defined(__APPLE__)
#include <mach/mach.h>
#include <malloc/malloc.h>
#include <sys/mount.h>
#elif defined(__linux__)
#include <malloc.h>
#endif

namespace pst {
namespace tb {

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
namespace {
bool readWhole(const std::string& path, std::vector<uint8_t>* out) {
    FILE* f = std::fopen(path.c_str(), "rb");
    if (!f) return false;
    std::fseek(f, 0, SEEK_END);
    const long n = std::ftell(f);
    std::fseek(f, 0, SEEK_SET);
    out->resize(n > 0 ? size_t(n) : 0);
    const bool ok = n <= 0 || std::fread(out->data(), 1, out->size(), f) == out->size();
    std::fclose(f);
    return ok;
}

std::string typeNameOf(const std::string& schemaName) {
    const size_t dot = schemaName.find('.');
    return dot == std::string::npos ? schemaName : schemaName.substr(0, dot);
}

bool finishType(TbType* t, std::string* err) {
    t->cfg = std::make_shared<TypeConfig>();
    const std::string e = t->cfg->parse(t->config.data(), t->config.size());
    if (!e.empty()) {
        if (err) *err = e;
        return false;
    }
    t->name = typeNameOf(t->cfg->schemaName());
    std::memcpy(t->fid, t->cfg->fid(), 4);
    return true;
}
}  // namespace

bool loadTypeConfigs(const std::string& dir, std::vector<TbType>* out, std::string* err) {
#if defined(__wasm__)
    (void)dir;
    (void)out;
    if (err) *err = "production type configs: native only";
    return false;
#else
    std::vector<std::string> files;
    std::error_code ec;
    for (auto& e : std::filesystem::directory_iterator(dir, ec))
        if (e.path().extension() == ".fsc") files.push_back(e.path().string());
    if (ec || files.empty()) {
        if (err) *err = "no .fsc files in " + dir;
        return false;
    }
    std::sort(files.begin(), files.end());
    for (const auto& f : files) {
        std::vector<uint8_t> b;
        // [u32 magic][u32 ver][u32 len][u32 crc][config] (open.cpp loadTypeConfig)
        if (!readWhole(f, &b) || b.size() < 16 || getU32(b.data()) != kMagicTypeConfig ||
            getU32(b.data() + 8) != b.size() - 16 || crc32c(b.data() + 16, b.size() - 16) != getU32(b.data() + 12)) {
            if (err) *err = "type config corrupt: " + f;
            return false;
        }
        TbType t;
        t.config.assign(b.begin() + 16, b.end());
        if (!finishType(&t, err)) return false;
        out->push_back(std::move(t));
    }
    return true;
#endif
}

std::vector<TbType> testTypes() {
    std::vector<TbType> out;
    for (TestType* tt : {&ommType(), &catType(), &mpeType(), &iqcType()}) {
        TbType t;
        t.config = tt->config;
        std::string err;
        if (!finishType(&t, &err)) {
            std::fprintf(stderr, "test type %s: %s\n", tt->name.c_str(), err.c_str());
            std::abort();
        }
        out.push_back(std::move(t));
    }
    return out;
}

bool variantType(const TbType& base, int baseIndex, const char fid[4], const std::string& name, TbType* out,
                 std::string* err) {
    const TypeConfig& c = *base.cfg;
    std::vector<uint8_t> bfbs = c.bfbs();
    if (!c.schema() || !c.schema()->file_ident() || c.schema()->file_ident()->size() != 4) {
        if (err) *err = base.name + ": no 4-byte file identifier in the BFBS";
        return false;
    }
    const size_t at = size_t(reinterpret_cast<const uint8_t*>(c.schema()->file_ident()->c_str()) - c.bfbs().data());
    if (at + 4 > bfbs.size()) {
        if (err) *err = "file identifier outside the BFBS";
        return false;
    }
    std::memcpy(bfbs.data() + at, fid, 4);
    TbType t;
    t.config = TypeConfig::build(name + ".fbs", reinterpret_cast<const uint8_t*>(fid), bfbs, c.rules(), c.maxFrame(),
                                 c.ringCap(), c.flags());
    t.base = baseIndex;
    if (!finishType(&t, err)) return false;
    *out = std::move(t);
    return true;
}

// ---------------------------------------------------------------------------
// Epoch strings (no libc time: the wasm command has none worth trusting)
// ---------------------------------------------------------------------------
namespace {
int64_t daysFromCivil(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = unsigned(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + int64_t(doe) - 719468;
}
void civilFromDays(int64_t z, int64_t* y, unsigned* m, unsigned* d) {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = unsigned(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const int64_t yy = int64_t(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    *d = doy - (153 * mp + 2) / 5 + 1;
    *m = mp + (mp < 10 ? 3 : -9);
    *y = yy + (*m <= 2);
}
bool isDigit(uint8_t c) { return c >= '0' && c <= '9'; }
int num(const uint8_t* p, int n) {
    int v = 0;
    for (int i = 0; i < n; i++) v = v * 10 + (p[i] - '0');
    return v;
}
// "YYYY-MM-DDTHH:MM:SS" (T or space) at p.
bool isoAt(const uint8_t* p, size_t n) {
    if (n < 19) return false;
    static const int digits[] = {0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18};
    for (int i : digits)
        if (!isDigit(p[i])) return false;
    return p[4] == '-' && p[7] == '-' && (p[10] == 'T' || p[10] == ' ') && p[13] == ':' && p[16] == ':';
}
void put2(uint8_t* p, int v) {
    p[0] = uint8_t('0' + v / 10 % 10);
    p[1] = uint8_t('0' + v % 10);
}
// Shifts the ISO second at p by `sec` seconds, in place (years 1000..9999).
void shiftIso(uint8_t* p, int64_t sec) {
    const int64_t days = daysFromCivil(num(p, 4), unsigned(num(p + 5, 2)), unsigned(num(p + 8, 2)));
    int64_t t = days * 86400 + num(p + 11, 2) * 3600 + num(p + 14, 2) * 60 + num(p + 17, 2) + sec;
    int64_t dd = t >= 0 ? t / 86400 : -((-t + 86399) / 86400);
    int64_t rem = t - dd * 86400;
    int64_t y;
    unsigned m, d;
    civilFromDays(dd, &y, &m, &d);
    if (y < 1000 || y > 9999) return;
    p[0] = uint8_t('0' + y / 1000);
    p[1] = uint8_t('0' + y / 100 % 10);
    p[2] = uint8_t('0' + y / 10 % 10);
    p[3] = uint8_t('0' + y % 10);
    put2(p + 5, int(m));
    put2(p + 8, int(d));
    put2(p + 11, int(rem / 3600));
    put2(p + 14, int(rem / 60 % 60));
    put2(p + 17, int(rem % 60));
}
bool identityName(const std::string& n) {
    return n.find("NORAD") != std::string::npos || n == "ID" ||
           (n.size() > 3 && n.compare(n.size() - 3, 3, "_ID") == 0);
}
}  // namespace

std::string isoSecond(uint64_t n) {
    uint8_t b[20] = "2026-01-01T00:00:00";
    shiftIso(b, int64_t(n));
    return std::string(reinterpret_cast<char*>(b), 19);
}

// ---------------------------------------------------------------------------
// Patch sites and clones
// ---------------------------------------------------------------------------
std::vector<Patch> patchSites(const TypeConfig& cfg, const uint8_t* fb, size_t len) {
    std::vector<Patch> out;
    const reflection::Schema* s = cfg.schema();
    if (!s || !s->root_table() || len < 8) return out;
    const reflection::Object* root = s->root_table();
    const flatbuffers::Table* tab = flatbuffers::GetAnyRoot(fb);
    const size_t tpos = size_t(reinterpret_cast<const uint8_t*>(tab) - fb);
    Patch bytes8{};
    bool haveBytes = false;
    for (const reflection::Field* f : *root->fields()) {
        if (f->deprecated()) continue;
        const uint16_t fo = tab->GetOptionalFieldOffset(f->offset());
        if (!fo) continue;
        const size_t pos = tpos + fo;
        const auto bt = f->type()->base_type();
        const bool isEnum = f->type()->index() >= 0;
        const std::string name = f->name()->str();
        Patch p{};
        p.off = uint32_t(pos);
        switch (bt) {
            case reflection::Int:
            case reflection::UInt:
                if (isEnum || pos + 4 > len) break;
                p.kind = Patch::kInt32;
                p.identity = identityName(name);
                out.push_back(p);
                break;
            case reflection::Long:
            case reflection::ULong:
                if (isEnum || pos + 8 > len) break;
                p.kind = Patch::kInt64;
                p.identity = identityName(name);
                out.push_back(p);
                break;
            case reflection::Double:
                if (pos + 8 > len) break;
                p.kind = name.find("EPOCH") != std::string::npos ? Patch::kEpochF64 : Patch::kF64;
                out.push_back(p);
                break;
            case reflection::Float:
                if (pos + 4 > len) break;
                p.kind = Patch::kF32;
                out.push_back(p);
                break;
            case reflection::String:
            case reflection::Vector: {
                if (pos + 4 > len) break;
                const size_t sp = pos + getU32(fb + pos);
                if (sp + 4 > len) break;
                const uint32_t n = getU32(fb + sp);
                if (sp + 4 + n > len) break;
                if (bt == reflection::String && isoAt(fb + sp + 4, n)) {
                    p.off = uint32_t(sp + 4);
                    p.kind = Patch::kEpochStr;
                    p.len = uint8_t(std::min<uint32_t>(n, 255));
                    out.push_back(p);
                } else if (!haveBytes && n >= 8 &&
                           (bt == reflection::String ||
                            (f->type()->element() == reflection::UByte || f->type()->element() == reflection::Byte))) {
                    // A byte payload (IQC IMAGE) or a free string: its last 8
                    // bytes carry the clone number when nothing else would.
                    bytes8.off = uint32_t(sp + 4 + n - 8);
                    bytes8.kind = 0xff;
                    haveBytes = true;
                }
                break;
            }
            default:
                break;
        }
    }
    bool unique = false;
    for (const Patch& p : out)
        if (p.kind == Patch::kEpochStr || p.kind == Patch::kEpochF64 || p.identity) unique = true;
    if (!unique && haveBytes) out.push_back(bytes8);
    return out;
}

void mutateClone(const std::vector<Patch>& patches, uint64_t c, bool keepIdentity, uint8_t* fb) {
    if (c == 0) return;
    // A record with an epoch is cloned as the same object at a later epoch
    // (an OMM of the same NORAD_CAT_ID an hour on): its identity fields stay,
    // so object cardinality stays the corpus's and history per object grows,
    // as it does in production. Without an epoch the identity shifts so the
    // clone is another object, or keeps clone c - 1's (a supersede of it).
    bool hasEpoch = false;
    for (const Patch& p : patches)
        if (p.kind == Patch::kEpochStr || p.kind == Patch::kEpochF64) hasEpoch = true;
    const uint64_t idc = hasEpoch ? 0 : keepIdentity ? c - 1 : c;
    for (const Patch& p : patches) {
        uint8_t* at = fb + p.off;
        switch (p.kind) {
            case Patch::kInt32: {
                uint32_t v;
                std::memcpy(&v, at, 4);
                v += p.identity ? uint32_t(idc * 100003u) : uint32_t(c % 97);
                std::memcpy(at, &v, 4);
                break;
            }
            case Patch::kInt64: {
                uint64_t v;
                std::memcpy(&v, at, 8);
                v += p.identity ? idc * 100003u : c % 97;
                std::memcpy(at, &v, 8);
                break;
            }
            case Patch::kF64: {
                double v;
                std::memcpy(&v, at, 8);
                v *= 1.0 + double(int64_t(mix64(c) % 2001) - 1000) * 1e-9;
                std::memcpy(at, &v, 8);
                break;
            }
            case Patch::kF32: {
                float v;
                std::memcpy(&v, at, 4);
                v *= float(1.0 + double(int64_t(mix64(c) % 2001) - 1000) * 1e-6);
                std::memcpy(at, &v, 4);
                break;
            }
            case Patch::kEpochStr:
                shiftIso(at, int64_t(c) * 61);
                break;
            case Patch::kEpochF64: {
                double v;
                std::memcpy(&v, at, 8);
                v += double(c) * 61.0;
                std::memcpy(at, &v, 8);
                break;
            }
            case 0xff:
                std::memcpy(at, &c, 8);
                break;
            default:
                break;
        }
    }
}

bool Corpus::load(const std::string& path, const std::vector<TbType>& types, std::string* err) {
    std::vector<uint8_t> b;
    if (!readWhole(path, &b) || b.size() < 16 || std::memcmp(b.data(), "TBC1", 4) != 0) {
        if (err) *err = "corpus unreadable or not TBC1: " + path;
        return false;
    }
    const uint64_t count = getU64(b.data() + 8);
    byType_.assign(types.size(), {});
    bytes_.assign(types.size(), 0);
    size_t off = 16;
    auto str = [&](std::string* s) {
        if (off + 2 > b.size()) return false;
        const uint16_t n = getU16(b.data() + off);
        off += 2;
        if (off + n > b.size()) return false;
        s->assign(reinterpret_cast<const char*>(b.data() + off), n);
        off += n;
        return true;
    };
    for (uint64_t i = 0; i < count; i++) {
        if (off + 8 > b.size()) break;
        uint8_t fid[4];
        std::memcpy(fid, b.data() + off, 4);
        const uint32_t n = getU32(b.data() + off + 4);
        off += 8;
        if (off + n > b.size()) break;
        SeedRecord r;
        r.fb.assign(b.data() + off, b.data() + off + n);
        off += n;
        if (!str(&r.peer) || !str(&r.provider) || !str(&r.source) || !str(&r.batch) || !str(&r.supersedeKey)) break;
        int ti = -1;
        for (size_t t = 0; t < types.size(); t++)
            if (types[t].base < 0 && std::memcmp(types[t].fid, fid, 4) == 0) ti = int(t);
        if (ti < 0) continue;
        const TypeConfig& cfg = *types[size_t(ti)].cfg;
        std::vector<uint8_t> fr(4 + r.fb.size());
        putU32(fr.data(), uint32_t(r.fb.size()));
        std::memcpy(fr.data() + 4, r.fb.data(), r.fb.size());
        if (cfg.checkFrame(fr.data(), fr.size()) != 0) continue;  // not valid under the production schema
        r.type = uint32_t(ti);
        r.patches = patchSites(cfg, r.fb.data(), r.fb.size());
        bytes_[size_t(ti)] += r.fb.size();
        byType_[size_t(ti)].push_back(uint32_t(recs_.size()));
        recs_.push_back(std::move(r));
    }
    // Variants draw on their base type's records.
    for (size_t t = 0; t < types.size(); t++)
        if (types[t].base >= 0) {
            byType_[t] = byType_[size_t(types[t].base)];
            bytes_[t] = bytes_[size_t(types[t].base)];
        }
    if (recs_.empty()) {
        if (err) *err = "corpus holds no record of the given types";
        return false;
    }
    return true;
}

const std::vector<uint32_t>& Corpus::ofType(uint32_t t) const { return byType_[t]; }
uint64_t Corpus::bytesOfType(uint32_t t) const { return bytes_[t]; }

const SeedRecord& Corpus::seedFor(uint32_t t, uint64_t k) const {
    const auto& v = byType_[t];
    return recs_[v[size_t(k % v.size())]];
}

void Corpus::frame(uint32_t t, uint64_t k, bool keepIdentity, const uint8_t* fidOverride,
                   std::vector<uint8_t>* out) const {
    const auto& v = byType_[t];
    const SeedRecord& r = recs_[v[size_t(k % v.size())]];
    const uint64_t c = k / v.size();
    out->resize(4 + r.fb.size());
    putU32(out->data(), uint32_t(r.fb.size()));
    std::memcpy(out->data() + 4, r.fb.data(), r.fb.size());
    mutateClone(r.patches, c, keepIdentity && c > 0, out->data() + 4);
    if (fidOverride) std::memcpy(out->data() + 8, fidOverride, 4);
}

// ---------------------------------------------------------------------------
// Minimal frames
// ---------------------------------------------------------------------------
bool MinimalFrames::init(const TbType& t, std::string* err) {
    std::memcpy(fid_, t.fid, 4);
    const reflection::Schema* s = t.cfg->schema();
    if (!s || !s->root_table()) {
        if (err) *err = t.name + ": no schema";
        return false;
    }
    const auto* fields = s->root_table()->fields();
    auto find = [&](const char* n) -> const reflection::Field* { return fields->LookupByKey(n); };
    if (const auto* f = find("NORAD_CAT_ID")) {
        const auto bt = f->type()->base_type();
        if (bt == reflection::UInt || bt == reflection::Int) idWidth_ = 4;
        if (bt == reflection::ULong || bt == reflection::Long) idWidth_ = 8;
        if (idWidth_) voId_ = f->offset();
    }
    if (const auto* f = find("EPOCH")) {
        if (f->type()->base_type() == reflection::String) voEpochStr_ = f->offset();
        if (f->type()->base_type() == reflection::Double) voEpochF64_ = f->offset();
    }
    if (!voId_) {
        for (const char* n : {"ENTITY_ID", "OBJECT_ID"})
            if (const auto* f = find(n))
                if (f->type()->base_type() == reflection::String) {
                    voEntity_ = f->offset();
                    break;
                }
    }
    if (!voEpochStr_ && !voEpochF64_) {
        // No epoch: n goes into a string field that is not the identity.
        for (const reflection::Field* f : *fields)
            if (!f->deprecated() && f->type()->base_type() == reflection::String && f->offset() != voEntity_) {
                voAny_ = f->offset();
                break;
            }
        if (!voAny_) {
            if (err) *err = t.name + ": no field to make a record unique";
            return false;
        }
    }
    if (!voId_ && !voEntity_) {
        if (err) *err = t.name + ": no identity field (NORAD_CAT_ID, ENTITY_ID, OBJECT_ID)";
        return false;
    }
    what_ = t.name + ":" + (voId_ ? " NORAD_CAT_ID" : "") + (voEntity_ ? " ENTITY_ID" : "") +
            (voEpochStr_ ? " EPOCH(str)" : "") + (voEpochF64_ ? " EPOCH(f64)" : "") + (voAny_ ? " first-string" : "");
    return true;
}

void MinimalFrames::frame(uint64_t n, uint64_t id, uint32_t objects, const uint8_t* fidOverride,
                          std::vector<uint8_t>* out) const {
    const uint64_t obj = id;
    const uint64_t slot = n / (objects ? objects : 1);
    flatbuffers::FlatBufferBuilder b(128);
    flatbuffers::Offset<flatbuffers::String> epoch, entity, any;
    if (voEpochStr_) {
        std::string e = isoSecond(slot * 60);
        char us[8];
        std::snprintf(us, sizeof(us), ".%06u", unsigned(n % 1000000));
        epoch = b.CreateString(e + us);
    }
    if (voEntity_) entity = b.CreateString("E" + std::to_string(obj));
    if (voAny_) any = b.CreateString("N" + std::to_string(n));
    const auto start = b.StartTable();
    if (voId_ && idWidth_ == 4) b.AddElement<uint32_t>(voId_, uint32_t(obj), 0);  // wraps past 2^32
    if (voId_ && idWidth_ == 8) b.AddElement<uint64_t>(voId_, obj, 0);
    if (voEpochF64_) b.AddElement<double>(voEpochF64_, 1767225600.0 + double(slot) * 60.0 + double(n % 1000) * 1e-3, 0);
    if (voEpochStr_) b.AddOffset(voEpochStr_, epoch);
    if (voEntity_) b.AddOffset(voEntity_, entity);
    if (voAny_) b.AddOffset(voAny_, any);
    const auto root = b.EndTable(start);
    char fid[5] = {0};
    std::memcpy(fid, fidOverride ? fidOverride : fid_, 4);
    b.Finish(flatbuffers::Offset<flatbuffers::Table>(root), fid);
    out->resize(4 + b.GetSize());
    putU32(out->data(), uint32_t(b.GetSize()));
    std::memcpy(out->data() + 4, b.GetBufferPointer(), b.GetSize());
}

// ---------------------------------------------------------------------------
// CountingIo
// ---------------------------------------------------------------------------
int32_t CountingIo::open(const char* path, int32_t pathLen, int32_t flags) {
    // PROBE and UNLINK answer without a handle.
    const bool handle = !(flags & (FLATSQL_IO_PROBE | FLATSQL_IO_UNLINK));
    if (handle && cap_ && cur_.load(std::memory_order_relaxed) >= cap_) {
        refused_.fetch_add(1, std::memory_order_relaxed);
        return FLATSQL_IO_ERR_GENERIC;
    }
    const int32_t rc = base_->open(path, pathLen, flags);
    if (handle && rc >= 0) {
        opens_.fetch_add(1, std::memory_order_relaxed);
        const uint64_t now = cur_.fetch_add(1, std::memory_order_relaxed) + 1;
        uint64_t hw = hw_.load(std::memory_order_relaxed);
        while (now > hw && !hw_.compare_exchange_weak(hw, now, std::memory_order_relaxed)) {}
    }
    return rc;
}

int32_t CountingIo::close(int32_t h) {
    const int32_t rc = base_->close(h);
    if (rc >= 0) cur_.fetch_sub(1, std::memory_order_relaxed);
    return rc;
}

// ---------------------------------------------------------------------------
// Gauges
// ---------------------------------------------------------------------------
uint64_t heapBytes() {
#if defined(__APPLE__)
    malloc_statistics_t st;
    malloc_zone_statistics(nullptr, &st);
    return st.size_in_use;
#elif defined(__linux__) && defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 33))
    const struct mallinfo2 mi = mallinfo2();
    return uint64_t(mi.uordblks) + uint64_t(mi.hblkhd);
#else
    return 0;
#endif
}
uint64_t heapInUse() { return heapBytes(); }

uint64_t rssBytes() {
#if defined(__APPLE__)
    mach_task_basic_info_data_t info;
    mach_msg_type_number_t n = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &n) != KERN_SUCCESS)
        return 0;
    return info.resident_size;
#elif defined(__linux__)
    FILE* f = std::fopen("/proc/self/statm", "r");
    if (!f) return 0;
    unsigned long long size = 0, res = 0;
    const int got = std::fscanf(f, "%llu %llu", &size, &res);
    std::fclose(f);
    return got == 2 ? res * uint64_t(sysconf(_SC_PAGESIZE)) : 0;
#else
    return 0;
#endif
}

uint64_t wasmPages() {
#if defined(__wasm__)
    return uint64_t(__builtin_wasm_memory_size(0));
#else
    return 0;
#endif
}

uint64_t openFds() {
#if defined(__linux__) || defined(__APPLE__)
#if defined(__linux__)
    DIR* d = opendir("/proc/self/fd");
#else
    DIR* d = opendir("/dev/fd");
#endif
    if (!d) return 0;
    uint64_t n = 0;
    while (struct dirent* e = readdir(d))
        if (e->d_name[0] != '.') n++;
    closedir(d);
    return n > 0 ? n - 1 : 0;  // the directory stream itself
#else
    return 0;
#endif
}

namespace {
// {total, free to unprivileged users, free} bytes. macOS statvfs has 32-bit
// block counts (they overflow on large APFS containers): statfs there.
bool fsStat(const std::string& path, double* total, double* avail, double* bfree) {
#if defined(__APPLE__)
    struct statfs v;
    if (statfs(path.c_str(), &v) != 0) return false;
    const double bs = double(v.f_bsize);
#elif !defined(__wasm__)
    struct statvfs v;
    if (statvfs(path.c_str(), &v) != 0) return false;
    const double bs = double(v.f_frsize);
#else
    (void)path;
    return false;
#endif
#if !defined(__wasm__)
    *total = double(v.f_blocks) * bs;
    *avail = double(v.f_bavail) * bs;
    *bfree = double(v.f_bfree) * bs;
    return true;
#endif
}
}  // namespace

uint64_t fsFreeBytes(const std::string& path) {
    double t, a, f;
    return fsStat(path, &t, &a, &f) ? uint64_t(a) : 0;
}

uint64_t fsTotalBytes(const std::string& path) {
    double t, a, f;
    return fsStat(path, &t, &a, &f) ? uint64_t(t) : 0;
}

double fsUsedPct(const std::string& path) {
    double t, a, f;
    if (!fsStat(path, &t, &a, &f) || t <= 0) return 0;
    // df's Use%: used / (used + available to unprivileged users).
    const double used = t - f;
    return 100.0 * used / (used + a);
}

DirUsage dirUsage(const std::string& dir) {
    DirUsage u;
#if !defined(__wasm__)
    std::error_code ec;
    for (auto it = std::filesystem::recursive_directory_iterator(dir, ec);
         !ec && it != std::filesystem::recursive_directory_iterator(); it.increment(ec)) {
        struct stat st;
        if (lstat(it->path().c_str(), &st) != 0 || !S_ISREG(st.st_mode)) continue;
        u.apparent += uint64_t(st.st_size);
        u.allocated += uint64_t(st.st_blocks) * 512;
        u.files++;
    }
#else
    (void)dir;
#endif
    return u;
}

std::string machineLine() {
    std::string s;
#if !defined(__wasm__)
    struct utsname u;
    if (uname(&u) == 0) s = std::string(u.sysname) + " " + u.release + " " + u.machine;
    double la[3] = {0, 0, 0};
    getloadavg(la, 3);
    char b[160];
    std::snprintf(b, sizeof(b), ", %u hardware threads, load %.1f %.1f %.1f", std::thread::hardware_concurrency(),
                  la[0], la[1], la[2]);
    s += b;
#else
    s = "wasm32-wasip1-threads";
#endif
    return s;
}

uint64_t nowNs() { return monoNs(); }

HistSnap snap(const LockHist& h) {
    HistSnap s;
    s.b.resize(LockHist::kBuckets);
    for (int i = 0; i < LockHist::kBuckets; i++) s.b[size_t(i)] = h.buckets[i].load(std::memory_order_relaxed);
    s.count = h.count.load(std::memory_order_relaxed);
    s.maxNs = h.maxNs.load(std::memory_order_relaxed);
    return s;
}

uint64_t windowCount(const HistSnap& a, const HistSnap& b) {
    uint64_t n = 0;
    for (size_t i = 0; i < b.b.size(); i++) n += b.b[i] - (i < a.b.size() ? a.b[i] : 0);
    return n;
}

double windowPercentileMs(const HistSnap& a, const HistSnap& b, double q) {
    const uint64_t total = windowCount(a, b);
    if (!total) return 0;
    const uint64_t want = uint64_t(std::ceil(q * double(total)));
    uint64_t cum = 0;
    for (size_t i = 0; i < b.b.size(); i++) {
        cum += b.b[i] - (i < a.b.size() ? a.b[i] : 0);
        if (cum >= want) return double(LockHist::upperNs(int(i))) / 1e6;
    }
    return double(LockHist::upperNs(LockHist::kBuckets - 1)) / 1e6;
}

void HistWindow::take(const LockHist& h) {
    prev_ = std::move(cur_);
    cur_ = snap(h);
    if (prev_.b.empty()) {
        prev_ = cur_;
        return;
    }
    // A bucket that went down is a new histogram (the engine was reopened):
    // the window starts from zero, not from the old engine's counts.
    for (size_t i = 0; i < cur_.b.size() && i < prev_.b.size(); i++)
        if (cur_.b[i] < prev_.b[i]) {
            prev_.b.assign(cur_.b.size(), 0);
            prev_.count = 0;
            break;
        }
}

double HistWindow::sumNs() const {
    double s = 0;
    for (size_t i = 0; i < cur_.b.size(); i++) {
        const uint64_t n = cur_.b[i] - (i < prev_.b.size() ? prev_.b[i] : 0);
        if (!n) continue;
        const double hi = double(LockHist::upperNs(int(i)));
        const double lo = i ? double(LockHist::upperNs(int(i) - 1)) : 0;
        s += double(n) * (lo + hi) / 2;
    }
    return s;
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------
bool Csv::open(const std::string& path, const std::vector<std::string>& cols) {
    std::lock_guard<std::mutex> g(mu_);
    f_ = std::fopen(path.c_str(), "w");
    if (!f_) return false;
    n_ = cols.size();
    for (size_t i = 0; i < cols.size(); i++) std::fprintf(f_, "%s%s", i ? "," : "", cols[i].c_str());
    std::fprintf(f_, "\n");
    std::fflush(f_);
    return true;
}

void Csv::row(const std::vector<double>& v) {
    std::lock_guard<std::mutex> g(mu_);
    if (!f_) return;
    for (size_t i = 0; i < v.size(); i++) {
        const double x = v[i];
        if (x == std::floor(x) && std::fabs(x) < 9e15) std::fprintf(f_, "%s%.0f", i ? "," : "", x);
        else std::fprintf(f_, "%s%.6g", i ? "," : "", x);
    }
    std::fprintf(f_, "\n");
    std::fflush(f_);
}

void Csv::close() {
    std::lock_guard<std::mutex> g(mu_);
    if (f_) std::fclose(f_);
    f_ = nullptr;
}

// ---------------------------------------------------------------------------
// Heads (B1)
// ---------------------------------------------------------------------------
namespace {
// The newest valid slot of a head file (heads.go readHeadSlot / open.cpp
// readHeadSlots semantics).
bool readHead(Io* io, const PathBuf& p, uint16_t kind, std::vector<uint8_t>* out) {
    const int32_t h = io->open(p.c_str(), int32_t(p.len), FLATSQL_IO_READ);
    if (h < 0) return false;
    std::vector<uint8_t> buf(2 * kHeadSlotBytes);
    const int32_t n = io->read(h, buf.data(), int32_t(buf.size()), 0);
    io->close(h);
    if (n <= 0) return false;
    uint64_t bestGen = 0;
    bool found = false;
    for (int s = 0; s < 2; s++) {
        const size_t base = size_t(s) * kHeadSlotBytes;
        if (size_t(n) < base + sizeof(HeadPrefix) + 4) continue;
        HeadPrefix hp;
        std::memcpy(&hp, buf.data() + base, sizeof(hp));
        const size_t avail = std::min<size_t>(size_t(n) - base, kHeadSlotBytes);
        if (hp.magic != kMagicHead || hp.kind != kind || hp.usedLen < sizeof(HeadPrefix) + 4 || hp.usedLen > avail)
            continue;
        if (crc32c(buf.data() + base, hp.usedLen - 4) != getU32(buf.data() + base + hp.usedLen - 4)) continue;
        if (!found || hp.gen > bestGen) {
            out->assign(buf.data() + base, buf.data() + base + hp.usedLen);
            bestGen = hp.gen;
            found = true;
        }
    }
    return found;
}
}  // namespace

bool readDurableHead(Io* io, const PathBuf& path, uint16_t kind, std::vector<uint8_t>* out) {
    return readHead(io, path, kind, out);
}

bool headLabeledThrough(Io* io, const std::string& root, const uint8_t fid[4], uint32_t pid, uint64_t* out) {
    *out = 0;
    PathBuf p;
    pathType(&p, root.c_str(), fid, "h.fsh");
    std::vector<uint8_t> h;
    if (!readHead(io, p, kHeadType, &h) || h.size() < sizeof(TypeHeadFixed)) return false;
    TypeHeadFixed th;
    std::memcpy(&th, h.data(), sizeof(th));
    if (th.nLabels != 0xffff) {
        size_t at = sizeof(TypeHeadFixed) + size_t(th.nL0) * sizeof(TypeL0DirEntry);
        for (uint16_t i = 0; i < th.nLabels && at + sizeof(LabelEntry) <= h.size(); i++, at += sizeof(LabelEntry)) {
            LabelEntry le;
            std::memcpy(&le, h.data() + at, sizeof(le));
            if (le.pid == pid) {
                *out = le.labeledThrough;
                return true;
            }
        }
        return true;  // inline table without the pid: nothing labeled yet
    }
    // More than 128 partitions (A10): fold the type log from the label
    // checkpoint through the head's (mSeg, mEnd), as SDN's heads.go
    // (typeLabels.fold) does. A batch that does not parse, or a missing
    // segment, leaves the label unknown.
    uint32_t seg = th.labelCkptSeg;
    uint64_t off = th.labelCkptOff;
    if (seg > th.mSeg) return false;
    std::vector<LabelEntry> labels;
    for (;;) {
        PathBuf mp;
        pathTypeSeg(&mp, root.c_str(), fid, 'm', seg, "fsl");
        const int32_t f = io->open(mp.c_str(), int32_t(mp.len), FLATSQL_IO_READ);
        if (f < 0) return false;
        const uint64_t end = seg < th.mSeg ? uint64_t(io->size(f)) : th.mEnd;
        bool ok = true;
        while (ok && off + sizeof(TypeBatchHeader) <= end) {
            TypeBatchHeader bh;
            if (io->read(f, &bh, int32_t(sizeof(bh)), double(off)) != int32_t(sizeof(bh)) ||
                bh.magic != kMagicTypeBatch || bh.batchLen < sizeof(TypeBatchHeader) + 8 || off + bh.batchLen > end ||
                sizeof(TypeBatchHeader) + uint64_t(bh.nLabel) * sizeof(LabelEntry) + 8 > bh.batchLen) {
                ok = false;
                break;
            }
            if (bh.nLabel) {
                labels.resize(bh.nLabel);
                const int32_t n = int32_t(bh.nLabel * sizeof(LabelEntry));
                if (io->read(f, labels.data(), n, double(off + sizeof(TypeBatchHeader))) != n) {
                    ok = false;
                    break;
                }
                for (const LabelEntry& le : labels)
                    if (le.pid == pid) *out = le.labeledThrough;
            }
            off += bh.batchLen;
        }
        io->close(f);
        if (!ok) return false;
        if (seg >= th.mSeg) return true;
        seg++;
        off = 0;
    }
}

uint64_t headPseqHi(Io* io, const std::string& root, uint32_t pid) {
    PathBuf p;
    pathPartition(&p, root.c_str(), pid, "h.fsh");
    std::vector<uint8_t> h;
    if (!readHead(io, p, kHeadPartition, &h) || h.size() < sizeof(PartitionHeadFixed)) return 0;
    PartitionHeadFixed ph;
    std::memcpy(&ph, h.data(), sizeof(ph));
    return ph.pseqHi;
}

// ---------------------------------------------------------------------------
// Gates
// ---------------------------------------------------------------------------
namespace {
bool gHang = false;
}

void gateBegin(const Gate& g) {
    gHang = false;
    std::printf("  GATE %s (audit %s): expected on the engine as built: %s\n  GATE mechanism: %s\n", g.id, g.audit,
                g.fails ? "FAIL" : "PASS", g.why);
    std::printf("  GATE machine: %s\n", machineLine().c_str());
    std::fflush(stdout);
}

void gateHang(const std::string& what) {
    gHang = true;
    std::fprintf(stderr, "  HANG %s\n", what.c_str());
    std::printf("  HANG %s\n", what.c_str());
    std::fflush(stdout);
    gFailures++;
}

bool gateHangs() { return gHang; }

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------
std::string peerId(uint32_t i) {
    static const char* kB58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    std::string s = "16Uiu2HAm";
    uint64_t x = mix64(i);
    while (s.size() < 53) {
        s.push_back(kB58[x % 58]);
        x /= 58;
        if (x == 0) x = mix64(i + s.size());
    }
    return s;
}

void Zipf::init(uint32_t n, double s) {
    cdf_.resize(n);
    double acc = 0;
    for (uint32_t i = 0; i < n; i++) {
        acc += 1.0 / std::pow(double(i + 1), s);
        cdf_[i] = acc;
    }
    for (auto& v : cdf_) v /= acc;
}

uint32_t Zipf::sample(uint64_t u64) const {
    const double u = double(u64 >> 11) * (1.0 / 9007199254740992.0);
    const auto it = std::lower_bound(cdf_.begin(), cdf_.end(), u);
    return uint32_t(std::min<size_t>(size_t(it - cdf_.begin()), cdf_.size() - 1));
}

double pct(std::vector<double> v, double q) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    size_t i = size_t(std::ceil(q * double(v.size())));
    if (i > 0) i--;
    return v[std::min(i, v.size() - 1)];
}

}  // namespace tb
}  // namespace pst
