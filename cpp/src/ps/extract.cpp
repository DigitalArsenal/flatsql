// FlatSQL partition store: per-type configuration and extraction (see
// ps/extract.h). Go parity notes cite sdn-server/internal/storage.
#include "flatsql/ps/extract.h"

#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/reflection.h>

#include <cmath>
#include <cstdlib>
#include <cstring>

#include "flatsql/ps/format.h"
#include "flatsql/ps/platform.h"

namespace flatsql {
namespace ps {

// ---------------------------------------------------------------------------
// UTF-8 and Go strings.TrimSpace
// ---------------------------------------------------------------------------
namespace {

// Go utf8.DecodeRune: returns rune and width; invalid -> (0xFFFD, 1).
uint32_t decodeRune(const uint8_t* s, size_t n, size_t* width) {
    if (n == 0) {
        *width = 0;
        return 0xFFFD;
    }
    const uint8_t c0 = s[0];
    if (c0 < 0x80) {
        *width = 1;
        return c0;
    }
    auto cont = [&](size_t i) { return i < n && (s[i] & 0xC0) == 0x80; };
    if (c0 >= 0xC2 && c0 <= 0xDF && cont(1)) {
        *width = 2;
        return (uint32_t(c0 & 0x1F) << 6) | (s[1] & 0x3F);
    }
    if (c0 >= 0xE0 && c0 <= 0xEF && n >= 3 && cont(1) && cont(2)) {
        const uint8_t c1 = s[1];
        if ((c0 == 0xE0 && c1 < 0xA0) || (c0 == 0xED && c1 > 0x9F)) {
            *width = 1;
            return 0xFFFD;
        }
        *width = 3;
        return (uint32_t(c0 & 0x0F) << 12) | (uint32_t(c1 & 0x3F) << 6) | (s[2] & 0x3F);
    }
    if (c0 >= 0xF0 && c0 <= 0xF4 && n >= 4 && cont(1) && cont(2) && cont(3)) {
        const uint8_t c1 = s[1];
        if ((c0 == 0xF0 && c1 < 0x90) || (c0 == 0xF4 && c1 > 0x8F)) {
            *width = 1;
            return 0xFFFD;
        }
        *width = 4;
        return (uint32_t(c0 & 0x07) << 18) | (uint32_t(c1 & 0x3F) << 12) |
               (uint32_t(s[2] & 0x3F) << 6) | (s[3] & 0x3F);
    }
    *width = 1;
    return 0xFFFD;
}

// Go utf8.DecodeLastRune.
uint32_t decodeLastRune(const uint8_t* s, size_t n, size_t* width) {
    if (n == 0) {
        *width = 0;
        return 0xFFFD;
    }
    if (s[n - 1] < 0x80) {
        *width = 1;
        return s[n - 1];
    }
    size_t lim = n > 4 ? n - 4 : 0;
    size_t start = n - 1;
    while (start > lim && (s[start] & 0xC0) == 0x80) start--;
    size_t w;
    const uint32_t r = decodeRune(s + start, n - start, &w);
    if (start + w != n) {
        *width = 1;
        return 0xFFFD;
    }
    *width = w;
    return r;
}

bool isGoSpace(uint32_t r) {
    switch (r) {
        case '\t': case '\n': case '\v': case '\f': case '\r': case ' ':
        case 0x85: case 0xA0: case 0x1680: case 0x2028: case 0x2029: case 0x202F:
        case 0x205F: case 0x3000:
            return true;
        default:
            return r >= 0x2000 && r <= 0x200A;
    }
}

}  // namespace

void trimSpace(const uint8_t** s, size_t* n) {
    const uint8_t* p = *s;
    size_t len = *n;
    while (len) {
        size_t w;
        const uint32_t r = decodeRune(p, len, &w);
        if (!isGoSpace(r)) break;
        p += w;
        len -= w;
    }
    while (len) {
        size_t w;
        const uint32_t r = decodeLastRune(p, len, &w);
        if (!isGoSpace(r)) break;
        len -= w;
    }
    *s = p;
    *n = len;
}

std::string producerToken(const uint8_t* peer, size_t len) {
    const uint8_t* s = peer;
    size_t n = len;
    trimSpace(&s, &n);
    if (n == 0) return "unattributed";
    std::string out;
    out.reserve(n);
    size_t i = 0;
    while (i < n) {
        size_t w;
        const uint32_t r = decodeRune(s + i, n - i, &w);
        if ((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'))
            out.push_back(char(r));
        else
            out.push_back('_');
        i += w ? w : 1;
    }
    return out;
}

// ---------------------------------------------------------------------------
// Epochs (parseEpochString parity) and civil dates
// ---------------------------------------------------------------------------
namespace {

int64_t daysFromCivil(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = unsigned(y - era * 400);
    const unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
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
    *m = mp < 10 ? mp + 3 : mp - 9;
    *y = yy + (*m <= 2);
}

bool isLeap(int64_t y) { return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0; }
unsigned daysIn(int64_t y, unsigned m) {
    static const unsigned kDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return m == 2 && isLeap(y) ? 29 : kDays[m - 1];
}

bool digitsAt(const uint8_t* s, size_t n, size_t i, size_t count, int64_t* v) {
    if (i + count > n) return false;
    int64_t x = 0;
    for (size_t k = 0; k < count; k++) {
        const uint8_t c = s[i + k];
        if (c < '0' || c > '9') return false;
        x = x * 10 + (c - '0');
    }
    *v = x;
    return true;
}

bool isDigit(uint8_t c) { return c >= '0' && c <= '9'; }

}  // namespace

bool parseEpochString(const uint8_t* raw, size_t rawLen, int64_t* sec, int64_t* ms) {
    const uint8_t* s = raw;
    size_t n = rawLen;
    trimSpace(&s, &n);
    if (n == 0) return false;
    // Date: YYYY-MM-DD (every Go layout starts with it).
    int64_t y, mo, d;
    if (n >= 10 && digitsAt(s, n, 0, 4, &y) && s[4] == '-' && digitsAt(s, n, 5, 2, &mo) &&
        s[7] == '-' && digitsAt(s, n, 8, 2, &d) && mo >= 1 && mo <= 12 && d >= 1 &&
        unsigned(d) <= daysIn(y, unsigned(mo))) {
        int64_t h = 0, mi = 0, se = 0, fracMs = 0, tz = 0;
        size_t i = 10;
        bool ok = true;
        if (i < n) {
            const uint8_t sep = s[i];
            if (sep != 'T' && sep != ' ') ok = false;
            i++;
            // hour: one or two digits (Go stdHour is not fixed-width)
            if (ok) {
                if (i < n && isDigit(s[i])) {
                    h = s[i] - '0';
                    i++;
                    if (i < n && isDigit(s[i])) {
                        h = h * 10 + (s[i] - '0');
                        i++;
                    }
                } else {
                    ok = false;
                }
            }
            if (ok && !(i < n && s[i] == ':' && digitsAt(s, n, i + 1, 2, &mi))) ok = false;
            if (ok) i += 3;
            if (ok && !(i < n && s[i] == ':' && digitsAt(s, n, i + 1, 2, &se))) ok = false;
            if (ok) i += 3;
            if (ok && (h > 23 || mi > 59 || se > 59)) ok = false;
            if (ok && i + 1 < n && (s[i] == '.' || s[i] == ',') && isDigit(s[i + 1])) {
                size_t j = i + 1;
                int64_t frac = 0;
                int digits = 0;
                while (j < n && isDigit(s[j])) {
                    if (digits < 3) {
                        frac = frac * 10 + (s[j] - '0');
                        digits++;
                    }
                    j++;
                }
                while (digits < 3) {
                    frac *= 10;
                    digits++;
                }
                fracMs = frac;
                i = j;
            }
            if (ok && i < n) {
                // A zone is accepted only by the RFC3339 layout ('T' form).
                if (sep != 'T') ok = false;
                else if (s[i] == 'Z' && i + 1 == n) i++;
                else if ((s[i] == '+' || s[i] == '-') && i + 6 == n && s[i + 3] == ':') {
                    int64_t zh, zm;
                    if (!digitsAt(s, n, i + 1, 2, &zh) || !digitsAt(s, n, i + 4, 2, &zm) ||
                        zh > 23 || zm > 59)
                        ok = false;
                    else {
                        tz = (zh * 3600 + zm * 60) * (s[i] == '-' ? -1 : 1);
                        i = n;
                    }
                } else {
                    ok = false;
                }
            }
            if (ok && i != n) ok = false;
        }
        if (ok) {
            const int64_t days = daysFromCivil(y, unsigned(mo), unsigned(d));
            *sec = days * 86400 + h * 3600 + mi * 60 + se - tz;
            *ms = *sec * 1000 + fracMs;
            return true;
        }
    }
    // strconv.ParseFloat(normalized) > 0
    if (n > 64) return false;
    char buf[65];
    std::memcpy(buf, s, n);
    buf[n] = 0;
    for (size_t k = 0; k < n; k++) {
        const char c = buf[k];
        if (!(isDigit(uint8_t(c)) || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-'))
            return false;
    }
    char* end = nullptr;
    const double f = std::strtod(buf, &end);
    if (end != buf + n || !std::isfinite(f) || !(f > 0) || f >= 9.2e18) return false;
    *sec = int64_t(f);
    *ms = int64_t(f * 1000.0);
    return true;
}

void formatEpochDay(int64_t sec, char out[10]) {
    int64_t days = sec / 86400;
    if (sec % 86400 < 0) days--;
    int64_t y;
    unsigned m, d;
    civilFromDays(days, &y, &m, &d);
    int64_t yy = y < 0 ? 0 : (y > 9999 ? 9999 : y);
    out[0] = char('0' + (yy / 1000) % 10);
    out[1] = char('0' + (yy / 100) % 10);
    out[2] = char('0' + (yy / 10) % 10);
    out[3] = char('0' + yy % 10);
    out[4] = '-';
    out[5] = char('0' + m / 10);
    out[6] = char('0' + m % 10);
    out[7] = '-';
    out[8] = char('0' + d / 10);
    out[9] = char('0' + d % 10);
}

bool sealedEnvelopeValid(const uint8_t* b, size_t len) {
    if (len < 5) return false;
    if (std::memcmp(b, "SDF1", 4) != 0 && std::memcmp(b, "SDFN", 4) != 0) return false;
    return b[4] == 1;
}

// ---------------------------------------------------------------------------
// TypeConfig serialization: TLV [u16 tag][u32 len][bytes]
// ---------------------------------------------------------------------------
namespace {
enum Tag : uint16_t {
    kTagSchemaName = 1, kTagFid = 2, kTagBfbs = 3, kTagRules = 4,
    kTagMaxFrame = 5, kTagRingCap = 6, kTagFlags = 7,
};
void putTlv(std::vector<uint8_t>& out, uint16_t tag, const void* data, size_t len) {
    const size_t at = out.size();
    out.resize(at + 6 + len);
    putU16(out.data() + at, tag);
    putU32(out.data() + at + 2, uint32_t(len));
    if (len) std::memcpy(out.data() + at + 6, data, len);
}
}  // namespace

std::vector<uint8_t> TypeConfig::build(const std::string& schemaName, const uint8_t fid[4],
                                       const std::vector<uint8_t>& bfbs, const std::string& rules,
                                       uint64_t maxFrame, uint64_t ringCap, uint32_t flags) {
    std::vector<uint8_t> out;
    putTlv(out, kTagSchemaName, schemaName.data(), schemaName.size());
    putTlv(out, kTagFid, fid, 4);
    putTlv(out, kTagBfbs, bfbs.data(), bfbs.size());
    putTlv(out, kTagRules, rules.data(), rules.size());
    uint8_t b8[8];
    putU64(b8, maxFrame);
    putTlv(out, kTagMaxFrame, b8, 8);
    putU64(b8, ringCap);
    putTlv(out, kTagRingCap, b8, 8);
    uint8_t b4[4];
    putU32(b4, flags);
    putTlv(out, kTagFlags, b4, 4);
    return out;
}

std::vector<uint8_t> TypeConfig::serialize() const {
    return build(schemaName_, fid_, bfbs_, rules_, maxFrame_, ringCap_, flags_);
}

std::string TypeConfig::parse(const uint8_t* data, size_t len) {
    size_t off = 0;
    bool haveFid = false;
    while (off + 6 <= len) {
        const uint16_t tag = getU16(data + off);
        const uint32_t l = getU32(data + off + 2);
        if (off + 6 + l > len) return "config: truncated field";
        const uint8_t* v = data + off + 6;
        switch (tag) {
            case kTagSchemaName: schemaName_.assign(reinterpret_cast<const char*>(v), l); break;
            case kTagFid:
                if (l != 4) return "config: fid must be 4 bytes";
                std::memcpy(fid_, v, 4);
                haveFid = true;
                break;
            case kTagBfbs: bfbs_.assign(v, v + l); break;
            case kTagRules: rules_.assign(reinterpret_cast<const char*>(v), l); break;
            case kTagMaxFrame: if (l == 8) maxFrame_ = getU64(v); break;
            case kTagRingCap: if (l == 8) ringCap_ = getU64(v); break;
            case kTagFlags: if (l == 4) flags_ = getU32(v); break;
            default: break;  // forward compatible
        }
        off += 6 + l;
    }
    if (off != len) return "config: trailing bytes";
    if (!haveFid) return "config: no fid";
    fp_ = hash64(data, len, 0x74797065);
    schema_ = nullptr;
    if (!bfbs_.empty()) {
        flatbuffers::Verifier v(bfbs_.data(), bfbs_.size(), 64, 1000000);
        if (!reflection::VerifySchemaBuffer(v)) return "config: invalid BFBS";
        schema_ = reflection::GetSchema(bfbs_.data());
        if (!schema_->root_table()) return "config: BFBS has no root table";
        if (schema_->file_ident() && schema_->file_ident()->size() == 4 &&
            std::memcmp(schema_->file_ident()->c_str(), fid_, 4) != 0)
            return "config: BFBS file identifier differs from fid";
    }
    return compile(rules_);
}

// ---------------------------------------------------------------------------
// Rule compilation
// ---------------------------------------------------------------------------
namespace {
std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    size_t start = 0;
    while (true) {
        const size_t at = s.find(sep, start);
        out.push_back(s.substr(start, at == std::string::npos ? std::string::npos : at - start));
        if (at == std::string::npos) break;
        start = at + 1;
    }
    return out;
}
std::string trimAscii(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && (s[a] == ' ' || s[a] == '\t' || s[a] == '\r')) a++;
    while (b > a && (s[b - 1] == ' ' || s[b - 1] == '\t' || s[b - 1] == '\r')) b--;
    return s.substr(a, b - a);
}
}  // namespace

std::string TypeConfig::resolve(const std::string& text, Path* out) const {
    if (!schema_) return "rule needs a BFBS: " + text;
    const reflection::Object* obj = schema_->root_table();
    out->steps.clear();
    size_t i = 0;
    while (i < text.size()) {
        size_t j = i;
        while (j < text.size() && text[j] != '.' && text[j] != '[') j++;
        const std::string name = text.substr(i, j - i);
        if (!obj || obj->is_struct()) return "path walks through a non-table: " + text;
        const reflection::Field* f = obj->fields()->LookupByKey(name.c_str());
        if (!f) return "unknown field '" + name + "' in " + text;
        Step st;
        st.voffset = f->offset();
        st.baseType = uint8_t(f->type()->base_type());
        st.element = uint8_t(f->type()->element());
        st.defInt = f->default_integer();
        st.defReal = f->default_real();
        st.enumIndex = f->type()->index();
        i = j;
        if (i < text.size() && text[i] == '[') {
            if (text.compare(i, 3, "[0]") != 0) return "only [0] is supported: " + text;
            if (st.baseType != reflection::Vector || st.element != reflection::Obj)
                return "[0] needs a vector of tables: " + text;
            st.first = true;
            i += 3;
        }
        const bool more = i < text.size() && text[i] == '.';
        if (more) {
            i++;
            if (st.baseType == reflection::Obj || (st.first && st.element == reflection::Obj)) {
                obj = schema_->objects()->Get(uint32_t(f->type()->index()));
            } else {
                return "path continues past a leaf: " + text;
            }
        } else if (st.first) {
            return "path ends at a vector element: " + text;
        }
        out->steps.push_back(st);
    }
    if (out->steps.empty()) return "empty path";
    return "";
}

std::string TypeConfig::compile(const std::string& rules) {
    epoch_.clear();
    for (auto& c : cols_) c.clear();
    supersede_.clear();
    objectCols_.clear();
    require_.clear();
    nCols_ = 0;
    epochDayCol_ = -1;
    for (const std::string& rawLine : split(rules, '\n')) {
        std::string line = rawLine;
        const size_t hash = line.find('#');
        if (hash != std::string::npos) line = line.substr(0, hash);
        line = trimAscii(line);
        if (line.empty()) continue;
        const size_t sp = line.find(' ');
        const std::string cmd = line.substr(0, sp);
        std::string rest = sp == std::string::npos ? "" : trimAscii(line.substr(sp + 1));
        if (cmd == "epoch") {
            for (const std::string& a : split(rest, '|')) {
                const size_t c = a.find(':');
                if (c == std::string::npos) return "epoch alternative needs kind:path";
                const std::string k = a.substr(0, c);
                Alt alt;
                if (k == "str") alt.kind = kAltEpochStr;
                else if (k == "f64floor") alt.kind = kAltEpochF64Floor;
                else if (k == "i64s") alt.kind = kAltEpochI64s;
                else if (k == "i64ms") alt.kind = kAltEpochI64ms;
                else return "unknown epoch alternative " + k;
                const std::string err = resolve(a.substr(c + 1), &alt.a);
                if (!err.empty()) return err;
                epoch_.push_back(alt);
            }
        } else if (cmd == "col") {
            const size_t sp2 = rest.find(' ');
            if (sp2 == std::string::npos) return "col needs an index and alternatives";
            const int n = std::atoi(rest.substr(0, sp2).c_str());
            if (n < 0 || n >= int(kMaxCols)) return "col index out of range";
            for (const std::string& a : split(trimAscii(rest.substr(sp2 + 1)), '|')) {
                const size_t c = a.find(':');
                if (c == std::string::npos) return "col alternative needs kind:path";
                const std::string k = a.substr(0, c);
                Alt alt;
                if (k == "u64pos") alt.kind = kAltColU64Pos;
                else if (k == "str") alt.kind = kAltColStr;
                else if (k == "enum") alt.kind = kAltColEnum;
                else return "unknown col alternative " + k;
                const std::string err = resolve(a.substr(c + 1), &alt.a);
                if (!err.empty()) return err;
                cols_[n].push_back(alt);
            }
            if (uint32_t(n + 1) > nCols_) nCols_ = uint32_t(n + 1);
        } else if (cmd == "epoch_day") {
            const int n = std::atoi(rest.c_str());
            if (n < 0 || n >= int(kMaxCols)) return "epoch_day index out of range";
            epochDayCol_ = n;
            if (uint32_t(n + 1) > nCols_) nCols_ = uint32_t(n + 1);
        } else if (cmd == "object") {
            for (const std::string& a : split(rest, ',')) {
                const int n = std::atoi(trimAscii(a).c_str());
                if (n < 0 || n >= int(kMaxCols)) return "object col out of range";
                objectCols_.push_back(n);
            }
        } else if (cmd == "require") {
            Path p;
            const std::string err = resolve(rest, &p);
            if (!err.empty()) return err;
            if (p.steps.back().baseType != reflection::Obj) return "require needs a table: " + rest;
            require_.push_back(p);
        } else if (cmd == "supersede") {
            for (const std::string& a : split(rest, '|')) {
                const std::vector<std::string> parts = split(a, ':');
                if (parts.size() < 3) return "supersede alternative needs kind:prefix:path";
                Alt alt;
                alt.prefix = parts[1] + ":";
                std::string path = parts[2];
                for (size_t k = 3; k < parts.size(); k++) path += ":" + parts[k];
                if (parts[0] == "pair") {
                    alt.kind = kAltSupPair;
                    const size_t comma = path.find(',');
                    if (comma == std::string::npos) return "pair needs two paths";
                    std::string err = resolve(path.substr(0, comma), &alt.a);
                    if (!err.empty()) return err;
                    err = resolve(path.substr(comma + 1), &alt.b);
                    if (!err.empty()) return err;
                } else if (parts[0] == "u64") {
                    alt.kind = kAltSupU64;
                    const std::string err = resolve(path, &alt.a);
                    if (!err.empty()) return err;
                } else if (parts[0] == "str") {
                    alt.kind = kAltSupStr;
                    const std::string err = resolve(path, &alt.a);
                    if (!err.empty()) return err;
                } else {
                    return "unknown supersede alternative " + parts[0];
                }
                supersede_.push_back(alt);
            }
        } else {
            return "unknown rule directive " + cmd;
        }
    }
    return "";
}

// ---------------------------------------------------------------------------
// Record checks and extraction
// ---------------------------------------------------------------------------
int32_t TypeConfig::checkFrame(const uint8_t* frame, size_t len) const {
    if (len < 12 || len - 4 > maxFrame_) return kRejFrameSize;
    if (getU32(frame) != len - 4) return kRejFrameSize;
    if (std::memcmp(frame + 8, fid_, 4) != 0) return kRejFid;
    if ((flags_ & kVerifyBfbs) && schema_) {
        const bool prefixed = flatbuffers::VerifySizePrefixed(*schema_, *schema_->root_table(),
                                                              frame, len, 64, 1000000);
        if (!prefixed &&
            !flatbuffers::Verify(*schema_, *schema_->root_table(), frame + 4, len - 4, 64, 1000000))
            return kRejVerify;
    }
    return 0;
}

bool TypeConfig::leaf(const uint8_t* root, const Path& p, const uint8_t** table,
                      const Step** last) const {
    const flatbuffers::Table* t = flatbuffers::GetRoot<flatbuffers::Table>(root);
    for (size_t i = 0; i + 1 < p.steps.size(); i++) {
        const Step& st = p.steps[i];
        if (st.first) {
            const auto* vec =
                t->GetPointer<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::Table>>*>(
                    st.voffset);
            if (!vec || vec->size() == 0) return false;
            t = vec->Get(0);
        } else {
            t = t->GetPointer<const flatbuffers::Table*>(st.voffset);
        }
        if (!t) return false;
    }
    *table = reinterpret_cast<const uint8_t*>(t);
    *last = &p.steps.back();
    return true;
}

bool TypeConfig::present(const uint8_t* root, const Path& p) const {
    const uint8_t* tb;
    const Step* st;
    if (!leaf(root, p, &tb, &st)) return false;
    const auto* t = reinterpret_cast<const flatbuffers::Table*>(tb);
    return t->GetPointer<const flatbuffers::Table*>(st->voffset) != nullptr;
}

bool TypeConfig::readString(const uint8_t* root, const Path& p, const uint8_t** s,
                            size_t* n) const {
    const uint8_t* tb;
    const Step* st;
    if (!leaf(root, p, &tb, &st) || st->baseType != reflection::String) return false;
    const auto* t = reinterpret_cast<const flatbuffers::Table*>(tb);
    const auto* str = t->GetPointer<const flatbuffers::String*>(st->voffset);
    if (!str) return false;
    *s = reinterpret_cast<const uint8_t*>(str->data());
    *n = str->size();
    return true;
}

namespace {
template <typename T>
T fieldOr(const flatbuffers::Table* t, uint16_t vo, T def) {
    return t->GetField<T>(vo, def);
}
}  // namespace

bool TypeConfig::readI64(const uint8_t* root, const Path& p, int64_t* v) const {
    const uint8_t* tb;
    const Step* st;
    if (!leaf(root, p, &tb, &st)) return false;
    const auto* t = reinterpret_cast<const flatbuffers::Table*>(tb);
    const int64_t d = st->defInt;
    switch (st->baseType) {
        case reflection::Bool:
        case reflection::UByte: *v = fieldOr<uint8_t>(t, st->voffset, uint8_t(d)); return true;
        case reflection::Byte: *v = fieldOr<int8_t>(t, st->voffset, int8_t(d)); return true;
        case reflection::Short: *v = fieldOr<int16_t>(t, st->voffset, int16_t(d)); return true;
        case reflection::UShort: *v = fieldOr<uint16_t>(t, st->voffset, uint16_t(d)); return true;
        case reflection::Int: *v = fieldOr<int32_t>(t, st->voffset, int32_t(d)); return true;
        case reflection::UInt: *v = fieldOr<uint32_t>(t, st->voffset, uint32_t(d)); return true;
        case reflection::Long: *v = fieldOr<int64_t>(t, st->voffset, d); return true;
        case reflection::ULong: *v = int64_t(fieldOr<uint64_t>(t, st->voffset, uint64_t(d))); return true;
        default: return false;
    }
}

bool TypeConfig::readU64(const uint8_t* root, const Path& p, uint64_t* v) const {
    const uint8_t* tb;
    const Step* st;
    if (!leaf(root, p, &tb, &st)) return false;
    if (st->baseType == reflection::ULong) {
        const auto* t = reinterpret_cast<const flatbuffers::Table*>(tb);
        *v = fieldOr<uint64_t>(t, st->voffset, uint64_t(st->defInt));
        return true;
    }
    int64_t x;
    if (!readI64(root, p, &x)) return false;
    if (x < 0) return false;
    *v = uint64_t(x);
    return true;
}

bool TypeConfig::readF64(const uint8_t* root, const Path& p, double* v) const {
    const uint8_t* tb;
    const Step* st;
    if (!leaf(root, p, &tb, &st)) return false;
    const auto* t = reinterpret_cast<const flatbuffers::Table*>(tb);
    if (st->baseType == reflection::Double) {
        *v = fieldOr<double>(t, st->voffset, st->defReal);
        return true;
    }
    if (st->baseType == reflection::Float) {
        *v = fieldOr<float>(t, st->voffset, float(st->defReal));
        return true;
    }
    int64_t x;
    if (!readI64(root, p, &x)) return false;
    *v = double(x);
    return true;
}

bool TypeConfig::readEnumName(const uint8_t* root, const Path& p, const uint8_t** s, size_t* n,
                              uint8_t* scratch, size_t scratchLen) const {
    int64_t value;
    if (!readI64(root, p, &value)) return false;
    const Step& st = p.steps.back();
    if (st.enumIndex < 0 || !schema_->enums() ||
        uint32_t(st.enumIndex) >= schema_->enums()->size()) {
        return false;
    }
    const reflection::Enum* e = schema_->enums()->Get(uint32_t(st.enumIndex));
    const reflection::EnumVal* ev = e->values()->LookupByKey(value);
    if (ev && ev->name()) {
        *s = reinterpret_cast<const uint8_t*>(ev->name()->c_str());
        *n = ev->name()->size();
        return true;
    }
    // Go's generated String(): "<EnumName>(<value>)".
    const char* qualified = e->name()->c_str();
    const char* dot = std::strrchr(qualified, '.');
    const char* base = dot ? dot + 1 : qualified;
    const int w = snprintf(reinterpret_cast<char*>(scratch), scratchLen, "%s(%lld)", base,
                           (long long)value);
    if (w <= 0 || size_t(w) >= scratchLen) return false;
    *s = scratch;
    *n = size_t(w);
    return true;
}

void TypeConfig::extract(const uint8_t* frame, size_t len, Extracted* out, uint8_t* scratch,
                         size_t scratchLen) const {
    *out = Extracted();
    if (!schema_ || len < 12) return;
    const uint8_t* root = frame + 4;  // the FlatBuffer after the size prefix
    size_t used = 0;
    auto take = [&](size_t n) -> uint8_t* {
        if (used + n > scratchLen) return nullptr;
        uint8_t* p = scratch + used;
        used += n;
        return p;
    };
    bool keyed = true;
    for (const Path& p : require_) {
        if (!present(root, p)) {
            keyed = false;
            break;
        }
    }
    // Epoch
    for (size_t ai = 0; keyed && ai < epoch_.size(); ai++) {
        const Alt& a = epoch_[ai];
        if (a.kind == kAltEpochStr) {
            const uint8_t* s;
            size_t n;
            if (!readString(root, a.a, &s, &n)) continue;
            trimSpace(&s, &n);
            if (n == 0) continue;  // Go: empty -> next alternative
            int64_t sec, ms;
            // Go tries the fallback only when the first string is empty; a
            // present but unparsable string leaves the epoch absent.
            if (parseEpochString(s, n, &sec, &ms)) {
                out->hasEpoch = true;
                out->epochSec = sec;
                out->epochMs = ms;
            }
            break;
        } else if (a.kind == kAltEpochF64Floor) {
            double v;
            if (!readF64(root, a.a, &v) || v == 0 || !std::isfinite(v)) continue;
            const double fl = std::floor(v);
            if (fl < -9.2e18 || fl > 9.2e18) continue;
            out->hasEpoch = true;
            out->epochSec = int64_t(fl);
            out->epochMs = int64_t(std::floor(v * 1000.0));
            break;
        } else {
            int64_t v;
            if (!readI64(root, a.a, &v) || v == 0) continue;
            out->hasEpoch = true;
            if (a.kind == kAltEpochI64s) {
                out->epochSec = v;
                out->epochMs = v * 1000;
            } else {
                out->epochMs = v;
                out->epochSec = v >= 0 ? v / 1000 : -((-v + 999) / 1000);
            }
            break;
        }
    }
    // Columns
    for (uint32_t c = 0; keyed && c < nCols_; c++) {
        ColValue& cv = out->cols[c];
        if (int(c) == epochDayCol_) {
            if (out->hasEpoch) {
                uint8_t* p = take(10);
                if (p) {
                    formatEpochDay(out->epochSec, reinterpret_cast<char*>(p));
                    cv.present = true;
                    cv.s = p;
                    cv.n = 10;
                }
            }
            continue;
        }
        for (const Alt& a : cols_[c]) {
            if (a.kind == kAltColU64Pos) {
                uint64_t v;
                if (!readU64(root, a.a, &v) || v == 0) continue;
                cv.present = true;
                cv.isU64 = true;
                cv.u = v;
                break;
            }
            const uint8_t* s;
            size_t n;
            if (a.kind == kAltColStr) {
                if (!readString(root, a.a, &s, &n)) continue;
                trimSpace(&s, &n);
                if (n == 0) continue;
            } else {
                uint8_t* p = take(96);
                if (!p || !readEnumName(root, a.a, &s, &n, p, 96)) continue;
                trimSpace(&s, &n);
                if (n == 0 || (n == 7 && std::memcmp(s, "UNKNOWN", 7) == 0)) continue;
            }
            cv.present = true;
            cv.s = s;
            cv.n = n;
            break;
        }
    }
    for (int oc : objectCols_) {
        if (out->cols[oc].present) {
            out->objectCol = oc;
            break;
        }
    }
    // Supersede identity (record_supersede.go recordSupersedeKey)
    for (const Alt& a : supersede_) {
        if (a.kind == kAltSupPair) {
            const uint8_t *sa, *sb;
            size_t na, nb;
            if (!readString(root, a.a, &sa, &na) || !readString(root, a.b, &sb, &nb)) continue;
            trimSpace(&sa, &na);
            trimSpace(&sb, &nb);
            if (!na || !nb) continue;
            const size_t total = a.prefix.size() + na + 1 + nb;
            uint8_t* p = take(total);
            if (!p) return;
            std::memcpy(p, a.prefix.data(), a.prefix.size());
            std::memcpy(p + a.prefix.size(), sa, na);
            p[a.prefix.size() + na] = 0;
            std::memcpy(p + a.prefix.size() + na + 1, sb, nb);
            out->identity = p;
            out->identityLen = total;
            return;
        }
        if (a.kind == kAltSupU64) {
            uint64_t v;
            if (!readU64(root, a.a, &v) || v == 0) continue;
            char num[24];
            const int w = snprintf(num, sizeof(num), "%llu", (unsigned long long)v);
            const size_t total = a.prefix.size() + size_t(w);
            uint8_t* p = take(total);
            if (!p) return;
            std::memcpy(p, a.prefix.data(), a.prefix.size());
            std::memcpy(p + a.prefix.size(), num, size_t(w));
            out->identity = p;
            out->identityLen = total;
            return;
        }
        const uint8_t* s;
        size_t n;
        if (!readString(root, a.a, &s, &n)) continue;
        trimSpace(&s, &n);
        if (!n) continue;
        const size_t total = a.prefix.size() + n;
        uint8_t* p = take(total);
        if (!p) return;
        std::memcpy(p, a.prefix.data(), a.prefix.size());
        std::memcpy(p + a.prefix.size(), s, n);
        out->identity = p;
        out->identityLen = total;
        return;
    }
}

}  // namespace ps
}  // namespace flatsql
