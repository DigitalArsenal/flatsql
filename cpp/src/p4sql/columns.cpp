// SQL scalar columns come from the registered binary schema. Complex fields
// remain available in the complete _data FlatBuffer; they never truncate the
// remaining scalar columns or create fake placeholder values.
#include <algorithm>
#include <cstring>

#include "flatbuffers/flatbuffers.h"
#include "flatbuffers/reflection_generated.h"
#include "internal.h"

namespace flatsql {
namespace p4sql {

const char* Column::sqlType() const {
    switch (kind) {
        case kColNull: return "NULL";
        case kColF32:
        case kColF64: return "REAL";
        case kColText: return "TEXT";
        case kColBlob: return "BLOB";
        default: return "INTEGER";
    }
}

namespace {

ColKind scalarKind(reflection::BaseType bt) {
    switch (bt) {
        case reflection::Bool: return kColBool;
        case reflection::Byte: return kColI8;
        case reflection::UByte: return kColU8;
        case reflection::Short: return kColI16;
        case reflection::UShort: return kColU16;
        case reflection::Int: return kColI32;
        case reflection::UInt: return kColU32;
        case reflection::Long: return kColI64;
        case reflection::ULong: return kColU64;
        case reflection::Float: return kColF32;
        case reflection::Double: return kColF64;
        default: return kColNull;
    }
}

// [ubyte] or [byte]; a vector of a byte-backed enum is not one (format 1
// reads only the plain byte vector types as a BLOB).
bool isByteVector(const reflection::Type* t) {
    return t->base_type() == reflection::Vector &&
           (t->element() == reflection::UByte || t->element() == reflection::Byte) && t->index() < 0;
}

}  // namespace

bool schemaColumns(const std::string& type, const uint8_t* bfbs, size_t n, std::vector<Column>* out,
                    std::string* err) {
    out->clear();
    flatbuffers::Verifier v(bfbs, n);
    if (!bfbs || !reflection::VerifySchemaBuffer(v)) {
        if (err) *err = "binary schema of " + type + " does not verify";
        return false;
    }
    const reflection::Schema* s = reflection::GetSchema(bfbs);
    const reflection::Object* root = s->root_table();
    if (!root || !root->fields() || root->fields()->size() == 0) {
        if (err) *err = "binary schema of " + type + " has no root table fields";
        return false;
    }
    std::vector<const reflection::Field*> fields(root->fields()->begin(), root->fields()->end());
    std::sort(fields.begin(), fields.end(),
              [](const reflection::Field* a, const reflection::Field* b) { return a->id() < b->id(); });
    for (const reflection::Field* f : fields) {
        const auto* t = f->type();
        const auto bt = t->base_type();
        Column c;
        c.name = f->name()->str();
        c.slot = f->id();
        c.defaultInt = f->default_integer();
        c.defaultReal = f->default_real();
        c.optional = f->optional();
        if (bt == reflection::UType) c.kind = kColU8;
        else if (bt >= reflection::Bool && bt <= reflection::Double) c.kind = scalarKind(bt);
        else if (bt == reflection::String) c.kind = kColText;
        else if (isByteVector(t)) c.kind = kColBlob;
        else continue;
        out->push_back(std::move(c));
    }
    return true;
}

// ---------------------------------------------------------------------------
// Values: format 1's generic extractor (database.cpp), check for check.
// ---------------------------------------------------------------------------
namespace {

template <typename T>
T rd(const uint8_t* p) {
    T v;
    std::memcpy(&v, p, sizeof(T));
    return flatbuffers::EndianScalar(v);
}

// The record's root, vtable and vtable size; false when malformed.
bool layout(const uint8_t* data, size_t length, size_t* root, size_t* vtable, uint16_t* vtSize) {
    if (!data || length < sizeof(uint32_t)) return false;
    const uint32_t r = rd<uint32_t>(data);
    if (r > length || length - r < sizeof(int32_t)) return false;
    const int64_t vt = int64_t(r) - rd<int32_t>(data + r);
    if (vt < 0 || uint64_t(vt) > length) return false;
    const size_t vtu = size_t(vt);
    if (vtu > length || length - vtu < sizeof(uint16_t)) return false;
    const uint16_t size = rd<uint16_t>(data + vtu);
    if (size < 4 || size > length - vtu) return false;
    *root = r;
    *vtable = vtu;
    *vtSize = size;
    return true;
}

uint16_t fieldOffset(const uint8_t* data, size_t length, size_t vtable, uint16_t vtSize, size_t slot) {
    const size_t entry = vtable + 4 + slot * sizeof(uint16_t);
    if (entry > length || sizeof(uint16_t) > length - entry) return 0;
    if (entry + sizeof(uint16_t) > vtable + vtSize) return 0;
    return rd<uint16_t>(data + entry);
}

// The [u32 len][bytes] a string or vector field refers to.
bool lengthPrefixed(const uint8_t* data, size_t length, size_t at, const uint8_t** p, uint32_t* n) {
    if (at > length || sizeof(uint32_t) > length - at) return false;
    const uint32_t rel = rd<uint32_t>(data + at);
    if (rel > length || at > length - rel) return false;
    const size_t target = at + rel;
    if (target > length || sizeof(uint32_t) > length - target) return false;
    const uint32_t len = rd<uint32_t>(data + target);
    if (target + sizeof(uint32_t) > length || len > length - target - sizeof(uint32_t)) return false;
    *p = data + target + sizeof(uint32_t);
    *n = len;
    return true;
}

size_t widthOf(ColKind k) {
    switch (k) {
        case kColBool:
        case kColI8:
        case kColU8: return 1;
        case kColI16:
        case kColU16: return 2;
        case kColI32:
        case kColU32:
        case kColF32: return 4;
        case kColI64:
        case kColU64:
        case kColF64: return 8;
        default: return 0;
    }
}

}  // namespace

void resultColumn(sqlite3_context* ctx, const Column& c, const uint8_t* data, size_t n) {
    size_t root = 0, vt = 0;
    uint16_t vtSize = 0;
    if (c.kind == kColNull || !layout(data, n, &root, &vt, &vtSize)) {
        sqlite3_result_null(ctx);
        return;
    }
    const uint16_t fo = fieldOffset(data, n, vt, vtSize, c.slot);
    if (fo == 0) {
        if (c.optional || c.kind == kColText || c.kind == kColBlob) sqlite3_result_null(ctx);
        else if (c.kind == kColF32 || c.kind == kColF64) sqlite3_result_double(ctx, c.defaultReal);
        else sqlite3_result_int64(ctx, c.defaultInt);
        return;
    }
    const size_t at = root + fo;
    if (at >= n) {
        sqlite3_result_null(ctx);
        return;
    }
    if (c.kind == kColText || c.kind == kColBlob) {
        const uint8_t* p;
        uint32_t len;
        if (!lengthPrefixed(data, n, at, &p, &len)) sqlite3_result_null(ctx);
        else if (c.kind == kColText) sqlite3_result_text(ctx, reinterpret_cast<const char*>(p), int(len), SQLITE_TRANSIENT);
        else sqlite3_result_blob(ctx, p, int(len), SQLITE_TRANSIENT);
        return;
    }
    const size_t w = widthOf(c.kind);
    if (at > n || w > n - at) {
        sqlite3_result_null(ctx);
        return;
    }
    const uint8_t* p = data + at;
    switch (c.kind) {
        case kColBool: sqlite3_result_int(ctx, rd<uint8_t>(p) != 0 ? 1 : 0); break;
        case kColI8: sqlite3_result_int(ctx, rd<int8_t>(p)); break;
        case kColU8: sqlite3_result_int(ctx, rd<uint8_t>(p)); break;
        case kColI16: sqlite3_result_int(ctx, rd<int16_t>(p)); break;
        case kColU16: sqlite3_result_int(ctx, rd<uint16_t>(p)); break;
        case kColI32: sqlite3_result_int(ctx, rd<int32_t>(p)); break;
        case kColU32: sqlite3_result_int64(ctx, rd<uint32_t>(p)); break;
        case kColI64: sqlite3_result_int64(ctx, rd<int64_t>(p)); break;
        case kColU64:
            if (rd<uint64_t>(p) > uint64_t(INT64_MAX))
                sqlite3_result_error(ctx, "uint64 exceeds SQL int64 range; select _data for the original FlatBuffer", -1);
            else sqlite3_result_int64(ctx, sqlite3_int64(rd<uint64_t>(p)));
            break;
        case kColF32: sqlite3_result_double(ctx, double(rd<float>(p))); break;
        case kColF64: sqlite3_result_double(ctx, rd<double>(p)); break;
        default: sqlite3_result_null(ctx); break;
    }
}

void payloadOf(const uint8_t fid[4], const uint8_t* d, size_t n, const uint8_t** p, size_t* len) {
    if (d && n >= 12 && uint64_t(rd<uint32_t>(d)) + 4 == uint64_t(n) && std::memcmp(d + 8, fid, 4) == 0) {
        *p = d + 4;
        *len = n - 4;
        return;
    }
    *p = d;
    *len = n;
}

}  // namespace p4sql
}  // namespace flatsql
