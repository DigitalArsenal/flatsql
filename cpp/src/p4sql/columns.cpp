// FlatSQL store format 4, SQL surface: format 1's relation columns and their
// values (CONTRACT.md §3.9 "Columns", C-9).
//
// The column list is what format 1's engine declares for the type (SDN
// storage/enginecatalog): FlatBuffers vtable slots are positional, so the
// projection is the LEADING RUN of root fields its engine can represent, in
// declaration order (scalars, enums as their underlying scalar, strings,
// byte vectors); a union's discriminator (<NAME>_type) ends the run with a
// column; a table, struct, union value or other vector ends it without one.
// When not even the first field is representable, slot 0 becomes a one-byte
// placeholder column (the >= 1 column invariant).
//
// Two standards keep table graphs that format 1 pins as cross-host contracts
// (SDN storage/engine_records.go engineRecordSchema and engineTBSTableGraph):
//   OMM  every root field, not the leading run;
//   OMM, TBS  their enum-typed fields name the enum type unresolved, so
//        format 1 reads them as NULL (as it reads tables and vectors).
//
// Values are read exactly as format 1's generic extractor reads them
// (database.cpp readGenericColumnValue, sqlite_vtab.cpp setResultFromValue):
// an absent field is NULL (never its default), a uint32 is narrowed to a
// signed int, every read is bounds-checked against the record.
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

struct Pin {
    const char* type;
    bool allFields;     // every root field (not the leading run)
    bool enumsOpaque;   // enum-typed fields read as NULL
};
const Pin kPins[] = {
    {"OMM", true, true},
    {"TBS", false, true},
};

const Pin* pinOf(const std::string& type) {
    for (const Pin& p : kPins)
        if (type == p.type) return &p;
    return nullptr;
}

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

bool format1Columns(const std::string& type, const uint8_t* bfbs, size_t n, std::vector<Column>* out,
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
    const Pin* pin = pinOf(type);
    for (const reflection::Field* f : fields) {
        const reflection::Type* t = f->type();
        const reflection::BaseType bt = t->base_type();
        Column c;
        c.name = f->name()->str();
        c.slot = uint16_t(out->size());   // format 1: field id = position in its table
        if (pin && pin->allFields) {
            if (bt == reflection::UType) continue;   // the pinned graph names a union once
            if (bt >= reflection::Bool && bt <= reflection::Double)
                c.kind = (pin->enumsOpaque && t->index() >= 0) ? kColNull : scalarKind(bt);
            else if (bt == reflection::String) c.kind = kColText;
            else if (isByteVector(t)) c.kind = kColBlob;
            else c.kind = kColNull;
            out->push_back(std::move(c));
            continue;
        }
        if (bt == reflection::UType) {   // <NAME>_type: representable, ends the run
            c.kind = kColU8;
            out->push_back(std::move(c));
            break;
        }
        if (bt >= reflection::Bool && bt <= reflection::Double) {
            c.kind = (pin && pin->enumsOpaque && t->index() >= 0) ? kColNull : scalarKind(bt);
        } else if (bt == reflection::String) {
            c.kind = kColText;
        } else if (isByteVector(t)) {
            c.kind = kColBlob;
        } else {
            break;
        }
        out->push_back(std::move(c));
    }
    if (out->empty()) {
        Column c;
        c.name = fields[0]->name()->str();
        c.slot = 0;
        c.kind = kColU8;
        c.placeholder = true;
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
        sqlite3_result_null(ctx);
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
        case kColU32: sqlite3_result_int(ctx, static_cast<int>(rd<uint32_t>(p))); break;   // as format 1
        case kColI64: sqlite3_result_int64(ctx, rd<int64_t>(p)); break;
        case kColU64: sqlite3_result_int64(ctx, sqlite3_int64(rd<uint64_t>(p))); break;
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
