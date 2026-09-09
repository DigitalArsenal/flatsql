#include "flatsql/record_search.h"
#include <flatbuffers/reflection.h>
#include <algorithm>
#include <cstring>
#include <limits>
#include <locale>
#include <set>
#include <sstream>

namespace flatsql {
namespace {
constexpr size_t maxBytes = 16 * 1024 * 1024;
constexpr size_t maxSchemaBytes = 1024 * 1024;
constexpr size_t maxVisits = 100000;
constexpr unsigned maxDepth = 64;

bool schemaTypeValid(const reflection::Schema& schema, const reflection::Type& type) {
    const auto base = type.base_type();
    if (base <= reflection::None || base >= reflection::Vector64) return false;
    auto referenceValid = [&](reflection::BaseType value) {
        if (value == reflection::Obj)
            return type.index() >= 0 && schema.objects() && size_t(type.index()) < schema.objects()->size();
        if (value == reflection::Union)
            return type.index() >= 0 && schema.enums() && size_t(type.index()) < schema.enums()->size() &&
                   schema.enums()->Get(type.index())->is_union();
        return value > reflection::None && value < reflection::Vector;
    };
    if (base == reflection::Vector || base == reflection::Array) {
        if (!referenceValid(type.element())) return false;
        if (base == reflection::Array && (type.fixed_length() == 0 ||
            type.element() == reflection::String || type.element() == reflection::Union)) return false;
        return true;
    }
    return referenceValid(base);
}

// BFBS verification establishes buffer bounds, but does not validate cross-
// references or layout numbers used by the reflection record verifier.
bool schemaLayoutValid(const reflection::Schema& schema) {
    const uint64_t supportedFeatures = reflection::AdvancedArrayFeatures | reflection::AdvancedUnionFeatures |
        reflection::OptionalScalars | reflection::DefaultVectorsAndStrings;
    if (uint64_t(schema.advanced_features()) & ~supportedFeatures) return false;
    if (!schema.objects() || schema.objects()->size() > 4096 || !schema.root_table() ||
        schema.root_table()->is_struct()) return false;
    bool rootFound = false;
    for (const auto* object : *schema.objects()) {
        rootFound |= object == schema.root_table();
        if (!object->fields() || object->fields()->size() > 4096 ||
            object->bytesize() < 0 || object->bytesize() > 65535) return false;
        std::set<uint16_t> ids;
        for (const auto* field : *object->fields()) {
            if (!field->type() || field->offset64() || !schemaTypeValid(schema, *field->type()) || !ids.insert(field->id()).second)
                return false;
            if (object->is_struct()) {
                const auto base = field->type()->base_type();
                if (base == reflection::String || base == reflection::Vector || base == reflection::Union ||
                    field->offset() >= object->bytesize()) return false;
                if (base == reflection::Obj && !schema.objects()->Get(field->type()->index())->is_struct()) return false;
                if (base == reflection::Array && field->type()->element() == reflection::Obj &&
                    !schema.objects()->Get(field->type()->index())->is_struct()) return false;
                const auto element = base == reflection::Array ? field->type()->element() : base;
                const size_t width = element == reflection::Obj ?
                    size_t(schema.objects()->Get(field->type()->index())->bytesize()) : flatbuffers::GetTypeSize(element);
                const size_t count = base == reflection::Array ? field->type()->fixed_length() : 1;
                if (width == 0 || size_t(field->offset()) + count * width > size_t(object->bytesize())) return false;
            } else if (field->id() > 32765 || field->offset() != 4 + 2 * field->id()) return false;
            const auto& type = *field->type();
            if (type.base_type() == reflection::Union ||
                (type.base_type() == reflection::Vector && type.element() == reflection::Union)) {
                const reflection::Field* tag = nullptr;
                for (const auto* candidate : *object->fields())
                    if (field->id() > 0 && candidate->id() == field->id() - 1) tag = candidate;
                if (!tag || !tag->type()) return false;
                if (type.base_type() == reflection::Union && tag->type()->base_type() != reflection::UType) return false;
                if (type.base_type() == reflection::Vector &&
                    (tag->type()->base_type() != reflection::Vector || tag->type()->element() != reflection::UType)) return false;
            }
        }
    }
    if (schema.enums()) for (const auto* enumeration : *schema.enums()) {
        if (!enumeration->values() || enumeration->values()->size() > 65536) return false;
        if (!enumeration->is_union()) continue;
        for (const auto* value : *enumeration->values()) {
            if (value->value() == 0) continue;
            if (!value->union_type() || !schemaTypeValid(schema, *value->union_type())) return false;
            const auto base = value->union_type()->base_type();
            if (base != reflection::Obj && base != reflection::String) return false;
        }
    }
    return rootFound;
}

class SearchText {
    const reflection::Schema& schema;
    std::ostringstream text;
    size_t visits = 0;
    bool good = true;

    bool visit(unsigned depth) {
        good = good && depth <= maxDepth && ++visits <= maxVisits && text.tellp() <= std::streamoff(maxBytes);
        return good;
    }
    const uint8_t* indirect(const uint8_t* data) const {
        return data + flatbuffers::ReadScalar<flatbuffers::uoffset_t>(data);
    }
    size_t elementSize(reflection::BaseType base, int index) const {
        if (base == reflection::Obj && schema.objects()->Get(index)->is_struct())
            return size_t(schema.objects()->Get(index)->bytesize());
        return flatbuffers::GetTypeSize(base);
    }
    void scalar(reflection::BaseType base, const uint8_t* data) {
        switch (base) {
        case reflection::Bool: text << (flatbuffers::ReadScalar<uint8_t>(data) ? "true" : "false"); break;
        case reflection::Byte: text << +flatbuffers::ReadScalar<int8_t>(data); break;
        case reflection::UType: case reflection::UByte: text << +flatbuffers::ReadScalar<uint8_t>(data); break;
        case reflection::Short: text << flatbuffers::ReadScalar<int16_t>(data); break;
        case reflection::UShort: text << flatbuffers::ReadScalar<uint16_t>(data); break;
        case reflection::Int: text << flatbuffers::ReadScalar<int32_t>(data); break;
        case reflection::UInt: text << flatbuffers::ReadScalar<uint32_t>(data); break;
        case reflection::Long: text << flatbuffers::ReadScalar<int64_t>(data); break;
        case reflection::ULong: text << flatbuffers::ReadScalar<uint64_t>(data); break;
        case reflection::Float: text << flatbuffers::ReadScalar<float>(data); break;
        case reflection::Double: text << flatbuffers::ReadScalar<double>(data); break;
        default: good = false; return;
        }
        text << '\n';
    }
    void value(reflection::BaseType base, int index, const uint8_t* data, unsigned depth) {
        if (!visit(depth)) return;
        if (base == reflection::String) {
            const auto* string = reinterpret_cast<const flatbuffers::String*>(indirect(data));
            if (string->size() > maxBytes || text.tellp() + std::streamoff(string->size()) > std::streamoff(maxBytes)) {
                good = false; return;
            }
            text.write(string->c_str(), string->size()); text << '\n';
        } else if (base == reflection::Obj) {
            const auto* child = schema.objects()->Get(index);
            object(*child, child->is_struct() ? data : indirect(data), depth + 1);
        } else scalar(base, data);
    }
    void unionValue(const reflection::Type& type, uint8_t tag, const uint8_t* data, unsigned depth) {
        if (!visit(depth)) return;
        if (tag == 0) return;
        const auto* choice = schema.enums()->Get(type.index())->values()->LookupByKey(tag);
        if (!choice || !choice->union_type()) { good = false; return; }
        const auto* selected = choice->union_type();
        if (selected->base_type() == reflection::Obj && schema.objects()->Get(selected->index())->is_struct())
            object(*schema.objects()->Get(selected->index()), indirect(data), depth + 1);
        else value(selected->base_type(), selected->index(), data, depth + 1);
    }
    const uint8_t* unionTags(const reflection::Object& definition, const flatbuffers::Table& table,
                             const reflection::Field& field) {
        if (field.id() == 0) return nullptr;
        for (const auto* candidate : *definition.fields())
            if (candidate->id() == field.id() - 1) return table.GetAddressOf(candidate->offset());
        return nullptr;
    }
    void object(const reflection::Object& definition, const uint8_t* data, unsigned depth) {
        if (!visit(depth)) return;
        const auto* table = reinterpret_cast<const flatbuffers::Table*>(data);
        for (const auto* field : *definition.fields()) {
            if (!good) return;
            if ((field->attributes() && field->attributes()->LookupByKey("encrypted")) ||
                (field->name() && field->name()->size() && field->name()->Get(0) == '_')) continue;
            const auto* address = definition.is_struct() ? data + field->offset() : table->GetAddressOf(field->offset());
            if (!address) continue;
            const auto& type = *field->type();
            if (type.base_type() == reflection::Vector || type.base_type() == reflection::Array) {
                // Byte vectors are opaque payloads, not searchable text.
                if (type.element() == reflection::Byte || type.element() == reflection::UByte) continue;
                const auto* vector = type.base_type() == reflection::Vector ?
                    reinterpret_cast<const flatbuffers::VectorOfAny*>(indirect(address)) : nullptr;
                const size_t count = vector ? vector->size() : type.fixed_length();
                if (count > maxVisits - visits) { good = false; return; }
                const uint8_t* elements = vector ? vector->Data() : address;
                const uint8_t* tags = nullptr;
                if (type.element() == reflection::Union) {
                    const auto* tagged = unionTags(definition, *table, *field);
                    if (!tagged) { good = false; return; }
                    const auto* kinds = reinterpret_cast<const flatbuffers::VectorOfAny*>(indirect(tagged));
                    if (kinds->size() != count) { good = false; return; }
                    tags = kinds->Data();
                }
                const size_t stride = elementSize(type.element(), type.index());
                for (size_t i = 0; i < count && good; ++i) {
                    if (tags) unionValue(type, tags[i], elements + i * stride, depth + 1);
                    else value(type.element(), type.index(), elements + i * stride, depth + 1);
                }
            } else if (type.base_type() == reflection::Union) {
                const auto* tag = unionTags(definition, *table, *field);
                if (!tag) { good = false; return; }
                unionValue(type, *tag, address, depth + 1);
            } else value(type.base_type(), type.index(), address, depth + 1);
        }
    }
public:
    explicit SearchText(const reflection::Schema& schema) : schema(schema) {
        text.imbue(std::locale::classic());
        text.precision(std::numeric_limits<double>::max_digits10);
    }
    bool read(const uint8_t* record, std::string& output) {
        object(*schema.root_table(), indirect(record), 0);
        if (!good || text.tellp() > std::streamoff(maxBytes)) return false;
        output = text.str(); return true;
    }
};
}

bool reflectedRecordSearchText(const uint8_t* binarySchema, size_t schemaLength,
                               const std::string& fileId,
                               const uint8_t* record, size_t recordLength,
                               std::string& output, std::string* error) {
    output.clear();
    auto fail = [&](const char* reason) { if (error) *error = reason; return false; };
    if (!binarySchema || schemaLength < 8 || schemaLength > maxSchemaBytes)
        return fail("Binary search schema exceeds bounds");
    flatbuffers::Verifier schemaVerifier(binarySchema, schemaLength, maxDepth, maxVisits);
    if (!reflection::VerifySchemaBuffer(schemaVerifier)) return fail("Invalid binary search schema");
    const auto* schema = reflection::GetSchema(binarySchema);
    if (!schema->file_ident() || fileId.size() != 4 || schema->file_ident()->str() != fileId)
        return fail("Binary search schema identifier differs from registered table");
    if (!schemaLayoutValid(*schema)) return fail("Unsupported binary search schema layout");
    if (!record || recordLength < 8 || recordLength > maxBytes) return fail("Search record exceeds bounds");
    const bool prefixed = recordLength >= 12 && flatbuffers::ReadScalar<uint32_t>(record) == recordLength - 4 &&
        std::memcmp(record + 8, fileId.data(), 4) == 0;
    // A compiler-emitted size prefix participates in alignment. An external
    // transport length around a bare FlatBuffer does not. Verify each supported
    // framing form against its original alignment origin before extracting.
    bool verified = prefixed && flatbuffers::VerifySizePrefixed(*schema, *schema->root_table(),
        record, recordLength, maxDepth, maxVisits);
    if (prefixed) { record += 4; recordLength -= 4; }
    if (std::memcmp(record + 4, fileId.data(), 4) != 0) return fail("Record identifier differs from search schema");
    if (!verified && !flatbuffers::Verify(*schema, *schema->root_table(), record, recordLength, maxDepth, maxVisits))
        return fail("Invalid FlatBuffer search record");
    if (!SearchText(*schema).read(record, output)) return fail("Search extraction exceeded its traversal or text limit");
    return true;
}
}
