#include "flatsql/database.h"
#include "flatsql/record_search.h"
#include <flatbuffers/reflection_generated.h>
#include <iostream>
#include <limits>
#include <stdexcept>

using namespace flatsql;
using namespace reflection;
using flatbuffers::Offset;
using flatbuffers::FlatBufferBuilder;
static void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

static std::vector<uint8_t> schemaBytes() {
    FlatBufferBuilder b;
    auto field = [&](const char* name, uint16_t id, BaseType base, BaseType element = None,
                     int index = -1, bool encrypted = false) {
        auto type = CreateType(b, base, element, index);
        auto key = b.CreateString(name);
        Offset<flatbuffers::Vector<Offset<KeyValue>>> attributes;
        if (encrypted) {
            auto attribute = CreateKeyValue(b, b.CreateString("encrypted"), b.CreateString("true"));
            attributes = b.CreateVector(std::vector<Offset<KeyValue>>{attribute});
        }
        return CreateField(b, key, type, id, 4 + id * 2, 0, 0, false, false, false, attributes);
    };
    std::vector<Offset<Field>> children{field("name", 0, String), field("secret", 1, String, None, -1, true)};
    const auto child = CreateObject(b, b.CreateString("Child"), b.CreateVectorOfSortedTables(&children));
    std::vector<Offset<Field>> fields{
        field("first", 0, String), field("comments", 1, Vector, String),
        field("nested", 2, Obj, None, 0), field("last", 3, String),
        field("secret", 4, String, None, -1, true), field("opaque", 5, Vector, UByte),
        field("unsigned", 6, ULong), field("choice_type", 7, UType, None, 0),
        field("choice", 8, Union, None, 0), field("tail", 9, String)
    };
    const auto root = CreateObject(b, b.CreateString("Record"), b.CreateVectorOfSortedTables(&fields));
    const auto absent = CreateEnumVal(b, b.CreateString("NONE"), 0, CreateType(b, None));
    const auto present = CreateEnumVal(b, b.CreateString("Child"), 1, CreateType(b, Obj, None, 0));
    const auto choices = CreateEnum(b, b.CreateString("Choice"),
        b.CreateVector(std::vector<Offset<EnumVal>>{absent, present}), true, CreateType(b, UType));
    const auto result = CreateSchema(b, b.CreateVector(std::vector<Offset<Object>>{child, root}),
        b.CreateVector(std::vector<Offset<Enum>>{choices}), b.CreateString("FTSR"), 0, root);
    FinishSchemaBuffer(b, result);
    return {b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize()};
}

static std::vector<uint8_t> recordBytes(bool sizePrefixed = false) {
    FlatBufferBuilder b;
    const auto first = b.CreateString("prefix-token");
    const auto comment = b.CreateString("comment-token");
    const auto comments = b.CreateVector(std::vector<Offset<flatbuffers::String>>{comment});
    const auto name = b.CreateString("nested-token");
    const auto secret = b.CreateString("hidden-token");
    const auto childStart = b.StartTable();
    b.AddOffset(4, name); b.AddOffset(6, secret);
    const auto child = Offset<flatbuffers::Table>(b.EndTable(childStart));
    const auto last = b.CreateString("late-hash-token");
    const auto tail = b.CreateString("after-union-token");
    const std::string opaqueText = "opaque-token";
    const auto opaque = b.CreateVector(reinterpret_cast<const uint8_t*>(opaqueText.data()), opaqueText.size());
    const auto start = b.StartTable();
    b.AddOffset(4, first); b.AddOffset(6, comments); b.AddOffset(8, child); b.AddOffset(10, last);
    b.AddOffset(12, secret); b.AddOffset(14, opaque);
    b.AddElement<uint64_t>(16, std::numeric_limits<uint64_t>::max(), 0);
    b.AddElement<uint8_t>(18, 1, 0); b.AddOffset(20, child); b.AddOffset(22, tail);
    const auto root = Offset<flatbuffers::Table>(b.EndTable(start));
    if (sizePrefixed) b.FinishSizePrefixed(root, "FTSR");
    else b.Finish(root, "FTSR");
    return {b.GetBufferPointer(), b.GetBufferPointer() + b.GetSize()};
}

int main() {
    try {
        auto schema = schemaBytes();
        auto record = recordBytes();
        DatabaseSchema shortSchema;
        shortSchema.name = "projected";
        TableDef definition;
        definition.name = "Record";
        definition.columns = {{"first", ValueType::String}};
        shortSchema.tables.push_back(definition);
        FlatSQLDatabase db(shortSchema);
        db.registerFileId("FTSR", "Record");
        const auto old = db.query("SELECT flatsql_record_text('Record',?)", {record});
        require(std::get<std::string>(old.rows[0][0]) == "prefix-token\n", "legacy extraction must remain compatible");
        const auto result = db.query("SELECT flatsql_record_text('Record',?,?)", {record, schema});
        const auto text = std::get<std::string>(result.rows[0][0]);
        for (const auto* token : {"prefix-token", "comment-token", "nested-token", "late-hash-token",
                                   "after-union-token", "18446744073709551615"})
            require(text.find(token) != std::string::npos, "complete schema lost a searchable field");
        for (const auto* token : {"hidden-token", "opaque-token"})
            require(text.find(token) == std::string::npos, "protected or opaque bytes were indexed");
        std::vector<uint8_t> framed(record.size() + 4);
        flatbuffers::WriteScalar<uint32_t>(framed.data(), uint32_t(record.size()));
        std::copy(record.begin(), record.end(), framed.begin() + 4);
        require(db.query("SELECT flatsql_record_text('Record',?,?)", {framed, schema}).rows == result.rows,
                "externally framed input differs");
        require(db.query("SELECT flatsql_record_text('Record',?,?)", {recordBytes(true), schema}).rows == result.rows,
                "compiler-emitted size prefix changed alignment or extraction");
        db.query("CREATE VIRTUAL TABLE search_records USING fts5(text)");
        db.query("INSERT INTO search_records(text) VALUES(flatsql_record_text('Record',?,?))", {record, schema});
        require(db.query("SELECT rowid FROM search_records WHERE search_records MATCH 'late AND nested'").rows.size() == 1,
                "late and nested fields did not reach the real FTS index");
        std::string output, error;
        auto checkBad = [&](const std::vector<uint8_t>& badSchema, const std::vector<uint8_t>& badRecord, const std::string& id = "FTSR") {
            output = "stale output";
            require(!reflectedRecordSearchText(badSchema.data(), badSchema.size(), id,
                badRecord.data(), badRecord.size(), output, &error), "malformed input accepted");
            require(output.empty() && !error.empty(), "failed extraction retained partial text");
        };
        checkBad(schema, record, "NOPE");
        auto shortSchemaBytes = schema; shortSchemaBytes.resize(12); checkBad(shortSchemaBytes, record);
        auto badRecord = record; flatbuffers::WriteScalar<uint32_t>(badRecord.data(), 0xffffffff); checkBad(schema, badRecord);
        auto badSchema = schema;
        auto* nested = const_cast<flatbuffers::Table*>(reinterpret_cast<const flatbuffers::Table*>(
            GetSchema(badSchema.data())->root_table()->fields()->LookupByKey("nested")->type()));
        flatbuffers::WriteScalar<int32_t>(nested->GetAddressOf(Type::VT_INDEX), 99999);
        checkBad(badSchema, record);
        // FlatBuffers permits unused alignment padding at the end; truncate
        // through actual content instead of assuming every trailing byte matters.
        for (size_t size : {size_t(0), size_t(4), size_t(8), record.size() / 2})
            checkBad(schema, std::vector<uint8_t>(record.begin(), record.begin() + size));
        require(db.query("SELECT 1").rows.size() == 1, "invalid search poisoned the database");
        std::cout << "complete FlatBuffer search checks passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n'; return 1;
    }
}
