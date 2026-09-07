// Behavioral reference: https://www.sqlite.org/fts5.html sections 3 and 4.
// Exercise the actual FlatSQL connection, not the system SQLite library.
#include "flatsql/sqlite_engine.h"
#include "flatsql/database.h"
#include "flatsql/schema_parser.h"
#include <flatbuffers/flatbuffers.h>
#include <iostream>
#include <stdexcept>

using namespace flatsql;
static void require(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}
int main() {
    SQLiteEngine engine;
    StreamingFlatBufferStore store;
    TableDef definition; definition.name = "Sample";
    definition.columns = {{"name", ValueType::String}, {"number", ValueType::UInt32},
                          {"secret", ValueType::String}, {"raw", ValueType::Bytes}};
    definition.columns[2].encrypted = true;
    engine.registerSource("Sample", &store, &definition, "$TST", {});
    flatbuffers::FlatBufferBuilder builder;
    auto name = builder.CreateString("München payload");
    auto secret = builder.CreateString("hidden-search-secret");
    auto raw = builder.CreateVector(std::vector<uint8_t>{1,2,3});
    auto start = builder.StartTable();
    builder.AddOffset(4, name); builder.AddElement<uint32_t>(6, 12345, 0);
    builder.AddOffset(8, secret); builder.AddOffset(10, raw);
    auto root = flatbuffers::Offset<flatbuffers::Table>(builder.EndTable(start));
    builder.Finish(root, "$TST");
    std::vector<uint8_t> record(builder.GetBufferPointer(), builder.GetBufferPointer()+builder.GetSize());
    auto extracted = engine.execute("SELECT flatsql_record_text(?,?)", {std::string("Sample"), record});
    auto text = std::get<std::string>(extracted.rows[0][0]);
    require(text == "München payload\n12345\n", "extract text and numeric fields without encrypted or opaque bytes");
    std::vector<uint8_t> framed(record.size()+4);
    flatbuffers::WriteScalar<uint32_t>(framed.data(), static_cast<uint32_t>(record.size()));
    std::copy(record.begin(),record.end(),framed.begin()+4);
    require(engine.execute("SELECT flatsql_record_text('Sample',?)", {framed}).rows == extracted.rows, "size-prefixed extraction");
    auto forward = record;
    const auto rootOffset = flatbuffers::ReadScalar<uint32_t>(forward.data());
    const auto vtableOffset = rootOffset - flatbuffers::ReadScalar<int32_t>(forward.data()+rootOffset);
    const auto vtableBytes = flatbuffers::ReadScalar<uint16_t>(forward.data()+vtableOffset);
    forward.insert(forward.end(), record.begin()+vtableOffset, record.begin()+vtableOffset+vtableBytes);
    flatbuffers::WriteScalar<int32_t>(forward.data()+rootOffset, static_cast<int32_t>(rootOffset)-static_cast<int32_t>(record.size()));
    require(engine.execute("SELECT flatsql_record_text('Sample',?)", {forward}).rows == extracted.rows, "valid forward vtable is readable");
    DatabaseSchema lateSchema; lateSchema.name = "late-identity"; lateSchema.tables.push_back(definition);
    FlatSQLDatabase lateDatabase(lateSchema);
    lateDatabase.query("SELECT 1");
    lateDatabase.registerFileId("$TST", "Sample");
    require(lateDatabase.query("SELECT flatsql_record_text('Sample',?)", {record}).rows == extracted.rows, "late file identifier registration reaches search extraction");
    FlatSQLDatabase viewDatabase(lateSchema);
    viewDatabase.registerFileId("$TST", "Sample");
    viewDatabase.registerSource("provider");
    viewDatabase.createUnifiedViews();
    require(viewDatabase.query("SELECT flatsql_record_text('Sample',?)", {record}).rows == extracted.rows, "unified view retains its record schema");
    auto malformed = record; flatbuffers::WriteScalar<uint32_t>(malformed.data(),0xffffffff);
    QueryResult ignored; std::string error;
    require(!engine.executeNoThrow("SELECT flatsql_record_text('Sample',?)", {malformed}, ignored, &error), "reject malformed FlatBuffer");
    require(!engine.executeNoThrow("SELECT flatsql_record_text('Missing',?)", {record}, ignored, &error), "reject unknown schemas");
    SQLiteEngine moved(std::move(engine));
    require(moved.execute("SELECT flatsql_record_text('Sample',?)", {record}).rows == extracted.rows, "function survives engine move");
    engine = std::move(moved);
    require(engine.execute("SELECT flatsql_record_text('Sample',?)", {record}).rows == extracted.rows, "function survives move assignment");

    // Live OMM archives contain non-default enums. An unresolved enum must
    // never be interpreted as a string offset; explicit enum types retain
    // their scalar width, and scalar defaults do not change that width.
    const auto typedSchema = SchemaParser::parseIDL(R"(
        enum Mode : ubyte { NONE, PASSIVE, ACTIVE }
        table Typed {
            name:string;
            known:Mode = NONE;
            external:UnresolvedMode = DEFAULT;
            count:uint = 0;
            samples:[double];
            child:Nested;
        }
    )");
    require(typedSchema.tables[0].columns[1].type == ValueType::UInt8, "declared enum retains its integer width");
    require(typedSchema.tables[0].columns[2].type == ValueType::Null, "unresolved enum has no assumed wire layout");
    require(typedSchema.tables[0].columns[3].type == ValueType::UInt32, "scalar default is not part of its type");
    flatbuffers::FlatBufferBuilder typedBuilder;
    const auto typedName = typedBuilder.CreateString("Active payload");
    const auto samples = typedBuilder.CreateVector(std::vector<double>{1.0, 2.0});
    const auto childStart = typedBuilder.StartTable();
    typedBuilder.AddElement<uint32_t>(4, 77, 0);
    const auto child = flatbuffers::Offset<flatbuffers::Table>(typedBuilder.EndTable(childStart));
    const auto typedStart = typedBuilder.StartTable();
    typedBuilder.AddOffset(4, typedName);
    typedBuilder.AddElement<uint8_t>(6, 2, 0);
    typedBuilder.AddElement<uint8_t>(8, 3, 0);
    typedBuilder.AddElement<uint32_t>(10, 42, 0);
    typedBuilder.AddOffset(12, samples);
    typedBuilder.AddOffset(14, child);
    typedBuilder.Finish(flatbuffers::Offset<flatbuffers::Table>(typedBuilder.EndTable(typedStart)), "$TYP");
    const std::vector<uint8_t> typedRecord(typedBuilder.GetBufferPointer(), typedBuilder.GetBufferPointer()+typedBuilder.GetSize());
    FlatSQLDatabase typedDatabase(typedSchema);
    typedDatabase.registerFileId("$TYP", "Typed");
    require(std::get<std::string>(typedDatabase.query("SELECT flatsql_record_text('Typed',?)", {typedRecord}).rows[0][0]) == "Active payload\n2\n42\n",
        "search reads typed scalars and skips unresolved layouts and non-text vectors");

    engine.execute("CREATE VIRTUAL TABLE offering_fts USING fts5(node UNINDEXED, object_id, name, description, tokenize='unicode61')");
    for (int i = 1; i <= 205; ++i) {
        engine.execute("INSERT INTO offering_fts(rowid,node,object_id,name,description) VALUES(?,?,?,?,?)",
            {int64_t(i), std::string("provider-a"), std::to_string(i), std::string("Orbit object"),
             i == 205 ? std::string("München optical payload") : std::string("Catalog entry")});
    }
    engine.execute("INSERT INTO offering_fts(rowid,node,object_id,name,description) VALUES(?,?,?,?,?)",
        {int64_t(206), std::string("provider-b"), std::string("205"), std::string("Other object"), std::string("München optical payload")});
    auto search = [&](const std::string& query) {
        return engine.execute("SELECT rowid FROM offering_fts WHERE offering_fts MATCH ? AND node=? ORDER BY rowid LIMIT 100",
            {query, std::string("provider-a")});
    };
    auto match = search("munchen AND optical");
    require(match.rowCount() == 1 && std::get<int64_t>(match.rows[0][0]) == 205,
        "search must find an unloaded-page match, fold diacritics and retain source scope");
    require(search("\"optical payload\"").rowCount() == 1, "phrase search");
    require(search("payl*").rowCount() == 1, "prefix search");
    require(search("205").rowCount() == 1, "numeric object identifier search");
    require(search("absent").rowCount() == 0, "no matches");
    require(search("\"x' OR 1=1 --\"").rowCount() == 0, "query text remains a bound value");
    auto page2 = engine.execute("SELECT rowid FROM offering_fts WHERE offering_fts MATCH ? AND node=? ORDER BY rowid LIMIT 100 OFFSET 100",
        {std::string("orbit"), std::string("provider-a")});
    require(page2.rowCount() == 100 && std::get<int64_t>(page2.rows[0][0]) == 101, "paginate the full match set");
    engine.execute("UPDATE offering_fts SET description=? WHERE rowid=205", {std::string("radio payload")});
    require(search("optical").rowCount() == 0 && search("radio").rowCount() == 1, "replacement updates the index");
    engine.execute("DELETE FROM offering_fts WHERE rowid=205");
    require(search("radio").rowCount() == 0, "withdrawal removes indexed terms");
    std::cout << "FlatSQL full-text search passed\n";
}
