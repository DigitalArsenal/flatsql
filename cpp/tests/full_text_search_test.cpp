// Behavioral reference: https://www.sqlite.org/fts5.html sections 3 and 4.
// Exercise the actual FlatSQL connection, not the system SQLite library.
#include "flatsql/sqlite_engine.h"
#include "flatsql/database.h"
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
    auto malformed = record; flatbuffers::WriteScalar<uint32_t>(malformed.data(),0xffffffff);
    QueryResult ignored; std::string error;
    require(!engine.executeNoThrow("SELECT flatsql_record_text('Sample',?)", {malformed}, ignored, &error), "reject malformed FlatBuffer");
    require(!engine.executeNoThrow("SELECT flatsql_record_text('Missing',?)", {record}, ignored, &error), "reject unknown schemas");
    SQLiteEngine moved(std::move(engine));
    require(moved.execute("SELECT flatsql_record_text('Sample',?)", {record}).rows == extracted.rows, "function survives engine move");
    engine = std::move(moved);
    require(engine.execute("SELECT flatsql_record_text('Sample',?)", {record}).rows == extracted.rows, "function survives move assignment");

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
