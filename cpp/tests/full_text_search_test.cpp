// Behavioral reference: https://www.sqlite.org/fts5.html sections 3 and 4.
// Exercise the actual FlatSQL connection, not the system SQLite library.
#include "flatsql/sqlite_engine.h"
#include <iostream>
#include <stdexcept>

using namespace flatsql;
static void require(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}
int main() {
    SQLiteEngine engine;
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
