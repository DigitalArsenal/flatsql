#include "flatsql/database.h"
#include <iostream>
#include <cassert>

using namespace flatsql;

void testSchemaParser() {
    std::cout << "Testing schema parser..." << std::endl;

    // Test IDL parsing
    std::string idl = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }

        table Post {
            id: int (id);
            user_id: int (key);
            title: string;
            content: string;
        }
    )";

    DatabaseSchema schema = SchemaParser::parseIDL(idl, "test_db");

    assert(schema.name == "test_db");
    assert(schema.tables.size() == 2);

    const TableDef* userTable = schema.getTable("User");
    assert(userTable != nullptr);
    assert(userTable->columns.size() == 4);
    assert(userTable->columns[0].name == "id");
    assert(userTable->columns[0].type == ValueType::Int32);
    assert(userTable->columns[0].primaryKey == true);

    std::cout << "Schema parser tests passed!" << std::endl;
}

void testSQLParser() {
    std::cout << "Testing SQL parser..." << std::endl;

    // Test SELECT
    ParsedSQL select = SQLParser::parse("SELECT name, email FROM users WHERE age > 18 LIMIT 10");
    assert(select.type == SQLStatementType::Select);
    assert(select.tableName == "users");
    assert(select.columns.size() == 2);
    assert(select.columns[0] == "name");
    assert(select.columns[1] == "email");
    assert(select.where.has_value());
    assert(select.where->column == "age");
    assert(select.where->op == ">");
    assert(select.limit.has_value());
    assert(select.limit.value() == 10);

    // Test BETWEEN
    ParsedSQL between = SQLParser::parse("SELECT * FROM orders WHERE amount BETWEEN 100 AND 500");
    assert(between.where.has_value());
    assert(between.where->hasBetween);

    // Test INSERT
    ParsedSQL insert = SQLParser::parse("INSERT INTO users (name, age) VALUES ('John', 25)");
    assert(insert.type == SQLStatementType::Insert);
    assert(insert.tableName == "users");
    assert(insert.columns.size() == 2);
    assert(insert.insertValues.size() == 2);

    std::cout << "SQL parser tests passed!" << std::endl;
}

void testBTree() {
    std::cout << "Testing B-tree..." << std::endl;

    BTree tree(ValueType::Int32, 4);  // Small order for testing

    // Insert some values
    for (int i = 0; i < 100; i++) {
        tree.insert(i, static_cast<uint64_t>(i * 100), 50, static_cast<uint64_t>(i));
    }

    assert(tree.getEntryCount() == 100);

    // Search for specific value
    auto results = tree.search(42);
    assert(results.size() == 1);
    assert(results[0].dataOffset == 4200);

    // Range search
    auto rangeResults = tree.range(10, 20);
    assert(rangeResults.size() == 11);  // 10 through 20 inclusive

    // Get all
    auto all = tree.all();
    assert(all.size() == 100);

    std::cout << "B-tree tests passed!" << std::endl;
}

void testStorage() {
    std::cout << "Testing stacked FlatBuffer storage..." << std::endl;

    StackedFlatBufferStore store("test_schema");

    // Create some fake "FlatBuffer" data
    std::vector<uint8_t> data1 = {0x10, 0x00, 0x00, 0x00, 0x08, 0x00, 0x0C, 0x00};
    std::vector<uint8_t> data2 = {0x12, 0x00, 0x00, 0x00, 0x08, 0x00, 0x0C, 0x00, 0x04, 0x00};

    uint64_t offset1 = store.append("users", data1);
    uint64_t offset2 = store.append("posts", data2);

    assert(store.getRecordCount() == 2);
    assert(offset1 == FILE_HEADER_SIZE);

    // Read back
    StoredRecord record1 = store.readRecord(offset1);
    assert(record1.header.tableName == "users");
    assert(record1.data == data1);

    StoredRecord record2 = store.readRecord(offset2);
    assert(record2.header.tableName == "posts");
    assert(record2.data == data2);

    // Export and reload
    auto exportedData = store.getDataCopy();
    auto reloaded = StackedFlatBufferStore::fromData(exportedData);
    assert(reloaded.getRecordCount() == 2);
    assert(reloaded.getSchemaName() == "test_schema");

    std::cout << "Storage tests passed!" << std::endl;
}

void testDatabase() {
    std::cout << "Testing FlatSQL database..." << std::endl;

    std::string schema = R"(
        table items {
            id: int (id);
            name: string;
            price: float;
        }
    )";

    FlatSQLDatabase db = FlatSQLDatabase::fromSchema(schema, "test");

    auto tables = db.listTables();
    assert(tables.size() == 1);
    assert(tables[0] == "items");

    const TableDef* itemsDef = db.getTableDef("items");
    assert(itemsDef != nullptr);
    assert(itemsDef->columns.size() == 3);

    // Insert some fake data
    std::vector<uint8_t> fakeData = {0x10, 0x00, 0x00, 0x00};
    db.insertRaw("items", fakeData);
    db.insertRaw("items", fakeData);

    // Query
    QueryResult result = db.query("SELECT * FROM items");
    assert(result.rowCount() == 2);

    std::cout << "Database tests passed!" << std::endl;
}

int main() {
    std::cout << "=== FlatSQL Test Suite ===" << std::endl;
    std::cout << std::endl;

    try {
        testSchemaParser();
        testSQLParser();
        testBTree();
        testStorage();
        testDatabase();

        std::cout << std::endl;
        std::cout << "=== All tests passed! ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
