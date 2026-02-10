#include "flatsql/database.h"
#include "flatsql/junction.h"
#include "flatsql/sqlite_index.h"
#include "flatbuffers/encryption.h"
#include <sqlite3.h>
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

void testSQLiteEngine() {
    std::cout << "Testing SQLite engine..." << std::endl;

    // SQL parsing is now handled by SQLite virtual tables
    // This test verifies the integration works

    std::string schema = R"(
        table users {
            id: int (id);
            name: string;
            age: int;
        }
    )";

    FlatSQLDatabase db = FlatSQLDatabase::fromSchema(schema, "sql_test");
    db.registerFileId("USER", "users");

    // Create fake FlatBuffer data with file identifier "USER"
    std::vector<uint8_t> fakeData = {0x08, 0x00, 0x00, 0x00, 'U', 'S', 'E', 'R', 0x00, 0x00};

    db.ingestOne(fakeData.data(), fakeData.size());

    // Query using SQLite - this tests SELECT parsing
    QueryResult result = db.query("SELECT * FROM users LIMIT 10");
    assert(result.rowCount() == 1);

    std::cout << "SQLite engine tests passed!" << std::endl;
}

void testSqliteIndex() {
    std::cout << "Testing SQLite-backed index..." << std::endl;

    // Create an in-memory SQLite database
    sqlite3* db;
    int rc = sqlite3_open(":memory:", &db);
    assert(rc == SQLITE_OK);

    // ==================== Integer Index Tests ====================
    std::cout << "  Testing integer index..." << std::endl;
    {
        SqliteIndex index(db, "test_table", "int_column", ValueType::Int32);

        // Insert some values
        for (int i = 0; i < 100; i++) {
            index.insert(i, static_cast<uint64_t>(i * 100), 50, static_cast<uint64_t>(i));
        }

        assert(index.getEntryCount() == 100);

        // Search for specific value
        auto results = index.search(42);
        assert(results.size() == 1);
        assert(results[0].dataOffset == 4200);

        // searchFirst (Value API)
        IndexEntry entry;
        bool found = index.searchFirst(50, entry);
        assert(found);
        assert(entry.dataOffset == 5000);

        // searchFirstInt64 (fast path API)
        uint64_t offset, seq;
        uint32_t len;
        found = index.searchFirstInt64(50, offset, len, seq);
        assert(found);
        assert(offset == 5000);
        assert(len == 50);
        assert(seq == 50);

        // Test not found case
        found = index.searchFirstInt64(999, offset, len, seq);
        assert(!found);

        // Range search
        auto rangeResults = index.range(10, 20);
        assert(rangeResults.size() == 11);  // 10 through 20 inclusive

        // Get all
        auto all = index.all();
        assert(all.size() == 100);

        // Test clear
        index.clear();
        assert(index.getEntryCount() == 0);
    }

    // ==================== String Index Tests ====================
    std::cout << "  Testing string index..." << std::endl;
    {
        SqliteIndex stringIndex(db, "test_table", "string_column", ValueType::String);

        // Insert string keys (like NORAD CAT IDs)
        std::vector<std::string> keys = {"00001", "00002", "00003", "12345", "99999"};
        for (size_t i = 0; i < keys.size(); i++) {
            stringIndex.insert(keys[i], i * 1000, 100, i + 1);
        }

        assert(stringIndex.getEntryCount() == 5);

        // searchFirst (Value API)
        IndexEntry entry;
        bool found = stringIndex.searchFirst(std::string("12345"), entry);
        assert(found);
        assert(entry.dataOffset == 3000);  // index 3

        // searchFirstString (fast path API)
        uint64_t offset, seq;
        uint32_t len;
        found = stringIndex.searchFirstString("12345", offset, len, seq);
        assert(found);
        assert(offset == 3000);
        assert(len == 100);
        assert(seq == 4);

        // Test not found case
        found = stringIndex.searchFirstString("NOTFOUND", offset, len, seq);
        assert(!found);

        // Range search on strings
        auto rangeResults = stringIndex.range(std::string("00001"), std::string("00003"));
        assert(rangeResults.size() == 3);

        stringIndex.clear();
    }

    // ==================== Non-Unique Index Tests ====================
    std::cout << "  Testing non-unique index (multiple entries per key)..." << std::endl;
    {
        SqliteIndex nonUniqueIndex(db, "posts", "user_id", ValueType::Int32);

        // Simulate multiple posts per user (user_id -> post offset)
        // User 1 has 3 posts
        nonUniqueIndex.insert(1, 100, 50, 1);
        nonUniqueIndex.insert(1, 200, 50, 2);
        nonUniqueIndex.insert(1, 300, 50, 3);
        // User 2 has 2 posts
        nonUniqueIndex.insert(2, 400, 50, 4);
        nonUniqueIndex.insert(2, 500, 50, 5);
        // User 3 has 1 post
        nonUniqueIndex.insert(3, 600, 50, 6);

        assert(nonUniqueIndex.getEntryCount() == 6);

        // search() should return ALL matching entries
        auto user1Posts = nonUniqueIndex.search(1);
        assert(user1Posts.size() == 3);

        auto user2Posts = nonUniqueIndex.search(2);
        assert(user2Posts.size() == 2);

        auto user3Posts = nonUniqueIndex.search(3);
        assert(user3Posts.size() == 1);

        // searchFirst should return just one
        IndexEntry entry;
        bool found = nonUniqueIndex.searchFirst(1, entry);
        assert(found);
        // Should be one of the user 1 posts
        assert(entry.dataOffset == 100 || entry.dataOffset == 200 || entry.dataOffset == 300);

        // User with no posts
        auto noPosts = nonUniqueIndex.search(999);
        assert(noPosts.empty());

        nonUniqueIndex.clear();
    }

    // ==================== Edge Case Tests ====================
    std::cout << "  Testing edge cases..." << std::endl;
    {
        SqliteIndex edgeIndex(db, "edge_test", "value", ValueType::Int64);

        // Test with boundary values
        edgeIndex.insert(static_cast<int64_t>(0), 0, 10, 1);
        edgeIndex.insert(static_cast<int64_t>(-1), 10, 10, 2);
        edgeIndex.insert(static_cast<int64_t>(INT64_MAX), 20, 10, 3);
        edgeIndex.insert(static_cast<int64_t>(INT64_MIN), 30, 10, 4);

        assert(edgeIndex.getEntryCount() == 4);

        // Verify lookups work for boundary values
        uint64_t offset, seq;
        uint32_t len;

        bool found = edgeIndex.searchFirstInt64(INT64_MAX, offset, len, seq);
        assert(found);
        assert(offset == 20);

        found = edgeIndex.searchFirstInt64(INT64_MIN, offset, len, seq);
        assert(found);
        assert(offset == 30);

        found = edgeIndex.searchFirstInt64(0, offset, len, seq);
        assert(found);
        assert(offset == 0);

        edgeIndex.clear();
    }

    // ==================== Empty Index Tests ====================
    std::cout << "  Testing empty index behavior..." << std::endl;
    {
        SqliteIndex emptyIndex(db, "empty_test", "col", ValueType::Int32);

        assert(emptyIndex.getEntryCount() == 0);

        auto results = emptyIndex.search(42);
        assert(results.empty());

        IndexEntry entry;
        bool found = emptyIndex.searchFirst(42, entry);
        assert(!found);

        uint64_t offset, seq;
        uint32_t len;
        found = emptyIndex.searchFirstInt64(42, offset, len, seq);
        assert(!found);

        auto all = emptyIndex.all();
        assert(all.empty());

        auto range = emptyIndex.range(0, 100);
        assert(range.empty());
    }

    sqlite3_close(db);

    std::cout << "SQLite-backed index tests passed!" << std::endl;
}

void testStorage() {
    std::cout << "Testing streaming FlatBuffer storage..." << std::endl;

    StreamingFlatBufferStore store;

    // Create fake FlatBuffer data with file identifiers at bytes 4-7
    // Format: [root offset 4 bytes][file_id 4 bytes][data...]
    std::vector<uint8_t> data1 = {0x08, 0x00, 0x00, 0x00, 'U', 'S', 'E', 'R', 0x0C, 0x00};
    std::vector<uint8_t> data2 = {0x08, 0x00, 0x00, 0x00, 'P', 'O', 'S', 'T', 0x0C, 0x00, 0x04, 0x00};

    // Track ingested sequences
    std::vector<std::pair<std::string, uint64_t>> ingested;

    // ingestFlatBuffer returns sequence (stable rowid)
    uint64_t seq1 = store.ingestFlatBuffer(data1.data(), data1.size(),
        [&](std::string_view fileId, const uint8_t*, size_t, uint64_t seq, uint64_t) {
            ingested.push_back({std::string(fileId), seq});
        });
    uint64_t seq2 = store.ingestFlatBuffer(data2.data(), data2.size(),
        [&](std::string_view fileId, const uint8_t*, size_t, uint64_t seq, uint64_t) {
            ingested.push_back({std::string(fileId), seq});
        });

    assert(store.getRecordCount() == 2);
    assert(seq1 == 1);  // First sequence is 1
    assert(seq2 == 2);  // Second sequence is 2

    // Verify file identifiers were extracted
    assert(ingested.size() == 2);
    assert(ingested[0].first == "USER");
    assert(ingested[1].first == "POST");

    // Read back by sequence
    StoredRecord record1 = store.readRecord(seq1);
    assert(record1.header.fileId == "USER");
    assert(record1.data == data1);
    assert(record1.header.sequence == seq1);

    StoredRecord record2 = store.readRecord(seq2);
    assert(record2.header.fileId == "POST");
    assert(record2.data == data2);
    assert(record2.header.sequence == seq2);

    // Test hasRecord
    assert(store.hasRecord(seq1));
    assert(store.hasRecord(seq2));
    assert(!store.hasRecord(999));

    // Export and reload
    auto exportedData = store.exportData();

    StreamingFlatBufferStore reloaded;
    std::vector<std::pair<std::string, uint64_t>> reloadedIngested;
    reloaded.loadAndRebuild(exportedData.data(), exportedData.size(),
        [&](std::string_view fileId, const uint8_t*, size_t, uint64_t seq, uint64_t) {
            reloadedIngested.push_back({std::string(fileId), seq});
        });

    assert(reloaded.getRecordCount() == 2);

    // Verify records can still be read by sequence after reload
    StoredRecord reloadedRecord1 = reloaded.readRecord(1);
    assert(reloadedRecord1.header.fileId == "USER");
    assert(reloadedRecord1.data == data1);

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

    // Register file identifier for routing
    db.registerFileId("ITEM", "items");

    // Create fake FlatBuffer data with file identifier "ITEM" at bytes 4-7
    std::vector<uint8_t> fakeData = {0x08, 0x00, 0x00, 0x00, 'I', 'T', 'E', 'M'};

    // Ingest using streaming API
    db.ingestOne(fakeData.data(), fakeData.size());
    db.ingestOne(fakeData.data(), fakeData.size());

    // Query
    QueryResult result = db.query("SELECT * FROM items");
    assert(result.rowCount() == 2);

    std::cout << "Database tests passed!" << std::endl;
}

void testSchemaAnalyzer() {
    std::cout << "Testing schema analyzer..." << std::endl;

    SchemaAnalyzer analyzer;

    // Add a schema with tables and references
    std::string weaponSchema = R"(
        namespace game;
        table Weapon {
            name: string;
            damage: int;
        }
    )";

    std::string monsterSchema = R"(
        include "weapons.fbs";
        namespace game;

        table Monster {
            name: string;
            hp: int;
            weapon: Weapon;
            inventory: [Weapon];
        }
    )";

    analyzer.addSchema("weapons.fbs", weaponSchema);
    analyzer.addSchema("monster.fbs", monsterSchema);

    SchemaAnalysis analysis = analyzer.analyze();

    // Verify no cycles
    assert(!analysis.cycle.has_value() || !analysis.cycle->hasCycle);
    std::cout << "  No circular dependencies detected" << std::endl;

    // Verify import order
    assert(analysis.importOrder.size() == 2);
    std::cout << "  Import order: ";
    for (const auto& file : analysis.importOrder) {
        std::cout << file << " ";
    }
    std::cout << std::endl;

    // Verify tables detected
    assert(analysis.tables.count("Weapon") > 0);
    assert(analysis.tables.count("Monster") > 0);
    std::cout << "  Found tables: Weapon, Monster" << std::endl;

    // Verify junction tables generated
    std::cout << "  Junction tables: " << analysis.junctionTables.size() << std::endl;
    for (const auto& jt : analysis.junctionTables) {
        std::cout << "    - " << jt.name << " (";
        switch (jt.relationType) {
            case RelationType::SINGLE_TABLE: std::cout << "single"; break;
            case RelationType::VECTOR_TABLE: std::cout << "vector"; break;
            case RelationType::UNION: std::cout << "union"; break;
            case RelationType::VECTOR_UNION: std::cout << "vector_union"; break;
        }
        std::cout << ")" << std::endl;
    }

    // Verify struct detection
    assert(!analyzer.isStruct("Weapon"));  // Weapon is a table
    assert(!analyzer.isStruct("Monster")); // Monster is a table

    std::cout << "Schema analyzer tests passed!" << std::endl;
}

void testCycleDetection() {
    std::cout << "Testing cycle detection..." << std::endl;

    SchemaAnalyzer analyzer;

    // Create schemas with circular dependency
    std::string schemaA = R"(
        include "b.fbs";
        table A { b: B; }
    )";

    std::string schemaB = R"(
        include "c.fbs";
        table B { c: C; }
    )";

    std::string schemaC = R"(
        include "a.fbs";
        table C { a: A; }
    )";

    analyzer.addSchema("a.fbs", schemaA);
    analyzer.addSchema("b.fbs", schemaB);
    analyzer.addSchema("c.fbs", schemaC);

    SchemaAnalysis analysis = analyzer.analyze();

    // Should detect cycle
    assert(analysis.cycle.has_value());
    assert(analysis.cycle->hasCycle);
    std::cout << "  Cycle detected: ";
    for (const auto& node : analysis.cycle->cyclePath) {
        std::cout << node << " -> ";
    }
    std::cout << std::endl;

    // Should have error message
    assert(!analysis.errors.empty());
    std::cout << "  Error: " << analysis.errors[0] << std::endl;

    std::cout << "Cycle detection tests passed!" << std::endl;
}

void testJunctionManager() {
    std::cout << "Testing junction manager..." << std::endl;

    std::string schema = R"(
        table Monster {
            id: int (id);
            name: string;
        }
        table Weapon {
            id: int (id);
            name: string;
            damage: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "junction_test");

    // Create analysis with junction table definition
    SchemaAnalysis analysis;
    TableInfo monsterInfo;
    monsterInfo.name = "Monster";
    monsterInfo.sourceFile = "test.fbs";
    monsterInfo.isImported = false;

    TableReference weaponRef;
    weaponRef.fieldName = "weapon";
    weaponRef.referencedType = "Weapon";
    weaponRef.relationType = RelationType::SINGLE_TABLE;
    monsterInfo.references.push_back(weaponRef);

    analysis.tables["Monster"] = monsterInfo;

    TableInfo weaponInfo;
    weaponInfo.name = "Weapon";
    weaponInfo.sourceFile = "test.fbs";
    weaponInfo.isImported = false;
    analysis.tables["Weapon"] = weaponInfo;

    JunctionTable jt;
    jt.name = "Monster__weapon";
    jt.parentTable = "Monster";
    jt.fieldName = "weapon";
    jt.relationType = RelationType::SINGLE_TABLE;
    jt.childTable = "Weapon";
    analysis.junctionTables.push_back(jt);

    // Initialize junction manager
    JunctionManager junctionMgr(db);
    junctionMgr.initialize(analysis);

    auto junctions = junctionMgr.getJunctionTables();
    assert(junctions.size() == 1);
    assert(junctions[0].name == "Monster__weapon");
    std::cout << "  Junction table created: " << junctions[0].name << std::endl;

    // Test junction table SQL generation
    std::string sql = jt.createSQL();
    std::cout << "  Generated SQL:" << std::endl;
    std::cout << "    " << sql.substr(0, sql.find('\n')) << "..." << std::endl;
    assert(sql.find("Monster__weapon") != std::string::npos);
    assert(sql.find("parent_rowid") != std::string::npos);
    assert(sql.find("child_rowid") != std::string::npos);

    std::cout << "Junction manager tests passed!" << std::endl;
}

void testSqleanExtensions() {
    std::cout << "Testing sqlean extensions..." << std::endl;

    // Create a database to get the engine with registered functions
    std::string schema = R"(
        table dummy {
            id: int (id);
            name: string;
        }
    )";
    FlatSQLDatabase db = FlatSQLDatabase::fromSchema(schema, "sqlean_test");

    // Test math functions
    {
        QueryResult r = db.query("SELECT sqrt(4), ceil(3.14), floor(3.14), pi()");
        assert(r.rowCount() == 1);
        assert(r.columnCount() == 4);
        double sqrtVal = std::get<double>(r.rows[0][0]);
        assert(sqrtVal == 2.0);
        double ceilVal = std::get<double>(r.rows[0][1]);
        assert(ceilVal == 4.0);
        double floorVal = std::get<double>(r.rows[0][2]);
        assert(floorVal == 3.0);
        double piVal = std::get<double>(r.rows[0][3]);
        assert(piVal > 3.14 && piVal < 3.15);
        std::cout << "  math: sqrt(4)=" << sqrtVal << " ceil(3.14)=" << ceilVal
                  << " floor(3.14)=" << floorVal << " pi()=" << piVal << std::endl;
    }

    // Test text functions
    {
        QueryResult r = db.query("SELECT text_upper('hello'), text_lower('WORLD'), text_length('test')");
        assert(r.rowCount() == 1);
        std::string upper = std::get<std::string>(r.rows[0][0]);
        std::string lower = std::get<std::string>(r.rows[0][1]);
        int64_t length = std::get<int64_t>(r.rows[0][2]);
        assert(upper == "HELLO");
        assert(lower == "world");
        assert(length == 4);
        std::cout << "  text: upper='hello'->" << upper << " lower='WORLD'->" << lower
                  << " length('test')=" << length << std::endl;
    }

    // Test fuzzy functions
    {
        QueryResult r = db.query("SELECT fuzzy_leven('kitten', 'sitting')");
        assert(r.rowCount() == 1);
        int64_t dist = std::get<int64_t>(r.rows[0][0]);
        assert(dist == 3);
        std::cout << "  fuzzy: leven('kitten','sitting')=" << dist << std::endl;
    }

    // Test geo functions
    {
        // Distance from NYC to DC (~328 km)
        QueryResult r = db.query("SELECT geo_distance(40.7128, -74.0060, 38.9072, -77.0369)");
        assert(r.rowCount() == 1);
        double dist = std::get<double>(r.rows[0][0]);
        assert(dist > 300 && dist < 350);
        std::cout << "  geo: NYC->DC distance=" << dist << " km" << std::endl;
    }

    std::cout << "sqlean and geo extension tests passed!" << std::endl;
}

void testEncryptionRoundTrip() {
    std::cout << "Testing encryption round-trip..." << std::endl;

    // Create a 32-byte test key
    uint8_t key[32];
    for (int i = 0; i < 32; i++) key[i] = static_cast<uint8_t>(i + 1);

    flatbuffers::EncryptionContext ctx(key, 32);
    assert(ctx.IsValid());

    // Test scalar round-trip (int64)
    {
        int64_t original = 123456789LL;
        int64_t value = original;
        flatbuffers::EncryptScalar(reinterpret_cast<uint8_t*>(&value), sizeof(value), ctx, 1);
        assert(value != original);  // Should be different after encryption
        flatbuffers::DecryptScalar(reinterpret_cast<uint8_t*>(&value), sizeof(value), ctx, 1);
        assert(value == original);  // Should be back to original
        std::cout << "  scalar int64 round-trip: OK" << std::endl;
    }

    // Test scalar round-trip (double)
    {
        double original = 3.14159265358979;
        double value = original;
        flatbuffers::EncryptScalar(reinterpret_cast<uint8_t*>(&value), sizeof(value), ctx, 2);
        assert(value != original);
        flatbuffers::DecryptScalar(reinterpret_cast<uint8_t*>(&value), sizeof(value), ctx, 2);
        assert(value == original);
        std::cout << "  scalar double round-trip: OK" << std::endl;
    }

    // Test string round-trip
    {
        std::string original = "Hello, encrypted world!";
        std::string value = original;
        flatbuffers::EncryptString(reinterpret_cast<uint8_t*>(value.data()), value.size(), ctx, 3);
        assert(value != original);
        flatbuffers::DecryptString(reinterpret_cast<uint8_t*>(value.data()), value.size(), ctx, 3);
        assert(value == original);
        std::cout << "  string round-trip: OK" << std::endl;
    }

    // Test blob round-trip
    {
        std::vector<uint8_t> original = {0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE};
        std::vector<uint8_t> value = original;
        uint8_t fieldKey[32], fieldIV[16];
        ctx.DeriveFieldKey(4, fieldKey);
        ctx.DeriveFieldIV(4, fieldIV);
        flatbuffers::EncryptBytes(value.data(), value.size(), fieldKey, fieldIV);
        assert(value != original);
        flatbuffers::DecryptBytes(value.data(), value.size(), fieldKey, fieldIV);
        assert(value == original);
        std::cout << "  blob round-trip: OK" << std::endl;
    }

    // Test HMAC authentication via FlatSQLDatabase
    {
        std::string schema = R"(
            table dummy {
                id: int (id);
            }
        )";
        FlatSQLDatabase db = FlatSQLDatabase::fromSchema(schema, "hmac_test");
        db.setEncryptionKey(key, 32);

        std::vector<uint8_t> buffer = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        uint8_t mac[32];
        bool computed = db.computeHMAC(buffer.data(), buffer.size(), mac);
        if (computed) {
            // Verify should succeed with original data
            assert(db.verifyHMAC(buffer.data(), buffer.size(), mac));

            // Tamper with the buffer
            buffer[0] = 99;
            assert(!db.verifyHMAC(buffer.data(), buffer.size(), mac));
            std::cout << "  HMAC authentication: OK" << std::endl;
        } else {
            std::cout << "  HMAC authentication: SKIPPED (no OpenSSL)" << std::endl;
        }
    }

    // Test different field IDs produce different ciphertexts
    {
        int64_t val1 = 42, val2 = 42;
        flatbuffers::EncryptScalar(reinterpret_cast<uint8_t*>(&val1), sizeof(val1), ctx, 10);
        flatbuffers::EncryptScalar(reinterpret_cast<uint8_t*>(&val2), sizeof(val2), ctx, 20);
        assert(val1 != val2);  // Same plaintext, different field IDs -> different ciphertext
        std::cout << "  field ID isolation: OK" << std::endl;
    }

    std::cout << "Encryption round-trip tests passed!" << std::endl;
}

int main() {
    std::cout << "=== FlatSQL Test Suite ===" << std::endl;
    std::cout << std::endl;

    try {
        testSchemaParser();
        testSQLiteEngine();
        testSqliteIndex();
        testStorage();
        testDatabase();
        testSchemaAnalyzer();
        testCycleDetection();
        testJunctionManager();
        testSqleanExtensions();
        testEncryptionRoundTrip();

        std::cout << std::endl;
        std::cout << "=== All tests passed! ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
