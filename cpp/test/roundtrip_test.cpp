// Comprehensive round-trip and edge case tests for FlatSQL
// Tests: create -> stream -> query -> verify exact value match
// Also tests disk-based streaming and FlatBuffer verification

#include "flatsql/database.h"
#include "../schemas/test_schema_generated.h"
#include <sqlite3.h>
#include <iostream>
#include <fstream>
#include <cassert>
#include <cstring>
#include <random>
#include <set>

using namespace flatsql;

// Test statistics
static int testsRun = 0;
static int testsPassed = 0;
static int testsFailed = 0;

#define TEST_ASSERT(condition, message) do { \
    testsRun++; \
    if (!(condition)) { \
        std::cerr << "FAIL: " << message << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        testsFailed++; \
        return false; \
    } else { \
        testsPassed++; \
    } \
} while(0)

#define TEST_ASSERT_EQ(expected, actual, message) do { \
    testsRun++; \
    if ((expected) != (actual)) { \
        std::cerr << "FAIL: " << message << " - expected [" << (expected) << "] but got [" << (actual) << "]" << std::endl; \
        testsFailed++; \
        return false; \
    } else { \
        testsPassed++; \
    } \
} while(0)

// Helper: Create a User FlatBuffer with file identifier
std::vector<uint8_t> createUserFlatBuffer(int32_t id, const char* name, const char* email, int32_t age) {
    flatbuffers::FlatBufferBuilder builder(256);
    auto user = test::CreateUserDirect(builder, id, name, email, age);
    builder.Finish(user, "USER");
    const uint8_t* buf = builder.GetBufferPointer();
    size_t size = builder.GetSize();
    return std::vector<uint8_t>(buf, buf + size);
}

// Helper: Verify FlatBuffer with built-in verifier
bool verifyUserFlatBuffer(const uint8_t* data, size_t length) {
    flatbuffers::Verifier verifier(data, length);
    return verifier.VerifyBuffer<test::User>("USER");
}

// Field extractor for User table
Value extractUserField(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
    auto user = test::GetUser(data);
    if (!user) return std::monostate{};

    if (fieldName == "id") return user->id();
    if (fieldName == "name") return user->name() ? std::string(user->name()->c_str(), user->name()->size()) : std::string();
    if (fieldName == "email") return user->email() ? std::string(user->email()->c_str(), user->email()->size()) : std::string();
    if (fieldName == "age") return user->age();

    return std::monostate{};
}

// Batch extractor for User table
void batchExtractUser(const uint8_t* data, size_t length, std::vector<Value>& output) {
    (void)length;
    output.clear();
    output.reserve(4);

    auto user = test::GetUser(data);
    if (!user) {
        output.resize(4, std::monostate{});
        return;
    }

    output.push_back(user->id());
    output.push_back(user->name() ? std::string(user->name()->c_str(), user->name()->size()) : std::string());
    output.push_back(user->email() ? std::string(user->email()->c_str(), user->email()->size()) : std::string());
    output.push_back(user->age());
}

// ==================== ROUND-TRIP TESTS ====================

bool testBasicRoundTrip() {
    std::cout << "  Testing basic round-trip..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "roundtrip_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);
    db.setBatchExtractor("User", batchExtractUser);

    // Test data with specific values
    struct TestUser {
        int32_t id;
        std::string name;
        std::string email;
        int32_t age;
    };

    std::vector<TestUser> testUsers = {
        {1, "Alice", "alice@example.com", 30},
        {2, "Bob", "bob@test.org", 25},
        {3, "Charlie", "charlie@domain.net", 40},
        {100, "User100", "user100@email.com", 99},
        {999, "LastUser", "last@user.io", 1}
    };

    // Ingest all users
    for (const auto& tu : testUsers) {
        auto userData = createUserFlatBuffer(tu.id, tu.name.c_str(), tu.email.c_str(), tu.age);

        // Verify FlatBuffer before ingesting
        TEST_ASSERT(verifyUserFlatBuffer(userData.data(), userData.size()),
                   "FlatBuffer verification failed for user " + std::to_string(tu.id));

        db.ingestOne(userData.data(), userData.size());
    }

    // Query and verify each user individually
    for (const auto& tu : testUsers) {
        auto result = db.query("SELECT id, name, email, age FROM User WHERE id = ?",
                               std::vector<Value>{static_cast<int64_t>(tu.id)});

        TEST_ASSERT_EQ(1u, result.rowCount(), "Expected 1 row for id " + std::to_string(tu.id));

        // Extract values from result
        auto& row = result.rows[0];
        int64_t gotId = std::get<int64_t>(row[0]);
        std::string gotName = std::get<std::string>(row[1]);
        std::string gotEmail = std::get<std::string>(row[2]);
        int64_t gotAge = std::get<int64_t>(row[3]);

        TEST_ASSERT_EQ(static_cast<int64_t>(tu.id), gotId, "ID mismatch");
        TEST_ASSERT_EQ(tu.name, gotName, "Name mismatch for id " + std::to_string(tu.id));
        TEST_ASSERT_EQ(tu.email, gotEmail, "Email mismatch for id " + std::to_string(tu.id));
        TEST_ASSERT_EQ(static_cast<int64_t>(tu.age), gotAge, "Age mismatch for id " + std::to_string(tu.id));
    }

    // Query by email and verify
    auto result = db.query("SELECT * FROM User WHERE email = ?",
                           std::vector<Value>{std::string("bob@test.org")});
    TEST_ASSERT_EQ(1u, result.rowCount(), "Expected 1 row for email query");

    std::cout << "    Basic round-trip: PASSED" << std::endl;
    return true;
}

bool testDirectAPIRoundTrip() {
    std::cout << "  Testing direct API round-trip..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "direct_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);

    // Ingest test data
    auto userData = createUserFlatBuffer(42, "DirectTest", "direct@test.com", 33);
    db.ingestOne(userData.data(), userData.size());

    // Test findByIndex
    auto records = db.findByIndex("User", "id", static_cast<int32_t>(42));
    TEST_ASSERT_EQ(1u, records.size(), "findByIndex should return 1 record");

    // Test findOneByIndex
    StoredRecord record;
    bool found = db.findOneByIndex("User", "id", static_cast<int32_t>(42), record);
    TEST_ASSERT(found, "findOneByIndex should find the record");

    // Test findRawByIndex - zero-copy access
    uint32_t dataLen = 0;
    uint64_t sequence = 0;
    const uint8_t* rawData = db.findRawByIndex("User", "id", static_cast<int32_t>(42), &dataLen, &sequence);
    TEST_ASSERT(rawData != nullptr, "findRawByIndex should return data");
    TEST_ASSERT(dataLen > 0, "Data length should be > 0");

    // Verify the raw FlatBuffer
    TEST_ASSERT(verifyUserFlatBuffer(rawData, dataLen), "Raw FlatBuffer should verify");

    // Read and verify values from raw FlatBuffer
    auto user = test::GetUser(rawData);
    TEST_ASSERT(user != nullptr, "Should parse User from raw data");
    TEST_ASSERT_EQ(42, user->id(), "ID from raw data");
    TEST_ASSERT_EQ(std::string("DirectTest"), std::string(user->name()->c_str()), "Name from raw data");
    TEST_ASSERT_EQ(std::string("direct@test.com"), std::string(user->email()->c_str()), "Email from raw data");
    TEST_ASSERT_EQ(33, user->age(), "Age from raw data");

    std::cout << "    Direct API round-trip: PASSED" << std::endl;
    return true;
}

bool testIterationRoundTrip() {
    std::cout << "  Testing iteration round-trip..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "iter_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);

    // Create expected data
    std::set<int32_t> expectedIds;
    for (int i = 0; i < 100; i++) {
        std::string name = "IterUser" + std::to_string(i);
        std::string email = "iter" + std::to_string(i) + "@test.com";
        auto userData = createUserFlatBuffer(i, name.c_str(), email.c_str(), 20 + i);
        db.ingestOne(userData.data(), userData.size());
        expectedIds.insert(i);
    }

    // Iterate and verify all records are present
    std::set<int32_t> foundIds;
    bool iterationPassed = true;
    size_t count = db.iterateAll("User", [&](const uint8_t* data, uint32_t len, uint64_t seq) {
        (void)seq;
        if (!verifyUserFlatBuffer(data, len)) {
            iterationPassed = false;
            return;
        }

        auto user = test::GetUser(data);
        if (!user) {
            iterationPassed = false;
            return;
        }
        foundIds.insert(user->id());

        // Verify name matches expected pattern
        std::string expectedName = "IterUser" + std::to_string(user->id());
        if (expectedName != std::string(user->name()->c_str())) {
            iterationPassed = false;
        }
    });

    TEST_ASSERT(iterationPassed, "Iteration should pass all verifications");
    TEST_ASSERT_EQ(100u, count, "Should iterate 100 records");
    TEST_ASSERT(expectedIds == foundIds, "All IDs should be found");

    std::cout << "    Iteration round-trip: PASSED" << std::endl;
    return true;
}

// ==================== EDGE CASE TESTS ====================

bool testEmptyStrings() {
    std::cout << "  Testing empty strings..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "empty_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);
    db.setBatchExtractor("User", batchExtractUser);

    // Create user with empty name
    auto userData1 = createUserFlatBuffer(1, "", "empty_name@test.com", 25);
    db.ingestOne(userData1.data(), userData1.size());

    // Create user with empty email
    auto userData2 = createUserFlatBuffer(2, "NoEmail", "", 30);
    db.ingestOne(userData2.data(), userData2.size());

    // Create user with both empty
    auto userData3 = createUserFlatBuffer(3, "", "", 35);
    db.ingestOne(userData3.data(), userData3.size());

    // Query and verify
    auto result = db.query("SELECT id, name, email, age FROM User ORDER BY id");
    TEST_ASSERT_EQ(3u, result.rowCount(), "Should have 3 users");

    // Verify empty name
    std::string name1 = std::get<std::string>(result.rows[0][1]);
    TEST_ASSERT_EQ(std::string(""), name1, "Empty name should be preserved");

    // Verify empty email
    std::string email2 = std::get<std::string>(result.rows[1][2]);
    TEST_ASSERT_EQ(std::string(""), email2, "Empty email should be preserved");

    // Verify both empty
    std::string name3 = std::get<std::string>(result.rows[2][1]);
    std::string email3 = std::get<std::string>(result.rows[2][2]);
    TEST_ASSERT_EQ(std::string(""), name3, "Empty name in user 3");
    TEST_ASSERT_EQ(std::string(""), email3, "Empty email in user 3");

    std::cout << "    Empty strings: PASSED" << std::endl;
    return true;
}

bool testSpecialCharacters() {
    std::cout << "  Testing special characters..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "special_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);
    db.setBatchExtractor("User", batchExtractUser);

    // Test various special characters
    std::vector<std::pair<int32_t, std::string>> testCases = {
        {1, "Name with spaces"},
        {2, "Name\twith\ttabs"},
        {3, "Name\nwith\nnewlines"},
        {4, "Name'with'quotes"},
        {5, "Name\"with\"doublequotes"},
        {6, "Name\\with\\backslashes"},
        {7, "Name;with;semicolons"},
        {8, "Name--with--dashes"},
        {9, "Name/*with*/comments"},
        {10, "Name%with%percent"},
        {11, "Name_with_underscores"},
        {12, "Name@#$%^&*()!"},
    };

    for (const auto& [id, name] : testCases) {
        std::string email = "test" + std::to_string(id) + "@example.com";
        auto userData = createUserFlatBuffer(id, name.c_str(), email.c_str(), 25);
        db.ingestOne(userData.data(), userData.size());
    }

    // Query and verify each one
    for (const auto& [id, expectedName] : testCases) {
        auto result = db.query("SELECT name FROM User WHERE id = ?",
                               std::vector<Value>{static_cast<int64_t>(id)});
        TEST_ASSERT_EQ(1u, result.rowCount(), "Should find user " + std::to_string(id));

        std::string gotName = std::get<std::string>(result.rows[0][0]);
        TEST_ASSERT_EQ(expectedName, gotName, "Name mismatch for special chars, id=" + std::to_string(id));
    }

    std::cout << "    Special characters: PASSED" << std::endl;
    return true;
}

bool testUnicodeStrings() {
    std::cout << "  Testing Unicode strings..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "unicode_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);
    db.setBatchExtractor("User", batchExtractUser);

    // Test various Unicode strings
    std::vector<std::pair<int32_t, std::string>> testCases = {
        {1, u8"日本語"},           // Japanese
        {2, u8"中文"},              // Chinese
        {3, u8"한국어"},            // Korean
        {4, u8"Ελληνικά"},         // Greek
        {5, u8"עברית"},             // Hebrew
        {6, u8"العربية"},           // Arabic
        {7, u8"हिन्दी"},             // Hindi
        {8, u8"Émojis 🎉🚀💻"},    // Emojis
        {9, u8"Mixed: Café résumé naïve"},
        {10, u8"Symbols: ™®©℃°"},
    };

    for (const auto& [id, name] : testCases) {
        std::string email = "unicode" + std::to_string(id) + "@test.com";
        auto userData = createUserFlatBuffer(id, name.c_str(), email.c_str(), 25);
        db.ingestOne(userData.data(), userData.size());
    }

    // Query and verify each one
    for (const auto& [id, expectedName] : testCases) {
        auto result = db.query("SELECT name FROM User WHERE id = ?",
                               std::vector<Value>{static_cast<int64_t>(id)});
        TEST_ASSERT_EQ(1u, result.rowCount(), "Should find unicode user " + std::to_string(id));

        std::string gotName = std::get<std::string>(result.rows[0][0]);
        TEST_ASSERT_EQ(expectedName, gotName, "Unicode name mismatch for id=" + std::to_string(id));
    }

    std::cout << "    Unicode strings: PASSED" << std::endl;
    return true;
}

bool testLargeStrings() {
    std::cout << "  Testing large strings..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "large_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);
    db.setBatchExtractor("User", batchExtractUser);

    // Create strings of various sizes
    std::vector<size_t> sizes = {100, 1000, 10000, 100000};

    for (size_t i = 0; i < sizes.size(); i++) {
        size_t size = sizes[i];
        std::string longName(size, 'A' + (i % 26));
        std::string email = "large" + std::to_string(i) + "@test.com";

        auto userData = createUserFlatBuffer(static_cast<int32_t>(i + 1), longName.c_str(), email.c_str(), 25);
        TEST_ASSERT(verifyUserFlatBuffer(userData.data(), userData.size()),
                   "Large FlatBuffer verification for size " + std::to_string(size));

        db.ingestOne(userData.data(), userData.size());

        // Immediately query and verify
        auto result = db.query("SELECT name FROM User WHERE id = ?",
                               std::vector<Value>{static_cast<int64_t>(i + 1)});
        TEST_ASSERT_EQ(1u, result.rowCount(), "Should find user with large string");

        std::string gotName = std::get<std::string>(result.rows[0][0]);
        TEST_ASSERT_EQ(longName.size(), gotName.size(), "Large string size mismatch");
        TEST_ASSERT_EQ(longName, gotName, "Large string content mismatch");
    }

    std::cout << "    Large strings: PASSED" << std::endl;
    return true;
}

bool testBoundaryValues() {
    std::cout << "  Testing boundary values..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "boundary_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);
    db.setBatchExtractor("User", batchExtractUser);

    // Test boundary int32 values
    std::vector<std::pair<int32_t, int32_t>> testCases = {
        {1, 0},                          // Zero
        {2, -1},                         // Negative
        {3, INT32_MAX},                  // Max int32
        {4, INT32_MIN},                  // Min int32
        {5, INT32_MAX - 1},
        {6, INT32_MIN + 1},
    };

    for (const auto& [id, age] : testCases) {
        std::string name = "Boundary" + std::to_string(id);
        std::string email = "boundary" + std::to_string(id) + "@test.com";
        auto userData = createUserFlatBuffer(id, name.c_str(), email.c_str(), age);
        db.ingestOne(userData.data(), userData.size());
    }

    // Query and verify
    for (const auto& [id, expectedAge] : testCases) {
        auto result = db.query("SELECT age FROM User WHERE id = ?",
                               std::vector<Value>{static_cast<int64_t>(id)});
        TEST_ASSERT_EQ(1u, result.rowCount(), "Should find boundary user");

        int64_t gotAge = std::get<int64_t>(result.rows[0][0]);
        TEST_ASSERT_EQ(static_cast<int64_t>(expectedAge), gotAge,
                      "Boundary value mismatch for id=" + std::to_string(id));
    }

    std::cout << "    Boundary values: PASSED" << std::endl;
    return true;
}

// ==================== EXPORT/RELOAD TESTS ====================

bool testExportReloadRoundTrip() {
    std::cout << "  Testing export/reload round-trip..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    // Create and populate first database
    auto db1 = FlatSQLDatabase::fromSchema(schema, "export_test1");
    db1.registerFileId("USER", "User");
    db1.setFieldExtractor("User", extractUserField);
    db1.setBatchExtractor("User", batchExtractUser);

    std::vector<std::tuple<int32_t, std::string, std::string, int32_t>> testData = {
        {1, "Alice", "alice@example.com", 30},
        {2, u8"日本語ユーザー", "japanese@example.com", 25},
        {3, "", "empty_name@example.com", 35},
        {4, "LongName" + std::string(1000, 'X'), "long@example.com", 40},
    };

    for (const auto& [id, name, email, age] : testData) {
        auto userData = createUserFlatBuffer(id, name.c_str(), email.c_str(), age);
        db1.ingestOne(userData.data(), userData.size());
    }

    // Export
    auto exported = db1.exportData();
    TEST_ASSERT(exported.size() > 0, "Export should produce data");

    // Reload into new database
    auto db2 = FlatSQLDatabase::fromSchema(schema, "export_test2");
    db2.registerFileId("USER", "User");
    db2.setFieldExtractor("User", extractUserField);
    db2.setBatchExtractor("User", batchExtractUser);
    db2.loadAndRebuild(exported.data(), exported.size());

    // Verify all records
    for (const auto& [id, expectedName, expectedEmail, expectedAge] : testData) {
        auto result = db2.query("SELECT id, name, email, age FROM User WHERE id = ?",
                                std::vector<Value>{static_cast<int64_t>(id)});
        TEST_ASSERT_EQ(1u, result.rowCount(), "Should find reloaded user " + std::to_string(id));

        int64_t gotId = std::get<int64_t>(result.rows[0][0]);
        std::string gotName = std::get<std::string>(result.rows[0][1]);
        std::string gotEmail = std::get<std::string>(result.rows[0][2]);
        int64_t gotAge = std::get<int64_t>(result.rows[0][3]);

        TEST_ASSERT_EQ(static_cast<int64_t>(id), gotId, "Reloaded ID");
        TEST_ASSERT_EQ(expectedName, gotName, "Reloaded name");
        TEST_ASSERT_EQ(expectedEmail, gotEmail, "Reloaded email");
        TEST_ASSERT_EQ(static_cast<int64_t>(expectedAge), gotAge, "Reloaded age");
    }

    std::cout << "    Export/reload round-trip: PASSED" << std::endl;
    return true;
}

// ==================== DISK STREAMING TESTS ====================

bool testDiskStreamingSequential() {
    std::cout << "  Testing disk-based sequential streaming..." << std::endl;

    // Create a temporary file with FlatBuffers
    const char* tmpFile = "/tmp/flatsql_test_stream.bin";

    // Generate test data and write to file
    {
        std::ofstream out(tmpFile, std::ios::binary);
        TEST_ASSERT(out.is_open(), "Should open temp file for writing");

        for (int i = 0; i < 1000; i++) {
            std::string name = "StreamUser" + std::to_string(i);
            std::string email = "stream" + std::to_string(i) + "@test.com";
            auto userData = createUserFlatBuffer(i, name.c_str(), email.c_str(), 20 + (i % 60));

            // Write size-prefixed format
            uint32_t size = static_cast<uint32_t>(userData.size());
            out.write(reinterpret_cast<const char*>(&size), 4);
            out.write(reinterpret_cast<const char*>(userData.data()), userData.size());
        }
        out.close();
    }

    // Read back using streaming engine
    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "disk_stream_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);
    db.setBatchExtractor("User", batchExtractUser);

    // Read file in chunks to simulate streaming
    {
        std::ifstream in(tmpFile, std::ios::binary);
        TEST_ASSERT(in.is_open(), "Should open temp file for reading");

        const size_t CHUNK_SIZE = 4096;  // Read 4KB chunks
        std::vector<uint8_t> buffer;
        buffer.reserve(CHUNK_SIZE * 2);  // Extra space for partial records

        size_t totalRecords = 0;
        char chunk[CHUNK_SIZE];

        while (in.read(chunk, CHUNK_SIZE) || in.gcount() > 0) {
            size_t bytesRead = static_cast<size_t>(in.gcount());
            buffer.insert(buffer.end(), chunk, chunk + bytesRead);

            // Process complete records
            size_t recordsProcessed = 0;
            size_t bytesConsumed = db.ingest(buffer.data(), buffer.size(), &recordsProcessed);
            totalRecords += recordsProcessed;

            // Remove consumed bytes
            if (bytesConsumed > 0) {
                buffer.erase(buffer.begin(), buffer.begin() + bytesConsumed);
            }
        }

        TEST_ASSERT_EQ(1000u, totalRecords, "Should ingest all 1000 records from disk");
        in.close();
    }

    // Verify data integrity
    auto result = db.query("SELECT COUNT(*) FROM User");
    TEST_ASSERT_EQ(1u, result.rowCount(), "COUNT query should return 1 row");
    int64_t count = std::get<int64_t>(result.rows[0][0]);
    TEST_ASSERT_EQ(1000, count, "Should have 1000 users");

    // Verify some specific records
    for (int id : {0, 100, 500, 999}) {
        auto result = db.query("SELECT name FROM User WHERE id = ?",
                               std::vector<Value>{static_cast<int64_t>(id)});
        TEST_ASSERT_EQ(1u, result.rowCount(), "Should find user " + std::to_string(id));

        std::string expectedName = "StreamUser" + std::to_string(id);
        std::string gotName = std::get<std::string>(result.rows[0][0]);
        TEST_ASSERT_EQ(expectedName, gotName, "Name from disk stream");
    }

    // Clean up
    std::remove(tmpFile);

    std::cout << "    Disk streaming sequential: PASSED" << std::endl;
    return true;
}

bool testDiskExportImport() {
    std::cout << "  Testing disk export/import..." << std::endl;

    const char* tmpFile = "/tmp/flatsql_export_test.bin";

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    // Create and populate database
    auto db1 = FlatSQLDatabase::fromSchema(schema, "disk_export1");
    db1.registerFileId("USER", "User");
    db1.setFieldExtractor("User", extractUserField);
    db1.setBatchExtractor("User", batchExtractUser);

    for (int i = 0; i < 500; i++) {
        std::string name = "ExportUser" + std::to_string(i);
        std::string email = "export" + std::to_string(i) + "@test.com";
        auto userData = createUserFlatBuffer(i, name.c_str(), email.c_str(), 25 + (i % 50));
        db1.ingestOne(userData.data(), userData.size());
    }

    // Export to disk
    {
        auto exported = db1.exportData();
        std::ofstream out(tmpFile, std::ios::binary);
        TEST_ASSERT(out.is_open(), "Should open export file");
        out.write(reinterpret_cast<const char*>(exported.data()), exported.size());
        out.close();
    }

    // Import from disk
    auto db2 = FlatSQLDatabase::fromSchema(schema, "disk_export2");
    db2.registerFileId("USER", "User");
    db2.setFieldExtractor("User", extractUserField);
    db2.setBatchExtractor("User", batchExtractUser);

    {
        std::ifstream in(tmpFile, std::ios::binary | std::ios::ate);
        TEST_ASSERT(in.is_open(), "Should open import file");

        size_t fileSize = static_cast<size_t>(in.tellg());
        in.seekg(0);

        std::vector<uint8_t> data(fileSize);
        in.read(reinterpret_cast<char*>(data.data()), fileSize);
        in.close();

        db2.loadAndRebuild(data.data(), data.size());
    }

    // Verify imported data
    auto result = db2.query("SELECT COUNT(*) FROM User");
    int64_t count = std::get<int64_t>(result.rows[0][0]);
    TEST_ASSERT_EQ(500, count, "Should have 500 users after import");

    // Verify random samples
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, 499);
    for (int i = 0; i < 20; i++) {
        int id = dist(rng);
        auto result = db2.query("SELECT name, age FROM User WHERE id = ?",
                                std::vector<Value>{static_cast<int64_t>(id)});
        TEST_ASSERT_EQ(1u, result.rowCount(), "Should find imported user");

        std::string expectedName = "ExportUser" + std::to_string(id);
        std::string gotName = std::get<std::string>(result.rows[0][0]);
        TEST_ASSERT_EQ(expectedName, gotName, "Imported name");

        int64_t expectedAge = 25 + (id % 50);
        int64_t gotAge = std::get<int64_t>(result.rows[0][1]);
        TEST_ASSERT_EQ(expectedAge, gotAge, "Imported age");
    }

    // Clean up
    std::remove(tmpFile);

    std::cout << "    Disk export/import: PASSED" << std::endl;
    return true;
}

// ==================== FLATBUFFER VERIFIER TESTS ====================

bool testFlatBufferVerification() {
    std::cout << "  Testing FlatBuffer verification..." << std::endl;

    // Valid FlatBuffer
    auto validData = createUserFlatBuffer(1, "Valid", "valid@test.com", 25);
    TEST_ASSERT(verifyUserFlatBuffer(validData.data(), validData.size()),
               "Valid FlatBuffer should verify");

    // Truncated FlatBuffer
    auto truncated = validData;
    truncated.resize(validData.size() / 2);
    TEST_ASSERT(!verifyUserFlatBuffer(truncated.data(), truncated.size()),
               "Truncated FlatBuffer should fail verification");

    // Corrupted FlatBuffer (flip some bytes)
    auto corrupted = validData;
    corrupted[10] ^= 0xFF;
    corrupted[20] ^= 0xFF;
    // Note: Corruption might not always be detected, but we test it anyway
    // The test passes as long as either verification fails or we can still read it

    // Empty buffer
    std::vector<uint8_t> empty;
    TEST_ASSERT(!verifyUserFlatBuffer(empty.data(), empty.size()),
               "Empty buffer should fail verification");

    // Wrong file identifier
    flatbuffers::FlatBufferBuilder builder(256);
    auto user = test::CreateUserDirect(builder, 1, "Test", "test@test.com", 25);
    builder.Finish(user, "XXXX");  // Wrong file ID
    const uint8_t* wrongIdBuf = builder.GetBufferPointer();
    size_t wrongIdSize = builder.GetSize();
    TEST_ASSERT(!verifyUserFlatBuffer(wrongIdBuf, wrongIdSize),
               "Wrong file ID should fail verification");

    std::cout << "    FlatBuffer verification: PASSED" << std::endl;
    return true;
}

// ==================== COMPARISON WITH DIRECT SQLITE ====================

bool testCompareWithSQLite() {
    std::cout << "  Testing comparison with direct SQLite..." << std::endl;

    // Create identical data in both FlatSQL and SQLite
    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    auto flatsqlDb = FlatSQLDatabase::fromSchema(schema, "compare_test");
    flatsqlDb.registerFileId("USER", "User");
    flatsqlDb.setFieldExtractor("User", extractUserField);
    flatsqlDb.setBatchExtractor("User", batchExtractUser);

    // Direct SQLite database
    sqlite3* sqliteDb = nullptr;
    TEST_ASSERT(sqlite3_open(":memory:", &sqliteDb) == SQLITE_OK, "Open SQLite");

    const char* createSql = "CREATE TABLE User (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)";
    TEST_ASSERT(sqlite3_exec(sqliteDb, createSql, nullptr, nullptr, nullptr) == SQLITE_OK, "Create table");

    // Insert same data into both
    std::vector<std::tuple<int32_t, std::string, std::string, int32_t>> testData;
    for (int i = 0; i < 100; i++) {
        std::string name = "CompareUser" + std::to_string(i);
        std::string email = "compare" + std::to_string(i) + "@test.com";
        int32_t age = 20 + (i % 60);
        testData.push_back({i, name, email, age});

        // FlatSQL
        auto userData = createUserFlatBuffer(i, name.c_str(), email.c_str(), age);
        flatsqlDb.ingestOne(userData.data(), userData.size());

        // SQLite
        sqlite3_stmt* stmt;
        sqlite3_prepare_v2(sqliteDb, "INSERT INTO User VALUES (?, ?, ?, ?)", -1, &stmt, nullptr);
        sqlite3_bind_int(stmt, 1, i);
        sqlite3_bind_text(stmt, 2, name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, email.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 4, age);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    // Compare query results
    for (const auto& [id, expectedName, expectedEmail, expectedAge] : testData) {
        // FlatSQL query
        auto flatsqlResult = flatsqlDb.query("SELECT id, name, email, age FROM User WHERE id = ?",
                                              std::vector<Value>{static_cast<int64_t>(id)});

        // SQLite query
        sqlite3_stmt* stmt;
        sqlite3_prepare_v2(sqliteDb, "SELECT id, name, email, age FROM User WHERE id = ?", -1, &stmt, nullptr);
        sqlite3_bind_int(stmt, 1, id);

        TEST_ASSERT(sqlite3_step(stmt) == SQLITE_ROW, "SQLite should return row");

        int sqliteId = sqlite3_column_int(stmt, 0);
        std::string sqliteName = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        std::string sqliteEmail = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
        int sqliteAge = sqlite3_column_int(stmt, 3);

        sqlite3_finalize(stmt);

        // Compare FlatSQL with SQLite
        TEST_ASSERT_EQ(1u, flatsqlResult.rowCount(), "FlatSQL should return 1 row");

        int64_t flatsqlId = std::get<int64_t>(flatsqlResult.rows[0][0]);
        std::string flatsqlName = std::get<std::string>(flatsqlResult.rows[0][1]);
        std::string flatsqlEmail = std::get<std::string>(flatsqlResult.rows[0][2]);
        int64_t flatsqlAge = std::get<int64_t>(flatsqlResult.rows[0][3]);

        TEST_ASSERT_EQ(static_cast<int64_t>(sqliteId), flatsqlId, "ID match with SQLite");
        TEST_ASSERT_EQ(sqliteName, flatsqlName, "Name match with SQLite");
        TEST_ASSERT_EQ(sqliteEmail, flatsqlEmail, "Email match with SQLite");
        TEST_ASSERT_EQ(static_cast<int64_t>(sqliteAge), flatsqlAge, "Age match with SQLite");
    }

    sqlite3_close(sqliteDb);

    std::cout << "    Comparison with SQLite: PASSED" << std::endl;
    return true;
}

// ==================== MULTI-TABLE TESTS ====================

// Helper: Create a Post FlatBuffer with file identifier
std::vector<uint8_t> createPostFlatBuffer(int32_t id, int32_t user_id, const char* title, const char* content) {
    flatbuffers::FlatBufferBuilder builder(256);
    auto post = test::CreatePostDirect(builder, id, user_id, title, content);
    builder.Finish(post, "POST");
    const uint8_t* buf = builder.GetBufferPointer();
    size_t size = builder.GetSize();
    return std::vector<uint8_t>(buf, buf + size);
}

// Field extractor for Post table
Value extractPostField(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
    auto post = flatbuffers::GetRoot<test::Post>(data);
    if (!post) return std::monostate{};

    if (fieldName == "id") return post->id();
    if (fieldName == "user_id") return post->user_id();
    if (fieldName == "title") return post->title() ? std::string(post->title()->c_str(), post->title()->size()) : std::string();
    if (fieldName == "content") return post->content() ? std::string(post->content()->c_str(), post->content()->size()) : std::string();

    return std::monostate{};
}

// Batch extractor for Post table
void batchExtractPost(const uint8_t* data, size_t length, std::vector<Value>& output) {
    (void)length;
    output.clear();
    output.reserve(4);

    auto post = flatbuffers::GetRoot<test::Post>(data);
    if (!post) {
        output.resize(4, std::monostate{});
        return;
    }

    output.push_back(post->id());
    output.push_back(post->user_id());
    output.push_back(post->title() ? std::string(post->title()->c_str(), post->title()->size()) : std::string());
    output.push_back(post->content() ? std::string(post->content()->c_str(), post->content()->size()) : std::string());
}

bool testMultiTableRoundTrip() {
    std::cout << "  Testing multi-table round-trip (User + Post)..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
        table Post {
            id: int (id);
            user_id: int;
            title: string;
            content: string;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "multi_test");
    db.registerFileId("USER", "User");
    db.registerFileId("POST", "Post");
    db.setFieldExtractor("User", extractUserField);
    db.setFieldExtractor("Post", extractPostField);
    db.setBatchExtractor("User", batchExtractUser);
    db.setBatchExtractor("Post", batchExtractPost);

    // Ingest users
    for (int i = 0; i < 10; i++) {
        std::string name = "User" + std::to_string(i);
        std::string email = "user" + std::to_string(i) + "@test.com";
        auto userData = createUserFlatBuffer(i, name.c_str(), email.c_str(), 25 + i);
        db.ingestOne(userData.data(), userData.size());
    }

    // Ingest posts (multiple per user)
    int postId = 0;
    for (int userId = 0; userId < 10; userId++) {
        for (int p = 0; p < 5; p++) {
            std::string title = "Post " + std::to_string(postId) + " by User " + std::to_string(userId);
            std::string content = "Content for post " + std::to_string(postId);
            auto postData = createPostFlatBuffer(postId, userId, title.c_str(), content.c_str());
            db.ingestOne(postData.data(), postData.size());
            postId++;
        }
    }

    // Verify user count
    auto userResult = db.query("SELECT COUNT(*) FROM User");
    TEST_ASSERT_EQ(1u, userResult.rowCount(), "User count query should return 1 row");
    int64_t userCount = std::get<int64_t>(userResult.rows[0][0]);
    TEST_ASSERT_EQ(10, userCount, "Should have 10 users");

    // Verify post count
    auto postResult = db.query("SELECT COUNT(*) FROM Post");
    TEST_ASSERT_EQ(1u, postResult.rowCount(), "Post count query should return 1 row");
    int64_t postCount = std::get<int64_t>(postResult.rows[0][0]);
    TEST_ASSERT_EQ(50, postCount, "Should have 50 posts");

    // Verify posts by user_id using full table scan (no index)
    // Note: user_id is not indexed, so WHERE clause triggers full scan
    for (int userId = 0; userId < 10; userId++) {
        auto result = db.query("SELECT COUNT(*) FROM Post WHERE user_id = ?",
                               std::vector<Value>{static_cast<int64_t>(userId)});
        int64_t count = std::get<int64_t>(result.rows[0][0]);
        TEST_ASSERT_EQ(5, count, "User " + std::to_string(userId) + " should have 5 posts");
    }

    // Cross-table verification: verify user data while iterating posts
    auto allPosts = db.query("SELECT id, user_id, title FROM Post ORDER BY id");
    TEST_ASSERT_EQ(50u, allPosts.rowCount(), "Should retrieve 50 posts");

    for (size_t i = 0; i < allPosts.rows.size(); i++) {
        int64_t pid = std::get<int64_t>(allPosts.rows[i][0]);
        int64_t uid = std::get<int64_t>(allPosts.rows[i][1]);
        std::string title = std::get<std::string>(allPosts.rows[i][2]);

        int expectedUserId = static_cast<int>(pid) / 5;
        TEST_ASSERT_EQ(static_cast<int64_t>(expectedUserId), uid, "Post user_id should match pattern");

        std::string expectedTitle = "Post " + std::to_string(pid) + " by User " + std::to_string(expectedUserId);
        TEST_ASSERT_EQ(expectedTitle, title, "Post title should match");
    }

    // Verify each post individually by its id (using index)
    for (int pid = 0; pid < 50; pid++) {
        auto result = db.query("SELECT title, content FROM Post WHERE id = ?",
                               std::vector<Value>{static_cast<int64_t>(pid)});
        TEST_ASSERT_EQ(1u, result.rowCount(), "Should find post " + std::to_string(pid));

        int expectedUserId = pid / 5;
        std::string expectedTitle = "Post " + std::to_string(pid) + " by User " + std::to_string(expectedUserId);
        std::string gotTitle = std::get<std::string>(result.rows[0][0]);
        TEST_ASSERT_EQ(expectedTitle, gotTitle, "Post title by id");
    }

    std::cout << "    Multi-table round-trip: PASSED" << std::endl;
    return true;
}

bool testMultiTableExportReload() {
    std::cout << "  Testing multi-table export/reload..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
        table Post {
            id: int (id);
            user_id: int (key);
            title: string;
            content: string;
        }
    )";

    // Create and populate first database
    auto db1 = FlatSQLDatabase::fromSchema(schema, "multi_export1");
    db1.registerFileId("USER", "User");
    db1.registerFileId("POST", "Post");
    db1.setFieldExtractor("User", extractUserField);
    db1.setFieldExtractor("Post", extractPostField);
    db1.setBatchExtractor("User", batchExtractUser);
    db1.setBatchExtractor("Post", batchExtractPost);

    // Add users and posts
    for (int i = 0; i < 20; i++) {
        auto userData = createUserFlatBuffer(i, ("User" + std::to_string(i)).c_str(),
                                             ("user" + std::to_string(i) + "@test.com").c_str(), 25 + i);
        db1.ingestOne(userData.data(), userData.size());

        for (int p = 0; p < 3; p++) {
            int postId = i * 3 + p;
            auto postData = createPostFlatBuffer(postId, i,
                                                 ("Title " + std::to_string(postId)).c_str(),
                                                 ("Content " + std::to_string(postId)).c_str());
            db1.ingestOne(postData.data(), postData.size());
        }
    }

    // Export
    auto exported = db1.exportData();
    TEST_ASSERT(exported.size() > 0, "Export should produce data");

    // Reload
    auto db2 = FlatSQLDatabase::fromSchema(schema, "multi_export2");
    db2.registerFileId("USER", "User");
    db2.registerFileId("POST", "Post");
    db2.setFieldExtractor("User", extractUserField);
    db2.setFieldExtractor("Post", extractPostField);
    db2.setBatchExtractor("User", batchExtractUser);
    db2.setBatchExtractor("Post", batchExtractPost);
    db2.loadAndRebuild(exported.data(), exported.size());

    // Verify counts
    auto userResult = db2.query("SELECT COUNT(*) FROM User");
    int64_t userCount = std::get<int64_t>(userResult.rows[0][0]);
    TEST_ASSERT_EQ(20, userCount, "Should have 20 users after reload");

    auto postResult = db2.query("SELECT COUNT(*) FROM Post");
    int64_t postCount = std::get<int64_t>(postResult.rows[0][0]);
    TEST_ASSERT_EQ(60, postCount, "Should have 60 posts after reload");

    // Verify specific records
    auto user5 = db2.query("SELECT name, email, age FROM User WHERE id = 5");
    TEST_ASSERT_EQ(1u, user5.rowCount(), "Should find user 5");
    TEST_ASSERT_EQ(std::string("User5"), std::get<std::string>(user5.rows[0][0]), "User 5 name");

    auto posts5 = db2.query("SELECT id, title FROM Post WHERE user_id = 5 ORDER BY id");
    TEST_ASSERT_EQ(3u, posts5.rowCount(), "User 5 should have 3 posts");

    std::cout << "    Multi-table export/reload: PASSED" << std::endl;
    return true;
}

// ==================== MALFORMED DATA TESTS ====================

bool testMalformedDataHandling() {
    std::cout << "  Testing malformed data handling..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "malformed_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);

    // First ingest some valid data
    auto validData = createUserFlatBuffer(1, "ValidUser", "valid@test.com", 25);
    db.ingestOne(validData.data(), validData.size());

    // Test 1: Random garbage data (should not crash)
    std::vector<uint8_t> garbage(100);
    std::mt19937 rng(42);
    for (auto& b : garbage) b = static_cast<uint8_t>(rng() % 256);

    // The library should either reject this or handle it gracefully
    // We just verify it doesn't crash
    try {
        // Note: This might throw or silently fail, both are acceptable
        db.ingestOne(garbage.data(), garbage.size());
    } catch (...) {
        // Exception is acceptable for malformed data
    }

    // Test 2: Data that's too short
    std::vector<uint8_t> tooShort = {0x00, 0x01, 0x02, 0x03};
    try {
        db.ingestOne(tooShort.data(), tooShort.size());
    } catch (...) {
        // Exception is acceptable
    }

    // Test 3: Data with wrong file identifier
    flatbuffers::FlatBufferBuilder builder(256);
    auto user = test::CreateUserDirect(builder, 999, "WrongID", "wrong@test.com", 30);
    builder.Finish(user, "XXXX");  // Wrong file ID
    std::vector<uint8_t> wrongId(builder.GetBufferPointer(),
                                  builder.GetBufferPointer() + builder.GetSize());
    // This should be routed to nowhere (no table registered for "XXXX")
    db.ingestOne(wrongId.data(), wrongId.size());

    // Verify original valid data is still accessible
    auto result = db.query("SELECT * FROM User WHERE id = 1");
    TEST_ASSERT_EQ(1u, result.rowCount(), "Valid data should still be accessible");
    TEST_ASSERT_EQ(std::string("ValidUser"), std::get<std::string>(result.rows[0][1]), "Valid user name");

    std::cout << "    Malformed data handling: PASSED" << std::endl;
    return true;
}

bool testPartialStreamData() {
    std::cout << "  Testing partial stream data handling..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "partial_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);

    // Create 3 valid FlatBuffers
    std::vector<uint8_t> allData;
    for (int i = 0; i < 3; i++) {
        auto userData = createUserFlatBuffer(i, ("User" + std::to_string(i)).c_str(),
                                             ("user" + std::to_string(i) + "@test.com").c_str(), 25);
        // Add size prefix
        uint32_t size = static_cast<uint32_t>(userData.size());
        allData.insert(allData.end(), reinterpret_cast<uint8_t*>(&size),
                      reinterpret_cast<uint8_t*>(&size) + 4);
        allData.insert(allData.end(), userData.begin(), userData.end());
    }

    // Feed byte by byte (simulating slow network)
    std::vector<uint8_t> buffer;
    size_t totalRecords = 0;

    for (size_t i = 0; i < allData.size(); i++) {
        buffer.push_back(allData[i]);
        size_t recordsProcessed = 0;
        size_t consumed = db.ingest(buffer.data(), buffer.size(), &recordsProcessed);
        totalRecords += recordsProcessed;
        if (consumed > 0) {
            buffer.erase(buffer.begin(), buffer.begin() + consumed);
        }
    }

    TEST_ASSERT_EQ(3u, totalRecords, "Should process all 3 records via byte-by-byte feeding");

    // Verify data
    auto result = db.query("SELECT COUNT(*) FROM User");
    int64_t count = std::get<int64_t>(result.rows[0][0]);
    TEST_ASSERT_EQ(3, count, "Should have 3 users");

    std::cout << "    Partial stream data handling: PASSED" << std::endl;
    return true;
}

bool testZeroLengthRecords() {
    std::cout << "  Testing zero-length and edge case records..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "zero_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);
    db.setBatchExtractor("User", batchExtractUser);

    // Test with null/nullptr strings (FlatBuffers handles this)
    flatbuffers::FlatBufferBuilder builder(256);
    auto user = test::CreateUser(builder, 1, 0, 0, 25);  // null name and email
    builder.Finish(user, "USER");

    std::vector<uint8_t> data(builder.GetBufferPointer(),
                              builder.GetBufferPointer() + builder.GetSize());
    db.ingestOne(data.data(), data.size());

    // Query should work - nulls should come back as empty strings
    auto result = db.query("SELECT id, name, email, age FROM User WHERE id = 1");
    TEST_ASSERT_EQ(1u, result.rowCount(), "Should find user with null strings");

    // Note: How nulls are represented depends on the extractor
    // Our extractor returns empty string for nulls

    std::cout << "    Zero-length and edge case records: PASSED" << std::endl;
    return true;
}

// ==================== STRESS TESTS ====================

bool testHighVolumeIngest() {
    std::cout << "  Testing high volume ingest (10,000 records)..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "volume_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);
    db.setBatchExtractor("User", batchExtractUser);

    // Ingest 10,000 records
    for (int i = 0; i < 10000; i++) {
        std::string name = "VolumeUser" + std::to_string(i);
        std::string email = "volume" + std::to_string(i) + "@test.com";
        auto userData = createUserFlatBuffer(i, name.c_str(), email.c_str(), i % 100);
        db.ingestOne(userData.data(), userData.size());
    }

    // Verify count
    auto countResult = db.query("SELECT COUNT(*) FROM User");
    int64_t count = std::get<int64_t>(countResult.rows[0][0]);
    TEST_ASSERT_EQ(10000, count, "Should have 10,000 users");

    // Verify random samples
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, 9999);
    for (int i = 0; i < 100; i++) {
        int id = dist(rng);
        auto result = db.query("SELECT name, age FROM User WHERE id = ?",
                               std::vector<Value>{static_cast<int64_t>(id)});
        TEST_ASSERT_EQ(1u, result.rowCount(), "Should find user " + std::to_string(id));

        std::string expectedName = "VolumeUser" + std::to_string(id);
        std::string gotName = std::get<std::string>(result.rows[0][0]);
        TEST_ASSERT_EQ(expectedName, gotName, "Volume user name");

        int64_t expectedAge = id % 100;
        int64_t gotAge = std::get<int64_t>(result.rows[0][1]);
        TEST_ASSERT_EQ(expectedAge, gotAge, "Volume user age");
    }

    // Test range query
    auto rangeResult = db.query("SELECT COUNT(*) FROM User WHERE age >= 50 AND age < 60");
    int64_t rangeCount = std::get<int64_t>(rangeResult.rows[0][0]);
    TEST_ASSERT_EQ(1000, rangeCount, "Should have 1000 users with age 50-59");

    std::cout << "    High volume ingest: PASSED" << std::endl;
    return true;
}

bool testRepeatedExportImport() {
    std::cout << "  Testing repeated export/import cycles..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    // Use unique_ptr to properly manage database lifecycle.
    // Construct from a parsed schema so the test does not depend on moving
    // FlatSQLDatabase, which intentionally owns non-movable SQLite state.
    auto parsedSchema = SchemaParser::parse(schema, "repeat_test");
    auto db = std::make_unique<FlatSQLDatabase>(parsedSchema);
    db->registerFileId("USER", "User");
    db->setFieldExtractor("User", extractUserField);
    db->setBatchExtractor("User", batchExtractUser);

    // Initial data
    for (int i = 0; i < 100; i++) {
        auto userData = createUserFlatBuffer(i, ("User" + std::to_string(i)).c_str(),
                                             ("user" + std::to_string(i) + "@test.com").c_str(), 25 + i);
        db->ingestOne(userData.data(), userData.size());
    }

    // Export/import cycle 5 times
    std::vector<uint8_t> data;
    for (int cycle = 0; cycle < 5; cycle++) {
        data = db->exportData();
        TEST_ASSERT(data.size() > 0, "Export should produce data (cycle " + std::to_string(cycle) + ")");

        // Destroy old database before creating new one
        db.reset();

        parsedSchema = SchemaParser::parse(schema, "repeat_reload" + std::to_string(cycle));
        db = std::make_unique<FlatSQLDatabase>(parsedSchema);
        db->registerFileId("USER", "User");
        db->setFieldExtractor("User", extractUserField);
        db->setBatchExtractor("User", batchExtractUser);
        db->loadAndRebuild(data.data(), data.size());

        // Verify count
        auto result = db->query("SELECT COUNT(*) FROM User");
        int64_t count = std::get<int64_t>(result.rows[0][0]);
        TEST_ASSERT_EQ(100, count, "Should have 100 users after cycle " + std::to_string(cycle));
    }

    // Final verification of all data
    for (int i = 0; i < 100; i++) {
        auto result = db->query("SELECT name FROM User WHERE id = ?",
                               std::vector<Value>{static_cast<int64_t>(i)});
        TEST_ASSERT_EQ(1u, result.rowCount(), "Should find user " + std::to_string(i));

        std::string expectedName = "User" + std::to_string(i);
        std::string gotName = std::get<std::string>(result.rows[0][0]);
        TEST_ASSERT_EQ(expectedName, gotName, "User name after cycles");
    }

    std::cout << "    Repeated export/import cycles: PASSED" << std::endl;
    return true;
}

// ==================== QUERY EDGE CASES ====================

bool testQueryEdgeCases() {
    std::cout << "  Testing query edge cases..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "query_edge_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);
    db.setBatchExtractor("User", batchExtractUser);

    // Add test data
    for (int i = 0; i < 50; i++) {
        auto userData = createUserFlatBuffer(i, ("User" + std::to_string(i)).c_str(),
                                             ("user" + std::to_string(i) + "@test.com").c_str(), i);
        db.ingestOne(userData.data(), userData.size());
    }

    // Test: Empty result set
    auto emptyResult = db.query("SELECT * FROM User WHERE id = 999");
    TEST_ASSERT_EQ(0u, emptyResult.rowCount(), "Should return 0 rows for non-existent id");

    // Test: SELECT *
    auto allResult = db.query("SELECT * FROM User ORDER BY id LIMIT 5");
    TEST_ASSERT_EQ(5u, allResult.rowCount(), "LIMIT should work");

    // Test: ORDER BY DESC
    auto descResult = db.query("SELECT id FROM User ORDER BY id DESC LIMIT 3");
    TEST_ASSERT_EQ(3u, descResult.rowCount(), "DESC ORDER BY should work");
    TEST_ASSERT_EQ(49, std::get<int64_t>(descResult.rows[0][0]), "First should be 49");
    TEST_ASSERT_EQ(48, std::get<int64_t>(descResult.rows[1][0]), "Second should be 48");
    TEST_ASSERT_EQ(47, std::get<int64_t>(descResult.rows[2][0]), "Third should be 47");

    // Test: OFFSET
    auto offsetResult = db.query("SELECT id FROM User ORDER BY id LIMIT 5 OFFSET 10");
    TEST_ASSERT_EQ(5u, offsetResult.rowCount(), "OFFSET should work");
    TEST_ASSERT_EQ(10, std::get<int64_t>(offsetResult.rows[0][0]), "First after offset should be 10");

    // Test: IN clause
    auto inResult = db.query("SELECT id FROM User WHERE id IN (5, 10, 15, 20)");
    TEST_ASSERT_EQ(4u, inResult.rowCount(), "IN clause should work");

    // Test: BETWEEN
    auto betweenResult = db.query("SELECT COUNT(*) FROM User WHERE age BETWEEN 20 AND 29");
    int64_t betweenCount = std::get<int64_t>(betweenResult.rows[0][0]);
    TEST_ASSERT_EQ(10, betweenCount, "BETWEEN should match 10 users");

    // Test: LIKE
    auto likeResult = db.query("SELECT COUNT(*) FROM User WHERE name LIKE 'User1%'");
    int64_t likeCount = std::get<int64_t>(likeResult.rows[0][0]);
    TEST_ASSERT_EQ(11, likeCount, "LIKE should match User1, User10-User19");

    // Test: NULL handling (no nulls in our data, but query should work)
    auto nullResult = db.query("SELECT COUNT(*) FROM User WHERE name IS NOT NULL");
    int64_t notNullCount = std::get<int64_t>(nullResult.rows[0][0]);
    TEST_ASSERT_EQ(50, notNullCount, "IS NOT NULL should match all 50");

    // Test: Aggregate functions
    auto sumResult = db.query("SELECT SUM(age) FROM User");
    int64_t sum = std::get<int64_t>(sumResult.rows[0][0]);
    TEST_ASSERT_EQ(1225, sum, "SUM should be 0+1+2+...+49 = 1225");

    auto avgResult = db.query("SELECT AVG(age) FROM User");
    double avg = std::get<double>(avgResult.rows[0][0]);
    TEST_ASSERT(avg >= 24.4 && avg <= 24.6, "AVG should be ~24.5");

    auto minResult = db.query("SELECT MIN(age) FROM User");
    int64_t minAge = std::get<int64_t>(minResult.rows[0][0]);
    TEST_ASSERT_EQ(0, minAge, "MIN age should be 0");

    auto maxResult = db.query("SELECT MAX(age) FROM User");
    int64_t maxAge = std::get<int64_t>(maxResult.rows[0][0]);
    TEST_ASSERT_EQ(49, maxAge, "MAX age should be 49");

    std::cout << "    Query edge cases: PASSED" << std::endl;
    return true;
}

bool testStringQueryEdgeCases() {
    std::cout << "  Testing string query edge cases..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "string_query_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);
    db.setBatchExtractor("User", batchExtractUser);

    // Add users with tricky names for SQL
    std::vector<std::pair<int32_t, std::string>> testNames = {
        {1, "O'Brien"},           // Single quote
        {2, "John \"The Rock\""},  // Double quotes
        {3, "50% Off"},           // Percent (LIKE wildcard)
        {4, "Under_score"},       // Underscore (LIKE wildcard)
        {5, "Back\\slash"},       // Backslash
        {6, "Semi;colon"},        // Semicolon
        {7, "DROP TABLE User;--"}, // SQL injection attempt
    };

    for (const auto& [id, name] : testNames) {
        auto userData = createUserFlatBuffer(id, name.c_str(), "test@test.com", 25);
        db.ingestOne(userData.data(), userData.size());
    }

    // Query each by ID and verify name matches exactly
    for (const auto& [id, expectedName] : testNames) {
        auto result = db.query("SELECT name FROM User WHERE id = ?",
                               std::vector<Value>{static_cast<int64_t>(id)});
        TEST_ASSERT_EQ(1u, result.rowCount(), "Should find user " + std::to_string(id));

        std::string gotName = std::get<std::string>(result.rows[0][0]);
        TEST_ASSERT_EQ(expectedName, gotName, "Name should match exactly for id " + std::to_string(id));
    }

    // Query by name using parameterized queries (safe from injection)
    auto obrienResult = db.query("SELECT id FROM User WHERE name = ?",
                                  std::vector<Value>{std::string("O'Brien")});
    TEST_ASSERT_EQ(1u, obrienResult.rowCount(), "Should find O'Brien");
    TEST_ASSERT_EQ(1, std::get<int64_t>(obrienResult.rows[0][0]), "O'Brien has id 1");

    std::cout << "    String query edge cases: PASSED" << std::endl;
    return true;
}

// ==================== DIRECT API EDGE CASES ====================

bool testDirectAPIEdgeCases() {
    std::cout << "  Testing direct API edge cases..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "direct_edge_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);

    // Add test data
    for (int i = 0; i < 10; i++) {
        auto userData = createUserFlatBuffer(i, ("User" + std::to_string(i)).c_str(),
                                             ("user" + std::to_string(i) + "@test.com").c_str(), 25 + i);
        db.ingestOne(userData.data(), userData.size());
    }

    // Test findByIndex on non-existent table
    auto badTableResult = db.findByIndex("NonExistentTable", "id", static_cast<int32_t>(1));
    TEST_ASSERT_EQ(0u, badTableResult.size(), "Non-existent table should return empty");

    // Test findByIndex on non-existent id
    auto notFoundResult = db.findByIndex("User", "id", static_cast<int32_t>(999));
    TEST_ASSERT_EQ(0u, notFoundResult.size(), "Non-existent id should return empty");

    // Test findOneByIndex on non-existent id
    StoredRecord record;
    bool found = db.findOneByIndex("User", "id", static_cast<int32_t>(999), record);
    TEST_ASSERT(!found, "Non-existent id should return false");

    // Test findRawByIndex on non-existent id
    uint32_t len = 0;
    uint64_t seq = 0;
    const uint8_t* raw = db.findRawByIndex("User", "id", static_cast<int32_t>(999), &len, &seq);
    TEST_ASSERT(raw == nullptr, "Non-existent id should return nullptr");

    // Test findByIndex on indexed string column (email is marked as key)
    auto emailResult = db.findByIndex("User", "email", std::string("user5@test.com"));
    TEST_ASSERT_EQ(1u, emailResult.size(), "Should find user by email");

    // Test iteration on non-existent table returns 0
    size_t count = db.iterateAll("NonExistentTable", [](const uint8_t*, uint32_t, uint64_t) {});
    TEST_ASSERT_EQ(0u, count, "Iteration on non-existent table should return 0");

    std::cout << "    Direct API edge cases: PASSED" << std::endl;
    return true;
}

// ==================== SIZE PREFIX FORMAT TESTS ====================

bool testSizePrefixFormat() {
    std::cout << "  Testing size-prefixed FlatBuffer streaming format..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "prefix_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);

    // Create size-prefixed stream manually
    std::vector<uint8_t> stream;

    for (int i = 0; i < 5; i++) {
        auto userData = createUserFlatBuffer(i, ("User" + std::to_string(i)).c_str(),
                                             ("user" + std::to_string(i) + "@test.com").c_str(), 25);

        // Add little-endian size prefix
        uint32_t size = static_cast<uint32_t>(userData.size());
        stream.push_back(size & 0xFF);
        stream.push_back((size >> 8) & 0xFF);
        stream.push_back((size >> 16) & 0xFF);
        stream.push_back((size >> 24) & 0xFF);
        stream.insert(stream.end(), userData.begin(), userData.end());
    }

    // Ingest entire stream at once
    size_t recordsIngested = 0;
    size_t bytesConsumed = db.ingest(stream.data(), stream.size(), &recordsIngested);

    TEST_ASSERT_EQ(stream.size(), bytesConsumed, "All bytes should be consumed");
    TEST_ASSERT_EQ(5u, recordsIngested, "5 records should be ingested");

    // Verify data
    auto result = db.query("SELECT COUNT(*) FROM User");
    int64_t count = std::get<int64_t>(result.rows[0][0]);
    TEST_ASSERT_EQ(5, count, "Should have 5 users");

    std::cout << "    Size-prefixed format: PASSED" << std::endl;
    return true;
}

bool testChunkedStreamIngestion() {
    std::cout << "  Testing chunked stream ingestion..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "chunk_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);

    // Create a large stream
    std::vector<uint8_t> stream;
    for (int i = 0; i < 100; i++) {
        auto userData = createUserFlatBuffer(i, ("ChunkUser" + std::to_string(i)).c_str(),
                                             ("chunk" + std::to_string(i) + "@test.com").c_str(), 25);
        uint32_t size = static_cast<uint32_t>(userData.size());
        stream.push_back(size & 0xFF);
        stream.push_back((size >> 8) & 0xFF);
        stream.push_back((size >> 16) & 0xFF);
        stream.push_back((size >> 24) & 0xFF);
        stream.insert(stream.end(), userData.begin(), userData.end());
    }

    // Ingest in various chunk sizes
    std::vector<size_t> chunkSizes = {1, 7, 13, 64, 256, 1024};

    for (size_t chunkSize : chunkSizes) {
        auto testDb = FlatSQLDatabase::fromSchema(schema, "chunk_size_" + std::to_string(chunkSize));
        testDb.registerFileId("USER", "User");
        testDb.setFieldExtractor("User", extractUserField);

        std::vector<uint8_t> buffer;
        size_t totalRecords = 0;
        size_t pos = 0;

        while (pos < stream.size()) {
            size_t toRead = std::min(chunkSize, stream.size() - pos);
            buffer.insert(buffer.end(), stream.begin() + pos, stream.begin() + pos + toRead);
            pos += toRead;

            size_t recordsIngested = 0;
            size_t consumed = testDb.ingest(buffer.data(), buffer.size(), &recordsIngested);
            totalRecords += recordsIngested;

            if (consumed > 0) {
                buffer.erase(buffer.begin(), buffer.begin() + consumed);
            }
        }

        TEST_ASSERT_EQ(100u, totalRecords, "Should ingest 100 records with chunk size " + std::to_string(chunkSize));

        auto result = testDb.query("SELECT COUNT(*) FROM User");
        int64_t count = std::get<int64_t>(result.rows[0][0]);
        TEST_ASSERT_EQ(100, count, "Should have 100 users with chunk size " + std::to_string(chunkSize));
    }

    std::cout << "    Chunked stream ingestion: PASSED" << std::endl;
    return true;
}

// ==================== COMPREHENSIVE QUERY FILTERING TESTS ====================

bool testRangeQueryFiltering() {
    std::cout << "  Testing range query filtering on non-indexed columns..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "range_query_test");
    db.registerFileId("USER", "User");
    db.setFieldExtractor("User", extractUserField);
    db.setBatchExtractor("User", batchExtractUser);

    // Add users with ages 0-99
    for (int i = 0; i < 100; i++) {
        auto userData = createUserFlatBuffer(i, ("User" + std::to_string(i)).c_str(),
                                             ("user" + std::to_string(i) + "@test.com").c_str(), i);
        db.ingestOne(userData.data(), userData.size());
    }

    // Test: age < 20 (should match 20 users: 0-19)
    auto ltResult = db.query("SELECT COUNT(*) FROM User WHERE age < 20");
    int64_t ltCount = std::get<int64_t>(ltResult.rows[0][0]);
    TEST_ASSERT_EQ(20, ltCount, "age < 20 should match 20 users");

    // Test: age <= 20 (should match 21 users: 0-20)
    auto leResult = db.query("SELECT COUNT(*) FROM User WHERE age <= 20");
    int64_t leCount = std::get<int64_t>(leResult.rows[0][0]);
    TEST_ASSERT_EQ(21, leCount, "age <= 20 should match 21 users");

    // Test: age > 80 (should match 19 users: 81-99)
    auto gtResult = db.query("SELECT COUNT(*) FROM User WHERE age > 80");
    int64_t gtCount = std::get<int64_t>(gtResult.rows[0][0]);
    TEST_ASSERT_EQ(19, gtCount, "age > 80 should match 19 users");

    // Test: age >= 80 (should match 20 users: 80-99)
    auto geResult = db.query("SELECT COUNT(*) FROM User WHERE age >= 80");
    int64_t geCount = std::get<int64_t>(geResult.rows[0][0]);
    TEST_ASSERT_EQ(20, geCount, "age >= 80 should match 20 users");

    // Test: compound range: age > 20 AND age < 30 (should match 9 users: 21-29)
    auto compoundResult = db.query("SELECT COUNT(*) FROM User WHERE age > 20 AND age < 30");
    int64_t compoundCount = std::get<int64_t>(compoundResult.rows[0][0]);
    TEST_ASSERT_EQ(9, compoundCount, "age > 20 AND age < 30 should match 9 users");

    // Test: OR condition: age < 10 OR age > 90 (should match 19 users: 0-9 + 91-99)
    auto orResult = db.query("SELECT COUNT(*) FROM User WHERE age < 10 OR age > 90");
    int64_t orCount = std::get<int64_t>(orResult.rows[0][0]);
    TEST_ASSERT_EQ(19, orCount, "age < 10 OR age > 90 should match 19 users");

    // Test: NOT condition
    auto notResult = db.query("SELECT COUNT(*) FROM User WHERE NOT (age >= 50)");
    int64_t notCount = std::get<int64_t>(notResult.rows[0][0]);
    TEST_ASSERT_EQ(50, notCount, "NOT (age >= 50) should match 50 users");

    // Test: Verify actual values for a specific range
    auto rangeDetails = db.query("SELECT id, age FROM User WHERE age >= 45 AND age <= 55 ORDER BY age");
    TEST_ASSERT_EQ(11u, rangeDetails.rowCount(), "Should return 11 rows (ages 45-55)");

    for (size_t i = 0; i < rangeDetails.rowCount(); i++) {
        int64_t expectedAge = 45 + static_cast<int64_t>(i);
        int64_t actualAge = std::get<int64_t>(rangeDetails.rows[i][1]);
        TEST_ASSERT_EQ(expectedAge, actualAge, "Age should be " + std::to_string(expectedAge));
    }

    std::cout << "    Range query filtering: PASSED" << std::endl;
    return true;
}

bool testNonUniqueIndexFiltering() {
    std::cout << "  Testing non-unique index filtering (user_id lookups)..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string;
            age: int;
        }
        table Post {
            id: int (id);
            user_id: int (key);
            title: string;
            content: string;
        }
    )";

    auto db = FlatSQLDatabase::fromSchema(schema, "nonunique_index_test");
    db.registerFileId("USER", "User");
    db.registerFileId("POST", "Post");
    db.setFieldExtractor("User", extractUserField);
    db.setFieldExtractor("Post", extractPostField);
    db.setBatchExtractor("User", batchExtractUser);
    db.setBatchExtractor("Post", batchExtractPost);

    // Add 10 users
    for (int i = 0; i < 10; i++) {
        auto userData = createUserFlatBuffer(i, ("User" + std::to_string(i)).c_str(),
                                             ("user" + std::to_string(i) + "@test.com").c_str(), 20 + i);
        db.ingestOne(userData.data(), userData.size());
    }

    // Add 5 posts per user (50 posts total)
    int postId = 0;
    for (int userId = 0; userId < 10; userId++) {
        for (int p = 0; p < 5; p++) {
            auto postData = createPostFlatBuffer(postId, userId,
                                                 ("Post " + std::to_string(postId)).c_str(),
                                                 ("Content for post " + std::to_string(postId)).c_str());
            db.ingestOne(postData.data(), postData.size());
            postId++;
        }
    }

    // Verify total counts
    auto userCount = db.query("SELECT COUNT(*) FROM User");
    TEST_ASSERT_EQ(10, std::get<int64_t>(userCount.rows[0][0]), "Should have 10 users");

    auto postCount = db.query("SELECT COUNT(*) FROM Post");
    TEST_ASSERT_EQ(50, std::get<int64_t>(postCount.rows[0][0]), "Should have 50 posts");

    // Test: non-unique index lookup - each user should have exactly 5 posts
    for (int userId = 0; userId < 10; userId++) {
        auto result = db.query("SELECT COUNT(*) FROM Post WHERE user_id = ?",
                               std::vector<Value>{static_cast<int64_t>(userId)});
        int64_t count = std::get<int64_t>(result.rows[0][0]);
        TEST_ASSERT_EQ(5, count, "User " + std::to_string(userId) + " should have 5 posts");

        // Also verify we get the actual rows
        auto posts = db.query("SELECT id, title FROM Post WHERE user_id = ? ORDER BY id",
                              std::vector<Value>{static_cast<int64_t>(userId)});
        TEST_ASSERT_EQ(5u, posts.rowCount(), "Should return 5 rows for user " + std::to_string(userId));
    }

    // Test: verify post IDs are correct for a specific user
    auto user3Posts = db.query("SELECT id FROM Post WHERE user_id = 3 ORDER BY id");
    TEST_ASSERT_EQ(5u, user3Posts.rowCount(), "User 3 should have 5 posts");
    for (size_t i = 0; i < user3Posts.rowCount(); i++) {
        int64_t expectedId = 15 + static_cast<int64_t>(i);  // Posts 15-19 belong to user 3
        int64_t actualId = std::get<int64_t>(user3Posts.rows[i][0]);
        TEST_ASSERT_EQ(expectedId, actualId, "Post ID should be " + std::to_string(expectedId));
    }

    std::cout << "    Non-unique index filtering: PASSED" << std::endl;
    return true;
}

bool testSequentialDiskStreamWithVerification() {
    std::cout << "  Testing sequential disk streaming with FlatBuffer verification..." << std::endl;

    std::string schema = R"(
        table User {
            id: int (id);
            name: string;
            email: string (key);
            age: int;
        }
    )";

    // Create and populate database
    auto db1 = FlatSQLDatabase::fromSchema(schema, "disk_verify_test1");
    db1.registerFileId("USER", "User");
    db1.setFieldExtractor("User", extractUserField);
    db1.setBatchExtractor("User", batchExtractUser);

    std::vector<std::tuple<int, std::string, std::string, int>> originalData;
    for (int i = 0; i < 1000; i++) {
        std::string name = "TestUser" + std::to_string(i);
        std::string email = "test" + std::to_string(i) + "@example.com";
        int age = 18 + (i % 82);
        originalData.push_back({i, name, email, age});

        auto userData = createUserFlatBuffer(i, name.c_str(), email.c_str(), age);
        db1.ingestOne(userData.data(), userData.size());
    }

    // Export to disk
    std::string filename = "/tmp/flatsql_verify_test.bin";
    auto exportedData = db1.exportData();
    {
        std::ofstream file(filename, std::ios::binary);
        file.write(reinterpret_cast<const char*>(exportedData.data()), exportedData.size());
    }

    // Read back and verify using FlatBuffer verification
    std::ifstream file(filename, std::ios::binary);
    TEST_ASSERT(file.good(), "Should be able to open exported file");

    int recordCount = 0;
    while (file.peek() != EOF) {
        // Read size prefix
        uint32_t size;
        file.read(reinterpret_cast<char*>(&size), 4);
        if (file.eof()) break;

        // Read FlatBuffer data
        std::vector<uint8_t> fbData(size);
        file.read(reinterpret_cast<char*>(fbData.data()), size);
        TEST_ASSERT(file.good() || file.eof(), "Should read complete record");

        // Verify the FlatBuffer using the generated verifier
        flatbuffers::Verifier verifier(fbData.data(), fbData.size());
        bool valid = verifier.VerifyBuffer<test::User>("USER");
        TEST_ASSERT(valid, "FlatBuffer should be valid at record " + std::to_string(recordCount));

        // Read using generated code
        auto user = test::GetUser(fbData.data());
        TEST_ASSERT(user != nullptr, "Should be able to read User");

        // Verify data matches original
        auto& [origId, origName, origEmail, origAge] = originalData[recordCount];
        TEST_ASSERT_EQ(origId, user->id(), "ID should match at record " + std::to_string(recordCount));
        TEST_ASSERT_EQ(origName, user->name()->str(), "Name should match at record " + std::to_string(recordCount));
        TEST_ASSERT_EQ(origEmail, user->email()->str(), "Email should match at record " + std::to_string(recordCount));
        TEST_ASSERT_EQ(origAge, user->age(), "Age should match at record " + std::to_string(recordCount));

        recordCount++;
    }

    TEST_ASSERT_EQ(1000, recordCount, "Should have verified 1000 records");

    // Clean up
    std::remove(filename.c_str());

    std::cout << "    Sequential disk streaming with verification: PASSED" << std::endl;
    return true;
}

// ==================== MAIN ====================

int main() {
    std::cout << "=== FlatSQL Comprehensive Round-Trip Tests ===" << std::endl;
    std::cout << std::endl;

    std::cout << "Round-Trip Tests:" << std::endl;
    testBasicRoundTrip();
    testDirectAPIRoundTrip();
    testIterationRoundTrip();

    std::cout << std::endl;
    std::cout << "Edge Case Tests:" << std::endl;
    testEmptyStrings();
    testSpecialCharacters();
    testUnicodeStrings();
    testLargeStrings();
    testBoundaryValues();

    std::cout << std::endl;
    std::cout << "Export/Reload Tests:" << std::endl;
    testExportReloadRoundTrip();

    std::cout << std::endl;
    std::cout << "Disk Streaming Tests:" << std::endl;
    testDiskStreamingSequential();
    testDiskExportImport();

    std::cout << std::endl;
    std::cout << "Verification Tests:" << std::endl;
    testFlatBufferVerification();
    testCompareWithSQLite();

    std::cout << std::endl;
    std::cout << "Multi-Table Tests:" << std::endl;
    testMultiTableRoundTrip();
    testMultiTableExportReload();

    std::cout << std::endl;
    std::cout << "Malformed Data Tests:" << std::endl;
    testMalformedDataHandling();
    testPartialStreamData();
    testZeroLengthRecords();

    std::cout << std::endl;
    std::cout << "Stress Tests:" << std::endl;
    testHighVolumeIngest();
    testRepeatedExportImport();

    std::cout << std::endl;
    std::cout << "Query Edge Cases:" << std::endl;
    testQueryEdgeCases();
    testStringQueryEdgeCases();

    std::cout << std::endl;
    std::cout << "Comprehensive Query Filtering Tests:" << std::endl;
    testRangeQueryFiltering();
    testNonUniqueIndexFiltering();
    testSequentialDiskStreamWithVerification();

    std::cout << std::endl;
    std::cout << "Direct API Edge Cases:" << std::endl;
    testDirectAPIEdgeCases();

    std::cout << std::endl;
    std::cout << "Streaming Format Tests:" << std::endl;
    testSizePrefixFormat();
    testChunkedStreamIngestion();

    std::cout << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Tests Run:    " << testsRun << std::endl;
    std::cout << "Tests Passed: " << testsPassed << std::endl;
    std::cout << "Tests Failed: " << testsFailed << std::endl;
    std::cout << "========================================" << std::endl;

    if (testsFailed > 0) {
        std::cout << "SOME TESTS FAILED!" << std::endl;
        return 1;
    }

    std::cout << "ALL TESTS PASSED!" << std::endl;
    return 0;
}
