#include "flatsql/database.h"
#include <flatbuffers/flatbuffers.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>

#ifdef __EMSCRIPTEN__
#include <emscripten/bind.h>
#include <emscripten/val.h>

using namespace emscripten;

namespace flatsql {

// ==================== FlatBuffer Helpers ====================

// Create a simple User FlatBuffer manually using raw buffer construction
// Schema: table User { id:int; name:string; email:string; age:int; }
std::vector<uint8_t> createUserFlatBuffer(int32_t id, const std::string& name,
                                          const std::string& email, int32_t age) {
    std::vector<uint8_t> buffer;
    buffer.reserve(256);

    // Build from end (FlatBuffers are built back-to-front)
    // Strings first
    std::vector<uint8_t> temp;

    // Helper to write little-endian values
    auto writeU32 = [](std::vector<uint8_t>& v, uint32_t val) {
        v.push_back(val & 0xFF);
        v.push_back((val >> 8) & 0xFF);
        v.push_back((val >> 16) & 0xFF);
        v.push_back((val >> 24) & 0xFF);
    };
    auto writeI32 = [&writeU32](std::vector<uint8_t>& v, int32_t val) {
        writeU32(v, static_cast<uint32_t>(val));
    };
    auto writeU16 = [](std::vector<uint8_t>& v, uint16_t val) {
        v.push_back(val & 0xFF);
        v.push_back((val >> 8) & 0xFF);
    };

    // Simplified: construct a valid FlatBuffer with vtable
    // Format: [root_offset:4][file_id:4]...[data]

    // We'll create a simpler format that our extractor understands
    // Root offset points to table, table starts with negative vtable offset

    std::vector<uint8_t> fb;

    // Leave space for root offset (will fill in at end)
    fb.resize(4);

    // File identifier "USER"
    fb.push_back('U'); fb.push_back('S'); fb.push_back('E'); fb.push_back('R');

    // String data (at end of buffer, we'll add offsets later)
    size_t stringsStart = fb.size();

    // Add padding to align to 4 bytes for table
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t vtableStart = fb.size();

    // VTable: [vtable_size:2][object_size:2][field0_offset:2][field1_offset:2][field2_offset:2][field3_offset:2]
    // 4 fields: id, name, email, age
    writeU16(fb, 12);  // vtable size (2+2+4*2 = 12)
    writeU16(fb, 20);  // object size (4 + 4 + 4 + 4 + 4 = 20, vtable_ref + 4 fields)

    // Field offsets from start of object (after vtable reference)
    writeU16(fb, 4);   // field 0 (id) at offset 4
    writeU16(fb, 8);   // field 1 (name) at offset 8
    writeU16(fb, 12);  // field 2 (email) at offset 12
    writeU16(fb, 16);  // field 3 (age) at offset 16

    // Align to 4 bytes
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t tableStart = fb.size();

    // Table data: [vtable_offset:4][id:4][name_offset:4][email_offset:4][age:4]
    // vtable_offset is signed, points back to vtable
    int32_t vtableOffset = static_cast<int32_t>(tableStart - vtableStart);
    writeI32(fb, vtableOffset);  // points back to vtable
    writeI32(fb, id);            // field 0: id

    // We'll store name string inline after table
    size_t nameStringPos = fb.size() + 12;  // after email_offset and age
    size_t emailStringPos = nameStringPos + 4 + name.size() + 1;
    while ((emailStringPos) % 4 != 0) emailStringPos++;
    emailStringPos += 4;  // length prefix

    // String offsets are relative to the field position
    writeU32(fb, static_cast<uint32_t>(nameStringPos - fb.size()));  // name offset (relative)
    size_t emailOffsetPos = fb.size();
    writeU32(fb, 0);  // placeholder for email offset
    writeI32(fb, age);  // field 3: age

    // Now add strings
    size_t actualNamePos = fb.size();
    writeU32(fb, static_cast<uint32_t>(name.size()));
    for (char c : name) fb.push_back(static_cast<uint8_t>(c));
    fb.push_back(0);  // null terminator

    // Align
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t actualEmailPos = fb.size();
    writeU32(fb, static_cast<uint32_t>(email.size()));
    for (char c : email) fb.push_back(static_cast<uint8_t>(c));
    fb.push_back(0);

    // Fix up name offset (relative from field position)
    size_t nameFieldPos = tableStart + 8;  // vtable_ref(4) + id(4)
    uint32_t nameRelOffset = static_cast<uint32_t>(actualNamePos - nameFieldPos);
    fb[nameFieldPos] = nameRelOffset & 0xFF;
    fb[nameFieldPos+1] = (nameRelOffset >> 8) & 0xFF;
    fb[nameFieldPos+2] = (nameRelOffset >> 16) & 0xFF;
    fb[nameFieldPos+3] = (nameRelOffset >> 24) & 0xFF;

    // Fix up email offset
    size_t emailFieldPos = tableStart + 12;
    uint32_t emailRelOffset = static_cast<uint32_t>(actualEmailPos - emailFieldPos);
    fb[emailFieldPos] = emailRelOffset & 0xFF;
    fb[emailFieldPos+1] = (emailRelOffset >> 8) & 0xFF;
    fb[emailFieldPos+2] = (emailRelOffset >> 16) & 0xFF;
    fb[emailFieldPos+3] = (emailRelOffset >> 24) & 0xFF;

    // Fix up root offset (points to table)
    uint32_t rootOffset = static_cast<uint32_t>(tableStart);
    fb[0] = rootOffset & 0xFF;
    fb[1] = (rootOffset >> 8) & 0xFF;
    fb[2] = (rootOffset >> 16) & 0xFF;
    fb[3] = (rootOffset >> 24) & 0xFF;

    return fb;
}

// Create a simple Post FlatBuffer
std::vector<uint8_t> createPostFlatBuffer(int32_t id, int32_t userId, const std::string& title) {
    std::vector<uint8_t> fb;

    auto writeU32 = [](std::vector<uint8_t>& v, uint32_t val) {
        v.push_back(val & 0xFF);
        v.push_back((val >> 8) & 0xFF);
        v.push_back((val >> 16) & 0xFF);
        v.push_back((val >> 24) & 0xFF);
    };
    auto writeI32 = [&writeU32](std::vector<uint8_t>& v, int32_t val) {
        writeU32(v, static_cast<uint32_t>(val));
    };
    auto writeU16 = [](std::vector<uint8_t>& v, uint16_t val) {
        v.push_back(val & 0xFF);
        v.push_back((val >> 8) & 0xFF);
    };

    // Root offset placeholder
    fb.resize(4);

    // File identifier "POST"
    fb.push_back('P'); fb.push_back('O'); fb.push_back('S'); fb.push_back('T');

    // Align
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t vtableStart = fb.size();

    // VTable for Post: id, user_id, title
    writeU16(fb, 10);  // vtable size
    writeU16(fb, 16);  // object size
    writeU16(fb, 4);   // field 0 (id)
    writeU16(fb, 8);   // field 1 (user_id)
    writeU16(fb, 12);  // field 2 (title)

    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t tableStart = fb.size();

    int32_t vtableOffset = static_cast<int32_t>(tableStart - vtableStart);
    writeI32(fb, vtableOffset);
    writeI32(fb, id);
    writeI32(fb, userId);
    writeU32(fb, 0);  // title offset placeholder

    size_t titleFieldPos = tableStart + 12;
    size_t actualTitlePos = fb.size();
    writeU32(fb, static_cast<uint32_t>(title.size()));
    for (char c : title) fb.push_back(static_cast<uint8_t>(c));
    fb.push_back(0);

    // Fix title offset
    uint32_t titleRelOffset = static_cast<uint32_t>(actualTitlePos - titleFieldPos);
    fb[titleFieldPos] = titleRelOffset & 0xFF;
    fb[titleFieldPos+1] = (titleRelOffset >> 8) & 0xFF;
    fb[titleFieldPos+2] = (titleRelOffset >> 16) & 0xFF;
    fb[titleFieldPos+3] = (titleRelOffset >> 24) & 0xFF;

    // Fix root offset
    fb[0] = tableStart & 0xFF;
    fb[1] = (tableStart >> 8) & 0xFF;
    fb[2] = (tableStart >> 16) & 0xFF;
    fb[3] = (tableStart >> 24) & 0xFF;

    return fb;
}

// Generic field extractor for User FlatBuffers (reads from vtable)
Value extractUserFieldGeneric(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
    if (!data) return std::monostate{};

    // Read root table offset
    uint32_t rootOffset = flatbuffers::ReadScalar<uint32_t>(data);
    const uint8_t* root = data + rootOffset;

    // Read vtable offset (signed, relative to root)
    int32_t vtableOffset = flatbuffers::ReadScalar<int32_t>(root);
    const uint8_t* vtable = root - vtableOffset;

    // Read vtable size and object size
    uint16_t vtableSize = flatbuffers::ReadScalar<uint16_t>(vtable);
    // uint16_t objectSize = flatbuffers::ReadScalar<uint16_t>(vtable + 2);

    auto getFieldOffset = [&](int fieldIndex) -> uint16_t {
        size_t vtableEntry = 4 + fieldIndex * 2;
        if (vtableEntry + 2 > vtableSize) return 0;
        return flatbuffers::ReadScalar<uint16_t>(vtable + vtableEntry);
    };

    if (fieldName == "id") {
        uint16_t off = getFieldOffset(0);
        if (off == 0) return 0;
        return static_cast<int32_t>(flatbuffers::ReadScalar<int32_t>(root + off));
    }
    if (fieldName == "name") {
        uint16_t off = getFieldOffset(1);
        if (off == 0) return std::string();
        uint32_t strOffset = flatbuffers::ReadScalar<uint32_t>(root + off);
        const char* str = reinterpret_cast<const char*>(root + off + strOffset);
        return std::string(str);
    }
    if (fieldName == "email") {
        uint16_t off = getFieldOffset(2);
        if (off == 0) return std::string();
        uint32_t strOffset = flatbuffers::ReadScalar<uint32_t>(root + off);
        const char* str = reinterpret_cast<const char*>(root + off + strOffset);
        return std::string(str);
    }
    if (fieldName == "age") {
        uint16_t off = getFieldOffset(3);
        if (off == 0) return 0;
        return static_cast<int32_t>(flatbuffers::ReadScalar<int32_t>(root + off));
    }

    return std::monostate{};
}

// Generic field extractor for Post FlatBuffers
Value extractPostFieldGeneric(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
    if (!data) return std::monostate{};

    uint32_t rootOffset = flatbuffers::ReadScalar<uint32_t>(data);
    const uint8_t* root = data + rootOffset;
    int32_t vtableOffset = flatbuffers::ReadScalar<int32_t>(root);
    const uint8_t* vtable = root - vtableOffset;
    uint16_t vtableSize = flatbuffers::ReadScalar<uint16_t>(vtable);

    auto getFieldOffset = [&](int fieldIndex) -> uint16_t {
        size_t vtableEntry = 4 + fieldIndex * 2;
        if (vtableEntry + 2 > vtableSize) return 0;
        return flatbuffers::ReadScalar<uint16_t>(vtable + vtableEntry);
    };

    if (fieldName == "id") {
        uint16_t off = getFieldOffset(0);
        if (off == 0) return 0;
        return static_cast<int32_t>(flatbuffers::ReadScalar<int32_t>(root + off));
    }
    if (fieldName == "user_id") {
        uint16_t off = getFieldOffset(1);
        if (off == 0) return 0;
        return static_cast<int32_t>(flatbuffers::ReadScalar<int32_t>(root + off));
    }
    if (fieldName == "title") {
        uint16_t off = getFieldOffset(2);
        if (off == 0) return std::string();
        uint32_t strOffset = flatbuffers::ReadScalar<uint32_t>(root + off);
        const char* str = reinterpret_cast<const char*>(root + off + strOffset);
        return std::string(str);
    }

    return std::monostate{};
}

// ==================== JavaScript Wrappers ====================

// JavaScript-friendly wrapper for Value
val valueToJS(const Value& v) {
    return std::visit([](const auto& value) -> val {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return val::null();
        } else if constexpr (std::is_same_v<T, bool>) {
            return val(value);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return val(value);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            // Copy to a proper Uint8Array (typed_memory_view becomes invalid after WASM call)
            val result = val::global("Uint8Array").new_(value.size());
            val memView = val(typed_memory_view(value.size(), value.data()));
            result.call<void>("set", memView);
            return result;
        } else {
            return val(static_cast<double>(value));
        }
    }, v);
}

// JavaScript-friendly wrapper for QueryResult
struct JSQueryResult {
    std::vector<std::string> columns;
    std::vector<std::vector<val>> rows;
    size_t rowCount;

    JSQueryResult(const QueryResult& result) : columns(result.columns), rowCount(result.rowCount()) {
        for (const auto& row : result.rows) {
            std::vector<val> jsRow;
            for (const auto& cell : row) {
                jsRow.push_back(valueToJS(cell));
            }
            rows.push_back(std::move(jsRow));
        }
    }

    val getColumns() const {
        val arr = val::array();
        for (const auto& col : columns) {
            arr.call<void>("push", val(col));
        }
        return arr;
    }

    val getRows() const {
        val arr = val::array();
        for (const auto& row : rows) {
            val jsRow = val::array();
            for (const auto& cell : row) {
                jsRow.call<void>("push", cell);
            }
            arr.call<void>("push", jsRow);
        }
        return arr;
    }

    size_t getRowCount() const { return rowCount; }
};

// JavaScript-friendly wrapper for FlatSQLDatabase
class JSFlatSQLDatabase {
public:
    JSFlatSQLDatabase(const std::string& schemaSource, const std::string& dbName = "default")
        : db_(FlatSQLDatabase::fromSchema(schemaSource, dbName)) {}

    // Register file identifier for routing
    void registerFileId(const std::string& fileId, const std::string& tableName) {
        db_.registerFileId(fileId, tableName);
    }

    // Ingest size-prefixed FlatBuffers from Uint8Array
    double ingest(const std::vector<uint8_t>& data) {
        return static_cast<double>(db_.ingest(data.data(), data.size()));
    }

    // Ingest a single FlatBuffer (without size prefix)
    double ingestOne(const std::vector<uint8_t>& data) {
        return static_cast<double>(db_.ingestOne(data.data(), data.size()));
    }

    // Load and rebuild from exported data
    void loadAndRebuild(const std::vector<uint8_t>& data) {
        db_.loadAndRebuild(data.data(), data.size());
    }

    JSQueryResult query(const std::string& sql) {
        return JSQueryResult(db_.query(sql));
    }

    val exportData() {
        std::vector<uint8_t> data = db_.exportData();
        val result = val::global("Uint8Array").new_(data.size());
        val memView = val(typed_memory_view(data.size(), data.data()));
        result.call<void>("set", memView);
        return result;
    }

    val listTables() {
        std::vector<std::string> tables = db_.listTables();
        val result = val::array();
        for (const auto& t : tables) {
            result.call<void>("push", val(t));
        }
        return result;
    }

    val getStats() {
        auto stats = db_.getStats();
        val result = val::array();
        for (const auto& s : stats) {
            val stat = val::object();
            stat.set("tableName", val(s.tableName));
            stat.set("fileId", val(s.fileId));
            stat.set("recordCount", val(static_cast<double>(s.recordCount)));
            val indexes = val::array();
            for (const auto& idx : s.indexes) {
                indexes.call<void>("push", val(idx));
            }
            stat.set("indexes", indexes);
            result.call<void>("push", stat);
        }
        return result;
    }

    // Enable built-in extractors for demo User/Post tables
    void enableDemoExtractors() {
        db_.setFieldExtractor("User", extractUserFieldGeneric);
        db_.setFieldExtractor("Post", extractPostFieldGeneric);
    }

private:
    FlatSQLDatabase db_;
};

// Standalone functions for creating test FlatBuffers
val createTestUser(int32_t id, const std::string& name, const std::string& email, int32_t age) {
    auto fb = createUserFlatBuffer(id, name, email, age);
    val result = val::global("Uint8Array").new_(fb.size());
    val memView = val(typed_memory_view(fb.size(), fb.data()));
    result.call<void>("set", memView);
    return result;
}

val createTestPost(int32_t id, int32_t userId, const std::string& title) {
    auto fb = createPostFlatBuffer(id, userId, title);
    val result = val::global("Uint8Array").new_(fb.size());
    val memView = val(typed_memory_view(fb.size(), fb.data()));
    result.call<void>("set", memView);
    return result;
}

// Helper to convert Uint8Array to hex string
std::string toHex(const std::vector<uint8_t>& data) {
    static const char* hex = "0123456789abcdef";
    std::string result;
    result.reserve(data.size() * 2);
    for (uint8_t b : data) {
        result.push_back(hex[b >> 4]);
        result.push_back(hex[b & 0xf]);
    }
    return result;
}

EMSCRIPTEN_BINDINGS(flatsql) {
    register_vector<uint8_t>("VectorUint8");

    class_<JSQueryResult>("QueryResult")
        .function("getColumns", &JSQueryResult::getColumns)
        .function("getRows", &JSQueryResult::getRows)
        .function("getRowCount", &JSQueryResult::getRowCount)
        ;

    class_<JSFlatSQLDatabase>("FlatSQLDatabase")
        .constructor<const std::string&>()
        .constructor<const std::string&, const std::string&>()
        .function("registerFileId", &JSFlatSQLDatabase::registerFileId)
        .function("ingest", &JSFlatSQLDatabase::ingest)
        .function("ingestOne", &JSFlatSQLDatabase::ingestOne)
        .function("loadAndRebuild", &JSFlatSQLDatabase::loadAndRebuild)
        .function("query", &JSFlatSQLDatabase::query)
        .function("exportData", &JSFlatSQLDatabase::exportData)
        .function("listTables", &JSFlatSQLDatabase::listTables)
        .function("getStats", &JSFlatSQLDatabase::getStats)
        .function("enableDemoExtractors", &JSFlatSQLDatabase::enableDemoExtractors)
        ;

    // Standalone helper functions
    function("createTestUser", &createTestUser);
    function("createTestPost", &createTestPost);
    function("toHex", &toHex);
}

}  // namespace flatsql

#else
// Native build - CLI tool with stdin piping support

namespace flatsql {

void printUsage(const char* prog) {
    std::cerr << "Usage: " << prog << " [options]\n"
              << "\n"
              << "Streaming FlatBuffer SQL engine - pipe size-prefixed FlatBuffers to stdin\n"
              << "\n"
              << "Options:\n"
              << "  --schema <file>     Schema file (IDL format)\n"
              << "  --map <id>=<table>  Map file identifier to table (repeatable)\n"
              << "  --query <sql>       SQL query to run after ingesting\n"
              << "  --export <file>     Export storage to file after ingesting\n"
              << "  --load <file>       Load existing storage file before stdin\n"
              << "  --stats             Print statistics after ingesting\n"
              << "  --help              Show this help\n"
              << "\n"
              << "Example:\n"
              << "  cat data.fb | " << prog << " --schema app.fbs --map USER=User --query 'SELECT * FROM User'\n"
              << "\n"
              << "Stream format: [4-byte size LE][FlatBuffer][4-byte size LE][FlatBuffer]...\n"
              << "Each FlatBuffer must have file_identifier at bytes 4-7.\n";
}

int runCLI(int argc, char* argv[]) {
    std::string schemaFile;
    std::string querySQL;
    std::string exportFile;
    std::string loadFile;
    std::vector<std::pair<std::string, std::string>> fileIdMappings;
    bool showStats = false;

    // Parse arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            printUsage(argv[0]);
            return 0;
        } else if (arg == "--schema" && i + 1 < argc) {
            schemaFile = argv[++i];
        } else if (arg == "--map" && i + 1 < argc) {
            std::string mapping = argv[++i];
            size_t eq = mapping.find('=');
            if (eq != std::string::npos) {
                fileIdMappings.emplace_back(
                    mapping.substr(0, eq),
                    mapping.substr(eq + 1)
                );
            }
        } else if (arg == "--query" && i + 1 < argc) {
            querySQL = argv[++i];
        } else if (arg == "--export" && i + 1 < argc) {
            exportFile = argv[++i];
        } else if (arg == "--load" && i + 1 < argc) {
            loadFile = argv[++i];
        } else if (arg == "--stats") {
            showStats = true;
        }
    }

    if (schemaFile.empty()) {
        std::cerr << "Error: --schema is required\n";
        printUsage(argv[0]);
        return 1;
    }

    // Read schema file
    std::ifstream schemaStream(schemaFile);
    if (!schemaStream) {
        std::cerr << "Error: Cannot open schema file: " << schemaFile << "\n";
        return 1;
    }
    std::string schemaSource((std::istreambuf_iterator<char>(schemaStream)),
                              std::istreambuf_iterator<char>());

    // Create database
    FlatSQLDatabase db = FlatSQLDatabase::fromSchema(schemaSource, "cli_db");

    // Register file ID mappings
    for (const auto& [fileId, tableName] : fileIdMappings) {
        db.registerFileId(fileId, tableName);
    }

    // Load existing data if specified
    if (!loadFile.empty()) {
        std::ifstream loadStream(loadFile, std::ios::binary);
        if (!loadStream) {
            std::cerr << "Error: Cannot open load file: " << loadFile << "\n";
            return 1;
        }
        std::vector<uint8_t> loadData((std::istreambuf_iterator<char>(loadStream)),
                                       std::istreambuf_iterator<char>());
        db.loadAndRebuild(loadData.data(), loadData.size());
        std::cerr << "Loaded " << loadData.size() << " bytes from " << loadFile << "\n";
    }

    // Read and ingest from stdin
    std::vector<uint8_t> buffer;
    constexpr size_t CHUNK_SIZE = 64 * 1024;
    char chunk[CHUNK_SIZE];

    std::cin.rdbuf()->pubsetbuf(nullptr, 0);  // Unbuffered stdin
    while (std::cin.read(chunk, CHUNK_SIZE) || std::cin.gcount() > 0) {
        size_t bytesRead = static_cast<size_t>(std::cin.gcount());
        buffer.insert(buffer.end(), chunk, chunk + bytesRead);

        // Process complete records
        size_t ingested = db.ingest(buffer.data(), buffer.size());
        if (ingested > 0) {
            std::cerr << "Ingested " << ingested << " records\n";
        }

        // Calculate bytes consumed (need to track this properly)
        // For now, after ingest we need to remove processed data
        // The storage already handles partial buffers, so we could track offset
    }

    // Final ingest of any remaining data
    if (!buffer.empty()) {
        size_t ingested = db.ingest(buffer.data(), buffer.size());
        if (ingested > 0) {
            std::cerr << "Ingested " << ingested << " final records\n";
        }
    }

    // Show stats if requested
    if (showStats) {
        auto stats = db.getStats();
        std::cerr << "\nDatabase Statistics:\n";
        for (const auto& s : stats) {
            std::cerr << "  Table: " << s.tableName;
            if (!s.fileId.empty()) {
                std::cerr << " (file_id: " << s.fileId << ")";
            }
            std::cerr << " - " << s.recordCount << " records";
            if (!s.indexes.empty()) {
                std::cerr << ", indexes: ";
                for (size_t i = 0; i < s.indexes.size(); i++) {
                    if (i > 0) std::cerr << ", ";
                    std::cerr << s.indexes[i];
                }
            }
            std::cerr << "\n";
        }
    }

    // Execute query if specified
    if (!querySQL.empty()) {
        try {
            QueryResult result = db.query(querySQL);

            // Print columns
            for (size_t i = 0; i < result.columns.size(); i++) {
                if (i > 0) std::cout << "\t";
                std::cout << result.columns[i];
            }
            std::cout << "\n";

            // Print rows
            for (const auto& row : result.rows) {
                for (size_t i = 0; i < row.size(); i++) {
                    if (i > 0) std::cout << "\t";
                    std::visit([](const auto& v) {
                        using T = std::decay_t<decltype(v)>;
                        if constexpr (std::is_same_v<T, std::monostate>) {
                            std::cout << "NULL";
                        } else if constexpr (std::is_same_v<T, std::string>) {
                            std::cout << v;
                        } else if constexpr (std::is_same_v<T, bool>) {
                            std::cout << (v ? "true" : "false");
                        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                            std::cout << "[" << v.size() << " bytes]";
                        } else {
                            std::cout << v;
                        }
                    }, row[i]);
                }
                std::cout << "\n";
            }
        } catch (const std::exception& e) {
            std::cerr << "Query error: " << e.what() << "\n";
            return 1;
        }
    }

    // Export if specified
    if (!exportFile.empty()) {
        std::vector<uint8_t> exportData = db.exportData();
        std::ofstream out(exportFile, std::ios::binary);
        if (!out) {
            std::cerr << "Error: Cannot open export file: " << exportFile << "\n";
            return 1;
        }
        out.write(reinterpret_cast<const char*>(exportData.data()), exportData.size());
        std::cerr << "Exported " << exportData.size() << " bytes to " << exportFile << "\n";
    }

    return 0;
}

}  // namespace flatsql

int main(int argc, char* argv[]) {
    return flatsql::runCLI(argc, argv);
}

#endif
