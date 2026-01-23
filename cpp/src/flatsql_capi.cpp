// C API for FlatSQL - Worker-compatible exports (no embind)
// This avoids the "table index out of bounds" issue with SQLite vtable callbacks in workers

#include "flatsql/database.h"
#include <flatbuffers/flatbuffers.h>
#include <cstring>
#include <vector>
#include <string>

#ifdef __EMSCRIPTEN__
#include <emscripten.h>

using namespace flatsql;

// ==================== C++ Helper Functions (outside extern "C") ====================

namespace {

void writeU32(std::vector<uint8_t>& v, uint32_t val) {
    v.push_back(val & 0xFF);
    v.push_back((val >> 8) & 0xFF);
    v.push_back((val >> 16) & 0xFF);
    v.push_back((val >> 24) & 0xFF);
}

void writeI32(std::vector<uint8_t>& v, int32_t val) {
    writeU32(v, static_cast<uint32_t>(val));
}

void writeU16(std::vector<uint8_t>& v, uint16_t val) {
    v.push_back(val & 0xFF);
    v.push_back((val >> 8) & 0xFF);
}

std::vector<uint8_t> createUserFlatBufferInternal(int32_t id, const std::string& name,
                                                   const std::string& email, int32_t age) {
    std::vector<uint8_t> fb;

    fb.resize(4);
    fb.push_back('U'); fb.push_back('S'); fb.push_back('E'); fb.push_back('R');
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t vtableStart = fb.size();
    writeU16(fb, 12);
    writeU16(fb, 20);
    writeU16(fb, 4);
    writeU16(fb, 8);
    writeU16(fb, 12);
    writeU16(fb, 16);
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t tableStart = fb.size();
    int32_t vtableOffset = static_cast<int32_t>(tableStart - vtableStart);
    writeI32(fb, vtableOffset);
    writeI32(fb, id);
    writeU32(fb, 0);  // name offset placeholder
    writeU32(fb, 0);  // email offset placeholder
    writeI32(fb, age);

    size_t nameFieldPos = tableStart + 8;
    size_t emailFieldPos = tableStart + 12;

    size_t actualNamePos = fb.size();
    writeU32(fb, static_cast<uint32_t>(name.size()));
    for (char c : name) fb.push_back(static_cast<uint8_t>(c));
    fb.push_back(0);
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t actualEmailPos = fb.size();
    writeU32(fb, static_cast<uint32_t>(email.size()));
    for (char c : email) fb.push_back(static_cast<uint8_t>(c));
    fb.push_back(0);

    uint32_t nameRelOffset = static_cast<uint32_t>(actualNamePos - nameFieldPos);
    fb[nameFieldPos] = nameRelOffset & 0xFF;
    fb[nameFieldPos+1] = (nameRelOffset >> 8) & 0xFF;
    fb[nameFieldPos+2] = (nameRelOffset >> 16) & 0xFF;
    fb[nameFieldPos+3] = (nameRelOffset >> 24) & 0xFF;

    uint32_t emailRelOffset = static_cast<uint32_t>(actualEmailPos - emailFieldPos);
    fb[emailFieldPos] = emailRelOffset & 0xFF;
    fb[emailFieldPos+1] = (emailRelOffset >> 8) & 0xFF;
    fb[emailFieldPos+2] = (emailRelOffset >> 16) & 0xFF;
    fb[emailFieldPos+3] = (emailRelOffset >> 24) & 0xFF;

    uint32_t rootOffset = static_cast<uint32_t>(tableStart);
    fb[0] = rootOffset & 0xFF;
    fb[1] = (rootOffset >> 8) & 0xFF;
    fb[2] = (rootOffset >> 16) & 0xFF;
    fb[3] = (rootOffset >> 24) & 0xFF;

    return fb;
}

std::vector<uint8_t> createPostFlatBufferInternal(int32_t id, int32_t userId, const std::string& title) {
    std::vector<uint8_t> fb;

    fb.resize(4);
    fb.push_back('P'); fb.push_back('O'); fb.push_back('S'); fb.push_back('T');
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t vtableStart = fb.size();
    writeU16(fb, 10);
    writeU16(fb, 16);
    writeU16(fb, 4);
    writeU16(fb, 8);
    writeU16(fb, 12);
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t tableStart = fb.size();
    int32_t vtableOffset = static_cast<int32_t>(tableStart - vtableStart);
    writeI32(fb, vtableOffset);
    writeI32(fb, id);
    writeI32(fb, userId);
    writeU32(fb, 0);

    size_t titleFieldPos = tableStart + 12;
    size_t actualTitlePos = fb.size();
    writeU32(fb, static_cast<uint32_t>(title.size()));
    for (char c : title) fb.push_back(static_cast<uint8_t>(c));
    fb.push_back(0);

    uint32_t titleRelOffset = static_cast<uint32_t>(actualTitlePos - titleFieldPos);
    fb[titleFieldPos] = titleRelOffset & 0xFF;
    fb[titleFieldPos+1] = (titleRelOffset >> 8) & 0xFF;
    fb[titleFieldPos+2] = (titleRelOffset >> 16) & 0xFF;
    fb[titleFieldPos+3] = (titleRelOffset >> 24) & 0xFF;

    fb[0] = tableStart & 0xFF;
    fb[1] = (tableStart >> 8) & 0xFF;
    fb[2] = (tableStart >> 16) & 0xFF;
    fb[3] = (tableStart >> 24) & 0xFF;

    return fb;
}

uint16_t getFieldOffset(const uint8_t* vtable, uint16_t vtableSize, int fieldIndex) {
    size_t vtableEntry = 4 + fieldIndex * 2;
    if (vtableEntry + 2 > vtableSize) return 0;
    return flatbuffers::ReadScalar<uint16_t>(vtable + vtableEntry);
}

Value extractUserFieldGeneric(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
    if (!data) return std::monostate{};

    uint32_t rootOffset = flatbuffers::ReadScalar<uint32_t>(data);
    const uint8_t* root = data + rootOffset;
    int32_t vtableOffset = flatbuffers::ReadScalar<int32_t>(root);
    const uint8_t* vtable = root - vtableOffset;
    uint16_t vtableSize = flatbuffers::ReadScalar<uint16_t>(vtable);

    if (fieldName == "id") {
        uint16_t off = getFieldOffset(vtable, vtableSize, 0);
        if (off == 0) return 0;
        return static_cast<int32_t>(flatbuffers::ReadScalar<int32_t>(root + off));
    }
    if (fieldName == "name") {
        uint16_t off = getFieldOffset(vtable, vtableSize, 1);
        if (off == 0) return std::string();
        uint32_t strOffset = flatbuffers::ReadScalar<uint32_t>(root + off);
        const uint8_t* strPtr = root + off + strOffset;
        uint32_t strLen = flatbuffers::ReadScalar<uint32_t>(strPtr);
        const char* str = reinterpret_cast<const char*>(strPtr + 4);
        return std::string(str, strLen);
    }
    if (fieldName == "email") {
        uint16_t off = getFieldOffset(vtable, vtableSize, 2);
        if (off == 0) return std::string();
        uint32_t strOffset = flatbuffers::ReadScalar<uint32_t>(root + off);
        const uint8_t* strPtr = root + off + strOffset;
        uint32_t strLen = flatbuffers::ReadScalar<uint32_t>(strPtr);
        const char* str = reinterpret_cast<const char*>(strPtr + 4);
        return std::string(str, strLen);
    }
    if (fieldName == "age") {
        uint16_t off = getFieldOffset(vtable, vtableSize, 3);
        if (off == 0) return 0;
        return static_cast<int32_t>(flatbuffers::ReadScalar<int32_t>(root + off));
    }
    return std::monostate{};
}

Value extractPostFieldGeneric(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
    if (!data) return std::monostate{};

    uint32_t rootOffset = flatbuffers::ReadScalar<uint32_t>(data);
    const uint8_t* root = data + rootOffset;
    int32_t vtableOffset = flatbuffers::ReadScalar<int32_t>(root);
    const uint8_t* vtable = root - vtableOffset;
    uint16_t vtableSize = flatbuffers::ReadScalar<uint16_t>(vtable);

    if (fieldName == "id") {
        uint16_t off = getFieldOffset(vtable, vtableSize, 0);
        if (off == 0) return 0;
        return static_cast<int32_t>(flatbuffers::ReadScalar<int32_t>(root + off));
    }
    if (fieldName == "user_id") {
        uint16_t off = getFieldOffset(vtable, vtableSize, 1);
        if (off == 0) return 0;
        return static_cast<int32_t>(flatbuffers::ReadScalar<int32_t>(root + off));
    }
    if (fieldName == "title") {
        uint16_t off = getFieldOffset(vtable, vtableSize, 2);
        if (off == 0) return std::string();
        uint32_t strOffset = flatbuffers::ReadScalar<uint32_t>(root + off);
        const uint8_t* strPtr = root + off + strOffset;
        uint32_t strLen = flatbuffers::ReadScalar<uint32_t>(strPtr);
        const char* str = reinterpret_cast<const char*>(strPtr + 4);
        return std::string(str, strLen);
    }
    return std::monostate{};
}

// Global state for result handling
QueryResult g_lastResult;
std::string g_lastError;
std::vector<uint8_t> g_exportBuffer;
std::vector<uint8_t> g_testBuffer;
std::vector<FlatSQLDatabase::TableStats> g_statsBuffer;

}  // anonymous namespace

// ==================== Exported C API Functions ====================

extern "C" {

EMSCRIPTEN_KEEPALIVE
void* flatsql_create_db(const char* schema, const char* dbName) {
    auto* db = new FlatSQLDatabase(FlatSQLDatabase::fromSchema(schema, dbName));
    return static_cast<void*>(db);
}

EMSCRIPTEN_KEEPALIVE
void flatsql_destroy_db(void* handle) {
    delete static_cast<FlatSQLDatabase*>(handle);
}

EMSCRIPTEN_KEEPALIVE
void flatsql_register_file_id(void* handle, const char* fileId, const char* tableName) {
    static_cast<FlatSQLDatabase*>(handle)->registerFileId(fileId, tableName);
}

EMSCRIPTEN_KEEPALIVE
void flatsql_enable_demo_extractors(void* handle) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    db->setFieldExtractor("User", extractUserFieldGeneric);
    db->setFieldExtractor("Post", extractPostFieldGeneric);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_ingest(void* handle, const uint8_t* data, size_t length) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->ingest(data, length));
}

EMSCRIPTEN_KEEPALIVE
double flatsql_ingest_one(void* handle, const uint8_t* data, size_t length) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->ingestOne(data, length));
}

EMSCRIPTEN_KEEPALIVE
int flatsql_query(void* handle, const char* sql) {
    try {
        g_lastResult = static_cast<FlatSQLDatabase*>(handle)->query(sql);
        g_lastError.clear();
        return 1;
    } catch (const std::exception& e) {
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
const char* flatsql_get_error() {
    return g_lastError.c_str();
}

EMSCRIPTEN_KEEPALIVE
int flatsql_result_column_count() {
    return static_cast<int>(g_lastResult.columns.size());
}

EMSCRIPTEN_KEEPALIVE
int flatsql_result_row_count() {
    return static_cast<int>(g_lastResult.rows.size());
}

EMSCRIPTEN_KEEPALIVE
const char* flatsql_result_column_name(int index) {
    if (index < 0 || index >= static_cast<int>(g_lastResult.columns.size())) return "";
    return g_lastResult.columns[index].c_str();
}

EMSCRIPTEN_KEEPALIVE
int flatsql_result_cell_type(int row, int col) {
    if (row < 0 || row >= static_cast<int>(g_lastResult.rows.size())) return 0;
    if (col < 0 || col >= static_cast<int>(g_lastResult.rows[row].size())) return 0;

    const Value& v = g_lastResult.rows[row][col];
    if (std::holds_alternative<std::monostate>(v)) return 0;
    if (std::holds_alternative<bool>(v)) return 1;
    if (std::holds_alternative<int32_t>(v)) return 2;
    if (std::holds_alternative<int64_t>(v)) return 3;
    if (std::holds_alternative<double>(v)) return 4;
    if (std::holds_alternative<std::string>(v)) return 5;
    if (std::holds_alternative<std::vector<uint8_t>>(v)) return 6;
    return 0;
}

EMSCRIPTEN_KEEPALIVE
double flatsql_result_cell_number(int row, int col) {
    if (row < 0 || row >= static_cast<int>(g_lastResult.rows.size())) return 0;
    if (col < 0 || col >= static_cast<int>(g_lastResult.rows[row].size())) return 0;

    const Value& v = g_lastResult.rows[row][col];
    if (std::holds_alternative<bool>(v)) return std::get<bool>(v) ? 1 : 0;
    if (std::holds_alternative<int32_t>(v)) return static_cast<double>(std::get<int32_t>(v));
    if (std::holds_alternative<int64_t>(v)) return static_cast<double>(std::get<int64_t>(v));
    if (std::holds_alternative<double>(v)) return std::get<double>(v);
    return 0;
}

EMSCRIPTEN_KEEPALIVE
const char* flatsql_result_cell_string(int row, int col) {
    if (row < 0 || row >= static_cast<int>(g_lastResult.rows.size())) return "";
    if (col < 0 || col >= static_cast<int>(g_lastResult.rows[row].size())) return "";

    const Value& v = g_lastResult.rows[row][col];
    if (std::holds_alternative<std::string>(v)) {
        return std::get<std::string>(v).c_str();
    }
    return "";
}

EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_result_cell_blob(int row, int col) {
    if (row < 0 || row >= static_cast<int>(g_lastResult.rows.size())) return nullptr;
    if (col < 0 || col >= static_cast<int>(g_lastResult.rows[row].size())) return nullptr;

    const Value& v = g_lastResult.rows[row][col];
    if (std::holds_alternative<std::vector<uint8_t>>(v)) {
        return std::get<std::vector<uint8_t>>(v).data();
    }
    return nullptr;
}

EMSCRIPTEN_KEEPALIVE
int flatsql_result_cell_blob_size(int row, int col) {
    if (row < 0 || row >= static_cast<int>(g_lastResult.rows.size())) return 0;
    if (col < 0 || col >= static_cast<int>(g_lastResult.rows[row].size())) return 0;

    const Value& v = g_lastResult.rows[row][col];
    if (std::holds_alternative<std::vector<uint8_t>>(v)) {
        return static_cast<int>(std::get<std::vector<uint8_t>>(v).size());
    }
    return 0;
}

EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_export_data(void* handle) {
    g_exportBuffer = static_cast<FlatSQLDatabase*>(handle)->exportData();
    return g_exportBuffer.data();
}

EMSCRIPTEN_KEEPALIVE
int flatsql_export_size() {
    return static_cast<int>(g_exportBuffer.size());
}

EMSCRIPTEN_KEEPALIVE
void flatsql_load_and_rebuild(void* handle, const uint8_t* data, size_t length) {
    static_cast<FlatSQLDatabase*>(handle)->loadAndRebuild(data, length);
}

EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_create_test_user(int32_t id, const char* name, const char* email, int32_t age) {
    g_testBuffer = createUserFlatBufferInternal(id, name, email, age);
    return g_testBuffer.data();
}

EMSCRIPTEN_KEEPALIVE
int flatsql_test_buffer_size() {
    return static_cast<int>(g_testBuffer.size());
}

EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_create_test_post(int32_t id, int32_t userId, const char* title) {
    g_testBuffer = createPostFlatBufferInternal(id, userId, title);
    return g_testBuffer.data();
}

EMSCRIPTEN_KEEPALIVE
int flatsql_get_stats_count(void* handle) {
    g_statsBuffer = static_cast<FlatSQLDatabase*>(handle)->getStats();
    return static_cast<int>(g_statsBuffer.size());
}

EMSCRIPTEN_KEEPALIVE
const char* flatsql_get_stat_table_name(int index) {
    if (index < 0 || index >= static_cast<int>(g_statsBuffer.size())) return "";
    return g_statsBuffer[index].tableName.c_str();
}

EMSCRIPTEN_KEEPALIVE
const char* flatsql_get_stat_file_id(int index) {
    if (index < 0 || index >= static_cast<int>(g_statsBuffer.size())) return "";
    return g_statsBuffer[index].fileId.c_str();
}

EMSCRIPTEN_KEEPALIVE
double flatsql_get_stat_record_count(int index) {
    if (index < 0 || index >= static_cast<int>(g_statsBuffer.size())) return 0;
    return static_cast<double>(g_statsBuffer[index].recordCount);
}

EMSCRIPTEN_KEEPALIVE
void flatsql_mark_deleted(void* handle, const char* tableName, double sequence) {
    static_cast<FlatSQLDatabase*>(handle)->markDeleted(tableName, static_cast<uint64_t>(sequence));
}

EMSCRIPTEN_KEEPALIVE
double flatsql_get_deleted_count(void* handle, const char* tableName) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->getDeletedCount(tableName));
}

EMSCRIPTEN_KEEPALIVE
void flatsql_clear_tombstones(void* handle, const char* tableName) {
    static_cast<FlatSQLDatabase*>(handle)->clearTombstones(tableName);
}

}  // extern "C"

#endif  // __EMSCRIPTEN__
