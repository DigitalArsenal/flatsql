// C API for FlatSQL - Worker-compatible exports (no embind)
// This avoids the "table index out of bounds" issue with SQLite vtable callbacks in workers

#include "flatsql/database.h"
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/encryption.h>
#include "../schemas/mpe_schema_generated.h"
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

std::vector<uint8_t> createMPEFlatBufferInternal(const std::string& entityId,
                                                 double epoch,
                                                 double meanMotion,
                                                 double eccentricity,
                                                 double inclination,
                                                 double raOfAscNode,
                                                 double argOfPericenter,
                                                 double meanAnomaly,
                                                 double bstar,
                                                 int32_t meanElementTheory) {
    flatbuffers::FlatBufferBuilder builder(256);
    auto mpeRecord = mpe::CreateMPEDirect(
        builder,
        entityId.c_str(),
        epoch,
        meanMotion,
        eccentricity,
        inclination,
        raOfAscNode,
        argOfPericenter,
        meanAnomaly,
        bstar,
        static_cast<mpe::meanElementTheory>(meanElementTheory)
    );
    builder.Finish(mpeRecord, "$MPE");
    const uint8_t* buf = builder.GetBufferPointer();
    const size_t size = builder.GetSize();
    return std::vector<uint8_t>(buf, buf + size);
}

std::vector<uint8_t> createTelemetryFlatBufferInternal(int32_t packetId,
                                                       const std::string& spacecraft,
                                                       const std::string& subsystem,
                                                       const std::string& mode,
                                                       int32_t temperatureC,
                                                       int32_t signalDb,
                                                       int32_t timestampS) {
    std::vector<uint8_t> fb;

    fb.resize(4);
    fb.push_back('T'); fb.push_back('E'); fb.push_back('L'); fb.push_back('E');
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t vtableStart = fb.size();
    writeU16(fb, 18);
    writeU16(fb, 32);
    writeU16(fb, 4);
    writeU16(fb, 8);
    writeU16(fb, 12);
    writeU16(fb, 16);
    writeU16(fb, 20);
    writeU16(fb, 24);
    writeU16(fb, 28);
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t tableStart = fb.size();
    int32_t vtableOffset = static_cast<int32_t>(tableStart - vtableStart);
    writeI32(fb, vtableOffset);
    writeI32(fb, packetId);
    writeU32(fb, 0);  // spacecraft offset placeholder
    writeU32(fb, 0);  // subsystem offset placeholder
    writeU32(fb, 0);  // mode offset placeholder
    writeI32(fb, temperatureC);
    writeI32(fb, signalDb);
    writeI32(fb, timestampS);

    size_t spacecraftFieldPos = tableStart + 8;
    size_t subsystemFieldPos = tableStart + 12;
    size_t modeFieldPos = tableStart + 16;

    size_t actualSpacecraftPos = fb.size();
    writeU32(fb, static_cast<uint32_t>(spacecraft.size()));
    for (char c : spacecraft) fb.push_back(static_cast<uint8_t>(c));
    fb.push_back(0);
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t actualSubsystemPos = fb.size();
    writeU32(fb, static_cast<uint32_t>(subsystem.size()));
    for (char c : subsystem) fb.push_back(static_cast<uint8_t>(c));
    fb.push_back(0);
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t actualModePos = fb.size();
    writeU32(fb, static_cast<uint32_t>(mode.size()));
    for (char c : mode) fb.push_back(static_cast<uint8_t>(c));
    fb.push_back(0);

    uint32_t spacecraftRelOffset = static_cast<uint32_t>(actualSpacecraftPos - spacecraftFieldPos);
    fb[spacecraftFieldPos] = spacecraftRelOffset & 0xFF;
    fb[spacecraftFieldPos + 1] = (spacecraftRelOffset >> 8) & 0xFF;
    fb[spacecraftFieldPos + 2] = (spacecraftRelOffset >> 16) & 0xFF;
    fb[spacecraftFieldPos + 3] = (spacecraftRelOffset >> 24) & 0xFF;

    uint32_t subsystemRelOffset = static_cast<uint32_t>(actualSubsystemPos - subsystemFieldPos);
    fb[subsystemFieldPos] = subsystemRelOffset & 0xFF;
    fb[subsystemFieldPos + 1] = (subsystemRelOffset >> 8) & 0xFF;
    fb[subsystemFieldPos + 2] = (subsystemRelOffset >> 16) & 0xFF;
    fb[subsystemFieldPos + 3] = (subsystemRelOffset >> 24) & 0xFF;

    uint32_t modeRelOffset = static_cast<uint32_t>(actualModePos - modeFieldPos);
    fb[modeFieldPos] = modeRelOffset & 0xFF;
    fb[modeFieldPos + 1] = (modeRelOffset >> 8) & 0xFF;
    fb[modeFieldPos + 2] = (modeRelOffset >> 16) & 0xFF;
    fb[modeFieldPos + 3] = (modeRelOffset >> 24) & 0xFF;

    uint32_t rootOffset = static_cast<uint32_t>(tableStart);
    fb[0] = rootOffset & 0xFF;
    fb[1] = (rootOffset >> 8) & 0xFF;
    fb[2] = (rootOffset >> 16) & 0xFF;
    fb[3] = (rootOffset >> 24) & 0xFF;

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

Value extractMPEFieldGeneric(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
    auto mpeRecord = mpe::GetMPE(data);
    if (!mpeRecord) return std::monostate{};

    if (fieldName == "ENTITY_ID") {
        if (mpeRecord->ENTITY_ID()) {
            return std::string(mpeRecord->ENTITY_ID()->c_str(), mpeRecord->ENTITY_ID()->size());
        }
        return std::string();
    }
    if (fieldName == "EPOCH") return mpeRecord->EPOCH();
    if (fieldName == "MEAN_MOTION") return mpeRecord->MEAN_MOTION();
    if (fieldName == "ECCENTRICITY") return mpeRecord->ECCENTRICITY();
    if (fieldName == "INCLINATION") return mpeRecord->INCLINATION();
    if (fieldName == "RA_OF_ASC_NODE") return mpeRecord->RA_OF_ASC_NODE();
    if (fieldName == "ARG_OF_PERICENTER") return mpeRecord->ARG_OF_PERICENTER();
    if (fieldName == "MEAN_ANOMALY") return mpeRecord->MEAN_ANOMALY();
    if (fieldName == "BSTAR") return mpeRecord->BSTAR();
    if (fieldName == "MEAN_ELEMENT_THEORY") {
        return static_cast<int32_t>(mpeRecord->MEAN_ELEMENT_THEORY());
    }

    return std::monostate{};
}

Value extractTelemetryFieldGeneric(const uint8_t* data, size_t length, const std::string& fieldName) {
    (void)length;
    if (!data) return std::monostate{};

    uint32_t rootOffset = flatbuffers::ReadScalar<uint32_t>(data);
    const uint8_t* root = data + rootOffset;
    int32_t vtableOffset = flatbuffers::ReadScalar<int32_t>(root);
    const uint8_t* vtable = root - vtableOffset;
    uint16_t vtableSize = flatbuffers::ReadScalar<uint16_t>(vtable);

    auto readString = [&](int fieldIndex) -> Value {
        uint16_t off = getFieldOffset(vtable, vtableSize, fieldIndex);
        if (off == 0) return std::string();
        uint32_t strOffset = flatbuffers::ReadScalar<uint32_t>(root + off);
        const uint8_t* strPtr = root + off + strOffset;
        uint32_t strLen = flatbuffers::ReadScalar<uint32_t>(strPtr);
        const char* str = reinterpret_cast<const char*>(strPtr + 4);
        return std::string(str, strLen);
    };

    auto readInt = [&](int fieldIndex) -> Value {
        uint16_t off = getFieldOffset(vtable, vtableSize, fieldIndex);
        if (off == 0) return static_cast<int32_t>(0);
        return static_cast<int32_t>(flatbuffers::ReadScalar<int32_t>(root + off));
    };

    if (fieldName == "packet_id") return readInt(0);
    if (fieldName == "spacecraft") return readString(1);
    if (fieldName == "subsystem") return readString(2);
    if (fieldName == "mode") return readString(3);
    if (fieldName == "temperature_c") return readInt(4);
    if (fieldName == "signal_db") return readInt(5);
    if (fieldName == "timestamp_s") return readInt(6);

    return std::monostate{};
}

enum ParamTag : uint8_t {
    PARAM_NULL = 0,
    PARAM_BOOL = 1,
    PARAM_INT64 = 2,
    PARAM_FLOAT64 = 3,
    PARAM_STRING = 4,
    PARAM_BYTES = 5,
};

void requireBytes(size_t offset, size_t need, size_t length) {
    if (offset > length || need > length - offset) {
        throw std::runtime_error("Malformed query parameter payload");
    }
}

std::vector<Value> decodeParams(const uint8_t* data, size_t length, int paramCount) {
    if (paramCount < 0) {
        throw std::runtime_error("Invalid parameter count");
    }
    if (paramCount == 0) {
        return {};
    }
    if (!data) {
        throw std::runtime_error("Missing query parameter payload");
    }

    std::vector<Value> params;
    params.reserve(static_cast<size_t>(paramCount));
    size_t offset = 0;

    for (int index = 0; index < paramCount; index++) {
        requireBytes(offset, 5, length);
        const uint8_t tag = data[offset++];
        const uint32_t size = flatbuffers::ReadScalar<uint32_t>(data + offset);
        offset += 4;
        requireBytes(offset, size, length);

        switch (tag) {
            case PARAM_NULL:
                if (size != 0) {
                    throw std::runtime_error("NULL parameter payload must be empty");
                }
                params.emplace_back(std::monostate{});
                break;
            case PARAM_BOOL:
                if (size != 1) {
                    throw std::runtime_error("Boolean parameter payload must be 1 byte");
                }
                params.emplace_back(data[offset] != 0);
                break;
            case PARAM_INT64:
                if (size != sizeof(int64_t)) {
                    throw std::runtime_error("Integer parameter payload must be 8 bytes");
                }
                params.emplace_back(flatbuffers::ReadScalar<int64_t>(data + offset));
                break;
            case PARAM_FLOAT64:
                if (size != sizeof(double)) {
                    throw std::runtime_error("Float parameter payload must be 8 bytes");
                }
                params.emplace_back(flatbuffers::ReadScalar<double>(data + offset));
                break;
            case PARAM_STRING:
                params.emplace_back(std::string(reinterpret_cast<const char*>(data + offset), size));
                break;
            case PARAM_BYTES:
                params.emplace_back(std::vector<uint8_t>(data + offset, data + offset + size));
                break;
            default:
                throw std::runtime_error("Unsupported query parameter tag");
        }

        offset += size;
    }

    if (offset != length) {
        throw std::runtime_error("Unexpected trailing bytes in query parameter payload");
    }

    return params;
}

struct QueryRequest {
    std::string sql;
    std::vector<Value> params;
};

std::vector<QueryRequest> decodeQueryRequests(const uint8_t* data, size_t length, int requestCount) {
    if (requestCount < 0) {
        throw std::runtime_error("Invalid batch query count");
    }
    if (requestCount == 0) {
        return {};
    }
    if (!data) {
        throw std::runtime_error("Missing batch query payload");
    }

    std::vector<QueryRequest> requests;
    requests.reserve(static_cast<size_t>(requestCount));
    size_t offset = 0;

    for (int index = 0; index < requestCount; index++) {
        requireBytes(offset, 12, length);
        const uint32_t sqlLength = flatbuffers::ReadScalar<uint32_t>(data + offset);
        offset += 4;
        const uint32_t paramCount = flatbuffers::ReadScalar<uint32_t>(data + offset);
        offset += 4;
        const uint32_t paramLength = flatbuffers::ReadScalar<uint32_t>(data + offset);
        offset += 4;

        requireBytes(offset, sqlLength, length);
        std::string sql(reinterpret_cast<const char*>(data + offset), sqlLength);
        offset += sqlLength;

        requireBytes(offset, paramLength, length);
        const uint8_t* paramData = paramLength > 0 ? data + offset : nullptr;
        auto params = decodeParams(paramData, paramLength, static_cast<int>(paramCount));
        offset += paramLength;

        requests.push_back({std::move(sql), std::move(params)});
    }

    if (offset != length) {
        throw std::runtime_error("Unexpected trailing bytes in batch query payload");
    }

    return requests;
}

// Global state for result handling
QueryResult g_lastResult;
std::vector<QueryResult> g_batchResults;
int g_selectedBatchResult = -1;
std::string g_lastError;
std::vector<uint8_t> g_exportBuffer;
std::vector<uint8_t> g_testBuffer;
std::vector<FlatSQLDatabase::TableStats> g_statsBuffer;
std::vector<std::string> g_sourcesBuffer;

QueryResult& currentResult() {
    if (g_selectedBatchResult >= 0 && g_selectedBatchResult < static_cast<int>(g_batchResults.size())) {
        return g_batchResults[static_cast<size_t>(g_selectedBatchResult)];
    }
    return g_lastResult;
}

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
    try {
        db->setFieldExtractor("User", extractUserFieldGeneric);
    } catch (const std::exception&) {
    }
    try {
        db->setFieldExtractor("Post", extractPostFieldGeneric);
    } catch (const std::exception&) {
    }
    try {
        db->setFieldExtractor("MPE", extractMPEFieldGeneric);
    } catch (const std::exception&) {
    }
    try {
        db->setFieldExtractor("Telemetry", extractTelemetryFieldGeneric);
    } catch (const std::exception&) {
    }
}

EMSCRIPTEN_KEEPALIVE
double flatsql_ingest(void* handle, const uint8_t* data, size_t length) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->ingest(data, length));
}

EMSCRIPTEN_KEEPALIVE
double flatsql_ingest_one(void* handle, const uint8_t* data, size_t length) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->ingestOne(data, length));
}

// Source-aware ingestion
EMSCRIPTEN_KEEPALIVE
void flatsql_register_source(void* handle, const char* sourceName) {
    try {
        static_cast<FlatSQLDatabase*>(handle)->registerSource(sourceName);
    } catch (const std::exception& e) {
        g_lastError = e.what();
    }
}

EMSCRIPTEN_KEEPALIVE
void flatsql_create_unified_views(void* handle) {
    static_cast<FlatSQLDatabase*>(handle)->createUnifiedViews();
}

EMSCRIPTEN_KEEPALIVE
double flatsql_ingest_with_source(void* handle, const uint8_t* data, size_t length, const char* source) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->ingestWithSource(data, length, source));
}

EMSCRIPTEN_KEEPALIVE
double flatsql_ingest_one_with_source(void* handle, const uint8_t* data, size_t length, const char* source) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->ingestOneWithSource(data, length, source));
}

EMSCRIPTEN_KEEPALIVE
int flatsql_query(void* handle, const char* sql) {
    try {
        g_batchResults.clear();
        g_selectedBatchResult = -1;
        g_lastResult = static_cast<FlatSQLDatabase*>(handle)->query(sql);
        g_lastError.clear();
        return 1;
    } catch (const std::exception& e) {
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
int flatsql_query_params(void* handle, const char* sql, const uint8_t* paramData, size_t paramLength, int paramCount) {
    try {
        g_batchResults.clear();
        g_selectedBatchResult = -1;
        g_lastResult = static_cast<FlatSQLDatabase*>(handle)->query(
            sql,
            decodeParams(paramData, paramLength, paramCount)
        );
        g_lastError.clear();
        return 1;
    } catch (const std::exception& e) {
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
int flatsql_query_many(void* handle, const uint8_t* requestData, size_t requestLength, int requestCount) {
    try {
        g_batchResults.clear();
        auto requests = decodeQueryRequests(requestData, requestLength, requestCount);
        g_batchResults.reserve(requests.size());
        for (const auto& request : requests) {
            g_batchResults.push_back(
                static_cast<FlatSQLDatabase*>(handle)->query(request.sql, request.params)
            );
        }
        g_selectedBatchResult = g_batchResults.empty() ? -1 : 0;
        g_lastError.clear();
        return 1;
    } catch (const std::exception& e) {
        g_batchResults.clear();
        g_selectedBatchResult = -1;
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
int flatsql_batch_result_count() {
    return static_cast<int>(g_batchResults.size());
}

EMSCRIPTEN_KEEPALIVE
int flatsql_select_batch_result(int index) {
    if (index < 0 || index >= static_cast<int>(g_batchResults.size())) {
        return 0;
    }
    g_selectedBatchResult = index;
    return 1;
}

EMSCRIPTEN_KEEPALIVE
const char* flatsql_get_error() {
    return g_lastError.c_str();
}

EMSCRIPTEN_KEEPALIVE
int flatsql_result_column_count() {
    return static_cast<int>(currentResult().columns.size());
}

EMSCRIPTEN_KEEPALIVE
int flatsql_result_row_count() {
    return static_cast<int>(currentResult().rows.size());
}

EMSCRIPTEN_KEEPALIVE
const char* flatsql_result_column_name(int index) {
    QueryResult& result = currentResult();
    if (index < 0 || index >= static_cast<int>(result.columns.size())) return "";
    return result.columns[static_cast<size_t>(index)].c_str();
}

EMSCRIPTEN_KEEPALIVE
int flatsql_result_cell_type(int row, int col) {
    QueryResult& result = currentResult();
    if (row < 0 || row >= static_cast<int>(result.rows.size())) return 0;
    if (col < 0 || col >= static_cast<int>(result.rows[static_cast<size_t>(row)].size())) return 0;

    const Value& v = result.rows[static_cast<size_t>(row)][static_cast<size_t>(col)];
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
    QueryResult& result = currentResult();
    if (row < 0 || row >= static_cast<int>(result.rows.size())) return 0;
    if (col < 0 || col >= static_cast<int>(result.rows[static_cast<size_t>(row)].size())) return 0;

    const Value& v = result.rows[static_cast<size_t>(row)][static_cast<size_t>(col)];
    if (std::holds_alternative<bool>(v)) return std::get<bool>(v) ? 1 : 0;
    if (std::holds_alternative<int32_t>(v)) return static_cast<double>(std::get<int32_t>(v));
    if (std::holds_alternative<int64_t>(v)) return static_cast<double>(std::get<int64_t>(v));
    if (std::holds_alternative<double>(v)) return std::get<double>(v);
    return 0;
}

EMSCRIPTEN_KEEPALIVE
const char* flatsql_result_cell_string(int row, int col) {
    QueryResult& result = currentResult();
    if (row < 0 || row >= static_cast<int>(result.rows.size())) return "";
    if (col < 0 || col >= static_cast<int>(result.rows[static_cast<size_t>(row)].size())) return "";

    const Value& v = result.rows[static_cast<size_t>(row)][static_cast<size_t>(col)];
    if (std::holds_alternative<std::string>(v)) {
        return std::get<std::string>(v).c_str();
    }
    return "";
}

EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_result_cell_blob(int row, int col) {
    QueryResult& result = currentResult();
    if (row < 0 || row >= static_cast<int>(result.rows.size())) return nullptr;
    if (col < 0 || col >= static_cast<int>(result.rows[static_cast<size_t>(row)].size())) return nullptr;

    const Value& v = result.rows[static_cast<size_t>(row)][static_cast<size_t>(col)];
    if (std::holds_alternative<std::vector<uint8_t>>(v)) {
        return std::get<std::vector<uint8_t>>(v).data();
    }
    return nullptr;
}

EMSCRIPTEN_KEEPALIVE
int flatsql_result_cell_blob_size(int row, int col) {
    QueryResult& result = currentResult();
    if (row < 0 || row >= static_cast<int>(result.rows.size())) return 0;
    if (col < 0 || col >= static_cast<int>(result.rows[static_cast<size_t>(row)].size())) return 0;

    const Value& v = result.rows[static_cast<size_t>(row)][static_cast<size_t>(col)];
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
const uint8_t* flatsql_create_test_mpe(const char* entityId,
                                       double epoch,
                                       double meanMotion,
                                       double eccentricity,
                                       double inclination,
                                       double raOfAscNode,
                                       double argOfPericenter,
                                       double meanAnomaly,
                                       double bstar,
                                       int32_t meanElementTheory) {
    g_testBuffer = createMPEFlatBufferInternal(entityId,
                                               epoch,
                                               meanMotion,
                                               eccentricity,
                                               inclination,
                                               raOfAscNode,
                                               argOfPericenter,
                                               meanAnomaly,
                                               bstar,
                                               meanElementTheory);
    return g_testBuffer.data();
}

EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_create_test_telemetry(int32_t packetId,
                                             const char* spacecraft,
                                             const char* subsystem,
                                             const char* mode,
                                             int32_t temperatureC,
                                             int32_t signalDb,
                                             int32_t timestampS) {
    g_testBuffer = createTelemetryFlatBufferInternal(packetId,
                                                     spacecraft,
                                                     subsystem,
                                                     mode,
                                                     temperatureC,
                                                     signalDb,
                                                     timestampS);
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
void flatsql_reset_ingest_profile(void* handle) {
    static_cast<FlatSQLDatabase*>(handle)->resetIngestProfile();
}

EMSCRIPTEN_KEEPALIVE
double flatsql_get_ingest_profile_record_count(void* handle) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->getIngestProfile().recordCount);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_get_ingest_profile_byte_count(void* handle) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->getIngestProfile().byteCount);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_get_ingest_profile_decode_nanos(void* handle) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->getIngestProfile().decodeNanos);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_get_ingest_profile_append_nanos(void* handle) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->getIngestProfile().appendNanos);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_get_ingest_profile_index_nanos(void* handle) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->getIngestProfile().indexNanos);
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

// Source listing
EMSCRIPTEN_KEEPALIVE
int flatsql_get_sources_count(void* handle) {
    g_sourcesBuffer = static_cast<FlatSQLDatabase*>(handle)->listSources();
    return static_cast<int>(g_sourcesBuffer.size());
}

EMSCRIPTEN_KEEPALIVE
const char* flatsql_get_source_name(int index) {
    if (index < 0 || index >= static_cast<int>(g_sourcesBuffer.size())) return "";
    return g_sourcesBuffer[index].c_str();
}

// ==================== Raw FlatBuffer Access API ====================
// These functions provide direct memory access to FlatBuffer data

// Global state for raw FlatBuffer access
static const uint8_t* g_rawFlatBuffer = nullptr;
static uint32_t g_rawFlatBufferSize = 0;
static uint64_t g_rawFlatBufferSequence = 0;

// Get raw FlatBuffer pointer by table name and indexed column value
// Returns pointer to FlatBuffer in WASM memory, or 0 if not found
// Call flatsql_get_raw_flatbuffer_size() after to get the size
EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_get_flatbuffer_by_id(void* handle, const char* tableName, int32_t id) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    g_rawFlatBuffer = db->findRawByIndex(tableName, "id", static_cast<int64_t>(id),
                                          &g_rawFlatBufferSize, &g_rawFlatBufferSequence);
    return g_rawFlatBuffer;
}

// Get raw FlatBuffer pointer by table name and email (string key)
EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_get_flatbuffer_by_email(void* handle, const char* tableName, const char* email) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    g_rawFlatBuffer = db->findRawByIndex(tableName, "email", std::string(email),
                                          &g_rawFlatBufferSize, &g_rawFlatBufferSequence);
    return g_rawFlatBuffer;
}

// Get size of the last accessed raw FlatBuffer
EMSCRIPTEN_KEEPALIVE
int flatsql_get_raw_flatbuffer_size() {
    return static_cast<int>(g_rawFlatBufferSize);
}

// Get sequence (rowid) of the last accessed raw FlatBuffer
EMSCRIPTEN_KEEPALIVE
double flatsql_get_raw_flatbuffer_sequence() {
    return static_cast<double>(g_rawFlatBufferSequence);
}

// Get the underlying storage buffer pointer (for advanced use)
// This returns the base address of all FlatBuffer storage
EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_get_storage_buffer(void* handle) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    return db->getStorage().getDataBuffer();
}

// Get the current storage buffer size
EMSCRIPTEN_KEEPALIVE
double flatsql_get_storage_size(void* handle) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    return static_cast<double>(db->getStorage().getDataSize());
}

// ==================== Encryption API ====================

EMSCRIPTEN_KEEPALIVE
int flatsql_set_encryption_key(void* handle, const uint8_t* key, int keySize) {
    try {
        auto* db = static_cast<FlatSQLDatabase*>(handle);
        db->setEncryptionKey(key, static_cast<size_t>(keySize));
        return 1;
    } catch (const std::exception& e) {
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
int flatsql_is_encrypted(void* handle) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    return db->isEncrypted() ? 1 : 0;
}

EMSCRIPTEN_KEEPALIVE
int flatsql_encrypt_buffer(void* handle, uint8_t* buffer, int bufferSize,
                            const uint8_t* schema, int schemaSize) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    auto* ctx = db->getEncryptionContext();
    if (!ctx) {
        g_lastError = "No encryption key set";
        return 0;
    }
    auto result = flatbuffers::EncryptBuffer(buffer, static_cast<size_t>(bufferSize),
                                              schema, static_cast<size_t>(schemaSize), *ctx);
    if (!result.ok()) {
        g_lastError = result.message;
        return 0;
    }
    return 1;
}

EMSCRIPTEN_KEEPALIVE
int flatsql_decrypt_buffer(void* handle, uint8_t* buffer, int bufferSize,
                            const uint8_t* schema, int schemaSize) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    auto* ctx = db->getEncryptionContext();
    if (!ctx) {
        g_lastError = "No encryption key set";
        return 0;
    }
    auto result = flatbuffers::DecryptBuffer(buffer, static_cast<size_t>(bufferSize),
                                              schema, static_cast<size_t>(schemaSize), *ctx);
    if (!result.ok()) {
        g_lastError = result.message;
        return 0;
    }
    return 1;
}

// ==================== HMAC Authentication API ====================

EMSCRIPTEN_KEEPALIVE
int flatsql_set_hmac_verification(void* handle, int enabled) {
    try {
        auto* db = static_cast<FlatSQLDatabase*>(handle);
        db->setHMACVerification(enabled != 0);
        return 1;
    } catch (const std::exception& e) {
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
int flatsql_is_hmac_enabled(void* handle) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    return db->isHMACVerificationEnabled() ? 1 : 0;
}

EMSCRIPTEN_KEEPALIVE
int flatsql_compute_hmac(void* handle, const uint8_t* buffer, int bufferSize, uint8_t* outMAC) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    return db->computeHMAC(buffer, static_cast<size_t>(bufferSize), outMAC) ? 1 : 0;
}

EMSCRIPTEN_KEEPALIVE
int flatsql_verify_hmac(void* handle, const uint8_t* buffer, int bufferSize, const uint8_t* mac) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    return db->verifyHMAC(buffer, static_cast<size_t>(bufferSize), mac) ? 1 : 0;
}

}  // extern "C"

#endif  // __EMSCRIPTEN__
