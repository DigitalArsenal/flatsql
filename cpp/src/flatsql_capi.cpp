// C API for FlatSQL - Worker-compatible exports (no embind)
// This avoids the "table index out of bounds" issue with SQLite vtable callbacks in workers

#include "flatsql/database.h"
#include "flatsql/query_cache.h"
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/encryption.h>
#include "../schemas/mpe_schema_generated.h"
#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
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

std::vector<uint8_t> createPublishEventFlatBufferInternal(const std::string& fileId,
                                                          const std::string& recordId,
                                                          int32_t eventIndex,
                                                          int32_t payloadSize) {
    const int32_t normalizedPayloadSize = std::max(payloadSize, 0);
    std::vector<uint8_t> fb;

    fb.resize(4);
    fb.push_back('P'); fb.push_back('U'); fb.push_back('B'); fb.push_back('L');
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t vtableStart = fb.size();
    writeU16(fb, 14);
    writeU16(fb, 24);
    writeU16(fb, 4);
    writeU16(fb, 8);
    writeU16(fb, 12);
    writeU16(fb, 16);
    writeU16(fb, 20);
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t tableStart = fb.size();
    int32_t vtableOffset = static_cast<int32_t>(tableStart - vtableStart);
    writeI32(fb, vtableOffset);
    writeU32(fb, 0);  // FILE_ID offset placeholder
    writeU32(fb, 0);  // RECORD_ID offset placeholder
    writeI32(fb, eventIndex);
    writeI32(fb, normalizedPayloadSize);
    writeU32(fb, 0);  // PAYLOAD offset placeholder

    size_t fileIdFieldPos = tableStart + 4;
    size_t recordIdFieldPos = tableStart + 8;
    size_t payloadFieldPos = tableStart + 20;

    size_t actualFileIdPos = fb.size();
    writeU32(fb, static_cast<uint32_t>(fileId.size()));
    for (char c : fileId) fb.push_back(static_cast<uint8_t>(c));
    fb.push_back(0);
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t actualRecordIdPos = fb.size();
    writeU32(fb, static_cast<uint32_t>(recordId.size()));
    for (char c : recordId) fb.push_back(static_cast<uint8_t>(c));
    fb.push_back(0);
    while (fb.size() % 4 != 0) fb.push_back(0);

    size_t actualPayloadPos = fb.size();
    writeU32(fb, static_cast<uint32_t>(normalizedPayloadSize));
    for (int32_t index = 0; index < normalizedPayloadSize; index++) {
        fb.push_back(static_cast<uint8_t>((eventIndex * 31 + index * 17) % 256));
    }
    while (fb.size() % 4 != 0) fb.push_back(0);

    uint32_t fileIdRelOffset = static_cast<uint32_t>(actualFileIdPos - fileIdFieldPos);
    fb[fileIdFieldPos] = fileIdRelOffset & 0xFF;
    fb[fileIdFieldPos + 1] = (fileIdRelOffset >> 8) & 0xFF;
    fb[fileIdFieldPos + 2] = (fileIdRelOffset >> 16) & 0xFF;
    fb[fileIdFieldPos + 3] = (fileIdRelOffset >> 24) & 0xFF;

    uint32_t recordIdRelOffset = static_cast<uint32_t>(actualRecordIdPos - recordIdFieldPos);
    fb[recordIdFieldPos] = recordIdRelOffset & 0xFF;
    fb[recordIdFieldPos + 1] = (recordIdRelOffset >> 8) & 0xFF;
    fb[recordIdFieldPos + 2] = (recordIdRelOffset >> 16) & 0xFF;
    fb[recordIdFieldPos + 3] = (recordIdRelOffset >> 24) & 0xFF;

    uint32_t payloadRelOffset = static_cast<uint32_t>(actualPayloadPos - payloadFieldPos);
    fb[payloadFieldPos] = payloadRelOffset & 0xFF;
    fb[payloadFieldPos + 1] = (payloadRelOffset >> 8) & 0xFF;
    fb[payloadFieldPos + 2] = (payloadRelOffset >> 16) & 0xFF;
    fb[payloadFieldPos + 3] = (payloadRelOffset >> 24) & 0xFF;

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

Value extractPublishEventFieldGeneric(const uint8_t* data, size_t length, const std::string& fieldName) {
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

    if (fieldName == "FILE_ID") return readString(0);
    if (fieldName == "RECORD_ID") return readString(1);
    if (fieldName == "EVENT_INDEX") return readInt(2);
    if (fieldName == "PAYLOAD_SIZE") return readInt(3);

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

bool hasBytes(size_t offset, size_t need, size_t length) {
    return offset <= length && need <= length - offset;
}

// Exception-free TLV parameter decoder. Returns false and sets *err on
// malformed payloads instead of throwing, so the no-exceptions WASI build
// never traps on user-supplied parameter data.
bool decodeParamsNoThrow(const uint8_t* data, size_t length, int paramCount,
                         std::vector<Value>* out, std::string* err) noexcept {
    try {
        if (out) out->clear();
        if (err) err->clear();

        auto fail = [&](const char* message) {
            if (err) *err = message;
            if (out) out->clear();
            return false;
        };

        if (!out) {
            return fail("Missing parameter output vector");
        }
        if (paramCount < 0) {
            return fail("Invalid parameter count");
        }
        if (paramCount == 0) {
            return true;
        }
        if (!data) {
            return fail("Missing query parameter payload");
        }
        if (static_cast<size_t>(paramCount) > length / 5) {
            return fail("Query parameter count exceeds payload capacity");
        }

        std::vector<Value>& params = *out;
        params.reserve(static_cast<size_t>(paramCount));
        size_t offset = 0;

        for (int index = 0; index < paramCount; index++) {
            if (!hasBytes(offset, 5, length)) {
                return fail("Malformed query parameter payload");
            }
            const uint8_t tag = data[offset++];
            const uint32_t size = flatbuffers::ReadScalar<uint32_t>(data + offset);
            offset += 4;
            if (!hasBytes(offset, size, length)) {
                return fail("Malformed query parameter payload");
            }

            switch (tag) {
                case PARAM_NULL:
                    if (size != 0) {
                        return fail("NULL parameter payload must be empty");
                    }
                    params.emplace_back(std::monostate{});
                    break;
                case PARAM_BOOL:
                    if (size != 1) {
                        return fail("Boolean parameter payload must be 1 byte");
                    }
                    params.emplace_back(data[offset] != 0);
                    break;
                case PARAM_INT64:
                    if (size != sizeof(int64_t)) {
                        return fail("Integer parameter payload must be 8 bytes");
                    }
                    params.emplace_back(flatbuffers::ReadScalar<int64_t>(data + offset));
                    break;
                case PARAM_FLOAT64:
                    if (size != sizeof(double)) {
                        return fail("Float parameter payload must be 8 bytes");
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
                    return fail("Unsupported query parameter tag");
            }

            offset += size;
        }

        if (offset != length) {
            return fail("Unexpected trailing bytes in query parameter payload");
        }

        return true;
    } catch (...) {
        if (err) {
            try { *err = "Malformed query parameter payload"; } catch (...) {}
        }
        if (out) out->clear();
        return false;
    }
}

std::vector<std::string> decodeStringList(const char* data) {
    std::vector<std::string> values;
    if (!data || data[0] == '\0') {
        return values;
    }

    std::string input(data);
    size_t start = 0;
    while (start <= input.size()) {
        const size_t end = input.find('\n', start);
        const size_t length = (end == std::string::npos) ? input.size() - start : end - start;
        if (length > 0) {
            values.emplace_back(input.substr(start, length));
        }
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }

    return values;
}

struct QueryRequest {
    std::string sql;
    std::vector<Value> params;
};

// Exception-free batch request decoder (see decodeParamsNoThrow).
bool decodeQueryRequestsNoThrow(const uint8_t* data, size_t length, int requestCount,
                                std::vector<QueryRequest>* out, std::string* err) noexcept {
    try {
        if (out) out->clear();
        if (err) err->clear();

        auto fail = [&](const char* message) {
            if (err) *err = message;
            if (out) out->clear();
            return false;
        };

        if (!out) {
            return fail("Missing batch query output vector");
        }
        if (requestCount < 0) {
            return fail("Invalid batch query count");
        }
        if (requestCount == 0) {
            return true;
        }
        if (!data) {
            return fail("Missing batch query payload");
        }
        if (static_cast<size_t>(requestCount) > length / 12) {
            return fail("Batch query count exceeds payload capacity");
        }

        std::vector<QueryRequest>& requests = *out;
        requests.reserve(static_cast<size_t>(requestCount));
        size_t offset = 0;

        for (int index = 0; index < requestCount; index++) {
            if (!hasBytes(offset, 12, length)) {
                return fail("Malformed query parameter payload");
            }
            const uint32_t sqlLength = flatbuffers::ReadScalar<uint32_t>(data + offset);
            offset += 4;
            const uint32_t paramCount = flatbuffers::ReadScalar<uint32_t>(data + offset);
            offset += 4;
            const uint32_t paramLength = flatbuffers::ReadScalar<uint32_t>(data + offset);
            offset += 4;

            if (!hasBytes(offset, sqlLength, length)) {
                return fail("Malformed query parameter payload");
            }
            std::string sql(reinterpret_cast<const char*>(data + offset), sqlLength);
            offset += sqlLength;

            if (!hasBytes(offset, paramLength, length)) {
                return fail("Malformed query parameter payload");
            }
            const uint8_t* paramData = paramLength > 0 ? data + offset : nullptr;
            std::vector<Value> params;
            if (!decodeParamsNoThrow(paramData, paramLength, static_cast<int>(paramCount), &params, err)) {
                if (out) out->clear();
                return false;
            }
            offset += paramLength;

            requests.push_back({std::move(sql), std::move(params)});
        }

        if (offset != length) {
            return fail("Unexpected trailing bytes in batch query payload");
        }

        return true;
    } catch (...) {
        if (err) {
            try { *err = "Malformed batch query payload"; } catch (...) {}
        }
        if (out) out->clear();
        return false;
    }
}

// Global state for result handling
QueryResult g_lastResult;
std::vector<QueryResult> g_batchResults;
int g_selectedBatchResult = -1;
std::string g_lastError;
std::string g_cacheKeyBuffer;
std::vector<uint8_t> g_exportBuffer;
std::shared_ptr<const std::vector<uint8_t>> g_responseArtifactStream;
size_t g_responseArtifactRowCount = 0;
size_t g_responseArtifactColumnCount = 0;
int g_responseArtifactCacheHit = 0;
std::vector<uint8_t> g_testBuffer;
std::vector<FlatSQLDatabase::TableStats> g_statsBuffer;
std::vector<std::string> g_sourcesBuffer;

QueryResult& currentResult() {
    if (g_selectedBatchResult >= 0 && g_selectedBatchResult < static_cast<int>(g_batchResults.size())) {
        return g_batchResults[static_cast<size_t>(g_selectedBatchResult)];
    }
    return g_lastResult;
}

std::string paramCountMismatchMessage(int expected, size_t provided) {
    // Must match SQLiteEngine::execute()'s throw message exactly.
    return "SQL statement expects " + std::to_string(expected) +
           " parameters but received " + std::to_string(provided);
}

}  // anonymous namespace

// ==================== Exported C API Functions ====================

extern "C" {

EMSCRIPTEN_KEEPALIVE
void* flatsql_create_db(const char* schema, const char* dbName) {
    try {
        DatabaseSchema parsedSchema;
        std::string parseError;
        if (!SchemaParser::tryParse(schema ? schema : "",
                                    &parsedSchema,
                                    &parseError,
                                    dbName ? dbName : "default")) {
            g_lastError = parseError.empty() ? "Failed to parse schema" : parseError;
            return nullptr;
        }

        auto* db = new FlatSQLDatabase(parsedSchema);
        g_lastError.clear();
        return static_cast<void*>(db);
    } catch (const std::exception& e) {
        g_lastError = e.what();
        return nullptr;
    }
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
    if (db->getTableDef("User")) {
        db->setFieldExtractor("User", extractUserFieldGeneric);
    }
    if (db->getTableDef("Post")) {
        db->setFieldExtractor("Post", extractPostFieldGeneric);
    }
    if (db->getTableDef("MPE")) {
        db->setFieldExtractor("MPE", extractMPEFieldGeneric);
    }
    if (db->getTableDef("Telemetry")) {
        db->setFieldExtractor("Telemetry", extractTelemetryFieldGeneric);
    }
    if (db->getTableDef("PublishEventRecord")) {
        db->setFieldExtractor("PublishEventRecord", extractPublishEventFieldGeneric);
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
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    const std::string name = sourceName ? sourceName : "";

    // Pre-check duplicates without exceptions so the no-eh build never traps.
    // Message must match FlatSQLDatabase::registerSource's throw text.
    if (db->hasSource(name)) {
        g_lastError = "Source already registered: " + name;
        return;
    }

    try {
        db->registerSource(name);
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
    g_batchResults.clear();
    g_selectedBatchResult = -1;

    auto* db = static_cast<FlatSQLDatabase*>(handle);
    const std::string sqlStr = sql ? sql : "";

    // Pre-validate without exceptions so invalid SQL never throws (and never
    // traps on the no-eh build); errors land in the g_lastError latch.
    std::string validationError;
    int expectedParams = 0;
    if (!db->validateSQL(sqlStr, &expectedParams, &validationError)) {
        g_lastError = validationError;
        return 0;
    }
    if (expectedParams != 0) {
        // The no-param entry point must not run SQL with placeholders:
        // unbound params execute as NULL, silently matching nothing.
        g_lastError = paramCountMismatchMessage(expectedParams, 0);
        return 0;
    }

    try {
        g_lastResult = db->query(sqlStr);
        g_lastError.clear();
        return 1;
    } catch (const std::exception& e) {
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
int flatsql_query_params(void* handle, const char* sql, const uint8_t* paramData, size_t paramLength, int paramCount) {
    g_batchResults.clear();
    g_selectedBatchResult = -1;

    auto* db = static_cast<FlatSQLDatabase*>(handle);
    const std::string sqlStr = sql ? sql : "";

    std::vector<Value> params;
    std::string errorMessage;
    if (!decodeParamsNoThrow(paramData, paramLength, paramCount, &params, &errorMessage)) {
        g_lastError = errorMessage;
        return 0;
    }

    int expectedParams = 0;
    if (!db->validateSQL(sqlStr, &expectedParams, &errorMessage)) {
        g_lastError = errorMessage;
        return 0;
    }
    if (expectedParams != static_cast<int>(params.size())) {
        g_lastError = paramCountMismatchMessage(expectedParams, params.size());
        return 0;
    }

    try {
        g_lastResult = db->query(sqlStr, params);
        g_lastError.clear();
        return 1;
    } catch (const std::exception& e) {
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
int flatsql_query_many(void* handle, const uint8_t* requestData, size_t requestLength, int requestCount) {
    g_batchResults.clear();
    g_selectedBatchResult = -1;

    auto* db = static_cast<FlatSQLDatabase*>(handle);

    std::vector<QueryRequest> requests;
    std::string errorMessage;
    if (!decodeQueryRequestsNoThrow(requestData, requestLength, requestCount, &requests, &errorMessage)) {
        g_lastError = errorMessage;
        return 0;
    }

    // Pre-validate every request before executing any of them.
    for (const auto& request : requests) {
        int expectedParams = 0;
        if (!db->validateSQL(request.sql, &expectedParams, &errorMessage)) {
            g_lastError = errorMessage;
            return 0;
        }
        if (expectedParams != static_cast<int>(request.params.size())) {
            g_lastError = paramCountMismatchMessage(expectedParams, request.params.size());
            return 0;
        }
    }

    try {
        g_batchResults.reserve(requests.size());
        for (const auto& request : requests) {
            g_batchResults.push_back(db->query(request.sql, request.params));
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
const char* flatsql_build_query_cache_key(
    const char* dataset,
    const char* artifactVersion,
    const char* queryId,
    const uint8_t* paramData,
    size_t paramLength,
    int paramCount
) {
    std::vector<Value> params;
    std::string errorMessage;
    if (!decodeParamsNoThrow(paramData, paramLength, paramCount, &params, &errorMessage)) {
        g_cacheKeyBuffer.clear();
        g_lastError = errorMessage;
        return "";
    }

    try {
        g_cacheKeyBuffer = buildQueryCacheKey(
            dataset ? dataset : "",
            artifactVersion ? artifactVersion : "",
            queryId ? queryId : "",
            params
        );
        g_lastError.clear();
        return g_cacheKeyBuffer.c_str();
    } catch (const std::exception& e) {
        g_cacheKeyBuffer.clear();
        g_lastError = e.what();
        return "";
    }
}

EMSCRIPTEN_KEEPALIVE
const char* flatsql_build_response_artifact_cache_key(
    const char* schemaName,
    const char* schemaVersion,
    const char* sql,
    const char* format,
    const char* publishEventKey,
    const char* projectionList,
    const uint8_t* paramData,
    size_t paramLength,
    int paramCount
) {
    std::vector<Value> params;
    std::string errorMessage;
    if (!decodeParamsNoThrow(paramData, paramLength, paramCount, &params, &errorMessage)) {
        g_cacheKeyBuffer.clear();
        g_lastError = errorMessage;
        return "";
    }

    try {
        g_cacheKeyBuffer = buildResponseArtifactCacheKey(
            schemaName ? schemaName : "",
            schemaVersion ? schemaVersion : "",
            sql ? sql : "",
            format ? format : "",
            publishEventKey ? publishEventKey : "",
            decodeStringList(projectionList),
            params
        );
        g_lastError.clear();
        return g_cacheKeyBuffer.c_str();
    } catch (const std::exception& e) {
        g_cacheKeyBuffer.clear();
        g_lastError = e.what();
        return "";
    }
}

EMSCRIPTEN_KEEPALIVE
int flatsql_register_query_template(void* handle,
                                    const char* queryId,
                                    const char* sql,
                                    int cacheable) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    const std::string sqlStr = sql ? sql : "";

    std::string validationError;
    if (!db->validateSQL(sqlStr, nullptr, &validationError)) {
        g_lastError = validationError;
        return 0;
    }

    try {
        db->registerQueryTemplate(
            queryId ? queryId : "",
            sqlStr,
            cacheable != 0
        );
        g_lastError.clear();
        return 1;
    } catch (const std::exception& e) {
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
int flatsql_query_template(void* handle,
                           const char* queryId,
                           const uint8_t* paramData,
                           size_t paramLength,
                           int paramCount) {
    g_batchResults.clear();
    g_selectedBatchResult = -1;

    auto* db = static_cast<FlatSQLDatabase*>(handle);
    const std::string queryIdStr = queryId ? queryId : "";

    // Pre-check template existence without exceptions.
    // Message must match FlatSQLDatabase::queryTemplate's throw text.
    if (!db->hasQueryTemplate(queryIdStr)) {
        g_lastError = "Query template not found: " + queryIdStr;
        return 0;
    }

    std::vector<Value> params;
    std::string errorMessage;
    if (!decodeParamsNoThrow(paramData, paramLength, paramCount, &params, &errorMessage)) {
        g_lastError = errorMessage;
        return 0;
    }

    const std::string* templateSql = db->queryTemplateSQL(queryIdStr);
    if (templateSql) {
        int expectedParams = 0;
        if (!db->validateSQL(*templateSql, &expectedParams, &errorMessage)) {
            g_lastError = errorMessage;
            return 0;
        }
        if (expectedParams != static_cast<int>(params.size())) {
            g_lastError = paramCountMismatchMessage(expectedParams, params.size());
            return 0;
        }
    }

    try {
        g_lastResult = db->queryTemplate(queryIdStr, params);
        g_lastError.clear();
        return 1;
    } catch (const std::exception& e) {
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
void flatsql_clear_query_cache(void* handle) {
    static_cast<FlatSQLDatabase*>(handle)->clearQueryResultCache();
}

EMSCRIPTEN_KEEPALIVE
int flatsql_configure_query_cache(void* handle, size_t maxEntries, size_t maxRows) {
    try {
        static_cast<FlatSQLDatabase*>(handle)->configureQueryResultCache(maxEntries, maxRows);
        g_lastError.clear();
        return 1;
    } catch (const std::exception& e) {
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
double flatsql_query_cache_hits(void* handle) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->getQueryCacheStats().hits);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_query_cache_misses(void* handle) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->getQueryCacheStats().misses);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_query_cache_size(void* handle) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->getQueryCacheStats().size);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_query_cache_generation(void* handle) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->getQueryCacheStats().generation);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_query_cache_max_entries(void* handle) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->getQueryCacheStats().maxEntries);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_query_cache_max_rows(void* handle) {
    return static_cast<double>(static_cast<FlatSQLDatabase*>(handle)->getQueryCacheStats().maxRows);
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
    if (std::holds_alternative<int8_t>(v)) return 2;
    if (std::holds_alternative<int16_t>(v)) return 2;
    if (std::holds_alternative<int32_t>(v)) return 2;
    if (std::holds_alternative<uint8_t>(v)) return 2;
    if (std::holds_alternative<uint16_t>(v)) return 2;
    if (std::holds_alternative<uint32_t>(v)) return 3;
    if (std::holds_alternative<int64_t>(v)) return 3;
    if (std::holds_alternative<uint64_t>(v)) return 3;
    if (std::holds_alternative<float>(v)) return 4;
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
    if (std::holds_alternative<int8_t>(v)) return static_cast<double>(std::get<int8_t>(v));
    if (std::holds_alternative<int16_t>(v)) return static_cast<double>(std::get<int16_t>(v));
    if (std::holds_alternative<int32_t>(v)) return static_cast<double>(std::get<int32_t>(v));
    if (std::holds_alternative<int64_t>(v)) return static_cast<double>(std::get<int64_t>(v));
    if (std::holds_alternative<uint8_t>(v)) return static_cast<double>(std::get<uint8_t>(v));
    if (std::holds_alternative<uint16_t>(v)) return static_cast<double>(std::get<uint16_t>(v));
    if (std::holds_alternative<uint32_t>(v)) return static_cast<double>(std::get<uint32_t>(v));
    if (std::holds_alternative<uint64_t>(v)) return static_cast<double>(std::get<uint64_t>(v));
    if (std::holds_alternative<float>(v)) return static_cast<double>(std::get<float>(v));
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

static void clearResponseArtifact() {
    g_responseArtifactStream.reset();
    g_responseArtifactRowCount = 0;
    g_responseArtifactColumnCount = 0;
    g_responseArtifactCacheHit = 0;
}

EMSCRIPTEN_KEEPALIVE
int flatsql_query_raw_flatbuffer_stream(
    void* handle,
    const char* sql,
    const uint8_t* paramData,
    size_t paramLength,
    int paramCount
) {
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    const std::string sqlStr = sql ? sql : "";

    std::vector<Value> params;
    std::string errorMessage;
    if (!decodeParamsNoThrow(paramData, paramLength, paramCount, &params, &errorMessage)) {
        clearResponseArtifact();
        g_lastError = errorMessage;
        return 0;
    }

    int expectedParams = 0;
    if (!db->validateSQL(sqlStr, &expectedParams, &errorMessage)) {
        clearResponseArtifact();
        g_lastError = errorMessage;
        return 0;
    }
    if (expectedParams != static_cast<int>(params.size())) {
        clearResponseArtifact();
        g_lastError = paramCountMismatchMessage(expectedParams, params.size());
        return 0;
    }

    try {
        // Cached response-artifact path: repeated (sql, params) requests
        // return the previously materialized aligned stream without
        // re-executing SQL; any ingest/DML/mark-deleted invalidates
        // (generation-keyed, see FlatSQLDatabase::queryRawFlatBufferStream).
        FlatSQLDatabase::RawStreamResult result;
        if (!db->queryRawFlatBufferStream(sqlStr, params, &result, &errorMessage)) {
            clearResponseArtifact();
            g_lastError = errorMessage;
            return 0;
        }

        g_responseArtifactStream = result.stream;
        g_responseArtifactRowCount = result.rowCount;
        g_responseArtifactColumnCount = result.columnCount;
        g_responseArtifactCacheHit = result.cacheHit ? 1 : 0;

        g_lastError.clear();
        return 1;
    } catch (const std::exception& e) {
        clearResponseArtifact();
        g_lastError = e.what();
        return 0;
    }
}

EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_response_artifact_data() {
    return g_responseArtifactStream ? g_responseArtifactStream->data() : nullptr;
}

EMSCRIPTEN_KEEPALIVE
int flatsql_response_artifact_size() {
    return g_responseArtifactStream ? static_cast<int>(g_responseArtifactStream->size()) : 0;
}

EMSCRIPTEN_KEEPALIVE
int flatsql_response_artifact_cache_hit() {
    return g_responseArtifactCacheHit;
}

EMSCRIPTEN_KEEPALIVE
int flatsql_configure_raw_stream_cache(void* handle, int maxEntries, double maxTotalBytes) {
    if (maxEntries < 0 || maxTotalBytes < 0) {
        g_lastError = "raw stream cache limits must be non-negative";
        return 0;
    }
    static_cast<FlatSQLDatabase*>(handle)->configureRawStreamCache(
        static_cast<size_t>(maxEntries),
        static_cast<size_t>(maxTotalBytes)
    );
    g_lastError.clear();
    return 1;
}

EMSCRIPTEN_KEEPALIVE
double flatsql_raw_stream_cache_hits(void* handle) {
    return static_cast<double>(
        static_cast<FlatSQLDatabase*>(handle)->getRawStreamCacheStats().hits);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_raw_stream_cache_misses(void* handle) {
    return static_cast<double>(
        static_cast<FlatSQLDatabase*>(handle)->getRawStreamCacheStats().misses);
}

EMSCRIPTEN_KEEPALIVE
int flatsql_raw_stream_cache_size(void* handle) {
    return static_cast<int>(
        static_cast<FlatSQLDatabase*>(handle)->getRawStreamCacheStats().entries);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_raw_stream_cache_total_bytes(void* handle) {
    return static_cast<double>(
        static_cast<FlatSQLDatabase*>(handle)->getRawStreamCacheStats().totalBytes);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_response_artifact_row_count() {
    return static_cast<double>(g_responseArtifactRowCount);
}

EMSCRIPTEN_KEEPALIVE
double flatsql_response_artifact_column_count() {
    return static_cast<double>(g_responseArtifactColumnCount);
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
void flatsql_reserve_storage(void* handle, size_t bytes) {
    static_cast<FlatSQLDatabase*>(handle)->reserveStorage(bytes);
}

EMSCRIPTEN_KEEPALIVE
void flatsql_load_from_db(void* handle, void* sourceHandle) {
    auto* source = static_cast<FlatSQLDatabase*>(sourceHandle);
    const auto& sourceStorage = source->getStorage();
    static_cast<FlatSQLDatabase*>(handle)->loadAndRebuild(
        sourceStorage.getDataBuffer(),
        static_cast<size_t>(sourceStorage.getDataSize())
    );
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
const uint8_t* flatsql_create_test_publish_event(const char* fileId,
                                                 const char* recordId,
                                                 int32_t eventIndex,
                                                 int32_t payloadSize) {
    g_testBuffer = createPublishEventFlatBufferInternal(
        fileId ? fileId : "",
        recordId ? recordId : "",
        eventIndex,
        payloadSize
    );
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

static void resetRawFlatBufferState() {
    g_rawFlatBuffer = nullptr;
    g_rawFlatBufferSize = 0;
    g_rawFlatBufferSequence = 0;
}

// Get raw FlatBuffer pointer by table name and indexed column value
// Returns pointer to FlatBuffer in WASM memory, or 0 if not found
// Call flatsql_get_raw_flatbuffer_size() after to get the size
EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_get_flatbuffer_by_id(void* handle, const char* tableName, int32_t id) {
    resetRawFlatBufferState();
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    g_rawFlatBuffer = db->findRawByIndex(tableName, "id", static_cast<int64_t>(id),
                                          &g_rawFlatBufferSize, &g_rawFlatBufferSequence);
    return g_rawFlatBuffer;
}

// Get raw FlatBuffer pointer by table name and email (string key)
EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_get_flatbuffer_by_email(void* handle, const char* tableName, const char* email) {
    resetRawFlatBufferState();
    auto* db = static_cast<FlatSQLDatabase*>(handle);
    g_rawFlatBuffer = db->findRawByIndex(tableName, "email", std::string(email),
                                          &g_rawFlatBufferSize, &g_rawFlatBufferSequence);
    return g_rawFlatBuffer;
}

// Get raw FlatBuffer pointer by table name, indexed column name, and one typed key.
EMSCRIPTEN_KEEPALIVE
const uint8_t* flatsql_get_flatbuffer_by_index(
    void* handle,
    const char* tableName,
    const char* columnName,
    const uint8_t* paramData,
    size_t paramLength,
    int paramCount
) {
    resetRawFlatBufferState();

    std::vector<Value> params;
    std::string errorMessage;
    if (!decodeParamsNoThrow(paramData, paramLength, paramCount, &params, &errorMessage)) {
        g_lastError = errorMessage;
        return nullptr;
    }
    if (params.size() != 1) {
        g_lastError = "Indexed FlatBuffer lookup expects exactly one key parameter";
        return nullptr;
    }

    try {
        auto* db = static_cast<FlatSQLDatabase*>(handle);
        g_rawFlatBuffer = db->findRawByIndex(
            tableName ? tableName : "",
            columnName ? columnName : "",
            params[0],
            &g_rawFlatBufferSize,
            &g_rawFlatBufferSequence
        );
        g_lastError.clear();
        return g_rawFlatBuffer;
    } catch (const std::exception& e) {
        resetRawFlatBufferState();
        g_lastError = e.what();
        return nullptr;
    }
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
