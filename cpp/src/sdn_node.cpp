#include "flatsql/sdn_node.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <regex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/idl.h>
#include <flatbuffers/reflection.h>
#include <flatbuffers/reflection_generated.h>

#include "flatsql/schema_parser.h"
#include "sds/FSB/main_generated.h"
#include "sds/FSO/main_generated.h"
#include "sds/FSB/main_aligned.h"
#include "sds/FSO/main_aligned.h"
#include "space_data_module_invoke.h"

namespace {

constexpr const char* kFsoSchemaName = "FSO.fbs";
constexpr const char* kFsoFileIdentifier = "$FSO";
constexpr const char* kFsoRootType = "FSO";
constexpr const char* kFsbSchemaName = "FSB.fbs";
constexpr const char* kFsbFileIdentifier = "$FSB";
constexpr const char* kFsbRootType = "FSB";
constexpr const char* kSnapshotSchemaName = "FlatSQLNodeSnapshot";
constexpr const char* kSnapshotFileIdentifier = "FSN1";
constexpr const char* kOpaqueNamespace = "primary";
constexpr const char* kOpaqueManifestKey = "snapshot.manifest";
constexpr const char* kOpaqueManifestIdentifier = "FDM2";
constexpr size_t kOpaqueChunkBytes = 1024 * 1024;
constexpr uint64_t kMaxOpaqueSnapshotBytes = 512ull * 1024 * 1024;
constexpr int32_t kMaxHostcallResponseBytes = 4 * 1024 * 1024;
constexpr uint32_t kFlatbufferWireFormat =
    PLUGIN_PAYLOAD_WIRE_FORMAT_FLATBUFFER;
constexpr uint32_t kAlignedWireFormat =
    PLUGIN_PAYLOAD_WIRE_FORMAT_ALIGNED_BINARY;
constexpr size_t kFsbChunkCapacity = 1024u * 1024u;

void* g_database = nullptr;
std::string g_schema_idl;
std::string g_database_name;
std::vector<std::pair<std::string, std::string>> g_table_bindings;
std::vector<uint8_t> g_reflection_schema;

struct ViewDefinition {
    std::string query;
    std::string key_expression;
};

std::map<std::string, ViewDefinition> g_views;
uint64_t g_retention_max_records = 0;
uint64_t g_retention_max_age_millis = 0;
uint64_t g_compaction_target_bytes = 0;
bool g_durable_state_checked = false;
bool g_durable_state_poisoned = false;
std::string g_durable_poison_error;
bool g_durable_manifest_found = false;
uint64_t g_durable_generation = 0;
std::vector<uint8_t> g_durable_manifest;
std::vector<std::string> g_durable_chunk_keys;

extern "C" {

__attribute__((import_module("space_data_module_host"), import_name("call")))
int32_t sdm_host_call(
    const char* operation_ptr,
    int32_t operation_len,
    const char* payload_ptr,
    int32_t payload_len);
__attribute__((import_module("space_data_module_host"), import_name("response_len")))
int32_t sdm_host_response_len(void);
__attribute__((import_module("space_data_module_host"), import_name("read_response")))
int32_t sdm_host_read_response(char* destination_ptr, int32_t destination_len);
__attribute__((import_module("space_data_module_host"), import_name("clear_response")))
int32_t sdm_host_clear_response(void);
__attribute__((import_module("space_data_module_host"), import_name("last_status_code")))
int32_t sdm_host_last_status_code(void);

}  // extern "C"

extern "C" void* flatsql_create_db(const char* schema, const char* db_name);
extern "C" void flatsql_destroy_db(void* handle);
extern "C" void flatsql_register_file_id(
    void* handle,
    const char* file_identifier,
    const char* table_name);
extern "C" void flatsql_enable_demo_extractors(void* handle);
extern "C" double flatsql_ingest(
    void* handle,
    const uint8_t* data,
    size_t length);
extern "C" int flatsql_query_raw_flatbuffer_stream(
    void* handle,
    const char* sql,
    const uint8_t* parameter_data,
    size_t parameter_length,
    int parameter_count);
extern "C" const uint8_t* flatsql_response_artifact_data(void);
extern "C" int flatsql_response_artifact_size(void);
extern "C" double flatsql_response_artifact_row_count(void);
extern "C" double flatsql_response_artifact_column_count(void);
extern "C" const uint8_t* flatsql_export_data(void* handle);
extern "C" int flatsql_export_size(void);
extern "C" void flatsql_load_and_rebuild(
    void* handle,
    const uint8_t* data,
    size_t length);
extern "C" const char* flatsql_get_error(void);

std::string bytesToString(const flatbuffers::Vector<uint8_t>* value) {
    if (!value || value->size() == 0) {
        return {};
    }
    return std::string(
        reinterpret_cast<const char*>(value->data()),
        static_cast<size_t>(value->size()));
}

template <typename VectorType>
std::vector<uint8_t> alignedBytes(const VectorType& value) {
    const size_t size = value.size();
    return std::vector<uint8_t>(value.values, value.values + size);
}

template <typename VectorType>
std::string alignedText(const VectorType& value) {
    const size_t size = value.size();
    return std::string(
        reinterpret_cast<const char*>(value.values),
        reinterpret_cast<const char*>(value.values) + size);
}

struct ControlFrame {
    uint32_t wire_format = kFlatbufferWireFormat;
    flatSqlNodeOperation operation = flatSqlNodeOperation_NONE;
    uint64_t request_id = 0;
    std::string database_name;
    std::string schema_idl;
    std::vector<std::pair<std::string, std::string>> table_bindings;
    std::string table_name;
    std::string index_name;
    std::string index_expression;
    std::string view_name;
    std::string query;
    std::vector<uint8_t> parameters;
    uint32_t parameter_count = 0;
    std::string upsert_key_expression;
    uint64_t retention_max_records = 0;
    uint64_t retention_max_age_millis = 0;
    uint64_t compaction_target_bytes = 0;
};

bool readControlFrame(
    const plugin_input_frame_t* frame,
    ControlFrame* output,
    std::string* error) {
    if (!frame || !output || !frame->payload || frame->payload_length == 0) {
        if (error) *error = "missing FSO control payload";
        return false;
    }
    output->wire_format = frame->wire_format;
    if (frame->wire_format == kFlatbufferWireFormat) {
        flatbuffers::Verifier verifier(frame->payload, frame->payload_length);
        if (!VerifyFSOBuffer(verifier)) {
            if (error) *error = "invalid canonical FSO control payload";
            return false;
        }
        const FSO* control = GetFSO(frame->payload);
        output->operation = control->OPERATION();
        output->request_id = control->REQUEST_ID();
        output->database_name = control->DATABASE_NAME()
            ? control->DATABASE_NAME()->str()
            : std::string{};
        output->schema_idl = bytesToString(control->SCHEMA_IDL());
        if (const auto* bindings = control->TABLE_BINDINGS()) {
            for (const auto* binding : *bindings) {
                if (!binding || !binding->FILE_IDENTIFIER() ||
                    !binding->TABLE_NAME()) {
                    continue;
                }
                output->table_bindings.emplace_back(
                    binding->FILE_IDENTIFIER()->str(),
                    binding->TABLE_NAME()->str());
            }
        }
        output->table_name = control->TABLE_NAME()
            ? control->TABLE_NAME()->str()
            : std::string{};
        output->index_name = control->INDEX_NAME()
            ? control->INDEX_NAME()->str()
            : std::string{};
        output->index_expression = bytesToString(control->INDEX_EXPRESSION());
        output->view_name = control->VIEW_NAME()
            ? control->VIEW_NAME()->str()
            : std::string{};
        output->query = bytesToString(control->QUERY());
        if (const auto* parameters = control->PARAMETERS()) {
            output->parameters.assign(parameters->begin(), parameters->end());
        }
        output->parameter_count = control->PARAMETER_COUNT();
        output->upsert_key_expression =
            bytesToString(control->UPSERT_KEY_EXPRESSION());
        output->retention_max_records = control->RETENTION_MAX_RECORDS();
        output->retention_max_age_millis =
            control->RETENTION_MAX_AGE_MILLIS();
        output->compaction_target_bytes = control->COMPACTION_TARGET_BYTES();
        return true;
    }
    if (frame->wire_format == kAlignedWireFormat) {
        if (frame->payload_length != Aligned::FSO_SIZE ||
            reinterpret_cast<uintptr_t>(frame->payload) % Aligned::FSO_ALIGN != 0) {
            if (error) *error = "invalid aligned FSO control layout";
            return false;
        }
        const auto* control = Aligned::FSO::fromBytes(frame->payload);
        output->operation = control->OPERATION;
        output->request_id = control->REQUEST_ID;
        if (control->has_DATABASE_NAME()) {
            output->database_name = control->DATABASE_NAME.str();
        }
        if (control->has_SCHEMA_IDL()) {
            output->schema_idl = alignedText(control->SCHEMA_IDL);
        }
        if (control->has_TABLE_BINDINGS()) {
            const uint32_t count = control->TABLE_BINDINGS.size();
            for (uint32_t index = 0; index < count; ++index) {
                const auto& binding = control->TABLE_BINDINGS.at(index);
                if (!binding.has_FILE_IDENTIFIER() || !binding.has_TABLE_NAME()) {
                    continue;
                }
                output->table_bindings.emplace_back(
                    binding.FILE_IDENTIFIER.str(),
                    binding.TABLE_NAME.str());
            }
        }
        if (control->has_TABLE_NAME()) {
            output->table_name = control->TABLE_NAME.str();
        }
        if (control->has_INDEX_NAME()) {
            output->index_name = control->INDEX_NAME.str();
        }
        if (control->has_INDEX_EXPRESSION()) {
            output->index_expression = alignedText(control->INDEX_EXPRESSION);
        }
        if (control->has_VIEW_NAME()) {
            output->view_name = control->VIEW_NAME.str();
        }
        if (control->has_QUERY()) {
            output->query = alignedText(control->QUERY);
        }
        if (control->has_PARAMETERS()) {
            output->parameters = alignedBytes(control->PARAMETERS);
        }
        output->parameter_count = control->PARAMETER_COUNT;
        if (control->has_UPSERT_KEY_EXPRESSION()) {
            output->upsert_key_expression =
                alignedText(control->UPSERT_KEY_EXPRESSION);
        }
        output->retention_max_records = control->RETENTION_MAX_RECORDS;
        output->retention_max_age_millis = control->RETENTION_MAX_AGE_MILLIS;
        output->compaction_target_bytes = control->COMPACTION_TARGET_BYTES;
        return true;
    }
    if (error) *error = "unsupported FSO wire format";
    return false;
}

struct ByteStreamChunk {
    uint32_t wire_format = kFlatbufferWireFormat;
    uint64_t request_id = 0;
    flatSqlByteStreamKind kind = flatSqlByteStreamKind_UNSPECIFIED;
    uint32_t sequence = 0;
    bool final = false;
    uint64_t total_bytes = 0;
    uint64_t record_count = 0;
    uint32_t column_count = 0;
    std::string schema_name;
    std::string file_identifier;
    std::vector<uint8_t> data;
    std::vector<uint8_t> sha256;
};

struct CollectedByteStream {
    uint32_t wire_format = kFlatbufferWireFormat;
    uint64_t request_id = 0;
    uint64_t record_count = 0;
    std::string schema_name;
    std::string file_identifier;
    std::vector<uint8_t> data;
    std::vector<uint8_t> sha256;
};

bool readByteStreamFrame(
    const plugin_input_frame_t* frame,
    ByteStreamChunk* output,
    std::string* error) {
    if (!frame || !output || !frame->payload || frame->payload_length == 0) {
        if (error) *error = "missing FSB payload";
        return false;
    }
    output->wire_format = frame->wire_format;
    if (frame->wire_format == kFlatbufferWireFormat) {
        flatbuffers::Verifier verifier(frame->payload, frame->payload_length);
        if (!VerifyFSBBuffer(verifier)) {
            if (error) *error = "invalid canonical FSB payload";
            return false;
        }
        const FSB* stream = GetFSB(frame->payload);
        output->request_id = stream->REQUEST_ID();
        output->kind = stream->KIND();
        output->sequence = stream->CHUNK_SEQUENCE();
        output->final = stream->FINAL();
        output->total_bytes = stream->TOTAL_BYTES();
        output->record_count = stream->RECORD_COUNT();
        output->column_count = stream->COLUMN_COUNT();
        output->schema_name = stream->SCHEMA_NAME()
            ? stream->SCHEMA_NAME()->str()
            : std::string{};
        output->file_identifier = stream->FILE_IDENTIFIER()
            ? stream->FILE_IDENTIFIER()->str()
            : std::string{};
        if (const auto* data = stream->DATA()) {
            output->data.assign(data->begin(), data->end());
        }
        if (const auto* checksum = stream->SHA256()) {
            output->sha256.assign(checksum->begin(), checksum->end());
        }
        return true;
    }
    if (frame->wire_format == kAlignedWireFormat) {
        if (frame->payload_length != Aligned::FSB_SIZE ||
            reinterpret_cast<uintptr_t>(frame->payload) % Aligned::FSB_ALIGN != 0) {
            if (error) *error = "invalid aligned FSB layout";
            return false;
        }
        const auto* stream = Aligned::FSB::fromBytes(frame->payload);
        output->request_id = stream->REQUEST_ID;
        output->kind = stream->KIND;
        output->sequence = stream->CHUNK_SEQUENCE;
        output->final = stream->FINAL;
        output->total_bytes = stream->TOTAL_BYTES;
        output->record_count = stream->RECORD_COUNT;
        output->column_count = stream->COLUMN_COUNT;
        if (stream->has_SCHEMA_NAME()) {
            output->schema_name = stream->SCHEMA_NAME.str();
        }
        if (stream->has_FILE_IDENTIFIER()) {
            output->file_identifier = stream->FILE_IDENTIFIER.str();
        }
        if (stream->has_DATA()) {
            output->data = alignedBytes(stream->DATA);
        }
        if (stream->has_SHA256()) {
            output->sha256 = alignedBytes(stream->SHA256);
        }
        return true;
    }
    if (error) *error = "unsupported FSB wire format";
    return false;
}

bool assembleByteStream(
    std::vector<ByteStreamChunk> chunks,
    flatSqlByteStreamKind expected_kind,
    CollectedByteStream* output,
    std::string* error) {
    if (chunks.empty() || !output) return false;
    std::sort(
        chunks.begin(),
        chunks.end(),
        [](const ByteStreamChunk& left, const ByteStreamChunk& right) {
            return left.sequence < right.sequence;
        });
    const uint64_t expected_request = chunks.front().request_id;
    const uint32_t expected_wire = chunks.front().wire_format;
    const std::string expected_schema = chunks.front().schema_name;
    const std::string expected_file_identifier =
        chunks.front().file_identifier;
    const std::vector<uint8_t> expected_sha256 = chunks.front().sha256;
    uint64_t declared_total = chunks.front().total_bytes;
    const uint64_t declared_records = chunks.front().record_count;
    size_t total_size = 0;
    bool final_seen = false;
    for (size_t index = 0; index < chunks.size(); ++index) {
        const auto& chunk = chunks[index];
        if (chunk.kind != expected_kind) {
            if (error) *error = "FSB stream kind does not match the operation";
            return false;
        }
        if (chunk.request_id != expected_request ||
            chunk.wire_format != expected_wire ||
            chunk.sequence != index ||
            chunk.total_bytes != declared_total ||
            chunk.record_count != declared_records ||
            chunk.schema_name != expected_schema ||
            chunk.file_identifier != expected_file_identifier ||
            chunk.sha256 != expected_sha256) {
            if (error) *error = "FSB chunks are not one contiguous ordered stream";
            return false;
        }
        if (chunk.final != (index + 1 == chunks.size())) {
            if (error) *error = "FSB final marker is not on the last chunk";
            return false;
        }
        final_seen = final_seen || chunk.final;
        if (chunk.data.size() > std::numeric_limits<size_t>::max() - total_size) {
            if (error) *error = "FSB stream size overflows host size_t";
            return false;
        }
        total_size += chunk.data.size();
    }
    if (!final_seen || declared_total != total_size) {
        if (error) *error = "FSB total byte count does not match chunk data";
        return false;
    }
    output->data.clear();
    output->data.reserve(total_size);
    for (const auto& chunk : chunks) {
        output->data.insert(
            output->data.end(),
            chunk.data.begin(),
            chunk.data.end());
    }
    output->request_id = expected_request;
    output->record_count = declared_records;
    output->wire_format = expected_wire;
    output->schema_name = expected_schema;
    output->file_identifier = expected_file_identifier;
    output->sha256 = expected_sha256;
    return true;
}

bool collectByteStreams(
    const char* port_id,
    flatSqlByteStreamKind expected_kind,
    std::vector<CollectedByteStream>* outputs,
    std::string* error) {
    if (!outputs) return false;
    std::vector<std::vector<ByteStreamChunk>> groups;
    std::map<uint64_t, size_t> group_by_request;
    for (uint32_t occurrence = 0;; ++occurrence) {
        const int32_t input_index = plugin_find_input_index(port_id, occurrence);
        if (input_index < 0) break;
        const auto* frame =
            plugin_get_input_frame(static_cast<uint32_t>(input_index));
        ByteStreamChunk chunk;
        if (!readByteStreamFrame(frame, &chunk, error)) return false;
        const auto [found, inserted] = group_by_request.emplace(
            chunk.request_id,
            groups.size());
        if (inserted) groups.emplace_back();
        groups[found->second].emplace_back(std::move(chunk));
    }
    outputs->clear();
    outputs->reserve(groups.size());
    for (auto& chunks : groups) {
        CollectedByteStream stream;
        if (!assembleByteStream(
                std::move(chunks),
                expected_kind,
                &stream,
                error)) {
            return false;
        }
        outputs->emplace_back(std::move(stream));
    }
    return true;
}

bool collectByteStream(
    const char* port_id,
    flatSqlByteStreamKind expected_kind,
    std::vector<uint8_t>* data,
    uint64_t* request_id,
    uint64_t* record_count,
    uint32_t* wire_format,
    std::string* schema_name,
    std::string* file_identifier,
    std::vector<uint8_t>* checksum,
    std::string* error) {
    std::vector<CollectedByteStream> streams;
    if (!collectByteStreams(port_id, expected_kind, &streams, error)) {
        return false;
    }
    if (streams.size() != 1) {
        if (error) {
            *error = streams.empty()
                ? std::string("missing input frames on port ") + port_id
                : std::string("expected one byte stream on port ") + port_id;
        }
        return false;
    }
    CollectedByteStream& stream = streams.front();
    if (data) *data = std::move(stream.data);
    if (request_id) *request_id = stream.request_id;
    if (record_count) *record_count = stream.record_count;
    if (wire_format) *wire_format = stream.wire_format;
    if (schema_name) *schema_name = std::move(stream.schema_name);
    if (file_identifier) *file_identifier = std::move(stream.file_identifier);
    if (checksum) *checksum = std::move(stream.sha256);
    return true;
}

uint32_t rotateRight(uint32_t value, uint32_t amount) {
    return (value >> amount) | (value << (32u - amount));
}

void sha256Transform(
    std::array<uint32_t, 8>* state,
    const uint8_t* block) {
    static constexpr std::array<uint32_t, 64> constants = {
        0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u,
        0x3956c25bu, 0x59f111f1u, 0x923f82a4u, 0xab1c5ed5u,
        0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u,
        0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u,
        0xe49b69c1u, 0xefbe4786u, 0x0fc19dc6u, 0x240ca1ccu,
        0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
        0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u,
        0xc6e00bf3u, 0xd5a79147u, 0x06ca6351u, 0x14292967u,
        0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u,
        0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u,
        0xa2bfe8a1u, 0xa81a664bu, 0xc24b8b70u, 0xc76c51a3u,
        0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
        0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u,
        0x391c0cb3u, 0x4ed8aa4au, 0x5b9cca4fu, 0x682e6ff3u,
        0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
        0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u,
    };
    std::array<uint32_t, 64> words{};
    for (size_t index = 0; index < 16; ++index) {
        const size_t offset = index * 4;
        words[index] =
            (static_cast<uint32_t>(block[offset]) << 24u) |
            (static_cast<uint32_t>(block[offset + 1]) << 16u) |
            (static_cast<uint32_t>(block[offset + 2]) << 8u) |
            static_cast<uint32_t>(block[offset + 3]);
    }
    for (size_t index = 16; index < words.size(); ++index) {
        const uint32_t s0 =
            rotateRight(words[index - 15], 7) ^
            rotateRight(words[index - 15], 18) ^
            (words[index - 15] >> 3u);
        const uint32_t s1 =
            rotateRight(words[index - 2], 17) ^
            rotateRight(words[index - 2], 19) ^
            (words[index - 2] >> 10u);
        words[index] = words[index - 16] + s0 + words[index - 7] + s1;
    }

    uint32_t a = (*state)[0];
    uint32_t b = (*state)[1];
    uint32_t c = (*state)[2];
    uint32_t d = (*state)[3];
    uint32_t e = (*state)[4];
    uint32_t f = (*state)[5];
    uint32_t g = (*state)[6];
    uint32_t h = (*state)[7];
    for (size_t index = 0; index < words.size(); ++index) {
        const uint32_t sum1 =
            rotateRight(e, 6) ^ rotateRight(e, 11) ^ rotateRight(e, 25);
        const uint32_t choice = (e & f) ^ ((~e) & g);
        const uint32_t temporary1 =
            h + sum1 + choice + constants[index] + words[index];
        const uint32_t sum0 =
            rotateRight(a, 2) ^ rotateRight(a, 13) ^ rotateRight(a, 22);
        const uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
        const uint32_t temporary2 = sum0 + majority;
        h = g;
        g = f;
        f = e;
        e = d + temporary1;
        d = c;
        c = b;
        b = a;
        a = temporary1 + temporary2;
    }
    (*state)[0] += a;
    (*state)[1] += b;
    (*state)[2] += c;
    (*state)[3] += d;
    (*state)[4] += e;
    (*state)[5] += f;
    (*state)[6] += g;
    (*state)[7] += h;
}

std::vector<uint8_t> sha256(const uint8_t* data, size_t size) {
    std::array<uint32_t, 8> state = {
        0x6a09e667u,
        0xbb67ae85u,
        0x3c6ef372u,
        0xa54ff53au,
        0x510e527fu,
        0x9b05688cu,
        0x1f83d9abu,
        0x5be0cd19u,
    };
    size_t offset = 0;
    while (size - offset >= 64) {
        sha256Transform(&state, data + offset);
        offset += 64;
    }
    std::array<uint8_t, 128> tail{};
    const size_t remainder = size - offset;
    if (remainder > 0) {
        std::memcpy(tail.data(), data + offset, remainder);
    }
    tail[remainder] = 0x80;
    const size_t padded_size = remainder < 56 ? 64 : 128;
    const uint64_t bit_length = static_cast<uint64_t>(size) * 8u;
    for (size_t index = 0; index < 8; ++index) {
        tail[padded_size - 1 - index] =
            static_cast<uint8_t>((bit_length >> (index * 8u)) & 0xffu);
    }
    sha256Transform(&state, tail.data());
    if (padded_size == 128) {
        sha256Transform(&state, tail.data() + 64);
    }
    std::vector<uint8_t> digest(32);
    for (size_t index = 0; index < state.size(); ++index) {
        for (size_t byte = 0; byte < 4; ++byte) {
            digest[index * 4 + byte] = static_cast<uint8_t>(
                (state[index] >> (24u - byte * 8u)) & 0xffu);
        }
    }
    return digest;
}

std::vector<uint8_t> sha256(const std::vector<uint8_t>& data) {
    return sha256(data.empty() ? nullptr : data.data(), data.size());
}

int32_t emitOutput(
    const char* port_id,
    const char* schema_name,
    const char* file_identifier,
    const char* root_type,
    uint32_t wire_format,
    uint32_t aligned_byte_length,
    uint16_t aligned_alignment,
    const uint8_t* payload,
    uint32_t payload_length) {
    return plugin_push_output_typed(
        port_id,
        schema_name,
        file_identifier,
        wire_format,
        root_type,
        0,
        wire_format == kAlignedWireFormat ? aligned_byte_length : 0,
        wire_format == kAlignedWireFormat ? aligned_alignment : 0,
        payload,
        payload_length);
}

int pushStatus(
    flatSqlNodeOperation operation,
    uint64_t request_id,
    flatSqlNodeStatus status,
    uint64_t affected_records,
    uint64_t result_bytes,
    const char* error_code,
    const std::string& message,
    uint32_t wire_format) {
    int32_t output_index = -1;
    if (wire_format == kAlignedWireFormat) {
        std::vector<uint8_t> payload(Aligned::FSO_SIZE, 0);
        auto* record = Aligned::FSO::fromBytes(payload.data());
        record->OPERATION = operation;
        record->REQUEST_ID = request_id;
        record->STATUS = status;
        record->AFFECTED_RECORDS = affected_records;
        record->RESULT_BYTES = result_bytes;
        if (error_code && error_code[0]) {
            record->set_has_ERROR_CODE(true);
            record->ERROR_CODE.set(error_code);
        }
        if (!message.empty()) {
            record->set_has_MESSAGE(true);
            const size_t size = std::min<size_t>(
                message.size(),
                sizeof(record->MESSAGE.values));
            record->MESSAGE.set_length(static_cast<uint32_t>(size));
            std::memcpy(record->MESSAGE.values, message.data(), size);
        }
        output_index = emitOutput(
            "status",
            kFsoSchemaName,
            kFsoFileIdentifier,
            kFsoRootType,
            wire_format,
            static_cast<uint32_t>(Aligned::FSO_SIZE),
            static_cast<uint16_t>(Aligned::FSO_ALIGN),
            payload.data(),
            static_cast<uint32_t>(payload.size()));
    } else {
        flatbuffers::FlatBufferBuilder builder(512);
        const auto error = error_code && error_code[0]
            ? builder.CreateString(error_code)
            : flatbuffers::Offset<flatbuffers::String>{};
        const auto message_bytes = message.empty()
            ? flatbuffers::Offset<flatbuffers::Vector<uint8_t>>{}
            : builder.CreateVector(
                reinterpret_cast<const uint8_t*>(message.data()),
                message.size());
        FSOBuilder status_builder(builder);
        status_builder.add_OPERATION(operation);
        status_builder.add_REQUEST_ID(request_id);
        status_builder.add_STATUS(status);
        status_builder.add_AFFECTED_RECORDS(affected_records);
        status_builder.add_RESULT_BYTES(result_bytes);
        if (error.o != 0) status_builder.add_ERROR_CODE(error);
        if (message_bytes.o != 0) status_builder.add_MESSAGE(message_bytes);
        const auto status_record = status_builder.Finish();
        FinishFSOBuffer(builder, status_record);
        output_index = emitOutput(
            "status",
            kFsoSchemaName,
            kFsoFileIdentifier,
            kFsoRootType,
            kFlatbufferWireFormat,
            0,
            0,
            builder.GetBufferPointer(),
            static_cast<uint32_t>(builder.GetSize()));
    }
    return output_index >= 0 ? 0 : 500;
}

bool pushByteStreamChunk(
    const char* port_id,
    flatSqlByteStreamKind kind,
    uint64_t request_id,
    uint32_t sequence,
    bool final,
    uint64_t total_bytes,
    uint64_t record_count,
    uint32_t column_count,
    const std::string& schema_name,
    const std::string& file_identifier,
    const uint8_t* data,
    size_t data_size,
    const std::vector<uint8_t>& checksum,
    uint32_t wire_format) {
    if (data_size > kFsbChunkCapacity ||
        (!checksum.empty() && checksum.size() != 32)) {
        return false;
    }
    int32_t output_index = -1;
    if (wire_format == kAlignedWireFormat) {
        std::vector<uint8_t> payload(Aligned::FSB_SIZE, 0);
        auto* record = Aligned::FSB::fromBytes(payload.data());
        record->REQUEST_ID = request_id;
        record->KIND = kind;
        record->CHUNK_SEQUENCE = sequence;
        record->FINAL = final;
        record->TOTAL_BYTES = total_bytes;
        record->RECORD_COUNT = record_count;
        record->COLUMN_COUNT = column_count;
        if (!schema_name.empty()) {
            record->set_has_SCHEMA_NAME(true);
            record->SCHEMA_NAME.set(schema_name);
        }
        if (!file_identifier.empty()) {
            record->set_has_FILE_IDENTIFIER(true);
            record->FILE_IDENTIFIER.set(file_identifier);
        }
        if (data_size > 0) {
            record->set_has_DATA(true);
            record->DATA.set_length(static_cast<uint32_t>(data_size));
            std::memcpy(record->DATA.values, data, data_size);
        }
        if (!checksum.empty()) {
            record->set_has_SHA256(true);
            record->SHA256.set_length(static_cast<uint32_t>(checksum.size()));
            std::memcpy(
                record->SHA256.values,
                checksum.data(),
                checksum.size());
        }
        output_index = emitOutput(
            port_id,
            kFsbSchemaName,
            kFsbFileIdentifier,
            kFsbRootType,
            wire_format,
            static_cast<uint32_t>(Aligned::FSB_SIZE),
            static_cast<uint16_t>(Aligned::FSB_ALIGN),
            payload.data(),
            static_cast<uint32_t>(payload.size()));
    } else {
        flatbuffers::FlatBufferBuilder builder(data_size + 256);
        const auto schema = schema_name.empty()
            ? flatbuffers::Offset<flatbuffers::String>{}
            : builder.CreateString(schema_name);
        const auto file_id = file_identifier.empty()
            ? flatbuffers::Offset<flatbuffers::String>{}
            : builder.CreateString(file_identifier);
        const auto body = data_size == 0
            ? flatbuffers::Offset<flatbuffers::Vector<uint8_t>>{}
            : builder.CreateVector(data, data_size);
        const auto digest = checksum.empty()
            ? flatbuffers::Offset<flatbuffers::Vector<uint8_t>>{}
            : builder.CreateVector(checksum);
        FSBBuilder stream_builder(builder);
        stream_builder.add_REQUEST_ID(request_id);
        stream_builder.add_KIND(kind);
        stream_builder.add_CHUNK_SEQUENCE(sequence);
        stream_builder.add_FINAL(final);
        stream_builder.add_TOTAL_BYTES(total_bytes);
        stream_builder.add_RECORD_COUNT(record_count);
        stream_builder.add_COLUMN_COUNT(column_count);
        if (schema.o != 0) stream_builder.add_SCHEMA_NAME(schema);
        if (file_id.o != 0) stream_builder.add_FILE_IDENTIFIER(file_id);
        if (body.o != 0) stream_builder.add_DATA(body);
        if (digest.o != 0) stream_builder.add_SHA256(digest);
        const auto stream = stream_builder.Finish();
        FinishFSBBuffer(builder, stream);
        output_index = emitOutput(
            port_id,
            kFsbSchemaName,
            kFsbFileIdentifier,
            kFsbRootType,
            kFlatbufferWireFormat,
            0,
            0,
            builder.GetBufferPointer(),
            static_cast<uint32_t>(builder.GetSize()));
    }
    return output_index >= 0;
}

bool pushByteStream(
    const char* port_id,
    flatSqlByteStreamKind kind,
    uint64_t request_id,
    uint64_t record_count,
    uint32_t column_count,
    const std::string& schema_name,
    const std::string& file_identifier,
    const std::vector<uint8_t>& data,
    uint32_t wire_format,
    const std::vector<uint8_t>& checksum = {}) {
    const size_t chunk_count = std::max<size_t>(
        1,
        (data.size() + kFsbChunkCapacity - 1) / kFsbChunkCapacity);
    for (size_t index = 0; index < chunk_count; ++index) {
        const size_t offset = index * kFsbChunkCapacity;
        const size_t size = std::min(kFsbChunkCapacity, data.size() - offset);
        if (!pushByteStreamChunk(
                port_id,
                kind,
                request_id,
                static_cast<uint32_t>(index),
                index + 1 == chunk_count,
                data.size(),
                record_count,
                column_count,
                schema_name,
                file_identifier,
                size > 0 ? data.data() + offset : nullptr,
                size,
                checksum,
                wire_format)) {
            return false;
        }
    }
    return true;
}

std::string reflectionSchemaSource(const std::string& schema_idl) {
    // FlatSQL accepts shorthand index annotations such as `(id)` and `(key)`.
    // They affect FlatSQL's schema-derived indexes but are not valid reflection
    // field IDs unless every field is explicitly numbered. Strip field
    // annotations for structural verification; the original IDL still drives
    // FlatSQL's database and index construction.
    return std::regex_replace(
        schema_idl,
        std::regex(R"(\([^\)]*\))"),
        std::string{});
}

const reflection::Object* findReflectionTable(
    const reflection::Schema* schema,
    const std::string& table_name) {
    if (!schema || !schema->objects()) return nullptr;
    const reflection::Object* match = nullptr;
    for (const reflection::Object* object : *schema->objects()) {
        if (!object || !object->name() || object->is_struct()) continue;
        const std::string name = object->name()->str();
        const bool exact = name == table_name;
        const bool unqualified =
            name.size() > table_name.size() &&
            name.compare(
                name.size() - table_name.size(),
                table_name.size(),
                table_name) == 0 &&
            name[name.size() - table_name.size() - 1] == '.';
        if (!exact && !unqualified) continue;
        if (match) return nullptr;
        match = object;
    }
    return match;
}

bool compileRecordVerifier(
    const std::string& schema_idl,
    const std::vector<std::pair<std::string, std::string>>& bindings,
    std::vector<uint8_t>* reflection_schema,
    std::string* error) {
    flatsql::DatabaseSchema parsed_schema;
    std::string parse_error;
    if (!flatsql::SchemaParser::tryParse(
            schema_idl,
            &parsed_schema,
            &parse_error,
            "node-validation") ||
        parsed_schema.tables.empty()) {
        if (error) {
            *error = parse_error.empty()
                ? "SCHEMA_IDL declares no FlatSQL tables"
                : parse_error;
        }
        return false;
    }

    flatbuffers::Parser parser;
    const std::string verifier_source = reflectionSchemaSource(schema_idl);
    if (!parser.Parse(verifier_source.c_str())) {
        if (error) {
            *error = parser.error_.empty()
                ? "SCHEMA_IDL cannot produce a FlatBuffer verifier"
                : parser.error_;
        }
        return false;
    }
    parser.Serialize();
    reflection_schema->assign(
        parser.builder_.GetBufferPointer(),
        parser.builder_.GetBufferPointer() + parser.builder_.GetSize());
    flatbuffers::Verifier schema_verifier(
        reflection_schema->data(),
        reflection_schema->size());
    if (!reflection::VerifySchemaBuffer(schema_verifier)) {
        if (error) *error = "compiled FlatBuffer reflection schema is invalid";
        reflection_schema->clear();
        return false;
    }
    const reflection::Schema* schema =
        reflection::GetSchema(reflection_schema->data());
    std::set<std::string> file_identifiers;
    for (const auto& [file_identifier, table_name] : bindings) {
        if (file_identifier.size() != 4) {
            if (error) *error = "table binding file identifiers must be 4 bytes";
            reflection_schema->clear();
            return false;
        }
        if (!file_identifiers.insert(file_identifier).second) {
            if (error) *error = "table binding file identifiers must be unique";
            reflection_schema->clear();
            return false;
        }
        const std::string expected_table_name = table_name;
        const bool parsed_table_exists = std::any_of(
            parsed_schema.tables.begin(),
            parsed_schema.tables.end(),
            [&expected_table_name](const flatsql::TableDef& table) {
                return table.name == expected_table_name;
            });
        if (!parsed_table_exists || !findReflectionTable(schema, table_name)) {
            if (error) {
                *error = "table binding references undeclared table: " +
                    table_name;
            }
            reflection_schema->clear();
            return false;
        }
    }
    return true;
}

const std::string* boundTableName(
    const std::vector<std::pair<std::string, std::string>>& bindings,
    const std::string& file_identifier) {
    for (const auto& [configured_file_identifier, table_name] : bindings) {
        if (configured_file_identifier == file_identifier) return &table_name;
    }
    return nullptr;
}

bool verifyFlatBufferRecord(
    const uint8_t* size_prefixed_record,
    size_t size_prefixed_record_size,
    const std::string& file_identifier,
    const std::string& table_name,
    const std::vector<uint8_t>& reflection_schema,
    std::string* error) {
    if (!size_prefixed_record || size_prefixed_record_size < 12) {
        if (error) *error = "FlatBuffer record is too short";
        return false;
    }
    const uint32_t record_size =
        flatbuffers::ReadScalar<uint32_t>(size_prefixed_record);
    if (record_size != size_prefixed_record_size - 4) {
        if (error) *error = "FlatBuffer size prefix does not match the record";
        return false;
    }
    const uint8_t* record = size_prefixed_record + 4;
    const flatbuffers::uoffset_t root_offset =
        flatbuffers::ReadScalar<flatbuffers::uoffset_t>(record);
    if (root_offset > record_size - sizeof(flatbuffers::soffset_t) ||
        root_offset % alignof(flatbuffers::uoffset_t) != 0) {
        if (error) *error = "FlatBuffer record root offset is out of bounds";
        return false;
    }
    if (!flatbuffers::BufferHasIdentifier(record, file_identifier.c_str())) {
        if (error) {
            *error = "FlatBuffer record file identifier does not match FSB";
        }
        return false;
    }
    if (reflection_schema.empty()) {
        if (error) *error = "FlatBuffer record verifier is not configured";
        return false;
    }
    const reflection::Schema* schema =
        reflection::GetSchema(reflection_schema.data());
    const reflection::Object* table = findReflectionTable(schema, table_name);
    if (!table ||
        !flatbuffers::VerifySizePrefixed(
            *schema,
            *table,
            size_prefixed_record,
            size_prefixed_record_size)) {
        if (error) {
            *error = "FlatBuffer record does not verify against " + table_name;
        }
        return false;
    }
    return true;
}

bool verifyRecordStream(
    const std::vector<uint8_t>& stream,
    const std::string* required_schema_name,
    const std::string* required_file_identifier,
    const std::vector<std::pair<std::string, std::string>>& bindings,
    const std::vector<uint8_t>& reflection_schema,
    uint64_t* record_count,
    std::string* error) {
    if (required_file_identifier && required_file_identifier->empty()) {
        if (error) *error = "FSB record stream requires FILE_IDENTIFIER";
        return false;
    }
    const std::string* required_table = required_file_identifier
        ? boundTableName(bindings, *required_file_identifier)
        : nullptr;
    if (required_file_identifier && !required_table) {
        if (error) *error = "FSB FILE_IDENTIFIER is not configured";
        return false;
    }
    if (required_schema_name &&
        (!required_table || *required_schema_name != *required_table)) {
        if (error) {
            *error = "FSB SCHEMA_NAME does not match its configured table";
        }
        return false;
    }

    uint64_t records = 0;
    size_t offset = 0;
    while (offset < stream.size()) {
        if (stream.size() - offset < 4) {
            if (error) *error = "record stream ends inside a size prefix";
            return false;
        }
        const uint32_t record_size =
            flatbuffers::ReadScalar<uint32_t>(stream.data() + offset);
        if (record_size > stream.size() - offset - 4) {
            if (error) *error = "record stream ends inside a FlatBuffer record";
            return false;
        }
        const uint8_t* record = stream.data() + offset + 4;
        std::string file_identifier;
        const std::string* table_name = required_table;
        if (required_file_identifier) {
            file_identifier = *required_file_identifier;
        } else {
            if (record_size < 8) {
                if (error) *error = "stored FlatBuffer record is too short";
                return false;
            }
            file_identifier.assign(
                reinterpret_cast<const char*>(record + 4),
                4);
            table_name = boundTableName(bindings, file_identifier);
            if (!table_name) {
                if (error) {
                    *error = "stored record has an unconfigured file identifier";
                }
                return false;
            }
        }
        if (!verifyFlatBufferRecord(
                stream.data() + offset,
                static_cast<size_t>(record_size) + 4,
                file_identifier,
                *table_name,
                reflection_schema,
                error)) {
            return false;
        }
        offset += static_cast<size_t>(record_size) + 4;
        ++records;
    }
    if (record_count) *record_count = records;
    return true;
}

void* createConfiguredDatabase(
    const std::string& schema_idl,
    const std::string& database_name,
    const std::vector<std::pair<std::string, std::string>>& bindings,
    std::vector<uint8_t>* reflection_schema,
    std::string* error) {
    std::vector<uint8_t> compiled_reflection_schema;
    if (!compileRecordVerifier(
            schema_idl,
            bindings,
            &compiled_reflection_schema,
            error)) {
        return nullptr;
    }
    void* database = flatsql_create_db(
        schema_idl.c_str(),
        database_name.empty() ? "default" : database_name.c_str());
    if (!database) {
        const char* engine_error = flatsql_get_error();
        if (error) {
            *error = engine_error ? engine_error : "FlatSQL rejected SCHEMA_IDL";
        }
        return nullptr;
    }
    for (const auto& [file_identifier, table_name] : bindings) {
        flatsql_register_file_id(
            database,
            file_identifier.c_str(),
            table_name.c_str());
    }
    flatsql_enable_demo_extractors(database);
    if (reflection_schema) {
        *reflection_schema = std::move(compiled_reflection_schema);
    }
    return database;
}

std::string quoteIdentifier(const std::string& identifier) {
    std::string quoted = "\"";
    for (const char value : identifier) {
        if (value == '\"') quoted.push_back('\"');
        quoted.push_back(value);
    }
    quoted.push_back('\"');
    return quoted;
}

bool queryRaw(
    void* database,
    const std::string& sql,
    const std::vector<uint8_t>& parameters,
    uint32_t parameter_count,
    std::vector<uint8_t>* output,
    uint64_t* row_count,
    uint32_t* column_count,
    std::string* error) {
    if (!flatsql_query_raw_flatbuffer_stream(
            database,
            sql.c_str(),
            parameters.empty() ? nullptr : parameters.data(),
            parameters.size(),
            static_cast<int>(parameter_count))) {
        const char* engine_error = flatsql_get_error();
        if (error) *error = engine_error ? engine_error : "FlatSQL query failed";
        return false;
    }
    const int result_size = flatsql_response_artifact_size();
    const uint8_t* result_data = flatsql_response_artifact_data();
    output->clear();
    if (result_size > 0 && result_data) {
        output->assign(result_data, result_data + result_size);
    }
    if (row_count) {
        *row_count = static_cast<uint64_t>(flatsql_response_artifact_row_count());
    }
    if (column_count) {
        *column_count =
            static_cast<uint32_t>(flatsql_response_artifact_column_count());
    }
    return true;
}

uint32_t readU32(const uint8_t* data) {
    return static_cast<uint32_t>(data[0]) |
        (static_cast<uint32_t>(data[1]) << 8u) |
        (static_cast<uint32_t>(data[2]) << 16u) |
        (static_cast<uint32_t>(data[3]) << 24u);
}

uint64_t readU64(const uint8_t* data) {
    uint64_t value = 0;
    for (unsigned shift = 0; shift < 64; shift += 8) {
        value |= static_cast<uint64_t>(data[shift / 8]) << shift;
    }
    return value;
}

void appendU32(std::vector<uint8_t>* output, uint32_t value) {
    for (unsigned shift = 0; shift < 32; shift += 8) {
        output->push_back(static_cast<uint8_t>((value >> shift) & 0xffu));
    }
}

void appendU64(std::vector<uint8_t>* output, uint64_t value) {
    for (unsigned shift = 0; shift < 64; shift += 8) {
        output->push_back(static_cast<uint8_t>((value >> shift) & 0xffu));
    }
}

void appendString(std::vector<uint8_t>* output, const std::string& value) {
    appendU32(output, static_cast<uint32_t>(value.size()));
    output->insert(output->end(), value.begin(), value.end());
}

void appendBytes(std::vector<uint8_t>* output, const std::vector<uint8_t>& value) {
    appendU64(output, value.size());
    output->insert(output->end(), value.begin(), value.end());
}

struct HostcallResponse {
    std::string meta;
    std::vector<std::vector<uint8_t>> segments;
};

size_t skipJsonWhitespace(const std::string& json, size_t cursor) {
    while (cursor < json.size()) {
        const char value = json[cursor];
        if (value != ' ' && value != '\t' && value != '\r' && value != '\n') {
            break;
        }
        ++cursor;
    }
    return cursor;
}

size_t findJsonValue(const std::string& json, const char* key) {
    const std::string marker = std::string("\"") + key + "\"";
    const size_t key_position = json.find(marker);
    if (key_position == std::string::npos) return key_position;
    const size_t colon = json.find(':', key_position + marker.size());
    if (colon == std::string::npos) return colon;
    return skipJsonWhitespace(json, colon + 1);
}

bool findJsonBool(const std::string& json, const char* key, bool* output) {
    if (!output) return false;
    const size_t cursor = findJsonValue(json, key);
    if (cursor == std::string::npos) return false;
    if (json.compare(cursor, 4, "true") == 0) {
        *output = true;
        return true;
    }
    if (json.compare(cursor, 5, "false") == 0) {
        *output = false;
        return true;
    }
    return false;
}

bool findJsonString(
    const std::string& json,
    const char* key,
    std::string* output) {
    if (!output) return false;
    size_t cursor = findJsonValue(json, key);
    if (cursor == std::string::npos || cursor >= json.size() ||
        json[cursor] != '"') {
        return false;
    }
    ++cursor;
    const size_t end = json.find('"', cursor);
    if (end == std::string::npos) return false;
    output->assign(json.data() + cursor, end - cursor);
    return true;
}

bool findJsonStringArray(
    const std::string& json,
    const char* key,
    std::vector<std::string>* output) {
    if (!output) return false;
    size_t cursor = findJsonValue(json, key);
    if (cursor == std::string::npos || cursor >= json.size() ||
        json[cursor] != '[') {
        return false;
    }
    output->clear();
    cursor = skipJsonWhitespace(json, cursor + 1);
    while (cursor < json.size() && json[cursor] != ']') {
        if (json[cursor] != '"') return false;
        ++cursor;
        std::string value;
        while (cursor < json.size() && json[cursor] != '"') {
            if (json[cursor] == '\\') return false;
            value.push_back(json[cursor]);
            ++cursor;
        }
        if (cursor >= json.size()) return false;
        output->push_back(std::move(value));
        cursor = skipJsonWhitespace(json, cursor + 1);
        if (cursor < json.size() && json[cursor] == ',') {
            cursor = skipJsonWhitespace(json, cursor + 1);
        } else if (cursor >= json.size() || json[cursor] != ']') {
            return false;
        }
    }
    return cursor < json.size() && json[cursor] == ']';
}

bool findJsonBinaryReference(
    const std::string& json,
    const char* key,
    size_t* output) {
    if (!output) return false;
    size_t cursor = findJsonValue(json, key);
    if (cursor == std::string::npos || cursor >= json.size() ||
        json[cursor] != '{') {
        return false;
    }
    cursor = skipJsonWhitespace(json, cursor + 1);
    constexpr const char* kBinaryKey = "\"$bin\"";
    constexpr size_t kBinaryKeyLength = 6;
    if (json.compare(cursor, kBinaryKeyLength, kBinaryKey) != 0) return false;
    cursor = skipJsonWhitespace(json, cursor + kBinaryKeyLength);
    if (cursor >= json.size() || json[cursor] != ':') return false;
    cursor = skipJsonWhitespace(json, cursor + 1);
    if (cursor >= json.size() || json[cursor] < '0' || json[cursor] > '9') {
        return false;
    }
    size_t index = 0;
    while (cursor < json.size() && json[cursor] >= '0' && json[cursor] <= '9') {
        index = index * 10 + static_cast<size_t>(json[cursor] - '0');
        ++cursor;
    }
    cursor = skipJsonWhitespace(json, cursor);
    if (cursor >= json.size() || json[cursor] != '}') return false;
    *output = index;
    return true;
}

bool decodeBase64(const std::string& encoded, std::vector<uint8_t>* output) {
    if (!output || encoded.size() % 4 != 0) return false;
    std::array<int8_t, 256> table{};
    table.fill(-1);
    constexpr const char* kAlphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for (int index = 0; index < 64; ++index) {
        table[static_cast<uint8_t>(kAlphabet[index])] =
            static_cast<int8_t>(index);
    }
    output->clear();
    output->reserve(encoded.size() / 4 * 3);
    for (size_t offset = 0; offset < encoded.size(); offset += 4) {
        const bool final_group = offset + 4 == encoded.size();
        const bool pad_third = encoded[offset + 2] == '=';
        const bool pad_fourth = encoded[offset + 3] == '=';
        if ((!final_group && (pad_third || pad_fourth)) ||
            (pad_third && !pad_fourth)) {
            return false;
        }
        const int first = table[static_cast<uint8_t>(encoded[offset])];
        const int second = table[static_cast<uint8_t>(encoded[offset + 1])];
        const int third = pad_third
            ? 0
            : table[static_cast<uint8_t>(encoded[offset + 2])];
        const int fourth = pad_fourth
            ? 0
            : table[static_cast<uint8_t>(encoded[offset + 3])];
        if (first < 0 || second < 0 || third < 0 || fourth < 0) return false;
        const uint32_t value =
            (static_cast<uint32_t>(first) << 18u) |
            (static_cast<uint32_t>(second) << 12u) |
            (static_cast<uint32_t>(third) << 6u) |
            static_cast<uint32_t>(fourth);
        output->push_back(static_cast<uint8_t>(value >> 16u));
        if (!pad_third) output->push_back(static_cast<uint8_t>(value >> 8u));
        if (!pad_fourth) output->push_back(static_cast<uint8_t>(value));
    }
    return true;
}

bool parseHostcallEnvelope(
    const std::vector<uint8_t>& envelope,
    HostcallResponse* output) {
    if (!output || envelope.size() < 8) return false;
    size_t offset = 0;
    const uint32_t meta_length = readU32(envelope.data());
    offset += 4;
    if (meta_length > envelope.size() - offset - 4) return false;
    output->meta.assign(
        reinterpret_cast<const char*>(envelope.data() + offset),
        meta_length);
    offset += meta_length;
    const uint32_t segment_count = readU32(envelope.data() + offset);
    offset += 4;
    if (segment_count > (envelope.size() - offset) / 4) return false;
    output->segments.clear();
    output->segments.reserve(segment_count);
    for (uint32_t index = 0; index < segment_count; ++index) {
        if (offset + 4 > envelope.size()) return false;
        const uint32_t segment_length = readU32(envelope.data() + offset);
        offset += 4;
        if (segment_length > envelope.size() - offset) return false;
        output->segments.emplace_back(
            envelope.begin() + static_cast<std::ptrdiff_t>(offset),
            envelope.begin() +
                static_cast<std::ptrdiff_t>(offset + segment_length));
        offset += segment_length;
    }
    return offset == envelope.size();
}

bool callHost(
    const char* operation,
    const std::string& meta,
    const std::vector<uint8_t>* segment,
    HostcallResponse* response,
    std::string* error) {
    std::vector<uint8_t> request;
    request.reserve(
        8 + meta.size() + (segment ? 4 + segment->size() : 0));
    appendU32(&request, static_cast<uint32_t>(meta.size()));
    request.insert(request.end(), meta.begin(), meta.end());
    appendU32(&request, segment ? 1 : 0);
    if (segment) {
        appendU32(&request, static_cast<uint32_t>(segment->size()));
        request.insert(request.end(), segment->begin(), segment->end());
    }

    sdm_host_clear_response();
    const int32_t call_result = sdm_host_call(
        operation,
        static_cast<int32_t>(std::strlen(operation)),
        reinterpret_cast<const char*>(request.data()),
        static_cast<int32_t>(request.size()));
    const int32_t host_status = sdm_host_last_status_code();
    const int32_t response_length = sdm_host_response_len();
    if (response_length < 8 || response_length > kMaxHostcallResponseBytes) {
        sdm_host_clear_response();
        if (error) {
            *error = "opaque storage adapter response violates size bounds";
        }
        return false;
    }
    std::vector<uint8_t> response_bytes(
        static_cast<size_t>(response_length),
        0);
    const int32_t copied = sdm_host_read_response(
        reinterpret_cast<char*>(response_bytes.data()),
        response_length);
    sdm_host_clear_response();
    bool ok = false;
    if (call_result != 0 || host_status != 0 || copied != response_length ||
        !parseHostcallEnvelope(response_bytes, response) ||
        !findJsonBool(response->meta, "ok", &ok) || !ok) {
        if (error) *error = "opaque storage adapter rejected the operation";
        return false;
    }
    return true;
}

struct DurableManifest {
    uint64_t generation = 0;
    uint64_t total_size = 0;
    uint32_t chunk_size = 0;
    uint32_t chunk_count = 0;
    std::vector<uint8_t> digest;
};

std::string durableChunkKey(uint64_t generation, uint32_t chunk_index) {
    return "snapshot.g" + std::to_string(generation) + ".c" +
        std::to_string(chunk_index) + ".bin";
}

std::vector<uint8_t> encodeDurableManifest(
    const DurableManifest& manifest) {
    std::vector<uint8_t> bytes;
    bytes.reserve(60);
    bytes.insert(
        bytes.end(),
        kOpaqueManifestIdentifier,
        kOpaqueManifestIdentifier + 4);
    appendU64(&bytes, manifest.generation);
    appendU64(&bytes, manifest.total_size);
    appendU32(&bytes, manifest.chunk_size);
    appendU32(&bytes, manifest.chunk_count);
    bytes.insert(bytes.end(), manifest.digest.begin(), manifest.digest.end());
    return bytes;
}

bool parseDurableManifest(
    const std::vector<uint8_t>& bytes,
    DurableManifest* manifest,
    std::string* error) {
    if (!manifest || bytes.size() != 60 ||
        std::memcmp(bytes.data(), kOpaqueManifestIdentifier, 4) != 0) {
        if (error) *error = "opaque snapshot manifest is invalid";
        return false;
    }
    manifest->generation = readU64(bytes.data() + 4);
    manifest->total_size = readU64(bytes.data() + 12);
    manifest->chunk_size = readU32(bytes.data() + 20);
    manifest->chunk_count = readU32(bytes.data() + 24);
    manifest->digest.assign(bytes.begin() + 28, bytes.end());
    if (manifest->generation == 0 ||
        manifest->chunk_size != kOpaqueChunkBytes ||
        manifest->total_size > kMaxOpaqueSnapshotBytes ||
        manifest->total_size > std::numeric_limits<size_t>::max()) {
        if (error) *error = "opaque snapshot manifest fields are invalid";
        return false;
    }
    const uint64_t expected_chunks = manifest->total_size == 0
        ? 0
        : ((manifest->total_size - 1) / manifest->chunk_size) + 1;
    if (expected_chunks > std::numeric_limits<uint32_t>::max() ||
        manifest->chunk_count != expected_chunks ||
        manifest->digest.size() != 32) {
        if (error) *error = "opaque snapshot manifest chunk layout is invalid";
        return false;
    }
    return true;
}

bool readOpaqueValue(
    const std::string& key,
    std::vector<uint8_t>* value,
    bool* found,
    std::string* error) {
    if (!value || !found) return false;
    HostcallResponse response;
    const std::string meta =
        std::string("{\"namespace\":\"") + kOpaqueNamespace +
        "\",\"key\":\"" + key + "\"}";
    if (!callHost(
            "storage.adapter.opaque.read",
            meta,
            nullptr,
            &response,
            error) ||
        !findJsonBool(response.meta, "found", found)) {
        if (error && error->empty()) {
            *error = "opaque snapshot response is missing found state";
        }
        return false;
    }
    value->clear();
    if (!*found) return true;

    size_t segment_index = 0;
    if (findJsonBinaryReference(
            response.meta,
            "bytes_b64",
            &segment_index)) {
        if (segment_index >= response.segments.size()) {
            if (error) *error = "opaque snapshot references missing bytes";
            return false;
        }
        *value = std::move(response.segments[segment_index]);
        return true;
    }
    std::string encoded;
    if (!findJsonString(response.meta, "bytes_b64", &encoded) ||
        !decodeBase64(encoded, value)) {
        if (error) *error = "opaque snapshot bytes are not valid base64";
        return false;
    }
    return true;
}

bool replaceOpaqueValue(
    const std::string& key,
    const std::vector<uint8_t>& value,
    std::string* error) {
    HostcallResponse response;
    const std::string replace_meta =
        std::string("{\"namespace\":\"") + kOpaqueNamespace +
        "\",\"key\":\"" + key +
        "\",\"data\":{\"$bin\":0}}";
    return callHost(
        "storage.adapter.opaque.replace",
        replace_meta,
        &value,
        &response,
        error);
}

bool deleteOpaqueValue(const std::string& key, std::string* error) {
    HostcallResponse response;
    const std::string meta =
        std::string("{\"namespace\":\"") + kOpaqueNamespace +
        "\",\"key\":\"" + key + "\"}";
    return callHost(
        "storage.adapter.opaque.delete",
        meta,
        nullptr,
        &response,
        error);
}

bool syncOpaqueState(std::string* error) {
    HostcallResponse response;
    const std::string sync_meta =
        std::string("{\"namespace\":\"") + kOpaqueNamespace + "\"}";
    return callHost(
        "storage.adapter.opaque.sync",
        sync_meta,
        nullptr,
        &response,
        error);
}

bool listOpaqueValues(
    std::vector<std::string>* keys,
    std::string* error) {
    if (!keys) return false;
    HostcallResponse response;
    const std::string meta =
        std::string("{\"namespace\":\"") + kOpaqueNamespace + "\"}";
    if (!callHost(
            "storage.adapter.opaque.list",
            meta,
            nullptr,
            &response,
            error) ||
        !findJsonStringArray(response.meta, "keys", keys)) {
        if (error && error->empty()) {
            *error = "opaque storage adapter list response is invalid";
        }
        return false;
    }
    return true;
}

bool isDurableChunkKey(const std::string& key) {
    static const std::regex pattern(
        R"(^snapshot\.g[0-9]+\.c[0-9]+\.bin$)");
    return std::regex_match(key, pattern);
}

bool deleteOpaqueValues(
    const std::vector<std::string>& keys,
    std::string* error) {
    if (keys.empty()) return true;
    for (const auto& key : keys) {
        if (!deleteOpaqueValue(key, error)) return false;
    }
    return syncOpaqueState(error);
}

bool sweepOrphanDurableChunks(
    const std::vector<std::string>& committed_keys,
    std::string* error) {
    std::vector<std::string> stored_keys;
    if (!listOpaqueValues(&stored_keys, error)) return false;
    const std::set<std::string> committed(
        committed_keys.begin(),
        committed_keys.end());
    std::vector<std::string> orphaned;
    for (const auto& key : stored_keys) {
        if (isDurableChunkKey(key) && committed.count(key) == 0) {
            orphaned.push_back(key);
        }
    }
    return deleteOpaqueValues(orphaned, error);
}

bool readDurableSnapshot(
    std::vector<uint8_t>* snapshot,
    bool* found,
    uint64_t* generation,
    std::vector<uint8_t>* manifest_bytes,
    std::vector<std::string>* chunk_keys,
    std::string* error) {
    if (!snapshot || !found || !generation || !manifest_bytes || !chunk_keys) {
        return false;
    }
    if (!readOpaqueValue(
            kOpaqueManifestKey,
            manifest_bytes,
            found,
            error)) {
        return false;
    }
    snapshot->clear();
    chunk_keys->clear();
    *generation = 0;
    if (!*found) return true;

    DurableManifest manifest;
    if (!parseDurableManifest(*manifest_bytes, &manifest, error)) return false;
    snapshot->reserve(static_cast<size_t>(manifest.total_size));
    chunk_keys->reserve(manifest.chunk_count);
    uint64_t remaining = manifest.total_size;
    for (uint32_t index = 0; index < manifest.chunk_count; ++index) {
        const std::string key = durableChunkKey(manifest.generation, index);
        std::vector<uint8_t> chunk;
        bool chunk_found = false;
        if (!readOpaqueValue(key, &chunk, &chunk_found, error)) return false;
        const size_t expected_size = static_cast<size_t>(std::min<uint64_t>(
            remaining,
            manifest.chunk_size));
        if (!chunk_found || chunk.size() != expected_size) {
            if (error) *error = "opaque snapshot chunk is missing or truncated";
            return false;
        }
        snapshot->insert(snapshot->end(), chunk.begin(), chunk.end());
        chunk_keys->push_back(key);
        remaining -= chunk.size();
    }
    if (remaining != 0 || snapshot->size() != manifest.total_size ||
        sha256(*snapshot) != manifest.digest) {
        if (error) *error = "opaque snapshot digest does not match its chunks";
        return false;
    }
    *generation = manifest.generation;
    return true;
}

class SnapshotReader {
public:
    explicit SnapshotReader(const std::vector<uint8_t>& bytes)
        : bytes_(bytes) {}

    bool readU32Value(uint32_t* output) {
        if (!require(4)) return false;
        *output = readU32(bytes_.data() + offset_);
        offset_ += 4;
        return true;
    }

    bool readU64Value(uint64_t* output) {
        if (!require(8)) return false;
        uint64_t value = 0;
        for (unsigned shift = 0; shift < 64; shift += 8) {
            value |= static_cast<uint64_t>(bytes_[offset_++]) << shift;
        }
        *output = value;
        return true;
    }

    bool readStringValue(std::string* output) {
        uint32_t size = 0;
        if (!readU32Value(&size) || !require(size)) return false;
        output->assign(
            reinterpret_cast<const char*>(bytes_.data() + offset_),
            size);
        offset_ += size;
        return true;
    }

    bool readBytesValue(std::vector<uint8_t>* output) {
        uint64_t size = 0;
        if (!readU64Value(&size) ||
            size > std::numeric_limits<size_t>::max() ||
            !require(static_cast<size_t>(size))) {
            return false;
        }
        output->assign(
            bytes_.begin() + static_cast<std::ptrdiff_t>(offset_),
            bytes_.begin() + static_cast<std::ptrdiff_t>(offset_ + size));
        offset_ += static_cast<size_t>(size);
        return true;
    }

    bool finished() const { return offset_ == bytes_.size(); }

private:
    bool require(size_t size) const {
        return size <= bytes_.size() - offset_;
    }

    const std::vector<uint8_t>& bytes_;
    size_t offset_ = 0;
};

std::vector<uint8_t> createSnapshotBytes() {
    const uint8_t* export_data = flatsql_export_data(g_database);
    const int export_size = flatsql_export_size();
    std::vector<uint8_t> storage;
    if (export_size > 0 && export_data) {
        storage.assign(export_data, export_data + export_size);
    }
    std::vector<uint8_t> snapshot_bytes{'F', 'S', 'N', '1'};
    appendU32(&snapshot_bytes, 1);
    appendString(&snapshot_bytes, g_schema_idl);
    appendString(&snapshot_bytes, g_database_name);
    appendU32(&snapshot_bytes, static_cast<uint32_t>(g_table_bindings.size()));
    for (const auto& [file_identifier, table_name] : g_table_bindings) {
        appendString(&snapshot_bytes, file_identifier);
        appendString(&snapshot_bytes, table_name);
    }
    appendU32(&snapshot_bytes, static_cast<uint32_t>(g_views.size()));
    for (const auto& [name, definition] : g_views) {
        appendString(&snapshot_bytes, name);
        appendString(&snapshot_bytes, definition.query);
        appendString(&snapshot_bytes, definition.key_expression);
    }
    appendU64(&snapshot_bytes, g_retention_max_records);
    appendU64(&snapshot_bytes, g_retention_max_age_millis);
    appendU64(&snapshot_bytes, g_compaction_target_bytes);
    appendBytes(&snapshot_bytes, storage);
    return snapshot_bytes;
}

struct SnapshotState {
    std::string schema_idl;
    std::string database_name;
    std::vector<std::pair<std::string, std::string>> table_bindings;
    std::map<std::string, ViewDefinition> views;
    uint64_t retention_max_records = 0;
    uint64_t retention_max_age_millis = 0;
    uint64_t compaction_target_bytes = 0;
    std::vector<uint8_t> storage;
};

bool parseSnapshotBytes(
    const std::vector<uint8_t>& bytes,
    SnapshotState* state,
    std::string* error) {
    if (bytes.size() < 8 ||
        std::memcmp(bytes.data(), "FSN1", 4) != 0) {
        if (error) *error = "FlatSQL node snapshot magic is invalid";
        return false;
    }
    std::vector<uint8_t> body(bytes.begin() + 4, bytes.end());
    SnapshotReader reader(body);
    uint32_t version = 0;
    if (!reader.readU32Value(&version) || version != 1 ||
        !reader.readStringValue(&state->schema_idl) ||
        !reader.readStringValue(&state->database_name)) {
        if (error) *error = "FlatSQL node snapshot header is invalid";
        return false;
    }
    uint32_t binding_count = 0;
    if (!reader.readU32Value(&binding_count) || binding_count > 4096) {
        if (error) *error = "FlatSQL node snapshot binding count is invalid";
        return false;
    }
    for (uint32_t index = 0; index < binding_count; ++index) {
        std::string file_identifier;
        std::string table_name;
        if (!reader.readStringValue(&file_identifier) ||
            !reader.readStringValue(&table_name)) {
            if (error) *error = "FlatSQL node snapshot binding is truncated";
            return false;
        }
        state->table_bindings.emplace_back(
            std::move(file_identifier),
            std::move(table_name));
    }
    uint32_t view_count = 0;
    if (!reader.readU32Value(&view_count) || view_count > 4096) {
        if (error) *error = "FlatSQL node snapshot view count is invalid";
        return false;
    }
    for (uint32_t index = 0; index < view_count; ++index) {
        std::string name;
        ViewDefinition definition;
        if (!reader.readStringValue(&name) ||
            !reader.readStringValue(&definition.query) ||
            !reader.readStringValue(&definition.key_expression)) {
            if (error) *error = "FlatSQL node snapshot view is truncated";
            return false;
        }
        state->views.emplace(std::move(name), std::move(definition));
    }
    if (!reader.readU64Value(&state->retention_max_records) ||
        !reader.readU64Value(&state->retention_max_age_millis) ||
        !reader.readU64Value(&state->compaction_target_bytes) ||
        !reader.readBytesValue(&state->storage) ||
        !reader.finished()) {
        if (error) *error = "FlatSQL node snapshot payload is truncated";
        return false;
    }
    return true;
}

bool installSnapshotBytes(
    const std::vector<uint8_t>& snapshot_bytes,
    std::string* error_code,
    std::string* error) {
    SnapshotState state;
    if (!parseSnapshotBytes(snapshot_bytes, &state, error)) {
        if (error_code) *error_code = "snapshot-invalid";
        return false;
    }
    std::vector<uint8_t> replacement_reflection_schema;
    void* replacement = createConfiguredDatabase(
        state.schema_idl,
        state.database_name,
        state.table_bindings,
        &replacement_reflection_schema,
        error);
    if (!replacement) {
        if (error_code) *error_code = "snapshot-schema-invalid";
        return false;
    }
    uint64_t stored_records = 0;
    if (!verifyRecordStream(
            state.storage,
            nullptr,
            nullptr,
            state.table_bindings,
            replacement_reflection_schema,
            &stored_records,
            error)) {
        flatsql_destroy_db(replacement);
        if (error_code) *error_code = "snapshot-record-stream-invalid";
        return false;
    }
    flatsql_load_and_rebuild(
        replacement,
        state.storage.empty() ? nullptr : state.storage.data(),
        state.storage.size());
    if (g_database) flatsql_destroy_db(g_database);
    g_database = replacement;
    g_schema_idl = std::move(state.schema_idl);
    g_database_name = std::move(state.database_name);
    g_table_bindings = std::move(state.table_bindings);
    g_reflection_schema = std::move(replacement_reflection_schema);
    g_views = std::move(state.views);
    g_retention_max_records = state.retention_max_records;
    g_retention_max_age_millis = state.retention_max_age_millis;
    g_compaction_target_bytes = state.compaction_target_bytes;
    return true;
}

bool ensureDurableStateLoaded(std::string* error) {
    if (g_durable_state_poisoned) {
        if (error) *error = g_durable_poison_error;
        return false;
    }
    if (g_durable_state_checked) return true;
    std::vector<uint8_t> snapshot_bytes;
    std::vector<uint8_t> manifest_bytes;
    std::vector<std::string> chunk_keys;
    bool found = false;
    uint64_t generation = 0;
    if (!readDurableSnapshot(
            &snapshot_bytes,
            &found,
            &generation,
            &manifest_bytes,
            &chunk_keys,
            error)) {
        return false;
    }
    if (found) {
        std::string error_code;
        if (!installSnapshotBytes(snapshot_bytes, &error_code, error)) {
            if (error && !error_code.empty()) {
                *error = error_code + ": " + *error;
            }
            return false;
        }
    }
    g_durable_manifest_found = found;
    g_durable_generation = generation;
    g_durable_manifest = std::move(manifest_bytes);
    g_durable_chunk_keys = std::move(chunk_keys);
    g_durable_state_checked = true;
    std::string sweep_error;
    sweepOrphanDurableChunks(g_durable_chunk_keys, &sweep_error);
    return true;
}

bool prepareDurableStateForExplicitReload(std::string* error) {
    if (g_durable_state_checked && !g_durable_state_poisoned) return true;
    std::vector<uint8_t> manifest_bytes;
    bool found = false;
    if (!readOpaqueValue(
            kOpaqueManifestKey,
            &manifest_bytes,
            &found,
            error)) {
        return false;
    }
    uint64_t generation = 0;
    std::vector<std::string> chunk_keys;
    if (found) {
        DurableManifest manifest;
        std::string manifest_error;
        if (parseDurableManifest(
                manifest_bytes,
                &manifest,
                &manifest_error)) {
            generation = manifest.generation;
            chunk_keys.reserve(manifest.chunk_count);
            for (uint32_t index = 0; index < manifest.chunk_count; ++index) {
                chunk_keys.push_back(durableChunkKey(generation, index));
            }
        }
    }
    g_durable_manifest_found = found;
    g_durable_generation = generation;
    g_durable_manifest = std::move(manifest_bytes);
    g_durable_chunk_keys = std::move(chunk_keys);
    return true;
}

void bestEffortDeleteOpaqueValues(const std::vector<std::string>& keys) {
    std::string ignored;
    deleteOpaqueValues(keys, &ignored);
}

bool persistDurableState(std::string* error) {
    if (!g_database) {
        if (error) *error = "FlatSQL database is not configured";
        return false;
    }
    if (g_durable_generation == std::numeric_limits<uint64_t>::max()) {
        if (error) *error = "opaque snapshot generation is exhausted";
        return false;
    }
    const std::vector<uint8_t> snapshot_bytes = createSnapshotBytes();
    DurableManifest next;
    next.generation = g_durable_generation + 1;
    next.total_size = snapshot_bytes.size();
    next.chunk_size = static_cast<uint32_t>(kOpaqueChunkBytes);
    next.chunk_count = snapshot_bytes.empty()
        ? 0
        : static_cast<uint32_t>(
            (snapshot_bytes.size() - 1) / kOpaqueChunkBytes + 1);
    next.digest = sha256(snapshot_bytes);

    std::vector<std::string> next_chunk_keys;
    next_chunk_keys.reserve(next.chunk_count);
    for (uint32_t index = 0; index < next.chunk_count; ++index) {
        const size_t offset = static_cast<size_t>(index) * kOpaqueChunkBytes;
        const size_t length = std::min(
            kOpaqueChunkBytes,
            snapshot_bytes.size() - offset);
        const std::string key = durableChunkKey(next.generation, index);
        next_chunk_keys.push_back(key);
        const std::vector<uint8_t> chunk(
            snapshot_bytes.begin() + static_cast<std::ptrdiff_t>(offset),
            snapshot_bytes.begin() +
                static_cast<std::ptrdiff_t>(offset + length));
        if (!replaceOpaqueValue(key, chunk, error)) {
            const std::string primary_error = error ? *error : std::string();
            bestEffortDeleteOpaqueValues(next_chunk_keys);
            if (error) *error = primary_error;
            return false;
        }
    }
    if (!syncOpaqueState(error)) {
        const std::string primary_error = error ? *error : std::string();
        bestEffortDeleteOpaqueValues(next_chunk_keys);
        if (error) *error = primary_error;
        return false;
    }

    const std::vector<uint8_t> next_manifest = encodeDurableManifest(next);
    if (!replaceOpaqueValue(kOpaqueManifestKey, next_manifest, error)) {
        const std::string primary_error = error ? *error : std::string();
        bestEffortDeleteOpaqueValues(next_chunk_keys);
        if (error) *error = primary_error;
        return false;
    }
    if (!syncOpaqueState(error)) {
        const std::string primary_error = error ? *error : std::string();
        std::string rollback_error;
        const bool manifest_restored = g_durable_manifest_found
            ? replaceOpaqueValue(
                kOpaqueManifestKey,
                g_durable_manifest,
                &rollback_error)
            : deleteOpaqueValue(kOpaqueManifestKey, &rollback_error);
        const bool rollback_synced = manifest_restored &&
            syncOpaqueState(&rollback_error);
        if (rollback_synced) {
            bestEffortDeleteOpaqueValues(next_chunk_keys);
        } else {
            g_durable_state_checked = false;
            g_durable_state_poisoned = true;
            g_durable_poison_error =
                "opaque snapshot commit is indeterminate after manifest rollback failure";
        }
        if (error) {
            *error = primary_error;
            if (!rollback_synced) {
                *error += "; manifest rollback failed: " + rollback_error;
            }
        }
        return false;
    }

    const std::vector<std::string> previous_chunk_keys =
        g_durable_chunk_keys;
    g_durable_manifest_found = true;
    g_durable_generation = next.generation;
    g_durable_manifest = next_manifest;
    g_durable_chunk_keys = next_chunk_keys;
    g_durable_state_checked = true;
    g_durable_state_poisoned = false;
    g_durable_poison_error.clear();
    bestEffortDeleteOpaqueValues(previous_chunk_keys);
    return true;
}

struct InMemoryRollbackState {
    bool configured = false;
    std::vector<uint8_t> snapshot;
};

InMemoryRollbackState captureInMemoryState() {
    InMemoryRollbackState state;
    state.configured = g_database != nullptr;
    if (state.configured) state.snapshot = createSnapshotBytes();
    return state;
}

void clearInMemoryState() {
    if (g_database) flatsql_destroy_db(g_database);
    g_database = nullptr;
    g_schema_idl.clear();
    g_database_name.clear();
    g_table_bindings.clear();
    g_reflection_schema.clear();
    g_views.clear();
    g_retention_max_records = 0;
    g_retention_max_age_millis = 0;
    g_compaction_target_bytes = 0;
}

bool restoreInMemoryState(
    const InMemoryRollbackState& state,
    std::string* error) {
    if (!state.configured) {
        clearInMemoryState();
        return true;
    }
    std::string error_code;
    return installSnapshotBytes(state.snapshot, &error_code, error);
}

int durableStateFailure(
    flatSqlNodeOperation operation,
    uint64_t request_id,
    uint32_t wire_format,
    const char* error_code,
    const std::string& error) {
    return pushStatus(
        operation,
        request_id,
        flatSqlNodeStatus_INTERNAL_ERROR,
        0,
        0,
        error_code,
        error,
        wire_format);
}

int durableMutationFailure(
    flatSqlNodeOperation operation,
    uint64_t request_id,
    uint32_t wire_format,
    const InMemoryRollbackState& rollback,
    const std::string& persistence_error) {
    std::string message = persistence_error;
    std::string rollback_error;
    if (!restoreInMemoryState(rollback, &rollback_error)) {
        message += "; in-memory rollback failed: " + rollback_error;
    }
    return durableStateFailure(
        operation,
        request_id,
        wire_format,
        "durable-state-persist-failed",
        message);
}

bool trimRecordStream(
    const std::vector<uint8_t>& source,
    uint64_t max_records,
    std::vector<uint8_t>* output,
    uint64_t* dropped,
    std::string* error) {
    struct Slice { size_t offset; size_t size; };
    std::vector<Slice> records;
    size_t offset = 0;
    while (offset < source.size()) {
        if (source.size() - offset < 4) {
            if (error) *error = "FlatSQL query returned a truncated size prefix";
            return false;
        }
        const uint32_t size = readU32(source.data() + offset);
        if (size > source.size() - offset - 4) {
            if (error) *error = "FlatSQL query returned a truncated record";
            return false;
        }
        records.push_back({offset, static_cast<size_t>(size) + 4});
        offset += static_cast<size_t>(size) + 4;
    }
    size_t first = 0;
    if (max_records > 0 && records.size() > max_records) {
        first = records.size() - static_cast<size_t>(max_records);
    }
    output->clear();
    for (size_t index = first; index < records.size(); ++index) {
        const auto& record = records[index];
        output->insert(
            output->end(),
            source.begin() + static_cast<std::ptrdiff_t>(record.offset),
            source.begin() + static_cast<std::ptrdiff_t>(record.offset + record.size));
    }
    if (dropped) *dropped += first;
    return true;
}

int invalidControl(
    flatSqlNodeOperation operation,
    uint64_t request_id,
    uint32_t wire_format,
    const std::string& message) {
    return pushStatus(
        operation,
        request_id,
        flatSqlNodeStatus_INVALID_ARGUMENT,
        0,
        0,
        "invalid-control-frame",
        message,
        wire_format);
}

bool configurationMatches(const ControlFrame& control) {
    const std::string database_name = control.database_name.empty()
        ? "default"
        : control.database_name;
    return g_database &&
        g_schema_idl == control.schema_idl &&
        g_database_name == database_name &&
        g_table_bindings == control.table_bindings;
}

bool validateConfiguration(
    const ControlFrame& control,
    std::string* error_code,
    std::string* error) {
    if (control.operation != flatSqlNodeOperation_CONFIGURE_INDEX ||
        control.schema_idl.empty() ||
        control.table_bindings.empty()) {
        if (error_code) *error_code = "invalid-control-frame";
        if (error) {
            *error = "configure_index requires schema IDL and table bindings";
        }
        return false;
    }
    if (!control.table_name.empty() ||
        !control.index_name.empty() ||
        !control.index_expression.empty()) {
        if (error_code) *error_code = "explicit-index-controls-unsupported";
        if (error) {
            *error = "indexes must be declared in SCHEMA_IDL; explicit index fields are unsupported";
        }
        return false;
    }
    return true;
}

void publishConfiguration(
    const ControlFrame& control,
    void* replacement,
    std::vector<uint8_t> replacement_reflection_schema) {
    if (g_database) flatsql_destroy_db(g_database);
    g_database = replacement;
    g_schema_idl = control.schema_idl;
    g_database_name = control.database_name.empty()
        ? "default"
        : control.database_name;
    g_table_bindings = control.table_bindings;
    g_reflection_schema = std::move(replacement_reflection_schema);
    g_views.clear();
    g_retention_max_records = 0;
    g_retention_max_age_millis = 0;
    g_compaction_target_bytes = 0;
}

bool applyConfiguration(
    const ControlFrame& control,
    bool preserve_matching_database,
    std::string* error_code,
    std::string* error) {
    if (!validateConfiguration(control, error_code, error)) return false;
    if (preserve_matching_database && g_database) {
        if (configurationMatches(control)) return true;
        if (error_code) *error_code = "configuration-mismatch";
        if (error) {
            *error = "append_records refuses to replace a configured database";
        }
        return false;
    }
    std::vector<uint8_t> replacement_reflection_schema;
    void* replacement = createConfiguredDatabase(
        control.schema_idl,
        control.database_name,
        control.table_bindings,
        &replacement_reflection_schema,
        error);
    if (!replacement) {
        if (error_code) *error_code = "schema-configuration-failed";
        return false;
    }
    publishConfiguration(
        control,
        replacement,
        std::move(replacement_reflection_schema));
    return true;
}

}  // namespace

extern "C" int configure_index(void) {
    const int32_t input_index = plugin_find_input_index("control", 0);
    const auto* frame = input_index >= 0
        ? plugin_get_input_frame(static_cast<uint32_t>(input_index))
        : nullptr;
    ControlFrame control;
    std::string error;
    if (!readControlFrame(frame, &control, &error)) {
        return invalidControl(
            flatSqlNodeOperation_CONFIGURE_INDEX,
            control.request_id,
            control.wire_format,
            error);
    }
    if (!ensureDurableStateLoaded(&error)) {
        return durableStateFailure(
            flatSqlNodeOperation_CONFIGURE_INDEX,
            control.request_id,
            control.wire_format,
            "durable-state-load-failed",
            error);
    }
    std::string error_code;
    const bool configuration_unchanged = configurationMatches(control);
    InMemoryRollbackState rollback;
    if (!configuration_unchanged) rollback = captureInMemoryState();
    if (!applyConfiguration(
            control,
            configuration_unchanged,
            &error_code,
            &error)) {
        return pushStatus(
            flatSqlNodeOperation_CONFIGURE_INDEX,
            control.request_id,
            flatSqlNodeStatus_INVALID_ARGUMENT,
            0,
            0,
            error_code.c_str(),
            error,
            control.wire_format);
    }
    if (!configuration_unchanged && !persistDurableState(&error)) {
        return durableMutationFailure(
            flatSqlNodeOperation_CONFIGURE_INDEX,
            control.request_id,
            control.wire_format,
            rollback,
            error);
    }
    return pushStatus(
        flatSqlNodeOperation_CONFIGURE_INDEX,
        control.request_id,
        flatSqlNodeStatus_COMPLETE,
        0,
        0,
        nullptr,
        "FlatSQL schema and table bindings configured",
        control.wire_format);
}

extern "C" int append_records(void) {
    const int32_t control_index = plugin_find_input_index("control", 0);
    const bool has_control = control_index >= 0;
    ControlFrame control;
    std::string error;
    if (has_control) {
        const auto* frame =
            plugin_get_input_frame(static_cast<uint32_t>(control_index));
        if (!readControlFrame(frame, &control, &error)) {
            return invalidControl(
                flatSqlNodeOperation_APPEND_RECORDS,
                control.request_id,
                control.wire_format,
                error);
        }
        if (plugin_find_input_index("control", 1) >= 0) {
            return invalidControl(
                flatSqlNodeOperation_APPEND_RECORDS,
                control.request_id,
                control.wire_format,
                "append_records accepts at most one configuration frame");
        }
        if (!ensureDurableStateLoaded(&error)) {
            return durableStateFailure(
                flatSqlNodeOperation_APPEND_RECORDS,
                control.request_id,
                control.wire_format,
                "durable-state-load-failed",
                error);
        }
        std::string error_code;
        if (!validateConfiguration(control, &error_code, &error) ||
            (g_database && !configurationMatches(control))) {
            if (error_code.empty()) {
                error_code = "configuration-mismatch";
                error = "append_records refuses to replace a configured database";
            }
            return pushStatus(
                flatSqlNodeOperation_APPEND_RECORDS,
                control.request_id,
                flatSqlNodeStatus_INVALID_ARGUMENT,
                0,
                0,
                error_code.c_str(),
                error,
                control.wire_format);
        }
    }

    std::vector<CollectedByteStream> streams;
    if (!collectByteStreams(
            "records",
            flatSqlByteStreamKind_RECORD_STREAM,
            &streams,
            &error)) {
        return invalidControl(
            flatSqlNodeOperation_APPEND_RECORDS,
            has_control ? control.request_id : 0,
            has_control ? control.wire_format : kFlatbufferWireFormat,
            error);
    }
    if (streams.empty()) {
        if (!has_control) {
            return invalidControl(
                flatSqlNodeOperation_APPEND_RECORDS,
                0,
                kFlatbufferWireFormat,
                "append_records requires control or records input");
        }
        std::string error_code;
        const bool configuration_unchanged = configurationMatches(control);
        InMemoryRollbackState rollback;
        if (!configuration_unchanged) rollback = captureInMemoryState();
        if (!applyConfiguration(
                control,
                true,
                &error_code,
                &error)) {
            return pushStatus(
                flatSqlNodeOperation_CONFIGURE_INDEX,
                control.request_id,
                flatSqlNodeStatus_INVALID_ARGUMENT,
                0,
                0,
                error_code.c_str(),
                error,
                control.wire_format);
        }
        if (!configuration_unchanged && !persistDurableState(&error)) {
            return durableMutationFailure(
                flatSqlNodeOperation_CONFIGURE_INDEX,
                control.request_id,
                control.wire_format,
                rollback,
                error);
        }
        return pushStatus(
            flatSqlNodeOperation_CONFIGURE_INDEX,
            control.request_id,
            flatSqlNodeStatus_COMPLETE,
            0,
            0,
            nullptr,
            "FlatSQL schema and table bindings configured",
            control.wire_format);
    }

    uint64_t request_id = 0;
    uint32_t wire_format = streams.front().wire_format;
    if (has_control) {
        request_id = control.request_id;
        wire_format = control.wire_format;
    } else {
        request_id = streams.front().request_id;
    }
    if (!has_control && !ensureDurableStateLoaded(&error)) {
        return durableStateFailure(
            flatSqlNodeOperation_APPEND_RECORDS,
            request_id,
            wire_format,
            "durable-state-load-failed",
            error);
    }
    if (!g_database && !has_control) {
        return pushStatus(
            flatSqlNodeOperation_APPEND_RECORDS,
            request_id,
            flatSqlNodeStatus_NOT_CONFIGURED,
            0,
            0,
            "database-not-configured",
            "configure_index must run before append_records",
            wire_format);
    }

    void* append_database = g_database;
    bool staged_database = false;
    std::vector<uint8_t> staged_reflection_schema;
    const auto* append_bindings = &g_table_bindings;
    const auto* append_reflection_schema = &g_reflection_schema;
    if (!append_database) {
        append_database = createConfiguredDatabase(
            control.schema_idl,
            control.database_name,
            control.table_bindings,
            &staged_reflection_schema,
            &error);
        if (!append_database) {
            return pushStatus(
                flatSqlNodeOperation_APPEND_RECORDS,
                request_id,
                flatSqlNodeStatus_INVALID_ARGUMENT,
                0,
                0,
                "schema-configuration-failed",
                error,
                wire_format);
        }
        staged_database = true;
        append_bindings = &control.table_bindings;
        append_reflection_schema = &staged_reflection_schema;
    }
    const auto reject_append = [&](const char* error_code,
                                   flatSqlNodeStatus status,
                                   const std::string& message) {
        if (staged_database) flatsql_destroy_db(append_database);
        return pushStatus(
            flatSqlNodeOperation_APPEND_RECORDS,
            request_id,
            status,
            0,
            0,
            error_code,
            message,
            wire_format);
    };

    uint64_t actual_records = 0;
    size_t total_size = 0;
    for (const auto& stream : streams) {
        if (!stream.sha256.empty() &&
            (stream.sha256.size() != 32 ||
             stream.sha256 != sha256(stream.data))) {
            return reject_append(
                "record-stream-checksum-invalid",
                flatSqlNodeStatus_INVALID_ARGUMENT,
                "FSB record stream SHA256 does not match DATA");
        }
        uint64_t stream_records = 0;
        if (!verifyRecordStream(
                stream.data,
                &stream.schema_name,
                &stream.file_identifier,
                *append_bindings,
                *append_reflection_schema,
                &stream_records,
                &error)) {
            return reject_append(
                "record-stream-invalid",
                flatSqlNodeStatus_INVALID_ARGUMENT,
                error);
        }
        if (stream.record_count != stream_records) {
            return reject_append(
                "record-count-mismatch",
                flatSqlNodeStatus_INVALID_ARGUMENT,
                "FSB RECORD_COUNT does not match the enclosed records");
        }
        if (stream.data.size() > std::numeric_limits<size_t>::max() - total_size) {
            return reject_append(
                "record-stream-size-overflow",
                flatSqlNodeStatus_RESOURCE_EXHAUSTED,
                "combined record streams exceed the node address space");
        }
        total_size += stream.data.size();
        actual_records += stream_records;
    }
    std::vector<uint8_t> combined_stream;
    combined_stream.reserve(total_size);
    for (const auto& stream : streams) {
        combined_stream.insert(
            combined_stream.end(),
            stream.data.begin(),
            stream.data.end());
    }
    const InMemoryRollbackState rollback = captureInMemoryState();
    const uint64_t consumed_bytes = static_cast<uint64_t>(flatsql_ingest(
        append_database,
        combined_stream.empty() ? nullptr : combined_stream.data(),
        combined_stream.size()));
    const char* engine_error = flatsql_get_error();
    if (!combined_stream.empty() && consumed_bytes != combined_stream.size()) {
        return reject_append(
            "append-failed",
            flatSqlNodeStatus_INVALID_ARGUMENT,
            engine_error && engine_error[0]
                ? engine_error
                : "FlatSQL did not consume the complete record stream");
    }
    if (staged_database) {
        publishConfiguration(
            control,
            append_database,
            std::move(staged_reflection_schema));
        staged_database = false;
    }
    if (!persistDurableState(&error)) {
        return durableMutationFailure(
            flatSqlNodeOperation_APPEND_RECORDS,
            request_id,
            wire_format,
            rollback,
            error);
    }
    return pushStatus(
        flatSqlNodeOperation_APPEND_RECORDS,
        request_id,
        flatSqlNodeStatus_COMPLETE,
        actual_records,
        combined_stream.size(),
        nullptr,
        "FlatSQL records appended",
        wire_format);
}

extern "C" int query_records(void) {
    const int32_t input_index = plugin_find_input_index("query", 0);
    const auto* frame = input_index >= 0
        ? plugin_get_input_frame(static_cast<uint32_t>(input_index))
        : nullptr;
    ControlFrame control;
    std::string error;
    if (!readControlFrame(frame, &control, &error)) {
        return invalidControl(
            flatSqlNodeOperation_QUERY_RECORDS,
            control.request_id,
            control.wire_format,
            error);
    }
    if (control.operation != flatSqlNodeOperation_QUERY_RECORDS) {
        return invalidControl(
            flatSqlNodeOperation_QUERY_RECORDS,
            control.request_id,
            control.wire_format,
            "query_records requires QUERY_RECORDS operation");
    }
    if (!ensureDurableStateLoaded(&error)) {
        return durableStateFailure(
            flatSqlNodeOperation_QUERY_RECORDS,
            control.request_id,
            control.wire_format,
            "durable-state-load-failed",
            error);
    }
    if (!g_database) {
        return pushStatus(
            flatSqlNodeOperation_QUERY_RECORDS,
            control.request_id,
            flatSqlNodeStatus_NOT_CONFIGURED,
            0,
            0,
            "database-not-configured",
            "configure_index or reload must run before query_records",
            control.wire_format);
    }
    std::string sql = control.query;
    if (sql.empty() && !control.view_name.empty()) {
        const auto found = g_views.find(control.view_name);
        if (found == g_views.end()) {
            return pushStatus(
                flatSqlNodeOperation_QUERY_RECORDS,
                control.request_id,
                flatSqlNodeStatus_NOT_FOUND,
                0,
                0,
                "view-not-found",
                "requested FlatSQL view is not configured",
                control.wire_format);
        }
        sql = found->second.query;
    }
    if (sql.empty()) {
        return invalidControl(
            flatSqlNodeOperation_QUERY_RECORDS,
            control.request_id,
            control.wire_format,
            "query_records requires SQL bytes or an upserted view name");
    }
    std::vector<uint8_t> result;
    uint64_t row_count = 0;
    uint32_t column_count = 0;
    if (!queryRaw(
            g_database,
            sql,
            control.parameters,
            control.parameter_count,
            &result,
            &row_count,
            &column_count,
            &error)) {
        return pushStatus(
            flatSqlNodeOperation_QUERY_RECORDS,
            control.request_id,
            flatSqlNodeStatus_INVALID_ARGUMENT,
            0,
            0,
            "query-failed",
            error,
            control.wire_format);
    }
    if (!pushByteStream(
            "records",
            flatSqlByteStreamKind_QUERY_RESULT,
            control.request_id,
            row_count,
            column_count,
            std::string{},
            std::string{},
            result,
            control.wire_format)) {
        return 500;
    }
    return pushStatus(
        flatSqlNodeOperation_QUERY_RECORDS,
        control.request_id,
        flatSqlNodeStatus_COMPLETE,
        row_count,
        result.size(),
        nullptr,
        "FlatSQL query complete",
        control.wire_format);
}

extern "C" int upsert_view(void) {
    const int32_t input_index = plugin_find_input_index("control", 0);
    const auto* frame = input_index >= 0
        ? plugin_get_input_frame(static_cast<uint32_t>(input_index))
        : nullptr;
    ControlFrame control;
    std::string error;
    if (!readControlFrame(frame, &control, &error)) {
        return invalidControl(
            flatSqlNodeOperation_UPSERT_VIEW,
            control.request_id,
            control.wire_format,
            error);
    }
    if (control.operation != flatSqlNodeOperation_UPSERT_VIEW ||
        control.view_name.empty() ||
        control.query.empty()) {
        return invalidControl(
            flatSqlNodeOperation_UPSERT_VIEW,
            control.request_id,
            control.wire_format,
            "upsert_view requires a view name and SQL query");
    }
    if (!ensureDurableStateLoaded(&error)) {
        return durableStateFailure(
            flatSqlNodeOperation_UPSERT_VIEW,
            control.request_id,
            control.wire_format,
            "durable-state-load-failed",
            error);
    }
    if (!g_database) {
        return pushStatus(
            flatSqlNodeOperation_UPSERT_VIEW,
            control.request_id,
            flatSqlNodeStatus_NOT_CONFIGURED,
            0,
            0,
            "database-not-configured",
            "configure_index or reload must run before upsert_view",
            control.wire_format);
    }
    if (!control.upsert_key_expression.empty()) {
        return pushStatus(
            flatSqlNodeOperation_UPSERT_VIEW,
            control.request_id,
            flatSqlNodeStatus_INVALID_ARGUMENT,
            0,
            0,
            "upsert-key-unsupported",
            "UPSERT_KEY_EXPRESSION is not supported by named query views",
            control.wire_format);
    }
    const InMemoryRollbackState rollback = captureInMemoryState();
    g_views[control.view_name] = {
        control.query,
        control.upsert_key_expression,
    };
    if (!persistDurableState(&error)) {
        return durableMutationFailure(
            flatSqlNodeOperation_UPSERT_VIEW,
            control.request_id,
            control.wire_format,
            rollback,
            error);
    }
    return pushStatus(
        flatSqlNodeOperation_UPSERT_VIEW,
        control.request_id,
        flatSqlNodeStatus_COMPLETE,
        1,
        0,
        nullptr,
        "FlatSQL view upserted",
        control.wire_format);
}

extern "C" int compact(void) {
    const int32_t input_index = plugin_find_input_index("control", 0);
    const auto* frame = input_index >= 0
        ? plugin_get_input_frame(static_cast<uint32_t>(input_index))
        : nullptr;
    ControlFrame control;
    std::string error;
    if (!readControlFrame(frame, &control, &error)) {
        return invalidControl(
            flatSqlNodeOperation_COMPACT,
            control.request_id,
            control.wire_format,
            error);
    }
    if (control.operation != flatSqlNodeOperation_COMPACT) {
        return invalidControl(
            flatSqlNodeOperation_COMPACT,
            control.request_id,
            control.wire_format,
            "compact requires COMPACT operation");
    }
    if (!ensureDurableStateLoaded(&error)) {
        return durableStateFailure(
            flatSqlNodeOperation_COMPACT,
            control.request_id,
            control.wire_format,
            "durable-state-load-failed",
            error);
    }
    if (!g_database) {
        return pushStatus(
            flatSqlNodeOperation_COMPACT,
            control.request_id,
            flatSqlNodeStatus_NOT_CONFIGURED,
            0,
            0,
            "database-not-configured",
            "configure_index or reload must run before compact",
            control.wire_format);
    }
    std::vector<std::vector<uint8_t>> retained_streams;
    uint64_t dropped_records = 0;
    for (const auto& binding : g_table_bindings) {
        std::vector<uint8_t> live_records;
        uint64_t row_count = 0;
        uint32_t column_count = 0;
        const std::string sql =
            "SELECT _data FROM " + quoteIdentifier(binding.second) +
            " ORDER BY rowid";
        if (!queryRaw(
                g_database,
                sql,
                {},
                0,
                &live_records,
                &row_count,
                &column_count,
                &error)) {
            return pushStatus(
                flatSqlNodeOperation_COMPACT,
                control.request_id,
                flatSqlNodeStatus_INTERNAL_ERROR,
                0,
                0,
                "compaction-query-failed",
                error,
                control.wire_format);
        }
        std::vector<uint8_t> retained;
        if (!trimRecordStream(
                live_records,
                g_retention_max_records,
                &retained,
                &dropped_records,
                &error)) {
            return pushStatus(
                flatSqlNodeOperation_COMPACT,
                control.request_id,
                flatSqlNodeStatus_INTERNAL_ERROR,
                0,
                0,
                "compaction-stream-invalid",
                error,
                control.wire_format);
        }
        retained_streams.emplace_back(std::move(retained));
    }
    std::vector<uint8_t> replacement_reflection_schema;
    void* replacement = createConfiguredDatabase(
        g_schema_idl,
        g_database_name,
        g_table_bindings,
        &replacement_reflection_schema,
        &error);
    if (!replacement) {
        return pushStatus(
            flatSqlNodeOperation_COMPACT,
            control.request_id,
            flatSqlNodeStatus_INTERNAL_ERROR,
            0,
            0,
            "compaction-rebuild-failed",
            error,
            control.wire_format);
    }
    for (const auto& retained : retained_streams) {
        if (!retained.empty()) {
            const uint64_t consumed = static_cast<uint64_t>(flatsql_ingest(
                replacement,
                retained.data(),
                retained.size()));
            if (consumed != retained.size()) {
                const char* engine_error = flatsql_get_error();
                flatsql_destroy_db(replacement);
                return pushStatus(
                    flatSqlNodeOperation_COMPACT,
                    control.request_id,
                    flatSqlNodeStatus_INTERNAL_ERROR,
                    0,
                    0,
                    "compaction-ingest-failed",
                    engine_error && engine_error[0]
                        ? engine_error
                        : "FlatSQL did not consume retained records",
                    control.wire_format);
            }
        }
    }
    const InMemoryRollbackState rollback = captureInMemoryState();
    flatsql_destroy_db(g_database);
    g_database = replacement;
    g_reflection_schema = std::move(replacement_reflection_schema);
    if (!persistDurableState(&error)) {
        return durableMutationFailure(
            flatSqlNodeOperation_COMPACT,
            control.request_id,
            control.wire_format,
            rollback,
            error);
    }
    return pushStatus(
        flatSqlNodeOperation_COMPACT,
        control.request_id,
        flatSqlNodeStatus_COMPLETE,
        dropped_records,
        0,
        nullptr,
        "FlatSQL storage compacted",
        control.wire_format);
}

extern "C" int configure_retention(void) {
    const int32_t input_index = plugin_find_input_index("control", 0);
    const auto* frame = input_index >= 0
        ? plugin_get_input_frame(static_cast<uint32_t>(input_index))
        : nullptr;
    ControlFrame control;
    std::string error;
    if (!readControlFrame(frame, &control, &error)) {
        return invalidControl(
            flatSqlNodeOperation_CONFIGURE_RETENTION,
            control.request_id,
            control.wire_format,
            error);
    }
    if (control.operation != flatSqlNodeOperation_CONFIGURE_RETENTION) {
        return invalidControl(
            flatSqlNodeOperation_CONFIGURE_RETENTION,
            control.request_id,
            control.wire_format,
            "configure_retention requires CONFIGURE_RETENTION operation");
    }
    if (!ensureDurableStateLoaded(&error)) {
        return durableStateFailure(
            flatSqlNodeOperation_CONFIGURE_RETENTION,
            control.request_id,
            control.wire_format,
            "durable-state-load-failed",
            error);
    }
    if (!g_database) {
        return pushStatus(
            flatSqlNodeOperation_CONFIGURE_RETENTION,
            control.request_id,
            flatSqlNodeStatus_NOT_CONFIGURED,
            0,
            0,
            "database-not-configured",
            "configure_index or reload must run before configure_retention",
            control.wire_format);
    }
    if (control.retention_max_age_millis != 0 ||
        control.compaction_target_bytes != 0) {
        return pushStatus(
            flatSqlNodeOperation_CONFIGURE_RETENTION,
            control.request_id,
            flatSqlNodeStatus_INVALID_ARGUMENT,
            0,
            0,
            "retention-policy-unsupported",
            "this node supports RETENTION_MAX_RECORDS only; max-age and target-bytes require a typed timestamp/size policy",
            control.wire_format);
    }
    const InMemoryRollbackState rollback = captureInMemoryState();
    g_retention_max_records = control.retention_max_records;
    g_retention_max_age_millis = 0;
    g_compaction_target_bytes = 0;
    if (!persistDurableState(&error)) {
        return durableMutationFailure(
            flatSqlNodeOperation_CONFIGURE_RETENTION,
            control.request_id,
            control.wire_format,
            rollback,
            error);
    }
    return pushStatus(
        flatSqlNodeOperation_CONFIGURE_RETENTION,
        control.request_id,
        flatSqlNodeStatus_COMPLETE,
        0,
        0,
        nullptr,
        "FlatSQL retention policy configured",
        control.wire_format);
}

extern "C" int snapshot(void) {
    const int32_t input_index = plugin_find_input_index("control", 0);
    const auto* frame = input_index >= 0
        ? plugin_get_input_frame(static_cast<uint32_t>(input_index))
        : nullptr;
    ControlFrame control;
    std::string error;
    if (!readControlFrame(frame, &control, &error)) {
        return invalidControl(
            flatSqlNodeOperation_SNAPSHOT,
            control.request_id,
            control.wire_format,
            error);
    }
    if (control.operation != flatSqlNodeOperation_SNAPSHOT) {
        return invalidControl(
            flatSqlNodeOperation_SNAPSHOT,
            control.request_id,
            control.wire_format,
            "snapshot requires SNAPSHOT operation");
    }
    if (!ensureDurableStateLoaded(&error)) {
        return durableStateFailure(
            flatSqlNodeOperation_SNAPSHOT,
            control.request_id,
            control.wire_format,
            "durable-state-load-failed",
            error);
    }
    if (!g_database) {
        return pushStatus(
            flatSqlNodeOperation_SNAPSHOT,
            control.request_id,
            flatSqlNodeStatus_NOT_CONFIGURED,
            0,
            0,
            "database-not-configured",
            "configure_index or reload must run before snapshot",
            control.wire_format);
    }
    const std::vector<uint8_t> snapshot_bytes = createSnapshotBytes();
    const std::vector<uint8_t> checksum = sha256(snapshot_bytes);
    if (!pushByteStream(
            "snapshot",
            flatSqlByteStreamKind_SNAPSHOT,
            control.request_id,
            0,
            0,
            kSnapshotSchemaName,
            kSnapshotFileIdentifier,
            snapshot_bytes,
            control.wire_format,
            checksum)) {
        return 500;
    }
    return pushStatus(
        flatSqlNodeOperation_SNAPSHOT,
        control.request_id,
        flatSqlNodeStatus_COMPLETE,
        0,
        snapshot_bytes.size(),
        nullptr,
        "FlatSQL snapshot emitted",
        control.wire_format);
}

extern "C" int reload(void) {
    std::vector<uint8_t> snapshot_bytes;
    uint64_t request_id = 0;
    uint64_t record_count = 0;
    uint32_t wire_format = kFlatbufferWireFormat;
    std::string schema_name;
    std::string file_identifier;
    std::vector<uint8_t> checksum;
    std::string error;
    if (!collectByteStream(
            "snapshot",
            flatSqlByteStreamKind_SNAPSHOT,
            &snapshot_bytes,
            &request_id,
            &record_count,
            &wire_format,
            &schema_name,
            &file_identifier,
            &checksum,
            &error)) {
        return invalidControl(
            flatSqlNodeOperation_RELOAD,
            request_id,
            wire_format,
            error);
    }
    if (schema_name != kSnapshotSchemaName ||
        file_identifier != kSnapshotFileIdentifier) {
        return pushStatus(
            flatSqlNodeOperation_RELOAD,
            request_id,
            flatSqlNodeStatus_INVALID_ARGUMENT,
            0,
            0,
            "snapshot-identity-invalid",
            "FSB snapshot identity does not match FlatSQLNodeSnapshot/FSN1",
            wire_format);
    }
    if (checksum.size() != 32 || checksum != sha256(snapshot_bytes)) {
        return pushStatus(
            flatSqlNodeOperation_RELOAD,
            request_id,
            flatSqlNodeStatus_INVALID_ARGUMENT,
            0,
            0,
            "snapshot-checksum-invalid",
            "FSB snapshot SHA256 does not match DATA",
            wire_format);
    }
    if (!prepareDurableStateForExplicitReload(&error)) {
        return durableStateFailure(
            flatSqlNodeOperation_RELOAD,
            request_id,
            wire_format,
            "durable-state-load-failed",
            error);
    }
    const InMemoryRollbackState rollback = captureInMemoryState();
    std::string error_code;
    if (!installSnapshotBytes(snapshot_bytes, &error_code, &error)) {
        return pushStatus(
            flatSqlNodeOperation_RELOAD,
            request_id,
            flatSqlNodeStatus_INVALID_ARGUMENT,
            0,
            0,
            error_code.c_str(),
            error,
            wire_format);
    }
    if (!persistDurableState(&error)) {
        return durableMutationFailure(
            flatSqlNodeOperation_RELOAD,
            request_id,
            wire_format,
            rollback,
            error);
    }
    return pushStatus(
        flatSqlNodeOperation_RELOAD,
        request_id,
        flatSqlNodeStatus_COMPLETE,
        0,
        snapshot_bytes.size(),
        nullptr,
        "FlatSQL snapshot reloaded",
        wire_format);
}
