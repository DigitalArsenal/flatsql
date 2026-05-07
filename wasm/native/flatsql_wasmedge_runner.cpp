#include <wasmedge/wasmedge.h>

#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <unistd.h>

namespace {

constexpr uint32_t OP_CREATE_DB = 1;
constexpr uint32_t OP_DESTROY_DB = 2;
constexpr uint32_t OP_REGISTER_FILE_ID = 3;
constexpr uint32_t OP_ENABLE_DEMO_EXTRACTORS = 4;
constexpr uint32_t OP_INGEST = 5;
constexpr uint32_t OP_QUERY = 6;
constexpr uint32_t OP_QUERY_MANY = 7;
constexpr uint32_t OP_REGISTER_QUERY_TEMPLATE = 8;
constexpr uint32_t OP_QUERY_TEMPLATE = 9;
constexpr uint32_t OP_CLEAR_QUERY_CACHE = 10;
constexpr uint32_t OP_QUERY_CACHE_STATS = 11;
constexpr uint32_t OP_GET_FLATBUFFER_BY_INDEX = 12;
constexpr uint32_t OP_EXPORT_DATA = 13;
constexpr uint32_t OP_LOAD_AND_REBUILD = 14;
constexpr uint32_t OP_BUILD_RESPONSE_ARTIFACT_CACHE_KEY = 15;
constexpr uint32_t OP_QUERY_RAW_FLATBUFFER_STREAM = 16;
constexpr uint32_t OP_RESERVE_STORAGE = 17;
constexpr uint32_t OP_LOAD_FROM_DB = 18;
constexpr uint32_t OP_CONFIGURE_QUERY_CACHE = 19;

int g_protocolFd = STDOUT_FILENO;

bool readExact(std::istream& input, uint8_t* data, size_t length) {
    input.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(length));
    return input.good() || static_cast<size_t>(input.gcount()) == length;
}

uint32_t readU32FromStream(std::istream& input, bool& ok) {
    uint8_t bytes[4];
    if (!readExact(input, bytes, sizeof(bytes))) {
        ok = false;
        return 0;
    }
    ok = true;
    return static_cast<uint32_t>(bytes[0]) |
           (static_cast<uint32_t>(bytes[1]) << 8) |
           (static_cast<uint32_t>(bytes[2]) << 16) |
           (static_cast<uint32_t>(bytes[3]) << 24);
}

class Reader {
  public:
    explicit Reader(const std::vector<uint8_t>& data) : data_(data) {}

    uint8_t u8() {
        require(1);
        return data_[offset_++];
    }

    uint32_t u32() {
        require(4);
        uint32_t value = static_cast<uint32_t>(data_[offset_]) |
                         (static_cast<uint32_t>(data_[offset_ + 1]) << 8) |
                         (static_cast<uint32_t>(data_[offset_ + 2]) << 16) |
                         (static_cast<uint32_t>(data_[offset_ + 3]) << 24);
        offset_ += 4;
        return value;
    }

    std::string string() {
        auto bytes = byteVector();
        return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }

    std::vector<uint8_t> byteVector() {
        const uint32_t length = u32();
        require(length);
        std::vector<uint8_t> bytes(data_.begin() + static_cast<std::ptrdiff_t>(offset_),
                                   data_.begin() + static_cast<std::ptrdiff_t>(offset_ + length));
        offset_ += length;
        return bytes;
    }

    void finish() {
        if (offset_ != data_.size()) {
            throw std::runtime_error("request has trailing bytes");
        }
    }

  private:
    void require(size_t length) {
        if (offset_ > data_.size() || length > data_.size() - offset_) {
            throw std::runtime_error("malformed runner request");
        }
    }

    const std::vector<uint8_t>& data_;
    size_t offset_ = 0;
};

class Writer {
  public:
    void u8(uint8_t value) {
        data_.push_back(value);
    }

    void u32(uint32_t value) {
        data_.push_back(static_cast<uint8_t>(value & 0xff));
        data_.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
        data_.push_back(static_cast<uint8_t>((value >> 16) & 0xff));
        data_.push_back(static_cast<uint8_t>((value >> 24) & 0xff));
    }

    void f64(double value) {
        static_assert(sizeof(double) == 8);
        uint8_t bytes[8];
        std::memcpy(bytes, &value, sizeof(bytes));
        data_.insert(data_.end(), bytes, bytes + sizeof(bytes));
    }

    void string(std::string_view value) {
        if (value.size() > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error("string too large for runner response");
        }
        u32(static_cast<uint32_t>(value.size()));
        data_.insert(data_.end(), value.begin(), value.end());
    }

    void bytes(const std::vector<uint8_t>& value) {
        if (value.size() > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error("byte payload too large for runner response");
        }
        u32(static_cast<uint32_t>(value.size()));
        data_.insert(data_.end(), value.begin(), value.end());
    }

    const std::vector<uint8_t>& data() const {
        return data_;
    }

  private:
    std::vector<uint8_t> data_;
};

void writeFrame(const std::vector<uint8_t>& payload) {
    if (payload.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("runner response frame too large");
    }
    const uint32_t length = static_cast<uint32_t>(payload.size());
    uint8_t header[4] = {
        static_cast<uint8_t>(length & 0xff),
        static_cast<uint8_t>((length >> 8) & 0xff),
        static_cast<uint8_t>((length >> 16) & 0xff),
        static_cast<uint8_t>((length >> 24) & 0xff),
    };
    if (::write(g_protocolFd, header, sizeof(header)) != static_cast<ssize_t>(sizeof(header))) {
        throw std::runtime_error("failed to write runner frame header");
    }
    if (!payload.empty() &&
        ::write(g_protocolFd, payload.data(), payload.size()) != static_cast<ssize_t>(payload.size())) {
        throw std::runtime_error("failed to write runner frame payload");
    }
}

std::vector<uint8_t> makeOkFrame(const Writer& body) {
    Writer frame;
    frame.u8(1);
    const auto& payload = body.data();
    auto result = frame.data();
    result.insert(result.end(), payload.begin(), payload.end());
    return result;
}

std::vector<uint8_t> makeErrorFrame(const std::string& message) {
    Writer frame;
    frame.u8(0);
    frame.string(message);
    return frame.data();
}

WasmEdge_String wrapName(const char* name) {
    return WasmEdge_StringWrap(name, static_cast<uint32_t>(std::strlen(name)));
}

void requireResult(WasmEdge_Result result, const std::string& operation) {
    if (!WasmEdge_ResultOK(result)) {
        throw std::runtime_error(operation + " failed: " + WasmEdge_ResultGetMessage(result));
    }
}

class WasmEdgeRuntime {
  public:
    explicit WasmEdgeRuntime(const std::string& wasmPath) {
        configure_ = WasmEdge_ConfigureCreate();
        if (!configure_) {
            throw std::runtime_error("failed to create WasmEdge configure context");
        }
        WasmEdge_ConfigureAddProposal(configure_, WasmEdge_Proposal_ExceptionHandling);
        WasmEdge_ConfigureAddHostRegistration(configure_, WasmEdge_HostRegistration_Wasi);

        vm_ = WasmEdge_VMCreate(configure_, nullptr);
        if (!vm_) {
            throw std::runtime_error("failed to create WasmEdge VM");
        }

        requireResult(WasmEdge_VMLoadWasmFromFile(vm_, wasmPath.c_str()), "load wasm");
        requireResult(WasmEdge_VMValidate(vm_), "validate wasm");
        requireResult(WasmEdge_VMInstantiate(vm_), "instantiate wasm");
        callVoid("_initialize");

        const WasmEdge_ModuleInstanceContext* module = WasmEdge_VMGetActiveModule(vm_);
        if (!module) {
            throw std::runtime_error("WasmEdge VM has no active module");
        }
        auto memoryName = wrapName("memory");
        memory_ = WasmEdge_ModuleInstanceFindMemory(module, memoryName);
        if (!memory_) {
            throw std::runtime_error("FlatSQL WASI module does not export memory");
        }
    }

    WasmEdgeRuntime(const WasmEdgeRuntime&) = delete;
    WasmEdgeRuntime& operator=(const WasmEdgeRuntime&) = delete;

    ~WasmEdgeRuntime() {
        if (vm_) {
            WasmEdge_VMDelete(vm_);
        }
        if (configure_) {
            WasmEdge_ConfigureDelete(configure_);
        }
    }

    uint32_t callI32(const char* name, const std::vector<WasmEdge_Value>& params = {}) {
        WasmEdge_Value result[1];
        auto funcName = wrapName(name);
        requireResult(
            WasmEdge_VMExecute(
                vm_,
                funcName,
                params.empty() ? nullptr : params.data(),
                static_cast<uint32_t>(params.size()),
                result,
                1
            ),
            name
        );
        return static_cast<uint32_t>(WasmEdge_ValueGetI32(result[0]));
    }

    double callF64(const char* name, const std::vector<WasmEdge_Value>& params = {}) {
        WasmEdge_Value result[1];
        auto funcName = wrapName(name);
        requireResult(
            WasmEdge_VMExecute(
                vm_,
                funcName,
                params.empty() ? nullptr : params.data(),
                static_cast<uint32_t>(params.size()),
                result,
                1
            ),
            name
        );
        return WasmEdge_ValueGetF64(result[0]);
    }

    void callVoid(const char* name, const std::vector<WasmEdge_Value>& params = {}) {
        auto funcName = wrapName(name);
        requireResult(
            WasmEdge_VMExecute(
                vm_,
                funcName,
                params.empty() ? nullptr : params.data(),
                static_cast<uint32_t>(params.size()),
                nullptr,
                0
            ),
            name
        );
    }

    uint32_t mallocBytes(size_t size) {
        if (size == 0) {
            return 0;
        }
        if (size > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error("allocation request exceeds wasm32 address space");
        }
        const uint32_t ptr = callI32("malloc", {WasmEdge_ValueGenI32(static_cast<int32_t>(size))});
        if (ptr == 0) {
            throw std::runtime_error("FlatSQL WASM allocation failed");
        }
        return ptr;
    }

    void freeBytes(uint32_t ptr) {
        if (ptr != 0) {
            callVoid("free", {WasmEdge_ValueGenI32(static_cast<int32_t>(ptr))});
        }
    }

    template <typename Callback>
    auto withBytes(const std::vector<uint8_t>& bytes, Callback callback) -> decltype(callback(uint32_t{})) {
        const uint32_t ptr = mallocBytes(bytes.size());
        try {
            if (!bytes.empty()) {
                requireResult(
                    WasmEdge_MemoryInstanceSetData(memory_, bytes.data(), ptr, static_cast<uint32_t>(bytes.size())),
                    "write wasm memory"
                );
            }
            if constexpr (std::is_void_v<decltype(callback(ptr))>) {
                callback(ptr);
                freeBytes(ptr);
            } else {
                auto result = callback(ptr);
                freeBytes(ptr);
                return result;
            }
        } catch (...) {
            freeBytes(ptr);
            throw;
        }
    }

    template <typename Callback>
    auto withString(const std::string& value, Callback callback) -> decltype(callback(uint32_t{})) {
        std::vector<uint8_t> bytes(value.begin(), value.end());
        bytes.push_back(0);
        return withBytes(bytes, callback);
    }

    std::string readCString(uint32_t ptr) {
        if (ptr == 0) {
            return "";
        }
        const uint32_t pageCount = WasmEdge_MemoryInstanceGetPageSize(memory_);
        const uint64_t memorySize = static_cast<uint64_t>(pageCount) * 65536ULL;
        if (ptr >= memorySize) {
            throw std::runtime_error("WASM string pointer is outside memory");
        }
        const auto available = static_cast<uint32_t>(std::min<uint64_t>(memorySize - ptr, 16ULL * 1024ULL * 1024ULL));
        const uint8_t* raw = WasmEdge_MemoryInstanceGetPointerConst(memory_, ptr, available);
        if (!raw) {
            throw std::runtime_error("failed to read WASM string pointer");
        }
        size_t length = 0;
        while (length < available && raw[length] != 0) {
            length++;
        }
        if (length == available) {
            throw std::runtime_error("unterminated WASM string");
        }
        return std::string(reinterpret_cast<const char*>(raw), length);
    }

    std::vector<uint8_t> readBytes(uint32_t ptr, uint32_t length) {
        std::vector<uint8_t> bytes(length);
        if (length == 0) {
            return bytes;
        }
        requireResult(WasmEdge_MemoryInstanceGetData(memory_, bytes.data(), ptr, length), "read wasm memory");
        return bytes;
    }

    std::string lastError() {
        return readCString(callI32("flatsql_get_error"));
    }

  private:
    WasmEdge_ConfigureContext* configure_ = nullptr;
    WasmEdge_VMContext* vm_ = nullptr;
    WasmEdge_MemoryInstanceContext* memory_ = nullptr;
};

void checkFlatSQL(WasmEdgeRuntime& runtime, uint32_t success) {
    if (success == 0) {
        std::string error = runtime.lastError();
        if (error.empty()) {
            error = "FlatSQL operation failed";
        }
        throw std::runtime_error(error);
    }
}

std::string joinStrings(const std::vector<std::string>& values, char delimiter) {
    std::string out;
    for (size_t index = 0; index < values.size(); index++) {
        if (index > 0) {
            out.push_back(delimiter);
        }
        out += values[index];
    }
    return out;
}

void encodeQueryResult(WasmEdgeRuntime& runtime, Writer& writer) {
    const uint32_t columnCount = runtime.callI32("flatsql_result_column_count");
    const uint32_t rowCount = runtime.callI32("flatsql_result_row_count");

    writer.u32(columnCount);
    for (uint32_t column = 0; column < columnCount; column++) {
        const uint32_t ptr = runtime.callI32("flatsql_result_column_name", {
            WasmEdge_ValueGenI32(static_cast<int32_t>(column)),
        });
        writer.string(runtime.readCString(ptr));
    }

    writer.u32(rowCount);
    for (uint32_t row = 0; row < rowCount; row++) {
        for (uint32_t column = 0; column < columnCount; column++) {
            const uint32_t type = runtime.callI32("flatsql_result_cell_type", {
                WasmEdge_ValueGenI32(static_cast<int32_t>(row)),
                WasmEdge_ValueGenI32(static_cast<int32_t>(column)),
            });
            writer.u8(static_cast<uint8_t>(type));
            switch (type) {
                case 0:
                    break;
                case 1:
                    writer.u8(runtime.callF64("flatsql_result_cell_number", {
                        WasmEdge_ValueGenI32(static_cast<int32_t>(row)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(column)),
                    }) != 0.0 ? 1 : 0);
                    break;
                case 2:
                case 3:
                case 4:
                    writer.f64(runtime.callF64("flatsql_result_cell_number", {
                        WasmEdge_ValueGenI32(static_cast<int32_t>(row)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(column)),
                    }));
                    break;
                case 5: {
                    const uint32_t ptr = runtime.callI32("flatsql_result_cell_string", {
                        WasmEdge_ValueGenI32(static_cast<int32_t>(row)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(column)),
                    });
                    writer.string(runtime.readCString(ptr));
                    break;
                }
                case 6: {
                    const uint32_t ptr = runtime.callI32("flatsql_result_cell_blob", {
                        WasmEdge_ValueGenI32(static_cast<int32_t>(row)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(column)),
                    });
                    const uint32_t size = runtime.callI32("flatsql_result_cell_blob_size", {
                        WasmEdge_ValueGenI32(static_cast<int32_t>(row)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(column)),
                    });
                    writer.bytes(runtime.readBytes(ptr, size));
                    break;
                }
                default:
                    throw std::runtime_error("unsupported FlatSQL cell type");
            }
        }
    }
}

std::vector<uint8_t> handleRequest(WasmEdgeRuntime& runtime, const std::vector<uint8_t>& request) {
    Reader reader(request);
    const uint32_t op = reader.u32();
    Writer body;

    switch (op) {
        case OP_CREATE_DB: {
            const std::string schema = reader.string();
            const std::string name = reader.string();
            reader.finish();
            const uint32_t handle = runtime.withString(schema, [&](uint32_t schemaPtr) {
                return runtime.withString(name, [&](uint32_t namePtr) {
                    return runtime.callI32("flatsql_create_db", {
                        WasmEdge_ValueGenI32(static_cast<int32_t>(schemaPtr)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(namePtr)),
                    });
                });
            });
            if (handle == 0) {
                throw std::runtime_error("failed to create FlatSQL database");
            }
            body.u32(handle);
            break;
        }
        case OP_DESTROY_DB: {
            const uint32_t handle = reader.u32();
            reader.finish();
            runtime.callVoid("flatsql_destroy_db", {WasmEdge_ValueGenI32(static_cast<int32_t>(handle))});
            break;
        }
        case OP_REGISTER_FILE_ID: {
            const uint32_t handle = reader.u32();
            const std::string fileId = reader.string();
            const std::string tableName = reader.string();
            reader.finish();
            runtime.withString(fileId, [&](uint32_t fileIdPtr) {
                runtime.withString(tableName, [&](uint32_t tablePtr) {
                    runtime.callVoid("flatsql_register_file_id", {
                        WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(fileIdPtr)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(tablePtr)),
                    });
                });
            });
            break;
        }
        case OP_ENABLE_DEMO_EXTRACTORS: {
            const uint32_t handle = reader.u32();
            reader.finish();
            runtime.callVoid("flatsql_enable_demo_extractors", {WasmEdge_ValueGenI32(static_cast<int32_t>(handle))});
            break;
        }
        case OP_INGEST: {
            const uint32_t handle = reader.u32();
            const uint8_t hasSource = reader.u8();
            const std::string source = hasSource != 0 ? reader.string() : "";
            const std::vector<uint8_t> data = reader.byteVector();
            reader.finish();
            const double count = runtime.withBytes(data, [&](uint32_t dataPtr) {
                if (hasSource != 0) {
                    return runtime.withString(source, [&](uint32_t sourcePtr) {
                        return runtime.callF64("flatsql_ingest_with_source", {
                            WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                            WasmEdge_ValueGenI32(static_cast<int32_t>(dataPtr)),
                            WasmEdge_ValueGenI32(static_cast<int32_t>(data.size())),
                            WasmEdge_ValueGenI32(static_cast<int32_t>(sourcePtr)),
                        });
                    });
                }
                return runtime.callF64("flatsql_ingest", {
                    WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                    WasmEdge_ValueGenI32(static_cast<int32_t>(dataPtr)),
                    WasmEdge_ValueGenI32(static_cast<int32_t>(data.size())),
                });
            });
            body.f64(count);
            break;
        }
        case OP_QUERY: {
            const uint32_t handle = reader.u32();
            const std::string sql = reader.string();
            const uint32_t paramCount = reader.u32();
            const std::vector<uint8_t> params = reader.byteVector();
            reader.finish();
            const uint32_t success = runtime.withString(sql, [&](uint32_t sqlPtr) {
                return runtime.withBytes(params, [&](uint32_t paramsPtr) {
                    return runtime.callI32("flatsql_query_params", {
                        WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(sqlPtr)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(paramsPtr)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(params.size())),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(paramCount)),
                    });
                });
            });
            checkFlatSQL(runtime, success);
            encodeQueryResult(runtime, body);
            break;
        }
        case OP_QUERY_MANY: {
            const uint32_t handle = reader.u32();
            const uint32_t requestCount = reader.u32();
            const std::vector<uint8_t> requestBytes = reader.byteVector();
            reader.finish();
            const uint32_t success = runtime.withBytes(requestBytes, [&](uint32_t ptr) {
                return runtime.callI32("flatsql_query_many", {
                    WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                    WasmEdge_ValueGenI32(static_cast<int32_t>(ptr)),
                    WasmEdge_ValueGenI32(static_cast<int32_t>(requestBytes.size())),
                    WasmEdge_ValueGenI32(static_cast<int32_t>(requestCount)),
                });
            });
            checkFlatSQL(runtime, success);
            const uint32_t resultCount = runtime.callI32("flatsql_batch_result_count");
            body.u32(resultCount);
            for (uint32_t index = 0; index < resultCount; index++) {
                checkFlatSQL(runtime, runtime.callI32("flatsql_select_batch_result", {
                    WasmEdge_ValueGenI32(static_cast<int32_t>(index)),
                }));
                encodeQueryResult(runtime, body);
            }
            break;
        }
        case OP_QUERY_RAW_FLATBUFFER_STREAM: {
            const uint32_t handle = reader.u32();
            const std::string sql = reader.string();
            const uint32_t paramCount = reader.u32();
            const std::vector<uint8_t> params = reader.byteVector();
            reader.finish();
            const uint32_t success = runtime.withString(sql, [&](uint32_t sqlPtr) {
                return runtime.withBytes(params, [&](uint32_t paramsPtr) {
                    return runtime.callI32("flatsql_query_raw_flatbuffer_stream", {
                        WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(sqlPtr)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(paramsPtr)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(params.size())),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(paramCount)),
                    });
                });
            });
            checkFlatSQL(runtime, success);
            const uint32_t ptr = runtime.callI32("flatsql_response_artifact_data");
            const uint32_t size = runtime.callI32("flatsql_response_artifact_size");
            body.bytes(runtime.readBytes(ptr, size));
            break;
        }
        case OP_REGISTER_QUERY_TEMPLATE: {
            const uint32_t handle = reader.u32();
            const std::string queryId = reader.string();
            const std::string sql = reader.string();
            const uint8_t cacheable = reader.u8();
            reader.finish();
            const uint32_t success = runtime.withString(queryId, [&](uint32_t queryIdPtr) {
                return runtime.withString(sql, [&](uint32_t sqlPtr) {
                    return runtime.callI32("flatsql_register_query_template", {
                        WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(queryIdPtr)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(sqlPtr)),
                        WasmEdge_ValueGenI32(cacheable != 0 ? 1 : 0),
                    });
                });
            });
            checkFlatSQL(runtime, success);
            break;
        }
        case OP_QUERY_TEMPLATE: {
            const uint32_t handle = reader.u32();
            const std::string queryId = reader.string();
            const uint32_t paramCount = reader.u32();
            const std::vector<uint8_t> params = reader.byteVector();
            reader.finish();
            const uint32_t success = runtime.withString(queryId, [&](uint32_t queryIdPtr) {
                return runtime.withBytes(params, [&](uint32_t paramsPtr) {
                    return runtime.callI32("flatsql_query_template", {
                        WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(queryIdPtr)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(paramsPtr)),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(params.size())),
                        WasmEdge_ValueGenI32(static_cast<int32_t>(paramCount)),
                    });
                });
            });
            checkFlatSQL(runtime, success);
            encodeQueryResult(runtime, body);
            break;
        }
        case OP_CLEAR_QUERY_CACHE: {
            const uint32_t handle = reader.u32();
            reader.finish();
            runtime.callVoid("flatsql_clear_query_cache", {WasmEdge_ValueGenI32(static_cast<int32_t>(handle))});
            break;
        }
        case OP_CONFIGURE_QUERY_CACHE: {
            const uint32_t handle = reader.u32();
            const uint32_t maxEntries = reader.u32();
            const uint32_t maxRows = reader.u32();
            reader.finish();
            const uint32_t success = runtime.callI32("flatsql_configure_query_cache", {
                WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                WasmEdge_ValueGenI32(static_cast<int32_t>(maxEntries)),
                WasmEdge_ValueGenI32(static_cast<int32_t>(maxRows)),
            });
            checkFlatSQL(runtime, success);
            break;
        }
        case OP_QUERY_CACHE_STATS: {
            const uint32_t handle = reader.u32();
            reader.finish();
            body.f64(runtime.callF64("flatsql_query_cache_hits", {WasmEdge_ValueGenI32(static_cast<int32_t>(handle))}));
            body.f64(runtime.callF64("flatsql_query_cache_misses", {WasmEdge_ValueGenI32(static_cast<int32_t>(handle))}));
            body.f64(runtime.callF64("flatsql_query_cache_size", {WasmEdge_ValueGenI32(static_cast<int32_t>(handle))}));
            body.f64(runtime.callF64("flatsql_query_cache_generation", {WasmEdge_ValueGenI32(static_cast<int32_t>(handle))}));
            body.f64(runtime.callF64("flatsql_query_cache_max_entries", {WasmEdge_ValueGenI32(static_cast<int32_t>(handle))}));
            body.f64(runtime.callF64("flatsql_query_cache_max_rows", {WasmEdge_ValueGenI32(static_cast<int32_t>(handle))}));
            break;
        }
        case OP_GET_FLATBUFFER_BY_INDEX: {
            const uint32_t handle = reader.u32();
            const std::string table = reader.string();
            const std::string index = reader.string();
            const uint32_t paramCount = reader.u32();
            const std::vector<uint8_t> params = reader.byteVector();
            reader.finish();
            const uint32_t ptr = runtime.withString(table, [&](uint32_t tablePtr) {
                return runtime.withString(index, [&](uint32_t indexPtr) {
                    return runtime.withBytes(params, [&](uint32_t paramsPtr) {
                        return runtime.callI32("flatsql_get_flatbuffer_by_index", {
                            WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                            WasmEdge_ValueGenI32(static_cast<int32_t>(tablePtr)),
                            WasmEdge_ValueGenI32(static_cast<int32_t>(indexPtr)),
                            WasmEdge_ValueGenI32(static_cast<int32_t>(paramsPtr)),
                            WasmEdge_ValueGenI32(static_cast<int32_t>(params.size())),
                            WasmEdge_ValueGenI32(static_cast<int32_t>(paramCount)),
                        });
                    });
                });
            });
            const uint32_t size = runtime.callI32("flatsql_get_raw_flatbuffer_size");
            if (ptr == 0 || size == 0) {
                body.u8(0);
            } else {
                body.u8(1);
                body.bytes(runtime.readBytes(ptr, size));
            }
            break;
        }
        case OP_EXPORT_DATA: {
            const uint32_t handle = reader.u32();
            reader.finish();
            const uint32_t ptr = runtime.callI32("flatsql_export_data", {
                WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
            });
            const uint32_t size = runtime.callI32("flatsql_export_size");
            body.bytes(runtime.readBytes(ptr, size));
            break;
        }
        case OP_LOAD_AND_REBUILD: {
            const uint32_t handle = reader.u32();
            const std::vector<uint8_t> data = reader.byteVector();
            reader.finish();
            runtime.withBytes(data, [&](uint32_t dataPtr) {
                runtime.callVoid("flatsql_load_and_rebuild", {
                    WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                    WasmEdge_ValueGenI32(static_cast<int32_t>(dataPtr)),
                    WasmEdge_ValueGenI32(static_cast<int32_t>(data.size())),
                });
            });
            break;
        }
        case OP_RESERVE_STORAGE: {
            const uint32_t handle = reader.u32();
            const uint32_t bytes = reader.u32();
            reader.finish();
            runtime.callVoid("flatsql_reserve_storage", {
                WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                WasmEdge_ValueGenI32(static_cast<int32_t>(bytes)),
            });
            break;
        }
        case OP_LOAD_FROM_DB: {
            const uint32_t handle = reader.u32();
            const uint32_t sourceHandle = reader.u32();
            reader.finish();
            runtime.callVoid("flatsql_load_from_db", {
                WasmEdge_ValueGenI32(static_cast<int32_t>(handle)),
                WasmEdge_ValueGenI32(static_cast<int32_t>(sourceHandle)),
            });
            break;
        }
        case OP_BUILD_RESPONSE_ARTIFACT_CACHE_KEY: {
            const std::string schemaName = reader.string();
            const std::string schemaVersion = reader.string();
            const std::string sql = reader.string();
            const std::string format = reader.string();
            const std::string publishEventKey = reader.string();
            const uint32_t projectionCount = reader.u32();
            std::vector<std::string> projection;
            projection.reserve(projectionCount);
            for (uint32_t index = 0; index < projectionCount; index++) {
                projection.push_back(reader.string());
            }
            const uint32_t paramCount = reader.u32();
            const std::vector<uint8_t> params = reader.byteVector();
            reader.finish();

            const std::string projectionList = joinStrings(projection, '\n');
            const uint32_t keyPtr = runtime.withString(schemaName, [&](uint32_t schemaNamePtr) {
                return runtime.withString(schemaVersion, [&](uint32_t schemaVersionPtr) {
                    return runtime.withString(sql, [&](uint32_t sqlPtr) {
                        return runtime.withString(format, [&](uint32_t formatPtr) {
                            return runtime.withString(publishEventKey, [&](uint32_t publishEventKeyPtr) {
                                return runtime.withString(projectionList, [&](uint32_t projectionListPtr) {
                                    return runtime.withBytes(params, [&](uint32_t paramsPtr) {
                                        return runtime.callI32("flatsql_build_response_artifact_cache_key", {
                                            WasmEdge_ValueGenI32(static_cast<int32_t>(schemaNamePtr)),
                                            WasmEdge_ValueGenI32(static_cast<int32_t>(schemaVersionPtr)),
                                            WasmEdge_ValueGenI32(static_cast<int32_t>(sqlPtr)),
                                            WasmEdge_ValueGenI32(static_cast<int32_t>(formatPtr)),
                                            WasmEdge_ValueGenI32(static_cast<int32_t>(publishEventKeyPtr)),
                                            WasmEdge_ValueGenI32(static_cast<int32_t>(projectionListPtr)),
                                            WasmEdge_ValueGenI32(static_cast<int32_t>(paramsPtr)),
                                            WasmEdge_ValueGenI32(static_cast<int32_t>(params.size())),
                                            WasmEdge_ValueGenI32(static_cast<int32_t>(paramCount)),
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
            if (keyPtr == 0) {
                std::string error = runtime.lastError();
                if (error.empty()) {
                    error = "failed to build response artifact cache key";
                }
                throw std::runtime_error(error);
            }
            body.string(runtime.readCString(keyPtr));
            break;
        }
        default:
            throw std::runtime_error("unknown runner opcode");
    }

    return makeOkFrame(body);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <flatsql-wasi.wasm>\n";
        return 64;
    }

    try {
        g_protocolFd = ::dup(STDOUT_FILENO);
        if (g_protocolFd < 0) {
            throw std::runtime_error("failed to duplicate runner stdout");
        }
        WasmEdge_LogOff();

        WasmEdgeRuntime runtime(argv[1]);

        while (true) {
            bool ok = false;
            const uint32_t length = readU32FromStream(std::cin, ok);
            if (!ok) {
                break;
            }

            std::vector<uint8_t> request(length);
            if (!readExact(std::cin, request.data(), request.size())) {
                break;
            }

            try {
                writeFrame(handleRequest(runtime, request));
            } catch (const std::exception& error) {
                writeFrame(makeErrorFrame(error.what()));
            }
        }

        return 0;
    } catch (const std::exception& error) {
        std::cerr << "flatsql-wasmedge-runner: " << error.what() << "\n";
        return 1;
    }
}
