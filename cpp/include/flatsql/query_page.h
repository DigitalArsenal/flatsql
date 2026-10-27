// The engine's result stream is size-prefixed SDS QRP pages. No text renderer.
#ifndef FLATSQL_QUERY_PAGE_H
#define FLATSQL_QUERY_PAGE_H
#include "QRP/main_generated.h"
#include "flatsql/ps/result_block.h" // private predicate cells, not the wire format

namespace flatsql::query {
constexpr size_t kPageTarget = 32u << 10;
class Encoder {
public:
    explicit Encoder(std::vector<uint8_t>* out) : out_(out) {}
    void header(const std::vector<std::string>& names);
    void beginRow() { cells_.clear(); }
    void null();
    void i64(int64_t v);
    void real(double v);
    void text(const void* p, size_t n);
    void blob(const void* p, size_t n);
    void endRow();
    void flushBlock();
    void end(int32_t status, uint64_t rows, uint64_t examined, uint64_t bytes, const std::string& message = {});
    size_t blockBytes() const { return rows_.empty() ? 0 : builder_.GetSize(); }
    // Upper bound including vector offsets, names, page table and alignment.
    size_t pendingBytes() const;
private:
    void page(bool headerOnly = false);
    void append();
    std::vector<uint8_t>* out_;
    flatbuffers::FlatBufferBuilder builder_;
    std::vector<std::string> names_;
    std::vector<flatbuffers::Offset<QRPValue>> cells_;
    std::vector<flatbuffers::Offset<QRPRow>> rows_;
};

// Verification and typed parameter binding for a QRP request payload.
bool parameters(const uint8_t* data, size_t size, std::vector<ps::rb1::Cell>* out);
const char* errorCode(int32_t status);
int32_t errorStatus(const char* code);

// Incremental native stream reader used by the real-engine E2E driver.
class Decoder {
public:
    bool feed(const uint8_t* p, size_t n);
    bool done() const { return done_; }
    const std::vector<std::string>& names() const { return names_; }
    std::vector<std::vector<ps::rb1::Cell>>& rows() { return rows_; }
    int32_t status() const { return status_; }
    uint64_t rowsReported() const { return reported_; }
    const std::string& error() const { return error_; }
private:
    std::vector<uint8_t> pending_;
    std::vector<std::string> names_;
    std::vector<std::vector<ps::rb1::Cell>> rows_;
    std::string error_;
    int32_t status_ = 0;
    uint64_t reported_ = 0;
    bool done_ = false;
};
}
#endif
