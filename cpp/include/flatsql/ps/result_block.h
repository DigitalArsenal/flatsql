// FlatSQL partition store: RB1 result blocks and raw-stream framing (design
// §9 "Results"; T2 acceptance #3).
//
// RB1 is a byte stream (little-endian) written into a request slot's result
// ring and copied by the router straight to the socket:
//
//   header   'RB1H' u32 | ncols u16 | rsv u16 | ncols x (u16 len, name bytes)
//   block    'RB1B' u32 | nrows u32 | bodyLen u32 | rsv u32 | body
//            body = nrows x ncols cells; a cell is a u8 type then:
//              0 NULL     (nothing)
//              1 INTEGER  8 bytes, exact int64 (no float64 rounding, §2)
//              2 REAL     8 bytes IEEE-754 binary64
//              3 TEXT     u32 len + UTF-8 bytes
//              4 BLOB     u32 len + bytes
//   end      'RB1E' u32 | status i32 | rows u64 | rowsExamined u64 | bytesRead u64
//
// Cells carry their own type tag (SQLite values are dynamically typed, so a
// column's type can change from row to row); the design's per-block type
// vector is not used (PARTITION-STORE.md deviations).
//
// Raw-stream mode emits [u32le size][bytes] per BLOB cell (the stored frames
// verbatim when the column is _data) and nothing else; the slot's status
// word reports the outcome.
//
// Parameters use the same cell encoding: u32 count, then count cells.
#ifndef FLATSQL_PS_RESULT_BLOCK_H
#define FLATSQL_PS_RESULT_BLOCK_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "flatsql/ps/format.h"

namespace flatsql {
namespace ps {
namespace rb1 {

constexpr uint32_t kMagicHeader = fourcc('R', 'B', '1', 'H');
constexpr uint32_t kMagicBlock = fourcc('R', 'B', '1', 'B');
constexpr uint32_t kMagicEnd = fourcc('R', 'B', '1', 'E');
constexpr size_t kBlockTarget = 32u << 10;  // flush a block at this body size

enum CellType : uint8_t { kNull = 0, kInt = 1, kReal = 2, kText = 3, kBlob = 4 };

// Appends to a byte vector (the lane's pending output).
class Encoder {
public:
    explicit Encoder(std::vector<uint8_t>* out) : out_(out) {}
    void header(const std::vector<std::string>& names);
    void beginRow();
    void null();
    void i64(int64_t v);
    void real(double v);
    void text(const void* p, size_t n);
    void blob(const void* p, size_t n);
    void endRow();
    // Closes the open block (if any).
    void flushBlock();
    void end(int32_t status, uint64_t rows, uint64_t rowsExamined, uint64_t bytesRead);
    size_t blockBytes() const { return blockAt_ == SIZE_MAX ? 0 : out_->size() - blockAt_; }

private:
    void put(const void* p, size_t n);
    std::vector<uint8_t>* out_;
    size_t blockAt_ = SIZE_MAX;   // offset of the open block header
    uint32_t rows_ = 0;
};

struct Cell {
    uint8_t type = kNull;
    int64_t i = 0;
    double d = 0;
    std::string s;     // text or blob bytes
    bool operator==(const Cell& o) const {
        return type == o.type && i == o.i && (type != kReal || d == o.d) && s == o.s;
    }
};

// Incremental decoder: feed() bytes in any split; rows accumulate.
class Decoder {
public:
    // Returns false on a malformed stream (error() says why).
    bool feed(const uint8_t* p, size_t n);
    bool done() const { return done_; }
    const std::vector<std::string>& names() const { return names_; }
    std::vector<std::vector<Cell>>& rows() { return rows_; }
    int32_t status() const { return status_; }
    uint64_t rowsReported() const { return rowsReported_; }
    const std::string& error() const { return err_; }

private:
    bool parse();
    std::vector<uint8_t> buf_;
    size_t at_ = 0;
    bool haveHeader_ = false;
    bool done_ = false;
    std::vector<std::string> names_;
    std::vector<std::vector<Cell>> rows_;
    int32_t status_ = 0;
    uint64_t rowsReported_ = 0;
    std::string err_;
};

// Parameters: u32 count, then cells.
void encodeParams(const std::vector<Cell>& params, std::vector<uint8_t>* out);
bool decodeParams(const uint8_t* p, size_t n, std::vector<Cell>* out);

// Raw-stream framing helpers.
void rawFrame(const void* p, size_t n, std::vector<uint8_t>* out);
bool rawSplit(const uint8_t* p, size_t n, std::vector<std::string>* frames);

}  // namespace rb1
}  // namespace ps
}  // namespace flatsql

#endif
