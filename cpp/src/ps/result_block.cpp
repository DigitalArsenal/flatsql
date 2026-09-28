// FlatSQL partition store: RB1 result blocks (see ps/result_block.h).
#include "flatsql/ps/result_block.h"

#include <cstring>

namespace flatsql {
namespace ps {
namespace rb1 {

void Encoder::put(const void* p, size_t n) {
    const uint8_t* b = static_cast<const uint8_t*>(p);
    out_->insert(out_->end(), b, b + n);
}

void Encoder::header(const std::vector<std::string>& names) {
    uint8_t h[8];
    putU32(h, kMagicHeader);
    putU16(h + 4, uint16_t(names.size()));
    putU16(h + 6, 0);
    put(h, 8);
    for (const auto& n : names) {
        const uint16_t len = uint16_t(n.size() > 0xffff ? 0xffff : n.size());
        uint8_t l[2];
        putU16(l, len);
        put(l, 2);
        put(n.data(), len);
    }
}

void Encoder::beginRow() {
    if (blockAt_ == SIZE_MAX) {
        blockAt_ = out_->size();
        uint8_t h[16] = {0};
        putU32(h, kMagicBlock);
        put(h, 16);
        rows_ = 0;
    }
}

void Encoder::null() { out_->push_back(kNull); }

void Encoder::i64(int64_t v) {
    uint8_t c[9];
    c[0] = kInt;
    putU64(c + 1, uint64_t(v));
    put(c, 9);
}

void Encoder::real(double v) {
    uint8_t c[9];
    c[0] = kReal;
    uint64_t bits;
    std::memcpy(&bits, &v, 8);
    putU64(c + 1, bits);
    put(c, 9);
}

void Encoder::text(const void* p, size_t n) {
    uint8_t c[5];
    c[0] = kText;
    putU32(c + 1, uint32_t(n));
    put(c, 5);
    if (n) put(p, n);
}

void Encoder::blob(const void* p, size_t n) {
    uint8_t c[5];
    c[0] = kBlob;
    putU32(c + 1, uint32_t(n));
    put(c, 5);
    if (n) put(p, n);
}

void Encoder::endRow() {
    rows_++;
    if (blockBytes() >= kBlockTarget) flushBlock();
}

void Encoder::flushBlock() {
    if (blockAt_ == SIZE_MAX) return;
    uint8_t* h = out_->data() + blockAt_;
    putU32(h + 4, rows_);
    putU32(h + 8, uint32_t(out_->size() - blockAt_ - 16));
    blockAt_ = SIZE_MAX;
    rows_ = 0;
}

void Encoder::end(int32_t status, uint64_t rows, uint64_t rowsExamined, uint64_t bytesRead) {
    flushBlock();
    uint8_t e[32];
    putU32(e, kMagicEnd);
    putU32(e + 4, uint32_t(status));
    putU64(e + 8, rows);
    putU64(e + 16, rowsExamined);
    putU64(e + 24, bytesRead);
    put(e, 32);
}

namespace {
// Parses one cell at p (n bytes available). Returns bytes used, 0 if more
// input is needed, SIZE_MAX if malformed.
size_t parseCell(const uint8_t* p, size_t n, Cell* c) {
    if (n < 1) return 0;
    c->type = p[0];
    c->s.clear();
    switch (p[0]) {
        case kNull: return 1;
        case kInt:
            if (n < 9) return 0;
            c->i = int64_t(getU64(p + 1));
            return 9;
        case kReal: {
            if (n < 9) return 0;
            const uint64_t bits = getU64(p + 1);
            std::memcpy(&c->d, &bits, 8);
            return 9;
        }
        case kText:
        case kBlob: {
            if (n < 5) return 0;
            const uint32_t len = getU32(p + 1);
            if (n < 5 + size_t(len)) return 0;
            c->s.assign(reinterpret_cast<const char*>(p + 5), len);
            return 5 + size_t(len);
        }
        default: return SIZE_MAX;
    }
}
}  // namespace

bool Decoder::feed(const uint8_t* p, size_t n) {
    buf_.insert(buf_.end(), p, p + n);
    const bool ok = parse();
    if (at_ > (1u << 20)) {
        buf_.erase(buf_.begin(), buf_.begin() + long(at_));
        at_ = 0;
    }
    return ok;
}

bool Decoder::parse() {
    for (;;) {
        const uint8_t* p = buf_.data() + at_;
        const size_t n = buf_.size() - at_;
        if (done_) return n == 0 ? true : (err_ = "bytes after end", false);
        if (n < 4) return true;
        const uint32_t magic = getU32(p);
        if (!haveHeader_) {
            if (magic != kMagicHeader) return err_ = "missing header", false;
            if (n < 8) return true;
            const uint16_t ncols = getU16(p + 4);
            size_t off = 8;
            std::vector<std::string> names;
            for (uint16_t i = 0; i < ncols; i++) {
                if (n < off + 2) return true;
                const uint16_t l = getU16(p + off);
                if (n < off + 2 + l) return true;
                names.emplace_back(reinterpret_cast<const char*>(p + off + 2), l);
                off += 2 + l;
            }
            names_ = std::move(names);
            haveHeader_ = true;
            at_ += off;
            continue;
        }
        if (magic == kMagicBlock) {
            if (n < 16) return true;
            const uint32_t nrows = getU32(p + 4);
            const uint32_t body = getU32(p + 8);
            if (n < 16 + size_t(body)) return true;
            const uint8_t* q = p + 16;
            size_t left = body;
            for (uint32_t r = 0; r < nrows; r++) {
                std::vector<Cell> row(names_.size());
                for (size_t c = 0; c < names_.size(); c++) {
                    const size_t used = parseCell(q, left, &row[c]);
                    if (used == 0 || used == SIZE_MAX) return err_ = "bad cell", false;
                    q += used;
                    left -= used;
                }
                rows_.push_back(std::move(row));
            }
            if (left != 0) return err_ = "block length mismatch", false;
            at_ += 16 + body;
            continue;
        }
        if (magic == kMagicEnd) {
            if (n < 32) return true;
            status_ = int32_t(getU32(p + 4));
            rowsReported_ = getU64(p + 8);
            at_ += 32;
            done_ = true;
            continue;
        }
        return err_ = "bad magic", false;
    }
}

void encodeParams(const std::vector<Cell>& params, std::vector<uint8_t>* out) {
    uint8_t n[4];
    putU32(n, uint32_t(params.size()));
    out->insert(out->end(), n, n + 4);
    Encoder e(out);
    for (const Cell& c : params) {
        switch (c.type) {
            case kInt: e.i64(c.i); break;
            case kReal: e.real(c.d); break;
            case kText: e.text(c.s.data(), c.s.size()); break;
            case kBlob: e.blob(c.s.data(), c.s.size()); break;
            default: e.null(); break;
        }
    }
}

bool decodeParams(const uint8_t* p, size_t n, std::vector<Cell>* out) {
    out->clear();
    if (n == 0) return true;
    if (n < 4) return false;
    const uint32_t count = getU32(p);
    size_t off = 4;
    for (uint32_t i = 0; i < count; i++) {
        Cell c;
        const size_t used = parseCell(p + off, n - off, &c);
        if (used == 0 || used == SIZE_MAX) return false;
        off += used;
        out->push_back(std::move(c));
    }
    return off == n;
}

void rawFrame(const void* p, size_t n, std::vector<uint8_t>* out) {
    uint8_t h[4];
    putU32(h, uint32_t(n));
    out->insert(out->end(), h, h + 4);
    const uint8_t* b = static_cast<const uint8_t*>(p);
    out->insert(out->end(), b, b + n);
}

bool rawSplit(const uint8_t* p, size_t n, std::vector<std::string>* frames) {
    size_t off = 0;
    while (off + 4 <= n) {
        const uint32_t len = getU32(p + off);
        if (off + 4 + len > n) return false;
        frames->emplace_back(reinterpret_cast<const char*>(p + off + 4), len);
        off += 4 + len;
    }
    return off == n;
}

}  // namespace rb1
}  // namespace ps
}  // namespace flatsql
