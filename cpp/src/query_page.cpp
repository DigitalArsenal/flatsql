#include "flatsql/query_page.h"
#include <cstring>

namespace flatsql::query {
namespace {
constexpr const char* codes[] = {"", "ARG", "NOMEM", "IO", "CORRUPT", "FORMAT", "BUSY", "CANCELLED", "BUDGET",
                               "UNSUPPORTED", "NOTYPE", "STOPPED", "NOSPACE", "SQL", "INTERNAL"};
bool value(const QRPValue* v, ps::rb1::Cell* c) {
    if (!v) return false;
    switch (v->KIND()) {
        case qrpValueKind_Null: c->type = ps::rb1::kNull; break;
        case qrpValueKind_Bool: c->type = ps::rb1::kInt; c->i = v->BOOL_VALUE(); break;
        case qrpValueKind_Int: c->type = ps::rb1::kInt; c->i = v->INT_VALUE(); break;
        case qrpValueKind_Float: c->type = ps::rb1::kReal; c->d = v->FLOAT_VALUE(); break;
        case qrpValueKind_Text:
            c->type = ps::rb1::kText;
            if (v->TEXT_VALUE()) c->s = v->TEXT_VALUE()->str();
            break;
        case qrpValueKind_Bytes:
            c->type = ps::rb1::kBlob;
            if (v->BYTES_VALUE()) c->s.assign(reinterpret_cast<const char*>(v->BYTES_VALUE()->data()), v->BYTES_VALUE()->size());
            break;
        default: return false;
    }
    return true;
}
}
const char* errorCode(int32_t status) { return status <= 0 && status >= -14 ? codes[-status] : "INTERNAL"; }
int32_t errorStatus(const char* code) {
    for (int i = 0; i <= 14; ++i) if (!std::strcmp(code, codes[i])) return -i;
    return -14;
}
void Encoder::append() {
    out_->insert(out_->end(), builder_.GetBufferPointer(), builder_.GetBufferPointer() + builder_.GetSize());
    builder_.Clear();
    rows_.clear();
    cells_.clear();
}
void Encoder::header(const std::vector<std::string>& names) {
    names_ = names;
    page(true);
}
size_t Encoder::pendingBytes() const {
    size_t n = builder_.GetSize() + rows_.size() * 4 + 256;
    for (const auto& name : names_) n += name.size() + 48;
    return n;
}
void Encoder::null() { cells_.push_back(CreateQRPValue(builder_)); }
void Encoder::i64(int64_t v) { cells_.push_back(CreateQRPValue(builder_, qrpValueKind_Int, false, v)); }
void Encoder::real(double v) { cells_.push_back(CreateQRPValue(builder_, qrpValueKind_Float, false, 0, v)); }
void Encoder::text(const void* p, size_t n) {
    auto str = builder_.CreateString(static_cast<const char*>(p), n);
    cells_.push_back(CreateQRPValue(builder_, qrpValueKind_Text, false, 0, 0, str));
}
void Encoder::blob(const void* p, size_t n) {
    auto bytes = builder_.CreateVector(static_cast<const uint8_t*>(p), n);
    cells_.push_back(CreateQRPValue(builder_, qrpValueKind_Bytes, false, 0, 0, 0, bytes));
}
void Encoder::endRow() {
    rows_.push_back(CreateQRPRow(builder_, builder_.CreateVector(cells_)));
    if (pendingBytes() >= kPageTarget) flushBlock();
}
void Encoder::page(bool headerOnly) {
    std::vector<flatbuffers::Offset<QRPColumn>> columns;
    for (const auto& name : names_) columns.push_back(CreateQRPColumn(builder_, builder_.CreateString(name)));
    auto cs = builder_.CreateVector(columns);
    auto rs = builder_.CreateVector(rows_);
    QRPBuilder q(builder_);
    q.add_KIND(qrpKind_Page);
    q.add_REPRESENTATION(qrpRepresentation_FlatBuffer);
    q.add_COLUMNS(cs);
    q.add_ROWS(rs);
    // Each block is a partial view of the statement until its final summary.
    q.add_PARTIAL(true);
    auto root = q.Finish();
    FinishSizePrefixedQRPBuffer(builder_, root);
    append();
    (void)headerOnly;
}
void Encoder::flushBlock() { if (!rows_.empty()) page(); }
void Encoder::end(int32_t status, uint64_t rows, uint64_t examined, uint64_t bytes, const std::string& message) {
    flushBlock();
    auto code = status ? builder_.CreateString(errorCode(status)) : 0;
    auto msg = message.empty() ? 0 : builder_.CreateString(message);
    QRPBuilder q(builder_);
    q.add_KIND(status ? qrpKind_Error : qrpKind_Page);
    q.add_STATUS(status == -6 ? qrpStatus_Busy : status ? qrpStatus_Error : qrpStatus_Ok);
    q.add_ERROR_CODE(code);
    q.add_MESSAGE(msg);
    q.add_TOTAL_COUNT(rows);
    q.add_SCANNED(examined);
    q.add_PARTIAL(status != 0);
    q.add_TRUNCATED(status == -8);
    q.add_REPRESENTATION(qrpRepresentation_FlatBuffer);
    // The summary has no ROWS/COLUMNS. The transport's DONE/EOF ends the
    // stream; clients must read it before treating earlier pages as success.
    FinishSizePrefixedQRPBuffer(builder_, q.Finish());
    append();
    (void)bytes; // I/O accounting remains in the transport's completion slot.
}
bool parameters(const uint8_t* data, size_t size, std::vector<ps::rb1::Cell>* out) {
    out->clear();
    if (!data || size < 8) return false;
    flatbuffers::Verifier verifier(data, size, 64, 1000000, false);
    if (!VerifyQRPBuffer(verifier)) return false;
    const auto* q = GetQRP(data);
    if (q->KIND() != qrpKind_Request || q->REPRESENTATION() == qrpRepresentation_Json) return false;
    if (q->PARAMETERS()) for (const auto* v : *q->PARAMETERS()) {
        ps::rb1::Cell c;
        if (!value(v, &c)) return false;
        out->push_back(std::move(c));
    }
    return true;
}
bool Decoder::feed(const uint8_t* p, size_t n) {
    if (done_ && n) { error_ = "bytes after completion"; return false; }
    pending_.insert(pending_.end(), p, p + n);
    size_t at = 0;
    while (pending_.size() - at >= 4) {
        const auto size = flatbuffers::ReadScalar<uint32_t>(pending_.data() + at);
        if (size > (256u << 20)) { error_ = "oversized QRP frame"; return false; }
        if (pending_.size() - at - 4 < size) break;
        flatbuffers::Verifier v(pending_.data() + at, size + 4);
        if (!VerifySizePrefixedQRPBuffer(v)) { error_ = "invalid QRP frame"; return false; }
        const auto* q = GetSizePrefixedQRP(pending_.data() + at);
        if (q->KIND() == qrpKind_Request) { error_ = "request in result stream"; return false; }
        if (q->COLUMNS()) {
            std::vector<std::string> ns;
            for (const auto* c : *q->COLUMNS()) ns.push_back(c->NAME() ? c->NAME()->str() : "");
            if (!names_.empty() && names_ != ns) { error_ = "columns changed"; return false; }
            names_ = std::move(ns);
        }
        if (q->ROWS()) for (const auto* row : *q->ROWS()) {
            if (!row->CELLS() || row->CELLS()->size() != names_.size()) { error_ = "row width"; return false; }
            std::vector<ps::rb1::Cell> cells;
            for (const auto* c : *row->CELLS()) {
                ps::rb1::Cell cell;
                if (!value(c, &cell)) { error_ = "invalid typed value"; return false; }
                cells.push_back(std::move(cell));
            }
            rows_.push_back(std::move(cells));
        }
        if (!q->COLUMNS() && !q->ROWS()) {
            done_ = true;
            status_ = q->ERROR_CODE() ? errorStatus(q->ERROR_CODE()->c_str()) : 0;
            reported_ = q->TOTAL_COUNT();
        }
        at += size + 4;
        if (done_ && at != pending_.size()) { error_ = "bytes after completion"; return false; }
    }
    pending_.erase(pending_.begin(), pending_.begin() + at);
    return true;
}
}
