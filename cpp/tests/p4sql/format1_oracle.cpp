// Format 1 as the oracle: see format1_oracle.h.
#ifdef FLATSQL_P4SQL_FAKE

#include "format1_oracle.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <map>
#include <set>
#include <sstream>

#include "flatsql/database.h"
#include "flatsql/schema_parser.h"

namespace p4sqlt {

namespace {

std::string graphText(const std::vector<std::string>& tables) {
    std::map<std::string, std::string> lines;
    std::ifstream f(vectorDir() + "/format1_columns.txt");
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty() || line[0] == '#') continue;
        lines[line.substr(0, line.find(' '))] = line;
    }
    std::string text;
    for (const std::string& t : tables) {
        std::istringstream in(lines.at(t));
        std::string name, tok;
        in >> name;
        text += "table " + name + " {\n";
        while (in >> tok) text += "  " + tok + ";\n";
        text += "}\n";
    }
    return text;
}

flatsql::Value valueOf(const rb1::Cell& c) {
    switch (c.type) {
        case rb1::kInt: return c.i;
        case rb1::kReal: return c.d;
        case rb1::kText: return c.s;
        case rb1::kBlob: return std::vector<uint8_t>(c.s.begin(), c.s.end());
        default: return std::monostate{};
    }
}

rb1::Cell cellOf(const flatsql::Value& v) {
    rb1::Cell c;
    std::visit(
        [&](const auto& x) {
            using T = std::decay_t<decltype(x)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
                c.type = rb1::kNull;
            } else if constexpr (std::is_same_v<T, bool>) {
                c.type = rb1::kInt;
                c.i = x ? 1 : 0;
            } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
                c.type = rb1::kReal;
                c.d = double(x);
            } else if constexpr (std::is_same_v<T, std::string>) {
                c.type = rb1::kText;
                c.s = x;
            } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                c.type = rb1::kBlob;
                c.s.assign(x.begin(), x.end());
            } else {
                c.type = rb1::kInt;
                c.i = int64_t(x);
            }
        },
        v);
    return c;
}

std::vector<flatsql::Value> valuesOf(const std::vector<rb1::Cell>& params) {
    std::vector<flatsql::Value> out;
    for (const rb1::Cell& c : params) out.push_back(valueOf(c));
    return out;
}

// storage/engine_records.go engineRecordPayload (the type's own identifier).
std::vector<uint8_t> payload(const std::vector<uint8_t>& d) {
    if (d.size() >= 12) {
        uint32_t n;
        std::memcpy(&n, d.data(), 4);
        if (uint64_t(n) + 4 == d.size()) return std::vector<uint8_t>(d.begin() + 4, d.end());
    }
    return d;
}

}  // namespace

Format1::Format1(const std::vector<std::string>& tables, const std::vector<std::string>& fids) {
    flatsql::DatabaseSchema schema = flatsql::SchemaParser::parseIDL(graphText(tables), "oracle");
    db_.reset(new flatsql::FlatSQLDatabase(schema));
    for (size_t i = 0; i < tables.size(); i++) db_->registerFileId(fids[i], tables[i]);
}

Format1::~Format1() = default;

void Format1::sources(const std::vector<std::string>& names) {
    for (const std::string& n : names) db_->registerSource(n);
    db_->createUnifiedViews();
}

void Format1::ingest(const std::string& source, const std::vector<uint8_t>& d) {
    const std::vector<uint8_t> p = payload(d);
    db_->ingestOneWithSource(p.data(), p.size(), source);
}

void Format1::feedWindow(const p4fake::Type& t) {
    const size_t n = t.recs.size();
    const size_t first = t.bound && n > t.bound ? n - size_t(t.bound) : 0;
    for (size_t i = first; i < n; i++) {
        std::set<std::string> srcs;
        for (const p4fake::Tag& tag : t.recs[i].tags) srcs.insert(tag.source);
        for (const std::string& s : srcs) ingest(s, t.recs[i].data);
    }
}

bool Format1::query(const std::string& sql, const std::vector<rb1::Cell>& params, std::vector<std::string>* names,
                    std::vector<std::vector<rb1::Cell>>* rows, std::string* err) {
    flatsql::QueryResult r;
    if (!db_->queryNoThrow(sql, valuesOf(params), r, err)) return false;
    *names = r.columns;
    rows->clear();
    for (const auto& row : r.rows) {
        std::vector<rb1::Cell> out;
        for (const auto& v : row) out.push_back(cellOf(v));
        rows->push_back(std::move(out));
    }
    return true;
}

bool Format1::raw(const std::string& sql, const std::vector<rb1::Cell>& params, std::vector<uint8_t>* out,
                  std::string* err) {
    flatsql::FlatSQLDatabase::RawStreamResult r;
    if (!db_->queryRawFlatBufferStream(sql, valuesOf(params), &r, err)) return false;
    out->assign(r.stream->begin(), r.stream->end());
    return true;
}

}  // namespace p4sqlt

#endif  // FLATSQL_P4SQL_FAKE
