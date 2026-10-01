// Format 1's relation columns from the BFBS (CONTRACT.md §3.9, C-9): names
// and the way format 1's engine reads each one, against the table graph
// format 1 creates its engine from (vectors/format1_columns.txt).
#ifdef FLATSQL_P4SQL_FAKE

#include <dirent.h>

#include <fstream>
#include <map>
#include <sstream>

#include "../../src/p4sql/internal.h"
#include "p4sql_test.h"

using namespace p4sqlt;
using flatsql::p4sql::Column;
using flatsql::p4sql::ColKind;

namespace {

struct F1Col {
    std::string name, type;
};

std::map<std::string, std::vector<F1Col>> format1Tables() {
    std::map<std::string, std::vector<F1Col>> out;
    std::ifstream f(vectorDir() + "/format1_columns.txt");
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty() || line[0] == '#') continue;
        std::istringstream in(line);
        std::string table, tok;
        in >> table;
        auto& cols = out[table];
        while (in >> tok) {
            const size_t colon = tok.find(':');
            cols.push_back({tok.substr(0, colon), tok.substr(colon + 1)});
        }
    }
    return out;
}

// What format 1's schema parser (schema_parser.cpp idlTypeToValueType) reads
// for a type name of its engine table graph.
ColKind format1Kind(std::string t) {
    for (char& c : t) c = char(std::tolower(uint8_t(c)));
    if (t == "bool") return flatsql::p4sql::kColBool;
    if (t == "byte" || t == "int8") return flatsql::p4sql::kColI8;
    if (t == "ubyte" || t == "uint8") return flatsql::p4sql::kColU8;
    if (t == "short" || t == "int16") return flatsql::p4sql::kColI16;
    if (t == "ushort" || t == "uint16") return flatsql::p4sql::kColU16;
    if (t == "int" || t == "int32") return flatsql::p4sql::kColI32;
    if (t == "uint" || t == "uint32") return flatsql::p4sql::kColU32;
    if (t == "long" || t == "int64") return flatsql::p4sql::kColI64;
    if (t == "ulong" || t == "uint64") return flatsql::p4sql::kColU64;
    if (t == "float" || t == "float32") return flatsql::p4sql::kColF32;
    if (t == "double" || t == "float64") return flatsql::p4sql::kColF64;
    if (t == "string") return flatsql::p4sql::kColText;
    if (t.find("[ubyte]") != std::string::npos || t.find("[uint8]") != std::string::npos ||
        t.find("[byte]") != std::string::npos)
        return flatsql::p4sql::kColBlob;
    return flatsql::p4sql::kColNull;
}

// Compares one type; returns the number of differences (printed).
int compareType(const std::string& type, const std::vector<uint8_t>& bfbs, const std::vector<F1Col>& want,
                bool print) {
    std::vector<Column> got;
    std::string err;
    if (!flatsql::p4sql::format1Columns(type, bfbs.data(), bfbs.size(), &got, &err)) {
        if (print) std::fprintf(stderr, "    %s: %s\n", type.c_str(), err.c_str());
        return 1;
    }
    int diffs = 0;
    if (got.size() != want.size()) {
        if (print) std::fprintf(stderr, "    %s: %zu columns, format 1 has %zu\n", type.c_str(), got.size(), want.size());
        diffs++;
    }
    for (size_t i = 0; i < std::min(got.size(), want.size()); i++) {
        if (got[i].name != want[i].name || got[i].kind != format1Kind(want[i].type) || got[i].slot != i) {
            if (print)
                std::fprintf(stderr, "    %s column %zu: %s kind %d slot %u, format 1 %s:%s\n", type.c_str(), i,
                             got[i].name.c_str(), int(got[i].kind), unsigned(got[i].slot), want[i].name.c_str(),
                             want[i].type.c_str());
            diffs++;
        }
    }
    return diffs;
}

}  // namespace

// Fixture types, the pinned OMM and TBS graphs, a placeholder (LDM) and a
// union discriminator ending the run (RFM).
P4SQL_TEST(columns_equal_format1_vectors) {
    const auto tables = format1Tables();
    CHECK_EQ(tables.size(), size_t(235));
    for (const char* t : {"OMM", "MPE", "CAT", "IQC", "TBS", "LDM", "RFM"}) {
        auto it = tables.find(t);
        CHECK(it != tables.end());
        if (it == tables.end()) continue;
        CHECK_EQ(compareType(t, readFile(vectorDir() + "/" + t + ".bfbs"), it->second, true), 0);
    }
    std::vector<Column> ldm;
    std::string err;
    const auto bfbs = readFile(vectorDir() + "/LDM.bfbs");
    CHECK(flatsql::p4sql::format1Columns("LDM", bfbs.data(), bfbs.size(), &ldm, &err));
    CHECK_EQ(ldm.size(), size_t(1));
    if (!ldm.empty()) {
        CHECK_EQ(ldm[0].name, std::string("SITE"));
        CHECK(ldm[0].placeholder);
    }
    std::vector<Column> omm;
    const auto ob = readFile(vectorDir() + "/OMM.bfbs");
    CHECK(flatsql::p4sql::format1Columns("OMM", ob.data(), ob.size(), &omm, &err));
    for (const Column& c : omm) CHECK(!c.placeholder);
}

// Every standard SDN routes: P4SQL_SEARCH_SCHEMAS = SDN's
// internal/sds/search-schemas (the BFBS format 4's type specs carry).
P4SQL_TEST(columns_equal_format1_every_standard) {
    const std::string dir = env("P4SQL_SEARCH_SCHEMAS");
    if (dir.empty()) {
        std::printf("    SKIP: P4SQL_SEARCH_SCHEMAS not set\n");
        return;
    }
    const auto tables = format1Tables();
    int compared = 0, missing = 0, diffs = 0;
    for (const auto& kv : tables) {
        const std::string path = dir + "/" + kv.first + ".bfbs";
        std::ifstream probe(path);
        if (!probe) {
            missing++;
            std::printf("    no BFBS for %s\n", kv.first.c_str());
            continue;
        }
        compared++;
        if (compareType(kv.first, readFile(path), kv.second, true)) diffs++;
    }
    std::printf("    %d standards compared, %d without a BFBS, %d differ\n", compared, missing, diffs);
    CHECK_EQ(diffs, 0);
    CHECK(compared >= 230);
}

#endif  // FLATSQL_P4SQL_FAKE
