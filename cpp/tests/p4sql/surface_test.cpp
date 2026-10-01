// SURFACE (op 31; CONTRACT.md §3.5, §3.6): one row per (relation, column),
// the columns format 1's PublicQuerySurface lists (its engine table graph
// plus _source, _rowid, _offset, _data), the placeholder column marked.
#ifdef FLATSQL_P4SQL_FAKE

#include <fstream>
#include <map>
#include <sstream>

#include "p4sql_test.h"

using namespace p4sqlt;

namespace {

std::vector<std::string> format1Columns(const std::string& table) {
    std::ifstream f(vectorDir() + "/format1_columns.txt");
    std::string line;
    while (std::getline(f, line)) {
        if (line.compare(0, table.size() + 1, table + " ") != 0) continue;
        std::istringstream in(line);
        std::string t, tok;
        in >> t;
        std::vector<std::string> out;
        while (in >> tok) out.push_back(tok.substr(0, tok.find(':')));
        for (const char* m : {"_source", "_rowid", "_offset", "_data"}) out.push_back(m);
        return out;
    }
    return {};
}

void tagged(Harness& h, const std::string& type, int64_t seq, const std::vector<std::string>& sources) {
    p4fake::Rec r;
    r.seq = seq;
    r.cid = "bafkreisurface" + std::to_string(seq);
    for (const std::string& s : sources) {
        p4fake::Tag t;
        t.source = s;
        r.tags.push_back(t);
    }
    h.put(type, r);
}

}  // namespace

P4SQL_TEST(surface_lists_relations_and_format1_columns) {
    Harness h;
    h.addType("OMM", "$OMM", readFile(vectorDir() + "/OMM.bfbs"), 400000);
    h.addType("CAT", "$CAT", readFile(vectorDir() + "/CAT.bfbs"), 10000);
    h.addType("LDM", "$LDM", readFile(vectorDir() + "/LDM.bfbs"), 10000);
    const char none[4] = {0, 0, 0, 0};
    h.addType("VCM", none, readFile(vectorDir() + "/MPE.bfbs"), 10000);   // no file identifier: not routed
    tagged(h, "OMM", 1, {"celestrak-gp"});
    tagged(h, "CAT", 2, {"celestrak-satcat-csv"});
    tagged(h, "CAT", 3, {"celestrak-satcat"});
    const Result r = h.surface();
    CHECK_EQ(r.status, 0);
    CHECK((r.names == std::vector<std::string>{"name", "kind", "source", "column", "placeholder", "bound"}));
    // Expected, relation by relation.
    struct Rel {
        std::string name, kind, source, table;
        int64_t bound;
    };
    const Rel rels[] = {
        {"CAT", "view", "", "CAT", 10000},
        {"CAT@celestrak-satcat", "table", "celestrak-satcat", "CAT", 10000},
        {"CAT@celestrak-satcat-csv", "table", "celestrak-satcat-csv", "CAT", 10000},
        {"LDM", "table", "", "LDM", 10000},
        {"OMM", "view", "", "OMM", 400000},
        {"OMM@celestrak-gp", "table", "celestrak-gp", "OMM", 400000},
    };
    size_t at = 0;
    for (const Rel& rel : rels) {
        const auto cols = format1Columns(rel.table);
        CHECK(!cols.empty());
        for (size_t i = 0; i < cols.size(); i++, at++) {
            if (at >= r.rows.size()) {
                failAt(__FILE__, __LINE__, "SURFACE ends early at " + rel.name);
                return;
            }
            const auto& row = r.rows[at];
            const bool placeholder = rel.table == "LDM" && i == 0;
            const std::vector<rb1::Cell> want = {cText(rel.name), cText(rel.kind), cText(rel.source), cText(cols[i]),
                                                 cInt(placeholder ? 1 : 0), cInt(rel.bound)};
            if (!(row == want)) {
                failAt(__FILE__, __LINE__, "row " + std::to_string(at) + " " + showRow(row) + " want " + showRow(want));
                return;
            }
        }
    }
    CHECK_EQ(r.rows.size(), at);
    CHECK(r.ended && r.endStatus == 0 && r.endRows == at);
}

#endif  // FLATSQL_P4SQL_FAKE
