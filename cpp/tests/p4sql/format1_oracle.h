// Format 1 as the oracle: flatsql's own format-1 engine (database.cpp,
// sqlite_vtab.cpp), created from format 1's engine table graph
// (vectors/format1_columns.txt) and fed the way SDN fills its hot window:
// the type's newest N records in seq order, each into the per-source table
// of every source it carries, the payload without its size prefix
// (storage/engine_records.go engineRecordPayload).
#ifndef FLATSQL_P4SQL_FORMAT1_ORACLE_H
#define FLATSQL_P4SQL_FORMAT1_ORACLE_H

#ifdef FLATSQL_P4SQL_FAKE

#include <memory>
#include <string>
#include <vector>

#include "p4sql_test.h"

namespace flatsql {
class FlatSQLDatabase;
}

namespace p4sqlt {

class Format1 {
public:
    // `tables`: type names; `fids`: their 4-byte file identifiers.
    Format1(const std::vector<std::string>& tables, const std::vector<std::string>& fids);
    ~Format1();
    // Registers the sources (in this order: the UNION ALL order) and builds the views.
    void sources(const std::vector<std::string>& names);
    // One record into one source's table.
    void ingest(const std::string& source, const std::vector<uint8_t>& d);
    // Feeds the window of a fake type: its newest `bound` records by seq, each
    // into every source it is tagged with.
    void feedWindow(const p4fake::Type& t);
    bool query(const std::string& sql, const std::vector<rb1::Cell>& params, std::vector<std::string>* names,
               std::vector<std::vector<rb1::Cell>>* rows, std::string* err);
    bool raw(const std::string& sql, const std::vector<rb1::Cell>& params, std::vector<uint8_t>* out, std::string* err);

private:
    std::unique_ptr<flatsql::FlatSQLDatabase> db_;
};

}  // namespace p4sqlt

#endif
#endif
