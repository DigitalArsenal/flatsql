// FlatSQL partition store: SQL surface of a lane (design §9, A17, A18, A28).
//
// Virtual tables, created lazily per lane connection (in the temp schema)
// the first time a statement names them:
//   sds_p_<producer>__<TYPE>   one partition (partition-level visibility)
//   <TYPE>                     the fan-out over every partition of a type:
//                              FIRST copies only, one row per live cid,
//                              k-way merged in index order (default order:
//                              floor(epoch/1000) DESC, text CID ASC, A19)
//   <TYPE>@<source>            <TYPE> restricted to one source (alias)
//   <TYPE>_current             latest live record per supersede key, or per
//                              object key for types without supersede rules
//   flatsql_partitions         counters from heads, O(partitions)
//   flatsql_lanes              lane counters per tag tuple
//   flatsql_licences           live LICENCE rows
//   flatsql_arrivals           a type's arrivals log (gseq order)
// Record vtabs expose the schema columns (root table fields in vtable-slot
// order) plus hidden columns _pseq, _cid, _cid_bin, _epoch, _arrival, _gseq,
// _producer, _source, _source_name, _provider, _batch, _peer_id, _signature,
// _data, _offset, _len, _kind, _rowid, _pid.
//
// Every plan records whether it is index-bounded in its idxStr ('B' or 'U'
// first); admission (admission.cpp) reads it from EXPLAIN before a
// statement runs.
#ifndef FLATSQL_PS_VTAB_H
#define FLATSQL_PS_VTAB_H

#include <cstdint>
#include <string>

struct sqlite3;

namespace flatsql {
namespace ps {

class Lane;

// Registers the flatsql_ps module on a lane's connection.
int vtabRegister(sqlite3* db, Lane* lane);
// Creates the virtual table for `name` if it names one (see above). Returns
// 1 when created, 0 when the name is not a store table, < 0 on error.
// `sandbox`: untrusted statements may not create flatsql_* meta tables.
int vtabEnsure(Lane* lane, const std::string& name, bool sandbox, std::string* err);
// Whether a table name is part of the public (sandbox) surface.
bool vtabPublicName(Lane* lane, const std::string& name);

}  // namespace ps
}  // namespace flatsql

#endif
