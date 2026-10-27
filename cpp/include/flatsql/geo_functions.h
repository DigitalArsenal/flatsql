#ifndef FLATSQL_GEO_FUNCTIONS_H
#define FLATSQL_GEO_FUNCTIONS_H
#include "sqlite3.h"
namespace flatsql {
// Scalar geospatial queries and binary SDS GJN geometry. Full contracts and
// supported operation combinations are documented in docs/API-3.12.md.
void registerGeoFunctions(sqlite3* db);
}
#endif
