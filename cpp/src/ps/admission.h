// FlatSQL partition store: admission helpers (admission.cpp).
#ifndef FLATSQL_PS_ADMISSION_H
#define FLATSQL_PS_ADMISSION_H

#include <cstdint>
#include <string>

#include "flatsql/ps/snapshot.h"

struct sqlite3;
struct sqlite3_stmt;

namespace flatsql {
namespace ps {

class ReaderLane;

// Every VFilter of the statement's program has a bounded plan ('B').
int32_t planBounded(sqlite3_stmt* stmt, bool* bounded, std::string* unboundedTable);

struct SandboxAuth {
    ReaderLane* lane = nullptr;
    sqlite3* db = nullptr;
    std::string violation;
};
int sandboxAuthorizer(void* user, int action, const char* a1, const char* a2, const char* db, const char* trigger);

}  // namespace ps
}  // namespace flatsql

#endif
