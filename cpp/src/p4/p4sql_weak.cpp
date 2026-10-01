// Weak defaults for the SQL surface hooks (CONTRACT.md §3.9). The engine's
// CMake globs cpp/src/p4sql/*.cpp into the engine; their strong definitions
// replace these. Without the SQL surface, ops 30 (SQL) and 31 (SURFACE)
// return P4_E_UNSUPPORTED.
#include "sql_bridge.h"

extern "C" {

__attribute__((weak)) int32_t p4sql_global_init(void) { return P4_OK; }
__attribute__((weak)) int32_t p4sql_lane_init(P4Lane*) { return P4_OK; }
__attribute__((weak)) int32_t p4sql_exec(P4Lane*, const P4SqlRequest*) { return P4_E_UNSUPPORTED; }
__attribute__((weak)) int32_t p4sql_surface(P4Lane*) { return P4_E_UNSUPPORTED; }
__attribute__((weak)) void p4sql_lane_free(P4Lane*) {}

}
