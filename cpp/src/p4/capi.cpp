// Store format 4: the instance C ABI (CONTRACT §3.3). One engine per instance.
// Control calls (init, start, layout, activate, stop, alloc, free) come from
// the host's exec thread and are never concurrent with each other;
// register_type, set_quota and stats may run while service threads run.
// Errors are return values: no C++ exception crosses this boundary.
#include <cstdlib>

#include "flatsql/p4/flatsql_p4.h"
#include "internal.h"

namespace {
P4Engine* gEngine = nullptr;
std::string gLastError;
}  // namespace

namespace flatsql {
namespace p4 {
// For native tests (the in-process mailbox client).
P4Engine* currentEngine() { return gEngine; }
const std::string& lastError() { return gLastError; }
}  // namespace p4
}  // namespace flatsql

extern "C" {

FLATSQL_P4_EXPORT("flatsql_p4_init") int32_t flatsql_p4_init(const uint8_t* cfg, int32_t cfgLen) {
    if (gEngine) {
        if (gEngine->started && !gEngine->stopping.load()) return P4_E_ARG;  // one engine per instance
        flatsql::p4::engineStop(gEngine, 0);
        delete gEngine;
        gEngine = nullptr;
    }
    if (!cfg || cfgLen <= 0) return P4_E_ARG;
    P4Engine* e = new (std::nothrow) P4Engine();
    if (!e) return P4_E_NOMEM;
    gLastError.clear();
    const int32_t rc = flatsql::p4::engineInit(e, cfg, size_t(cfgLen), &gLastError);
    if (rc != P4_OK) {
        flatsql::p4::engineStop(e, 0);
        delete e;
        return rc;
    }
    gEngine = e;
    return P4_OK;
}

FLATSQL_P4_EXPORT("flatsql_p4_start") int32_t flatsql_p4_start(void) {
    if (!gEngine) return P4_E_STOPPED;
    return flatsql::p4::startThreads(gEngine);
}

FLATSQL_P4_EXPORT("flatsql_p4_layout") int32_t flatsql_p4_layout(FlatsqlP4Layout* out) {
    if (!gEngine || !out) return P4_E_ARG;
    flatsql::p4::mailboxLayout(gEngine, out);
    return int32_t(sizeof(FlatsqlP4Layout));
}

FLATSQL_P4_EXPORT("flatsql_p4_wake") int32_t flatsql_p4_wake(uint32_t* addr, int32_t n) {
    if (!addr) return 0;
#if defined(__wasm__)
    return __builtin_wasm_memory_atomic_notify(reinterpret_cast<int*>(addr), n < 0 ? 0xffffffffu : uint32_t(n));
#else
    flatsql::ps::wakeU32(reinterpret_cast<std::atomic<uint32_t>*>(addr), n);
    return 0;
#endif
}

FLATSQL_P4_EXPORT("flatsql_p4_register_type") int32_t flatsql_p4_register_type(const uint8_t* spec, int32_t specLen) {
    if (!gEngine) return P4_E_STOPPED;
    if (!spec || specLen <= 0) return P4_E_ARG;
    std::string err;
    const int32_t rc = flatsql::p4::engineRegisterType(gEngine, spec, size_t(specLen), &err);
    if (rc != P4_OK) gLastError = err;
    return rc;
}

FLATSQL_P4_EXPORT("flatsql_p4_set_quota") int32_t flatsql_p4_set_quota(double bytes) {
    if (!gEngine) return P4_E_STOPPED;
    if (!(bytes >= 0)) return P4_E_ARG;
    gEngine->quota.store(uint64_t(bytes));
    if (bytes == 0) {
        std::lock_guard<std::mutex> g(gEngine->typesMu);
        for (auto& t : gEngine->types) {
            std::lock_guard<std::mutex> tg(t->mu);
            t->overQuota = false;
        }
    }
    gEngine->kickMaintenance();
    return P4_OK;
}

FLATSQL_P4_EXPORT("flatsql_p4_activate") int32_t flatsql_p4_activate(void) {
    if (!gEngine) return P4_E_STOPPED;
    return flatsql::p4::engineActivate(gEngine);
}

FLATSQL_P4_EXPORT("flatsql_p4_stats") int32_t flatsql_p4_stats(uint8_t* out, int32_t cap) {
    if (!gEngine || !out || cap < 0) return P4_E_ARG;
    return flatsql::p4::engineStats(gEngine, out, cap);
}

FLATSQL_P4_EXPORT("flatsql_p4_stop") int32_t flatsql_p4_stop(double deadlineMs) {
    if (!gEngine) return P4_OK;
    return flatsql::p4::engineStop(gEngine, deadlineMs);
}

FLATSQL_P4_EXPORT("flatsql_p4_alloc") void* flatsql_p4_alloc(int32_t n) {
    return n > 0 ? std::malloc(size_t(n)) : nullptr;
}

FLATSQL_P4_EXPORT("flatsql_p4_free") void flatsql_p4_free(void* p) { std::free(p); }

}  // extern "C"
