// Store format 4: the instance C ABI (CONTRACT §3.3). One engine per instance.
// Control calls (init, start, layout, activate, stop, alloc, free) come from
// the host's exec thread and are never concurrent with each other;
// register_type, set_quota and stats may run while service threads run.
// Errors are return values: no C++ exception crosses this boundary.
#include <cstdlib>

#include "flatsql/flatsql.h"
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

FLATSQL_EXPORT("flatsql_init") int32_t flatsql_init(const uint8_t* cfg, int32_t cfgLen) {
    if (gEngine) {
        if (gEngine->started && !gEngine->stopping.load()) return FLATSQL_E_ARG;  // one engine per instance
        flatsql::p4::engineStop(gEngine, 0);
        delete gEngine;
        gEngine = nullptr;
    }
    if (!cfg || cfgLen <= 0) return FLATSQL_E_ARG;
    P4Engine* e = new (std::nothrow) P4Engine();
    if (!e) return FLATSQL_E_NOMEM;
    gLastError.clear();
    const int32_t rc = flatsql::p4::engineInit(e, cfg, size_t(cfgLen), &gLastError);
    if (rc != FLATSQL_OK) {
        flatsql::p4::engineStop(e, 0);
        delete e;
        return rc;
    }
    gEngine = e;
    return FLATSQL_OK;
}

FLATSQL_EXPORT("flatsql_start") int32_t flatsql_start(void) {
    if (!gEngine) return FLATSQL_E_STOPPED;
    return flatsql::p4::startThreads(gEngine);
}

FLATSQL_EXPORT("flatsql_layout") int32_t flatsql_layout(FlatSQLLayout* out) {
    if (!gEngine || !out) return FLATSQL_E_ARG;
    flatsql::p4::mailboxLayout(gEngine, out);
    return int32_t(sizeof(FlatSQLLayout));
}

FLATSQL_EXPORT("flatsql_wake") int32_t flatsql_wake(uint32_t* addr, int32_t n) {
    if (!addr) return 0;
#if defined(__wasm__)
    return __builtin_wasm_memory_atomic_notify(reinterpret_cast<int*>(addr), n < 0 ? 0xffffffffu : uint32_t(n));
#else
    flatsql::ps::wakeU32(reinterpret_cast<std::atomic<uint32_t>*>(addr), n);
    return 0;
#endif
}

FLATSQL_EXPORT("flatsql_register_type") int32_t flatsql_register_type(const uint8_t* spec, int32_t specLen) {
    if (!gEngine) return FLATSQL_E_STOPPED;
    if (!spec || specLen <= 0) return FLATSQL_E_ARG;
    std::string err;
    const int32_t rc = flatsql::p4::engineRegisterType(gEngine, spec, size_t(specLen), &err);
    if (rc != FLATSQL_OK) gLastError = err;
    return rc;
}

FLATSQL_EXPORT("flatsql_set_quota") int32_t flatsql_set_quota(double bytes) {
    if (!gEngine) return FLATSQL_E_STOPPED;
    if (!(bytes >= 0)) return FLATSQL_E_ARG;
    gEngine->quota.store(uint64_t(bytes));
    if (bytes == 0) {
        std::lock_guard<std::mutex> g(gEngine->typesMu);
        for (auto& t : gEngine->types) {
            std::lock_guard<std::mutex> tg(t->mu);
            t->overQuota = false;
        }
    }
    gEngine->kickMaintenance();
    return FLATSQL_OK;
}

FLATSQL_EXPORT("flatsql_activate") int32_t flatsql_activate(void) {
    if (!gEngine) return FLATSQL_E_STOPPED;
    gLastError.clear();
    return flatsql::p4::engineActivate(gEngine, &gLastError);
}

FLATSQL_EXPORT("flatsql_stats") int32_t flatsql_stats(uint8_t* out, int32_t cap) {
    if (!gEngine || !out || cap < 0) return FLATSQL_E_ARG;
    return flatsql::p4::engineStats(gEngine, out, cap);
}

FLATSQL_EXPORT("flatsql_stop") int32_t flatsql_stop(double deadlineMs) {
    if (!gEngine) return FLATSQL_OK;
    return flatsql::p4::engineStop(gEngine, deadlineMs);
}

FLATSQL_EXPORT("flatsql_alloc") void* flatsql_alloc(int32_t n) {
    return n > 0 ? std::malloc(size_t(n)) : nullptr;
}

FLATSQL_EXPORT("flatsql_free") void flatsql_free(void* p) { std::free(p); }

FLATSQL_EXPORT("flatsql_ingest_request") int32_t flatsql_ingest_request(
    const uint8_t* frames, int32_t size, const uint8_t* metadata, int32_t metadataSize, uint8_t** output, uint32_t* outLen) {
    using namespace flatsql::p4;
    if (!gEngine) return FLATSQL_E_STOPPED;
    if (!frames || size < 12 || metadataSize < 0 || (!metadata && metadataSize) || !output || !outLen) return FLATSQL_E_ARG;
    *output = nullptr;
    *outLen = 0;
    if (uint32_t(size) > gEngine->cfg.writeReqBytes) return FLATSQL_E_BUDGET;
    std::vector<Tlv> tags;
    if (!tlvParse(metadata, size_t(metadataSize), &tags)) return FLATSQL_E_ARG;
    std::string name;
    if (!tlvText(tags, 1, &name) || name.empty()) return FLATSQL_E_ARG;
    int64_t at = nowSec();
    bool bad = false;
    uint64_t timestamp;
    if (tlvU64(tags, 54, &timestamp, &bad)) at = int64_t(timestamp);
    if (bad) return FLATSQL_E_ARG;
    std::vector<uint8_t> request;
    for (const auto& tag : tags) {
        if (tag.tag != 1 && tag.tag != 50 && tag.tag != 51 && tag.tag != 54) return FLATSQL_E_ARG;
        tlvPut(request, tag.tag, tag.v, tag.n);
    }
    uint8_t time[8]; st64(time, uint64_t(at));
    if (!tlvFind(tags, 54)) tlvPut(request, 54, time, sizeof time);
    std::vector<uint8_t> records(4, 0);
    uint32_t count = 0;
    for (size_t offset = 0; offset < size_t(size); ++count) {
        if (size_t(size) - offset < 4) return FLATSQL_E_ARG;
        const size_t n = ld32(frames + offset);
        if (n < 8 || n > size_t(size) - offset - 4) return FLATSQL_E_ARG;
        const size_t start = records.size();
        // flags/reserved, CIDv1 raw SHA-256, timestamp, frame size, frame,
        // empty signature. The storage engine verifies and commits this batch.
        if (request.size() + start + n + 64 > gEngine->cfg.writeReqBytes) return FLATSQL_E_BUDGET;
        records.resize(start + 52 + n + 4 + 2, 0);
        uint8_t* out = records.data() + start;
        out[4] = 1; out[5] = 0x55; out[6] = 0x12; out[7] = 0x20;
        flatsql::ps::sha256(frames + offset + 4, n, out + 8);
        st64(out + 40, uint64_t(at));
        st32(out + 48, uint32_t(n + 4));
        std::memcpy(out + 52, frames + offset, n + 4);
        offset += n + 4;
    }
    st32(records.data(), count);
    tlvPut(request, 53, records.data(), uint32_t(records.size()));
    if (request.size() > gEngine->cfg.writeReqBytes) return FLATSQL_E_BUDGET;
    auto* bytes = static_cast<uint8_t*>(std::malloc(request.size()));
    if (!bytes) return FLATSQL_E_NOMEM;
    std::memcpy(bytes, request.data(), request.size());
    *outLen = uint32_t(request.size());
    *output = bytes;
    return FLATSQL_OK;
}

}  // extern "C"
