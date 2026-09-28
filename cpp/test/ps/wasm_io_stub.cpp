// flatsql-ps-test-memio.wasm only (cmake/flatsql_ps_wasm.cmake): the seven
// flatsql_io functions defined in the guest, so the command imports nothing
// but WASI, wasi.thread-spawn and env.memory and runs on a WASI-only host
// (the SDK WasmEdge C runner). Every call fails with ACCESS: tests that need
// real files (--dir, the C ABI test) belong to flatsql-ps-test.wasm, whose
// host supplies these imports. Deliberately does not include flatsql_io.h:
// its declarations carry the import attributes.
#include <cstdint>

namespace {
constexpr int32_t kAccess = -3;  // FLATSQL_IO_ERR_ACCESS
}

extern "C" {
int32_t flatsql_io_open(const char*, int32_t, int32_t) { return kAccess; }
int32_t flatsql_io_read(int32_t, void*, int32_t, double) { return kAccess; }
int32_t flatsql_io_write(int32_t, const void*, int32_t, double) { return kAccess; }
int32_t flatsql_io_truncate(int32_t, double) { return kAccess; }
int32_t flatsql_io_sync(int32_t) { return kAccess; }
double flatsql_io_size(int32_t) { return kAccess; }
int32_t flatsql_io_close(int32_t) { return kAccess; }
}
