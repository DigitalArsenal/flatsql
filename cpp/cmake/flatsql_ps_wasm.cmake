# Partition store for wasm32-wasip1-threads (T4, docs/PARTITION-STORE-WASM.md).
# Included by CMakeLists.txt when the WASI toolchain is active
# (cmake/wasi-threads-toolchain.cmake); that toolchain builds only:
#
#   flatsql_ps_threads      flatsql-ps-threads.wasm: the engine artifact, a
#                           reactor whose imports are WASI preview1,
#                           wasi.thread-spawn, the shared env.memory and the
#                           seven env.flatsql_io_* (scripts/check-wasm-imports.mjs)
#   flatsql_ps_test_wasm    flatsql-ps-test.wasm: the native flatsql_ps_test
#                           suite as a wasi-threads command (the Node host runs
#                           it: wasm/ps-node-host.mjs)
#   flatsql_ps_test_memio   flatsql-ps-test-memio.wasm: the same command with
#                           the host I/O imports resolved in the guest, so a
#                           WASI-only host (the SDK WasmEdge C runner) runs the
#                           in-memory tests and the deterministic parity vectors
#
# Sources are globbed so engine and test files added by other tasks build here
# without edits to this file.

if(NOT CMAKE_SYSTEM_NAME STREQUAL "WASI")
    message(FATAL_ERROR "flatsql_ps_wasm.cmake needs the WASI toolchain")
endif()

set(FLATSQL_ROOT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")
get_filename_component(FLATSQL_ROOT_DIR "${FLATSQL_ROOT_DIR}" ABSOLUTE)
get_filename_component(FLATSQL_FB_ABS "${FLATBUFFERS_DIR}" ABSOLUTE)

# Reproducible bytes: no build paths in the output, no asserts.
set(FLATSQL_PS_WASM_FLAGS
    -pthread -matomics -mbulk-memory -fno-exceptions
    -ffile-prefix-map=${FLATSQL_ROOT_DIR}=flatsql
    -ffile-prefix-map=${FLATSQL_FB_ABS}=flatbuffers
    -ffile-prefix-map=${WASI_SDK_PREFIX}=wasi-sdk
)

# ---- SQLite: the per-lane in-memory SQL layer (ruling 1) ----------------------
# Multi-thread mode with real pthread mutexes, no memory statistics, no WAL,
# temp storage in memory. SQLITE_OS_OTHER: no unix VFS (it would import WASI
# file calls); lanes open ":memory:" on the flatsql_ps_null VFS
# (src/ps/lane_arena.cpp); sqlite3_os_init and the pthread mutexes
# (SQLITE_OS_OTHER compiles only no-op ones) live in src/ps/capi_wasm.cpp.
add_library(sqlite3_ps_wasm STATIC ${SQLITE_DIR}/sqlite3.c)
target_include_directories(sqlite3_ps_wasm PUBLIC ${SQLITE_DIR})
target_compile_options(sqlite3_ps_wasm PRIVATE ${FLATSQL_PS_WASM_FLAGS} -w)
target_compile_definitions(sqlite3_ps_wasm PRIVATE
    SQLITE_THREADSAFE=2
    SQLITE_OS_OTHER=1
    SQLITE_DEFAULT_MEMSTATUS=0
    SQLITE_OMIT_WAL=1
    SQLITE_TEMP_STORE=3
    SQLITE_OMIT_LOAD_EXTENSION=1
    SQLITE_OMIT_DEPRECATED=1
    SQLITE_OMIT_SHARED_CACHE=1
    SQLITE_DQS=0
)

# ---- engine ------------------------------------------------------------------
# An OBJECT library: every object reaches the link, so the export_name ABI of
# capi_ps.cpp is exported without a reference to it. capi_wasm.cpp (the wasm
# entry glue: allocator exports, SQLite OS hooks, thread stacks) is each
# executable's own source.
file(GLOB FLATSQL_PS_WASM_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/ps/*.cpp")
list(FILTER FLATSQL_PS_WASM_SOURCES EXCLUDE REGEX "/capi_wasm\\.cpp$")
set(FLATSQL_PS_WASM_ENTRY "${CMAKE_CURRENT_SOURCE_DIR}/src/ps/capi_wasm.cpp")
add_library(flatsql_ps_wasm OBJECT
    ${FLATSQL_PS_WASM_SOURCES}
    ${FLATBUFFERS_SRC_DIR}/reflection.cpp
)
target_include_directories(flatsql_ps_wasm PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${FLATBUFFERS_INCLUDE_DIR}
)
target_compile_options(flatsql_ps_wasm PRIVATE ${FLATSQL_PS_WASM_FLAGS}
    -Wall -Wextra -Wno-unused-parameter)
target_link_libraries(flatsql_ps_wasm PUBLIC sqlite3_ps_wasm)

# Shared memory: imported as env.memory, shared, at most 32768 pages (2 GiB,
# design §18 T4). The initial size is the smallest memory a host may pass; a
# host grows the heap before starting threads with flatsql_ps_alloc/free.
set(FLATSQL_PS_THREADS_LINK
    -pthread
    -Wl,--strip-debug
    -Wl,--import-memory
    -Wl,--export-memory
    -Wl,--shared-memory
    -Wl,--max-memory=2147483648
    -Wl,--initial-memory=33554432
    -Wl,-z,stack-size=1048576
)

add_executable(flatsql_ps_threads ${FLATSQL_PS_WASM_ENTRY})
target_compile_options(flatsql_ps_threads PRIVATE ${FLATSQL_PS_WASM_FLAGS})
target_link_libraries(flatsql_ps_threads PRIVATE flatsql_ps_wasm)
target_link_options(flatsql_ps_threads PRIVATE ${FLATSQL_PS_THREADS_LINK} -mexec-model=reactor)
set_target_properties(flatsql_ps_threads PROPERTIES
    OUTPUT_NAME "flatsql-ps-threads"
    SUFFIX ".wasm"
)

# ---- test commands -----------------------------------------------------------
file(GLOB FLATSQL_PS_WASM_TEST_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/test/ps/*_test.cpp")
list(APPEND FLATSQL_PS_WASM_TEST_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/test/ps/ps_test_main.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/test/ps/ps_fixtures.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/test/ps/reader_fixtures.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/test/io_fault.cpp
)
add_library(flatsql_ps_wasm_testutil STATIC
    ${FLATBUFFERS_SRC_DIR}/idl_parser.cpp
    ${FLATBUFFERS_SRC_DIR}/idl_gen_text.cpp
    ${FLATBUFFERS_SRC_DIR}/util.cpp
    ${FLATBUFFERS_SRC_DIR}/file_manager.cpp
    ${FLATBUFFERS_SRC_DIR}/file_name_manager.cpp
    ${FLATBUFFERS_SRC_DIR}/options.cpp
)
target_include_directories(flatsql_ps_wasm_testutil PUBLIC ${FLATBUFFERS_INCLUDE_DIR})
target_compile_options(flatsql_ps_wasm_testutil PRIVATE ${FLATSQL_PS_WASM_FLAGS} -w)
add_library(flatsql_ps_wasm_tests OBJECT ${FLATSQL_PS_WASM_TEST_SOURCES})
target_include_directories(flatsql_ps_wasm_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/test
    ${FLATBUFFERS_INCLUDE_DIR}
    ${SQLITE_DIR}
)
target_compile_options(flatsql_ps_wasm_tests PRIVATE ${FLATSQL_PS_WASM_FLAGS})
target_compile_definitions(flatsql_ps_wasm_tests PRIVATE
    PS_VECTOR_DIR="${CMAKE_CURRENT_SOURCE_DIR}/test/ps/vectors")

# Test commands grow past 2 GiB (in-memory stores); wasm32 allows 4 GiB.
set(FLATSQL_PS_TEST_LINK
    -pthread
    -Wl,--strip-debug
    -Wl,--import-memory
    -Wl,--export-memory
    -Wl,--shared-memory
    -Wl,--max-memory=4294967296
    -Wl,--initial-memory=67108864
    -Wl,-z,stack-size=4194304
)
add_executable(flatsql_ps_test_wasm ${FLATSQL_PS_WASM_ENTRY} $<TARGET_OBJECTS:flatsql_ps_wasm_tests>)
target_compile_options(flatsql_ps_test_wasm PRIVATE ${FLATSQL_PS_WASM_FLAGS})
target_link_libraries(flatsql_ps_test_wasm PRIVATE flatsql_ps_wasm flatsql_ps_wasm_testutil)
target_link_options(flatsql_ps_test_wasm PRIVATE ${FLATSQL_PS_TEST_LINK})
set_target_properties(flatsql_ps_test_wasm PROPERTIES
    OUTPUT_NAME "flatsql-ps-test"
    SUFFIX ".wasm"
)

add_executable(flatsql_ps_test_memio
    ${FLATSQL_PS_WASM_ENTRY} $<TARGET_OBJECTS:flatsql_ps_wasm_tests>
    ${CMAKE_CURRENT_SOURCE_DIR}/test/ps/wasm_io_stub.cpp)
target_compile_options(flatsql_ps_test_memio PRIVATE ${FLATSQL_PS_WASM_FLAGS})
target_link_libraries(flatsql_ps_test_memio PRIVATE flatsql_ps_wasm flatsql_ps_wasm_testutil)
target_link_options(flatsql_ps_test_memio PRIVATE ${FLATSQL_PS_TEST_LINK})
set_target_properties(flatsql_ps_test_memio PROPERTIES
    OUTPUT_NAME "flatsql-ps-test-memio"
    SUFFIX ".wasm"
)
