# Store format 4 for wasm32-wasip1-threads (docs/STORE-FORMAT-4.md).
# Included by CMakeLists.txt when the WASI toolchain is active
# (cmake/wasi-threads-toolchain.cmake), next to flatsql_ps_wasm.cmake:
#
#   flatsql_p4_threads    flatsql-p4-threads.wasm: the engine artifact, a
#                         reactor whose imports are WASI preview1 (clocks,
#                         random, proc_exit; no file descriptor is opened),
#                         wasi.thread-spawn, the shared env.memory and the seven
#                         env.flatsql_io_* (scripts/check-wasm-imports.mjs).
#                         Exports flatsql_p4_*, _initialize, wasi_thread_start.
#   flatsql_p4_test_wasm  flatsql-p4-test.wasm: the cpp/tests/p4 suite (and the
#                         SQL surface's engine tests) as a wasi-threads command
#                         (the Node host runs it: wasm/ps-node-host.mjs).
#
# Sources are globbed: src/p4 (the engine), src/p4sql (the SQL surface; without
# it the engine's weak defaults answer SQL with P4_E_UNSUPPORTED), tests/p4 and
# tests/p4sql.

if(NOT CMAKE_SYSTEM_NAME STREQUAL "WASI")
    message(FATAL_ERROR "flatsql_p4_wasm.cmake needs the WASI toolchain")
endif()

set(FLATSQL_P4_ROOT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")
get_filename_component(FLATSQL_P4_ROOT_DIR "${FLATSQL_P4_ROOT_DIR}" ABSOLUTE)
get_filename_component(FLATSQL_P4_FB_ABS "${FLATBUFFERS_DIR}" ABSOLUTE)

# Reproducible bytes: no build paths in the output, no asserts.
set(FLATSQL_P4_WASM_FLAGS
    -pthread -matomics -mbulk-memory -fno-exceptions
    -ffile-prefix-map=${FLATSQL_P4_ROOT_DIR}=flatsql
    -ffile-prefix-map=${FLATSQL_P4_FB_ABS}=flatbuffers
    -ffile-prefix-map=${WASI_SDK_PREFIX}=wasi-sdk
)

# ---- SQLite 3.53.4, unmodified ------------------------------------------------
# Multi-thread mode (a connection is used by one thread at a time), WAL, FTS5,
# memory statistics (the engine's heap limits), temp storage in memory.
# SQLITE_OS_OTHER: no unix VFS (it would import WASI file calls); the only VFS
# is flatsql_io, registered as the default by src/p4/capi_wasm.cpp, which also
# installs pthread mutexes (SQLITE_OS_OTHER compiles only no-op ones).
add_library(sqlite3_p4_wasm STATIC ${SQLITE_DIR}/sqlite3.c)
target_include_directories(sqlite3_p4_wasm PUBLIC ${SQLITE_DIR})
target_compile_options(sqlite3_p4_wasm PRIVATE ${FLATSQL_P4_WASM_FLAGS} -w)
target_compile_definitions(sqlite3_p4_wasm PRIVATE
    SQLITE_THREADSAFE=2
    SQLITE_OS_OTHER=1
    SQLITE_DEFAULT_MEMSTATUS=1
    SQLITE_TEMP_STORE=3
    SQLITE_OMIT_LOAD_EXTENSION=1
    SQLITE_OMIT_DEPRECATED=1
    SQLITE_OMIT_SHARED_CACHE=1
    SQLITE_DQS=0
    SQLITE_ENABLE_FTS5=1
)

# ---- engine ------------------------------------------------------------------
file(GLOB FLATSQL_P4_WASM_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/p4/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/p4sql/*.cpp")
list(FILTER FLATSQL_P4_WASM_SOURCES EXCLUDE REGEX "/capi_wasm\\.cpp$")
set(FLATSQL_P4_WASM_ENTRY "${CMAKE_CURRENT_SOURCE_DIR}/src/p4/capi_wasm.cpp")
add_library(flatsql_p4_wasm OBJECT
    ${FLATSQL_P4_WASM_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/src/typecfg/extract.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ps/platform.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ps/result_block.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/flatsql_vfs.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/record_search.cpp
    ${FLATBUFFERS_SRC_DIR}/reflection.cpp
)
target_include_directories(flatsql_p4_wasm PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${FLATBUFFERS_INCLUDE_DIR}
)
target_compile_options(flatsql_p4_wasm PRIVATE ${FLATSQL_P4_WASM_FLAGS} -Wall -Wextra -Wno-unused-parameter)
target_link_libraries(flatsql_p4_wasm PUBLIC sqlite3_p4_wasm)

# Shared memory imported as env.memory, shared, at most 32768 pages (2 GiB).
# The initial size is the smallest memory a host may pass; a host grows the
# heap before flatsql_p4_start with flatsql_p4_alloc/free.
set(FLATSQL_P4_THREADS_LINK
    -pthread
    -Wl,--strip-debug
    -Wl,--import-memory
    -Wl,--export-memory
    -Wl,--shared-memory
    -Wl,--max-memory=2147483648
    -Wl,--initial-memory=33554432
    -Wl,-z,stack-size=1048576
)

add_executable(flatsql_p4_threads ${FLATSQL_P4_WASM_ENTRY})
target_compile_options(flatsql_p4_threads PRIVATE ${FLATSQL_P4_WASM_FLAGS})
target_link_libraries(flatsql_p4_threads PRIVATE flatsql_p4_wasm)
target_link_options(flatsql_p4_threads PRIVATE ${FLATSQL_P4_THREADS_LINK} -mexec-model=reactor)
set_target_properties(flatsql_p4_threads PROPERTIES
    OUTPUT_NAME "flatsql-p4-threads"
    SUFFIX ".wasm"
)

# ---- the test command ----------------------------------------------------------
file(GLOB FLATSQL_P4_WASM_TEST_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/tests/p4/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/tests/p4sql/*.cpp")
list(FILTER FLATSQL_P4_WASM_TEST_SOURCES EXCLUDE REGEX "/fake_reader\\.cpp$")
list(FILTER FLATSQL_P4_WASM_TEST_SOURCES EXCLUDE REGEX "/tests/p4sql/.*fake.*\\.cpp$")
list(FILTER FLATSQL_P4_WASM_TEST_SOURCES EXCLUDE REGEX "/p4_fault_io\\.cpp$")
if(FLATSQL_P4_WASM_TEST_SOURCES)
    add_library(flatsql_p4_wasm_testutil STATIC
        ${FLATBUFFERS_SRC_DIR}/idl_parser.cpp
        ${FLATBUFFERS_SRC_DIR}/idl_gen_text.cpp
        ${FLATBUFFERS_SRC_DIR}/file_manager.cpp
        ${FLATBUFFERS_SRC_DIR}/file_name_manager.cpp
        ${FLATBUFFERS_SRC_DIR}/util.cpp
        ${FLATBUFFERS_SRC_DIR}/options.cpp
    )
    target_include_directories(flatsql_p4_wasm_testutil PUBLIC ${FLATBUFFERS_INCLUDE_DIR})
    target_compile_options(flatsql_p4_wasm_testutil PRIVATE ${FLATSQL_P4_WASM_FLAGS} -w)
    add_executable(flatsql_p4_test_wasm ${FLATSQL_P4_WASM_ENTRY} ${FLATSQL_P4_WASM_TEST_SOURCES})
    target_include_directories(flatsql_p4_test_wasm PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${CMAKE_CURRENT_SOURCE_DIR}/src/p4
        ${SQLITE_DIR}
    )
    target_compile_options(flatsql_p4_test_wasm PRIVATE ${FLATSQL_P4_WASM_FLAGS})
    target_compile_definitions(flatsql_p4_test_wasm PRIVATE
        P4_SCHEMA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/p4/schemas")
    target_link_libraries(flatsql_p4_test_wasm PRIVATE flatsql_p4_wasm flatsql_p4_wasm_testutil)
    target_link_options(flatsql_p4_test_wasm PRIVATE
        -pthread
        -Wl,--strip-debug
        -Wl,--import-memory
        -Wl,--export-memory
        -Wl,--shared-memory
        -Wl,--max-memory=4294967296
        -Wl,--initial-memory=134217728
        -Wl,-z,stack-size=4194304
    )
    set_target_properties(flatsql_p4_test_wasm PROPERTIES
        OUTPUT_NAME "flatsql-p4-test"
        SUFFIX ".wasm"
    )
endif()
