cmake_minimum_required(VERSION 3.20)
project(FlatSQL VERSION 3.12.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# FlatBuffers location: -DFLATBUFFERS_DIR, else $FLATBUFFERS_DIR (worktrees
# outside the stack layout), else the sibling checkout.
set(FLATSQL_FLATBUFFERS_DEFAULT "${CMAKE_CURRENT_SOURCE_DIR}/../../flatbuffers")
if(DEFINED ENV{FLATBUFFERS_DIR})
    set(FLATSQL_FLATBUFFERS_DEFAULT "$ENV{FLATBUFFERS_DIR}")
endif()
set(FLATBUFFERS_DIR "${FLATSQL_FLATBUFFERS_DEFAULT}" CACHE PATH "FlatBuffers source checkout")
set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_DIR}/include")
set(FLATBUFFERS_SRC_DIR "${FLATBUFFERS_DIR}/src")

# Verify flatbuffers exists
if(NOT EXISTS "${FLATBUFFERS_INCLUDE_DIR}/flatbuffers/flatbuffers.h")
    message(FATAL_ERROR "FlatBuffers not found at ${FLATBUFFERS_INCLUDE_DIR}")
endif()

message(STATUS "FlatBuffers include dir: ${FLATBUFFERS_INCLUDE_DIR}")

# SDS format-5 tables are generated from the released schema bytes. The same
# host compiler and output layout serve native and cross-compiled WASI builds.
set(SDS_SCHEMA_DIR "${PROJECT_SOURCE_DIR}/../node_modules/spacedatastandards.org/schema" CACHE PATH "SDS schema directory")
include(ExternalProject)
set(FLATSQL_HOST_FLATC_DIR "${CMAKE_BINARY_DIR}/host-flatc")
ExternalProject_Add(flatsql_host_flatc
    SOURCE_DIR "${FLATBUFFERS_DIR}"
    BINARY_DIR "${FLATSQL_HOST_FLATC_DIR}"
    CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=
        -DCMAKE_C_COMPILER=cc -DCMAKE_CXX_COMPILER=c++
        -DFLATBUFFERS_BUILD_TESTS=OFF -DFLATBUFFERS_BUILD_FLATLIB=OFF
        -DFLATBUFFERS_BUILD_FLATHASH=OFF -DFLATBUFFERS_BUILD_GRPCTEST=OFF
        -DFLATBUFFERS_BUILD_SHAREDLIB=OFF -DFLATBUFFERS_INSTALL=OFF
        -DFLATBUFFERS_USE_SEAL=OFF -DFLATBUFFERS_USE_OPENSSL=OFF
        -DFLATBUFFERS_USE_CRYPTOPP=OFF -DFETCHCONTENT_FULLY_DISCONNECTED=ON
    BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target flatc -j 8
    INSTALL_COMMAND ""
    BUILD_BYPRODUCTS "${FLATSQL_HOST_FLATC_DIR}/flatc")
set(FLATSQL_SDS_GENERATED_DIR "${CMAKE_BINARY_DIR}/generated-sds")
set(FLATSQL_SDS_HEADERS)
foreach(SDS_TABLE FMB FCK FSG GJN QRP)
    if(NOT EXISTS "${SDS_SCHEMA_DIR}/${SDS_TABLE}/main.fbs")
        message(FATAL_ERROR "SDS 1.256.0 schema missing: ${SDS_SCHEMA_DIR}/${SDS_TABLE}/main.fbs")
    endif()
    set(SDS_HEADER "${FLATSQL_SDS_GENERATED_DIR}/${SDS_TABLE}/main_generated.h")
    add_custom_command(OUTPUT "${SDS_HEADER}"
        COMMAND ${CMAKE_COMMAND} -E make_directory "${FLATSQL_SDS_GENERATED_DIR}/${SDS_TABLE}"
        COMMAND "${FLATSQL_HOST_FLATC_DIR}/flatc" --cpp --keep-prefix
            -I "${SDS_SCHEMA_DIR}" -o "${FLATSQL_SDS_GENERATED_DIR}/${SDS_TABLE}"
            "${SDS_SCHEMA_DIR}/${SDS_TABLE}/main.fbs"
        COMMAND ${CMAKE_COMMAND} -DHEADER=${SDS_HEADER} -DTABLE=${SDS_TABLE}
            -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/sds-header-guard.cmake"
        DEPENDS flatsql_host_flatc "${CMAKE_CURRENT_SOURCE_DIR}/cmake/sds-header-guard.cmake"
            "${SDS_SCHEMA_DIR}/${SDS_TABLE}/main.fbs"
            "${SDS_SCHEMA_DIR}/FMB/main.fbs"
            "${SDS_SCHEMA_DIR}/FCK/main.fbs" "${SDS_SCHEMA_DIR}/FSG/main.fbs"
        VERBATIM)
    list(APPEND FLATSQL_SDS_HEADERS "${SDS_HEADER}")
endforeach()
add_custom_target(flatsql_p4_sds DEPENDS ${FLATSQL_SDS_HEADERS})
function(flatsql_p4_use_sds target)
    add_dependencies(${target} flatsql_p4_sds)
    target_include_directories(${target} PUBLIC "${FLATSQL_SDS_GENERATED_DIR}")
endfunction()


# SQLite amalgamation (vendored): the official 3.53.4 release, byte-identical
# (sqlite-amalgamation-3530400.zip, SHA3-256 628a44cf... as sqlite.org lists
# it). SQLite is never modified: only its public API, pragmas, sqlite3_wal_hook
# and FlatSQL's own VFS. A mismatch fails the configure.
set(SQLITE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/vendor/sqlite")
set(FLATSQL_SQLITE_SHA256 "b1dd5d74ec7f29055a6684fa06fb3c2f6821c87dd38f9a458dfd2e8a1db28189")
if(NOT EXISTS "${SQLITE_DIR}/sqlite3.c")
    message(FATAL_ERROR "SQLite amalgamation not found at ${SQLITE_DIR}")
endif()
file(SHA256 "${SQLITE_DIR}/sqlite3.c" FLATSQL_SQLITE_SHA256_FOUND)
if(NOT FLATSQL_SQLITE_SHA256_FOUND STREQUAL FLATSQL_SQLITE_SHA256)
    message(FATAL_ERROR "vendor/sqlite/sqlite3.c is not the SQLite 3.53.4 amalgamation "
        "(sha256 ${FLATSQL_SQLITE_SHA256_FOUND}, expected ${FLATSQL_SQLITE_SHA256})")
endif()

# The unified engine is the only buildable public implementation.
if(CMAKE_SYSTEM_NAME STREQUAL "WASI")
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/flatsql_p4_wasm.cmake)
    return()
endif()
if(EMSCRIPTEN OR CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
    message(FATAL_ERROR "Use wasi-sdk 30: browsers and servers run the same flatsql.wasm")
endif()
find_package(Threads REQUIRED)
enable_testing()

# ---- store format 4 ("p4": one SQLite file per partition, producer x record type) ----
# SQLite 3.53.4 unmodified: multi-thread mode, WAL, FTS5, no memory
# statistics (C-30: the SQL surface's allocator counts and enforces the
# hard heap); files only through FlatSQL's own VFS (flatsql_io). The SQL
# surface (src/p4sql) is globbed in; without it src/p4/p4sql_weak.cpp
# answers ops 30/31 with UNSUPPORTED (and is left out when it is there).
add_library(sqlite3_p4 STATIC ${SQLITE_DIR}/sqlite3.c)
target_include_directories(sqlite3_p4 PUBLIC ${SQLITE_DIR})
target_compile_definitions(sqlite3_p4 PRIVATE
    SQLITE_THREADSAFE=2
    SQLITE_DEFAULT_MEMSTATUS=0
    SQLITE_TEMP_STORE=3
    SQLITE_OMIT_LOAD_EXTENSION=1
    SQLITE_OMIT_DEPRECATED=1
    SQLITE_OMIT_SHARED_CACHE=1
    SQLITE_OMIT_JSON=1
    SQLITE_DQS=0
    SQLITE_ENABLE_FTS5=1
)
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(sqlite3_p4 PRIVATE -w)
endif()
file(GLOB FLATSQL_P4_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/p4/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/p4sql/*.cpp")
list(FILTER FLATSQL_P4_SOURCES EXCLUDE REGEX "/capi_wasm\\.cpp$")
file(GLOB FLATSQL_P4SQL_PRESENT "${CMAKE_CURRENT_SOURCE_DIR}/src/p4sql/*.cpp")
if(FLATSQL_P4SQL_PRESENT)
    list(FILTER FLATSQL_P4_SOURCES EXCLUDE REGEX "/p4sql_weak\\.cpp$")
endif()
# The engine without host I/O: flatsql_p4 adds the native seven imports;
# the fault tests supply their own.
add_library(flatsql_p4_core OBJECT
    ${FLATSQL_P4_SOURCES}
    src/geo_functions.cpp
    src/geo_geometry.cpp
    src/geo_gjn.cpp
    src/query_page.cpp
    src/typecfg/extract.cpp
    src/ps/platform.cpp
    src/ps/result_block.cpp
    src/flatsql_vfs.cpp
    src/record_search.cpp
    ${FLATBUFFERS_SRC_DIR}/reflection.cpp
    ${FLATBUFFERS_SRC_DIR}/util.cpp
    ${FLATBUFFERS_SRC_DIR}/options.cpp
)
target_include_directories(flatsql_p4_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${FLATBUFFERS_INCLUDE_DIR}
    ${SQLITE_DIR}
)
flatsql_p4_use_sds(flatsql_p4_core)
target_link_libraries(flatsql_p4_core PUBLIC sqlite3_p4 Threads::Threads)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(flatsql_p4_core PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()
add_library(flatsql STATIC $<TARGET_OBJECTS:flatsql_p4_core> src/flatsql_io_native.cpp)
target_include_directories(flatsql PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${FLATBUFFERS_INCLUDE_DIR}
    ${SQLITE_DIR}
)
target_link_libraries(flatsql PUBLIC sqlite3_p4 Threads::Threads)
# Tests: cpp/tests/p4 (and the SQL surface's engine tests in cpp/tests/p4sql;
# its fake reader builds only its own fake target).
file(GLOB FLATSQL_P4_TEST_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/tests/p4/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/tests/p4sql/*.cpp")
list(FILTER FLATSQL_P4_TEST_SOURCES EXCLUDE REGEX "/fake_reader\\.cpp$")
list(FILTER FLATSQL_P4_TEST_SOURCES EXCLUDE REGEX "/p4_fault_io\\.cpp$")
list(FILTER FLATSQL_P4_TEST_SOURCES EXCLUDE REGEX "/tests/p4sql/.*fake.*\\.cpp$")
if(FLATSQL_P4_TEST_SOURCES)
    # The replay counts actual host sync calls, including WAL-header syncs.
    # Rename only this test object's symbol; production I/O stays unchanged.
    add_library(flatsql_p4_replay_native_io OBJECT src/flatsql_io_native.cpp)
    target_include_directories(flatsql_p4_replay_native_io PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_compile_definitions(flatsql_p4_replay_native_io PRIVATE flatsql_io_sync=p4ReplayNativeSync)
    # The engine's objects, not the archive: every object is linked
    # (the SQL surface's included), as in the wasm artifact.
    add_executable(flatsql_p4_test ${FLATSQL_P4_TEST_SOURCES}
        $<TARGET_OBJECTS:flatsql_p4_core>
        $<TARGET_OBJECTS:flatsql_p4_replay_native_io>
        ${FLATBUFFERS_SRC_DIR}/idl_parser.cpp
        ${FLATBUFFERS_SRC_DIR}/idl_gen_text.cpp
        ${FLATBUFFERS_SRC_DIR}/file_manager.cpp
        ${FLATBUFFERS_SRC_DIR}/file_name_manager.cpp)
    flatsql_p4_use_sds(flatsql_p4_test)
    target_link_libraries(flatsql_p4_test PRIVATE sqlite3_p4 Threads::Threads)
    target_include_directories(flatsql_p4_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR} ${SQLITE_DIR})
    target_include_directories(flatsql_p4_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests ${CMAKE_CURRENT_SOURCE_DIR}/src/p4)
    target_compile_definitions(flatsql_p4_test PRIVATE
        FLATSQL_REPLAY_NATIVE_IO=1
        FLATSQL_SCHEMA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/p4/schemas")
    add_test(NAME FlatSQLBinaryQueryReopen COMMAND flatsql_p4_test --test=t_gjn_binary_query_reopen)
    # Power loss: the same engine objects over FaultFs (in-memory files
    # that keep what a crash leaves), the test main and its loop only.
    add_executable(flatsql_p4_fault_test
        tests/p4/p4_test_main.cpp tests/p4/p4_fault_io.cpp test/io_fault.cpp
        $<TARGET_OBJECTS:flatsql_p4_core>
        ${FLATBUFFERS_SRC_DIR}/idl_parser.cpp
        ${FLATBUFFERS_SRC_DIR}/idl_gen_text.cpp
        ${FLATBUFFERS_SRC_DIR}/file_manager.cpp
        ${FLATBUFFERS_SRC_DIR}/file_name_manager.cpp)
    flatsql_p4_use_sds(flatsql_p4_fault_test)
    target_link_libraries(flatsql_p4_fault_test PRIVATE sqlite3_p4 Threads::Threads)
    target_include_directories(flatsql_p4_fault_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR} ${SQLITE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${CMAKE_CURRENT_SOURCE_DIR}/src/p4 ${CMAKE_CURRENT_SOURCE_DIR}/test)
    target_compile_definitions(flatsql_p4_fault_test PRIVATE
        FLATSQL_SCHEMA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/p4/schemas")
endif()
