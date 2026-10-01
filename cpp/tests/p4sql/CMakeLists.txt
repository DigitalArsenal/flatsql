# flatsql_p4sql_fake_test: the SQL surface (cpp/src/p4sql) over the format-4
# reader's test double (fake_reader.cpp). It never links the engine
# (CONTRACT.md §3.9).
#
# Native, with format 1's engine (flatsql_lib) as the oracle:
#   FLATBUFFERS_DIR=<flatbuffers checkout> \
#     cmake -S cpp/tests/p4sql -B build-p4sql -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
#   cmake --build build-p4sql --target flatsql_p4sql_fake_test
#   build-p4sql/flatsql_p4sql_fake_test [name-substring ...]
#
# wasm32-wasip1-threads, the same suite without the oracle tests, as a
# wasi-threads command (flatsql-p4sql-fake-test.wasm):
#   FLATBUFFERS_DIR=... cmake -S cpp/tests/p4sql -B build-p4sql-wasm -G Ninja \
#     -DCMAKE_TOOLCHAIN_FILE=cpp/cmake/wasi-threads-toolchain.cmake -DWASI_SDK_PREFIX=<wasi-sdk 30>
#   cmake --build build-p4sql-wasm
#   node wasm/ps-node-host.mjs build-p4sql-wasm/flatsql-p4sql-fake-test.wasm [name-substring ...]
#
# Fixture tests read P4_FIXTURE (a format-1 control.flatsqldb, opened
# read-only and immutable) and skip without it. The engine's own CMake globs
# this directory's *.cpp into its test binaries; every file here compiles to
# nothing there (FLATSQL_P4SQL_FAKE is defined only by this project).
cmake_minimum_required(VERSION 3.20)
project(flatsql_p4sql_fake_test C CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

get_filename_component(FLATSQL_CPP "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)
file(GLOB P4SQL_SOURCES CONFIGURE_DEPENDS ${FLATSQL_CPP}/src/p4sql/*.cpp)
file(GLOB P4SQL_TEST_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
set(P4SQL_DEFINES FLATSQL_P4SQL_FAKE=1 P4SQL_VECTOR_DIR="${CMAKE_CURRENT_SOURCE_DIR}/vectors")

if(CMAKE_SYSTEM_NAME STREQUAL "WASI")
    if(NOT FLATBUFFERS_DIR)
        set(FLATBUFFERS_DIR "$ENV{FLATBUFFERS_DIR}")
    endif()
    # Format 1's engine is not built for this target: no oracle tests.
    list(FILTER P4SQL_TEST_SOURCES EXCLUDE REGEX "/(format1_oracle|fixture_test)\\.cpp$")
    list(APPEND P4SQL_DEFINES P4SQL_NO_ORACLE=1)
    set(P4SQL_WASM_FLAGS -pthread -matomics -mbulk-memory -fno-exceptions)
    add_library(sqlite3_p4sql_wasm STATIC ${FLATSQL_CPP}/vendor/sqlite/sqlite3.c)
    target_include_directories(sqlite3_p4sql_wasm PUBLIC ${FLATSQL_CPP}/vendor/sqlite)
    target_compile_options(sqlite3_p4sql_wasm PRIVATE ${P4SQL_WASM_FLAGS} -w)
    target_compile_definitions(sqlite3_p4sql_wasm PRIVATE
        SQLITE_THREADSAFE=2 SQLITE_TEMP_STORE=3 SQLITE_OMIT_LOAD_EXTENSION=1 SQLITE_OMIT_DEPRECATED=1
        SQLITE_OMIT_SHARED_CACHE=1 SQLITE_DQS=0 _WASI_EMULATED_GETPID _WASI_EMULATED_MMAN)
    add_executable(flatsql_p4sql_fake_test
        ${P4SQL_SOURCES}
        ${P4SQL_TEST_SOURCES}
        ${FLATSQL_CPP}/src/ps/result_block.cpp
        ${FLATBUFFERS_DIR}/src/idl_parser.cpp
        ${FLATBUFFERS_DIR}/src/idl_gen_text.cpp
        ${FLATBUFFERS_DIR}/src/util.cpp
        ${FLATBUFFERS_DIR}/src/file_manager.cpp
        ${FLATBUFFERS_DIR}/src/file_name_manager.cpp
        ${FLATBUFFERS_DIR}/src/options.cpp
    )
    target_compile_options(flatsql_p4sql_fake_test PRIVATE ${P4SQL_WASM_FLAGS})
    target_link_libraries(flatsql_p4sql_fake_test PRIVATE sqlite3_p4sql_wasm wasi-emulated-getpid wasi-emulated-mman)
    target_include_directories(flatsql_p4sql_fake_test PRIVATE ${FLATSQL_CPP}/include ${FLATBUFFERS_DIR}/include
                               ${CMAKE_CURRENT_SOURCE_DIR})
    # A wasi-threads command: shared env.memory imported (format 2's test link).
    target_link_options(flatsql_p4sql_fake_test PRIVATE
        -pthread -Wl,--import-memory -Wl,--export-memory -Wl,--shared-memory
        -Wl,--max-memory=4294967296 -Wl,--initial-memory=67108864 -Wl,-z,stack-size=4194304)
    set_target_properties(flatsql_p4sql_fake_test PROPERTIES OUTPUT_NAME "flatsql-p4sql-fake-test" SUFFIX ".wasm")
else()
    add_subdirectory(${FLATSQL_CPP} flatsql EXCLUDE_FROM_ALL)
    # The flatbuffers schema parser builds test records from JSON with the
    # real BFBS (util.cpp and options.cpp come with flatsql_lib).
    add_executable(flatsql_p4sql_fake_test
        ${P4SQL_SOURCES}
        ${P4SQL_TEST_SOURCES}
        ${FLATSQL_CPP}/src/ps/result_block.cpp
        ${FLATBUFFERS_DIR}/src/idl_parser.cpp
        ${FLATBUFFERS_DIR}/src/idl_gen_text.cpp
        ${FLATBUFFERS_DIR}/src/file_manager.cpp
        ${FLATBUFFERS_DIR}/src/file_name_manager.cpp
    )
    target_link_libraries(flatsql_p4sql_fake_test PRIVATE flatsql_lib)
    target_include_directories(flatsql_p4sql_fake_test PRIVATE ${FLATSQL_CPP}/include ${CMAKE_CURRENT_SOURCE_DIR})
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        set_source_files_properties(
            ${FLATBUFFERS_DIR}/src/idl_parser.cpp ${FLATBUFFERS_DIR}/src/idl_gen_text.cpp
            ${FLATBUFFERS_DIR}/src/file_manager.cpp ${FLATBUFFERS_DIR}/src/file_name_manager.cpp
            PROPERTIES COMPILE_OPTIONS "-w")
        set_source_files_properties(${P4SQL_SOURCES} PROPERTIES COMPILE_OPTIONS "-Wall;-Wextra;-Wno-unused-parameter")
    endif()
    enable_testing()
    add_test(NAME P4SqlFakeTest COMMAND flatsql_p4sql_fake_test)
endif()
target_compile_definitions(flatsql_p4sql_fake_test PRIVATE ${P4SQL_DEFINES})
