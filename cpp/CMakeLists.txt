cmake_minimum_required(VERSION 3.20)
project(FlatSQL VERSION 1.0.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# FlatBuffers location
set(FLATBUFFERS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../flatbuffers")
set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_DIR}/include")

# Verify flatbuffers exists
if(NOT EXISTS "${FLATBUFFERS_INCLUDE_DIR}/flatbuffers/flatbuffers.h")
    message(FATAL_ERROR "FlatBuffers not found at ${FLATBUFFERS_INCLUDE_DIR}")
endif()

message(STATUS "FlatBuffers include dir: ${FLATBUFFERS_INCLUDE_DIR}")

# SQLite amalgamation (vendored)
set(SQLITE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/vendor/sqlite")
if(NOT EXISTS "${SQLITE_DIR}/sqlite3.c")
    message(FATAL_ERROR "SQLite amalgamation not found at ${SQLITE_DIR}")
endif()

# Source files (library - no main)
set(FLATSQL_LIB_SOURCES
    src/storage.cpp
    src/btree.cpp
    src/schema_parser.cpp
    src/database.cpp
    src/junction.cpp
    src/sqlite_vtab.cpp
    src/sqlite_engine.cpp
)

# Sources for WASM build (includes bindings)
set(FLATSQL_WASM_SOURCES
    ${FLATSQL_LIB_SOURCES}
    src/flatsql.cpp
)

set(FLATSQL_HEADERS
    include/flatsql/storage.h
    include/flatsql/btree.h
    include/flatsql/schema_parser.h
    include/flatsql/database.h
    include/flatsql/junction.h
    include/flatsql/types.h
    include/flatsql/sqlite_vtab.h
    include/flatsql/sqlite_engine.h
)

# Emscripten/WASM configuration
if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten" OR EMSCRIPTEN)
    message(STATUS "")
    message(STATUS "*** EMSCRIPTEN/WEBASSEMBLY BUILD ***")
    message(STATUS "")

    set(FLATSQL_BUILD_WASM ON)

    # WASM-specific compiler flags
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexceptions")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")

    # Create the WASM library (includes SQLite amalgamation directly)
    add_executable(flatsql ${FLATSQL_WASM_SOURCES} ${SQLITE_DIR}/sqlite3.c)

    target_include_directories(flatsql PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
    )

    # SQLite compile definitions for minimal WASM build
    target_compile_definitions(flatsql PRIVATE
        SQLITE_THREADSAFE=0
        SQLITE_OMIT_LOAD_EXTENSION=1
        SQLITE_OMIT_WAL=1
        SQLITE_OMIT_DEPRECATED=1
        SQLITE_OMIT_SHARED_CACHE=1
        SQLITE_OMIT_PROGRESS_CALLBACK=1
        SQLITE_DEFAULT_MEMSTATUS=0
        SQLITE_DQS=0
    )

    # Emscripten link flags for WASM output
    set_target_properties(flatsql PROPERTIES
        SUFFIX ".js"
        LINK_FLAGS "\
            -s WASM=1 \
            -s EXPORT_ES6=1 \
            -s MODULARIZE=1 \
            -s EXPORT_NAME='FlatSQL' \
            -s EXPORTED_FUNCTIONS='[\"_malloc\", \"_free\"]' \
            -s EXPORTED_RUNTIME_METHODS='[\"ccall\", \"cwrap\", \"UTF8ToString\", \"stringToUTF8\", \"lengthBytesUTF8\"]' \
            -s ALLOW_MEMORY_GROWTH=1 \
            -s INITIAL_MEMORY=16777216 \
            -s STACK_SIZE=1048576 \
            -s NO_EXIT_RUNTIME=1 \
            -s FILESYSTEM=0 \
            -s ENVIRONMENT='web,worker,node' \
            -s SINGLE_FILE=0 \
            -s ALLOW_TABLE_GROWTH=1 \
            -fexceptions \
            --bind \
        "
    )

    message(STATUS "WASM build configured")
    message(STATUS "Output: flatsql.js + flatsql.wasm")

    # Post-build: copy WASM files to docs/ for GitHub Pages
    add_custom_command(TARGET flatsql POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
            $<TARGET_FILE_DIR:flatsql>/flatsql.js
            $<TARGET_FILE_DIR:flatsql>/flatsql.wasm
            ${CMAKE_CURRENT_SOURCE_DIR}/../docs/
        COMMENT "Copying WASM files to docs/ for GitHub Pages"
    )

else()
    message(STATUS "Native build (for testing and CLI)")

    # SQLite static library (from amalgamation)
    add_library(sqlite3 STATIC ${SQLITE_DIR}/sqlite3.c)
    target_include_directories(sqlite3 PUBLIC ${SQLITE_DIR})
    target_compile_definitions(sqlite3 PRIVATE
        SQLITE_THREADSAFE=0
        SQLITE_OMIT_LOAD_EXTENSION=1
        SQLITE_OMIT_WAL=1
        SQLITE_OMIT_DEPRECATED=1
        SQLITE_DQS=0
    )
    # Suppress warnings in SQLite code
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(sqlite3 PRIVATE -w)
    endif()

    # Native library
    add_library(flatsql_lib STATIC ${FLATSQL_LIB_SOURCES})

    target_include_directories(flatsql_lib PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
    )
    target_link_libraries(flatsql_lib PUBLIC sqlite3)

    # Native CLI executable (with stdin piping support)
    add_executable(flatsql src/flatsql.cpp)
    target_link_libraries(flatsql PRIVATE flatsql_lib)
    target_include_directories(flatsql PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
    )

    # Native test executable (basic unit tests)
    add_executable(flatsql_test test/test_main.cpp)
    target_link_libraries(flatsql_test PRIVATE flatsql_lib)
    target_include_directories(flatsql_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
    )

    # Integration test (uses real FlatBuffer generated code)
    add_executable(flatsql_integration_test test/integration_test.cpp)
    target_link_libraries(flatsql_integration_test PRIVATE flatsql_lib)
    target_include_directories(flatsql_integration_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/schemas
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
    )

    # Socket streaming test (Unix domain socket streaming)
    add_executable(flatsql_socket_test test/socket_stream_test.cpp)
    target_link_libraries(flatsql_socket_test PRIVATE flatsql_lib)
    target_include_directories(flatsql_socket_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/schemas
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
    )

    # Benchmark: FlatSQL vs SQLite (using vendored SQLite)
    message(STATUS "Building benchmark with vendored SQLite")
    add_executable(flatsql_benchmark test/benchmark.cpp)
    target_link_libraries(flatsql_benchmark PRIVATE flatsql_lib)
    target_include_directories(flatsql_benchmark PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/schemas
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
    )
    # Optimize for benchmark
    target_compile_options(flatsql_benchmark PRIVATE -O3)

    # Comprehensive round-trip test (using vendored SQLite)
    add_executable(flatsql_roundtrip_test test/roundtrip_test.cpp)
    target_link_libraries(flatsql_roundtrip_test PRIVATE flatsql_lib)
    target_include_directories(flatsql_roundtrip_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/schemas
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
    )

    enable_testing()
    add_test(NAME FlatSQLTest COMMAND flatsql_test)
    add_test(NAME FlatSQLIntegrationTest COMMAND flatsql_integration_test)
    add_test(NAME FlatSQLSocketTest COMMAND flatsql_socket_test)
    add_test(NAME FlatSQLRoundTripTest COMMAND flatsql_roundtrip_test)
endif()
