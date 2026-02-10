cmake_minimum_required(VERSION 3.20)
project(FlatSQL VERSION 1.0.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# FlatBuffers location
set(FLATBUFFERS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../flatbuffers")
set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_DIR}/include")
set(FLATBUFFERS_SRC_DIR "${FLATBUFFERS_DIR}/src")

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

# OpenSSL for HMAC-SHA256 buffer authentication
find_package(OpenSSL QUIET)
if(NOT OpenSSL_FOUND AND APPLE)
    set(OPENSSL_ROOT_DIR "/opt/homebrew/opt/openssl@3")
    find_package(OpenSSL QUIET)
endif()
if(OpenSSL_FOUND)
    message(STATUS "OpenSSL found: ${OPENSSL_VERSION} - HMAC authentication available")
else()
    message(WARNING "OpenSSL not found - HMAC authentication will not be available")
endif()

# FlatBuffers encryption source (uses fallback AES-CTR, no external crypto needed)
set(FLATBUFFERS_ENCRYPTION_SOURCES
    ${FLATBUFFERS_SRC_DIR}/encryption.cpp
)

# sqlean extensions (vendored, MIT License)
set(SQLEAN_DIR "${CMAKE_CURRENT_SOURCE_DIR}/vendor/sqlean")
set(SQLEAN_SOURCES
    ${SQLEAN_DIR}/math/extension.c
    ${SQLEAN_DIR}/stats/extension.c
    ${SQLEAN_DIR}/stats/scalar.c
    ${SQLEAN_DIR}/stats/series.c
    ${SQLEAN_DIR}/text/extension.c
    ${SQLEAN_DIR}/text/bstring.c
    ${SQLEAN_DIR}/text/rstring.c
    ${SQLEAN_DIR}/text/runes.c
    ${SQLEAN_DIR}/text/utf8/utf8.c
    ${SQLEAN_DIR}/text/utf8/rune.c
    ${SQLEAN_DIR}/text/utf8/case.c
    ${SQLEAN_DIR}/uuid/extension.c
    ${SQLEAN_DIR}/fuzzy/extension.c
    ${SQLEAN_DIR}/fuzzy/common.c
    ${SQLEAN_DIR}/fuzzy/caver.c
    ${SQLEAN_DIR}/fuzzy/damlev.c
    ${SQLEAN_DIR}/fuzzy/editdist.c
    ${SQLEAN_DIR}/fuzzy/hamming.c
    ${SQLEAN_DIR}/fuzzy/jarowin.c
    ${SQLEAN_DIR}/fuzzy/leven.c
    ${SQLEAN_DIR}/fuzzy/osadist.c
    ${SQLEAN_DIR}/fuzzy/phonetic.c
    ${SQLEAN_DIR}/fuzzy/rsoundex.c
    ${SQLEAN_DIR}/fuzzy/soundex.c
    ${SQLEAN_DIR}/fuzzy/translit.c
)

# sqlean files need SQLITE_CORE so SQLITE_EXTENSION_INIT3 becomes a no-op
set_source_files_properties(${SQLEAN_SOURCES} PROPERTIES
    COMPILE_DEFINITIONS "SQLITE_CORE"
)

# Source files (library - no main)
set(FLATSQL_LIB_SOURCES
    src/storage.cpp
    src/sqlite_index.cpp
    src/schema_parser.cpp
    src/database.cpp
    src/junction.cpp
    src/sqlite_vtab.cpp
    src/sqlite_engine.cpp
    src/geo_functions.cpp
    ${FLATBUFFERS_ENCRYPTION_SOURCES}
    ${SQLEAN_SOURCES}
)

# Sources for WASM build - C API (no embind, worker-compatible)
set(FLATSQL_WASM_SOURCES
    ${FLATSQL_LIB_SOURCES}
    src/flatsql_capi.cpp
)

set(FLATSQL_HEADERS
    include/flatsql/storage.h
    include/flatsql/sqlite_index.h
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
        ${SQLEAN_DIR}
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
        SQLITE_ENABLE_RTREE=1
    )

    # Emscripten link flags for WASM output
    # Using C API exports (no embind) for worker compatibility
    set_target_properties(flatsql PROPERTIES
        SUFFIX ".js"
        LINK_FLAGS "\
            -s WASM=1 \
            -s EXPORT_ES6=1 \
            -s MODULARIZE=1 \
            -s EXPORT_NAME='FlatSQL' \
            -s EXPORTED_FUNCTIONS='[ \
                \"_malloc\", \"_free\", \
                \"_flatsql_create_db\", \"_flatsql_destroy_db\", \
                \"_flatsql_register_file_id\", \"_flatsql_enable_demo_extractors\", \
                \"_flatsql_ingest\", \"_flatsql_ingest_one\", \
                \"_flatsql_register_source\", \"_flatsql_create_unified_views\", \
                \"_flatsql_ingest_with_source\", \"_flatsql_ingest_one_with_source\", \
                \"_flatsql_get_sources_count\", \"_flatsql_get_source_name\", \
                \"_flatsql_query\", \"_flatsql_get_error\", \
                \"_flatsql_result_column_count\", \"_flatsql_result_row_count\", \
                \"_flatsql_result_column_name\", \"_flatsql_result_cell_type\", \
                \"_flatsql_result_cell_number\", \"_flatsql_result_cell_string\", \
                \"_flatsql_result_cell_blob\", \"_flatsql_result_cell_blob_size\", \
                \"_flatsql_export_data\", \"_flatsql_export_size\", \
                \"_flatsql_load_and_rebuild\", \
                \"_flatsql_create_test_user\", \"_flatsql_create_test_post\", \
                \"_flatsql_test_buffer_size\", \
                \"_flatsql_get_stats_count\", \"_flatsql_get_stat_table_name\", \
                \"_flatsql_get_stat_file_id\", \"_flatsql_get_stat_record_count\", \
                \"_flatsql_mark_deleted\", \"_flatsql_get_deleted_count\", \
                \"_flatsql_clear_tombstones\", \
                \"_flatsql_get_flatbuffer_by_id\", \"_flatsql_get_flatbuffer_by_email\", \
                \"_flatsql_get_raw_flatbuffer_size\", \"_flatsql_get_raw_flatbuffer_sequence\", \
                \"_flatsql_get_storage_buffer\", \"_flatsql_get_storage_size\", \
                \"_flatsql_set_encryption_key\", \"_flatsql_is_encrypted\", \
                \"_flatsql_encrypt_buffer\", \"_flatsql_decrypt_buffer\", \
                \"_flatsql_set_hmac_verification\", \"_flatsql_is_hmac_enabled\", \
                \"_flatsql_compute_hmac\", \"_flatsql_verify_hmac\" \
            ]' \
            -s EXPORTED_RUNTIME_METHODS='[\"ccall\", \"cwrap\", \"UTF8ToString\", \"stringToUTF8\", \"lengthBytesUTF8\", \"HEAPU8\"]' \
            -s ALLOW_MEMORY_GROWTH=1 \
            -s INITIAL_MEMORY=16777216 \
            -s STACK_SIZE=1048576 \
            -s NO_EXIT_RUNTIME=1 \
            -s FILESYSTEM=0 \
            -s ENVIRONMENT='web,worker,node' \
            -s SINGLE_FILE=0 \
            -s ALLOW_TABLE_GROWTH=1 \
            -s EMULATE_FUNCTION_POINTER_CASTS=1 \
            -fexceptions \
            -s DISABLE_EXCEPTION_CATCHING=0 \
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
        SQLITE_ENABLE_RTREE=1
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
        ${SQLEAN_DIR}
    )
    target_link_libraries(flatsql_lib PUBLIC sqlite3)
    if(OpenSSL_FOUND)
        target_link_libraries(flatsql_lib PUBLIC OpenSSL::Crypto)
        target_include_directories(flatsql_lib PUBLIC ${OPENSSL_INCLUDE_DIR})
        target_compile_definitions(flatsql_lib PRIVATE FLATSQL_HAVE_OPENSSL=1)
    endif()

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

    # Advanced MPE (Minimum Propagatable Element) benchmark - NOT part of CI
    # Tests large-scale performance with realistic space catalog data
    # Usage: ./flatsql_mpe_benchmark [record_count]
    #        Default: 1M records, can test up to 500M+
    add_executable(flatsql_mpe_benchmark test/mpe_benchmark.cpp)
    target_link_libraries(flatsql_mpe_benchmark PRIVATE flatsql_lib)
    target_include_directories(flatsql_mpe_benchmark PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/schemas
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
    )
    # Optimize for benchmark
    target_compile_options(flatsql_mpe_benchmark PRIVATE -O3)

    enable_testing()
    add_test(NAME FlatSQLTest COMMAND flatsql_test)
    add_test(NAME FlatSQLIntegrationTest COMMAND flatsql_integration_test)
    add_test(NAME FlatSQLSocketTest COMMAND flatsql_socket_test)
    add_test(NAME FlatSQLRoundTripTest COMMAND flatsql_roundtrip_test)
endif()
