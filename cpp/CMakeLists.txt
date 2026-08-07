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
    src/query_cache.cpp
    src/junction.cpp
    src/sqlite_vtab.cpp
    src/sqlite_engine.cpp
    src/geo_functions.cpp
    src/geo_geometry.cpp
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
    include/flatsql/query_cache.h
    include/flatsql/junction.h
    include/flatsql/types.h
    include/flatsql/sqlite_vtab.h
    include/flatsql/sqlite_engine.h
    include/flatsql/geo_geometry.h
)

# Emscripten/WASM configuration
if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten" OR EMSCRIPTEN)
    message(STATUS "")
    message(STATUS "*** EMSCRIPTEN/WEBASSEMBLY BUILD ***")
    message(STATUS "")

    set(FLATSQL_BUILD_WASM ON)

    # WASM-specific compiler flags are applied per target below. The browser
    # bundle uses Emscripten JS exception helpers, while the standalone reactor
    # must stay import-clean for WasmEdge/browser direct instantiation.
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")

    # Create the WASM library (includes SQLite amalgamation directly)
    add_executable(flatsql ${FLATSQL_WASM_SOURCES} ${SQLITE_DIR}/sqlite3.c)

    target_include_directories(flatsql PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
        ${SQLEAN_DIR}
    )
    target_compile_options(flatsql PRIVATE -fexceptions)

    # SQLite compile definitions for minimal WASM build
    target_compile_definitions(flatsql PRIVATE
        SQLITE_THREADSAFE=0
        SQLITE_OMIT_LOAD_EXTENSION=1
        SQLITE_OMIT_WAL=1
        SQLITE_OMIT_DEPRECATED=1
        SQLITE_OMIT_SHARED_CACHE=1
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
                \"_flatsql_open_db\", \"_flatsql_is_disk_backed\", \
                \"_flatsql_register_file_id\", \"_flatsql_enable_demo_extractors\", \
                \"_flatsql_ingest\", \"_flatsql_ingest_one\", \
                \"_flatsql_register_source\", \"_flatsql_create_unified_views\", \
                \"_flatsql_ingest_with_source\", \"_flatsql_ingest_one_with_source\", \
                \"_flatsql_get_sources_count\", \"_flatsql_get_source_name\", \
                \"_flatsql_query\", \"_flatsql_query_params\", \"_flatsql_query_many\", \
                \"_flatsql_build_query_cache_key\", \
                \"_flatsql_build_response_artifact_cache_key\", \
                \"_flatsql_register_query_template\", \"_flatsql_query_template\", \
                \"_flatsql_clear_query_cache\", \"_flatsql_configure_query_cache\", \
                \"_flatsql_query_cache_hits\", \
                \"_flatsql_query_cache_misses\", \"_flatsql_query_cache_size\", \
                \"_flatsql_query_cache_generation\", \"_flatsql_query_cache_max_entries\", \
                \"_flatsql_query_cache_max_rows\", \
                \"_flatsql_batch_result_count\", \"_flatsql_select_batch_result\", \"_flatsql_get_error\", \
                \"_flatsql_result_column_count\", \"_flatsql_result_row_count\", \
                \"_flatsql_result_column_name\", \"_flatsql_result_cell_type\", \
                \"_flatsql_result_cell_number\", \"_flatsql_result_cell_string\", \
                \"_flatsql_result_cell_blob\", \"_flatsql_result_cell_blob_size\", \
                \"_flatsql_query_sandboxed\", \
                \"_flatsql_query_raw_flatbuffer_stream\", \"_flatsql_response_artifact_data\", \
                \"_flatsql_response_artifact_size\", \"_flatsql_response_artifact_row_count\", \
                \"_flatsql_response_artifact_column_count\", \"_flatsql_response_artifact_cache_hit\", \
                \"_flatsql_configure_raw_stream_cache\", \"_flatsql_raw_stream_cache_hits\", \
                \"_flatsql_raw_stream_cache_misses\", \"_flatsql_raw_stream_cache_size\", \
                \"_flatsql_raw_stream_cache_total_bytes\", \
                \"_flatsql_export_data\", \"_flatsql_export_size\", \
                \"_flatsql_load_and_rebuild\", \"_flatsql_reserve_storage\", \"_flatsql_load_from_db\", \
                \"_flatsql_create_test_user\", \"_flatsql_create_test_post\", \
                \"_flatsql_create_test_mpe\", \"_flatsql_create_test_telemetry\", \
                \"_flatsql_create_test_publish_event\", \
                \"_flatsql_test_buffer_size\", \
                \"_flatsql_get_stats_count\", \"_flatsql_get_stat_table_name\", \
                \"_flatsql_get_stat_file_id\", \"_flatsql_get_stat_record_count\", \
                \"_flatsql_reset_ingest_profile\", \"_flatsql_get_ingest_profile_record_count\", \
                \"_flatsql_get_ingest_profile_byte_count\", \"_flatsql_get_ingest_profile_decode_nanos\", \
                \"_flatsql_get_ingest_profile_append_nanos\", \"_flatsql_get_ingest_profile_index_nanos\", \
                \"_flatsql_mark_deleted\", \"_flatsql_get_deleted_count\", \
                \"_flatsql_clear_tombstones\", \
                \"_flatsql_get_flatbuffer_by_id\", \"_flatsql_get_flatbuffer_by_email\", \
                \"_flatsql_get_flatbuffer_by_index\", \
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
            -s MAXIMUM_MEMORY=4294967296 \
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

    # Build a standalone WASI-compatible module for Go runtimes (wazero/wasmtime)
    add_executable(flatsql_wasi ${FLATSQL_WASM_SOURCES} ${SQLITE_DIR}/sqlite3.c)

    target_include_directories(flatsql_wasi PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
        ${SQLEAN_DIR}
    )
    target_compile_options(flatsql_wasi PRIVATE -fwasm-exceptions -sWASM_LEGACY_EXCEPTIONS=0)

    target_compile_definitions(flatsql_wasi PRIVATE
        SQLITE_THREADSAFE=0
        SQLITE_OMIT_LOAD_EXTENSION=1
        SQLITE_OMIT_WAL=1
        SQLITE_OMIT_DEPRECATED=1
        SQLITE_OMIT_SHARED_CACHE=1
        SQLITE_DEFAULT_MEMSTATUS=0
        SQLITE_DQS=0
        SQLITE_ENABLE_RTREE=1
    )

    # Shared WASI link flags (everything except the exception strategy) so the
    # exceptions and no-exceptions artifacts stay export-identical.
    set(FLATSQL_WASI_BASE_LINK_FLAGS "\
            -s WASM=1 \
            -s STANDALONE_WASM=1 \
            -s PURE_WASI=1 \
            --no-entry \
            -s EXPORTED_FUNCTIONS='[ \
                \"_malloc\", \"_free\", \
                \"_flatsql_create_db\", \"_flatsql_destroy_db\", \
                \"_flatsql_open_db\", \"_flatsql_is_disk_backed\", \
                \"_flatsql_register_file_id\", \"_flatsql_enable_demo_extractors\", \
                \"_flatsql_ingest\", \"_flatsql_ingest_one\", \
                \"_flatsql_register_source\", \"_flatsql_create_unified_views\", \
                \"_flatsql_ingest_with_source\", \"_flatsql_ingest_one_with_source\", \
                \"_flatsql_get_sources_count\", \"_flatsql_get_source_name\", \
                \"_flatsql_query\", \"_flatsql_query_params\", \"_flatsql_query_many\", \
                \"_flatsql_build_query_cache_key\", \
                \"_flatsql_build_response_artifact_cache_key\", \
                \"_flatsql_register_query_template\", \"_flatsql_query_template\", \
                \"_flatsql_clear_query_cache\", \"_flatsql_configure_query_cache\", \
                \"_flatsql_query_cache_hits\", \
                \"_flatsql_query_cache_misses\", \"_flatsql_query_cache_size\", \
                \"_flatsql_query_cache_generation\", \"_flatsql_query_cache_max_entries\", \
                \"_flatsql_query_cache_max_rows\", \
                \"_flatsql_batch_result_count\", \"_flatsql_select_batch_result\", \"_flatsql_get_error\", \
                \"_flatsql_result_column_count\", \"_flatsql_result_row_count\", \
                \"_flatsql_result_column_name\", \"_flatsql_result_cell_type\", \
                \"_flatsql_result_cell_number\", \"_flatsql_result_cell_string\", \
                \"_flatsql_result_cell_blob\", \"_flatsql_result_cell_blob_size\", \
                \"_flatsql_query_sandboxed\", \
                \"_flatsql_query_raw_flatbuffer_stream\", \"_flatsql_response_artifact_data\", \
                \"_flatsql_response_artifact_size\", \"_flatsql_response_artifact_row_count\", \
                \"_flatsql_response_artifact_column_count\", \"_flatsql_response_artifact_cache_hit\", \
                \"_flatsql_configure_raw_stream_cache\", \"_flatsql_raw_stream_cache_hits\", \
                \"_flatsql_raw_stream_cache_misses\", \"_flatsql_raw_stream_cache_size\", \
                \"_flatsql_raw_stream_cache_total_bytes\", \
                \"_flatsql_export_data\", \"_flatsql_export_size\", \
                \"_flatsql_load_and_rebuild\", \"_flatsql_reserve_storage\", \"_flatsql_load_from_db\", \
                \"_flatsql_create_test_user\", \"_flatsql_create_test_post\", \
                \"_flatsql_create_test_mpe\", \"_flatsql_create_test_telemetry\", \
                \"_flatsql_create_test_publish_event\", \
                \"_flatsql_test_buffer_size\", \
                \"_flatsql_get_stats_count\", \"_flatsql_get_stat_table_name\", \
                \"_flatsql_get_stat_file_id\", \"_flatsql_get_stat_record_count\", \
                \"_flatsql_reset_ingest_profile\", \"_flatsql_get_ingest_profile_record_count\", \
                \"_flatsql_get_ingest_profile_byte_count\", \"_flatsql_get_ingest_profile_decode_nanos\", \
                \"_flatsql_get_ingest_profile_append_nanos\", \"_flatsql_get_ingest_profile_index_nanos\", \
                \"_flatsql_mark_deleted\", \"_flatsql_get_deleted_count\", \
                \"_flatsql_clear_tombstones\", \
                \"_flatsql_get_flatbuffer_by_id\", \"_flatsql_get_flatbuffer_by_email\", \
                \"_flatsql_get_flatbuffer_by_index\", \
                \"_flatsql_get_raw_flatbuffer_size\", \"_flatsql_get_raw_flatbuffer_sequence\", \
                \"_flatsql_get_storage_buffer\", \"_flatsql_get_storage_size\", \
                \"_flatsql_set_encryption_key\", \"_flatsql_is_encrypted\", \
                \"_flatsql_encrypt_buffer\", \"_flatsql_decrypt_buffer\", \
                \"_flatsql_set_hmac_verification\", \"_flatsql_is_hmac_enabled\", \
                \"_flatsql_compute_hmac\", \"_flatsql_verify_hmac\" \
            ]' \
            -s ALLOW_MEMORY_GROWTH=1 \
            -s INITIAL_MEMORY=67108864 \
            -s MAXIMUM_MEMORY=4294967296 \
            -s STACK_SIZE=1048576 \
            -s WASMFS=1 \
            -s ALLOW_TABLE_GROWTH=1 \
            -s EMULATE_FUNCTION_POINTER_CASTS=1 \
        ")

    set_target_properties(flatsql_wasi PROPERTIES
        OUTPUT_NAME "flatsql-wasi"
        SUFFIX ".wasm"
        LINK_FLAGS "${FLATSQL_WASI_BASE_LINK_FLAGS} -fwasm-exceptions -s WASM_LEGACY_EXCEPTIONS=0"
    )

    add_custom_command(TARGET flatsql_wasi POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE_DIR:flatsql_wasi>/flatsql-wasi.wasm
            ${CMAKE_CURRENT_SOURCE_DIR}/../docs/
        COMMENT "Copying flatsql-wasi.wasm to docs/"
    )

    # No-exceptions WASI variant: identical sources/exports, but throws abort
    # instead of unwinding. Server hosts need this because WasmEdge's AOT
    # compiler (0.14–0.17) cannot parse wasm-exceptions (exnref) modules and
    # its interpreter is ~100x too slow for query workloads. Interim artifact
    # until the engine's error paths are exception-free by construction.
    add_executable(flatsql_wasi_noeh ${FLATSQL_WASM_SOURCES} ${SQLITE_DIR}/sqlite3.c)

    target_include_directories(flatsql_wasi_noeh PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
        ${SQLEAN_DIR}
    )
    target_compile_options(flatsql_wasi_noeh PRIVATE -fignore-exceptions)

    target_compile_definitions(flatsql_wasi_noeh PRIVATE
        SQLITE_THREADSAFE=0
        SQLITE_OMIT_LOAD_EXTENSION=1
        SQLITE_OMIT_WAL=1
        SQLITE_OMIT_DEPRECATED=1
        SQLITE_OMIT_SHARED_CACHE=1
        SQLITE_DEFAULT_MEMSTATUS=0
        SQLITE_DQS=0
        SQLITE_ENABLE_RTREE=1
    )

    set_target_properties(flatsql_wasi_noeh PROPERTIES
        OUTPUT_NAME "flatsql-wasi-noeh"
        SUFFIX ".wasm"
        LINK_FLAGS "${FLATSQL_WASI_BASE_LINK_FLAGS}"
    )

    add_custom_command(TARGET flatsql_wasi_noeh POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE_DIR:flatsql_wasi_noeh>/flatsql-wasi-noeh.wasm
            ${CMAKE_CURRENT_SOURCE_DIR}/../wasm/
        COMMENT "Copying flatsql-wasi-noeh.wasm to wasm/"
    )

    # Independently packaged SDN FlatSQL node. The generated directory is
    # produced by wasm/node/build.mjs from the canonical PLG and SDS PIV/FSO/FSB
    # schemas before CMake configuration.
    set(
        FLATSQL_SDN_NODE_GENERATED_DIR
        "${CMAKE_CURRENT_SOURCE_DIR}/../wasm/node/.build/generated"
        CACHE PATH
        "Generated SDN FlatSQL node ABI sources"
    )
    if(EXISTS "${FLATSQL_SDN_NODE_GENERATED_DIR}/invoke_support.cpp")
        add_library(
            flatsql_sdn_node_capi
            OBJECT
            src/flatsql_capi.cpp
        )
        target_include_directories(flatsql_sdn_node_capi PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${FLATBUFFERS_INCLUDE_DIR}
            ${SQLITE_DIR}
            ${SQLEAN_DIR}
        )
        target_compile_options(
            flatsql_sdn_node_capi
            PRIVATE
                -fignore-exceptions
                -include
                ${FLATSQL_SDN_NODE_GENERATED_DIR}/private_capi_exports.h
        )
        target_compile_definitions(flatsql_sdn_node_capi PRIVATE
            SQLITE_THREADSAFE=0
            SQLITE_OMIT_LOAD_EXTENSION=1
            SQLITE_OMIT_WAL=1
            SQLITE_OMIT_DEPRECATED=1
            SQLITE_OMIT_SHARED_CACHE=1
            SQLITE_DEFAULT_MEMSTATUS=0
            SQLITE_DQS=0
            SQLITE_ENABLE_RTREE=1
        )
        add_executable(
            flatsql_sdn_node
            ${FLATSQL_LIB_SOURCES}
            $<TARGET_OBJECTS:flatsql_sdn_node_capi>
            ${FLATBUFFERS_SRC_DIR}/file_manager.cpp
            ${FLATBUFFERS_SRC_DIR}/file_name_manager.cpp
            ${FLATBUFFERS_SRC_DIR}/idl_parser.cpp
            ${FLATBUFFERS_SRC_DIR}/idl_gen_text.cpp
            ${FLATBUFFERS_SRC_DIR}/reflection.cpp
            ${FLATBUFFERS_SRC_DIR}/util.cpp
            ${FLATBUFFERS_SRC_DIR}/options.cpp
            src/sdn_node.cpp
            ${FLATSQL_SDN_NODE_GENERATED_DIR}/invoke_support.cpp
            ${FLATSQL_SDN_NODE_GENERATED_DIR}/plugin_manifest_exports.c
            ${SQLITE_DIR}/sqlite3.c
        )

        target_include_directories(flatsql_sdn_node PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${FLATBUFFERS_INCLUDE_DIR}
            ${SQLITE_DIR}
            ${SQLEAN_DIR}
            ${FLATSQL_SDN_NODE_GENERATED_DIR}
        )
        target_compile_options(
            flatsql_sdn_node
            PRIVATE
                -fignore-exceptions
                -fvisibility=hidden
        )
        target_compile_definitions(flatsql_sdn_node PRIVATE
            SQLITE_THREADSAFE=0
            SQLITE_OMIT_LOAD_EXTENSION=1
            SQLITE_OMIT_WAL=1
            SQLITE_OMIT_DEPRECATED=1
            SQLITE_OMIT_SHARED_CACHE=1
            SQLITE_DEFAULT_MEMSTATUS=0
            SQLITE_DQS=0
            SQLITE_ENABLE_RTREE=1
        )
        set_target_properties(flatsql_sdn_node PROPERTIES
            OUTPUT_NAME "flatsql-sdn-node"
            SUFFIX ".wasm"
            LINK_FLAGS "\
                -s WASM=1 \
                -s STANDALONE_WASM=1 \
                -s PURE_WASI=1 \
                --no-entry \
                -s EXPORTED_FUNCTIONS='[\
                    \"_plugin_alloc\", \"_plugin_free\", \
                    \"_plugin_invoke_stream\", \
                    \"_plugin_get_manifest_flatbuffer\", \
                    \"_plugin_get_manifest_flatbuffer_size\" \
                ]' \
                -s EXPORT_KEEPALIVE=0 \
                -s ALLOW_MEMORY_GROWTH=1 \
                -s INITIAL_MEMORY=67108864 \
                -s MAXIMUM_MEMORY=4294967296 \
                -s STACK_SIZE=1048576 \
                -s WASMFS=1 \
                -s ALLOW_TABLE_GROWTH=1 \
                -s EMULATE_FUNCTION_POINTER_CASTS=1 \
            "
        )
    endif()

    # Space Data Module (SDM) — standalone spatial engine WASM
    # Compliant with https://github.com/DigitalArsenal/space-data-module-sdk
    set(SDM_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/../sdm/sdm_entry.cpp
        src/geo_geometry.cpp
    )

    add_executable(flatsql_sdm ${SDM_SOURCES})

    target_include_directories(flatsql_sdm PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/../sdm
    )

    set_target_properties(flatsql_sdm PROPERTIES
        OUTPUT_NAME "flatsql-spatial"
        SUFFIX ".wasm"
        LINK_FLAGS "\
            -s WASM=1 \
            -s STANDALONE_WASM=1 \
            --no-entry \
            -s EXPORTED_FUNCTIONS='[ \
                \"_malloc\", \"_free\", \
                \"_plugin_get_manifest\", \"_plugin_get_manifest_size\", \
                \"_sdm_get_input_buffer\", \"_sdm_set_input_length\", \
                \"_sdm_get_output_buffer\", \"_sdm_get_output_length\", \
                \"_compute_distance\", \"_compute_bearing\", \
                \"_point_in_polygon\", \
                \"_polygon_intersection\", \"_polygon_union\", \
                \"_compute_voronoi\", \"_compute_delaunay\", \
                \"_transform_to_ecef\", \"_transform_from_ecef\", \
                \"_spatial_query\" \
            ]' \
            -s ALLOW_MEMORY_GROWTH=1 \
            -s INITIAL_MEMORY=4194304 \
            -s STACK_SIZE=524288 \
            -s FILESYSTEM=0 \
            -s ALLOW_TABLE_GROWTH=1 \
            -fexceptions \
            -s DISABLE_EXCEPTION_CATCHING=0 \
        "
    )

    add_custom_command(TARGET flatsql_sdm POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE_DIR:flatsql_sdm>/flatsql-spatial.wasm
            ${CMAKE_CURRENT_SOURCE_DIR}/../sdm/
        COMMENT "Copying flatsql-spatial.wasm to sdm/"
    )

    message(STATUS "WASM build configured")
    message(STATUS "Output: flatsql.js + flatsql.wasm + flatsql-wasi.wasm + flatsql-spatial.wasm (SDM)")

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
    find_package(Threads REQUIRED)

    # SQLite static library (from amalgamation)
    add_library(sqlite3 STATIC ${SQLITE_DIR}/sqlite3.c)
    target_include_directories(sqlite3 PUBLIC ${SQLITE_DIR})
    target_compile_definitions(sqlite3 PRIVATE
        SQLITE_THREADSAFE=1
        SQLITE_OMIT_LOAD_EXTENSION=1
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
    target_link_libraries(flatsql_lib PUBLIC sqlite3 Threads::Threads)
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

    # Native cluster validation harness
    add_executable(flatsql_cluster_validation test/cluster_validation_test.cpp)
    target_link_libraries(flatsql_cluster_validation PRIVATE flatsql_lib)
    target_include_directories(flatsql_cluster_validation PRIVATE
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

    # Spatial extensions benchmark
    add_executable(flatsql_geo_benchmark test/geo_benchmark.cpp)
    target_link_libraries(flatsql_geo_benchmark PRIVATE flatsql_lib)
    target_include_directories(flatsql_geo_benchmark PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
    )
    target_compile_options(flatsql_geo_benchmark PRIVATE -O3)

    # Spatial extensions test
    add_executable(flatsql_geo_test test/test_geo.cpp)
    target_link_libraries(flatsql_geo_test PRIVATE flatsql_lib)
    target_include_directories(flatsql_geo_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
    )

    # Disk-backed engine persistence (docs/STORAGE-DURABILITY.md)
    add_executable(flatsql_disk_persistence_test test/disk_persistence_test.cpp)
    target_link_libraries(flatsql_disk_persistence_test PRIVATE flatsql_lib)
    target_include_directories(flatsql_disk_persistence_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
        ${SQLITE_DIR}
    )

    enable_testing()
    add_test(NAME FlatSQLDiskPersistenceTest COMMAND flatsql_disk_persistence_test)
    add_test(NAME FlatSQLTest COMMAND flatsql_test)
    add_test(NAME FlatSQLIntegrationTest COMMAND flatsql_integration_test)
    add_test(NAME FlatSQLSocketTest COMMAND flatsql_socket_test)
    add_test(NAME FlatSQLRoundTripTest COMMAND flatsql_roundtrip_test)
    add_test(NAME FlatSQLGeoTest COMMAND flatsql_geo_test)
endif()
