cmake_minimum_required(VERSION 3.20)
project(FlatSQL VERSION 1.0.0 LANGUAGES CXX)

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

# Source files
set(FLATSQL_SOURCES
    src/storage.cpp
    src/btree.cpp
    src/schema_parser.cpp
    src/sql_parser.cpp
    src/database.cpp
    src/flatsql.cpp
)

set(FLATSQL_HEADERS
    include/flatsql/storage.h
    include/flatsql/btree.h
    include/flatsql/schema_parser.h
    include/flatsql/sql_parser.h
    include/flatsql/database.h
    include/flatsql/types.h
)

# Emscripten/WASM configuration
if(CMAKE_SYSTEM_NAME STREQUAL "Emscripten" OR EMSCRIPTEN)
    message(STATUS "")
    message(STATUS "*** EMSCRIPTEN/WEBASSEMBLY BUILD ***")
    message(STATUS "")

    set(FLATSQL_BUILD_WASM ON)

    # WASM-specific compiler flags
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexceptions")

    # Create the WASM library
    add_executable(flatsql ${FLATSQL_SOURCES})

    target_include_directories(flatsql PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
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
            -fexceptions \
            --bind \
        "
    )

    message(STATUS "WASM build configured")
    message(STATUS "Output: flatsql.js + flatsql.wasm")

else()
    message(STATUS "Native build (for testing)")

    # Native library for testing
    add_library(flatsql_lib STATIC ${FLATSQL_SOURCES})

    target_include_directories(flatsql_lib PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
    )

    # Native test executable
    add_executable(flatsql_test test/test_main.cpp)
    target_link_libraries(flatsql_test PRIVATE flatsql_lib)
    target_include_directories(flatsql_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${FLATBUFFERS_INCLUDE_DIR}
    )

    enable_testing()
    add_test(NAME FlatSQLTest COMMAND flatsql_test)
endif()
