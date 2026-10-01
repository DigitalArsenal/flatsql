# flatsql_p4sql_fake_test: the SQL surface (cpp/src/p4sql) over the format-4
# reader's test double (fake_reader.cpp), with format 1's engine (flatsql_lib)
# as the oracle. It never links the engine (CONTRACT.md §3.9).
#
#   FLATBUFFERS_DIR=<flatbuffers checkout> \
#     cmake -S cpp/tests/p4sql -B build-p4sql -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
#   cmake --build build-p4sql --target flatsql_p4sql_fake_test
#   build-p4sql/flatsql_p4sql_fake_test [name-substring ...]
#
# Fixture tests read P4_FIXTURE (a format-1 control.flatsqldb, opened
# read-only and immutable) and skip without it. The engine's own CMake globs
# this directory's *.cpp into its test binaries; every file here compiles to
# nothing there (FLATSQL_P4SQL_FAKE is defined only by this target).
cmake_minimum_required(VERSION 3.20)
project(flatsql_p4sql_fake_test C CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

get_filename_component(FLATSQL_CPP "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)
add_subdirectory(${FLATSQL_CPP} flatsql EXCLUDE_FROM_ALL)

file(GLOB P4SQL_SOURCES CONFIGURE_DEPENDS ${FLATSQL_CPP}/src/p4sql/*.cpp)
file(GLOB P4SQL_TEST_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
# The flatbuffers schema parser builds test records from JSON with the real
# BFBS (util.cpp and options.cpp come with flatsql_lib).
add_executable(flatsql_p4sql_fake_test
    ${P4SQL_SOURCES}
    ${P4SQL_TEST_SOURCES}
    ${FLATSQL_CPP}/src/ps/result_block.cpp
    ${FLATBUFFERS_DIR}/src/idl_parser.cpp
    ${FLATBUFFERS_DIR}/src/idl_gen_text.cpp
    ${FLATBUFFERS_DIR}/src/file_manager.cpp
    ${FLATBUFFERS_DIR}/src/file_name_manager.cpp
)
target_compile_definitions(flatsql_p4sql_fake_test PRIVATE
    FLATSQL_P4SQL_FAKE=1
    P4SQL_VECTOR_DIR="${CMAKE_CURRENT_SOURCE_DIR}/vectors")
target_include_directories(flatsql_p4sql_fake_test PRIVATE
    ${FLATSQL_CPP}/include
    ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(flatsql_p4sql_fake_test PRIVATE flatsql_lib)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set_source_files_properties(
        ${FLATBUFFERS_DIR}/src/idl_parser.cpp ${FLATBUFFERS_DIR}/src/idl_gen_text.cpp
        ${FLATBUFFERS_DIR}/src/file_manager.cpp ${FLATBUFFERS_DIR}/src/file_name_manager.cpp
        PROPERTIES COMPILE_OPTIONS "-w")
    set_source_files_properties(${P4SQL_SOURCES} PROPERTIES COMPILE_OPTIONS "-Wall;-Wextra;-Wno-unused-parameter")
endif()

enable_testing()
add_test(NAME P4SqlFakeTest COMMAND flatsql_p4sql_fake_test)
