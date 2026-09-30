# FlatSQL partition store T3 (compaction, reclamation, quota, hot split) and
# later sources and tests:
# sources registered on the targets cpp/CMakeLists.txt defines. Included from
# the native section after flatsql_ps, flatsql_ps_test and flatsql_ps_bench;
# paths are relative to cpp/ (include() evaluates in the including scope).
target_sources(flatsql_ps PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ps/compaction.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ps/reclaim.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ps/quota.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ps/stage1.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ps/hot_split.cpp
)
if(TARGET flatsql_ps_test)
    target_sources(flatsql_ps_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/test/ps/compaction_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test/ps/orphan_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test/ps/quota_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test/ps/hot_split_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test/ps/reader_memory_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test/ps/memory_safety_test.cpp
    )
endif()
