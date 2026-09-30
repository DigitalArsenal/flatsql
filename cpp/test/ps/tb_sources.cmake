# Terabyte harness (flatsql-ps-terabyte-harness): the unit gates, the engine
# tier and the metadata-only tier, registered on flatsql_ps_test. Included
# from the native section of cpp/CMakeLists.txt after flatsql_ps_test exists.
# The wasm test command picks up the *_test.cpp files through its glob
# (cmake/flatsql_ps_wasm.cmake); their native-only parts compile out there.
if(TARGET flatsql_ps_test)
    target_sources(flatsql_ps_test PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}/tb_corpus.cpp
        ${CMAKE_CURRENT_LIST_DIR}/tb_unit_gates_test.cpp
        ${CMAKE_CURRENT_LIST_DIR}/tb_engine_tier_test.cpp
        ${CMAKE_CURRENT_LIST_DIR}/tb_meta_tier_test.cpp
    )
endif()
