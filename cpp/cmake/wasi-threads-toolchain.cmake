# wasm32-wasip1-threads with wasi-sdk: the partition store artifact
# flatsql-ps-threads.wasm and its test commands (docs/PARTITION-STORE-WASM.md).
#
#   cmake -S cpp -B cpp/build-ps-wasm -G Ninja \
#     -DCMAKE_TOOLCHAIN_FILE=cmake/wasi-threads-toolchain.cmake \
#     -DWASI_SDK_PREFIX=/opt/wasi-sdk -DCMAKE_BUILD_TYPE=Release
#
# WASI_SDK_PREFIX defaults to $WASI_SDK_PATH, then /opt/wasi-sdk. The release
# artifact is built with wasi-sdk 30 (scripts/build-wasm.sh checks the version).

if(NOT WASI_SDK_PREFIX)
    if(DEFINED ENV{WASI_SDK_PATH})
        set(WASI_SDK_PREFIX "$ENV{WASI_SDK_PATH}")
    else()
        set(WASI_SDK_PREFIX "/opt/wasi-sdk")
    endif()
endif()
set(WASI_SDK_PREFIX "${WASI_SDK_PREFIX}" CACHE PATH "wasi-sdk installation")
# try_compile projects re-read this file: pass the prefix through.
list(APPEND CMAKE_TRY_COMPILE_PLATFORM_VARIABLES WASI_SDK_PREFIX)
if(NOT EXISTS "${WASI_SDK_PREFIX}/bin/clang")
    message(FATAL_ERROR "wasi-sdk not found at ${WASI_SDK_PREFIX} (set WASI_SDK_PREFIX or WASI_SDK_PATH)")
endif()

# Platform/WASI.cmake ships with wasi-sdk.
list(APPEND CMAKE_MODULE_PATH "${WASI_SDK_PREFIX}/share/cmake")

set(CMAKE_SYSTEM_NAME WASI)
set(CMAKE_SYSTEM_VERSION 1)
set(CMAKE_SYSTEM_PROCESSOR wasm32)
set(FLATSQL_WASI_TRIPLE wasm32-wasip1-threads)

if(WIN32)
    set(FLATSQL_WASI_EXE ".exe")
else()
    set(FLATSQL_WASI_EXE "")
endif()
set(CMAKE_C_COMPILER "${WASI_SDK_PREFIX}/bin/clang${FLATSQL_WASI_EXE}")
set(CMAKE_CXX_COMPILER "${WASI_SDK_PREFIX}/bin/clang++${FLATSQL_WASI_EXE}")
set(CMAKE_ASM_COMPILER "${WASI_SDK_PREFIX}/bin/clang${FLATSQL_WASI_EXE}")
set(CMAKE_AR "${WASI_SDK_PREFIX}/bin/llvm-ar${FLATSQL_WASI_EXE}")
set(CMAKE_RANLIB "${WASI_SDK_PREFIX}/bin/llvm-ranlib${FLATSQL_WASI_EXE}")
set(CMAKE_C_COMPILER_TARGET ${FLATSQL_WASI_TRIPLE})
set(CMAKE_CXX_COMPILER_TARGET ${FLATSQL_WASI_TRIPLE})
set(CMAKE_ASM_COMPILER_TARGET ${FLATSQL_WASI_TRIPLE})
set(CMAKE_SYSROOT "${WASI_SDK_PREFIX}/share/wasi-sysroot")

set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
