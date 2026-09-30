#!/usr/bin/env bash
# Builds flatsql_ps_test (with the terabyte harness) for linux/amd64 in Docker:
# ubuntu:24.04 (glibc 2.39) with the x86_64 GNU cross toolchain, so an arm64
# host builds at native speed. libstdc++ and libgcc are linked statically.
#
#   scripts/tb/build-linux-amd64.sh <flatsql checkout> <flatbuffers checkout> <out dir> [jobs]
#
# Writes <out>/flatsql_ps_test. Until cpp/CMakeLists.txt includes
# cpp/test/ps/tb_sources.cmake, a wrapper project registers the TB sources.
set -euo pipefail
SRC=$(cd "${1:?flatsql checkout}" && pwd)
FB=$(cd "${2:?flatbuffers checkout}" && pwd)
OUT=${3:?out dir}
JOBS=${4:-6}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
docker run --rm --platform linux/arm64 \
  -v "$SRC":/src:ro -v "$FB":/fb:ro -v "$OUT":/out \
  -e JOBS="$JOBS" ubuntu:24.04 bash -euo pipefail -c '
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq >/dev/null
    apt-get install -y -qq cmake make g++-x86-64-linux-gnu gcc-x86-64-linux-gnu >/dev/null
    mkdir -p /wrap
    if grep -q tb_sources.cmake /src/cpp/CMakeLists.txt; then
      SRCDIR=/src/cpp
    else
      printf "cmake_minimum_required(VERSION 3.20)\nproject(tbwrap C CXX)\nadd_subdirectory(/src/cpp flatsql)\ninclude(/src/cpp/test/ps/tb_sources.cmake)\n" > /wrap/CMakeLists.txt
      SRCDIR=/wrap
    fi
    cmake -S "$SRCDIR" -B /build -DCMAKE_BUILD_TYPE=RelWithDebInfo -DFLATBUFFERS_DIR=/fb \
      -DCMAKE_SYSTEM_NAME=Linux -DCMAKE_SYSTEM_PROCESSOR=x86_64 \
      -DCMAKE_C_COMPILER=x86_64-linux-gnu-gcc -DCMAKE_CXX_COMPILER=x86_64-linux-gnu-g++ \
      -DCMAKE_EXE_LINKER_FLAGS="-static-libstdc++ -static-libgcc" >/build.log 2>&1 || { tail -40 /build.log; exit 1; }
    cmake --build /build --target flatsql_ps_test -j "$JOBS" >>/build.log 2>&1 || { grep -E "error|Error" /build.log | head -40; exit 1; }
    BIN=$(find /build -name flatsql_ps_test -type f -perm -u+x | head -1)
    cp "$BIN" /out/flatsql_ps_test
    x86_64-linux-gnu-objdump -p /out/flatsql_ps_test | grep -E "NEEDED|GLIBC_2\.[0-9]+" | sort -u | tail -12
  '
file "$OUT/flatsql_ps_test"
