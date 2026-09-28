#!/bin/bash
# Build the wasm artifacts.
#
#   bash scripts/build-wasm.sh            emscripten artifacts, then the partition store
#   bash scripts/build-wasm.sh --ps       only the partition store (flatsql-ps-threads.wasm)
#   bash scripts/build-wasm.sh --ps-tests the partition store plus its wasm test
#                                         commands (cpp/build-ps-wasm/flatsql-ps-test*.wasm)
#
# The partition store is built with wasi-sdk 30 for wasm32-wasip1-threads
# (docs/PARTITION-STORE-WASM.md). WASI_SDK_PATH (default /opt/wasi-sdk, then
# ~/.local/wasi-sdk/current) names the installation; the build refuses any
# other wasi-sdk version, because the shipped bytes must be reproducible.
# FLATBUFFERS_DIR names the DigitalArsenal/flatbuffers checkout (the pin in
# .github/workflows/npm-publish.yml).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
EMSDK_DIR="$PROJECT_ROOT/packages/emsdk"
CPP_DIR="$PROJECT_ROOT/cpp"
WASI_SDK_VERSION="30.0"

MODE="all"
case "${1:-}" in
    --ps) MODE="ps" ;;
    --ps-tests) MODE="ps-tests" ;;
    "") ;;
    *) echo "usage: $0 [--ps|--ps-tests]"; exit 2 ;;
esac

build_emscripten() {
    # Prefer the repo-local emsdk when present, but allow production builders and
    # Homebrew-based developer machines to provide emcmake/emcc on PATH.
    if [ -f "$EMSDK_DIR/emsdk_env.sh" ]; then
        echo "Sourcing emsdk environment..."
        source "$EMSDK_DIR/emsdk_env.sh"
    elif command -v emcmake >/dev/null 2>&1 && command -v emcc >/dev/null 2>&1; then
        echo "Using emsdk tools from PATH..."
    else
        echo "Error: emsdk not found. Run 'npm run emsdk:install' or provide emcmake/emcc on PATH."
        exit 1
    fi

    cd "$CPP_DIR"

    # Clear stale cache if it exists from a different directory
    if [ -f "build-wasm/CMakeCache.txt" ]; then
        CACHE_DIR=$(grep "CMAKE_HOME_DIRECTORY:INTERNAL" build-wasm/CMakeCache.txt 2>/dev/null | cut -d= -f2)
        if [ -n "$CACHE_DIR" ] && [ "$CACHE_DIR" != "$CPP_DIR" ]; then
            echo "Clearing stale CMake cache..."
            rm -rf build-wasm
        fi
    fi

    echo "Configuring WASM build..."
    emcmake cmake -B build-wasm -DCMAKE_BUILD_TYPE=Release

    echo "Building WASM..."
    cmake --build build-wasm --config Release

    echo "Copying WASM files to wasm/..."
    cp build-wasm/flatsql.js build-wasm/flatsql.wasm "$PROJECT_ROOT/wasm/"
    if [ -f build-wasm/flatsql-wasi.wasm ]; then
        cp build-wasm/flatsql-wasi.wasm "$PROJECT_ROOT/wasm/"
    fi
    if [ -f build-wasm/flatsql-spatial.wasm ]; then
        cp build-wasm/flatsql-spatial.wasm "$PROJECT_ROOT/sdm/"
    fi
    cd "$PROJECT_ROOT"
}

find_wasi_sdk() {
    for candidate in "${WASI_SDK_PATH:-}" /opt/wasi-sdk "$HOME/.local/wasi-sdk/current"; do
        if [ -n "$candidate" ] && [ -x "$candidate/bin/clang" ]; then
            echo "$candidate"
            return 0
        fi
    done
    return 1
}

build_ps() {
    local targets="$1"
    local sdk
    if ! sdk="$(find_wasi_sdk)"; then
        echo "Error: wasi-sdk $WASI_SDK_VERSION not found (set WASI_SDK_PATH)."
        exit 1
    fi
    local version
    version="$(head -1 "$sdk/VERSION" 2>/dev/null || true)"
    if [ "$version" != "$WASI_SDK_VERSION" ]; then
        echo "Error: $sdk is wasi-sdk '$version'; flatsql-ps-threads.wasm is built with $WASI_SDK_VERSION."
        exit 1
    fi
    local generator=()
    if command -v ninja >/dev/null 2>&1; then generator=(-G Ninja); fi
    echo "Configuring the partition store build (wasi-sdk $version at $sdk)..."
    cmake -S "$CPP_DIR" -B "$CPP_DIR/build-ps-wasm" "${generator[@]}" \
        -DCMAKE_TOOLCHAIN_FILE="$CPP_DIR/cmake/wasi-threads-toolchain.cmake" \
        -DWASI_SDK_PREFIX="$sdk" -DCMAKE_BUILD_TYPE=Release
    # shellcheck disable=SC2086
    cmake --build "$CPP_DIR/build-ps-wasm" --target $targets -j "${FLATSQL_BUILD_JOBS:-8}"
    cp "$CPP_DIR/build-ps-wasm/flatsql-ps-threads.wasm" "$PROJECT_ROOT/wasm/"
}

if [ "$MODE" = "all" ]; then
    build_emscripten
fi
if [ "$MODE" = "ps-tests" ]; then
    build_ps "flatsql_ps_threads flatsql_ps_test_wasm flatsql_ps_test_memio"
else
    build_ps "flatsql_ps_threads"
fi

cd "$PROJECT_ROOT"
node scripts/check-wasm-imports.mjs
node scripts/write-integrity.mjs

echo ""
echo "WASM build complete:"
ls -la "$PROJECT_ROOT"/wasm/*.wasm "$PROJECT_ROOT/wasm/integrity.json"
