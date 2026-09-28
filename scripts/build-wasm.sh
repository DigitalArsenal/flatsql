#!/bin/bash
# Build the wasm artifacts.
#
#   bash scripts/build-wasm.sh            emscripten artifacts, then the partition store
#   bash scripts/build-wasm.sh --ps       only the partition store (flatsql-ps-threads.wasm)
#   bash scripts/build-wasm.sh --ps-tests the partition store plus its wasm test
#                                         commands (cpp/build-ps-wasm/flatsql-ps-test*.wasm)
#   bash scripts/build-wasm.sh --ps --linux
#                                         the partition store as released: inside
#                                         Docker with the Linux wasi-sdk 30 tarball
#
# The partition store is built with wasi-sdk 30 for wasm32-wasip1-threads
# (docs/PARTITION-STORE-WASM.md). WASI_SDK_PATH (default /opt/wasi-sdk, then
# ~/.local/wasi-sdk/current) names the installation; the build refuses any
# other wasi-sdk version. The released bytes are the Linux build (CI and
# npm-publish.yml; arm64 and x86_64 agree). The macOS wasi-sdk tarball ships a
# different sysroot, compiler-rt and clang build and links different bytes, so
# on macOS use --linux to reproduce the committed artifact.
# FLATBUFFERS_DIR names the DigitalArsenal/flatbuffers checkout (the pin in
# .github/workflows/npm-publish.yml).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
EMSDK_DIR="$PROJECT_ROOT/packages/emsdk"
CPP_DIR="$PROJECT_ROOT/cpp"
WASI_SDK_VERSION="30.0"

MODE="all"
LINUX=0
for arg in "$@"; do
    case "$arg" in
        --ps) MODE="ps" ;;
        --ps-tests) MODE="ps-tests" ;;
        --linux) LINUX=1 ;;
        *) echo "usage: $0 [--ps|--ps-tests] [--linux]"; exit 2 ;;
    esac
done

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

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# The released artifact: Docker, the Linux wasi-sdk 30 release tarball
# (sha256-pinned), the source and flatbuffers mounted read-only.
build_ps_linux() {
    command -v docker >/dev/null 2>&1 || { echo "Error: --linux needs Docker."; exit 1; }
    local arch sha
    case "$(uname -m)" in
        arm64|aarch64) arch=arm64; sha=6f2977942308d91b0123978da3c6a0d6fce780994b3b020008c617e26764ea40 ;;
        x86_64|amd64) arch=x86_64; sha=0507679dff16814b74516cd969a9b16d2ced1347388024bc7966264648c78bfb ;;
        *) echo "Error: no Linux wasi-sdk for $(uname -m)"; exit 1 ;;
    esac
    local cache="${XDG_CACHE_HOME:-$HOME/.cache}/flatsql"
    local tgz="$cache/wasi-sdk-$WASI_SDK_VERSION-$arch-linux.tar.gz"
    mkdir -p "$cache"
    if [ ! -f "$tgz" ]; then
        curl -fsSL -o "$tgz.part" \
            "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-30/wasi-sdk-$WASI_SDK_VERSION-$arch-linux.tar.gz"
        mv "$tgz.part" "$tgz"
    fi
    if [ "$(sha256_of "$tgz")" != "$sha" ]; then
        echo "Error: $tgz does not have the release sha256 $sha"
        exit 1
    fi
    local fb="${FLATBUFFERS_DIR:-$PROJECT_ROOT/../flatbuffers}"
    [ -f "$fb/include/flatbuffers/flatbuffers.h" ] || { echo "Error: FlatBuffers not found at $fb"; exit 1; }
    echo "Building flatsql-ps-threads.wasm in Docker (Linux wasi-sdk $WASI_SDK_VERSION, $arch)..."
    docker run --rm \
        -v "$CPP_DIR":/w/flatsql/cpp:ro \
        -v "$(cd "$fb" && pwd)":/w/flatbuffers:ro \
        -v "$tgz":/w/wasi-sdk.tar.gz:ro \
        -v "$PROJECT_ROOT/wasm":/w/out \
        ubuntu:22.04 bash -euc "
            apt-get update -qq >/dev/null && apt-get install -y -qq cmake ninja-build >/dev/null
            mkdir -p /opt/wasi-sdk && tar -xzf /w/wasi-sdk.tar.gz -C /opt/wasi-sdk --strip-components=1
            cmake -S /w/flatsql/cpp -B /tmp/b -G Ninja \
                -DCMAKE_TOOLCHAIN_FILE=/w/flatsql/cpp/cmake/wasi-threads-toolchain.cmake \
                -DWASI_SDK_PREFIX=/opt/wasi-sdk -DFLATBUFFERS_DIR=/w/flatbuffers -DCMAKE_BUILD_TYPE=Release >/dev/null
            cmake --build /tmp/b --target flatsql_ps_threads -j ${FLATSQL_BUILD_JOBS:-8}
            cp /tmp/b/flatsql-ps-threads.wasm /w/out/"
}

if [ "$MODE" = "all" ]; then
    build_emscripten
fi
if [ "$LINUX" = "1" ]; then
    [ "$MODE" = "ps" ] || { echo "Error: --linux builds only the artifact (--ps)"; exit 2; }
    build_ps_linux
elif [ "$MODE" = "ps-tests" ]; then
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
