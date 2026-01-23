#!/bin/bash
# Build WASM using locally installed emsdk

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
EMSDK_DIR="$PROJECT_ROOT/packages/emsdk"
CPP_DIR="$PROJECT_ROOT/cpp"

# Check if emsdk is installed
if [ ! -f "$EMSDK_DIR/emsdk_env.sh" ]; then
    echo "Error: emsdk not found. Run 'npm run emsdk:install' first."
    exit 1
fi

# Source emsdk environment
echo "Sourcing emsdk environment..."
source "$EMSDK_DIR/emsdk_env.sh"

# Configure and build
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

# Copy to wasm/ directory
echo "Copying WASM files to wasm/..."
cp build-wasm/flatsql.js build-wasm/flatsql.wasm "$PROJECT_ROOT/wasm/"

echo ""
echo "WASM build complete!"
echo "Output files:"
ls -la "$PROJECT_ROOT/wasm/flatsql.js" "$PROJECT_ROOT/wasm/flatsql.wasm"
