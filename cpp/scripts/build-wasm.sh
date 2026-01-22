#!/bin/bash
# Build FlatSQL as WebAssembly

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
EMSDK_DIR="$PROJECT_DIR/.emsdk/emsdk"
BUILD_DIR="$PROJECT_DIR/build-wasm"
OUTPUT_DIR="$PROJECT_DIR/../wasm"

echo "=== Building FlatSQL WASM ==="
echo ""

# Check if emsdk is installed
if [ ! -f "$EMSDK_DIR/emsdk_env.sh" ]; then
    echo "Error: Emscripten SDK not found."
    echo "Run ./scripts/setup-emsdk.sh first."
    exit 1
fi

# Source emscripten environment
echo "Loading Emscripten environment..."
source "$EMSDK_DIR/emsdk_env.sh"

# Create build directory
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Configure with CMake using Emscripten toolchain
echo ""
echo "Configuring CMake..."
emcmake cmake "$PROJECT_DIR" \
    -DCMAKE_BUILD_TYPE=Release

# Build
echo ""
echo "Building..."
emmake make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Copy output to wasm directory
echo ""
echo "Copying output files..."
mkdir -p "$OUTPUT_DIR"
cp flatsql.js "$OUTPUT_DIR/"
cp flatsql.wasm "$OUTPUT_DIR/"

echo ""
echo "=== Build complete ==="
echo "Output files:"
echo "  $OUTPUT_DIR/flatsql.js"
echo "  $OUTPUT_DIR/flatsql.wasm"
