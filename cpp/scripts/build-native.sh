#!/bin/bash
# Build FlatSQL native (for testing)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"

echo "=== Building FlatSQL Native ==="
echo ""

# Create build directory
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Configure with CMake
echo "Configuring CMake..."
cmake "$PROJECT_DIR" \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

# Build
echo ""
echo "Building..."
make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

echo ""
echo "=== Build complete ==="
echo ""
echo "To run tests:"
echo "  cd $BUILD_DIR && ctest --output-on-failure"
echo "  # or"
echo "  $BUILD_DIR/flatsql_test"
