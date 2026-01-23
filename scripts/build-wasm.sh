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

# Generate integrity hash for WASM file (SHA-384, base64 encoded)
echo "Generating integrity hash..."
WASM_FILE="$PROJECT_ROOT/wasm/flatsql.wasm"
WASM_SIZE=$(wc -c < "$WASM_FILE" | tr -d ' ')

# Generate SHA-384 hash (compatible with SRI)
if command -v shasum &> /dev/null; then
    WASM_HASH=$(shasum -a 384 "$WASM_FILE" | cut -d' ' -f1 | xxd -r -p | base64)
elif command -v sha384sum &> /dev/null; then
    WASM_HASH=$(sha384sum "$WASM_FILE" | cut -d' ' -f1 | xxd -r -p | base64)
else
    echo "Warning: No sha384 command found, skipping integrity hash generation"
    WASM_HASH=""
fi

# Write integrity file
if [ -n "$WASM_HASH" ]; then
    cat > "$PROJECT_ROOT/wasm/integrity.json" << EOF
{
  "algorithm": "sha384",
  "hash": "$WASM_HASH",
  "sri": "sha384-$WASM_HASH",
  "size": $WASM_SIZE,
  "generatedAt": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF
    echo "Integrity hash written to wasm/integrity.json"
    echo "  SRI: sha384-$WASM_HASH"
fi

echo ""
echo "WASM build complete!"
echo "Output files:"
ls -la "$PROJECT_ROOT/wasm/flatsql.js" "$PROJECT_ROOT/wasm/flatsql.wasm" "$PROJECT_ROOT/wasm/integrity.json" 2>/dev/null || ls -la "$PROJECT_ROOT/wasm/flatsql.js" "$PROJECT_ROOT/wasm/flatsql.wasm"
