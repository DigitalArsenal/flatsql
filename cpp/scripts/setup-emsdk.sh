#!/bin/bash
# Setup Emscripten SDK locally (following tudat pattern)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
EMSDK_DIR="$PROJECT_DIR/.emsdk"
EMSDK_INSTALL_DIR="$EMSDK_DIR/emsdk"
EMSDK_VERSION="${EMSDK_VERSION:-3.1.51}"

echo "=== Setting up Emscripten SDK ==="
echo "Project dir: $PROJECT_DIR"
echo "EMSDK dir: $EMSDK_DIR"
echo "EMSDK version: $EMSDK_VERSION"
echo ""

# Create emsdk directory
mkdir -p "$EMSDK_DIR"

# Clone or update emsdk
if [ -d "$EMSDK_INSTALL_DIR" ]; then
    echo "Updating existing emsdk installation..."
    cd "$EMSDK_INSTALL_DIR"
    git pull
else
    echo "Cloning emsdk..."
    git clone https://github.com/emscripten-core/emsdk.git "$EMSDK_INSTALL_DIR"
    cd "$EMSDK_INSTALL_DIR"
fi

# Install and activate the specified version
echo ""
echo "Installing emsdk version $EMSDK_VERSION..."
./emsdk install $EMSDK_VERSION

echo ""
echo "Activating emsdk version $EMSDK_VERSION..."
./emsdk activate $EMSDK_VERSION

# Record installed version
echo "$EMSDK_VERSION" > "$EMSDK_DIR/.installed_version"

echo ""
echo "=== Emscripten SDK setup complete ==="
echo ""
echo "To use emscripten in your shell, run:"
echo "  source $EMSDK_INSTALL_DIR/emsdk_env.sh"
echo ""
echo "Or use the build script which does this automatically:"
echo "  ./scripts/build-wasm.sh"
