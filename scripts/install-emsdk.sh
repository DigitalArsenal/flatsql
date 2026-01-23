#!/bin/bash
# Install Emscripten SDK locally in packages/emsdk

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
EMSDK_DIR="$PROJECT_ROOT/packages/emsdk"

# Check if emsdk is already installed and activated
if [ -f "$EMSDK_DIR/emsdk_env.sh" ]; then
    echo "emsdk already installed at $EMSDK_DIR"
    # Ensure latest is installed and activated
    cd "$EMSDK_DIR"
    ./emsdk install latest
    ./emsdk activate latest
    echo "emsdk is ready"
    exit 0
fi

echo "Installing Emscripten SDK to $EMSDK_DIR..."

# Clone emsdk
git clone https://github.com/emscripten-core/emsdk.git "$EMSDK_DIR"

cd "$EMSDK_DIR"

# Install and activate the latest SDK
./emsdk install latest
./emsdk activate latest

echo ""
echo "Emscripten SDK installed successfully!"
echo "Location: $EMSDK_DIR"
echo ""
echo "To use in this shell, run:"
echo "  source $EMSDK_DIR/emsdk_env.sh"
