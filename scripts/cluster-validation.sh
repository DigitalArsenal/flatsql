#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="${TMPDIR:-/tmp}/flatsql-cpp-build"

cmake -S "$PROJECT_ROOT/cpp" -B "$BUILD_DIR"
cmake --build "$BUILD_DIR" --target flatsql_cluster_validation -j4
"$BUILD_DIR/flatsql_cluster_validation" "$@"
