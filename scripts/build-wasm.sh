#!/usr/bin/env bash
# One reproducible wasm32-wasip1-threads artifact for every supported host.
set -euo pipefail
cd "$(dirname "$0")/.."
root="$PWD"
: "${FLATBUFFERS_DIR:?Set FLATBUFFERS_DIR to the pinned FlatBuffers checkout}"
[[ "$(git -C "$FLATBUFFERS_DIR" rev-parse HEAD)" == 8af3053eb8400de4a0a654805b8257e93fae3a8e ]]
if [[ "${1:-}" == --linux ]]; then
  case "$(uname -m)" in
    arm64|aarch64) arch=arm64; sha=6f2977942308d91b0123978da3c6a0d6fce780994b3b020008c617e26764ea40 ;;
    x86_64|amd64) arch=x86_64; sha=0507679dff16814b74516cd969a9b16d2ced1347388024bc7966264648c78bfb ;;
    *) echo 'Unsupported build architecture' >&2; exit 1 ;;
  esac
  cache="${XDG_CACHE_HOME:-$HOME/.cache}/flatsql"
  tgz="$cache/wasi-sdk-30.0-$arch-linux.tar.gz"
  mkdir -p "$cache"
  if [[ ! -f "$tgz" ]]; then
    curl -fSL -o "$tgz.part" "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-30/wasi-sdk-30.0-$arch-linux.tar.gz"
    mv "$tgz.part" "$tgz"
  fi
  [[ "$(shasum -a 256 "$tgz" | cut -d' ' -f1)" == "$sha" ]]
  docker run --rm \
    -v "$root/cpp":/w/flatsql/cpp:ro \
    -v "$root/node_modules/spacedatastandards.org/schema":/w/sds:ro \
    -v "$FLATBUFFERS_DIR":/w/flatbuffers:ro \
    -v "$tgz":/w/wasi-sdk.tar.gz:ro \
    -v "$root/wasm":/w/out \
    ubuntu:22.04 bash -euc '
      apt-get update -qq >/dev/null && apt-get install -y -qq cmake ninja-build g++ >/dev/null
      mkdir -p /opt/wasi-sdk
      tar -xzf /w/wasi-sdk.tar.gz -C /opt/wasi-sdk --strip-components=1
      cmake -S /w/flatsql/cpp -B /tmp/b -G Ninja \
        -DCMAKE_TOOLCHAIN_FILE=/w/flatsql/cpp/cmake/wasi-threads-toolchain.cmake \
        -DWASI_SDK_PREFIX=/opt/wasi-sdk -DFLATBUFFERS_DIR=/w/flatbuffers -DSDS_SCHEMA_DIR=/w/sds -DCMAKE_BUILD_TYPE=Release >/dev/null
      cmake --build /tmp/b --target flatsql_p4_threads -j 6
      cp /tmp/b/flatsql.wasm /w/out/flatsql.wasm'
elif [[ $# == 0 ]]; then
  : "${WASI_SDK_PATH:?Set WASI_SDK_PATH to wasi-sdk 30.0, or use --linux}"
  [[ "$(head -1 "$WASI_SDK_PATH/VERSION")" == 30.0 ]]
  cmake -S cpp -B cpp/build-unified-wasm \
    -DCMAKE_TOOLCHAIN_FILE="$root/cpp/cmake/wasi-threads-toolchain.cmake" \
    -DWASI_SDK_PREFIX="$WASI_SDK_PATH" -DCMAKE_BUILD_TYPE=Release
  cmake --build cpp/build-unified-wasm --target flatsql_p4_threads --parallel "${FLATSQL_BUILD_JOBS:-6}"
  cp cpp/build-unified-wasm/flatsql.wasm wasm/flatsql.wasm
else
  echo 'usage: bash scripts/build-wasm.sh [--linux]' >&2; exit 2
fi
node scripts/check-wasm-imports.mjs
node scripts/write-integrity.mjs
