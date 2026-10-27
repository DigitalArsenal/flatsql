# FlatSQL native engine

FlatSQL 3.12 has one public C ABI: `include/flatsql/flatsql.h`.
Build the `flatsql` static library with CMake and the pinned DigitalArsenal
FlatBuffers checkout at `8af3053eb8400de4a0a654805b8257e93fae3a8e`.

```sh
cmake -S cpp -B cpp/build -DFLATBUFFERS_DIR=/path/to/flatbuffers -DCMAKE_BUILD_TYPE=Release
cmake --build cpp/build --target flatsql
```

The same engine builds as `wasm/flatsql.wasm` using wasi-sdk 30.
Native hosts provide the seven filesystem imports described in the ABI header.
Public SQL requests/results use SDS QRP; records are size-prefixed FlatBuffers.
See `docs/API-3.12.md` for the binary and geospatial contracts.

The private source directory names `p4` and `p4sql` identify the storage
implementation. They are not alternate engines or public entry points.
