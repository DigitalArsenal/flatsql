# FlatSQL SDN WASM node

This package contains the independently signed, isomorphic FlatSQL node used by
SDN flow bundles. The exact `dist/isomorphic/module.wasm` bytes target both the
browser harness and WasmEdge. Hosts route typed FSO/FSB frames and may persist
opaque canonical snapshots; database behavior remains inside this module.

The node lazily restores a digest-protected `primary/snapshot.manifest` and its
generation-addressed 1 MiB chunks through the generic opaque storage adapter.
It stages and syncs chunks, commits the manifest, then cleans the prior
generation after each successful state mutation. The namespace, keys, snapshot
format, and reload policy belong to the signed node; hosts store only
uninterpreted bytes scoped to the node instance.

Development builds are deterministic and explicitly use the development-only
publisher:

```sh
npm run build
npm test
```

Production release builds require an external Ed25519 seed and non-development
key identifier. The release guard refuses a `developmentOnly` publisher:

```sh
FLATSQL_NODE_SIGNING_SEED_HEX=<64 hex characters> \
FLATSQL_NODE_SIGNING_KEY_ID=<production key id> \
npm run release
```

The signing seed is read only from the environment and is never written to the
package. `.build/` contains generated compiler state and is excluded from Git
and npm packages.
