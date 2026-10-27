# Short end-to-end CI

`bash scripts/ci-e2e.sh` is the default CI gate. It builds the runtime/demo,
checks public types, packages the exact npm tarball, and runs the installed
binary through the public API and CLI. External flatc-wasm prepares canonical
SDS GJN inputs and reads SDS QRP results. The flow covers geo predicates and
binary geometry, original record preservation, typed parameters, failures,
request budgets, cancellation, empty-schema reopen and durable record reopen.

The native portion runs three bounded real-engine flows: GJN query/reopen,
SQL surface/persistence, and compaction with concurrent reading. It does not
run Jest, CTest unit suites, or long stress jobs. The CI jobs have a ten-minute
hard timeout, with results/logs attached and a GitHub step summary. Normal
builds are expected to take minutes, not hours.

```sh
FLATBUFFERS_DIR=/path/to/pinned-flatbuffers bash scripts/ci-e2e.sh
bash scripts/ci-e2e.sh --wasm-only
npm run test:browser
```

FlatBuffers is pinned to `8af3053eb8400de4a0a654805b8257e93fae3a8e`.
The browser E2E uses Chromium headless shell and the identical packaged WASM.
It tests Pages-style isolation, binary ingest, spatial filtering/projection,
persistent reopen/reload, clear, mobile layout, and the homepage iframe.
`GAUNTLET_HEADLESS_SHELL` overrides the installed shell path;
`FLATSQL_BROWSER_RESULTS` selects the screenshot directory.

Release CI builds one artifact using wasi-sdk 30, verifies that the rebuilt
bytes match the committed WASM/integrity manifest, runs the same short E2Es,
and publishes through npm OIDC. A release tag must match the package version.
Generated browser/runtime assets must also match committed files.

Long replay/fault/scale harnesses are manual engineering diagnostics. Passing
the short gate does not assert that every long stress scenario has passed.
