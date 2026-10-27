# Short end-to-end CI

Push and pull-request CI runs the same command used locally:

```sh
FLATBUFFERS_DIR=/path/to/flatbuffers bash scripts/ci-e2e.sh
```

Use the FlatBuffers commit `8af3053eb8400de4a0a654805b8257e93fae3a8e`
and install the lockfile dependencies first. Node 24, CMake, and a C++ compiler
are required. Each GitHub job has a ten-minute ceiling, including its build.
Both Linux and macOS run the real engine in Release mode.

The gate verifies artifact integrity/imports, then exercises binary ingest and
SQL/geospatial queries through the package API; durable ingest, teardown,
reopen, reindex and bounded rebuild through the shipped WASM engines; native
SQL against a real store; and forced compaction while a reader verifies its
storage snapshot. No unit suite, benchmark, or randomized stress campaign runs
in push-time CI. Native filters must execute exactly one E2E each.

The runner prints per-check timings and writes a Markdown summary and full logs.
Set `FLATSQL_CI_RESULTS` to choose their directory. GitHub publishes the summary
and uploads those logs even when a check fails. `--wasm-only` runs the package
flows without a native build and is also the release-package gate.

Long durability campaigns remain available explicitly through
`flatsql_p4_test --test=t_kill --rounds=300`,
`flatsql_p4_fault_test --test=t_power_loss --slow=1 --rounds=200`, and
`node scripts/p4-wasm-suite.mjs --kill-rounds 100`. They are separate stress
investigations, not evidence that the short E2E gate ran them.

The superseded CI run 40436861568 passed native 300-crash/200-power-loss coverage
but its randomized WASM campaign reported a host-process crash at round 71.
The old runner retained only a stack-address tail and deleted the diagnostic
logs. That campaign is not recorded as passing or explained by this CI change.
