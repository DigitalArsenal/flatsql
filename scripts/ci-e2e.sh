#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

mode="${1:-}"
if [[ -n "$mode" && "$mode" != --wasm-only ]]; then
  echo 'usage: bash scripts/ci-e2e.sh [--wasm-only]' >&2
  exit 2
fi
results="${FLATSQL_CI_RESULTS:-$(mktemp -d "${TMPDIR:-/tmp}/flatsql-ci.XXXXXX")}"
mkdir -p "$results"
started=$SECONDS
printf '# FlatSQL end-to-end results\n\nCommit: `%s`\n\n| Check | Result | Seconds |\n| --- | --- | ---: |\n' "$(git rev-parse HEAD)" > "$results/summary.md"
finish() {
  local status=$?
  printf '\nExit status: %s. Total: %s seconds.\n' "$status" "$((SECONDS-started))" >> "$results/summary.md"
  cat "$results/summary.md"
  printf '\nLogs: %s\n' "$results"
}
trap finish EXIT
run() {
  local name=$1 before=$SECONDS status=0
  shift
  "$@" 2>&1 | tee "$results/$name.log" || status=$?
  printf '| %s | %s | %s |\n' "$name" "$([[ $status == 0 ]] && echo PASS || echo FAIL)" "$((SECONDS-before))" >> "$results/summary.md"
  return "$status"
}

run typescript-build npm run build
run artifact-imports npm run check:imports
run artifact-integrity npm run check:integrity
run binary-query-and-geo node scripts/ci-package-e2e.mjs
run binary-persistence node wasm/test-io-persistence.mjs

if [[ "$mode" != --wasm-only ]]; then
  : "${FLATBUFFERS_DIR:?Set FLATBUFFERS_DIR to the pinned FlatBuffers checkout}"
  [[ "$(git -C "$FLATBUFFERS_DIR" rev-parse HEAD)" == 8af3053eb8400de4a0a654805b8257e93fae3a8e ]]
  build="${FLATSQL_CI_BUILD_DIR:-cpp/build-ci-e2e}"
  run native-configure cmake -S cpp -B "$build" -DCMAKE_BUILD_TYPE=Release "-DFLATBUFFERS_DIR=$FLATBUFFERS_DIR"
  run native-build cmake --build "$build" --target flatsql_p4_test --parallel "${FLATSQL_BUILD_JOBS:-4}"
  for flow in t_sql_surface t_kill_check_compaction; do
    run "$flow" "$build/flatsql_p4_test" "--test=$flow"
    # The test harness permits an unmatched filter; an empty run is not evidence.
    grep -q '^1 tests, 0 failures$' "$results/$flow.log"
  done
fi
