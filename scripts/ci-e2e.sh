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
  local result=FAIL
  if [[ $status == 0 ]]; then
    # A check that cannot apply on this machine says so on a line of its own.
    result=PASS
    if grep -q '^SKIP: ' "$results/$name.log"; then result=SKIP; fi
  fi
  printf '| %s | %s | %s |\n' "$name" "$result" "$((SECONDS-before))" >> "$results/summary.md"
  return "$status"
}

run package-build npm run build
run public-types npm run check:types
run packaged-binary-cli-persistence-and-geo npm run check:package
run pool-worker-stale-memory-view node scripts/ci-stale-memory-view-e2e.mjs
run artifact-imports npm run check:imports
run artifact-integrity npm run check:integrity

if [[ "$mode" != --wasm-only ]]; then
  : "${FLATBUFFERS_DIR:?Set FLATBUFFERS_DIR to the pinned FlatBuffers checkout}"
  [[ "$(git -C "$FLATBUFFERS_DIR" rev-parse HEAD)" == 8af3053eb8400de4a0a654805b8257e93fae3a8e ]]
  build="${FLATSQL_CI_BUILD_DIR:-cpp/build-ci-e2e}"
  run native-configure cmake -S cpp -B "$build" -DCMAKE_BUILD_TYPE=Release "-DFLATBUFFERS_DIR=$FLATBUFFERS_DIR"
  run native-build cmake --build "$build" --target flatsql_p4_test --parallel "${FLATSQL_BUILD_JOBS:-4}"
  for flow in t_gjn_binary_query_reopen t_sql_surface t_kill_check_compaction t_fts_prefix_aligned t_fts_record_envelopes t_fts_upgrade t_fts_rebuild_immediate t_fts_repair_pages t_fts_rebuild_held t_fts_repair_crash t_fts_empty_type t_fts_concurrent_readers t_fts_snapshot_ingest t_fts_snapshot_reset t_fts_version_range t_fts_rebuild_retry t_fts_summary_ingest t_fts_summary_repair_mark; do
    run "$flow" "$build/flatsql_p4_test" "--test=$flow"
    # The test harness permits an unmatched filter; an empty run is not evidence.
    grep -q '^1 tests, 0 failures$' "$results/$flow.log"
  done
fi
