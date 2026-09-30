#!/usr/bin/env bash
# The four terabyte unit gates (audit §4.5), each in its own process under a
# deadline, classified against the status the engine as built is expected to
# have (tb_unit_gates_test.cpp, TB_GATE):
#
#   PASS   the gate's checks held
#   FAIL   a check failed (the test exited non-zero)
#   HANG   a bounded wait inside the gate expired (HANG line), or the deadline
#          killed the process
#   TRAP   the process died on a signal (abort, segfault, wasm trap)
#
#   scripts/tb/run-gates.sh <flatsql_ps_test> <log dir> [deadline seconds] [extra args...]
#
# Prints one line per gate and a summary: XFAIL (failed as expected), XPASS
# (passed although expected to fail: clear its mark), PASS, UNEXPECTED.
set -u
BIN=${1:?flatsql_ps_test binary}
LOGDIR=${2:?log directory}
DEADLINE=${3:-3600}
shift 3 2>/dev/null || shift $#
mkdir -p "$LOGDIR"

run_deadline() {  # seconds, command...
  local secs=$1; shift
  if command -v timeout >/dev/null 2>&1; then
    timeout --signal=KILL "$secs" "$@"
  else
    perl -e 'alarm shift; exec @ARGV or die "exec: $!"' "$secs" "$@"
  fi
}

# gate test name, expected status on the engine as built
GATES=(
  "tbgate_B1_129_partitions_one_type PASS"
  "tbgate_B2_20k_partitions_since_boot FAIL"
  "tbgate_M1_100k_registrations_with_drops FAIL"
  "tbgate_B4_5k_segments_one_partition FAIL"
)

echo "# $(uname -srm), $(getconf _NPROCESSORS_ONLN 2>/dev/null) hardware threads, load $(uptime | sed 's/.*load averages*: //')"
echo "# binary $BIN; deadline ${DEADLINE}s per gate"
summary=()
for entry in "${GATES[@]}"; do
  name=${entry% *}
  expect=${entry#* }
  # TB_GATE_FILTER (a grep -E pattern) runs a subset, e.g. "B1|B2|M1".
  if [ -n "${TB_GATE_FILTER:-}" ] && ! echo "$name" | grep -qE "$TB_GATE_FILTER"; then continue; fi
  log="$LOGDIR/$name.log"
  start=$(date +%s)
  run_deadline "$DEADLINE" "$BIN" "--test=$name" "$@" >"$log" 2>&1
  rc=$?
  secs=$(( $(date +%s) - start ))
  if [ $rc -eq 0 ]; then
    got=PASS
  elif [ $rc -eq 137 ] || [ $rc -eq 142 ] || grep -q '^  HANG ' "$log"; then
    got=HANG
  elif [ $rc -gt 128 ]; then
    got=TRAP
  else
    got=FAIL
  fi
  if [ "$got" = PASS ] && [ "$expect" = FAIL ]; then verdict=XPASS
  elif [ "$got" = PASS ]; then verdict=PASS
  elif [ "$expect" = FAIL ]; then verdict=XFAIL
  else verdict=UNEXPECTED
  fi
  line="$name: $got (exit $rc, ${secs}s) expected $expect -> $verdict"
  echo "$line"
  grep -E '^  (FAIL|HANG|MEASURED)' "$log" | head -40 | sed 's/^/    /'
  summary+=("$verdict")
done
echo "# summary: ${summary[*]}"
