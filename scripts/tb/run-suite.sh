#!/usr/bin/env bash
# The terabyte harness suite on one machine: the unit gates, the metadata-only
# tier, the count-scaled tier and the real-record engine tier, in that order.
#
#   scripts/tb/run-suite.sh <flatsql_ps_test> <work dir> <corpus dir> [stage...]
#
# <corpus dir> holds corpus.tbc and types/ (scripts/tb/extract-corpus.py).
# Stages: gates meta count real (default: all four). Everything is written
# under <work dir>: logs/, results/ (CSVs), stores/ (deleted after each tier).
#
# Disk: a watchdog checks the file system's available bytes every 60 s and
# kills the running test below TB_FLOOR_GB (default 20), then removes its
# store; each stage starts only above the floor plus TB_STAGE_GB (default 10).
# The tiers check the same floor themselves every sample.
#
# Knobs (environment): TB_PARTITIONS (10000), TB_WRITERS (6), TB_GEN_THREADS
# (8), TB_STEP_GB (10), TB_COUNT_MAX_GB (50), TB_REAL_MAX_GB (30),
# TB_META_TB (0.1,1,3,10), TB_META_WARM_MAX_GB (6), TB_GATE_DEADLINE (1800),
# TB_COUNT_MAX_S (14400), TB_REAL_MAX_S (7200).
set -u
BIN=$(cd "$(dirname "${1:?binary}")" && pwd)/$(basename "$1")
WORK=${2:?work dir}
CORPUS=${3:?corpus dir}
shift 3
STAGES=${*:-gates meta count real}
HERE=$(cd "$(dirname "$0")" && pwd)
FLOOR_GB=${TB_FLOOR_GB:-20}
STAGE_GB=${TB_STAGE_GB:-10}
P=${TB_PARTITIONS:-10000}
W=${TB_WRITERS:-6}
G=${TB_GEN_THREADS:-8}
mkdir -p "$WORK/logs" "$WORK/results" "$WORK/stores"
ulimit -n 1048576 2>/dev/null || ulimit -n "$(ulimit -Hn)"

avail_gb() { df -B1 --output=avail "$WORK" 2>/dev/null | tail -1 | awk '{printf "%d", $1/1e9}' || echo 0; }
say() { echo "[$(date -u +%FT%TZ)] $*" | tee -a "$WORK/logs/suite.log"; }

say "suite start: $(uname -srm), $(nproc) cpus, $(free -g | awk '/Mem:/{print $2}') GB RAM, fds $(ulimit -n), $(avail_gb) GB available, floor ${FLOOR_GB} GB; stages: $STAGES"

# Watchdog: below the floor, kill the test and remove the stores.
(
  while sleep 60; do
    a=$(avail_gb)
    echo "[$(date -u +%FT%TZ)] watchdog: ${a} GB available" >> "$WORK/logs/watchdog.log"
    if [ "$a" -lt "$FLOOR_GB" ]; then
      echo "[$(date -u +%FT%TZ)] watchdog: below the ${FLOOR_GB} GB floor: killing flatsql_ps_test" >> "$WORK/logs/watchdog.log"
      pkill -KILL -f "$BIN" || true
      rm -rf "$WORK/stores/count" "$WORK/stores/real" "$WORK/stores/meta"
    fi
  done
) &
WATCHDOG=$!
trap 'kill $WATCHDOG 2>/dev/null' EXIT

stage_ok() {
  local a
  a=$(avail_gb)
  if [ "$a" -lt $((FLOOR_GB + STAGE_GB)) ]; then
    say "stage $1 skipped: ${a} GB available (floor ${FLOOR_GB} + ${STAGE_GB})"
    return 1
  fi
  say "stage $1 start: ${a} GB available, load $(cut -d' ' -f1-3 /proc/loadavg)"
}

for s in $STAGES; do
  case $s in
    gates)
      stage_ok gates || continue
      "$HERE/run-gates.sh" "$BIN" "$WORK/logs/gates" "${TB_GATE_DEADLINE:-1800}" > "$WORK/results/gates.txt" 2>&1
      say "gates: $(tail -1 "$WORK/results/gates.txt")"
      ;;
    meta)
      stage_ok meta || continue
      "$BIN" --test=tb_meta_tier --tb-dir="$WORK/stores/meta" --tb-csv="$WORK/results/meta" \
        --tb-types-dir="$CORPUS/types" --tb-corpus="$CORPUS/corpus.tbc" --tb-partitions="$P" --tb-writers="$W" \
        --tb-meta-tb="${TB_META_TB:-0.1,1,3,10}" --tb-meta-warm-max-gb="${TB_META_WARM_MAX_GB:-6}" \
        --tb-min-free-gb="$FLOOR_GB" > "$WORK/logs/meta.log" 2>&1
      say "meta: exit $? ($(grep -c '^  META' "$WORK/logs/meta.log") META lines)"
      rm -rf "$WORK/stores/meta"
      ;;
    count)
      stage_ok count || continue
      "$BIN" --test=tb_engine_tier --tb-mode=count --tb-dir="$WORK/stores/count" --tb-csv="$WORK/results/count" \
        --tb-types-dir="$CORPUS/types" --tb-partitions="$P" --tb-types=30 --tb-writers="$W" --tb-gen-threads="$G" \
        --tb-step-gb="${TB_STEP_GB:-10}" --tb-max-gb="${TB_COUNT_MAX_GB:-50}" --tb-min-free-gb="$FLOOR_GB" \
        --tb-max-used-pct=100 --tb-max-seconds="${TB_COUNT_MAX_S:-14400}" > "$WORK/logs/count.log" 2>&1
      say "count: exit $?; $(grep -E 'stopping:' "$WORK/logs/count.log" | tail -1)"
      rm -rf "$WORK/stores/count"
      ;;
    real)
      stage_ok real || continue
      "$BIN" --test=tb_engine_tier --tb-mode=real --tb-dir="$WORK/stores/real" --tb-csv="$WORK/results/real" \
        --tb-types-dir="$CORPUS/types" --tb-corpus="$CORPUS/corpus.tbc" --tb-partitions="$P" --tb-types=30 \
        --tb-writers="$W" --tb-gen-threads="$G" --tb-step-gb="${TB_STEP_GB:-10}" --tb-max-gb="${TB_REAL_MAX_GB:-30}" \
        --tb-min-free-gb="$FLOOR_GB" --tb-max-used-pct=100 --tb-max-seconds="${TB_REAL_MAX_S:-7200}" \
        > "$WORK/logs/real.log" 2>&1
      say "real: exit $?; $(grep -E 'stopping:' "$WORK/logs/real.log" | tail -1)"
      rm -rf "$WORK/stores/real"
      ;;
    *) say "unknown stage $s" ;;
  esac
done
say "suite done: $(avail_gb) GB available"
