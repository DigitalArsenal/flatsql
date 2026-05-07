#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

COMMAND="smoke"
if [[ $# -gt 0 ]]; then
  COMMAND="$1"
  shift
fi

RUN_ID="${RUN_ID:-sds-$(date -u +%Y%m%dT%H%M%SZ)}"
SDS_SCHEMA_ROOT="${SDS_SCHEMA_ROOT:-$PROJECT_ROOT/../spacedatastandards.org/schema}"
if [[ -n "${OUTPUT_DIR:-}" ]]; then
  OUTPUT_DIR_EXPLICIT=1
else
  OUTPUT_DIR_EXPLICIT=0
  OUTPUT_DIR="$PROJECT_ROOT/stress/results/$RUN_ID"
fi

EXTRA_RUNNER_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --schema-root)
      SDS_SCHEMA_ROOT="$2"
      shift 2
      ;;
    --schema-root=*)
      SDS_SCHEMA_ROOT="${1#*=}"
      shift
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      OUTPUT_DIR_EXPLICIT=1
      shift 2
      ;;
    --output-dir=*)
      OUTPUT_DIR="${1#*=}"
      OUTPUT_DIR_EXPLICIT=1
      shift
      ;;
    --nodes)
      NODE_COUNT="$2"
      shift 2
      ;;
    --nodes=*)
      NODE_COUNT="${1#*=}"
      shift
      ;;
    --node-id-offset)
      NODE_ID_OFFSET="$2"
      shift 2
      ;;
    --node-id-offset=*)
      NODE_ID_OFFSET="${1#*=}"
      shift
      ;;
    --storage-gb)
      NODE_STORAGE_GB="$2"
      shift 2
      ;;
    --storage-gb=*)
      NODE_STORAGE_GB="${1#*=}"
      shift
      ;;
    --records-per-node)
      RECORDS_PER_NODE="$2"
      shift 2
      ;;
    --records-per-node=*)
      RECORDS_PER_NODE="${1#*=}"
      shift
      ;;
    --batch-bytes)
      BATCH_BYTES="$2"
      shift 2
      ;;
    --batch-bytes=*)
      BATCH_BYTES="${1#*=}"
      shift
      ;;
    --query-concurrency)
      QUERY_CONCURRENCY="$2"
      shift 2
      ;;
    --query-concurrency=*)
      QUERY_CONCURRENCY="${1#*=}"
      shift
      ;;
    --hot-query-ratio)
      HOT_QUERY_RATIO="$2"
      shift 2
      ;;
    --hot-query-ratio=*)
      HOT_QUERY_RATIO="${1#*=}"
      shift
      ;;
    --runtime)
      RUNTIME="$2"
      shift 2
      ;;
    --runtime=*)
      RUNTIME="${1#*=}"
      shift
      ;;
    --node-isolation)
      NODE_ISOLATION="$2"
      shift 2
      ;;
    --node-isolation=*)
      NODE_ISOLATION="${1#*=}"
      shift
      ;;
    --run-id)
      RUN_ID="$2"
      shift 2
      ;;
    --run-id=*)
      RUN_ID="${1#*=}"
      shift
      ;;
    *)
      EXTRA_RUNNER_ARGS+=("$1")
      shift
      ;;
  esac
done
if [[ "$OUTPUT_DIR_EXPLICIT" -eq 0 ]]; then
  OUTPUT_DIR="$PROJECT_ROOT/stress/results/$RUN_ID"
fi
if [[ "${#EXTRA_RUNNER_ARGS[@]}" -gt 0 ]]; then
  set -- "${EXTRA_RUNNER_ARGS[@]}"
else
  set --
fi

run_local_smoke() {
  local runner_args=(
    --mode "${STRESS_MODE:-smoke}"
    --schema-root "$SDS_SCHEMA_ROOT"
    --output-dir "$OUTPUT_DIR"
    --nodes "${NODE_COUNT:-2}"
    --storage-gb "${NODE_STORAGE_GB:-0.01}"
    --records-per-node "${RECORDS_PER_NODE:-25}"
    --run-id "$RUN_ID"
  )
  if [[ -n "${NODE_ID_OFFSET:-}" ]]; then runner_args+=(--node-id-offset "$NODE_ID_OFFSET"); fi
  if [[ -n "${BATCH_BYTES:-}" ]]; then runner_args+=(--batch-bytes "$BATCH_BYTES"); fi
  if [[ -n "${QUERY_CONCURRENCY:-}" ]]; then runner_args+=(--query-concurrency "$QUERY_CONCURRENCY"); fi
  if [[ -n "${HOT_QUERY_RATIO:-}" ]]; then runner_args+=(--hot-query-ratio "$HOT_QUERY_RATIO"); fi
  if [[ -n "${RUNTIME:-}" ]]; then runner_args+=(--runtime "$RUNTIME"); fi
  if [[ -n "${NODE_ISOLATION:-}" ]]; then runner_args+=(--node-isolation "$NODE_ISOLATION"); fi
  if [[ "$#" -gt 0 ]]; then runner_args+=("$@"); fi

  npm run build
  node "$PROJECT_ROOT/stress/sds/run.mjs" "${runner_args[@]}"
}

run_docker() {
  docker info >/dev/null
  local schema_root_abs
  schema_root_abs="$(cd "$SDS_SCHEMA_ROOT" && pwd)"
  mkdir -p "$OUTPUT_DIR"
  local output_dir_abs
  output_dir_abs="$(cd "$OUTPUT_DIR" && pwd)"
  SDS_SCHEMA_ROOT_HOST="$schema_root_abs" \
  OUTPUT_DIR_HOST="$output_dir_abs" \
  RUN_ID="$RUN_ID" \
  STRESS_MODE="${STRESS_MODE:-smoke}" \
  NODE_COUNT="${NODE_COUNT:-2}" \
  NODE_STORAGE_GB="${NODE_STORAGE_GB:-0.01}" \
  RECORDS_PER_NODE="${RECORDS_PER_NODE:-25}" \
  BATCH_BYTES="${BATCH_BYTES:-1048576}" \
  QUERY_CONCURRENCY="${QUERY_CONCURRENCY:-4}" \
  HOT_QUERY_RATIO="${HOT_QUERY_RATIO:-0.9}" \
  RUNTIME="${RUNTIME:-standalone}" \
  NODE_ISOLATION="${NODE_ISOLATION:-logical}" \
  docker compose -f "$PROJECT_ROOT/stress/docker/docker-compose.yml" up \
    --build \
    --abort-on-container-exit \
    --exit-code-from controller \
    controller
}

prepare_container_fleet_compose() {
  npm run build

  local schema_root_abs
  schema_root_abs="$(cd "$SDS_SCHEMA_ROOT" && pwd)"
  mkdir -p "$OUTPUT_DIR"
  local output_dir_abs
  output_dir_abs="$(cd "$OUTPUT_DIR" && pwd)"
  FLEET_COMPOSE_FILE="$output_dir_abs/docker-compose.full.yml"

  node "$PROJECT_ROOT/stress/sds/write-compose.mjs" \
    --output-file "$FLEET_COMPOSE_FILE" \
    --project-root "$PROJECT_ROOT" \
    --schema-root-host "$schema_root_abs" \
    --output-dir-host "$output_dir_abs" \
    --run-id "$RUN_ID" \
    --mode "${STRESS_MODE:-smoke}" \
    --nodes "${NODE_COUNT:-2}" \
    --storage-gb "${NODE_STORAGE_GB:-0.01}" \
    --records-per-node "${RECORDS_PER_NODE:-25}" \
    --batch-bytes "${BATCH_BYTES:-1048576}" \
    --query-concurrency "${QUERY_CONCURRENCY:-4}" \
    --hot-query-ratio "${HOT_QUERY_RATIO:-0.9}" \
    --runtime "${RUNTIME:-standalone}"
}

run_container_fleet() {
  prepare_container_fleet_compose
  docker info >/dev/null

  local node_count="${NODE_COUNT:-2}"
  local worker_services=()
  local node_id
  for ((node_id = 0; node_id < node_count; node_id++)); do
    worker_services+=("worker-$node_id")
  done

  docker compose -f "$FLEET_COMPOSE_FILE" up --build "${worker_services[@]}"
  docker compose -f "$FLEET_COMPOSE_FILE" build aggregator
  docker compose -f "$FLEET_COMPOSE_FILE" run --no-deps --rm aggregator
}

run_full() {
  if [[ "${CONFIRM_FULL_STRESS:-}" != "1" ]]; then
    echo "Full stress requires CONFIRM_FULL_STRESS=1 because it targets 100 nodes and 1 GB/node." >&2
    exit 2
  fi

  local parent
  parent="$(dirname "$OUTPUT_DIR")"
  mkdir -p "$parent"
  local available_kb
  available_kb="$(df -Pk "$parent" | awk 'NR == 2 { print $4 }')"
  local required_kb=$((110 * 1024 * 1024))
  if [[ "$available_kb" -lt "$required_kb" ]]; then
    echo "Full stress requires at least 110 GB free near $parent; available KB: $available_kb." >&2
    exit 3
  fi

  RECORDS_PER_NODE="${RECORDS_PER_NODE:-20000}"
  QUERY_CONCURRENCY="${QUERY_CONCURRENCY:-64}"
  HOT_QUERY_RATIO="${HOT_QUERY_RATIO:-0.01}"
  STRESS_MODE=full NODE_COUNT=100 NODE_STORAGE_GB=1 NODE_ISOLATION=container run_container_fleet
}

case "$COMMAND" in
  smoke)
    run_local_smoke "$@"
    ;;
  docker-smoke)
    run_docker "$@"
    ;;
  docker-fleet)
    STRESS_MODE="${STRESS_MODE:-smoke}" NODE_ISOLATION=container run_container_fleet "$@"
    ;;
  full-config)
    RECORDS_PER_NODE="${RECORDS_PER_NODE:-20000}" \
      QUERY_CONCURRENCY="${QUERY_CONCURRENCY:-64}" \
      HOT_QUERY_RATIO="${HOT_QUERY_RATIO:-0.01}" \
      STRESS_MODE=full \
      NODE_COUNT="${NODE_COUNT:-100}" \
      NODE_STORAGE_GB="${NODE_STORAGE_GB:-1}" \
      NODE_ISOLATION=container \
      prepare_container_fleet_compose
    ;;
  full)
    run_full "$@"
    ;;
  *)
    echo "Usage: scripts/stress-docker.sh [smoke|docker-smoke|docker-fleet|full-config|full] [runner args]" >&2
    exit 2
    ;;
esac
