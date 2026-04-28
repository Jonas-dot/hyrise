#!/usr/bin/env bash
# =============================================================================
# run_naive_sched.sh  –  Scheduler-enabled naive DVI transaction benchmark (T1-T6)
#
# Runs hyriseBenchmarkDVIndexNaiveSched – same dead-row accumulation workload
# as run_naive.sh but with NodeQueueScheduler active for intra-operator
# chunk-level parallelism.
#
# Experiments mirror the naive benchmark:
#   T1 workload, T2 mechanism, T3 violation, T4 duplicate rate,
#   T5 threads, T6 access pattern.
#
# Dead rows are never cleaned up (no pooling, no compaction) -- degradation is
# expected and intentional.
#
# Output:
#   results/naive_txn_sched/<TIMESTAMP>/results.csv
#   results/naive_txn_sched/<TIMESTAMP>/meta.txt
#   results/naive_txn_sched/<TIMESTAMP>/run.log
#
# Usage:
#   ./scripts/txn/run_naive_sched.sh --experiment T5 [--build] [--dry-run]
#   ./scripts/txn/run_naive_sched.sh --experiment all [--binary PATH] [--outdir DIR]
#                                    [--iterations N] [--scheduler-workers N]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
HYRISE_ROOT="$(cd "${BENCH_ROOT}/.." && pwd)"

source "${SCRIPT_DIR}/../common/config.sh"

# ---------------------------------------------------------------------------
# Defaults / argument parsing
# ---------------------------------------------------------------------------
BINARY="${HYRISE_ROOT}/build_release/hyriseBenchmarkDVIndexNaiveSched"
EXPERIMENT=""
DRY_RUN=false
DO_BUILD=false
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_BASE="${BENCH_ROOT}/results/naive_txn_sched/${TIMESTAMP}"
ITERATIONS="${TXN_ITERATIONS}"
SCHEDULER_WORKERS=0   # 0 = hardware_concurrency

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      DO_BUILD=true
      shift
      ;;
    --force-reconfigure)
      FORCE_RECONFIGURE=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --experiment=*)
      EXPERIMENT="${1#*=}"
      shift
      ;;
    --binary=*)
      BINARY="${1#*=}"
      shift
      ;;
    --outdir=*)
      RESULT_BASE="${1#*=}"
      shift
      ;;
    --iterations=*)
      ITERATIONS="${1#*=}"
      shift
      ;;
    --scheduler-workers=*)
      SCHEDULER_WORKERS="${1#*=}"
      shift
      ;;
    --experiment)
      EXPERIMENT="$2"
      shift 2
      ;;
    --binary)
      BINARY="$2"
      shift 2
      ;;
    --outdir)
      RESULT_BASE="$2"
      shift 2
      ;;
    --iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    --scheduler-workers)
      SCHEDULER_WORKERS="$2"
      shift 2
      ;;
    *)
      echo "WARNING: Unknown argument: $1"
      shift
      ;;
  esac
done

if [[ -z "${EXPERIMENT}" ]]; then
  echo "ERROR: --experiment is required (T1|T2|T3|T4|T5|T6|all)"
  exit 1
fi

# ---------------------------------------------------------------------------
# Optional build step
# ---------------------------------------------------------------------------
if [[ "${DO_BUILD}" == "true" ]]; then
  source "${SCRIPT_DIR}/../common/build.sh"
  build_naive_sched
fi

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
RESULT_CSV="${RESULT_BASE}/results.csv"
META_FILE="${RESULT_BASE}/meta.txt"
LOG_FILE="${RESULT_BASE}/run.log"

# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------
if [[ "${DRY_RUN}" == "false" ]] && [[ ! -x "${BINARY}" ]]; then
  echo "ERROR: Benchmark binary not found: ${BINARY}"
  echo "       Build with: ./scripts/txn/run_naive_sched.sh --build"
  echo "       Or manually: ninja -C ${HYRISE_ROOT}/build_release hyriseBenchmarkDVIndexNaiveSched"
  exit 1
fi

# ---------------------------------------------------------------------------
# Setup output directory and log
# ---------------------------------------------------------------------------
if [[ "${DRY_RUN}" == "false" ]]; then
  mkdir -p "${RESULT_BASE}"
  {
    echo "benchmark=naive_txn_sched"
    echo "experiment=${EXPERIMENT}"
    echo "timestamp=${TIMESTAMP}"
    echo "git_hash=$(cd "${HYRISE_ROOT}" && git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"
    echo "host=$(hostname)"
    echo "os=$(uname -srm)"
    echo "binary=${BINARY}"
    echo "workloads=${TXN_WORKLOADS_ALL}"
    echo "mechanisms=${TXN_MECHANISMS_ALL}"
    echo "violation_rates=${TXN_VIOLATION_RATES}"
    echo "duplicate_rates=${TXN_DUPLICATE_RATES}"
    echo "size=${TXN_SIZES}"
    echo "batch_size=${TXN_CHECKPOINT_INTERVAL}"
    echo "threads=${TXN_THREADS}"
    echo "access_patterns=${TXN_ACCESS_PATTERNS}"
    echo "iterations=${ITERATIONS}"
    echo "scheduler_workers=${SCHEDULER_WORKERS}"
    echo "canonical_workload=${CANONICAL_WORKLOAD}"
    echo "canonical_mechanism=${CANONICAL_MECHANISM}"
    echo "canonical_violation_rate=${CANONICAL_VIOLATION_RATE}"
    echo "canonical_duplicate_rate=${CANONICAL_DUPLICATE_RATE}"
    echo "canonical_size=${CANONICAL_SIZE}"
    echo "canonical_access_pattern=${CANONICAL_ACCESS_PATTERN}"
    echo "canonical_threads=${CANONICAL_THREADS}"
  } > "${META_FILE}"
  exec > >(tee -a "${LOG_FILE}") 2>&1
fi

# ---------------------------------------------------------------------------
# Runner: one invocation per experiment cell
# ---------------------------------------------------------------------------
run_cell() {
  local exp_id="$1"
  local workload="$2"
  local mechanism="$3"
  local violation_rate="$4"
  local dup_rate="$5"
  local access_pattern="$6"
  local threads="$7"
  local size="$8"
  local batch_size="$9"

  local tag="${workload}_${mechanism}_v${violation_rate}_d${dup_rate}_${access_pattern}_t${threads}_s${size}"
  echo "  [${exp_id}] ${tag}"

  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "  [DRY-RUN] ${BINARY} --experiment ${exp_id} --workload ${workload} ..."
    return
  fi

  ${NUMA_PREFIX} "${BINARY}" \
    --experiment        "${exp_id}" \
    --workload          "${workload}" \
    --mechanism         "${mechanism}" \
    --violation-rate    "${violation_rate}" \
    --duplicate-rate    "${dup_rate}" \
    --access-pattern    "${access_pattern}" \
    --size              "${size}" \
    --batch-size        "${batch_size}" \
    --threads           "${threads}" \
    --scheduler-workers "${SCHEDULER_WORKERS}" \
    --iterations        "${ITERATIONS}" \
    --output-csv        "${RESULT_CSV}" \
  || true   # do not abort the sweep on a single failure
}

# ---------------------------------------------------------------------------
# Experiment definitions (same T1-T6 structure as run_naive.sh)
# ---------------------------------------------------------------------------
run_T1() {
  IFS=',' read -ra workloads <<< "${TXN_WORKLOADS_ALL}"
  for w in "${workloads[@]}"; do
    run_cell T1 "${w}" "${CANONICAL_MECHANISM}" \
      "${CANONICAL_VIOLATION_RATE}" "${CANONICAL_DUPLICATE_RATE}" \
      "${CANONICAL_ACCESS_PATTERN}" "${CANONICAL_THREADS}" \
      "${CANONICAL_SIZE}" "${TXN_CHECKPOINT_INTERVAL}"
  done
}

run_T2() {
  IFS=',' read -ra mechs <<< "${TXN_MECHANISMS_ALL}"
  for m in "${mechs[@]}"; do
    run_cell T2 "${CANONICAL_WORKLOAD}" "${m}" \
      "${CANONICAL_VIOLATION_RATE}" "${CANONICAL_DUPLICATE_RATE}" \
      "${CANONICAL_ACCESS_PATTERN}" "${CANONICAL_THREADS}" \
      "${CANONICAL_SIZE}" "${TXN_CHECKPOINT_INTERVAL}"
  done
}

run_T3() {
  IFS=',' read -ra rates <<< "${TXN_VIOLATION_RATES}"
  for r in "${rates[@]}"; do
    run_cell T3 "${CANONICAL_WORKLOAD}" "${CANONICAL_MECHANISM}" \
      "${r}" "${CANONICAL_DUPLICATE_RATE}" \
      "${CANONICAL_ACCESS_PATTERN}" "${CANONICAL_THREADS}" \
      "${CANONICAL_SIZE}" "${TXN_CHECKPOINT_INTERVAL}"
  done
}

run_T4() {
  IFS=',' read -ra rates <<< "${TXN_DUPLICATE_RATES}"
  for d in "${rates[@]}"; do
    run_cell T4 "${CANONICAL_WORKLOAD}" "${CANONICAL_MECHANISM}" \
      "${CANONICAL_VIOLATION_RATE}" "${d}" \
      "${CANONICAL_ACCESS_PATTERN}" "${CANONICAL_THREADS}" \
      "${CANONICAL_SIZE}" "${TXN_CHECKPOINT_INTERVAL}"
  done
}

run_T5() {
  IFS=',' read -ra thread_counts <<< "${TXN_THREADS}"
  for t in "${thread_counts[@]}"; do
    run_cell T5 "${CANONICAL_WORKLOAD}" "${CANONICAL_MECHANISM}" \
      "${CANONICAL_VIOLATION_RATE}" "${CANONICAL_DUPLICATE_RATE}" \
      "${CANONICAL_ACCESS_PATTERN}" "${t}" \
      "${CANONICAL_SIZE}" "${TXN_CHECKPOINT_INTERVAL}"
  done
}

run_T6() {
  IFS=',' read -ra patterns <<< "${TXN_ACCESS_PATTERNS}"
  for p in "${patterns[@]}"; do
    run_cell T6 "${CANONICAL_WORKLOAD}" "${CANONICAL_MECHANISM}" \
      "${CANONICAL_VIOLATION_RATE}" "${CANONICAL_DUPLICATE_RATE}" \
      "${p}" "${CANONICAL_THREADS}" \
      "${CANONICAL_SIZE}" "${TXN_CHECKPOINT_INTERVAL}"
  done
}

echo "======================================================================"
echo "  Naive-Sched DVI transaction benchmark (T1-T6)"
echo "  Binary            : ${BINARY}"
echo "  Experiment        : ${EXPERIMENT}"
echo "  Iterations        : ${ITERATIONS}"
echo "  Scheduler workers : ${SCHEDULER_WORKERS} (0=hardware_concurrency)"
echo "  Output            : ${RESULT_BASE}/"
echo "======================================================================"

if [[ "${EXPERIMENT}" == "all" ]]; then
  run_T1; run_T2; run_T3; run_T4; run_T5; run_T6
else
  "run_${EXPERIMENT}"
fi

# Update latest symlink
if [[ "${DRY_RUN}" == "false" ]]; then
  mkdir -p "${BENCH_ROOT}/results/naive_txn_sched"
  ln -sfn "${TIMESTAMP}" "${BENCH_ROOT}/results/naive_txn_sched/latest"
fi

echo ""
echo "======================================================================"
echo "  Done."
echo "  Results : ${RESULT_CSV}"
echo "  Meta    : ${META_FILE}"
echo "  Log     : ${LOG_FILE}"
echo "======================================================================"
