#!/usr/bin/env bash
# =============================================================================
# run_m1.sh  –  M1: Ablation study – all validation mechanisms
#
# Mechanisms : off, fd_single, fd_multi, od_single, od_multi
# Fixed at   : 10M operations, 90% det-ratio, 0% violation
#              checkpointed every 100k ops
#              both access patterns, threads 1 and 16
# Iterations : 3 per cell
# Output     : results/micro/<TIMESTAMP>/results.csv
#              results/micro/<TIMESTAMP>/meta.txt
#              results/micro/<TIMESTAMP>/run.log
#
# Usage:
#   ./scripts/micro/run_m1.sh [--build] [--force-reconfigure] [--binary PATH]
#                             [--iterations N] [--dry-run]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
HYRISE_ROOT="$(cd "${BENCH_ROOT}/.." && pwd)"

source "${SCRIPT_DIR}/../common/config.sh"

# ---------------------------------------------------------------------------
# Defaults / argument parsing
# ---------------------------------------------------------------------------
BINARY="${BENCH_ROOT}/build/btree_benchmark"
ITERATIONS=${MICRO_ITERATIONS}
DRY_RUN=false
DO_BUILD=false

for arg in "$@"; do
  case "$arg" in
    --build)              DO_BUILD=true ;;
    --force-reconfigure)  FORCE_RECONFIGURE=true ;;
    --binary=*)           BINARY="${arg#*=}" ;;
    --iterations=*)       ITERATIONS="${arg#*=}" ;;
    --dry-run)            DRY_RUN=true ;;
  esac
done

# ---------------------------------------------------------------------------
# Optional build step
# ---------------------------------------------------------------------------
if [[ "${DO_BUILD}" == "true" ]]; then
  source "${SCRIPT_DIR}/../common/build.sh"
  build_micro
fi

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_DIR="${BENCH_ROOT}/results/micro/${TIMESTAMP}"
RESULT_FILE="${RESULT_DIR}/results.csv"
META_FILE="${RESULT_DIR}/meta.txt"
LOG_FILE="${RESULT_DIR}/run.log"

mkdir -p "${RESULT_DIR}"

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
if [[ "${DRY_RUN}" == "false" ]] && [[ ! -x "${BINARY}" ]]; then
  echo "ERROR: Benchmark binary not found or not executable: ${BINARY}"
  echo "       Build with: ./scripts/micro/run_m1.sh --build"
  echo "       Or manually: cd ${BENCH_ROOT}/build && cmake .. && ninja btree_benchmark"
  exit 1
fi

# ---------------------------------------------------------------------------
# Write meta.txt
# ---------------------------------------------------------------------------
if [[ "${DRY_RUN}" == "false" ]]; then
  {
    echo "experiment=M1"
    echo "timestamp=${TIMESTAMP}"
    echo "git_hash=$(cd "${BENCH_ROOT}" && git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"
    echo "host=$(hostname)"
    echo "os=$(uname -srm)"
    echo "cxx_compiler=$(${CMAKE_CXX_COMPILER} --version 2>&1 | head -1 || echo 'unknown')"
    echo "binary=${BINARY}"
    echo "mechanisms=${MICRO_M1_MECHANISMS}"
    echo "operations=${MICRO_OPERATIONS}"
    echo "violation_rates=${MICRO_M1_VIOLATION_RATES}"
    echo "det_ratios=${MICRO_M1_DUPLICATE_RATES}"
    echo "sizes=${MICRO_M1_SIZES}"
    echo "checkpoint_interval=${MICRO_CHECKPOINT_INTERVAL}"
    echo "threads=${MICRO_M1_THREADS}"
    echo "access_pattern=${MICRO_ACCESS_PATTERN}"
    echo "iterations=${ITERATIONS}"
  } > "${META_FILE}"
fi

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
echo "============================================================"
echo "  M1: Ablation study – all validation mechanisms"
echo "  Binary      : ${BINARY}"
echo "  Mechanisms  : ${MICRO_M1_MECHANISMS}"
echo "  Viol rates  : ${MICRO_M1_VIOLATION_RATES}"
echo "  Dup rates   : ${MICRO_M1_DUPLICATE_RATES}"
echo "  Size        : ${MICRO_M1_SIZES} (checkpointed every ${MICRO_CHECKPOINT_INTERVAL})"
echo "  Threads     : ${MICRO_M1_THREADS}"
echo "  Access      : ${MICRO_ACCESS_PATTERN}"
echo "  Iterations  : ${ITERATIONS}"
echo "  Output      : ${RESULT_FILE}"
echo "============================================================"

CMD=(
  ${NUMA_PREFIX} "${BINARY}"
  --experiment M1
  --mechanism "${MICRO_M1_MECHANISMS}"
  --operation "${MICRO_OPERATIONS}"
  --violation-rates "${MICRO_M1_VIOLATION_RATES}"
  --duplicate-rates "${MICRO_M1_DUPLICATE_RATES}"
  --sizes "${MICRO_M1_SIZES}"
  --checkpoint-interval "${MICRO_CHECKPOINT_INTERVAL}"
  --threads "${MICRO_M1_THREADS}"
  --access-pattern "${MICRO_ACCESS_PATTERN}"
  --iterations "${ITERATIONS}"
  --output "${RESULT_FILE}"
)

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "[DRY-RUN] Command: ${CMD[*]}"
  exit 0
fi

"${CMD[@]}" 2>&1 | tee "${LOG_FILE}"

# Update latest symlink
ln -sfn "${TIMESTAMP}" "${BENCH_ROOT}/results/micro/latest"

echo ""
echo "============================================================"
echo "  M1 complete."
echo "  Results : ${RESULT_FILE}"
echo "  Meta    : ${META_FILE}"
echo "  Log     : ${LOG_FILE}"
echo ""
echo "  Plot with:"
echo "    python3 scripts/micro/plot_micro.py \\"
echo "      --input ${RESULT_FILE} --experiment M1 \\"
echo "      --outdir results/latest/plots/micro"
echo "============================================================"