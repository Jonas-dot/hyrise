#!/usr/bin/env bash
# =============================================================================
# run_e2e.sh  –  E2E DVI benchmark driver (Q82 crossover experiment)
#
# Runs hyriseBenchmarkDVIndexE2E for all four workload types
# (INSERT_ONLY, UPDATE_ONLY, DELETE_ONLY, MIXED) with N iterations.
# Each iteration appends to the same results.csv so means can be
# computed across runs.
#
# Output:
#   results/e2e/<TIMESTAMP>/results.csv
#   results/e2e/<TIMESTAMP>/meta.txt
#   results/e2e/<TIMESTAMP>/run.log
#   results/e2e/latest  → symlink to most recent run
#
# Usage:
#   ./scripts/e2e/run_e2e.sh [--build] [--batches N] [--iterations N]
#                             [--scale-factor N] [--dry-run]
#                             [--binary PATH] [--outdir DIR]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
HYRISE_ROOT="$(cd "${BENCH_ROOT}/.." && pwd)"

source "${SCRIPT_DIR}/../common/config.sh"

# ---------------------------------------------------------------------------
# Defaults / argument parsing
# ---------------------------------------------------------------------------
BINARY="${HYRISE_ROOT}/build_release/hyriseBenchmarkDVIndexE2E"
BATCHES="${E2E_BATCHES}"
SCALE_FACTOR="${E2E_SCALE_FACTOR}"
ITERATIONS="${E2E_ITERATIONS}"
DRY_RUN=false
DO_BUILD=false
FORCE_RECONFIGURE=false
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_BASE="${BENCH_ROOT}/results/e2e/${TIMESTAMP}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)              DO_BUILD=true;               shift ;;
    --force-reconfigure)  FORCE_RECONFIGURE=true;      shift ;;
    --dry-run)            DRY_RUN=true;                shift ;;
    --batches=*)          BATCHES="${1#*=}";            shift ;;
    --batches)            BATCHES="$2";                shift 2 ;;
    --iterations=*)       ITERATIONS="${1#*=}";         shift ;;
    --iterations)         ITERATIONS="$2";             shift 2 ;;
    --scale-factor=*)     SCALE_FACTOR="${1#*=}";       shift ;;
    --scale-factor)       SCALE_FACTOR="$2";           shift 2 ;;
    --binary=*)           BINARY="${1#*=}";             shift ;;
    --binary)             BINARY="$2";                 shift 2 ;;
    --outdir=*)           RESULT_BASE="${1#*=}";        shift ;;
    --outdir)             RESULT_BASE="$2";            shift 2 ;;
    *) echo "WARNING: unknown argument: $1"; shift ;;
  esac
done

# ---------------------------------------------------------------------------
# Optional build step
# ---------------------------------------------------------------------------
if [[ "${DO_BUILD}" == "true" ]]; then
  source "${SCRIPT_DIR}/../common/build.sh"
  build_e2e
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
  echo "ERROR: Binary not found: ${BINARY}"
  echo "       Build with: ./scripts/e2e/run_e2e.sh --build"
  exit 1
fi

# ---------------------------------------------------------------------------
# Setup output directory and meta file
# ---------------------------------------------------------------------------
if [[ "${DRY_RUN}" == "false" ]]; then
  mkdir -p "${RESULT_BASE}"
  {
    echo "benchmark=e2e"
    echo "timestamp=${TIMESTAMP}"
    echo "git_hash=$(cd "${HYRISE_ROOT}" && git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"
    echo "host=$(hostname)"
    echo "os=$(uname -srm)"
    echo "binary=${BINARY}"
    echo "batches=${BATCHES}"
    echo "scale_factor=${SCALE_FACTOR}"
    echo "iterations=${ITERATIONS}"
  } > "${META_FILE}"
  exec > >(tee -a "${LOG_FILE}") 2>&1
fi

echo "======================================================================"
echo "  E2E DVI Benchmark (Q82 crossover)"
echo "  Binary      : ${BINARY}"
echo "  Batches     : ${BATCHES}"
echo "  Scale factor: ${SCALE_FACTOR}"
echo "  Iterations  : ${ITERATIONS}"
echo "  Output      : ${RESULT_BASE}/"
echo "  Dry-run     : ${DRY_RUN}"
echo "======================================================================"

# ---------------------------------------------------------------------------
# Run N iterations — each appends to the same CSV
# ---------------------------------------------------------------------------
for iter in $(seq 1 "${ITERATIONS}"); do
  echo ""
  echo "--- Iteration ${iter}/${ITERATIONS} ---"

  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "[DRY-RUN] ${BINARY} --batches ${BATCHES} --output-csv ${RESULT_CSV}"
    continue
  fi

  # TPC-DS generator resolves resource paths relative to CWD — must run from HYRISE_ROOT.
  (cd "${HYRISE_ROOT}" && ${NUMA_PREFIX} "${BINARY}" \
    --batches    "${BATCHES}" \
    --output-csv "${RESULT_CSV}") \
  || true   # do not abort on a single failed iteration
done

# ---------------------------------------------------------------------------
# Update latest symlink
# ---------------------------------------------------------------------------
if [[ "${DRY_RUN}" == "false" ]]; then
  mkdir -p "${BENCH_ROOT}/results/e2e"
  ln -sfn "${TIMESTAMP}" "${BENCH_ROOT}/results/e2e/latest"
fi

echo ""
echo "======================================================================"
echo "  Done."
echo "  Results : ${RESULT_CSV}"
echo "  Meta    : ${META_FILE}"
echo "  Log     : ${LOG_FILE}"
echo "  Plot    :"
echo "    python3 scripts/e2e/plot_e2e.py \\"
echo "      --input ${RESULT_CSV} \\"
echo "      --outdir ${RESULT_BASE}/plots"
echo "======================================================================"
