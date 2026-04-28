#!/usr/bin/env bash
# =============================================================================
# run_all.sh  -  Master pipeline for the DVI benchmark suite
#
# Orchestrates benchmark stages (micro, naive txn, naive-sched txn, e2e) in a
# single invocation. Delegates to the individual run scripts and updates
# results/latest/ symlinks.
#
# Usage:
#   ./scripts/run_all.sh [options]
#
# Stage selection (default: all):
#   --micro                    Run micro experiments (M0 + M1)
#   --naive                    Run naive txn experiments (T1-T6; no pooling/compaction)
#   --naive-sched              Run naive-sched txn experiments (T1-T6; same workload + scheduler)
#   --e2e                      Run E2E Q82 crossover experiment
#   --naive-experiment ID      Run only one naive txn experiment (T1|T2|...|T6)
#   --naive-sched-experiment ID  Run only one naive-sched txn experiment
#   --all                      Run micro + naive + naive-sched + e2e (explicit, same as default)
#
# Build options:
#   --build               Build required binaries before running
#   --force-reconfigure   Re-run cmake even if build directory already exists
#
# Quick mode:
#   --quick               Use 100K ops (instead of 10M) with 10K checkpoint
#                         interval. Everything else stays the same.
#
# Run options:
#   --iterations N        Override iteration count for all sub-runs
#   --dry-run             Print commands without executing anything
#   --scheduler-workers N Override scheduler worker count for naive-sched stage
#   --e2e-batches N       Override E2E batch count (default: E2E_BATCHES from config.sh)
#
# Binary overrides:
#   --micro-binary PATH        Path to btree_benchmark binary
#   --naive-binary PATH        Path to hyriseBenchmarkDVIndexNaive binary
#   --naive-sched-binary PATH  Path to hyriseBenchmarkDVIndexNaiveSched binary
#   --e2e-binary PATH          Path to hyriseBenchmarkDVIndexE2E binary
#
# Output:
#   results/micro/<TIMESTAMP>/           (if micro runs)
#   results/naive_txn/<TIMESTAMP>/       (if naive runs)
#   results/naive_txn_sched/<TIMESTAMP>/ (if naive-sched runs)
#   results/e2e/<TIMESTAMP>/             (if e2e runs)
#   results/latest/micro                 (symlink, updated after each micro run)
#   results/latest/naive                 (symlink, updated after each naive run)
#   results/latest/naive_sched           (symlink, updated after each naive-sched run)
#   results/latest/e2e                   (symlink, updated after each e2e run)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
HYRISE_ROOT="$(cd "${BENCH_ROOT}/.." && pwd)"

source "${SCRIPT_DIR}/common/config.sh"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
RUN_MICRO=false
RUN_NAIVE=false
RUN_NAIVE_SCHED=false
RUN_E2E=false
NAIVE_EXPERIMENT="all"
NAIVE_SCHED_EXPERIMENT="all"
DO_BUILD=false
FORCE_RECONFIGURE=false
QUICK_MODE=false
ITERATIONS_OVERRIDE=""
DRY_RUN=false
SCHEDULER_WORKERS_OVERRIDE=""
E2E_BATCHES_OVERRIDE=""
MICRO_BINARY="${BENCH_ROOT}/build/btree_benchmark"
NAIVE_BINARY="${HYRISE_ROOT}/build_release/hyriseBenchmarkDVIndexNaive"
NAIVE_SCHED_BINARY="${HYRISE_ROOT}/build_release/hyriseBenchmarkDVIndexNaiveSched"
E2E_BINARY="${HYRISE_ROOT}/build_release/hyriseBenchmarkDVIndexE2E"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
EXPLICIT_STAGE=false

for arg in "$@"; do
  case "$arg" in
    --micro)                    RUN_MICRO=true;        EXPLICIT_STAGE=true ;;
    --naive)                    RUN_NAIVE=true;        EXPLICIT_STAGE=true ;;
    --naive-sched)              RUN_NAIVE_SCHED=true;  EXPLICIT_STAGE=true ;;
    --e2e)                      RUN_E2E=true;          EXPLICIT_STAGE=true ;;
    --all)                      RUN_MICRO=true; RUN_NAIVE=true; RUN_NAIVE_SCHED=true; RUN_E2E=true; EXPLICIT_STAGE=true ;;
    --naive-experiment=*)       NAIVE_EXPERIMENT="${arg#*=}"; RUN_NAIVE=true; EXPLICIT_STAGE=true ;;
    --naive-sched-experiment=*) NAIVE_SCHED_EXPERIMENT="${arg#*=}"; RUN_NAIVE_SCHED=true; EXPLICIT_STAGE=true ;;
    --quick)                    QUICK_MODE=true ;;
    --build)                    DO_BUILD=true ;;
    --force-reconfigure)        FORCE_RECONFIGURE=true ;;
    --dry-run)                  DRY_RUN=true ;;
    --micro-binary=*)           MICRO_BINARY="${arg#*=}" ;;
    --naive-binary=*)           NAIVE_BINARY="${arg#*=}" ;;
    --naive-sched-binary=*)     NAIVE_SCHED_BINARY="${arg#*=}" ;;
    --e2e-binary=*)             E2E_BINARY="${arg#*=}" ;;
    --iterations=*)             ITERATIONS_OVERRIDE="${arg#*=}" ;;
    --scheduler-workers=*)      SCHEDULER_WORKERS_OVERRIDE="${arg#*=}" ;;
    --e2e-batches=*)            E2E_BATCHES_OVERRIDE="${arg#*=}" ;;
    --naive-experiment)         shift; NAIVE_EXPERIMENT="$1"; RUN_NAIVE=true; EXPLICIT_STAGE=true ;;
    --naive-sched-experiment)   shift; NAIVE_SCHED_EXPERIMENT="$1"; RUN_NAIVE_SCHED=true; EXPLICIT_STAGE=true ;;
    --micro-binary)             shift; MICRO_BINARY="$1" ;;
    --naive-binary)             shift; NAIVE_BINARY="$1" ;;
    --naive-sched-binary)       shift; NAIVE_SCHED_BINARY="$1" ;;
    --iterations)               shift; ITERATIONS_OVERRIDE="$1" ;;
    --scheduler-workers)        shift; SCHEDULER_WORKERS_OVERRIDE="$1" ;;
    --txn|--txn-experiment|--txn-binary|--keep-raw-json)
      echo "ERROR: Legacy txn mode was removed. Use --naive / --naive-experiment / --naive-binary."
      exit 1
      ;;
    --txn-experiment=*|--txn-binary=*)
      echo "ERROR: Legacy txn mode was removed. Use --naive / --naive-experiment / --naive-binary."
      exit 1
      ;;
    --help|-h)
      sed -n '/^# Usage/,/^# =====/{ /^# =====/d; s/^# \?//; p }' "$0"
      exit 0
      ;;
    *)
      echo "WARNING: Unknown argument: $arg"
      ;;
  esac
done

# Default to --all if no stage flag was given
if [[ "${EXPLICIT_STAGE}" == "false" ]]; then
  RUN_MICRO=true
  RUN_NAIVE=true
  RUN_NAIVE_SCHED=true
  RUN_E2E=true
fi

# ---------------------------------------------------------------------------
# Quick mode: override size-related config variables
# ---------------------------------------------------------------------------
if [[ "${QUICK_MODE}" == "true" ]]; then
  echo "[QUICK MODE] Overriding sizes: 100K ops, 10K checkpoint interval"
  export MICRO_SIZES="100000"
  export MICRO_CHECKPOINT_INTERVAL=10000
  export MICRO_M1_SIZES="100000"
  export TXN_SIZES="100000"
  export TXN_CHECKPOINT_INTERVAL=10000
  export CANONICAL_SIZE="100000"
fi

# ---------------------------------------------------------------------------
# Build if requested
# ---------------------------------------------------------------------------
if [[ "${DO_BUILD}" == "true" ]]; then
  export FORCE_RECONFIGURE
  source "${SCRIPT_DIR}/common/build.sh"
  if [[ "${RUN_MICRO}" == "true" ]]; then
    build_micro
  fi
  if [[ "${RUN_NAIVE}" == "true" ]]; then
    build_naive
  fi
  if [[ "${RUN_NAIVE_SCHED}" == "true" ]]; then
    build_naive_sched
  fi
  if [[ "${RUN_E2E}" == "true" ]]; then
    build_e2e
  fi
fi

# ---------------------------------------------------------------------------
# Compose per-stage flags
# ---------------------------------------------------------------------------
MICRO_FLAGS=()
NAIVE_FLAGS=(--experiment "${NAIVE_EXPERIMENT}")
NAIVE_SCHED_FLAGS=(--experiment "${NAIVE_SCHED_EXPERIMENT}")
E2E_FLAGS=()

[[ "${DRY_RUN}" == "true" ]]      && MICRO_FLAGS+=(--dry-run)
[[ "${DRY_RUN}" == "true" ]]      && NAIVE_FLAGS+=(--dry-run)
[[ "${DRY_RUN}" == "true" ]]      && NAIVE_SCHED_FLAGS+=(--dry-run)
[[ "${DRY_RUN}" == "true" ]]      && E2E_FLAGS+=(--dry-run)
[[ -n "${ITERATIONS_OVERRIDE}" ]]  && MICRO_FLAGS+=(--iterations="${ITERATIONS_OVERRIDE}") \
                                   && NAIVE_FLAGS+=(--iterations="${ITERATIONS_OVERRIDE}") \
                                   && NAIVE_SCHED_FLAGS+=(--iterations="${ITERATIONS_OVERRIDE}") \
                                   && E2E_FLAGS+=(--iterations="${ITERATIONS_OVERRIDE}")
[[ -n "${SCHEDULER_WORKERS_OVERRIDE}" ]] && NAIVE_SCHED_FLAGS+=(--scheduler-workers="${SCHEDULER_WORKERS_OVERRIDE}")
[[ -n "${E2E_BATCHES_OVERRIDE}" ]]       && E2E_FLAGS+=(--batches="${E2E_BATCHES_OVERRIDE}")

MICRO_FLAGS+=(--binary="${MICRO_BINARY}")
NAIVE_FLAGS+=(--binary="${NAIVE_BINARY}")
NAIVE_SCHED_FLAGS+=(--binary="${NAIVE_SCHED_BINARY}")
E2E_FLAGS+=(--binary="${E2E_BINARY}")

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
echo "======================================================================"
echo "  DVI Benchmark Suite - Master Pipeline"
echo "  Micro exps      : M0 (OD full-factorial), M1 (ablation)"
echo "  Naive exps      : T1 (workload), T2 (mechanism), T3 (violation),"
echo "                    T4 (dup-rate), T5 (threads), T6 (access pattern)"
echo "  Naive-sched exps: same workload, NodeQueueScheduler active"
echo "  Run micro       : ${RUN_MICRO}"
echo "  Run naive       : ${RUN_NAIVE}"
echo "  Run naive-sched : ${RUN_NAIVE_SCHED}"
if [[ "${RUN_NAIVE}" == "true" ]]; then
  echo "  Naive stage     : ${NAIVE_EXPERIMENT}"
fi
if [[ "${RUN_NAIVE_SCHED}" == "true" ]]; then
  echo "  Naive-sched stage: ${NAIVE_SCHED_EXPERIMENT}"
fi
echo "  Run e2e         : ${RUN_E2E}"
echo "  Build           : ${DO_BUILD}"
echo "  Quick mode      : ${QUICK_MODE}"
echo "  Dry-run         : ${DRY_RUN}"
echo "  Iterations      : ${ITERATIONS_OVERRIDE:-<default from config.sh>}"
echo "  Compiler        : ${CMAKE_CXX_COMPILER}"
echo "  Micro bin       : ${MICRO_BINARY}"
echo "  Naive bin       : ${NAIVE_BINARY}"
echo "  Naive-sched bin : ${NAIVE_SCHED_BINARY}"
echo "  E2E bin         : ${E2E_BINARY}"
echo "======================================================================"

# ---------------------------------------------------------------------------
# Run stages
# ---------------------------------------------------------------------------
MICRO_TIMESTAMP=""
NAIVE_TIMESTAMP=""
NAIVE_SCHED_TIMESTAMP=""

if [[ "${RUN_MICRO}" == "true" ]]; then
  echo ""
  echo ">>> Stage: MICRO (M0)"
  "${SCRIPT_DIR}/micro/run_m0.sh" "${MICRO_FLAGS[@]}"

  echo ""
  echo ">>> Stage: MICRO (M1)"
  "${SCRIPT_DIR}/micro/run_m1.sh" "${MICRO_FLAGS[@]}"

  if [[ "${DRY_RUN}" == "false" ]]; then
    MICRO_TIMESTAMP=$(ls -1t "${BENCH_ROOT}/results/micro/" | grep -v latest | head -1 || true)
  fi
fi

if [[ "${RUN_NAIVE}" == "true" ]]; then
  echo ""
  echo ">>> Stage: NAIVE-TXN (${NAIVE_EXPERIMENT})"
  "${SCRIPT_DIR}/txn/run_naive.sh" "${NAIVE_FLAGS[@]}"

  if [[ "${DRY_RUN}" == "false" ]]; then
    NAIVE_TIMESTAMP=$(ls -1t "${BENCH_ROOT}/results/naive_txn/" | grep -v latest | head -1 || true)
  fi
fi

if [[ "${RUN_NAIVE_SCHED}" == "true" ]]; then
  echo ""
  echo ">>> Stage: NAIVE-SCHED-TXN (${NAIVE_SCHED_EXPERIMENT})"
  "${SCRIPT_DIR}/txn/run_naive_sched.sh" "${NAIVE_SCHED_FLAGS[@]}"

  if [[ "${DRY_RUN}" == "false" ]]; then
    NAIVE_SCHED_TIMESTAMP=$(ls -1t "${BENCH_ROOT}/results/naive_txn_sched/" | grep -v latest | head -1 || true)
  fi
fi

E2E_TIMESTAMP=""

if [[ "${RUN_E2E}" == "true" ]]; then
  echo ""
  echo ">>> Stage: E2E (Q82 crossover)"
  "${SCRIPT_DIR}/e2e/run_e2e.sh" "${E2E_FLAGS[@]}"

  if [[ "${DRY_RUN}" == "false" ]]; then
    E2E_TIMESTAMP=$(ls -1t "${BENCH_ROOT}/results/e2e/" | grep -v latest | head -1 || true)
  fi
fi

# ---------------------------------------------------------------------------
# Update results/latest/ symlinks
# ---------------------------------------------------------------------------
if [[ "${DRY_RUN}" == "false" ]]; then
  mkdir -p "${BENCH_ROOT}/results/latest"

  if [[ -n "${MICRO_TIMESTAMP}" ]]; then
    ln -sfn "../micro/${MICRO_TIMESTAMP}" "${BENCH_ROOT}/results/latest/micro"
    echo "results/latest/micro -> results/micro/${MICRO_TIMESTAMP}"
  fi

  if [[ -n "${NAIVE_TIMESTAMP}" ]]; then
    ln -sfn "../naive_txn/${NAIVE_TIMESTAMP}" "${BENCH_ROOT}/results/latest/naive"
    echo "results/latest/naive -> results/naive_txn/${NAIVE_TIMESTAMP}"
  fi

  if [[ -n "${NAIVE_SCHED_TIMESTAMP}" ]]; then
    ln -sfn "../naive_txn_sched/${NAIVE_SCHED_TIMESTAMP}" "${BENCH_ROOT}/results/latest/naive_sched"
    echo "results/latest/naive_sched -> results/naive_txn_sched/${NAIVE_SCHED_TIMESTAMP}"
  fi

  if [[ -n "${E2E_TIMESTAMP}" ]]; then
    ln -sfn "../e2e/${E2E_TIMESTAMP}" "${BENCH_ROOT}/results/latest/e2e"
    echo "results/latest/e2e -> results/e2e/${E2E_TIMESTAMP}"
  fi

  # Remove legacy link/path from older pipeline layouts.
  if [[ -e "${BENCH_ROOT}/results/latest/txn" ]]; then
    rm -rf "${BENCH_ROOT}/results/latest/txn"
    echo "results/latest/txn removed (legacy path)"
  fi
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "======================================================================"
echo "  All stages complete."
echo ""
if [[ -n "${MICRO_TIMESTAMP}" ]]; then
  echo "  Micro results : results/micro/${MICRO_TIMESTAMP}/results.csv"
  echo "  Plot micro    :"
  echo "    python3 scripts/micro/plot_micro.py --input results/micro/${MICRO_TIMESTAMP}/results.csv --experiment M0 --outdir results/latest/plots/micro"
fi
if [[ -n "${NAIVE_TIMESTAMP}" ]]; then
  echo ""
  echo "  Naive txn results : results/naive_txn/${NAIVE_TIMESTAMP}/results.csv"
  echo "  Plot naive txn    :"
  echo "    python3 scripts/txn/plot_txn.py --input results/naive_txn/${NAIVE_TIMESTAMP}/results.csv --outdir results/latest/plots/naive"
fi
if [[ -n "${NAIVE_SCHED_TIMESTAMP}" ]]; then
  echo ""
  echo "  Naive-sched txn results : results/naive_txn_sched/${NAIVE_SCHED_TIMESTAMP}/results.csv"
  echo "  Plot naive-sched txn    :"
  echo "    python3 scripts/txn/plot_txn.py --input results/naive_txn_sched/${NAIVE_SCHED_TIMESTAMP}/results.csv --outdir results/latest/plots/naive_sched"
fi
if [[ -n "${E2E_TIMESTAMP}" ]]; then
  echo ""
  echo "  E2E results : results/e2e/${E2E_TIMESTAMP}/results.csv"
  echo "  Plot e2e    :"
  echo "    python3 scripts/e2e/plot_e2e.py --input results/e2e/${E2E_TIMESTAMP}/results.csv --outdir results/latest/plots/e2e"
fi
echo "======================================================================"
