#!/usr/bin/env bash
# =============================================================================
# Canonical benchmark configuration
# Source this file from all run scripts:
#   source "$(dirname "$0")/../common/config.sh"
# =============================================================================

# ---------------------------------------------------------------------------
# Build toolchain
# Override at call-site: CMAKE_CXX_COMPILER=clang++-18 ./scripts/run_all.sh
# ---------------------------------------------------------------------------
export CMAKE_CXX_COMPILER="${CMAKE_CXX_COMPILER:-clang++-20}"
export CMAKE_C_COMPILER="${CMAKE_C_COMPILER:-clang-20}"
export USE_NINJA=true

# NUMA pinning: enable on multi-socket servers, disable locally
export NUMA_ENABLED="${NUMA_ENABLED:-false}"
export NUMA_NODE="${NUMA_NODE:-0}"

# Prefix for binary invocations — prepend numactl when NUMA_ENABLED=true.
# Usage in run scripts:  ${NUMA_PREFIX} "${BINARY}" ...
if [[ "${NUMA_ENABLED}" == "true" ]]; then
  export NUMA_PREFIX="numactl --cpunodebind=${NUMA_NODE} --membind=${NUMA_NODE}"
else
  export NUMA_PREFIX=""
fi

# Build directories (relative to the repo roots, set in build.sh at runtime)
# These are documentation; build.sh resolves them with actual BENCH_ROOT / HYRISE_ROOT.
# MICRO_BUILD_DIR = <BENCH_ROOT>/build
# TXN_BUILD_DIR   = <HYRISE_ROOT>/build_release

# ---------------------------------------------------------------------------
# Micro-benchmark parameters (M0 / M1)
# ---------------------------------------------------------------------------

# Operations
MICRO_OPERATIONS="insert,lookup,delete"

# Mechanisms for M0: OD full-factorial sweep (no "off" – captured in ablation M1)
MICRO_M0_MECHANISMS="od_single,od_multi"

# Mechanisms for M1: Ablation study – all 5 mechanisms head-to-head
MICRO_M1_MECHANISMS="off,fd_single,fd_multi,od_single,od_multi"

# Violation rates (as 0.0–1.0 fractions)
MICRO_VIOLATION_RATES="0.0,0.1,0.2,1.0"

# Duplicate rates (fraction of lhs key collisions; 0.0 = all unique)
MICRO_DUPLICATE_RATES="0.1,0.0"

# Single 1M run; time is recorded every MICRO_CHECKPOINT_INTERVAL operations.
# This implicitly captures all intermediate size points (10k, 20k, …, 1M).
MICRO_SIZES="${MICRO_SIZES:-1000000}"
MICRO_CHECKPOINT_INTERVAL="${MICRO_CHECKPOINT_INTERVAL:-10000}"

# Thread counts (64 can be memory-heavy for large micro inputs on shared servers)
MICRO_THREADS="1,2,4,8,16,32"

# Access patterns
MICRO_ACCESS_PATTERN="both"  # random | sequential | both

# Iterations per configuration
MICRO_ITERATIONS=3

# M1-specific overrides (ablation: 1M, 10% duplicate-rate, 0% violation, threads 1+16)
MICRO_M1_SIZES="${MICRO_M1_SIZES:-1000000}"
MICRO_M1_VIOLATION_RATES="0.0"
MICRO_M1_DUPLICATE_RATES="0.1"
MICRO_M1_THREADS="1,16"

# ---------------------------------------------------------------------------
# Transaction-level benchmark parameters (T1–T6)
# ---------------------------------------------------------------------------

# Workload compositions
# insert_only / update_only / delete_only / mixed
TXN_WORKLOADS_ALL="insert_only,update_only,delete_only,mixed"
TXN_WORKLOAD_DEFAULT="mixed"

# Validation mechanisms (all 5)
TXN_MECHANISMS_ALL="off,fd_single,fd_multi,od_single,od_multi"

# Violation rates
TXN_VIOLATION_RATES="0.0,0.1,0.2,1.0"

# Duplicate rates (fraction of lhs key collisions; 0.0 = all unique)
TXN_DUPLICATE_RATES="0.1,0.0"

# Single 1M run; time is recorded every TXN_CHECKPOINT_INTERVAL operations.
# Size effect is implicit via checkpoint data points (10k, 20k, …, 1M).
# No separate T5 size experiment is needed.
TXN_SIZES="${TXN_SIZES:-1000000}"
TXN_CHECKPOINT_INTERVAL="${TXN_CHECKPOINT_INTERVAL:-10000}"

# Cycling pool capacity (for delete_only / update_only / mixed)
# Bounds dead-row accumulation between compaction checkpoints.
TXN_POOL_CAP=10000

# Thread counts
TXN_THREADS="1,2,4,8,16,32,64"

# Access patterns
TXN_ACCESS_PATTERNS="random,sequential"

# Iterations per configuration
TXN_ITERATIONS=3

# ---------------------------------------------------------------------------
# Canonical baseline for transaction experiments
# (used by T1–T6: all non-varied variables are fixed here)
# ---------------------------------------------------------------------------
CANONICAL_WORKLOAD="mixed"
CANONICAL_MECHANISM="od_multi"
CANONICAL_VIOLATION_RATE="0.0"
CANONICAL_DUPLICATE_RATE="0.1"
CANONICAL_SIZE="${CANONICAL_SIZE:-1000000}"
CANONICAL_ACCESS_PATTERN="random"
CANONICAL_THREADS="2"

# ---------------------------------------------------------------------------
# Mechanism display names and colors (for reference; used by plot scripts)
# ---------------------------------------------------------------------------
# Python plot scripts import result_utils.py which defines MECHANISM_COLORS.
# This section is documentation only.
#
#  off        -> grey    #555555
#  fd_single  -> blue    #1f77b4
#  fd_multi   -> lblue   #aec7e8
#  od_single  -> red     #d62728
#  od_multi   -> pink    #f7b6d2

# ---------------------------------------------------------------------------
# E2E benchmark parameters (Q82 crossover experiment)
# ---------------------------------------------------------------------------

# Number of write batches per workload type (1 op per batch).
# Use 1000 on sidon (SF=10) for stable results; 100 for quick local runs.
E2E_BATCHES="${E2E_BATCHES:-1000}"

# TPC-DS scale factor (1 = local dev, 10 = sidon thesis runs).
E2E_SCALE_FACTOR="${E2E_SCALE_FACTOR:-1}"

# Iterations (independent full runs; results are appended to the same CSV).
E2E_ITERATIONS="${E2E_ITERATIONS:-3}"

# ---------------------------------------------------------------------------
# Result directory layout
# ---------------------------------------------------------------------------
# results/micro/<TIMESTAMP>/results.csv    – one row per 100k checkpoint
# results/micro/<TIMESTAMP>/meta.txt
# results/micro/<TIMESTAMP>/run.log
# results/txn/<TIMESTAMP>/results.csv      – one row per 100k checkpoint
# results/txn/<TIMESTAMP>/meta.txt
# results/txn/<TIMESTAMP>/run.log
# results/latest/micro                     – symlink to most recent micro run
# results/latest/txn                       – symlink to most recent txn run