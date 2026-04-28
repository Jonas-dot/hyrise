#!/usr/bin/env bash
# =============================================================================
# build.sh  –  Shared build helper for the DVI benchmark suite
#
# SOURCE this file from run scripts – do NOT execute it directly:
#   source "${SCRIPT_DIR}/../common/build.sh"
#
# Functions provided:
#   build_micro       – configure (if needed) + compile btree_benchmark
#   build_naive       – configure (if needed) + compile hyriseBenchmarkDVIndexNaive
#   build_naive_sched – configure (if needed) + compile hyriseBenchmarkDVIndexNaiveSched
#   build_e2e         – configure (if needed) + compile hyriseBenchmarkDVIndexE2E
#   build_all         – build_micro + build_naive + build_naive_sched + build_e2e
#
# For sanitizer builds on sidon use Hyrise's standard CMake flags directly:
#   TSan:        cmake ... -DENABLE_THREAD_SANITIZATION=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo
#   ASan+LSan:   cmake ... -DENABLE_ADDR_UB_LEAK_SANITIZATION=ON -DCMAKE_BUILD_TYPE=Debug
#
# Variables read (all set by config.sh, overridable via environment):
#   CMAKE_CXX_COMPILER  (default: clang++-20)
#   CMAKE_C_COMPILER    (default: clang-20)
#   USE_NINJA           (default: true)
#   BENCH_ROOT          (set by the sourcing run script)
#   HYRISE_ROOT         (set by the sourcing run script)
#
# Flags respected by the sourcing run script:
#   FORCE_RECONFIGURE   set to "true" to re-run cmake even when build.ninja exists
# =============================================================================

FORCE_RECONFIGURE="${FORCE_RECONFIGURE:-false}"

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
_build_log() { echo "[build] $*"; }

_check_compiler() {
  local cxx="${CMAKE_CXX_COMPILER}"
  if ! command -v "${cxx}" &>/dev/null; then
    echo "ERROR: C++ compiler not found: ${cxx}"
    echo "       Install clang-20, or override: CMAKE_CXX_COMPILER=clang++-18 ..."
    return 1
  fi
  _build_log "Compiler : $(${cxx} --version | head -1)"
}

_generator_flags() {
  if [[ "${USE_NINJA:-true}" == "true" ]] && command -v ninja &>/dev/null; then
    echo "-G Ninja"
  else
    echo ""
  fi
}

_build_target() {
  local build_dir="$1"
  local target="$2"
  if [[ "${USE_NINJA:-true}" == "true" ]] && command -v ninja &>/dev/null; then
    ninja -C "${build_dir}" "${target}"
  else
    make -C "${build_dir}" -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu)" "${target}"
  fi
}

# ---------------------------------------------------------------------------
# build_micro
# Builds: btree_benchmark (standalone, in hyrise/dvi_benchmarks/build/)
# ---------------------------------------------------------------------------
build_micro() {
  local build_dir="${BENCH_ROOT}/build"

  _build_log "=== Building micro benchmark (btree_benchmark) ==="
  _check_compiler || return 1

  local gen_flags
  gen_flags="$(_generator_flags)"

  if [[ ! -f "${build_dir}/build.ninja" && ! -f "${build_dir}/Makefile" ]] || \
     [[ "${FORCE_RECONFIGURE}" == "true" ]]; then
    _build_log "Configuring cmake in: ${build_dir}"
    cmake -S "${BENCH_ROOT}" \
          -B "${build_dir}" \
          ${gen_flags} \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_CXX_COMPILER="${CMAKE_CXX_COMPILER}" \
          -DCMAKE_C_COMPILER="${CMAKE_C_COMPILER}" \
          -DCMAKE_CXX_FLAGS="-O3 -march=native"
  else
    _build_log "Build dir exists, skipping cmake configure (use FORCE_RECONFIGURE=true to override)"
  fi

  _build_log "Compiling btree_benchmark..."
  _build_target "${build_dir}" btree_benchmark
  _build_log "btree_benchmark built: ${build_dir}/btree_benchmark"
}

# ---------------------------------------------------------------------------
# build_naive
# Builds: hyriseBenchmarkDVIndexNaive (Hyrise, in hyrise/build_release/)
# ---------------------------------------------------------------------------
build_naive() {
  local build_dir="${HYRISE_ROOT}/build_release"

  _build_log "=== Building naive DVI benchmark (hyriseBenchmarkDVIndexNaive) ==="
  _check_compiler || return 1

  local gen_flags
  gen_flags="$(_generator_flags)"

  if [[ ! -f "${build_dir}/build.ninja" && ! -f "${build_dir}/Makefile" ]] || \
     [[ "${FORCE_RECONFIGURE}" == "true" ]]; then
    _build_log "Configuring cmake in: ${build_dir}"
    mkdir -p "${build_dir}"
    cmake -S "${HYRISE_ROOT}" \
          -B "${build_dir}" \
          ${gen_flags} \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_CXX_COMPILER="${CMAKE_CXX_COMPILER}" \
          -DCMAKE_C_COMPILER="${CMAKE_C_COMPILER}" \
          -DCMAKE_CXX_FLAGS="-O3 -march=native"
  else
    _build_log "Build dir exists, skipping cmake configure (use FORCE_RECONFIGURE=true to override)"
  fi

  _build_log "Compiling hyriseBenchmarkDVIndexNaive..."
  _build_target "${build_dir}" hyriseBenchmarkDVIndexNaive
  _build_log "hyriseBenchmarkDVIndexNaive built: ${build_dir}/hyriseBenchmarkDVIndexNaive"
}

# ---------------------------------------------------------------------------
# build_naive_sched
# Builds: hyriseBenchmarkDVIndexNaiveSched (Hyrise, in hyrise/build_release/)
# ---------------------------------------------------------------------------
build_naive_sched() {
  local build_dir="${HYRISE_ROOT}/build_release"

  _build_log "=== Building naive-sched DVI benchmark (hyriseBenchmarkDVIndexNaiveSched) ==="
  _check_compiler || return 1

  local gen_flags
  gen_flags="$(_generator_flags)"

  if [[ ! -f "${build_dir}/build.ninja" && ! -f "${build_dir}/Makefile" ]] || \
     [[ "${FORCE_RECONFIGURE}" == "true" ]]; then
    _build_log "Configuring cmake in: ${build_dir}"
    mkdir -p "${build_dir}"
    cmake -S "${HYRISE_ROOT}" \
          -B "${build_dir}" \
          ${gen_flags} \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_CXX_COMPILER="${CMAKE_CXX_COMPILER}" \
          -DCMAKE_C_COMPILER="${CMAKE_C_COMPILER}" \
          -DCMAKE_CXX_FLAGS="-O3 -march=native"
  else
    _build_log "Build dir exists, skipping cmake configure (use FORCE_RECONFIGURE=true to override)"
  fi

  _build_log "Compiling hyriseBenchmarkDVIndexNaiveSched..."
  _build_target "${build_dir}" hyriseBenchmarkDVIndexNaiveSched
  _build_log "hyriseBenchmarkDVIndexNaiveSched built: ${build_dir}/hyriseBenchmarkDVIndexNaiveSched"
}

# ---------------------------------------------------------------------------
# build_e2e
# Builds: hyriseBenchmarkDVIndexE2E (Hyrise, in hyrise/build_release/)
# ---------------------------------------------------------------------------
build_e2e() {
  local build_dir="${HYRISE_ROOT}/build_release"

  _build_log "=== Building E2E DVI benchmark (hyriseBenchmarkDVIndexE2E) ==="
  _check_compiler || return 1

  local gen_flags
  gen_flags="$(_generator_flags)"

  if [[ ! -f "${build_dir}/build.ninja" && ! -f "${build_dir}/Makefile" ]] || \
     [[ "${FORCE_RECONFIGURE}" == "true" ]]; then
    _build_log "Configuring cmake in: ${build_dir}"
    mkdir -p "${build_dir}"
    cmake -S "${HYRISE_ROOT}" \
          -B "${build_dir}" \
          ${gen_flags} \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_CXX_COMPILER="${CMAKE_CXX_COMPILER}" \
          -DCMAKE_C_COMPILER="${CMAKE_C_COMPILER}" \
          -DCMAKE_CXX_FLAGS="-O3 -march=native"
  else
    _build_log "Build dir exists, skipping cmake configure (use FORCE_RECONFIGURE=true to override)"
  fi

  _build_log "Compiling hyriseBenchmarkDVIndexE2E..."
  _build_target "${build_dir}" hyriseBenchmarkDVIndexE2E
  _build_log "hyriseBenchmarkDVIndexE2E built: ${build_dir}/hyriseBenchmarkDVIndexE2E"
}

# ---------------------------------------------------------------------------
# build_all
# ---------------------------------------------------------------------------
build_all() {
  build_micro
  build_naive
  build_naive_sched
  build_e2e
}
