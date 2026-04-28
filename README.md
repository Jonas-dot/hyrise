# DV-Tree: A Concurrent Index Structure for Online FD and OD Validation

Implementation artifacts for the master thesis by **Jonas Baltruschat**, submitted at the Hasso Plattner Institute, Digital Engineering Faculty, University of Potsdam, 2026.

**Advisor:** Prof. Dr. Felix Naumann  
**Reviewer:** Prof. Dr. Tilmann Rabl

## Overview

This repository contains the implementation of the Dependency Validation Tree (DV-Tree), a B+-Tree-based index structure for online, snapshot-aware validation of functional and order dependencies under multi-version concurrency control (MVCC). The DV-Tree is integrated into the [Hyrise](https://github.com/hyrise/hyrise) in-memory database system.

For general Hyrise documentation (build prerequisites, supported benchmarks, architecture), see the [upstream Hyrise repository](https://github.com/hyrise/hyrise).

## Repository Structure

```
src/lib/storage/index/b_tree/
  b_tree_olc_index.hpp                OLC B+-Tree engine + DV-Tree (core contribution)
  b_tree_olc_index.cpp                Multi-column validation + Hyrise integration
src/benchmark/operators/
  dvi_txn_benchmark_naive.cpp         Transaction benchmark (single-threaded scheduler)
  dvi_txn_benchmark_naive_sched.cpp   Transaction benchmark (multi-threaded scheduler)
src/test/lib/storage/index/b_tree/
  b_tree_olc_index_test.cpp           Unit tests incl. MVCC snapshot correctness
  b_tree_olc_txn_benchmark_setup_test.cpp  Benchmark setup validation tests
dvi_benchmarks/                       Standalone benchmark suite (no Hyrise dependency)
  src/standalone_btree/               Standalone OLC B+-Tree + DV-Tree
  src/standalone_benchmark/           Micro-benchmark runner
  src/e2e/                            End-to-end benchmark (Q82 crossover)
  tests/                              Standalone unit tests
  scripts/                            Run scripts, plotting, configuration
  results/                            Benchmark results referenced in the thesis
```

## Building and Running

### Prerequisites

- C++20 compiler (Clang 15+ or GCC 12+)
- CMake 3.16+
- pthread support

### Standalone Micro-Benchmarks and Tests

```bash
cd dvi_benchmarks
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
make -C build btree_benchmark btree_validation_test
./build/btree_validation_test
```

### Hyrise Integration

```bash
mkdir -p build_release && cd build_release
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc) hyriseTest hyriseBenchmarkDVIndexNaive hyriseBenchmarkDVIndexNaiveSched
```

### Running Tests

```bash
# Hyrise unit tests (DV-Tree + MVCC snapshot tests)
./build_release/hyriseTest --gtest_filter="BTreeOLCIndexTest.*:VersionedGHistoryTest.*"

# Standalone unit tests
./dvi_benchmarks/build/btree_validation_test
```

### Running Benchmarks

```bash
cd dvi_benchmarks

# Full pipeline (micro + naive txn + naive-sched txn + e2e)
./scripts/run_all.sh --build

# Individual stages
./scripts/micro/run_m0.sh --build
./scripts/txn/run_naive_sched.sh --experiment all --build
./scripts/e2e/run_e2e.sh --build
```

## License

The Hyrise database system is licensed under the MIT License.
The DV-Tree implementation and benchmark code are part of the above-named master thesis and are provided for academic review and reproducibility.
