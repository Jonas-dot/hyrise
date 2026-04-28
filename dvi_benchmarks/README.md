# DVI Benchmark Suite

Standalone benchmark code and scripts for evaluating the Dependency Validation Index (DVI).
The in-Hyrise transaction benchmark executables are defined in `src/benchmark/operators/`.

## Layout

```text
dvi_benchmarks/
  src/
    standalone_benchmark/    benchmark runner code
    standalone_btree/        standalone OLC B-tree implementation code
    e2e/                     end-to-end benchmark (Q82 crossover)
  tests/                     standalone unit tests
  scripts/
    common/                  shared config/build/helpers
    micro/                   micro benchmark run/plot scripts
    txn/                     naive txn benchmark run/analyze/plot scripts
    e2e/                     e2e benchmark run/plot scripts
    run_all.sh               runs micro + naive + naive-sched + e2e pipeline
  results/
    micro/                   timestamped micro runs
    naive_txn/               timestamped naive txn runs
    naive_txn_sched/         timestamped naive-sched txn runs
    e2e/                     e2e runs
    latest/                  pointers/snapshots for latest complete runs
```

## Build

Standalone micro benchmark + tests:

```bash
cd hyrise/dvi_benchmarks
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
make -C build btree_benchmark btree_validation_test
```

Hyrise naive txn benchmarks:

```bash
cd hyrise
cmake -S . -B build_release -DCMAKE_BUILD_TYPE=Release
make -C build_release hyriseBenchmarkDVIndexNaive hyriseBenchmarkDVIndexNaiveSched
```

## Run

Run all stages:

```bash
cd hyrise/dvi_benchmarks
./scripts/run_all.sh --build
```

Run individual stages:

```bash
./scripts/micro/run_m0.sh --build
./scripts/micro/run_m1.sh --build
./scripts/txn/run_naive.sh --experiment all --build
./scripts/txn/run_naive_sched.sh --experiment all --build
./scripts/e2e/run_e2e.sh --build
```

## Outputs

Each run directory contains:

- `results.csv`
- `meta.txt`
- `run.log`

Latest complete outputs are available under:

- `results/latest/micro/`
- `results/latest/naive/`
- `results/latest/naive_sched/`
- `results/latest/e2e/`

## Plotting

```bash
python3 scripts/micro/plot_micro.py \
  --input results/latest/micro/results.csv \
  --experiment M0 \
  --outdir results/latest/plots/micro

python3 scripts/txn/plot_txn.py \
  --input results/latest/naive/results.csv \
  --outdir results/latest/plots/naive
```

## Notes

- Canonical parameter sets are defined in `scripts/common/config.sh`.

## In-Hyrise DVI Files (`hyrise/src`)

Transaction benchmark executables:

- `src/benchmark/operators/dvi_txn_benchmark_naive.cpp`
- `src/benchmark/operators/dvi_txn_benchmark_naive_sched.cpp`

Core index implementation:

- `src/lib/storage/index/b_tree/b_tree_olc_index.hpp`
- `src/lib/storage/index/b_tree/b_tree_olc_index.cpp`

Index-related tests:

- `src/test/lib/storage/index/b_tree/b_tree_olc_index_test.cpp`
- `src/test/lib/storage/index/b_tree/b_tree_olc_txn_benchmark_setup_test.cpp`
