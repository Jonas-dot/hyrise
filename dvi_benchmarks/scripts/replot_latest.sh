#!/usr/bin/env bash
# Regenerate all plots from results/latest/* into results/latest/plots/*/
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PYTHON:-python3}"
LATEST="$REPO/results/latest"
PLOTS="$LATEST/plots"

run_plot() {
  local script="$1"; local input="$2"; local outdir="$3"
  mkdir -p "$outdir"
  echo "  → $(basename "$script")  →  $outdir"
  "$PYTHON" "$script" --input "$input" --outdir "$outdir"
}

echo "=== replot_latest: regenerating all plots ==="

# Merge M0 and M1 micro CSVs (header from M0, data rows from both)
MICRO_MERGED="$(mktemp /tmp/micro_merged_XXXXXX.csv)"
trap 'rm -f "$MICRO_MERGED"' EXIT
head -1 "$LATEST/micro_m0/results.csv" > "$MICRO_MERGED"
tail -n +2 "$LATEST/micro_m0/results.csv" >> "$MICRO_MERGED"
tail -n +2 "$LATEST/micro_m1/results.csv" >> "$MICRO_MERGED"

run_plot "$REPO/scripts/micro/plot_micro.py"  "$MICRO_MERGED"                   "$PLOTS/micro"
run_plot "$REPO/scripts/e2e/plot_e2e.py"      "$LATEST/e2e/results.csv"         "$PLOTS/e2e"
run_plot "$REPO/scripts/txn/plot_txn.py"      "$LATEST/naive/results.csv"       "$PLOTS/naive"
run_plot "$REPO/scripts/txn/plot_txn.py"      "$LATEST/naive_sched/results.csv" "$PLOTS/naive_sched"

echo "=== done ==="
