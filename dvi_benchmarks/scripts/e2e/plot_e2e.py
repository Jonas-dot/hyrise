#!/usr/bin/env python3
"""
plot_e2e.py  –  Generate plots for the E2E DVI Q82 crossover benchmark.

CSV columns (written by hyriseBenchmarkDVIndexE2E):
  workload, batches, scale_factor,
  ops_no_dvi_ms, ops_dvi_ms, overhead_ms, overhead_pct,
  query_base_ms, query_opt_ms, benefit_ms, benefit_pct, crossover

Figures produced (saved as PDF under <outdir>/):
  fig_e2e_overhead_pct.pdf   — write overhead % per workload type (bar)
  fig_e2e_benefit_pct.pdf    — query benefit % per workload type (bar)
  fig_e2e_crossover.pdf      — crossover (ops per query) per workload type (bar)
  fig_e2e_abs_times.pdf      — absolute write + query times, DVI vs no-DVI (grouped bar)

Usage:
  python3 scripts/e2e/plot_e2e.py --input results/e2e/<TS>/results.csv
  python3 scripts/e2e/plot_e2e.py --input results/e2e/<TS>/results.csv \\
      --outdir results/e2e/<TS>/plots
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "common"))
from result_utils import apply_thesis_style, save_fig, FIG_W_FULL, FIG_W_SINGLE, FIG_H_DEFAULT

apply_thesis_style()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORKLOAD_ORDER = ["insert_only", "update_only", "delete_only", "mixed"]

WORKLOAD_LABEL = {
    "insert_only": "Insert",
    "update_only": "Update\n(d_year)",
    "delete_only": "Delete",
    "mixed":       "Mixed\n(1+1+1)",
}

WORKLOAD_COLORS = {
    "insert_only": "#1f77b4",
    "update_only": "#ff7f0e",
    "delete_only": "#2ca02c",
    "mixed":       "#9467bd",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_e2e_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df["workload"] = df["workload"].str.strip().str.lower()
    # benchmark writes break_even_ratio; script uses crossover
    if "break_even_ratio" in df.columns and "crossover" not in df.columns:
        df = df.rename(columns={"break_even_ratio": "crossover"})
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """Mean + std across iterations (rows with the same workload/batches/scale_factor)."""
    keys = ["workload", "batches", "scale_factor"]
    numeric = [c for c in df.select_dtypes(include="number").columns if c not in keys]
    grp = df.groupby(keys)
    mean = grp[numeric].mean().reset_index()
    std  = grp[numeric].std(ddof=1).reset_index().rename(
        columns={c: c + "_std" for c in numeric}
    )
    return pd.merge(mean, std, on=keys)


def ordered_workloads(df: pd.DataFrame):
    present = df["workload"].unique()
    return [w for w in WORKLOAD_ORDER if w in present]


def bar_positions(n: int, width: float = 0.6):
    return np.arange(n), width


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------

def plot_overhead_pct(df: pd.DataFrame, outdir: str):
    """Bar chart: DVI write overhead as % of baseline write time."""
    wloads = ordered_workloads(df)
    vals   = [df.loc[df["workload"] == w, "overhead_pct"].values[0] for w in wloads]
    errs   = [df.loc[df["workload"] == w, "overhead_pct_std"].values[0]
              if "overhead_pct_std" in df.columns else 0.0
              for w in wloads]

    xs, width = bar_positions(len(wloads))
    fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
    bars = ax.bar(xs, vals, width, yerr=errs, capsize=4,
                  color=[WORKLOAD_COLORS[w] for w in wloads], zorder=3)
    ax.set_xticks(xs)
    ax.set_xticklabels([WORKLOAD_LABEL[w] for w in wloads])
    ax.set_ylabel("DVI write overhead (%)")
    ax.set_title("DVI maintenance overhead by workload type")
    ax.yaxis.grid(True, zorder=0)
    ax.set_axisbelow(True)
    # Annotate bars
    for bar, val in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                f"{val:.1f}%", ha="center", va="bottom", fontsize=8)
    save_fig(fig, os.path.join(outdir, "fig_e2e_overhead_pct.pdf"))


def plot_benefit_pct(df: pd.DataFrame, outdir: str):
    """Bar chart: query speedup from OD rewrite as % of base query time."""
    wloads = ordered_workloads(df)
    vals   = [df.loc[df["workload"] == w, "benefit_pct"].values[0] for w in wloads]
    errs   = [df.loc[df["workload"] == w, "benefit_pct_std"].values[0]
              if "benefit_pct_std" in df.columns else 0.0
              for w in wloads]

    xs, width = bar_positions(len(wloads))
    fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
    bars = ax.bar(xs, vals, width, yerr=errs, capsize=4,
                  color=[WORKLOAD_COLORS[w] for w in wloads], zorder=3)
    ax.set_xticks(xs)
    ax.set_xticklabels([WORKLOAD_LABEL[w] for w in wloads])
    ax.set_ylabel("Query speedup (%)")
    ax.set_title("Q82 benefit from OD rewrite by workload type")
    ax.yaxis.grid(True, zorder=0)
    ax.set_axisbelow(True)
    for bar, val in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                f"{val:.1f}%", ha="center", va="bottom", fontsize=8)
    save_fig(fig, os.path.join(outdir, "fig_e2e_benefit_pct.pdf"))


def plot_crossover(df: pd.DataFrame, outdir: str):
    """Bar chart: crossover ratio — ops per query."""
    wloads = ordered_workloads(df)
    vals   = [df.loc[df["workload"] == w, "crossover"].values[0] for w in wloads]

    xs, width = bar_positions(len(wloads))
    fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
    bars = ax.bar(xs, vals, width,
                  color=[WORKLOAD_COLORS[w] for w in wloads], zorder=3)
    ax.set_xticks(xs)
    ax.set_xticklabels([WORKLOAD_LABEL[w] for w in wloads])
    ax.set_ylabel("Write ops paid for by 1 faster query")
    ax.set_title("DVI crossover point by workload type")
    ax.yaxis.grid(True, zorder=0)
    ax.set_axisbelow(True)
    ax.set_yscale("log")
    for bar, val in zip(bars, vals):
        if val > 0:
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() * 1.05,
                    f"~{int(val):,}", ha="center", va="bottom", fontsize=8)
    save_fig(fig, os.path.join(outdir, "fig_e2e_crossover.pdf"))


def plot_abs_times(df: pd.DataFrame, outdir: str):
    """Grouped bar chart: absolute write and query times (DVI vs no-DVI)."""
    wloads = ordered_workloads(df)
    n = len(wloads)
    group_width = 0.35
    xs = np.arange(n)

    fig, axes = plt.subplots(1, 2, figsize=(FIG_W_FULL, FIG_H_DEFAULT))

    # Left: write times
    ax = axes[0]
    no_dvi = [df.loc[df["workload"] == w, "ops_no_dvi_ms"].values[0] for w in wloads]
    dvi    = [df.loc[df["workload"] == w, "ops_dvi_ms"].values[0]    for w in wloads]
    ax.bar(xs - group_width / 2, no_dvi, group_width, label="No DVI",
           color="#aec7e8", zorder=3)
    ax.bar(xs + group_width / 2, dvi,    group_width, label="With DVI",
           color="#1f77b4", zorder=3)
    ax.set_xticks(xs)
    ax.set_xticklabels([WORKLOAD_LABEL[w] for w in wloads])
    ax.set_ylabel("Total write time (ms)")
    ax.set_title("Write time: DVI vs no-DVI")
    ax.legend()
    ax.yaxis.grid(True, zorder=0)
    ax.set_axisbelow(True)

    # Right: query times
    ax = axes[1]
    base = [df.loc[df["workload"] == w, "query_base_ms"].values[0] for w in wloads]
    opt  = [df.loc[df["workload"] == w, "query_opt_ms"].values[0]  for w in wloads]
    ax.bar(xs - group_width / 2, base, group_width, label="Base plan",
           color="#f7b6d2", zorder=3)
    ax.bar(xs + group_width / 2, opt,  group_width, label="OD-optimized",
           color="#d62728", zorder=3)
    ax.set_xticks(xs)
    ax.set_xticklabels([WORKLOAD_LABEL[w] for w in wloads])
    ax.set_ylabel("Total query time (ms)")
    ax.set_title("Q82 time: base vs OD-optimized")
    ax.legend()
    ax.yaxis.grid(True, zorder=0)
    ax.set_axisbelow(True)

    fig.tight_layout()
    save_fig(fig, os.path.join(outdir, "fig_e2e_abs_times.pdf"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Plot E2E DVI benchmark results")
    parser.add_argument("--input",   required=True, help="Path to results.csv")
    parser.add_argument("--outdir",  default=None,  help="Output directory (default: same as input)")
    parser.add_argument("--experiment", default="all",
                        help="Which plot to generate (overhead|benefit|crossover|abs|all)")
    args = parser.parse_args()

    outdir = args.outdir or os.path.dirname(os.path.abspath(args.input))
    os.makedirs(outdir, exist_ok=True)

    df_raw = load_e2e_csv(args.input)
    df     = aggregate(df_raw)

    exp = args.experiment.lower()
    if exp in ("overhead", "all"):
        plot_overhead_pct(df, outdir)
        print(f"  fig_e2e_overhead_pct.pdf  → {outdir}")
    if exp in ("benefit", "all"):
        plot_benefit_pct(df, outdir)
        print(f"  fig_e2e_benefit_pct.pdf   → {outdir}")
    if exp in ("crossover", "all"):
        plot_crossover(df, outdir)
        print(f"  fig_e2e_crossover.pdf     → {outdir}")
    if exp in ("abs", "all"):
        plot_abs_times(df, outdir)
        print(f"  fig_e2e_abs_times.pdf     → {outdir}")


if __name__ == "__main__":
    main()
