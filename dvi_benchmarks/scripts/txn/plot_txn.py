#!/usr/bin/env python3
"""
plot_txn.py  –  Generate all plots for transaction-level benchmarks T1–T6.

Usage:
    python3 scripts/txn/plot_txn.py --input results/txn/<TS>/results.csv
    python3 scripts/txn/plot_txn.py --input results/txn/<TS>/results.csv --experiment T5
    python3 scripts/txn/plot_txn.py --input results/txn/<TS>/results.csv --outdir custom/dir

Figures produced (all saved as PDF under <outdir>/):
  T1  fig_t1_workload.pdf              throughput by workload type
  T2  fig_t2_mechanism.pdf             throughput by mechanism (bar)
  T3  fig_t3_violation_rate.pdf        throughput vs violation rate
  T4  fig_t4_duplicate_rate.pdf        throughput vs duplicate rate
  T5  fig_t5_threads.pdf               throughput vs thread count (log2 x-axis)
  T6  fig_t6_access_pattern.pdf        throughput by access pattern (bar)
"""

import sys
import os
import argparse

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "common"))
from result_utils import (
    apply_thesis_style, save_fig, load_txn_csv, aggregate_iterations,
    mechanisms_in, plot_lines,
    MECHANISM_ORDER, MECHANISM_LABEL, MECHANISM_COLORS, MECHANISM_MARKERS,
    WORKLOAD_ORDER, WORKLOAD_LABEL,
    FIG_W_FULL, FIG_W_SINGLE, FIG_H_DEFAULT,
    format_size, throughput_to_mops, throughput_label,
)

apply_thesis_style()

# Canonical defaults (must match config.sh CANONICAL_* values)
CANONICAL = {
    "workload":        "mixed",
    "mechanism":       "od_multi",
    "violation_rate":  0.0,
    "duplicate_rate":  0.1,
    "size":            10_000_000,
    "access_pattern":  "random",
    "threads":         2,
}

SIZE_LABELS = {100_000: "100K", 1_000_000: "1M", 10_000_000: "10M", 100_000_000: "100M"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fc(df, **kwargs):
    """Filter df by exact column=value matches."""
    mask = np.ones(len(df), dtype=bool)
    for col, val in kwargs.items():
        if col not in df.columns:
            continue
        if isinstance(val, float):
            mask &= (df[col] - val).abs() < 1e-9
        else:
            mask &= df[col] == val
    return df[mask]


def size_fmt(v, _):
    return SIZE_LABELS.get(int(v), str(int(v)))


def pct_fmt(v, _):
    return f"{int(round(v * 100))}%"


def thr_fmt(v, _):
    return str(int(v))


def bar_mechs(ax, agg, mechs, x_labels, ylabel, title):
    """Grouped bar chart: one group per x label, one bar per mechanism."""
    n_groups = len(x_labels)
    n_bars   = len(mechs)
    width    = 0.8 / max(n_bars, 1)
    x        = np.arange(n_groups)

    for i, mech in enumerate(mechs):
        sub = agg[agg["mechanism"] == mech]
        vals = []
        errs = []
        for lbl in x_labels:
            row = sub[sub["_x"] == lbl]
            if row.empty:
                vals.append(0.0); errs.append(0.0)
            else:
                vals.append(throughput_to_mops(row["mean"].values[0]))
                errs.append(throughput_to_mops(row["se"].values[0]))
        offset = (i - n_bars / 2 + 0.5) * width
        ax.bar(x + offset, vals, width, yerr=errs,
               label=MECHANISM_LABEL.get(mech, mech),
               color=MECHANISM_COLORS.get(mech, None),
               capsize=3, edgecolor="black", linewidth=0.4)

    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=15, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(loc="best")


# ---------------------------------------------------------------------------
# T1 – Workload type  (bar: one group per workload, one bar per mechanism)
# ---------------------------------------------------------------------------

def plot_t1(df, outdir):
    raw = fc(df,
             violation_rate=CANONICAL["violation_rate"],
             duplicate_rate=CANONICAL["duplicate_rate"],
             size=CANONICAL["size"],
             access_pattern=CANONICAL["access_pattern"],
             threads=CANONICAL["threads"])
    raw = df[df["experiment"] == "T1"] if raw.empty else raw
    if raw.empty:
        return
    agg = aggregate_iterations(raw, ["workload", "mechanism"])
    mechs    = mechanisms_in(agg)
    workloads = [w for w in WORKLOAD_ORDER if w in agg["workload"].unique()]
    agg["_x"] = agg["workload"].map(WORKLOAD_LABEL)
    x_labels = [WORKLOAD_LABEL[w] for w in workloads]

    fig, ax = plt.subplots(figsize=(FIG_W_FULL, FIG_H_DEFAULT))
    bar_mechs(ax, agg, mechs, x_labels,
              ylabel=throughput_label(), title="T1 – Throughput by workload type")
    save_fig(fig, os.path.join(outdir, "fig_t1_workload.pdf"))


# ---------------------------------------------------------------------------
# T2 – Mechanism  (horizontal bar, one bar per mechanism)
# ---------------------------------------------------------------------------

def plot_t2(df, outdir):
    raw = fc(df,
             workload=CANONICAL["workload"],
             violation_rate=CANONICAL["violation_rate"],
             duplicate_rate=CANONICAL["duplicate_rate"],
             size=CANONICAL["size"],
             access_pattern=CANONICAL["access_pattern"],
             threads=CANONICAL["threads"])
    raw = df[df["experiment"] == "T2"] if raw.empty else raw
    if raw.empty:
        return
    agg  = aggregate_iterations(raw, ["mechanism"])
    mechs = mechanisms_in(agg)

    y     = np.arange(len(mechs))
    vals  = [throughput_to_mops(agg.loc[agg["mechanism"] == m, "mean"].values[0])
             if m in agg["mechanism"].values else 0.0 for m in mechs]
    errs  = [throughput_to_mops(agg.loc[agg["mechanism"] == m, "se"].values[0])
             if m in agg["mechanism"].values else 0.0 for m in mechs]
    colors = [MECHANISM_COLORS.get(m, "#888") for m in mechs]

    fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
    ax.barh(y, vals, xerr=errs, color=colors, capsize=3,
            edgecolor="black", linewidth=0.4)
    ax.set_yticks(y)
    ax.set_yticklabels([MECHANISM_LABEL.get(m, m) for m in mechs])
    ax.set_xlabel(throughput_label())
    ax.set_title("T2 – Throughput by validation mechanism")
    save_fig(fig, os.path.join(outdir, "fig_t2_mechanism.pdf"))


# ---------------------------------------------------------------------------
# T3 – Violation rate  (line per mechanism)
# ---------------------------------------------------------------------------

def plot_t3(df, outdir):
    raw = fc(df,
             workload=CANONICAL["workload"],
             duplicate_rate=CANONICAL["duplicate_rate"],
             size=CANONICAL["size"],
             access_pattern=CANONICAL["access_pattern"],
             threads=CANONICAL["threads"])
    raw = df[df["experiment"] == "T3"] if raw.empty else raw
    if raw.empty:
        return
    agg = aggregate_iterations(raw, ["mechanism", "violation_rate"])

    fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
    plot_lines(ax, agg, x_col="violation_rate", y_to_mops=True,
               x_label="Violation rate", y_label=throughput_label())
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.set_title("T3 – Throughput vs. violation rate")
    ax.legend(loc="best")
    save_fig(fig, os.path.join(outdir, "fig_t3_violation_rate.pdf"))


# ---------------------------------------------------------------------------
# T4 – Duplicate rate  (line per mechanism)
# ---------------------------------------------------------------------------

def plot_t4(df, outdir):
    raw = fc(df,
             workload=CANONICAL["workload"],
             violation_rate=0.0,
             size=CANONICAL["size"],
             access_pattern=CANONICAL["access_pattern"],
             threads=CANONICAL["threads"])
    raw = df[df["experiment"] == "T4"] if raw.empty else raw
    if raw.empty:
        return
    agg = aggregate_iterations(raw, ["mechanism", "duplicate_rate"])

    fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
    plot_lines(ax, agg, x_col="duplicate_rate", y_to_mops=True,
               x_label="Duplicate rate", y_label=throughput_label())
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_fmt))
    ax.set_title("T4 – Throughput vs. duplicate rate")
    ax.legend(loc="best")
    save_fig(fig, os.path.join(outdir, "fig_t4_duplicate_rate.pdf"))


# ---------------------------------------------------------------------------
# T5 – Thread count  (line per mechanism, log2 x-axis)
# ---------------------------------------------------------------------------

def plot_t5(df, outdir):
    raw = fc(df,
             workload=CANONICAL["workload"],
             violation_rate=CANONICAL["violation_rate"],
             duplicate_rate=CANONICAL["duplicate_rate"],
             size=CANONICAL["size"],
             access_pattern=CANONICAL["access_pattern"])
    raw = df[df["experiment"] == "T5"] if raw.empty else raw
    if raw.empty:
        return
    agg = aggregate_iterations(raw, ["mechanism", "threads"])

    fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
    plot_lines(ax, agg, x_col="threads", y_to_mops=True,
               x_label="Threads", y_label=throughput_label())
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(thr_fmt))
    ax.set_title("T5 – Throughput vs. thread count")
    ax.legend(loc="best")
    save_fig(fig, os.path.join(outdir, "fig_t5_threads.pdf"))


# ---------------------------------------------------------------------------
# T6 – Access pattern  (grouped bar: random vs sequential)
# ---------------------------------------------------------------------------

def plot_t6(df, outdir):
    raw = fc(df,
             workload=CANONICAL["workload"],
             violation_rate=CANONICAL["violation_rate"],
             duplicate_rate=CANONICAL["duplicate_rate"],
             size=CANONICAL["size"],
             threads=CANONICAL["threads"])
    raw = df[df["experiment"] == "T6"] if raw.empty else raw
    if raw.empty:
        return
    agg = aggregate_iterations(raw, ["mechanism", "access_pattern"])
    mechs    = mechanisms_in(agg)
    patterns = [p for p in ["random", "sequential"] if p in agg["access_pattern"].unique()]
    agg["_x"] = agg["access_pattern"]

    fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
    bar_mechs(ax, agg, mechs, patterns,
              ylabel=throughput_label(), title="T6 – Throughput by access pattern")
    save_fig(fig, os.path.join(outdir, "fig_t6_access_pattern.pdf"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

EXPERIMENT_PLOTTERS = {
    "T1": plot_t1,
    "T2": plot_t2,
    "T3": plot_t3,
    "T4": plot_t4,
    "T5": plot_t5,
    "T6": plot_t6,
}


def main():
    parser = argparse.ArgumentParser(description="Plot txn benchmark results")
    parser.add_argument("--input",      required=True, help="Path to merged results.csv")
    parser.add_argument("--outdir",     default=None,  help="Output dir (default: <input_dir>/plots/)")
    parser.add_argument("--experiment", default="all", help="T1..T6 or 'all' (default: all)")
    args = parser.parse_args()

    df = load_txn_csv(args.input)
    print(f"Loaded {len(df)} rows from {args.input}")

    outdir = args.outdir or os.path.join(os.path.dirname(args.input), "plots")
    os.makedirs(outdir, exist_ok=True)
    print(f"Output dir: {outdir}")

    to_plot = list(EXPERIMENT_PLOTTERS.keys()) if args.experiment == "all" \
              else [e.upper() for e in args.experiment.split(",")]

    for exp in to_plot:
        if exp not in EXPERIMENT_PLOTTERS:
            print(f"  WARN: Unknown experiment '{exp}', skipping")
            continue
        print(f"Generating {exp} …")
        EXPERIMENT_PLOTTERS[exp](df, outdir)

    print(f"\nAll figures saved to: {outdir}")


if __name__ == "__main__":
    main()