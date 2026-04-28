#!/usr/bin/env python3
"""
plot_micro.py  –  Generate all plots for micro-benchmarks M0 and M1.

Usage:
    python3 scripts/micro/plot_micro.py --input results/micro/<TS>/results.csv
    python3 scripts/micro/plot_micro.py --input results/micro/<TS>/results.csv --experiment M1
    python3 scripts/micro/plot_micro.py --input results/micro/<TS>/results.csv --outdir custom/dir

Produces the following figure sets (saved under <outdir>/):
  M0 figures:
    fig1_throughput_vs_threads_<op>_<pattern>.pdf     Q1: scaling
    fig2_throughput_vs_size_<op>_<pattern>.pdf         Q2: dataset size
    fig3_overhead_vs_violation_rate_<op>.pdf           Q3: violation impact
    fig4_overhead_vs_det_ratio_<op>.pdf                Q4: determinant ratio
    fig5_overhead_bar_<op>_<pattern>.pdf               Q5: overhead summary

  M1 figures (OD comparison):
    fig_od_vs_fd_<op>_<pattern>.pdf
"""

import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "common"))
from result_utils import (
    apply_thesis_style, save_fig, load_micro_csv, aggregate_iterations,
    mechanisms_in, plot_lines, compute_overhead,
    MECHANISM_ORDER, MECHANISM_LABEL, MECHANISM_COLORS,
    OPERATION_ORDER, OPERATION_LABEL,
    FIG_W_FULL, FIG_W_SINGLE, FIG_H_DEFAULT,
    format_size, throughput_to_mops, throughput_label,
)

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

apply_thesis_style()

# ---------------------------------------------------------------------------
# Canonical parameter defaults (matching config.sh)
# ---------------------------------------------------------------------------
CANONICAL = {
    "violation_rate": 0.0,
    "det_ratio":      0.9,
    "size":           1_000_000,
    "threads":        1,
    "access_pattern": "random",
}

SIZE_LABELS = {100_000: "100K", 1_000_000: "1M", 10_000_000: "10M", 100_000_000: "100M"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def filter_canonical(df, fix: dict):
    """Filter df to rows matching all key=value pairs in fix."""
    mask = np.ones(len(df), dtype=bool)
    for col, val in fix.items():
        if col in df.columns:
            if isinstance(val, float):
                mask &= (df[col] - val).abs() < 1e-9
            else:
                mask &= df[col] == val
    return df[mask]


def size_formatter(val, _pos):
    return SIZE_LABELS.get(int(val), str(int(val)))


def pct_formatter(val, _pos):
    return f"{int(round(val * 100))}%"


def thread_formatter(val, _pos):
    return str(int(val))


# ---------------------------------------------------------------------------
# Figure 1 – Throughput vs. Threads  (scaling, Q1)
#   Fixed: size=canonical, det_ratio=canonical, violation_rate=canonical
#   X: threads  Y: throughput  Hue: mechanism
# ---------------------------------------------------------------------------

def fig1_scaling(df, operations, patterns, outdir, experiment):
    group_cols = ["mechanism", "threads", "operation", "access_pattern"]
    agg = aggregate_iterations(df, group_cols)

    for op in operations:
        for pat in patterns:
            sub = filter_canonical(
                agg[agg["operation"] == op][agg["access_pattern"] == pat],
                {"violation_rate": CANONICAL["violation_rate"],
                 "det_ratio":      CANONICAL["det_ratio"],
                 "size":           CANONICAL["size"]},
            ) if "violation_rate" in df.columns else agg[
                (agg["operation"] == op) & (agg["access_pattern"] == pat)
            ]
            # Re-filter from raw df for columns not in agg
            raw_sub = filter_canonical(
                df[(df["operation"] == op) & (df["access_pattern"] == pat)],
                {"violation_rate": CANONICAL["violation_rate"],
                 "det_ratio":      CANONICAL["det_ratio"],
                 "size":           CANONICAL["size"]},
            )
            if raw_sub.empty:
                continue
            sub_agg = aggregate_iterations(raw_sub, ["mechanism", "threads"])

            fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
            plot_lines(ax, sub_agg, x_col="threads", y_to_mops=True,
                       x_label="Threads", y_label=throughput_label())
            ax.set_xscale("log", base=2)
            ax.xaxis.set_major_formatter(mticker.FuncFormatter(thread_formatter))
            ax.set_title(f"{OPERATION_LABEL[op]} – {pat.capitalize()} access")
            ax.legend(loc="best")
            fname = f"fig1_scaling_{op}_{pat}.pdf"
            save_fig(fig, os.path.join(outdir, fname))


# ---------------------------------------------------------------------------
# Figure 2 – Throughput vs. Dataset Size  (Q2)
#   Fixed: threads=canonical, det_ratio=canonical, violation_rate=canonical
# ---------------------------------------------------------------------------

def fig2_size(df, operations, patterns, outdir, experiment):
    for op in operations:
        for pat in patterns:
            raw_sub = filter_canonical(
                df[(df["operation"] == op) & (df["access_pattern"] == pat)],
                {"violation_rate": CANONICAL["violation_rate"],
                 "det_ratio":      CANONICAL["det_ratio"],
                 "threads":        CANONICAL["threads"]},
            )
            if raw_sub.empty:
                continue
            sub_agg = aggregate_iterations(raw_sub, ["mechanism", "size"])

            fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
            plot_lines(ax, sub_agg, x_col="size", y_to_mops=True,
                       x_label="Dataset size (operations)", y_label=throughput_label())
            ax.set_xscale("log")
            ax.xaxis.set_major_formatter(mticker.FuncFormatter(size_formatter))
            ax.set_title(f"{OPERATION_LABEL[op]} – {pat.capitalize()} access")
            ax.legend(loc="best")
            fname = f"fig2_size_{op}_{pat}.pdf"
            save_fig(fig, os.path.join(outdir, fname))


# ---------------------------------------------------------------------------
# Figure 3 – Throughput vs. Violation Rate  (Q3)
#   Fixed: threads=canonical, det_ratio=canonical, size=canonical
# ---------------------------------------------------------------------------

def fig3_violation(df, operations, patterns, outdir, experiment):
    for op in operations:
        raw_sub = filter_canonical(
            df[(df["operation"] == op)],
            {"det_ratio":      CANONICAL["det_ratio"],
             "size":           CANONICAL["size"],
             "threads":        CANONICAL["threads"],
             "access_pattern": CANONICAL["access_pattern"]},
        )
        if raw_sub.empty:
            continue
        sub_agg = aggregate_iterations(raw_sub, ["mechanism", "violation_rate"])

        fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
        plot_lines(ax, sub_agg, x_col="violation_rate", y_to_mops=True,
                   x_label="Violation rate", y_label=throughput_label())
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_formatter))
        ax.set_title(f"{OPERATION_LABEL[op]} – violation rate impact")
        ax.legend(loc="best")
        fname = f"fig3_violation_{op}.pdf"
        save_fig(fig, os.path.join(outdir, fname))


# ---------------------------------------------------------------------------
# Figure 4 – Throughput vs. Determinant Ratio  (Q4)
#   Fixed: threads=canonical, violation_rate=0, size=canonical
# ---------------------------------------------------------------------------

def fig4_det_ratio(df, operations, outdir, experiment):
    for op in operations:
        raw_sub = filter_canonical(
            df[(df["operation"] == op)],
            {"violation_rate": 0.0,
             "size":           CANONICAL["size"],
             "threads":        CANONICAL["threads"],
             "access_pattern": CANONICAL["access_pattern"]},
        )
        if raw_sub.empty:
            continue
        sub_agg = aggregate_iterations(raw_sub, ["mechanism", "duplicate_rate"])

        fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
        plot_lines(ax, sub_agg, x_col="duplicate_rate", y_to_mops=True,
                   x_label="Determinant ratio", y_label=throughput_label())
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(pct_formatter))
        ax.set_title(f"{OPERATION_LABEL[op]} – determinant ratio impact")
        ax.legend(loc="best")
        fname = f"fig4_det_ratio_{op}.pdf"
        save_fig(fig, os.path.join(outdir, fname))


# ---------------------------------------------------------------------------
# Figure 5 – Overhead bar chart  (Q5)
#   Shows (throughput_mech - throughput_off) / throughput_off * 100
#   Fixed: threads=1 and threads=64, canonical viol/det/size/pattern
# ---------------------------------------------------------------------------

def fig5_overhead(df, operations, outdir, experiment):
    thread_pairs = [1, 64]
    for op in operations:
        for t in thread_pairs:
            raw_sub = filter_canonical(
                df[(df["operation"] == op)],
                {"violation_rate": CANONICAL["violation_rate"],
                 "det_ratio":      CANONICAL["det_ratio"],
                 "size":           CANONICAL["size"],
                 "access_pattern": CANONICAL["access_pattern"],
                 "threads":        t},
            )
            if raw_sub.empty:
                continue
            sub_agg = aggregate_iterations(raw_sub, ["mechanism"])
            mechs = mechanisms_in(sub_agg)
            if "off" not in mechs:
                continue

            base_val = sub_agg.loc[sub_agg["mechanism"] == "off", "mean"].values[0]
            overhead = sub_agg.copy()
            overhead["overhead_pct"] = (overhead["mean"] - base_val) / base_val * 100.0
            overhead = overhead[overhead["mechanism"] != "off"]

            fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
            x = np.arange(len(overhead))
            bars = ax.bar(
                x,
                overhead["overhead_pct"],
                color=[MECHANISM_COLORS.get(m, "#888888") for m in overhead["mechanism"]],
                edgecolor="black",
                linewidth=0.5,
            )
            ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
            ax.set_xticks(x)
            ax.set_xticklabels(
                [MECHANISM_LABEL.get(m, m) for m in overhead["mechanism"]],
                rotation=20, ha="right",
            )
            ax.set_ylabel("Throughput overhead vs. Off (%)")
            ax.set_title(f"{OPERATION_LABEL[op]} – overhead ({t} thread{'s' if t > 1 else ''})")
            fname = f"fig5_overhead_{op}_t{t}.pdf"
            save_fig(fig, os.path.join(outdir, fname))


# ---------------------------------------------------------------------------
# M1 – OD vs. FD comparison
# ---------------------------------------------------------------------------

def fig_m1_od(df, operations, patterns, outdir):
    group_cols = ["mechanism", "threads", "operation", "access_pattern"]
    for op in operations:
        for pat in patterns:
            raw_sub = df[(df["operation"] == op) & (df["access_pattern"] == pat)]
            if raw_sub.empty:
                continue
            sub_agg = aggregate_iterations(raw_sub, ["mechanism", "threads"])

            fig, ax = plt.subplots(figsize=(FIG_W_SINGLE, FIG_H_DEFAULT))
            plot_lines(ax, sub_agg, x_col="threads", y_to_mops=True,
                       x_label="Threads", y_label=throughput_label())
            ax.set_xscale("log", base=2)
            ax.xaxis.set_major_formatter(mticker.FuncFormatter(thread_formatter))
            ax.set_title(f"OD mechanisms – {OPERATION_LABEL[op]} – {pat.capitalize()}")
            ax.legend(loc="best")
            fname = f"fig_m1_od_{op}_{pat}.pdf"
            save_fig(fig, os.path.join(outdir, fname))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Plot micro-benchmark results")
    parser.add_argument("--input",      required=True,  help="Path to results.csv")
    parser.add_argument("--outdir",     default=None,   help="Output directory for figures (default: <input_dir>/plots/)")
    parser.add_argument("--experiment", default=None,   help="M0 or M1 (inferred from CSV if omitted)")
    args = parser.parse_args()

    df = load_micro_csv(args.input)
    print(f"Loaded {len(df)} rows from {args.input}")

    if args.outdir:
        outdir = args.outdir
    else:
        outdir = os.path.join(os.path.dirname(args.input), "plots")
    os.makedirs(outdir, exist_ok=True)

    experiment = args.experiment
    if experiment is None:
        experiments = df["experiment"].unique()
        experiment = experiments[0] if len(experiments) == 1 else "M0"
    print(f"Experiment: {experiment}")
    print(f"Output dir: {outdir}")

    df_exp = df[df["experiment"] == experiment]
    operations = [o for o in OPERATION_ORDER if o in df_exp["operation"].unique()]
    patterns   = [p for p in ["random", "sequential"] if p in df_exp["access_pattern"].unique()]

    if experiment == "M1":
        fig_m1_od(df_exp, operations, patterns, outdir)
    else:
        # M0: all five figures
        print("Generating Fig 1: Throughput vs. Threads …")
        fig1_scaling(df_exp, operations, patterns, outdir, experiment)

        print("Generating Fig 2: Throughput vs. Dataset Size …")
        fig2_size(df_exp, operations, patterns, outdir, experiment)

        print("Generating Fig 3: Throughput vs. Violation Rate …")
        fig3_violation(df_exp, operations, patterns, outdir, experiment)

        print("Generating Fig 4: Throughput vs. Determinant Ratio …")
        fig4_det_ratio(df_exp, operations, outdir, experiment)

        print("Generating Fig 5: Overhead Bar Chart …")
        fig5_overhead(df_exp, operations, outdir, experiment)

    print(f"\nAll figures saved to: {outdir}")


if __name__ == "__main__":
    main()