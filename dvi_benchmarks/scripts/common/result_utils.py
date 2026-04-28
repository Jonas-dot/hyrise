"""
result_utils.py  –  Shared utilities for all benchmark analysis and plotting.

Import with:
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'common'))
    from result_utils import *
"""

import os
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

matplotlib.use("Agg")

# ---------------------------------------------------------------------------
# Canonical mechanism ordering and display
# ---------------------------------------------------------------------------

MECHANISM_ORDER = ["off", "fd_single", "fd_multi", "od_single", "od_multi"]

MECHANISM_LABEL = {
    "off": "Off",
    "fd_single": "FD (single-col)",
    "fd_multi": "FD (multi-col)",
    "od_single": "OD (single-col)",
    "od_multi": "OD (multi-col)",
}

MECHANISM_COLORS = {
    "off": "#555555",
    "fd_single": "#1f77b4",
    "fd_multi": "#aec7e8",
    "od_single": "#d62728",
    "od_multi": "#f7b6d2",
}

MECHANISM_MARKERS = {
    "off": "s",
    "fd_single": "o",
    "fd_multi": "^",
    "od_single": "D",
    "od_multi": "v",
}

MECHANISM_LINESTYLE = {
    "off": "--",
    "fd_single": "-",
    "fd_multi": "-.",
    "od_single": "-",
    "od_multi": "-.",
}

# ---------------------------------------------------------------------------
# Canonical operation ordering and display
# ---------------------------------------------------------------------------

OPERATION_ORDER = ["insert", "lookup", "delete"]

OPERATION_LABEL = {
    "insert": "Insert",
    "lookup": "Lookup",
    "delete": "Delete",
}

# ---------------------------------------------------------------------------
# Workload labels (txn)
# ---------------------------------------------------------------------------

WORKLOAD_ORDER = ["insert_only", "update_only", "delete_only", "mixed"]

WORKLOAD_LABEL = {
    "insert_only": "Insert-only",
    "update_only": "Update-only",
    "delete_only": "Delete-only",
    "mixed": "Mixed (70/20/10)",
}

# ---------------------------------------------------------------------------
# Thesis plot style
# ---------------------------------------------------------------------------

# Consistent figure dimensions (width x height in inches)
FIG_W_SINGLE = 5.0  # single-column figure
FIG_W_FULL = 9.5  # full text width (two-column layout)
FIG_H_DEFAULT = 3.5

THESIS_RC = {
    "font.family": "serif",
    "font.size": 10,
    "axes.titlesize": 10,
    "axes.labelsize": 9,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.fontsize": 8,
    "figure.dpi": 150,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
    "lines.linewidth": 1.5,
    "lines.markersize": 5,
    "axes.spines.top": False,
    "axes.spines.right": False,
}


def apply_thesis_style():
    """Call once before creating any figures."""
    matplotlib.rcParams.update(THESIS_RC)


def save_fig(fig, path, tight=True):
    """Save figure; create parent directories if needed."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if tight:
        fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------

MICRO_COLUMNS = [
    "experiment",
    "operation",
    "mechanism",
    "violation_rate",
    "duplicate_rate",
    "size",
    "access_pattern",
    "threads",
    "iteration",
    "time_ms",
    "throughput_ops_per_s",
]

TXN_COLUMNS = [
    "experiment",
    "workload",
    "mechanism",
    "violation_rate",
    "duplicate_rate",
    "size",
    "access_pattern",
    "threads",
    "iteration",
    "time_ms",
    "throughput_ops_per_s",
]


def load_micro_csv(path):
    df = pd.read_csv(path)
    # Legacy: rename det_ratio -> duplicate_rate if present
    if "det_ratio" in df.columns and "duplicate_rate" not in df.columns:
        df = df.rename(columns={"det_ratio": "duplicate_rate"})
    for col in ["violation_rate", "duplicate_rate", "throughput_ops_per_s", "time_ms"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def load_txn_csv(path):
    df = pd.read_csv(path)

    # Normalize legacy/naive schema to the canonical txn schema expected by plotting.
    # naive_txn CSVs use ops_per_s + batch_ms (+ batch_end) instead of
    # throughput_ops_per_s + time_ms (+ iteration).
    if "throughput_ops_per_s" not in df.columns and "ops_per_s" in df.columns:
        df = df.rename(columns={"ops_per_s": "throughput_ops_per_s"})
    if "time_ms" not in df.columns and "batch_ms" in df.columns:
        df = df.rename(columns={"batch_ms": "time_ms"})
    if "iteration" not in df.columns and "batch_end" in df.columns:
        # Fallback for older exports without explicit iteration.
        df = df.rename(columns={"batch_end": "iteration"})
    # Legacy: rename det_ratio -> duplicate_rate if present
    if "det_ratio" in df.columns and "duplicate_rate" not in df.columns:
        df = df.rename(columns={"det_ratio": "duplicate_rate"})

    for col in ["violation_rate", "duplicate_rate", "throughput_ops_per_s", "time_ms"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def aggregate_iterations(df, group_cols, value_col="throughput_ops_per_s"):
    """Return mean ± std over iterations, per group."""
    agg = (
        df.groupby(group_cols)[value_col]
        .agg(mean="mean", std="std", count="count")
        .reset_index()
    )
    agg["se"] = agg["std"] / agg["count"].pow(0.5)
    return agg


# ---------------------------------------------------------------------------
# Axis helpers
# ---------------------------------------------------------------------------


def format_size(s):
    """100000 → '100K', 1000000 → '1M', 10000000 → '10M', 100000000 → '100M'."""
    if s >= 1_000_000:
        return f"{s // 1_000_000}M"
    elif s >= 1_000:
        return f"{s // 1_000}K"
    return str(s)


def throughput_label():
    return "Throughput (Mops/s)"


def throughput_to_mops(series):
    """Convert ops/s to Mops/s."""
    return series / 1e6


def mechanisms_in(df, col="mechanism"):
    """Return sorted list of mechanisms present in df, in canonical order."""
    present = set(df[col].unique())
    return [m for m in MECHANISM_ORDER if m in present]


# ---------------------------------------------------------------------------
# Common line-plot helper
# ---------------------------------------------------------------------------


def plot_lines(
    ax,
    df_agg,
    x_col,
    y_col="mean",
    hue_col="mechanism",
    hue_order=None,
    x_label=None,
    y_label=None,
    x_formatter=None,
    y_to_mops=True,
):
    """
    Draw one line per hue value onto ax.
    df_agg must have columns: [x_col, hue_col, y_col, 'se']
    """
    hue_vals = hue_order or mechanisms_in(df_agg, hue_col)

    for mech in hue_vals:
        sub = df_agg[df_agg[hue_col] == mech].sort_values(x_col)
        y = throughput_to_mops(sub[y_col]) if y_to_mops else sub[y_col]
        yerr = throughput_to_mops(sub["se"]) if y_to_mops else sub["se"]
        ax.errorbar(
            sub[x_col],
            y,
            yerr=yerr,
            label=MECHANISM_LABEL.get(mech, mech),
            color=MECHANISM_COLORS.get(mech, None),
            marker=MECHANISM_MARKERS.get(mech, "o"),
            linestyle=MECHANISM_LINESTYLE.get(mech, "-"),
        )

    if x_label:
        ax.set_xlabel(x_label)
    if y_label:
        ax.set_ylabel(y_label)
    elif y_to_mops:
        ax.set_ylabel(throughput_label())
    if x_formatter:
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(x_formatter))


# ---------------------------------------------------------------------------
# Overhead bar-chart helper
# ---------------------------------------------------------------------------


def compute_overhead(
    df_agg, baseline_mech="off", value_col="mean", mech_col="mechanism"
):
    """
    Compute relative overhead vs. baseline_mech.
    Returns df with extra column 'overhead_pct' = (mech - baseline) / baseline * 100.
    """
    base = df_agg[df_agg[mech_col] == baseline_mech][value_col].values
    if len(base) == 0:
        return df_agg.copy()
    base_val = base[0]
    df_out = df_agg.copy()
    df_out["overhead_pct"] = (df_out[value_col] - base_val) / base_val * 100.0
    return df_out
