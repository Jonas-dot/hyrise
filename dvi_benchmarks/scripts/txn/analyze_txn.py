#!/usr/bin/env python3
"""
analyze_txn.py  –  Merge per-run txn JSON/CSV files into a single unified CSV.

Usage:
    python3 scripts/txn/analyze_txn.py --indir results/txn/<TIMESTAMP>
    python3 scripts/txn/analyze_txn.py --indir results/txn/<TIMESTAMP> --output results.csv

The script walks the directory tree, reads every *.csv emitted by run_txn.sh,
and writes a merged results.csv with the unified schema:

    experiment,workload,mechanism,violation_rate,det_ratio,
    size,access_pattern,threads,iteration,time_ms,throughput_ops_per_s
"""

import sys
import os
import argparse
import glob

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "common"))
from result_utils import TXN_COLUMNS

REQUIRED_COLS = set(TXN_COLUMNS)


def parse_filename_fallback(path: str) -> dict:
    """
    Extract metadata from the filename when the CSV lacks some columns.
    Filename format: <workload>_<mechanism>_v<vrate>_d<dratio>_s<size>_<pattern>_t<threads>.csv
    """
    base = os.path.splitext(os.path.basename(path))[0]
    meta = {}
    try:
        parts = base.split("_")
        # Workload is first token (may have underscores, e.g. insert_only)
        # We rely on known prefixes to parse
        for i, p in enumerate(parts):
            if p.startswith("v") and p[1:].replace(".", "").isdigit():
                meta["violation_rate"] = float(p[1:])
            elif p.startswith("d") and p[1:].replace(".", "").isdigit():
                meta["det_ratio"] = float(p[1:])
            elif p.startswith("s") and p[1:].isdigit():
                meta["size"] = int(p[1:])
            elif p.startswith("t") and p[1:].isdigit():
                meta["threads"] = int(p[1:])
    except Exception:
        pass
    return meta


def load_csv(path: str, experiment: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"  WARN: could not read {path}: {e}")
        return pd.DataFrame()

    if df.empty:
        return df

    # Inject experiment column if missing
    if "experiment" not in df.columns:
        df["experiment"] = experiment

    # Fill missing metadata from filename
    fallback = parse_filename_fallback(path)
    for col, val in fallback.items():
        if col not in df.columns:
            df[col] = val

    # Ensure all required columns exist (with NaN if truly absent)
    for col in TXN_COLUMNS:
        if col not in df.columns:
            df[col] = float("nan")

    return df[TXN_COLUMNS]


def main():
    parser = argparse.ArgumentParser(description="Merge txn benchmark CSVs")
    parser.add_argument("--indir",  required=True, help="Root directory from run_txn.sh")
    parser.add_argument("--output", default=None,  help="Output CSV path (default: <indir>/results.csv)")
    args = parser.parse_args()

    indir = args.indir
    output = args.output or os.path.join(indir, "results.csv")

    # Discover per-experiment CSVs
    csv_files = sorted(glob.glob(os.path.join(indir, "**", "results.csv"), recursive=True))
    # Exclude the merged output file if it already exists inside indir
    csv_files = [f for f in csv_files if os.path.abspath(f) != os.path.abspath(output)]

    if not csv_files:
        print(f"No results.csv files found under {indir}")
        sys.exit(1)

    print(f"Found {len(csv_files)} per-experiment CSV(s):")
    frames = []
    for f in csv_files:
        # Infer experiment from parent directory name (e.g. .../T6/results.csv → T6)
        exp = os.path.basename(os.path.dirname(f))
        print(f"  {f}  (experiment={exp})")
        df = load_csv(f, exp)
        if not df.empty:
            frames.append(df)

    if not frames:
        print("ERROR: No valid data found.")
        sys.exit(1)

    merged = pd.concat(frames, ignore_index=True)

    # Coerce numeric columns
    for col in ["violation_rate", "det_ratio", "size", "threads",
                "iteration", "time_ms", "throughput_ops_per_s"]:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")

    merged.to_csv(output, index=False)
    print(f"\nMerged {len(merged)} rows → {output}")
    print("\nExperiments found:")
    if "experiment" in merged.columns:
        for exp, cnt in merged.groupby("experiment").size().items():
            print(f"  {exp}: {cnt} rows")


if __name__ == "__main__":
    main()