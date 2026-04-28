#!/usr/bin/env python3
"""Shared matplotlib style for thesis benchmark figures."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ThesisPalette:
    baseline: str = "#1F1F1F"
    fd: str = "#0072B2"
    od: str = "#D55E00"
    fd_multicol: str = "#009E73"
    od_multicol: str = "#CC79A7"


def apply_thesis_style(plt_module) -> None:
    """Apply consistent, publication-friendly plotting defaults."""
    plt_module.style.use("seaborn-v0_8-whitegrid")
    plt_module.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["DejaVu Serif", "Times New Roman", "Times"],
            "axes.titlesize": 12,
            "axes.labelsize": 11,
            "xtick.labelsize": 10,
            "ytick.labelsize": 10,
            "legend.fontsize": 10,
            "figure.dpi": 150,
            "savefig.dpi": 300,
            "axes.grid": True,
            "grid.alpha": 0.35,
            "grid.linestyle": ":",
            "axes.spines.top": False,
            "axes.spines.right": False,
        }
    )


def default_palette() -> ThesisPalette:
    return ThesisPalette()
