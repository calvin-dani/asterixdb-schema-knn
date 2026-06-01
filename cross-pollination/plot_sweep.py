#!/usr/bin/env python3
"""
Plot the cross-pollination × epsilon sweep CSV.
Each metric → one line per M, ε on the x-axis.

Usage:  python3 plot_sweep.py [csv_path]
Default csv: sweep_xpoll_eps_4x4_2026-06-01.csv (this directory)
Outputs PNGs into the same directory.
"""
import csv, os, sys
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
CSV  = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "sweep_xpoll_eps_4x4_2026-06-01.csv")
TAG  = os.path.splitext(os.path.basename(CSV))[0]

# Read CSV → rows[M][ε] = dict(metrics)
rows = defaultdict(dict)
with open(CSV) as f:
    for row in csv.DictReader(f):
        M = int(row["M"])
        e = float(row["epsilon"])
        rows[M][e] = {k: float(v) for k, v in row.items()}

M_values = sorted(rows.keys())
eps_values = sorted({e for d in rows.values() for e in d.keys()})

# Metric → (column key, axis title, y-format)
METRICS = [
    ("recall",          "Recall@100",            lambda y: f"{y:.2f}"),
    ("distComps",       "Distance computations\n(sum over 4 partitions)", lambda y: f"{int(y)}"),
    ("navPagePins",     "Nav-phase page pins\n(sum over 4 partitions)",   lambda y: f"{int(y)}"),
    ("clustersProbed",  "Clusters probed\n(sum over 4 partitions)",        lambda y: f"{int(y)}"),
]

# Colors per M
M_COLORS = {1: "#888888", 2: "#1f77b4", 4: "#2ca02c", 8: "#d62728"}
M_MARKERS = {1: "o", 2: "s", 4: "^", 8: "D"}

def plot_one(ax, col_key, title, fmt):
    for M in M_values:
        xs = [e for e in eps_values if e in rows[M]]
        ys = [rows[M][e][col_key] for e in xs]
        ax.plot(xs, ys, marker=M_MARKERS.get(M, "o"), color=M_COLORS.get(M, "black"),
                linewidth=2, markersize=7, label=f"M={M}")
    ax.set_xlabel("epsilon (ε)")
    ax.set_ylabel(title)
    ax.set_title(title.split("\n")[0])
    ax.grid(True, alpha=0.3)
    ax.set_xticks(eps_values)
    ax.legend(loc="best", framealpha=0.9)

# 1) Individual PNG per metric
for col_key, title, fmt in METRICS:
    fig, ax = plt.subplots(figsize=(7, 5))
    plot_one(ax, col_key, title, fmt)
    out = os.path.join(HERE, f"plot_{col_key}_{TAG}.png")
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print(f"wrote {out}")

# 2) Combined 2×2 panel
fig, axes = plt.subplots(2, 2, figsize=(13, 9))
for ax, (col_key, title, fmt) in zip(axes.flat, METRICS):
    plot_one(ax, col_key, title, fmt)
fig.suptitle(f"Cross-pollination × ε sweep — {TAG}", fontsize=13, y=0.99)
fig.tight_layout()
panel_path = os.path.join(HERE, f"plot_panel_{TAG}.png")
fig.savefig(panel_path, dpi=140)
plt.close(fig)
print(f"wrote {panel_path}")
