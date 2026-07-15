#!/usr/bin/env python3
"""
Generate report plots for the movie pruning experiment.

Plots produced:
  #1 recall_vs_mpf.png      — recall as a function of min_probe_fraction (5 queries)
  #2 probed_vs_contributing.png — clusters probed vs distinct contributing per mpf
  #3 prune_gap_hist.png     — rGap and lGap distributions across all cluster probes
  #4 q3_cluster_stack.png   — top-100 cluster composition for q3 across mpf values
"""

import argparse
import json
import os
import re
from collections import Counter

import matplotlib.pyplot as plt
import numpy as np


RESULT_RE = re.compile(r"(\d{2}:\d{2}:\d{2}\.\d+).*\[RESULT\]\s+cid=(-?\d+)\s+dqx=(\S+)")
PRUNE_RE = re.compile(r"(\d{2}:\d{2}:\d{2}\.\d+).*\[PRUNE_STATS\]\s+cid=(-?\d+)")


def to_ms(t):
    h, m, s = t.split(":")
    return int(h) * 3600000 + int(m) * 60000 + int(float(s) * 1000)


def load_blocks(log_path, gap_ms=250):
    events = []
    with open(log_path, "r", errors="replace") as f:
        for line in f:
            m = RESULT_RE.search(line)
            if m:
                events.append((to_ms(m.group(1)), "R", int(m.group(2)), float(m.group(3))))
                continue
            m = PRUNE_RE.search(line)
            if m:
                events.append((to_ms(m.group(1)), "P", int(m.group(2))))
    events.sort()
    blocks, cur, last = [], {"results": [], "probes": []}, -10 ** 12
    for ev in events:
        if ev[0] - last > gap_ms and (cur["results"] or cur["probes"]):
            blocks.append(cur)
            cur = {"results": [], "probes": []}
        if ev[1] == "R":
            cur["results"].append((ev[2], ev[3]))
        else:
            cur["probes"].append(ev[2])
        last = ev[0]
    if cur["results"] or cur["probes"]:
        blocks.append(cur)
    return blocks


def plot_recall_vs_mpf(queries_json, out_path):
    qs = json.load(open(queries_json))["queries"]
    by_q = {}
    for r in qs:
        by_q.setdefault(r["query_id"], {})[r["min_probe_fraction"]] = r["recall"]
    mpfs = sorted({r["min_probe_fraction"] for r in qs})

    fig, ax = plt.subplots(figsize=(7, 4.5))
    per_mpf = {mpf: [] for mpf in mpfs}
    for qid, d in by_q.items():
        ys = [d.get(m) for m in mpfs]
        if None in ys:
            continue
        ax.plot(mpfs, ys, "-o", color="gray", alpha=0.45, linewidth=1.5, markersize=4)
        for mpf, y in zip(mpfs, ys):
            per_mpf[mpf].append(y)
        ax.text(mpfs[-1] + 0.005, ys[-1], f"q{qid}", fontsize=8, va="center")

    medians = [np.median(per_mpf[m]) for m in mpfs]
    ax.plot(mpfs, medians, "-D", color="#e03030", linewidth=2.5, markersize=8, label="median")
    ax.set_xlabel("min_probe_fraction")
    ax.set_ylabel("recall @ K=100")
    ax.set_ylim(0, 1.0)
    ax.set_title("Recall vs. min_probe_fraction (same 5 query vectors)")
    ax.set_xticks(mpfs)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"[done] wrote {out_path}")


def plot_probed_vs_contributing(queries_json, log_path, out_path, topk=100):
    qs = json.load(open(queries_json))["queries"]
    blocks = load_blocks(log_path)
    # Aggregate per mpf
    by_mpf = {}
    for idx, q in enumerate(qs):
        mpf = q["min_probe_fraction"]
        b = blocks[idx] if idx < len(blocks) else None
        if not b:
            continue
        probes = len(set(b["probes"]))
        contributing = len(Counter(c for c, _ in b["results"][:topk]))
        by_mpf.setdefault(mpf, {"probed": [], "contrib": []})
        by_mpf[mpf]["probed"].append(probes)
        by_mpf[mpf]["contrib"].append(contributing)

    mpfs = sorted(by_mpf.keys())
    probed_med = [np.median(by_mpf[m]["probed"]) for m in mpfs]
    contrib_med = [np.median(by_mpf[m]["contrib"]) for m in mpfs]
    probed_err = [(np.percentile(by_mpf[m]["probed"], 75) - np.percentile(by_mpf[m]["probed"], 25)) / 2
                  for m in mpfs]
    contrib_err = [(np.percentile(by_mpf[m]["contrib"], 75) - np.percentile(by_mpf[m]["contrib"], 25)) / 2
                   for m in mpfs]

    x = np.arange(len(mpfs))
    w = 0.38
    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.bar(x - w / 2, probed_med, w, yerr=probed_err, capsize=3,
           label="probed (median)", color="#3b8bbe")
    ax.bar(x + w / 2, contrib_med, w, yerr=contrib_err, capsize=3,
           label="distinct contributing (median)", color="#e07b00")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{m}" for m in mpfs])
    ax.set_xlabel("min_probe_fraction")
    ax.set_ylabel("# clusters")
    ax.set_title("Clusters probed vs. distinct contributing to top-100")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    for i, (p, c) in enumerate(zip(probed_med, contrib_med)):
        ax.text(i - w / 2, p, f"{p:.0f}", ha="center", va="bottom", fontsize=9)
        ax.text(i + w / 2, c, f"{c:.0f}", ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"[done] wrote {out_path}")


def plot_prune_gap_hist(stats_json, out_path):
    rows = json.load(open(stats_json))["rows"]
    full = [r for r in rows if r.get("window_full")]
    r_gap = [r["r_gap"] for r in full if np.isfinite(r["r_gap"])]
    l_gap = [r["l_gap"] for r in full if np.isfinite(r["l_gap"])]

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=True)
    for ax, data, title, color in [
        (axes[0], r_gap, "Right-side gap  (rGap = max dxc − (kthDqx + dqc))", "#3b8bbe"),
        (axes[1], l_gap, "Left-side gap  (lGap = (dqc − kthDqx) − min dxc)", "#e07b00"),
    ]:
        ax.hist(data, bins=80, color=color, edgecolor="white")
        ax.axvline(0, color="red", linestyle="--", linewidth=1.5, label="firing threshold (gap=0)")
        reachable_frac = sum(1 for v in data if v > 0) / len(data)
        ax.set_title(f"{title}\nreachable = {reachable_frac:.1%} of {len(data):,} probes")
        ax.set_xlabel("gap (quantized distance units)")
        ax.legend()
        ax.grid(axis="y", linestyle="--", alpha=0.4)
    axes[0].set_ylabel("# cluster probes")
    fig.suptitle("Pruning-bound reachability across all cluster probes", fontsize=12)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"[done] wrote {out_path}")


def plot_cluster_stack(queries_json, log_path, qid, out_path, topk=100):
    qs = json.load(open(queries_json))["queries"]
    blocks = load_blocks(log_path)
    # Collect (mpf -> Counter(cid -> count)) for this qid
    data = {}
    for idx, q in enumerate(qs):
        if q["query_id"] != qid:
            continue
        b = blocks[idx] if idx < len(blocks) else None
        if not b:
            continue
        data[q["min_probe_fraction"]] = Counter(c for c, _ in b["results"][:topk])

    mpfs = sorted(data.keys())
    # Global ordering of cids by total contribution
    totals = Counter()
    for m in mpfs:
        totals.update(data[m])
    cids_sorted = [c for c, _ in totals.most_common()]

    # Build stacked values
    bottoms = np.zeros(len(mpfs))
    fig, ax = plt.subplots(figsize=(7.5, 5.5))
    cmap = plt.get_cmap("tab20")
    for i, cid in enumerate(cids_sorted):
        heights = np.array([data[m].get(cid, 0) for m in mpfs])
        color = cmap(i % 20)
        ax.bar(range(len(mpfs)), heights, bottom=bottoms, color=color,
               label=f"cid {cid}" if i < 12 else None,
               edgecolor="white", linewidth=0.3)
        bottoms += heights

    ax.set_xticks(range(len(mpfs)))
    ax.set_xticklabels([f"mpf={m}" for m in mpfs])
    ax.set_ylabel(f"records in top-{topk}")
    # Recall annotations
    recalls = {q["min_probe_fraction"]: q["recall"] for q in qs if q["query_id"] == qid}
    distinct = [len(data[m]) for m in mpfs]
    for i, m in enumerate(mpfs):
        ax.text(i, topk + 1.5, f"recall={recalls[m]:.0%}\n{distinct[i]} clusters",
                ha="center", va="bottom", fontsize=8)
    ax.set_ylim(0, topk * 1.22)
    ax.set_title(f"Top-{topk} cluster composition  (qid={qid})")
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), fontsize=8,
              title="cluster id (top 12)", frameon=False)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"[done] wrote {out_path}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--queries-json", default="/tmp/movie_5q_mpfsweep.json")
    p.add_argument("--log", default="/Users/hongyu/Projects/dev/asterixdb-schema-knn/debug.log")
    p.add_argument("--stats-json", default="/tmp/movie_prune_stats.json")
    p.add_argument("--outdir", default="/tmp/prune_plots")
    p.add_argument("--qid-stack", type=int, default=3)
    args = p.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    plot_recall_vs_mpf(args.queries_json, os.path.join(args.outdir, "01_recall_vs_mpf.png"))
    plot_probed_vs_contributing(args.queries_json, args.log,
                                os.path.join(args.outdir, "02_probed_vs_contributing.png"))
    plot_prune_gap_hist(args.stats_json, os.path.join(args.outdir, "03_prune_gap_hist.png"))
    plot_cluster_stack(args.queries_json, args.log, args.qid_stack,
                       os.path.join(args.outdir, f"04_q{args.qid_stack}_cluster_stack.png"))


if __name__ == "__main__":
    main()
