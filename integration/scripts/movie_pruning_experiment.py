#!/usr/bin/env python3
"""
Movie Dataset Pruning Effectiveness Experiment.

Goal: Determine per-cluster how close the triangle-inequality pruning came
      to firing when a leaf cluster was exhausted (vs. early-terminated).

Prerequisites:
  - AsterixDB cluster already running
  - EXPERIMENT.Movie dataset already loaded
  - Vector index already created on Movie.embedding (quantized SQ8 recommended)
  - Java code in LSMVTreeBlockedCursor patched to emit [PRUNE_STATS] log lines

Updated ann_distance signature:
    ann_distance(field, qvec, metric, min_probe_fraction, k_multiplier)

Usage:
    # 1. Run queries (pruning enabled); writes a JSON summary of query outcomes.
    python movie_pruning_experiment.py query \
        --index ix_quantized \
        --num-queries 20 \
        --output movie_prune_queries.json

    # 2. Parse NC log to analyse per-cluster pruning gaps.
    python movie_pruning_experiment.py parse-log \
        --log /path/to/nc.log \
        --output movie_prune_stats.json

    # 3. (Optional) Do both when NC stderr is captured to a file.
    python movie_pruning_experiment.py query --tail-log /path/to/nc.log ...
"""

import argparse
import json
import os
import re
import statistics
import sys
import time

import requests

# ── Fixed cluster config (matches movie_test.py in this repo) ───────────────
ASTERIX_URL = os.environ.get("ASTERIX_URL", "http://localhost:19002/query/service")
HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}
DATAVERSE = os.environ.get("DATAVERSE", "MovieDB")
DATASET = os.environ.get("DATASET", "Movies")
SIMILARITY = os.environ.get("SIMILARITY", "euclidean_squared")

# ── HTTP helpers ────────────────────────────────────────────────────────────
def execute(statement):
    resp = requests.post(ASTERIX_URL, headers=HEADERS,
                         data={"statement": statement, "pretty": "true"})
    resp.raise_for_status()
    return resp.json()


def fetch_query_vector(query_id):
    stmt = f"USE {DATAVERSE}; SELECT row.embedding FROM {DATASET} row WHERE row.id = {query_id};"
    r = execute(stmt)
    results = r.get("results", [])
    if not results:
        return None
    return results[0].get("embedding")


def sample_query_ids(n):
    stmt = (f"USE {DATAVERSE}; SELECT row.id FROM {DATASET} row "
            f"WHERE row.embedding IS NOT UNKNOWN LIMIT {n};")
    r = execute(stmt)
    return [row["id"] for row in r.get("results", [])]


def run_pruned_ann(qvec, k, min_probe_fraction, k_multiplier, query_tag):
    qvec_str = json.dumps(qvec)
    stmt = f"""SET `compiler.vector.prunedsearch` "true";
USE {DATAVERSE};
-- TAG:{query_tag}
LET qvec = {qvec_str}
FROM {DATASET} row
LET dist = ann_distance(row.embedding, qvec, "{SIMILARITY}", {min_probe_fraction}, {k_multiplier})
SELECT row.id, dist
ORDER BY dist
LIMIT {k};"""
    t0 = time.time()
    r = execute(stmt)
    wall_ms = (time.time() - t0) * 1000.0
    return r.get("results", []), wall_ms


def run_brute_force_knn(qvec, k):
    """Brute-force reference via vector_distance — bypasses the vector index."""
    qvec_str = json.dumps(qvec)
    stmt = f"""USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {DATASET} row
LET dist = vector_distance(row.embedding, qvec, "{SIMILARITY}")
SELECT row.id, dist
ORDER BY dist
LIMIT {k};"""
    t0 = time.time()
    r = execute(stmt)
    wall_ms = (time.time() - t0) * 1000.0
    return r.get("results", []), wall_ms


def compute_recall(ann_rows, knn_rows):
    ann_ids = {row.get("id") for row in ann_rows}
    knn_ids = {row.get("id") for row in knn_rows}
    return (len(ann_ids & knn_ids) / len(knn_ids)) if knn_ids else 0.0


# ── Subcommand: query ───────────────────────────────────────────────────────
def cmd_query(args):
    qids = sample_query_ids(args.num_queries)
    print(f"[info] sampled {len(qids)} query ids")

    sweep = [(k, mpf, km)
             for k in args.k_values
             for mpf in args.min_probe_fractions
             for km in args.k_multipliers]

    out = []
    # Cache ground-truth KNN per (qid, K) so we don't repeat brute force across the sweep.
    knn_cache = {}
    for qid in qids:
        qvec = fetch_query_vector(qid)
        if not qvec:
            print(f"[warn] no embedding for id={qid}, skipping")
            continue
        for (k, mpf, km) in sweep:
            tag = f"q{qid}_k{k}_mpf{mpf}_km{km}"
            results, wall_ms = run_pruned_ann(qvec, k, mpf, km, tag)
            recall = None
            knn_wall_ms = None
            if args.recall:
                if (qid, k) not in knn_cache:
                    knn_rows, knn_wall_ms = run_brute_force_knn(qvec, k)
                    knn_cache[(qid, k)] = (knn_rows, knn_wall_ms)
                knn_rows, knn_wall_ms = knn_cache[(qid, k)]
                recall = compute_recall(results, knn_rows)
            recall_str = f" recall={recall:.2%}" if recall is not None else ""
            print(f"  [{tag}] wall={wall_ms:6.1f}ms rows={len(results)}{recall_str}")
            out.append({
                "tag": tag, "query_id": qid, "K": k,
                "min_probe_fraction": mpf, "k_multiplier": km,
                "wall_ms": wall_ms, "num_results": len(results),
                "recall": recall, "knn_wall_ms": knn_wall_ms,
                "top_ids": [r.get("id") for r in results[:k]],
            })

    with open(args.output, "w") as f:
        json.dump({"similarity": SIMILARITY, "queries": out}, f, indent=2)
    print(f"[done] wrote {args.output}")


# ── Subcommand: parse-log ───────────────────────────────────────────────────
PRUNE_RE = re.compile(
    r"\[PRUNE_STATS\]\s+"
    r"cid=(?P<cid>-?\d+)\s+"
    r"dqc=(?P<dqc>\S+)\s+"
    r"kthDqx=(?P<kth>\S+)\s+"
    r"windowFull=(?P<wf>true|false)\s+"
    r"rLastDxc=(?P<rlast>\S+)\s+rLastDxcFull=(?P<rlastfull>\S+)\s+rMaxQPeek=(?P<rmaxq>\S+)\s+"
    r"rThr=(?P<rthr>\S+)\s+rGap=(?P<rgap>\S+)\s+"
    r"rEarly=(?P<rearly>true|false)\s+rScan=(?P<rscan>\d+)\s+"
    r"lLastDxc=(?P<llast>\S+)\s+lLastDxcFull=(?P<llastfull>\S+)\s+lMinQPeek=(?P<lminq>\S+)\s+"
    r"lThr=(?P<lthr>\S+)\s+lGap=(?P<lgap>\S+)\s+"
    r"lEarly=(?P<learly>true|false)\s+lScan=(?P<lscan>\d+)"
)


def _f(s):
    if s in ("NaN", "Infinity", "-Infinity"):
        return float(s.replace("Infinity", "inf"))
    try:
        return float(s)
    except ValueError:
        return float("nan")


def cmd_parse_log(args):
    rows = []
    with open(args.log, "r", errors="replace") as f:
        for line in f:
            m = PRUNE_RE.search(line)
            if not m:
                continue
            d = m.groupdict()
            rows.append({
                "cid": int(d["cid"]),
                "dqc": _f(d["dqc"]),
                "kth_dqx": _f(d["kth"]),
                "window_full": d["wf"] == "true",
                "r_last_dxc": _f(d["rlast"]),
                "r_threshold": _f(d["rthr"]),
                "r_gap": _f(d["rgap"]),
                "r_early": d["rearly"] == "true",
                "r_scan": int(d["rscan"]),
                "l_last_dxc": _f(d["llast"]),
                "l_threshold": _f(d["lthr"]),
                "l_gap": _f(d["lgap"]),
                "l_early": d["learly"] == "true",
                "l_scan": int(d["lscan"]),
            })

    print(f"[info] parsed {len(rows)} PRUNE_STATS rows")
    if not rows:
        return

    full = [r for r in rows if r["window_full"]]
    print(f"[info] of which {len(full)} had topK window full (gap numbers meaningful)")

    def summarize(label, values):
        values = [v for v in values if v == v and v not in (float("inf"), float("-inf"))]
        if not values:
            print(f"  {label}: no data")
            return
        values.sort()
        pct = lambda p: values[min(len(values) - 1, int(p * len(values)))]
        print(f"  {label}: n={len(values)} "
              f"min={values[0]:.4f} p25={pct(0.25):.4f} median={pct(0.5):.4f} "
              f"p75={pct(0.75):.4f} max={values[-1]:.4f} mean={statistics.mean(values):.4f}")

    print("\n== Cluster-exhaustion gap distribution (window-full rows) ==")
    summarize("right gap (rGap>0 => termination was reachable)", [r["r_gap"] for r in full])
    summarize("left  gap (lGap>0 => termination was reachable)", [r["l_gap"] for r in full])

    r_reachable = sum(1 for r in full if r["r_gap"] > 0)
    l_reachable = sum(1 for r in full if r["l_gap"] > 0)
    r_fired = sum(1 for r in full if r["r_early"])
    l_fired = sum(1 for r in full if r["l_early"])
    print(f"\n== Pruning outcome per cluster (n={len(full)}) ==")
    print(f"  right reachable: {r_reachable} ({r_reachable / len(full):.1%})  "
          f"early-fired: {r_fired} ({r_fired / len(full):.1%})")
    print(f"  left  reachable: {l_reachable} ({l_reachable / len(full):.1%})  "
          f"early-fired: {l_fired} ({l_fired / len(full):.1%})")

    summarize("right scan count", [r["r_scan"] for r in rows])
    summarize("left  scan count", [r["l_scan"] for r in rows])

    if args.output:
        with open(args.output, "w") as f:
            json.dump({"rows": rows}, f, indent=2)
        print(f"[done] wrote {args.output}")


# ── CLI ─────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    q = sub.add_parser("query")
    q.add_argument("--index", default="ix_quantized")
    q.add_argument("--num-queries", type=int, default=20)
    q.add_argument("--k-values", type=int, nargs="+", default=[10, 100])
    q.add_argument("--min-probe-fractions", type=float, nargs="+", default=[0.05, 0.2])
    q.add_argument("--k-multipliers", type=int, nargs="+", default=[1, 2])
    q.add_argument("--recall", action="store_true",
                   help="Also run brute-force vector_distance KNN for recall comparison")
    q.add_argument("--output", default="movie_prune_queries.json")

    pl = sub.add_parser("parse-log")
    pl.add_argument("--log", required=True)
    pl.add_argument("--output", default=None)

    args = p.parse_args()
    if args.cmd == "query":
        cmd_query(args)
    elif args.cmd == "parse-log":
        cmd_parse_log(args)


if __name__ == "__main__":
    main()
