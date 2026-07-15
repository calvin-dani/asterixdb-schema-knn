#!/usr/bin/env python3
"""Sweep nprobe x epsilon for GloVe-100 dot product on existing VectorTest dataverse."""

import os
import sys
import json
import requests

ASTERIX_URL = "http://localhost:19002/query/service"
HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}

DATAVERSE = "VectorTest"
DATASET_NAME = "glove_100_angular"
INDEX_NAME = "ix"
DIMENSION = 100
K = 30

NPROBES = [20, 50, 100]
EPSILONS = [0.15, 0.30, 0.45]


def execute_statement(statement, context_id):
    data = {"statement": statement, "pretty": "true", "client_context_id": context_id}
    resp = requests.post(ASTERIX_URL, headers=HEADERS, data=data)
    resp.raise_for_status()
    return resp


def drop_index():
    execute_statement(f"USE {DATAVERSE}; DROP INDEX {DATASET_NAME}.{INDEX_NAME} IF EXISTS;", "drop")


def analyze_dataset():
    execute_statement(f"USE {DATAVERSE}; ANALYZE DATASET {DATASET_NAME};", "analyze")


def create_index(quantization=None):
    with_params = {
        "dimension": DIMENSION,
        "train_list_number": 10000,
        "num_clusters": 500,
        "similarity": "dot",
    }
    if quantization:
        with_params["quantization"] = quantization

    entries = [f'"{k}": {json.dumps(v) if isinstance(v, str) else v}' for k, v in with_params.items()]
    stmt = f"""
    USE {DATAVERSE};
    CREATE VECTOR INDEX {INDEX_NAME} ON {DATASET_NAME}(embedding VECTOR)
    WITH {{ {", ".join(entries)} }};
    """
    execute_statement(stmt, "create_index")


def run_ann_query(qvec, nprobe, epsilon):
    stmt = f"""
    USE {DATAVERSE};
    LET qvec = {json.dumps(qvec)}
    FROM {DATASET_NAME} m
    LET dist = ann_distance(m.embedding, qvec, "dot", {nprobe}, {epsilon})
    SELECT m.idx, dist
    ORDER BY dist
    LIMIT {K};
    """
    result = execute_statement(stmt, "ann").json()
    return result.get("results", [])


def run_exact_query(qvec):
    stmt = f"""
    USE {DATAVERSE};
    LET qvec = {json.dumps(qvec)}
    FROM {DATASET_NAME} m
    LET dist = vector_distance(m.embedding, qvec, "dot")
    SELECT m.idx, dist
    ORDER BY dist
    LIMIT {K};
    """
    result = execute_statement(stmt, "exact").json()
    return result.get("results", [])


def compute_recall(ann_results, exact_results):
    ann_ids = set(r["idx"] for r in ann_results)
    exact_ids = set(r["idx"] for r in exact_results)
    overlap = ann_ids & exact_ids
    return len(overlap) / len(exact_ids) if exact_ids else 0.0


def load_query_vector():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)
    path = os.path.join(base_dir, "datasets", "glove-100-angular_train.jsonl")
    with open(path) as f:
        record = json.loads(f.readline())
        return record["embedding"]


def run_sweep(mode, qvec, exact_results):
    """Run all nprobe x epsilon combos for one index mode."""
    label = mode.upper()
    print(f"\n{'='*60}")
    print(f"  {label} INDEX")
    print(f"{'='*60}")

    drop_index()
    if mode == "quantized":
        analyze_dataset()
    create_index(quantization="SQ7" if mode == "quantized" else None)
    print(f"  Index created ({mode})")

    results = {}
    for nprobe in NPROBES:
        for eps in EPSILONS:
            ann = run_ann_query(qvec, nprobe, eps)
            recall = compute_recall(ann, exact_results)
            results[(nprobe, eps)] = recall
            print(f"  nprobe={nprobe:3d}, epsilon={eps:.2f} -> recall@{K} = {recall:.4f} ({int(recall*K)}/{K})")

    return results


def main():
    print("Loading query vector...")
    qvec = load_query_vector()

    print("Running exact query (baseline)...")
    exact_results = run_exact_query(qvec)
    print(f"Exact query returned {len(exact_results)} results\n")

    plain_results = run_sweep("plain", qvec, exact_results)
    quant_results = run_sweep("quantized", qvec, exact_results)

    # Print summary table
    print(f"\n{'='*60}")
    print(f"  SUMMARY: GloVe-100 Dot Product Recall@{K}")
    print(f"{'='*60}")
    print(f"{'nprobe':>8} {'epsilon':>8} {'Plain':>10} {'Quantized':>12}")
    print(f"{'-'*42}")
    for nprobe in NPROBES:
        for eps in EPSILONS:
            p = plain_results[(nprobe, eps)]
            q = quant_results[(nprobe, eps)]
            print(f"{nprobe:>8} {eps:>8.2f} {p:>10.4f} {q:>12.4f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
