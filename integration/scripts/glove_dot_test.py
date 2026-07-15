#!/usr/bin/env python3
"""
GloVe-100 dot product test: compare ANN recall with and without quantization.

Usage:
    python glove_dot_test.py [OPTIONS]

Options:
    --limit <N>           Load only first N records (default: all ~1.18M)
    --num-clusters <N>    Number of clusters (default: 500)
    --train-list <N>      Training list size (default: 5000)
    --k <N>               Number of results (default: 30)
    --nprobe <N>          Number of clusters to probe (default: 20)
    --epsilon <F>         Error tolerance (default: 0.15)
    --skip-load           Skip dataverse/dataset creation (reuse existing)
    --only <mode>         Run only one mode: 'plain' or 'quantized'

Examples:
    # Full test (load + both indexes + compare)
    python glove_dot_test.py --limit 50000

    # Skip loading, just re-run index + query
    python glove_dot_test.py --skip-load

    # Only test quantized index
    python glove_dot_test.py --skip-load --only quantized
"""

import os
import sys
import json
import requests

ASTERIX_URL = "http://localhost:19002/query/service"
HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}

DATASET_FILENAME = "glove-100-angular_train.jsonl"
DATAVERSE = "VectorTest"
DATASET_NAME = "glove_100_angular"
TYPE_NAME = "GloveType"
INDEX_NAME = "ix"
DIMENSION = 100


def execute_statement(statement, context_id, timeout=None):
    """Execute an AsterixDB SQL++ statement."""
    data = {
        "statement": statement,
        "pretty": "true",
        "client_context_id": context_id,
    }
    kwargs = {}
    if timeout:
        kwargs["timeout"] = timeout

    resp = requests.post(ASTERIX_URL, headers=HEADERS, data=data, **kwargs)

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"HTTP error from AsterixDB: {e}")
        print(f"Response text: {resp.text[:2000]}")
        sys.exit(1)

    return resp


def load_dataset(dataset_path, num_records=None):
    """Create dataverse, type, dataset and load data."""

    if num_records is not None:
        print(f"[info] Creating limited dataset with first {num_records} records...")
        limited_path = f"{dataset_path}.limited_{num_records}"
        with open(dataset_path, "r") as infile, open(limited_path, "w") as outfile:
            for i, line in enumerate(infile):
                if i >= num_records:
                    break
                outfile.write(line)
        print(f"[info] Limited dataset created: {limited_path}")
        load_path = limited_path
    else:
        load_path = dataset_path

    abs_path = os.path.abspath(load_path).replace("\\", "/")
    localfs_path = f"localhost://{abs_path}"

    statement = f"""
    DROP DATAVERSE {DATAVERSE} IF EXISTS;
    CREATE DATAVERSE {DATAVERSE};
    USE {DATAVERSE};

    CREATE TYPE {TYPE_NAME} AS {{
      idx: int
    }};

    CREATE DATASET {DATASET_NAME} ({TYPE_NAME})
    PRIMARY KEY idx WITH {{
      "storage-format":{{"format":"column"}}
    }};

    LOAD DATASET {DATASET_NAME} USING localfs (
      ("path" = "{localfs_path}"),
      ("format" = "json")
    );
    """

    print("[step] Creating dataverse and loading dataset")
    print(f"[info] Loading from: {localfs_path}")
    if num_records is not None:
        print(f"[info] Record limit: {num_records}")
    execute_statement(statement, "load_dataset")
    print("[done] Dataverse and dataset created\n")


def drop_index():
    """Drop existing vector index if it exists."""
    statement = f"""
    USE {DATAVERSE};
    DROP INDEX {DATASET_NAME}.{INDEX_NAME} IF EXISTS;
    """
    print("[step] Dropping existing index (if any)")
    execute_statement(statement, "drop_index")
    print("[done] Index dropped\n")


def analyze_dataset():
    """Run ANALYZE DATASET (required before creating quantized index)."""
    statement = f"""
    USE {DATAVERSE};
    ANALYZE DATASET {DATASET_NAME};
    """
    print("[step] Running ANALYZE DATASET")
    execute_statement(statement, "analyze_dataset")
    print("[done] ANALYZE DATASET complete\n")


def create_vector_index(num_clusters, train_list, quantization=None):
    """Create vector index with dot product similarity."""

    with_params = {
        "dimension": DIMENSION,
        "train_list_number": train_list,
        "num_clusters": num_clusters,
        "similarity": "dot",
    }
    if quantization:
        with_params["quantization"] = quantization

    with_entries = [
        f'"{k}": {json.dumps(v) if isinstance(v, str) else v}'
        for k, v in with_params.items()
    ]
    with_clause = ",\n      ".join(with_entries)

    statement = f"""
    USE {DATAVERSE};
    CREATE VECTOR INDEX {INDEX_NAME} ON {DATASET_NAME}(embedding VECTOR)
    WITH {{
      {with_clause}
    }};
    """

    label = f"quantized={quantization}" if quantization else "plain (no quantization)"
    print(f"[step] Creating vector index ({label}, similarity=dot, clusters={num_clusters})")
    print(f"[info] Statement:\n{statement}")
    execute_statement(statement, "create_index")
    print("[done] Vector index created\n")


def load_query_vector(dataset_path, idx=0):
    """Load embedding from dataset for a given idx."""
    print(f"[step] Loading query vector (idx={idx})")
    with open(dataset_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record.get("idx") == idx:
                emb = record["embedding"]
                print(f"[info] Loaded embedding dim={len(emb)}")
                return emb
    print(f"Error: idx={idx} not found in {dataset_path}")
    sys.exit(1)


def run_ann_query(qvec, k, nprobe, epsilon):
    """Execute ANN query and return results list."""
    qvec_str = json.dumps(qvec)

    statement = f"""
    USE {DATAVERSE};
    LET qvec = {qvec_str}
    FROM {DATASET_NAME} m
    LET dist = ann_distance(m.embedding, qvec, "dot", {nprobe}, {epsilon})
    SELECT m.idx, dist
    ORDER BY dist
    LIMIT {k};
    """

    print(f"[query] ANN query: k={k}, nprobe={nprobe}, epsilon={epsilon}")
    resp = execute_statement(statement, "ann_query")
    result = resp.json()
    if "results" in result:
        return result["results"]
    print(f"Unexpected response: {json.dumps(result)[:500]}")
    sys.exit(1)


def run_exact_query(qvec, k):
    """Execute exact (brute-force) query for recall comparison."""
    qvec_str = json.dumps(qvec)

    statement = f"""
    USE {DATAVERSE};
    LET qvec = {qvec_str}
    FROM {DATASET_NAME} m
    LET dist = vector_distance(m.embedding, qvec, "dot")
    SELECT m.idx, dist
    ORDER BY dist
    LIMIT {k};
    """

    print(f"[query] Exact (brute-force) query: k={k}")
    resp = execute_statement(statement, "exact_query")
    result = resp.json()
    if "results" in result:
        return result["results"]
    print(f"Unexpected response: {json.dumps(result)[:500]}")
    sys.exit(1)


def compute_recall(ann_results, exact_results):
    """Compute recall@k = |ANN ∩ Exact| / k."""
    ann_ids = set(r["idx"] for r in ann_results)
    exact_ids = set(r["idx"] for r in exact_results)
    overlap = ann_ids & exact_ids
    recall = len(overlap) / len(exact_ids) if exact_ids else 0.0
    return recall, overlap


def print_results(results, label, max_show=10):
    """Print query results."""
    print(f"\n  {label} ({len(results)} results):")
    for i, r in enumerate(results[:max_show]):
        idx = r.get("idx", "?")
        dist = r.get("dist", "?")
        print(f"    {i+1:3d}. idx={idx}, dist={dist}")
    if len(results) > max_show:
        print(f"    ... ({len(results) - max_show} more)")


def run_test(qvec, k, nprobe, epsilon, label):
    """Run ANN query, exact query, compute recall."""
    print(f"\n{'='*60}")
    print(f"  TEST: {label}")
    print(f"{'='*60}\n")

    ann_results = run_ann_query(qvec, k, nprobe, epsilon)
    exact_results = run_exact_query(qvec, k)

    recall, overlap = compute_recall(ann_results, exact_results)

    print_results(ann_results, "ANN results")
    print_results(exact_results, "Exact results")

    print(f"\n  Recall@{k}: {recall:.4f} ({len(overlap)}/{len(exact_results)})")
    print(f"{'='*60}\n")

    return {
        "label": label,
        "ann_results": ann_results,
        "exact_results": exact_results,
        "recall": recall,
    }


def parse_args():
    """Parse command-line arguments."""
    args = {
        "limit": None,
        "num_clusters": 500,
        "train_list": 5000,
        "k": 30,
        "nprobe": 20,
        "epsilon": 0.15,
        "skip_load": False,
        "only": None,
    }

    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == "--limit" and i + 1 < len(sys.argv):
            args["limit"] = int(sys.argv[i + 1])
            i += 2
        elif arg == "--num-clusters" and i + 1 < len(sys.argv):
            args["num_clusters"] = int(sys.argv[i + 1])
            i += 2
        elif arg == "--train-list" and i + 1 < len(sys.argv):
            args["train_list"] = int(sys.argv[i + 1])
            i += 2
        elif arg == "--k" and i + 1 < len(sys.argv):
            args["k"] = int(sys.argv[i + 1])
            i += 2
        elif arg == "--nprobe" and i + 1 < len(sys.argv):
            args["nprobe"] = int(sys.argv[i + 1])
            i += 2
        elif arg == "--epsilon" and i + 1 < len(sys.argv):
            args["epsilon"] = float(sys.argv[i + 1])
            i += 2
        elif arg == "--skip-load":
            args["skip_load"] = True
            i += 1
        elif arg == "--only" and i + 1 < len(sys.argv):
            args["only"] = sys.argv[i + 1]
            i += 2
        else:
            print(f"Unknown argument: {arg}")
            sys.exit(1)

    return args


def main():
    args = parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)
    dataset_path = os.path.join(base_dir, "datasets", DATASET_FILENAME)

    if not os.path.exists(dataset_path):
        print(f"Error: dataset not found: {dataset_path}")
        sys.exit(1)

    print("=" * 60)
    print("GloVe-100 Dot Product Test")
    print("=" * 60)
    print(f"Dataset:      {DATASET_FILENAME}")
    print(f"Limit:        {args['limit'] or 'all'}")
    print(f"Clusters:     {args['num_clusters']}")
    print(f"Train list:   {args['train_list']}")
    print(f"K:            {args['k']}")
    print(f"nprobe:       {args['nprobe']}")
    print(f"epsilon:      {args['epsilon']}")
    print(f"Skip load:    {args['skip_load']}")
    print(f"Only:         {args['only'] or 'both'}")
    print("=" * 60)
    print()

    # Step 1: Load dataset
    if not args["skip_load"]:
        load_dataset(dataset_path, args["limit"])

    # Load query vector (first record)
    qvec = load_query_vector(dataset_path, idx=0)
    print()

    results_summary = []

    # Step 2: Plain (no quantization) index + query
    if args["only"] is None or args["only"] == "plain":
        drop_index()
        create_vector_index(args["num_clusters"], args["train_list"], quantization=None)
        result = run_test(qvec, args["k"], args["nprobe"], args["epsilon"],
                          "Plain (no quantization) + dot similarity")
        results_summary.append(result)

    # Step 3: Quantized (SQ7) index + query
    if args["only"] is None or args["only"] == "quantized":
        drop_index()
        analyze_dataset()
        create_vector_index(args["num_clusters"], args["train_list"], quantization="SQ7")
        result = run_test(qvec, args["k"], args["nprobe"], args["epsilon"],
                          "Quantized (SQ7) + dot similarity")
        results_summary.append(result)

    # Summary
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    for r in results_summary:
        print(f"  {r['label']}: recall@{args['k']} = {r['recall']:.4f}")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
