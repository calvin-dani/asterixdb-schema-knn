#!/usr/bin/env python3
"""
AsterixDB cluster vector index management script.

Adapted for 4-node cloud mode cluster (EXPERIMENT dataverse, Movie dataset).
Connects via SSH tunnel on localhost:29002.

Usage:
    Create index:
        python cluster_index.py create --name ix_nonquantized --num-clusters 560 --dimension 384 --similarity euclidean_squared --train-list 30000 --include title popularity

    Create index with quantization:
        python cluster_index.py create --name ix_quantized --num-clusters 560 --dimension 384 --similarity euclidean_squared --quantization SQ7 --train-list 30000

    Drop index:
        python cluster_index.py drop --name ix_nonquantized

    Query (ANN search):
        python cluster_index.py query --k 50 --year 2000 --query-id 0

    Query with quantization params:
        python cluster_index.py query --k 50 --year 2000 --query-id 0 --nprobe 20 --epsilon 0.05

    Use a different dataset:
        python cluster_index.py --dataset Movie2 benchmark --nprobe 10 --epsilon 0.2 --num-queries 5

    Regression test (nprobe x epsilon):
        python cluster_index.py --dataset Movie2 regression --nprobes 10,20,30,40 --epsilons 0.15,0.2,0.25,0.3 --num-queries 3

    Filter benchmark (inline filter selectivity):
        python cluster_index.py --dataset Movie2 filter-bench --nprobe 20 --epsilon 0.2 --num-queries 3

    Filter regression (nprobe x epsilon x selectivity):
        python cluster_index.py --dataset Movie_10M_iq filter-regression --selectivities 10,20,40,60 --nprobes 30,45,60,80,100 --epsilons 0.3,0.4 --num-queries 2 --output filter_reg.json
        python cluster_index.py --dataset Movie_10M_iq filter-regression --selectivities 20 --nprobes 100,80,60,45,30 --epsilons 0.3,0.4 --no-warmup --output filter_reg.json

    KNN benchmark (brute-force vs index-driven):
        python cluster_index.py --dataset Movie2 knn-bench --num-queries 3

    Status (check cluster & dataset):
        python cluster_index.py status

    Count records:
        python cluster_index.py count

    Sample records:
        python cluster_index.py sample --limit 5

    List indexes:
        python cluster_index.py indexes
"""

import sys
import json
import time
import argparse
import requests

ASTERIX_URL = "http://localhost:29002/query/service"
HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}
DATAVERSE = "EXPERIMENT"
DATASET = "Movie"

SELECTIVITY_FILTERS = {
    10: 'm.title LIKE "% I"',
    20: 'm.title LIKE "% I" OR m.title LIKE "% II"',
    40: 'm.title LIKE "% I" OR m.title LIKE "% II" OR m.title LIKE "% III" OR m.title LIKE "% IV"',
    60: 'm.title LIKE "% I" OR m.title LIKE "% II" OR m.title LIKE "% III" OR m.title LIKE "% IV" OR m.title LIKE "% V" OR m.title LIKE "% VI"',
}


def execute_statement(statement, context_id="default", extra_params=None):
    """Execute an AsterixDB SQL++ statement."""
    data = {
        "statement": statement,
        "pretty": "true",
        "client_context_id": context_id,
    }
    if extra_params:
        data.update(extra_params)
    resp = requests.post(ASTERIX_URL, headers=HEADERS, data=data)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"[error] HTTP error: {e}")
        print(f"[error] Response: {resp.text}")
        sys.exit(1)
    return resp


def cmd_create(args):
    """Create a vector index."""
    include_clause = ""
    if args.include:
        fields = ", ".join(args.include)
        include_clause = f"INCLUDE({fields})"

    with_params = {
        "dimension": args.dimension,
        "num_clusters": args.num_clusters,
        "similarity": f'"{args.similarity}"',
        "train_list_number": args.train_list,
    }

    if args.quantization:
        with_params["quantization"] = f'"{args.quantization}"'

    with_entries = []
    for k, v in with_params.items():
        if isinstance(v, str) and v.startswith('"'):
            with_entries.append(f'"{k}":{v}')
        else:
            with_entries.append(f'"{k}":{v}')

    with_clause = ",\n       ".join(with_entries)

    statement = f"""USE {DATAVERSE};
CREATE VECTOR INDEX {args.name} ON {args.dataset}(embedding VECTOR) {include_clause} WITH {{
       {with_clause}
}};"""

    print("=" * 60)
    print("CREATE VECTOR INDEX")
    print("=" * 60)
    print(f"  Index Name:    {args.name}")
    print(f"  Dataset:       {DATAVERSE}.{args.dataset}")
    print(f"  Dimension:     {args.dimension}")
    print(f"  Num Clusters:  {args.num_clusters}")
    print(f"  Similarity:    {args.similarity}")
    print(f"  Train List:    {args.train_list}")
    if args.quantization:
        print(f"  Quantization:  {args.quantization}")
    if args.include:
        print(f"  Include:       {', '.join(args.include)}")
    print("=" * 60)
    print()
    print(f"[statement]\n{statement}")
    print()

    start = time.time()
    print("[step] Creating index...")
    resp = execute_statement(statement, "create_vector_index")
    elapsed = time.time() - start

    result = resp.json()
    print(f"[done] Index created in {elapsed:.1f}s")
    print(f"[status] {result.get('status', 'unknown')}")
    print()


def cmd_drop(args):
    """Drop a vector index."""
    statement = f"""USE {DATAVERSE};
DROP INDEX {args.dataset}.{args.name} IF EXISTS;"""

    print(f"[step] Dropping index {args.name}...")
    resp = execute_statement(statement, "drop_index")
    result = resp.json()
    print(f"[done] {result.get('status', 'unknown')}")
    print()


def cmd_query(args):
    """Execute an ANN query."""
    print(f"[step] Fetching query vector (id={args.query_id})...")
    fetch_stmt = f"""USE {DATAVERSE};
SELECT m.embedding FROM {args.dataset} m WHERE m.id = {args.query_id};"""

    resp = execute_statement(fetch_stmt, "fetch_query_vector")
    result = resp.json()

    if not result.get("results"):
        print(f"[error] No record found with id={args.query_id}")
        sys.exit(1)

    qvec = result["results"][0].get("embedding")
    if not qvec:
        print(f"[error] Record id={args.query_id} has no embedding field")
        sys.exit(1)

    dimension = len(qvec)
    print(f"[info] Query vector dimension: {dimension}")

    qvec_str = json.dumps(qvec)

    if args.nprobe is not None:
        ann_call = f'ann_distance(row.embedding, qvec, "{args.similarity}", {args.nprobe}, {args.epsilon})'
    else:
        ann_call = f'ann_distance(row.embedding, qvec, "{args.similarity}")'

    where_parts = []
    if args.year is not None:
        where_parts.append(f"row.year > {args.year}")
    if args.where:
        where_parts.append(args.where)

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    statement = f"""{pruned_search_prefix(args)}USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {args.dataset} row
LET dist = {ann_call}
{where_clause}
SELECT row.id, row.title, row.year, row.popularity, dist
ORDER BY dist
LIMIT {args.k};"""

    print(f"[step] Executing ANN query (k={args.k})")
    if args.year is not None:
        print(f"[info] Filter: year > {args.year}")

    start = time.time()
    resp = execute_statement(statement, "ann_query")
    elapsed = time.time() - start

    result = resp.json()
    results = result.get("results", [])

    print(f"[done] {len(results)} results in {elapsed:.1f}s")
    print()

    print(f"{'#':>3}  {'ID':>8}  {'Year':>5}  {'Popularity':>10}  {'Distance':>12}  Title")
    print("-" * 80)
    for i, rec in enumerate(results):
        rid = rec.get("id", "N/A")
        title = rec.get("title", "N/A")
        year = rec.get("year", "N/A")
        pop = rec.get("popularity", "N/A")
        dist = rec.get("dist", "N/A")
        if isinstance(dist, float):
            dist = f"{dist:.4f}"
        if isinstance(pop, float):
            pop = f"{pop:.1f}"
        print(f"{i+1:>3}  {rid:>8}  {year:>5}  {pop:>10}  {dist:>12}  {title}")

    print()

    metrics = result.get("metrics", {})
    print(f"[metrics] elapsed={metrics.get('elapsedTime', 'N/A')}, "
          f"execution={metrics.get('executionTime', 'N/A')}, "
          f"compile={metrics.get('compileTime', 'N/A')}")
    print()


def cmd_status(args):
    """Check cluster and dataset status."""
    print("[step] Checking cluster status...")
    try:
        resp = requests.get("http://localhost:29002/admin/cluster/summary")
        cluster = resp.json()
        state = cluster.get("state", "UNKNOWN")
        partitions = cluster.get("partitions", {})
        active = sum(1 for p in partitions.values() if p.get("active"))
        print(f"[cluster] State: {state}")
        print(f"[cluster] Partitions: {active}/{len(partitions)} active")
        print()
    except Exception as e:
        print(f"[error] Could not reach cluster: {e}")
        return

    print("[step] Checking dataset...")
    resp = execute_statement(f"USE {DATAVERSE}; SELECT COUNT(*) AS cnt FROM {args.dataset};", "status_count")
    result = resp.json()
    if result.get("results"):
        cnt = result["results"][0].get("cnt", "N/A")
        print(f"[dataset] {DATAVERSE}.{args.dataset}: {cnt} records")
    print()


def cmd_count(args):
    """Count records in dataset."""
    resp = execute_statement(f"USE {DATAVERSE}; SELECT COUNT(*) AS cnt FROM {args.dataset};", "count")
    result = resp.json()
    if result.get("results"):
        cnt = result["results"][0].get("cnt", "N/A")
        print(f"{cnt} records")


def cmd_sample(args):
    """Sample records from dataset."""
    resp = execute_statement(
        f"USE {DATAVERSE}; SELECT * FROM {args.dataset} m LIMIT {args.limit};",
        "sample"
    )
    result = resp.json()
    for rec in result.get("results", []):
        if "embedding" in rec:
            rec["embedding"] = f"[...{len(rec['embedding'])}d]"
        print(json.dumps(rec, indent=2))
        print()


def parse_time_ms(t):
    """Parse AsterixDB time strings (e.g., '1.234s' or '123.456ms') to milliseconds."""
    if not t:
        return 0
    t = t.strip()
    if t.endswith("ms"):
        return float(t[:-2])
    elif t.endswith("s"):
        return float(t[:-1]) * 1000
    return 0


def pruned_search_prefix(args):
    """Return SET statement prefix if pruned search is enabled."""
    if getattr(args, "pruned_search", False):
        return 'SET `compiler.vector.prunedsearch` "true";\n'
    return ""


def cmd_benchmark(args):
    """Run multiple ANN queries and report statistics."""
    total_queries = args.num_queries * 2

    print("=" * 60)
    print("ANN QUERY BENCHMARK")
    print("=" * 60)
    print(f"  Dataset:       {DATAVERSE}.{args.dataset}")
    print(f"  Num Queries:   {args.num_queries} (+ {args.num_queries} warmup)")
    print(f"  Total Runs:    {total_queries}")
    print(f"  K:             {args.k}")
    print(f"  Query IDs:     {args.start_id} to {args.start_id + args.num_queries - 1} (repeated twice)")
    print(f"  Similarity:    {args.similarity}")
    if args.year is not None:
        print(f"  Year Filter:   > {args.year}")
    if args.nprobe is not None:
        print(f"  Nprobe:        {args.nprobe}")
        print(f"  Epsilon:       {args.epsilon}")
    print(f"  Compute Recall: YES (exact KNN comparison + speedup)")
    if getattr(args, "pruned_search", False):
        print(f"  Pruned Search:  YES")
    if args.profile:
        print(f"  Profile:        YES (timings + optimized logical plan)")
    print("=" * 60)
    print()

    print("[step] Warming NVMe cache (scanning all embeddings)...")
    cache_start = time.time()
    execute_statement(f"USE {DATAVERSE}; SELECT SUM(m.embedding[0]) AS s FROM {args.dataset} m;", "cache_warm")
    cache_elapsed = time.time() - cache_start
    print(f"[done] Cache warmed in {cache_elapsed:.1f}s")
    print()

    profile_params = {}
    if args.profile:
        profile_params = {"profile": "timings", "optimized-logical-plan": "true"}

    elapsed_times = []
    execution_times = []
    compile_times = []
    knn_elapsed_times = []
    knn_execution_times = []
    recalls = []
    errors = []
    profile_data = []

    print(f"--- Warmup phase ({args.num_queries} queries) ---")
    for run in range(total_queries):
        if run == args.num_queries:
            print()
            print(f"--- Benchmark phase ({args.num_queries} queries) ---")

        is_warmup = run < args.num_queries
        query_id = args.start_id + (run % args.num_queries)
        phase = "warmup" if is_warmup else "bench"

        fetch_stmt = f"""USE {DATAVERSE};
SELECT m.embedding FROM {args.dataset} m WHERE m.id = {query_id};"""
        resp = execute_statement(fetch_stmt, f"{phase}_fetch_{run}")
        result = resp.json()

        if not result.get("results") or not result["results"][0].get("embedding"):
            print(f"  [{run+1}/{total_queries}] id={query_id} — SKIPPED (no embedding)")
            if not is_warmup:
                errors.append(query_id)
            continue

        qvec = result["results"][0]["embedding"]
        qvec_str = json.dumps(qvec)

        if args.nprobe is not None:
            ann_call = f'ann_distance(row.embedding, qvec, "{args.similarity}", {args.nprobe}, {args.epsilon})'
        else:
            ann_call = f'ann_distance(row.embedding, qvec, "{args.similarity}")'

        where_clause = f"WHERE row.year > {args.year}" if args.year is not None else ""

        statement = f"""{pruned_search_prefix(args)}USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {args.dataset} row
LET dist = {ann_call}
{where_clause}
SELECT row.id, dist
ORDER BY dist
LIMIT {args.k};"""

        start = time.time()
        resp = execute_statement(statement, f"{phase}_query_{run}", profile_params if not is_warmup else None)
        wall_time = time.time() - start

        result = resp.json()
        metrics = result.get("metrics", {})

        e_time = parse_time_ms(metrics.get("elapsedTime", "0ms"))
        x_time = parse_time_ms(metrics.get("executionTime", "0ms"))
        c_time = parse_time_ms(metrics.get("compileTime", "0ms"))

        num_results = len(result.get("results", []))
        tag = "[warmup]" if is_warmup else "[bench] "
        recall_str = ""

        if not is_warmup:
            elapsed_times.append(e_time)
            execution_times.append(x_time)
            compile_times.append(c_time)

            if args.profile:
                prof = {
                    "query_id": query_id,
                    "ann_plans": result.get("plans", {}),
                    "ann_profile": result.get("profile", {}),
                }

            ann_ids = set(r.get("id") for r in result.get("results", []))

            knn_call = f'vector_distance(row.embedding, qvec, "{args.similarity}")'
            knn_statement = f"""USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {args.dataset} row
{where_clause}
SELECT row.id, {knn_call} AS dist
ORDER BY {knn_call}
LIMIT {args.k};"""

            knn_resp = execute_statement(knn_statement, f"bench_knn_{run}", profile_params)
            knn_result = knn_resp.json()
            knn_metrics = knn_result.get("metrics", {})

            knn_e = parse_time_ms(knn_metrics.get("elapsedTime", "0ms"))
            knn_x = parse_time_ms(knn_metrics.get("executionTime", "0ms"))
            knn_elapsed_times.append(knn_e)
            knn_execution_times.append(knn_x)

            knn_ids = set(r.get("id") for r in knn_result.get("results", []))
            recall = len(ann_ids & knn_ids) / args.k if args.k > 0 else 0
            recalls.append(recall)
            speedup = knn_e / e_time if e_time > 0 else 0
            recall_str = f", recall={recall:.2%}, speedup={speedup:.1f}x"

            if args.profile:
                prof["knn_plans"] = knn_result.get("plans", {})
                prof["knn_profile"] = knn_result.get("profile", {})

            if args.profile:
                profile_data.append(prof)

        print(f"  {tag} [{run+1}/{total_queries}] id={query_id:>4} — {num_results} results, elapsed={e_time:.1f}ms, exec={x_time:.1f}ms{recall_str}")

    print()

    if not elapsed_times:
        print("[error] No successful queries")
        sys.exit(1)

    def stats(values, label):
        values_sorted = sorted(values)
        n = len(values_sorted)
        avg = sum(values_sorted) / n
        median = values_sorted[n // 2] if n % 2 == 1 else (values_sorted[n // 2 - 1] + values_sorted[n // 2]) / 2
        p95_idx = int(n * 0.95)
        p95 = values_sorted[min(p95_idx, n - 1)]
        p99_idx = int(n * 0.99)
        p99 = values_sorted[min(p99_idx, n - 1)]
        return {"label": label, "avg": avg, "median": median, "min": values_sorted[0], "max": values_sorted[-1], "p95": p95, "p99": p99}

    elapsed_stats = stats(elapsed_times, "Elapsed")
    exec_stats = stats(execution_times, "Execution")
    compile_stats = stats(compile_times, "Compile")

    print("=" * 60)
    print("BENCHMARK RESULTS (excluding warmup)")
    print("=" * 60)
    print(f"  Warmup Queries:     {args.num_queries}")
    print(f"  Benchmark Queries:  {len(elapsed_times)}/{args.num_queries}")
    if errors:
        print(f"  Skipped IDs:        {errors}")
    print()
    print(f"  {'Metric':<12} {'Avg':>10} {'Median':>10} {'Min':>10} {'Max':>10} {'P95':>10} {'P99':>10}")
    print(f"  {'-'*72}")

    for s in [elapsed_stats, exec_stats, compile_stats]:
        print(f"  {s['label']:<12} {s['avg']:>9.1f}ms {s['median']:>9.1f}ms {s['min']:>9.1f}ms {s['max']:>9.1f}ms {s['p95']:>9.1f}ms {s['p99']:>9.1f}ms")

    if knn_elapsed_times:
        knn_elapsed_stats = stats(knn_elapsed_times, "KNN Elapsed")
        knn_exec_stats = stats(knn_execution_times, "KNN Exec")
        print()
        print(f"  {'--- Exact KNN ---'}")
        print(f"  {'Metric':<12} {'Avg':>10} {'Median':>10} {'Min':>10} {'Max':>10} {'P95':>10} {'P99':>10}")
        print(f"  {'-'*72}")
        for s in [knn_elapsed_stats, knn_exec_stats]:
            print(f"  {s['label']:<12} {s['avg']:>9.1f}ms {s['median']:>9.1f}ms {s['min']:>9.1f}ms {s['max']:>9.1f}ms {s['p95']:>9.1f}ms {s['p99']:>9.1f}ms")

    if recalls:
        avg_recall = sum(recalls) / len(recalls)
        min_recall = min(recalls)
        max_recall = max(recalls)
        print()
        print(f"  --- Recall (ANN vs Exact KNN) ---")
        print(f"  Average Recall: {avg_recall:.2%}")
        print(f"  Min Recall:     {min_recall:.2%}")
        print(f"  Max Recall:     {max_recall:.2%}")

    if knn_elapsed_times and elapsed_times:
        avg_speedup_elapsed = (sum(knn_elapsed_times) / len(knn_elapsed_times)) / (sum(elapsed_times) / len(elapsed_times)) if sum(elapsed_times) > 0 else 0
        avg_speedup_exec = (sum(knn_execution_times) / len(knn_execution_times)) / (sum(execution_times) / len(execution_times)) if sum(execution_times) > 0 else 0
        per_query_speedups = [k / a if a > 0 else 0 for k, a in zip(knn_elapsed_times, elapsed_times)]
        min_speedup = min(per_query_speedups)
        max_speedup = max(per_query_speedups)
        print()
        print(f"  --- Speedup (KNN / ANN) ---")
        print(f"  Avg Speedup (elapsed):   {avg_speedup_elapsed:.2f}x")
        print(f"  Avg Speedup (execution): {avg_speedup_exec:.2f}x")
        print(f"  Min Speedup (elapsed):   {min_speedup:.2f}x")
        print(f"  Max Speedup (elapsed):   {max_speedup:.2f}x")

    print()

    if args.output:
        output = {
            "config": {
                "dataset": args.dataset,
                "num_queries": args.num_queries,
                "start_id": args.start_id,
                "warmup_queries": args.num_queries,
                "total_runs": total_queries,
                "k": args.k,
                "similarity": args.similarity,
                "year_filter": args.year,
                "nprobe": args.nprobe,
                "epsilon": args.epsilon,
            },
            "summary": {
                "successful_queries": len(elapsed_times),
                "ann_elapsed_ms": {"avg": elapsed_stats["avg"], "median": elapsed_stats["median"], "min": elapsed_stats["min"], "max": elapsed_stats["max"], "p95": elapsed_stats["p95"], "p99": elapsed_stats["p99"]},
                "ann_execution_ms": {"avg": exec_stats["avg"], "median": exec_stats["median"], "min": exec_stats["min"], "max": exec_stats["max"], "p95": exec_stats["p95"], "p99": exec_stats["p99"]},
                "compile_ms": {"avg": compile_stats["avg"], "median": compile_stats["median"], "min": compile_stats["min"], "max": compile_stats["max"], "p95": compile_stats["p95"], "p99": compile_stats["p99"]},
            },
            "raw_ann_elapsed_ms": elapsed_times,
            "raw_ann_execution_ms": execution_times,
            "raw_compile_ms": compile_times,
        }
        if recalls:
            avg_recall = sum(recalls) / len(recalls)
            output["summary"]["recall"] = {"avg": avg_recall, "min": min(recalls), "max": max(recalls), "per_query": recalls}
            if knn_elapsed_times:
                knn_elapsed_stats = stats(knn_elapsed_times, "KNN Elapsed")
                knn_exec_stats = stats(knn_execution_times, "KNN Exec")
                output["summary"]["knn_elapsed_ms"] = {"avg": knn_elapsed_stats["avg"], "median": knn_elapsed_stats["median"], "min": knn_elapsed_stats["min"], "max": knn_elapsed_stats["max"], "p95": knn_elapsed_stats["p95"], "p99": knn_elapsed_stats["p99"]}
                output["summary"]["knn_execution_ms"] = {"avg": knn_exec_stats["avg"], "median": knn_exec_stats["median"], "min": knn_exec_stats["min"], "max": knn_exec_stats["max"], "p95": knn_exec_stats["p95"], "p99": knn_exec_stats["p99"]}
                avg_speedup_elapsed = (sum(knn_elapsed_times) / len(knn_elapsed_times)) / (sum(elapsed_times) / len(elapsed_times)) if sum(elapsed_times) > 0 else 0
                avg_speedup_exec = (sum(knn_execution_times) / len(knn_execution_times)) / (sum(execution_times) / len(execution_times)) if sum(execution_times) > 0 else 0
                per_query_speedups = [k / a if a > 0 else 0 for k, a in zip(knn_elapsed_times, elapsed_times)]
                output["summary"]["speedup"] = {"avg_elapsed": avg_speedup_elapsed, "avg_execution": avg_speedup_exec, "min_elapsed": min(per_query_speedups), "max_elapsed": max(per_query_speedups), "per_query_elapsed": per_query_speedups}
            output["raw_knn_elapsed_ms"] = knn_elapsed_times
            output["raw_knn_execution_ms"] = knn_execution_times
        if args.profile and profile_data:
            output["profile_data"] = profile_data
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  Results saved to: {args.output}")
        print()


def cmd_regression(args):
    """Run regression test across nprobe x epsilon combinations."""
    nprobes = [int(x) for x in args.nprobes.split(",")]
    epsilons = [float(x) for x in args.epsilons.split(",")]
    num_queries = args.num_queries
    combos = [(np, ep) for np in nprobes for ep in epsilons]
    total_ann = len(combos) * num_queries
    total_all = 1 + num_queries + total_ann

    print("=" * 80)
    print("REGRESSION TEST: nprobe x epsilon")
    print("=" * 80)
    print(f"  Dataset:       {DATAVERSE}.{args.dataset}")
    print(f"  nprobes:       {nprobes}")
    print(f"  epsilons:      {epsilons}")
    print(f"  Combinations:  {len(combos)}")
    print(f"  Queries/combo: {num_queries}")
    print(f"  K:             {args.k}")
    print(f"  Similarity:    {args.similarity}")
    if args.year is not None:
        print(f"  Year Filter:   > {args.year}")
    print(f"  Query IDs:     {args.start_id} to {args.start_id + num_queries - 1}")
    if getattr(args, "pruned_search", False):
        print(f"  Pruned Search: YES")
    print()
    print(f"  Breakdown:     1 warmup + {num_queries} KNN + {total_ann} ANN = {total_all} total queries")
    print("=" * 80)
    print()

    where_clause = f"WHERE row.year > {args.year}" if args.year is not None else ""

    print("[step 1/4] Warming NVMe cache...")
    cache_start = time.time()
    execute_statement(f"USE {DATAVERSE}; SELECT SUM(m.embedding[0]) AS s FROM {args.dataset} m;", "cache_warm")
    print(f"[done] Cache warmed in {time.time() - cache_start:.1f}s")
    print()

    print(f"[step 2/4] Fetching {num_queries} query vectors...")
    query_vectors = {}
    for i in range(num_queries):
        qid = args.start_id + i
        fetch_stmt = f"""USE {DATAVERSE};
SELECT m.embedding FROM {args.dataset} m WHERE m.id = {qid};"""
        resp = execute_statement(fetch_stmt, f"fetch_{qid}")
        result = resp.json()
        if not result.get("results") or not result["results"][0].get("embedding"):
            print(f"  id={qid} — SKIPPED (no embedding)")
            continue
        qvec = result["results"][0]["embedding"]
        query_vectors[qid] = json.dumps(qvec)
    print(f"[done] {len(query_vectors)} vectors fetched")
    print()

    if not query_vectors:
        print("[error] No valid query vectors found")
        sys.exit(1)

    print(f"[step 3/4] Running {len(query_vectors)} exact KNN queries...")
    knn_results = {}
    knn_elapsed = []
    knn_execution = []

    for qid, qvec_str in query_vectors.items():
        knn_call = f'vector_distance(row.embedding, qvec, "{args.similarity}")'
        knn_stmt = f"""USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {args.dataset} row
{where_clause}
SELECT row.id, {knn_call} AS dist
ORDER BY {knn_call}
LIMIT {args.k};"""

        resp = execute_statement(knn_stmt, f"knn_{qid}")
        result = resp.json()
        metrics = result.get("metrics", {})

        knn_e = parse_time_ms(metrics.get("elapsedTime", "0ms"))
        knn_x = parse_time_ms(metrics.get("executionTime", "0ms"))
        knn_elapsed.append(knn_e)
        knn_execution.append(knn_x)

        knn_ids = set(r.get("id") for r in result.get("results", []))
        knn_results[qid] = knn_ids
        print(f"  KNN id={qid:>4} — {len(knn_ids)} results, elapsed={knn_e:.1f}ms")

    avg_knn_elapsed = sum(knn_elapsed) / len(knn_elapsed)
    avg_knn_exec = sum(knn_execution) / len(knn_execution)
    print(f"[done] KNN avg elapsed={avg_knn_elapsed:.1f}ms, avg exec={avg_knn_exec:.1f}ms")
    print()

    print(f"[step 4/4] Running {len(combos)} ANN combinations x {len(query_vectors)} queries...")
    print()

    regression_data = {}

    for combo_idx, (nprobe, epsilon) in enumerate(combos):
        key = f"nprobe={nprobe},eps={epsilon}"
        combo_elapsed = []
        combo_execution = []
        combo_recalls = []

        print(f"  [{combo_idx+1}/{len(combos)}] {key}")

        for qid, qvec_str in query_vectors.items():
            ann_call = f'ann_distance(row.embedding, qvec, "{args.similarity}", {nprobe}, {epsilon})'
            ann_stmt = f"""{pruned_search_prefix(args)}USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {args.dataset} row
LET dist = {ann_call}
{where_clause}
SELECT row.id, dist
ORDER BY dist
LIMIT {args.k};"""

            resp = execute_statement(ann_stmt, f"ann_{nprobe}_{epsilon}_{qid}")
            result = resp.json()
            metrics = result.get("metrics", {})

            e_time = parse_time_ms(metrics.get("elapsedTime", "0ms"))
            x_time = parse_time_ms(metrics.get("executionTime", "0ms"))
            combo_elapsed.append(e_time)
            combo_execution.append(x_time)

            ann_ids = set(r.get("id") for r in result.get("results", []))
            recall = len(ann_ids & knn_results[qid]) / args.k if args.k > 0 else 0
            combo_recalls.append(recall)

            print(f"    id={qid:>4} — elapsed={e_time:.1f}ms, recall={recall:.2%}")

        regression_data[key] = {"nprobe": nprobe, "epsilon": epsilon, "elapsed": combo_elapsed, "execution": combo_execution, "recalls": combo_recalls}

    print()

    print("=" * 110)
    print("REGRESSION RESULTS")
    print("=" * 110)
    print(f"  KNN Baseline:  avg_elapsed={avg_knn_elapsed:.1f}ms, avg_exec={avg_knn_exec:.1f}ms")
    print()

    hdr = f"  {'nprobe':>6}  {'epsilon':>7}  {'Avg Elapsed':>12}  {'Avg Exec':>10}  {'Avg Recall':>10}  {'Min Recall':>10}  {'Max Recall':>10}  {'Speedup(e)':>10}  {'Speedup(x)':>10}"
    print(hdr)
    print(f"  {'-' * (len(hdr) - 2)}")

    summary_rows = []
    for key, data in regression_data.items():
        avg_e = sum(data["elapsed"]) / len(data["elapsed"])
        avg_x = sum(data["execution"]) / len(data["execution"])
        avg_r = sum(data["recalls"]) / len(data["recalls"])
        min_r = min(data["recalls"])
        max_r = max(data["recalls"])
        sp_e = avg_knn_elapsed / avg_e if avg_e > 0 else 0
        sp_x = avg_knn_exec / avg_x if avg_x > 0 else 0

        row = {"nprobe": data["nprobe"], "epsilon": data["epsilon"], "avg_elapsed_ms": avg_e, "avg_exec_ms": avg_x, "avg_recall": avg_r, "min_recall": min_r, "max_recall": max_r, "speedup_elapsed": sp_e, "speedup_exec": sp_x, "raw_elapsed": data["elapsed"], "raw_execution": data["execution"], "raw_recalls": data["recalls"]}
        summary_rows.append(row)

        print(f"  {data['nprobe']:>6}  {data['epsilon']:>7.2f}  {avg_e:>10.1f}ms  {avg_x:>8.1f}ms  {avg_r:>9.2%}  {min_r:>9.2%}  {max_r:>9.2%}  {sp_e:>9.2f}x  {sp_x:>9.2f}x")

    print()

    best_recall = max(summary_rows, key=lambda r: r["avg_recall"])
    best_speed = max(summary_rows, key=lambda r: r["speedup_elapsed"])
    print(f"  Best Recall:   nprobe={best_recall['nprobe']}, eps={best_recall['epsilon']:.2f} — recall={best_recall['avg_recall']:.2%}, speedup={best_recall['speedup_elapsed']:.2f}x")
    print(f"  Best Speedup:  nprobe={best_speed['nprobe']}, eps={best_speed['epsilon']:.2f} — recall={best_speed['avg_recall']:.2%}, speedup={best_speed['speedup_elapsed']:.2f}x")
    print()

    if args.output:
        output = {"config": {"dataset": args.dataset, "nprobes": nprobes, "epsilons": epsilons, "num_queries": num_queries, "start_id": args.start_id, "k": args.k, "similarity": args.similarity, "year_filter": args.year}, "knn_baseline": {"avg_elapsed_ms": avg_knn_elapsed, "avg_exec_ms": avg_knn_exec, "raw_elapsed_ms": knn_elapsed, "raw_exec_ms": knn_execution}, "results": summary_rows}
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  Results saved to: {args.output}")
        print()


def cmd_filter_bench(args):
    """Benchmark inline filter selectivity on vector index."""
    num_queries = args.num_queries

    FILTER_PRESETS = {
        "20%": 'm.title LIKE "%I%" OR m.title LIKE "%II%"',
        "40%": 'm.title LIKE "%I%" OR m.title LIKE "%II%" OR m.title LIKE "%III%" OR m.title LIKE "%IV%"',
        "60%": 'm.title LIKE "%I%" OR m.title LIKE "%II%" OR m.title LIKE "%III%" OR m.title LIKE "%IV%" OR m.title LIKE "%V%" OR m.title LIKE "%VI%"',
    }

    if args.selectivities:
        requested = [s.strip() for s in args.selectivities.split(",")]
        FILTER_PRESETS = {k: v for k, v in FILTER_PRESETS.items() if k in requested}
        if not FILTER_PRESETS:
            print(f"[error] No valid selectivities. Choose from: 20%, 40%, 60%")
            sys.exit(1)

    total_ann = len(FILTER_PRESETS) * num_queries
    total_knn = len(FILTER_PRESETS) * num_queries
    total_all = 1 + total_knn + total_ann

    print("=" * 80)
    print("FILTER BENCHMARK: Inline Filter Selectivity")
    print("=" * 80)
    print(f"  Dataset:        {DATAVERSE}.{args.dataset}")
    print(f"  Selectivities:  {list(FILTER_PRESETS.keys())}")
    print(f"  Queries/level:  {num_queries}")
    print(f"  K:              {args.k}")
    print(f"  Similarity:     {args.similarity}")
    if args.nprobe is not None:
        print(f"  Nprobe:         {args.nprobe}")
        print(f"  Epsilon:        {args.epsilon}")
    print(f"  Query IDs:      {args.start_id} to {args.start_id + num_queries - 1}")
    if getattr(args, "pruned_search", False):
        print(f"  Pruned Search:  YES")
    print()
    print(f"  Breakdown:      1 warmup + {total_knn} KNN + {total_ann} ANN = {total_all} total queries")
    print("=" * 80)
    print()

    print("[step 1/4] Warming NVMe cache...")
    cache_start = time.time()
    execute_statement(f"USE {DATAVERSE}; SELECT SUM(m.embedding[0]) AS s FROM {args.dataset} m;", "cache_warm")
    print(f"[done] Cache warmed in {time.time() - cache_start:.1f}s")
    print()

    print(f"[step 2/4] Fetching {num_queries} query vectors...")
    query_vectors = {}
    for i in range(num_queries):
        qid = args.start_id + i
        fetch_stmt = f"""USE {DATAVERSE};
SELECT m.embedding FROM {args.dataset} m WHERE m.id = {qid};"""
        resp = execute_statement(fetch_stmt, f"fetch_{qid}")
        result = resp.json()
        if not result.get("results") or not result["results"][0].get("embedding"):
            print(f"  id={qid} — SKIPPED (no embedding)")
            continue
        query_vectors[qid] = json.dumps(result["results"][0]["embedding"])
    print(f"[done] {len(query_vectors)} vectors fetched")
    print()

    if not query_vectors:
        print("[error] No valid query vectors found")
        sys.exit(1)

    results_by_sel = {}

    for sel_label, filter_clause in FILTER_PRESETS.items():
        where_clause = f"WHERE {filter_clause}"
        print(f"[step 3/4] Selectivity: {sel_label}")
        print(f"  Filter: {filter_clause}")
        print()

        print(f"  Running {len(query_vectors)} exact KNN queries...")
        knn_results = {}
        knn_elapsed = []
        knn_execution = []

        for qid, qvec_str in query_vectors.items():
            knn_call = f'vector_distance(m.embedding, qvec, "{args.similarity}")'
            knn_stmt = f"""USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {args.dataset} m
{where_clause}
SELECT m.id, {knn_call} AS dist
ORDER BY {knn_call}
LIMIT {args.k};"""

            resp = execute_statement(knn_stmt, f"knn_{sel_label}_{qid}")
            result = resp.json()
            metrics = result.get("metrics", {})

            knn_e = parse_time_ms(metrics.get("elapsedTime", "0ms"))
            knn_x = parse_time_ms(metrics.get("executionTime", "0ms"))
            knn_elapsed.append(knn_e)
            knn_execution.append(knn_x)

            knn_ids = set(r.get("id") for r in result.get("results", []))
            knn_results[qid] = knn_ids
            print(f"    KNN id={qid:>4} — {len(knn_ids)} results, elapsed={knn_e:.1f}ms")

        avg_knn_e = sum(knn_elapsed) / len(knn_elapsed)
        avg_knn_x = sum(knn_execution) / len(knn_execution)
        print(f"  KNN done: avg_elapsed={avg_knn_e:.1f}ms, avg_exec={avg_knn_x:.1f}ms")
        print()

        print(f"  Running {len(query_vectors)} ANN queries...")
        ann_elapsed = []
        ann_execution = []
        recalls = []

        for qid, qvec_str in query_vectors.items():
            if args.nprobe is not None:
                ann_call = f'ann_distance(m.embedding, qvec, "{args.similarity}", {args.nprobe}, {args.epsilon})'
            else:
                ann_call = f'ann_distance(m.embedding, qvec, "{args.similarity}")'

            ann_stmt = f"""{pruned_search_prefix(args)}USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {args.dataset} m
LET dist = {ann_call}
{where_clause}
SELECT m.id, dist
ORDER BY dist
LIMIT {args.k};"""

            resp = execute_statement(ann_stmt, f"ann_{sel_label}_{qid}")
            result = resp.json()
            metrics = result.get("metrics", {})

            e_time = parse_time_ms(metrics.get("elapsedTime", "0ms"))
            x_time = parse_time_ms(metrics.get("executionTime", "0ms"))
            ann_elapsed.append(e_time)
            ann_execution.append(x_time)

            ann_ids = set(r.get("id") for r in result.get("results", []))
            recall = len(ann_ids & knn_results[qid]) / args.k if args.k > 0 else 0
            recalls.append(recall)

            print(f"    ANN id={qid:>4} — elapsed={e_time:.1f}ms, recall={recall:.2%}")

        avg_ann_e = sum(ann_elapsed) / len(ann_elapsed)
        avg_ann_x = sum(ann_execution) / len(ann_execution)
        avg_recall = sum(recalls) / len(recalls)
        print(f"  ANN done: avg_elapsed={avg_ann_e:.1f}ms, avg_exec={avg_ann_x:.1f}ms, avg_recall={avg_recall:.2%}")
        print()

        results_by_sel[sel_label] = {"filter": filter_clause, "knn_elapsed": knn_elapsed, "knn_execution": knn_execution, "ann_elapsed": ann_elapsed, "ann_execution": ann_execution, "recalls": recalls}

    print("=" * 110)
    print("FILTER BENCHMARK RESULTS")
    print("=" * 110)
    print()

    hdr = f"  {'Select':>8}  {'KNN Elapsed':>12}  {'ANN Elapsed':>12}  {'KNN Exec':>10}  {'ANN Exec':>10}  {'Avg Recall':>10}  {'Min Recall':>10}  {'Max Recall':>10}  {'Speedup(e)':>10}  {'Speedup(x)':>10}"
    print(hdr)
    print(f"  {'-' * (len(hdr) - 2)}")

    summary_rows = []
    for sel_label, data in results_by_sel.items():
        avg_knn_e = sum(data["knn_elapsed"]) / len(data["knn_elapsed"])
        avg_knn_x = sum(data["knn_execution"]) / len(data["knn_execution"])
        avg_ann_e = sum(data["ann_elapsed"]) / len(data["ann_elapsed"])
        avg_ann_x = sum(data["ann_execution"]) / len(data["ann_execution"])
        avg_r = sum(data["recalls"]) / len(data["recalls"])
        min_r = min(data["recalls"])
        max_r = max(data["recalls"])
        sp_e = avg_knn_e / avg_ann_e if avg_ann_e > 0 else 0
        sp_x = avg_knn_x / avg_ann_x if avg_ann_x > 0 else 0

        row = {"selectivity": sel_label, "filter": data["filter"], "avg_knn_elapsed_ms": avg_knn_e, "avg_knn_exec_ms": avg_knn_x, "avg_ann_elapsed_ms": avg_ann_e, "avg_ann_exec_ms": avg_ann_x, "avg_recall": avg_r, "min_recall": min_r, "max_recall": max_r, "speedup_elapsed": sp_e, "speedup_exec": sp_x, "raw_knn_elapsed": data["knn_elapsed"], "raw_knn_execution": data["knn_execution"], "raw_ann_elapsed": data["ann_elapsed"], "raw_ann_execution": data["ann_execution"], "raw_recalls": data["recalls"]}
        summary_rows.append(row)

        print(f"  {sel_label:>8}  {avg_knn_e:>10.1f}ms  {avg_ann_e:>10.1f}ms  {avg_knn_x:>8.1f}ms  {avg_ann_x:>8.1f}ms  {avg_r:>9.2%}  {min_r:>9.2%}  {max_r:>9.2%}  {sp_e:>9.2f}x  {sp_x:>9.2f}x")

    print()

    if args.output:
        output = {"config": {"dataset": args.dataset, "selectivities": list(FILTER_PRESETS.keys()), "num_queries": num_queries, "start_id": args.start_id, "k": args.k, "similarity": args.similarity, "nprobe": args.nprobe, "epsilon": args.epsilon}, "results": summary_rows}
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  Results saved to: {args.output}")
        print()


def cmd_knn_bench(args):
    """Benchmark brute-force KNN vs index-driven KNN."""
    num_queries = args.num_queries

    print("=" * 80)
    print("KNN BENCHMARK: Brute-Force vs Index-Driven")
    print("=" * 80)
    print(f"  Dataset:       {DATAVERSE}.{args.dataset}")
    print(f"  Queries:       {num_queries}")
    print(f"  K:             {args.k}")
    print(f"  Similarity:    {args.similarity}")
    if args.year is not None:
        print(f"  Year Filter:   > {args.year}")
    print(f"  Query IDs:     {args.start_id} to {args.start_id + num_queries - 1}")
    print()
    print(f"  Breakdown:     1 warmup + {num_queries} brute-force + {num_queries} index-driven = {1 + num_queries * 2} total")
    print("=" * 80)
    print()

    where_clause = f"WHERE row.year > {args.year}" if args.year is not None else ""

    print("[step 1/4] Warming NVMe cache...")
    cache_start = time.time()
    execute_statement(f"USE {DATAVERSE}; SELECT SUM(m.embedding[0]) AS s FROM {args.dataset} m;", "cache_warm")
    print(f"[done] Cache warmed in {time.time() - cache_start:.1f}s")
    print()

    print(f"[step 2/4] Fetching {num_queries} query vectors...")
    query_vectors = {}
    for i in range(num_queries):
        qid = args.start_id + i
        fetch_stmt = f"""USE {DATAVERSE};
SELECT m.embedding FROM {args.dataset} m WHERE m.id = {qid};"""
        resp = execute_statement(fetch_stmt, f"fetch_{qid}")
        result = resp.json()
        if not result.get("results") or not result["results"][0].get("embedding"):
            print(f"  id={qid} — SKIPPED (no embedding)")
            continue
        query_vectors[qid] = json.dumps(result["results"][0]["embedding"])
    print(f"[done] {len(query_vectors)} vectors fetched")
    print()

    if not query_vectors:
        print("[error] No valid query vectors found")
        sys.exit(1)

    print(f"[step 3/4] Running {len(query_vectors)} brute-force KNN queries...")
    bf_results = {}
    bf_elapsed = []
    bf_execution = []

    for qid, qvec_str in query_vectors.items():
        knn_call = f'vector_distance(row.embedding, qvec, "{args.similarity}")'
        stmt = f"""USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {args.dataset} row
LET dist = {knn_call}
{where_clause}
SELECT row.id, dist
ORDER BY dist
LIMIT {args.k};"""

        resp = execute_statement(stmt, f"bf_knn_{qid}")
        result = resp.json()
        metrics = result.get("metrics", {})

        e_time = parse_time_ms(metrics.get("elapsedTime", "0ms"))
        x_time = parse_time_ms(metrics.get("executionTime", "0ms"))
        bf_elapsed.append(e_time)
        bf_execution.append(x_time)

        bf_ids = set(r.get("id") for r in result.get("results", []))
        bf_results[qid] = bf_ids
        print(f"  BF  id={qid:>4} — {len(bf_ids)} results, elapsed={e_time:.1f}ms")

    avg_bf_e = sum(bf_elapsed) / len(bf_elapsed)
    avg_bf_x = sum(bf_execution) / len(bf_execution)
    print(f"[done] Brute-force avg_elapsed={avg_bf_e:.1f}ms, avg_exec={avg_bf_x:.1f}ms")
    print()

    print(f"[step 4/4] Running {len(query_vectors)} index-driven KNN queries...")
    idx_elapsed = []
    idx_execution = []
    recalls = []

    for qid, qvec_str in query_vectors.items():
        idx_call = f'vector_distance(row.embedding, qvec, "{args.similarity}", true)'
        stmt = f"""{pruned_search_prefix(args)}USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {args.dataset} row
LET dist = {idx_call}
{where_clause}
SELECT row.id, dist
ORDER BY dist
LIMIT {args.k};"""

        resp = execute_statement(stmt, f"idx_knn_{qid}")
        result = resp.json()
        metrics = result.get("metrics", {})

        e_time = parse_time_ms(metrics.get("elapsedTime", "0ms"))
        x_time = parse_time_ms(metrics.get("executionTime", "0ms"))
        idx_elapsed.append(e_time)
        idx_execution.append(x_time)

        idx_ids = set(r.get("id") for r in result.get("results", []))
        recall = len(idx_ids & bf_results[qid]) / args.k if args.k > 0 else 0
        recalls.append(recall)

        speedup = bf_elapsed[len(idx_elapsed) - 1] / e_time if e_time > 0 else 0
        print(f"  IDX id={qid:>4} — {len(idx_ids)} results, elapsed={e_time:.1f}ms, recall={recall:.2%}, speedup={speedup:.1f}x")

    avg_idx_e = sum(idx_elapsed) / len(idx_elapsed)
    avg_idx_x = sum(idx_execution) / len(idx_execution)
    avg_recall = sum(recalls) / len(recalls)
    print(f"[done] Index-driven avg_elapsed={avg_idx_e:.1f}ms, avg_exec={avg_idx_x:.1f}ms, avg_recall={avg_recall:.2%}")
    print()

    per_query_speedups_e = [b / i if i > 0 else 0 for b, i in zip(bf_elapsed, idx_elapsed)]
    per_query_speedups_x = [b / i if i > 0 else 0 for b, i in zip(bf_execution, idx_execution)]
    avg_sp_e = avg_bf_e / avg_idx_e if avg_idx_e > 0 else 0
    avg_sp_x = avg_bf_x / avg_idx_x if avg_idx_x > 0 else 0

    print("=" * 90)
    print("KNN BENCHMARK RESULTS")
    print("=" * 90)
    print()

    hdr = f"  {'Method':<16}  {'Avg Elapsed':>12}  {'Avg Exec':>10}  {'Min':>10}  {'Max':>10}"
    print(hdr)
    print(f"  {'-' * (len(hdr) - 2)}")
    print(f"  {'Brute-Force':<16}  {avg_bf_e:>10.1f}ms  {avg_bf_x:>8.1f}ms  {min(bf_elapsed):>8.1f}ms  {max(bf_elapsed):>8.1f}ms")
    print(f"  {'Index-Driven':<16}  {avg_idx_e:>10.1f}ms  {avg_idx_x:>8.1f}ms  {min(idx_elapsed):>8.1f}ms  {max(idx_elapsed):>8.1f}ms")
    print()
    print(f"  --- Recall (Index-Driven vs Brute-Force) ---")
    print(f"  Average Recall: {avg_recall:.2%}")
    print(f"  Min Recall:     {min(recalls):.2%}")
    print(f"  Max Recall:     {max(recalls):.2%}")
    print()
    print(f"  --- Speedup (Brute-Force / Index-Driven) ---")
    print(f"  Avg Speedup (elapsed):   {avg_sp_e:.2f}x")
    print(f"  Avg Speedup (execution): {avg_sp_x:.2f}x")
    print(f"  Min Speedup (elapsed):   {min(per_query_speedups_e):.2f}x")
    print(f"  Max Speedup (elapsed):   {max(per_query_speedups_e):.2f}x")
    print()

    if args.output:
        output = {"config": {"dataset": args.dataset, "num_queries": num_queries, "start_id": args.start_id, "k": args.k, "similarity": args.similarity, "year_filter": args.year, "pruned_search": getattr(args, "pruned_search", False)}, "brute_force": {"avg_elapsed_ms": avg_bf_e, "avg_exec_ms": avg_bf_x, "raw_elapsed_ms": bf_elapsed, "raw_exec_ms": bf_execution}, "index_driven": {"avg_elapsed_ms": avg_idx_e, "avg_exec_ms": avg_idx_x, "raw_elapsed_ms": idx_elapsed, "raw_exec_ms": idx_execution}, "recall": {"avg": avg_recall, "min": min(recalls), "max": max(recalls), "per_query": recalls}, "speedup": {"avg_elapsed": avg_sp_e, "avg_execution": avg_sp_x, "min_elapsed": min(per_query_speedups_e), "max_elapsed": max(per_query_speedups_e), "per_query_elapsed": per_query_speedups_e}}
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  Results saved to: {args.output}")
        print()


def cmd_filter_regression(args):
    """Run regression test across nprobe x epsilon combinations with filter queries."""
    nprobes = [int(x) for x in args.nprobes.split(",")]
    epsilons = [float(x) for x in args.epsilons.split(",")]
    num_queries = args.num_queries

    if args.selectivities:
        requested = [int(x) for x in args.selectivities.split(",")]
        filter_map = {}
        for s in requested:
            if s not in SELECTIVITY_FILTERS:
                print(f"[error] Unknown selectivity {s}%. Available: {list(SELECTIVITY_FILTERS.keys())}")
                sys.exit(1)
            filter_map[f"{s}%"] = SELECTIVITY_FILTERS[s]
    elif args.filter:
        filter_map = {"custom": args.filter}
    else:
        print("[error] Must specify either --selectivities or --filter")
        sys.exit(1)

    combos_per_sel = len(nprobes) * len(epsilons)
    ann_per_sel = combos_per_sel * num_queries
    warmup_per_sel = 0 if args.no_warmup else ann_per_sel
    total_all = 1 + num_queries + len(filter_map) * (num_queries + ann_per_sel + warmup_per_sel)

    print("=" * 100)
    print("FILTER REGRESSION TEST: nprobe x epsilon x selectivity")
    print("=" * 100)
    print(f"  Dataset:       {DATAVERSE}.{args.dataset}")
    print(f"  Num Queries:   {num_queries}{'' if args.no_warmup else ' (+ warmup per combo)'}")
    print(f"  K:             {args.k}")
    print(f"  Query IDs:     {args.start_id} to {args.start_id + num_queries - 1}")
    print(f"  Similarity:    {args.similarity}")
    print(f"  Selectivities: {list(filter_map.keys())}")
    print(f"  nprobe values: {nprobes}")
    print(f"  epsilon values:{epsilons}")
    print(f"  Combinations:  {combos_per_sel} per selectivity, {combos_per_sel * len(filter_map)} total")
    if getattr(args, "pruned_search", False):
        print(f"  Pruned Search: YES")
    if args.no_warmup:
        print(f"  Warmup:        DISABLED")
    print()
    print(f"  Estimated total queries: ~{total_all}")
    print("=" * 100)
    print()

    # Cache warming
    print("[step 1] Warming NVMe cache...")
    cache_start = time.time()
    execute_statement(f"USE {DATAVERSE}; SELECT SUM(m.embedding[0]) AS s FROM {args.dataset} m;", "cache_warm")
    print(f"[done] Cache warmed in {time.time() - cache_start:.1f}s")
    print()

    # Pre-fetch query vectors
    print(f"[step 2] Fetching {num_queries} query vectors...")
    query_vectors = {}
    for i in range(num_queries):
        qid = args.start_id + i
        fetch_stmt = f"USE {DATAVERSE};\nSELECT m.embedding FROM {args.dataset} m WHERE m.id = {qid};"
        resp = execute_statement(fetch_stmt, f"fetch_qvec_{qid}")
        result = resp.json()
        if result.get("results") and result["results"][0].get("embedding"):
            query_vectors[qid] = json.dumps(result["results"][0]["embedding"])
        else:
            print(f"  [warn] id={qid} has no embedding, skipping")
    print(f"[done] {len(query_vectors)} vectors fetched")
    print()

    if not query_vectors:
        print("[error] No valid query vectors found")
        sys.exit(1)

    all_selectivity_results = {}

    for sel_label, filter_pred in filter_map.items():
        print()
        print("#" * 100)
        print(f"# SELECTIVITY: {sel_label}  |  FILTER: {filter_pred}")
        print("#" * 100)
        print()

        where_clause = f"WHERE {filter_pred}"

        # Exact KNN for this selectivity
        print(f"[step 3] Running {len(query_vectors)} exact KNN queries for {sel_label}...")
        knn_elapsed_all = []
        knn_exec_all = []
        knn_results_all = {}

        for qid, qvec_str in query_vectors.items():
            knn_call = f'vector_distance(m.embedding, qvec, "{args.similarity}")'
            knn_stmt = f"""USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {args.dataset} m
{where_clause}
SELECT m.id, {knn_call} AS dist
ORDER BY {knn_call}
LIMIT {args.k};"""

            resp = execute_statement(knn_stmt, f"knn_{sel_label}_{qid}")
            result = resp.json()
            metrics = result.get("metrics", {})
            knn_e = parse_time_ms(metrics.get("elapsedTime", "0ms"))
            knn_x = parse_time_ms(metrics.get("executionTime", "0ms"))
            knn_elapsed_all.append(knn_e)
            knn_exec_all.append(knn_x)
            knn_results_all[qid] = set(r.get("id") for r in result.get("results", []))
            print(f"  KNN id={qid:>4} — {len(knn_results_all[qid])} results, elapsed={knn_e:.1f}ms")

        avg_knn_elapsed = sum(knn_elapsed_all) / len(knn_elapsed_all)
        avg_knn_exec = sum(knn_exec_all) / len(knn_exec_all)
        print(f"[done] KNN avg elapsed={avg_knn_elapsed:.1f}ms, avg exec={avg_knn_exec:.1f}ms")
        print()

        # ANN for each (nprobe, epsilon) combination
        combos = [(mp, eps) for mp in nprobes for eps in epsilons]
        sel_results = []

        print(f"[step 4] Running {len(combos)} ANN combinations x {len(query_vectors)} queries...")
        print()

        for combo_idx, (mp, eps) in enumerate(combos):
            print(f"  [{combo_idx+1}/{len(combos)}] nprobe={mp}, epsilon={eps}")

            # Warmup (skipped if --no-warmup)
            if not args.no_warmup:
                print(f"    Warmup ({len(query_vectors)} queries)...")
                for qid, qvec_str in query_vectors.items():
                    ann_call = f'ann_distance(m.embedding, qvec, "{args.similarity}", {mp}, {eps})'
                    stmt = f"""{pruned_search_prefix(args)}USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {args.dataset} m
LET dist = {ann_call}
{where_clause}
SELECT m.id, dist
ORDER BY dist
LIMIT {args.k};"""
                    execute_statement(stmt, f"warmup_{sel_label}_{mp}_{eps}_{qid}")

            # Benchmark
            print(f"    Benchmark ({len(query_vectors)} queries)...")
            ann_elapsed = []
            ann_exec = []
            recalls = []

            for qid, qvec_str in query_vectors.items():
                ann_call = f'ann_distance(m.embedding, qvec, "{args.similarity}", {mp}, {eps})'
                stmt = f"""{pruned_search_prefix(args)}USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {args.dataset} m
LET dist = {ann_call}
{where_clause}
SELECT m.id, dist
ORDER BY dist
LIMIT {args.k};"""

                resp = execute_statement(stmt, f"bench_{sel_label}_{mp}_{eps}_{qid}")
                result = resp.json()
                metrics = result.get("metrics", {})

                e_time = parse_time_ms(metrics.get("elapsedTime", "0ms"))
                x_time = parse_time_ms(metrics.get("executionTime", "0ms"))
                ann_elapsed.append(e_time)
                ann_exec.append(x_time)

                ann_ids = set(r.get("id") for r in result.get("results", []))
                recall = len(ann_ids & knn_results_all[qid]) / args.k if args.k > 0 else 0
                recalls.append(recall)

            avg_ann_elapsed = sum(ann_elapsed) / len(ann_elapsed)
            avg_ann_exec = sum(ann_exec) / len(ann_exec)
            avg_recall = sum(recalls) / len(recalls)
            min_recall = min(recalls)
            max_recall = max(recalls)
            speedup_e = avg_knn_elapsed / avg_ann_elapsed if avg_ann_elapsed > 0 else 0
            speedup_x = avg_knn_exec / avg_ann_exec if avg_ann_exec > 0 else 0

            row_result = {
                "nprobe": mp, "epsilon": eps,
                "avg_ann_elapsed_ms": avg_ann_elapsed, "avg_ann_exec_ms": avg_ann_exec,
                "avg_recall": avg_recall, "min_recall": min_recall, "max_recall": max_recall,
                "speedup_elapsed": speedup_e, "speedup_exec": speedup_x,
                "raw_ann_elapsed": ann_elapsed, "raw_ann_execution": ann_exec, "raw_recalls": recalls,
            }
            sel_results.append(row_result)
            print(f"    → Avg Elapsed={avg_ann_elapsed:.1f}ms, Avg Exec={avg_ann_exec:.1f}ms, "
                  f"Recall={avg_recall:.2%} [{min_recall:.2%}–{max_recall:.2%}], "
                  f"Speedup={speedup_e:.2f}×(e)/{speedup_x:.2f}×(x)")

        # Summary table
        print()
        print(f"  {'=' * 97}")
        print(f"  SUMMARY — {sel_label} (KNN baseline: elapsed={avg_knn_elapsed:.1f}ms, exec={avg_knn_exec:.1f}ms)")
        print(f"  {'=' * 97}")
        hdr = f"  {'nprobe':>8}  {'epsilon':>8}  {'Avg Elapsed':>14}  {'Avg Exec':>12}  {'Avg Recall':>10}  {'Min Recall':>10}  {'Max Recall':>10}  {'Speedup(e)':>10}  {'Speedup(x)':>10}"
        print(hdr)
        print(f"  {'-' * 97}")
        for r in sel_results:
            print(f"  {r['nprobe']:>8}  {r['epsilon']:>8.2f}  "
                  f"{r['avg_ann_elapsed_ms']:>11.1f}ms  {r['avg_ann_exec_ms']:>9.1f}ms  "
                  f"{r['avg_recall']:>9.2%}  {r['min_recall']:>9.2%}  {r['max_recall']:>9.2%}  "
                  f"{r['speedup_elapsed']:>9.2f}x  {r['speedup_exec']:>9.2f}x")
        print()

        all_selectivity_results[sel_label] = {
            "filter": filter_pred,
            "knn_baseline": {"avg_elapsed_ms": avg_knn_elapsed, "avg_exec_ms": avg_knn_exec, "raw_elapsed_ms": knn_elapsed_all, "raw_exec_ms": knn_exec_all},
            "results": sel_results,
        }

    # Save to JSON
    if args.output:
        output = {
            "config": {
                "dataset": args.dataset, "selectivities": list(filter_map.keys()),
                "num_queries": num_queries, "start_id": args.start_id, "k": args.k,
                "similarity": args.similarity, "nprobes": nprobes, "epsilons": epsilons,
                "pruned_search": getattr(args, "pruned_search", False),
            },
            "selectivity_results": all_selectivity_results,
        }
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  Results saved to: {args.output}")
        print()


def cmd_indexes(args):
    """List all indexes on the dataset."""
    statement = f"""SELECT * FROM Metadata.`Index` ix
WHERE ix.DataverseName = "{DATAVERSE}" AND ix.DatasetName = "{args.dataset}";"""

    resp = execute_statement(statement, "list_indexes")
    result = resp.json()
    indexes = result.get("results", [])

    if not indexes:
        print(f"No indexes found on {DATAVERSE}.{args.dataset}")
        return

    print(f"Indexes on {DATAVERSE}.{args.dataset}:")
    print("-" * 60)
    for ix in indexes:
        name = ix.get("IndexName", "N/A")
        idx_type = ix.get("IndexType", "N/A")
        is_primary = ix.get("IsPrimary", False)
        search_key = ix.get("SearchKey", [])
        print(f"  {name} ({idx_type}){' [PRIMARY]' if is_primary else ''}")
        if search_key:
            print(f"    Key: {search_key}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="AsterixDB cluster vector index management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dataset", default=DATASET, help=f"Dataset name (default: {DATASET})")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # create
    p_create = subparsers.add_parser("create", help="Create a vector index")
    p_create.add_argument("--name", required=True, help="Index name")
    p_create.add_argument("--num-clusters", type=int, required=True, help="Number of clusters")
    p_create.add_argument("--dimension", type=int, default=384, help="Vector dimension (default: 384)")
    p_create.add_argument("--similarity", default="euclidean_squared", help="Similarity metric (default: euclidean_squared)")
    p_create.add_argument("--train-list", type=int, default=10000, help="Training list size (default: 10000)")
    p_create.add_argument("--quantization", help="Quantization type (e.g., SQ7, SQ8)")
    p_create.add_argument("--include", nargs="+", help="Fields to include in index (e.g., title popularity)")

    # drop
    p_drop = subparsers.add_parser("drop", help="Drop a vector index")
    p_drop.add_argument("--name", required=True, help="Index name")

    # query
    p_query = subparsers.add_parser("query", help="Execute an ANN query")
    p_query.add_argument("--k", type=int, default=50, help="Number of results (default: 50)")
    p_query.add_argument("--year", type=int, help="Year filter (e.g., 2000 for year > 2000)")
    p_query.add_argument("--query-id", type=int, default=0, help="ID of record to use as query vector (default: 0)")
    p_query.add_argument("--similarity", default="euclidean_squared", help="Similarity metric")
    p_query.add_argument("--nprobe", type=int, help="Number of clusters to probe (for quantized)")
    p_query.add_argument("--epsilon", type=float, default=0.2, help="Error tolerance (default: 0.2)")
    p_query.add_argument("--where", help="Additional WHERE clause")
    p_query.add_argument("--pruned-search", action="store_true", help="Enable vector pruned search optimization")

    # status
    subparsers.add_parser("status", help="Check cluster and dataset status")

    # count
    subparsers.add_parser("count", help="Count records in dataset")

    # sample
    p_sample = subparsers.add_parser("sample", help="Sample records from dataset")
    p_sample.add_argument("--limit", type=int, default=5, help="Number of records (default: 5)")

    # indexes
    subparsers.add_parser("indexes", help="List indexes on dataset")

    # benchmark
    p_bench = subparsers.add_parser("benchmark", help="Run multiple ANN queries and report statistics")
    p_bench.add_argument("--num-queries", type=int, default=50, help="Number of queries to run (default: 50)")
    p_bench.add_argument("--start-id", type=int, default=201, help="Starting record ID for query vectors (default: 201)")
    p_bench.add_argument("--k", type=int, default=50, help="Number of results per query (default: 50)")
    p_bench.add_argument("--year", type=int, help="Year filter (e.g., 2000 for year > 2000)")
    p_bench.add_argument("--similarity", default="euclidean_squared", help="Similarity metric")
    p_bench.add_argument("--nprobe", type=int, help="Number of clusters to probe")
    p_bench.add_argument("--epsilon", type=float, default=0.2, help="Error tolerance (default: 0.2)")
    p_bench.add_argument("--output", help="Save results to JSON file (e.g., results.json)")
    p_bench.add_argument("--profile", action="store_true", help="Enable query profiling (timings + optimized logical plan)")
    p_bench.add_argument("--pruned-search", action="store_true", help="Enable vector pruned search optimization")

    # regression
    p_reg = subparsers.add_parser("regression", help="Regression test across nprobe x epsilon combinations")
    p_reg.add_argument("--nprobes", default="10,20,30,40", help="Comma-separated nprobe values (default: 10,20,30,40)")
    p_reg.add_argument("--epsilons", default="0.15,0.2,0.25,0.3", help="Comma-separated epsilon values (default: 0.15,0.2,0.25,0.3)")
    p_reg.add_argument("--num-queries", type=int, default=3, help="Number of queries per combination (default: 3)")
    p_reg.add_argument("--start-id", type=int, default=201, help="Starting record ID for query vectors (default: 201)")
    p_reg.add_argument("--k", type=int, default=50, help="Number of results per query (default: 50)")
    p_reg.add_argument("--year", type=int, help="Year filter (e.g., 2000 for year > 2000)")
    p_reg.add_argument("--similarity", default="euclidean_squared", help="Similarity metric")
    p_reg.add_argument("--output", help="Save results to JSON file (e.g., regression_results.json)")
    p_reg.add_argument("--pruned-search", action="store_true", help="Enable vector pruned search optimization")

    # filter-bench
    p_filt = subparsers.add_parser("filter-bench", help="Benchmark inline filter selectivity on vector index")
    p_filt.add_argument("--selectivities", help="Comma-separated selectivities to test (default: all). Choose from: 20%%,40%%,60%%")
    p_filt.add_argument("--num-queries", type=int, default=3, help="Number of queries per selectivity level (default: 3)")
    p_filt.add_argument("--start-id", type=int, default=201, help="Starting record ID for query vectors (default: 201)")
    p_filt.add_argument("--k", type=int, default=50, help="Number of results per query (default: 50)")
    p_filt.add_argument("--similarity", default="euclidean_squared", help="Similarity metric")
    p_filt.add_argument("--nprobe", type=int, help="Number of clusters to probe")
    p_filt.add_argument("--epsilon", type=float, default=0.2, help="Error tolerance (default: 0.2)")
    p_filt.add_argument("--output", help="Save results to JSON file (e.g., filter_results.json)")
    p_filt.add_argument("--pruned-search", action="store_true", help="Enable vector pruned search optimization")

    # knn-bench
    p_knn = subparsers.add_parser("knn-bench", help="Compare brute-force KNN vs index-driven KNN")
    p_knn.add_argument("--num-queries", type=int, default=3, help="Number of queries (default: 3)")
    p_knn.add_argument("--start-id", type=int, default=201, help="Starting record ID for query vectors (default: 201)")
    p_knn.add_argument("--k", type=int, default=50, help="Number of results per query (default: 50)")
    p_knn.add_argument("--year", type=int, help="Year filter (e.g., 2000 for year > 2000)")
    p_knn.add_argument("--similarity", default="euclidean_squared", help="Similarity metric")
    p_knn.add_argument("--output", help="Save results to JSON file (e.g., knn_bench_results.json)")
    p_knn.add_argument("--pruned-search", action="store_true", help="Enable vector pruned search optimization")

    # filter-regression
    p_filter_reg = subparsers.add_parser("filter-regression", help="Regression test across nprobe x epsilon with filter selectivities")
    p_filter_reg.add_argument("--selectivities", help="Comma-separated selectivity levels: 10,20,40,60")
    p_filter_reg.add_argument("--filter", help='Custom SQL++ filter predicate')
    p_filter_reg.add_argument("--nprobes", default="30,45,60,80,100", help="Comma-separated nprobe values (default: 30,45,60,80,100)")
    p_filter_reg.add_argument("--epsilons", default="0.3,0.4", help="Comma-separated epsilon values (default: 0.3,0.4)")
    p_filter_reg.add_argument("--num-queries", type=int, default=3, help="Number of queries per combination (default: 3)")
    p_filter_reg.add_argument("--start-id", type=int, default=201, help="Starting record ID for query vectors (default: 201)")
    p_filter_reg.add_argument("--k", type=int, default=50, help="Number of results per query (default: 50)")
    p_filter_reg.add_argument("--similarity", default="euclidean_squared", help="Similarity metric")
    p_filter_reg.add_argument("--no-warmup", action="store_true", help="Skip ANN warmup queries for each nprobe/epsilon combination")
    p_filter_reg.add_argument("--output", help="Save results to JSON file")
    p_filter_reg.add_argument("--pruned-search", action="store_true", help="Enable vector pruned search optimization")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "create": cmd_create,
        "drop": cmd_drop,
        "query": cmd_query,
        "status": cmd_status,
        "count": cmd_count,
        "sample": cmd_sample,
        "indexes": cmd_indexes,
        "benchmark": cmd_benchmark,
        "regression": cmd_regression,
        "filter-bench": cmd_filter_bench,
        "knn-bench": cmd_knn_bench,
        "filter-regression": cmd_filter_regression,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()