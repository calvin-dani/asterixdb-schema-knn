"""Part 2: ANN Query Recall and Speedup Tests.

Reuses dataverses created by Part 1. For each test case, runs multiple
ANN queries and compares recall against exact brute-force KNN.
"""

import os
import time

from report import TestResult
from asterixdb_client import (
    AsterixDBClient,
    load_query_vector_from_file,
)


class TestPart2AnnQuery:
    """Test suite for ANN query recall and speedup validation."""

    def __init__(self, client: AsterixDBClient, config: dict, datasets_dir: str):
        self.client = client
        self.config = config
        self.datasets_dir = datasets_dir
        self.datasets_config = config["datasets"]

    def run(self):
        """Run all ANN query test cases. Returns list of TestResult."""
        results = []
        p2_cfg = self.config["part2_ann_query"]
        k = p2_cfg["k"]
        num_queries = p2_cfg["num_query_vectors"]
        start_idx = p2_cfg["query_start_idx"]

        for tc in p2_cfg["test_cases"]:
            result = self._run_test_case(tc, k, num_queries, start_idx)
            results.append(result)

        return results

    def _run_test_case(self, tc, k, num_queries, start_idx):
        """Run a single test case: multiple ANN queries vs exact KNN."""
        name = tc["name"]
        dataverse = tc["dataverse"]
        dataset = tc["dataset"]
        source_key = tc["source_dataset"]
        min_probe_fraction = tc["min_probe_fraction"]
        k_multiplier = tc["k_multiplier"]
        where_clause = tc.get("where_clause")
        warn_thresh = tc["recall_warn_threshold"]
        fail_thresh = tc["recall_fail_threshold"]

        ds_cfg = self.datasets_config[source_key]
        vector_field = ds_cfg["vector_field"]
        similarity = ds_cfg["similarity"]
        pk_field = ds_cfg["pk_field"]

        # Resolve dataset file path for loading query vectors
        source_file = os.path.join(self.datasets_dir, ds_cfg["file"])
        limit = ds_cfg.get("record_limit")
        if limit:
            limited = f"{source_file}.limited_{limit}"
            if os.path.exists(limited):
                source_file = limited

        print(f"\n  --- Test: {name} ---")
        filter_info = f" (filter: {where_clause})" if where_clause else ""
        print(f"  Dataset: {dataverse}.{dataset}{filter_info}")
        print(f"  min_probe_fraction={min_probe_fraction}, k_multiplier={k_multiplier}, k={k}")

        t0 = time.time()
        recalls = []
        speedups = []

        try:
            for qi in range(num_queries):
                pk_value = start_idx + qi

                # Load query vector from file (fast) with fallback to DB
                qvec = None
                if os.path.exists(source_file):
                    qvec = load_query_vector_from_file(
                        source_file, pk_field, pk_value, vector_field
                    )
                if qvec is None:
                    qvec = self.client.fetch_embedding(
                        dataverse, dataset, pk_field, pk_value, vector_field
                    )
                if qvec is None:
                    print(f"    q{qi}: SKIP (no embedding for {pk_field}={pk_value})")
                    continue

                # ANN query
                ann = self.client.run_ann_query(
                    dataverse, dataset, vector_field, qvec,
                    similarity, k, min_probe_fraction, k_multiplier,
                    where_clause=where_clause,
                )

                # Exact KNN query
                knn = self.client.run_exact_knn_query(
                    dataverse, dataset, vector_field, qvec,
                    similarity, k,
                    where_clause=where_clause,
                )

                # Compute recall
                ann_ids = set(r.get(pk_field) for r in ann["results"])
                knn_ids = set(r.get(pk_field) for r in knn["results"])
                recall = len(ann_ids & knn_ids) / k if k > 0 else 0.0
                recalls.append(recall)

                # Compute speedup
                speedup = (knn["elapsed_ms"] / ann["elapsed_ms"]
                           if ann["elapsed_ms"] > 0 else 0.0)
                speedups.append(speedup)

                print(f"    q{qi} (idx={pk_value}): recall={recall:.2%}, "
                      f"ann={ann['elapsed_ms']:.0f}ms, "
                      f"knn={knn['elapsed_ms']:.0f}ms, "
                      f"speedup={speedup:.1f}x")

            elapsed = time.time() - t0

            if not recalls:
                return TestResult(
                    part="Part2", test_name=name, status="FAIL",
                    elapsed_seconds=elapsed,
                    error="No valid query vectors found",
                )

            avg_recall = sum(recalls) / len(recalls)
            min_recall = min(recalls)
            avg_speedup = sum(speedups) / len(speedups) if speedups else 0.0

            # Determine status (use avg_recall for both thresholds)
            if avg_recall < fail_thresh:
                status = "FAIL"
                error = (f"avg_recall={avg_recall:.2%} < "
                         f"fail_threshold={fail_thresh:.2%}")
            elif avg_recall < warn_thresh:
                status = "WARN"
                error = (f"avg_recall={avg_recall:.2%} < "
                         f"warn_threshold={warn_thresh:.2%}")
            else:
                status = "PASS"
                error = None

            details = {
                "avg_recall": round(avg_recall, 4),
                "min_recall": round(min_recall, 4),
                "avg_speedup": round(avg_speedup, 2),
                "recalls": [round(r, 4) for r in recalls],
            }

            print(f"  Summary: avg_recall={avg_recall:.2%}, "
                  f"min_recall={min_recall:.2%}, "
                  f"avg_speedup={avg_speedup:.1f}x → {status}")

            return TestResult(
                part="Part2", test_name=name, status=status,
                elapsed_seconds=elapsed, error=error, details=details,
            )

        except Exception as e:
            elapsed = time.time() - t0
            return TestResult(
                part="Part2", test_name=name, status="FAIL",
                elapsed_seconds=elapsed, error=str(e),
            )
