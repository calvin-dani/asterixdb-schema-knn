"""Part 4: Flush via Restart + COMPACT Tests.

Reuses the dataverse from Part 3. Validates that data survives:
  1. A cluster restart (SIGINT triggers flush, then restart with cleanup=false)
  2. A COMPACT DATASET operation

Tests all targets (e.g., non-quantized and quantized).
"""

import time

from report import TestResult
from asterixdb_client import AsterixDBClient
from asterixdb_lifecycle import AsterixDBLifecycleManager


class TestPart4FlushCompact:
    """Test suite for flush/restart and compaction durability."""

    def __init__(self, client: AsterixDBClient, config: dict,
                 lifecycle: AsterixDBLifecycleManager = None):
        self.client = client
        self.config = config
        self.lifecycle = lifecycle

    def run(self):
        """Run flush/compact tests. Returns list of TestResult."""
        results = []
        p4_cfg = self.config["part4_flush_compact"]
        targets = p4_cfg.get("targets",
                             [{"dataset": p4_cfg.get("target_dataset",
                                                     "MovieDataset")}])

        # Sub-test 1: Restart (flush + recovery) — once for all targets
        restart_result = self._test_restart(p4_cfg, targets)
        results.extend(restart_result)

        # Sub-test 2: Compact — each target independently
        any_restart_fail = any(r.status == "FAIL" for r in restart_result)
        if not any_restart_fail or self.lifecycle is None:
            compact_results = self._test_compact(p4_cfg, targets)
            results.extend(compact_results)

        return results

    def _test_restart(self, p4_cfg, targets):
        """Test that data survives cluster restart for all targets."""
        results = []
        t0 = time.time()

        if self.lifecycle is None:
            for target_cfg in targets:
                results.append(TestResult(
                    part="Part4",
                    test_name=f"restart_{target_cfg['dataset']}",
                    status="FAIL", elapsed_seconds=0,
                    error="Lifecycle manager not available (--skip-lifecycle?)",
                ))
            return results

        try:
            print(f"\n  [step] Restarting cluster (SIGINT -> flush -> restart)...")
            self.lifecycle.restart()

            print(f"  [step] Waiting for cluster to become ACTIVE...")
            health_url = self.config["asterixdb"]["health_url"]
            timeout = self.config["asterixdb"]["startup_timeout"]
            self.client.wait_for_ready(health_url, timeout)

            # Verify each target
            for target_cfg in targets:
                dataset = target_cfg["dataset"]
                print(f"  [step] Verifying PKs after restart for {dataset}...")
                result = self._verify_pks(p4_cfg, dataset)
                result.test_name = f"restart_{dataset}"
                result.elapsed_seconds = time.time() - t0
                results.append(result)

                # ANN verify after restart
                ann_cfg = p4_cfg.get("ann_verify")
                if ann_cfg:
                    ann_result = self._verify_ann_query(
                        p4_cfg, dataset, ann_cfg)
                    ann_result.test_name = f"ann_restart_{dataset}"
                    ann_result.elapsed_seconds = time.time() - t0
                    results.append(ann_result)

        except Exception as e:
            for target_cfg in targets:
                dataset = target_cfg["dataset"]
                if not any(r.test_name == f"restart_{dataset}"
                           for r in results):
                    results.append(TestResult(
                        part="Part4",
                        test_name=f"restart_{dataset}",
                        status="FAIL", elapsed_seconds=time.time() - t0,
                        error=str(e),
                    ))

        return results

    def _test_compact(self, p4_cfg, targets):
        """Test that data survives COMPACT DATASET for each target."""
        results = []
        dataverse = p4_cfg["dataverse"]

        for target_cfg in targets:
            dataset = target_cfg["dataset"]
            t0 = time.time()

            try:
                print(f"\n  [step] Running COMPACT DATASET "
                      f"{dataverse}.{dataset}...")
                self.client.compact_dataset(dataverse, dataset)
                print(f"  [done] Compact complete ({time.time() - t0:.1f}s)")

                print(f"  [step] Verifying PKs after compact for {dataset}...")
                result = self._verify_pks(p4_cfg, dataset)
                result.test_name = f"compact_{dataset}"
                result.elapsed_seconds = time.time() - t0
                results.append(result)

                # ANN verify after compact
                ann_cfg = p4_cfg.get("ann_verify")
                if ann_cfg:
                    ann_result = self._verify_ann_query(
                        p4_cfg, dataset, ann_cfg)
                    ann_result.test_name = f"ann_compact_{dataset}"
                    ann_result.elapsed_seconds = time.time() - t0
                    results.append(ann_result)

            except Exception as e:
                results.append(TestResult(
                    part="Part4",
                    test_name=f"compact_{dataset}",
                    status="FAIL", elapsed_seconds=time.time() - t0,
                    error=str(e),
                ))

        return results

    def _verify_ann_query(self, p4_cfg, dataset, ann_cfg):
        """Verify that deleted PKs do not appear in ANN query results."""
        dataverse = p4_cfg["dataverse"]
        query_pk = ann_cfg["query_pk"]
        vector_field = ann_cfg["vector_field"]
        similarity = ann_cfg["similarity"]
        min_probe_fraction = ann_cfg["min_probe_fraction"]
        k_multiplier = ann_cfg["k_multiplier"]
        limit = ann_cfg["limit"]

        print(f"  [step] ANN verify for {dataset}: fetching query vector "
              f"(pk={query_pk})...")
        qvec = self.client.fetch_embedding(
            dataverse, dataset, "idx", query_pk, vector_field)
        if qvec is None:
            msg = f"Could not fetch embedding for pk={query_pk}"
            print(f"  [FAIL] {dataset}: {msg}")
            return TestResult(
                part="Part4", test_name="ann_verify",
                status="FAIL", error=msg,
            )

        print(f"  [step] Running ANN query (limit={limit}, "
              f"min_probe_fraction={min_probe_fraction}, "
              f"k_multiplier={k_multiplier})...")
        ann_pks = self.client.run_ann_query_pks(
            dataverse, dataset, vector_field, qvec,
            similarity, min_probe_fraction, k_multiplier, limit)

        deleted_pks = set()
        for r in p4_cfg["verify_deleted_ranges"]:
            deleted_pks.update(range(r[0], r[1] + 1))

        leaked = deleted_pks & ann_pks
        if leaked:
            sample = sorted(leaked)[:10]
            msg = (f"{len(leaked)} deleted PKs found in ANN results "
                   f"(sample: {sample})")
            print(f"  [FAIL] {dataset}: {msg}")
            return TestResult(
                part="Part4", test_name="ann_verify",
                status="FAIL", error=msg,
                details={
                    "ann_result_count": len(ann_pks),
                    "leaked_count": len(leaked),
                },
            )

        # Check for missing expected PKs
        expected_pks = set()
        for r in p4_cfg["verify_expected_ranges"]:
            expected_pks.update(range(r[0], r[1] + 1))
        missing = expected_pks - ann_pks
        if missing:
            sample = sorted(missing)[:20]
            msg = (f"ANN missing {len(missing)}/{len(expected_pks)} "
                   f"expected PKs (sample: {sample})")
            print(f"  [FAIL] {dataset}: {msg}")
            return TestResult(
                part="Part4", test_name="ann_verify",
                status="FAIL", error=msg,
                details={
                    "expected_count": len(expected_pks),
                    "ann_result_count": len(ann_pks),
                    "missing_count": len(missing),
                },
            )

        print(f"  [PASS] {dataset}: ANN returned {len(ann_pks)} PKs, "
              f"0 deleted PKs leaked, all {len(expected_pks)} expected PKs present")
        return TestResult(
            part="Part4", test_name="ann_verify",
            status="PASS",
            details={
                "expected_count": len(expected_pks),
                "ann_result_count": len(ann_pks),
            },
        )

    def _verify_pks(self, p4_cfg, dataset):
        """Verify PK ranges match expected state from Part 3."""
        dataverse = p4_cfg["dataverse"]
        pk_field = "idx"  # movie dataset

        actual_pks = self.client.fetch_all_pks(dataverse, dataset, pk_field)

        expected_pks = set()
        for r in p4_cfg["verify_expected_ranges"]:
            expected_pks.update(range(r[0], r[1] + 1))

        deleted_pks = set()
        for r in p4_cfg["verify_deleted_ranges"]:
            deleted_pks.update(range(r[0], r[1] + 1))

        missing = expected_pks - actual_pks
        still_present = deleted_pks & actual_pks

        if missing or still_present:
            errors = []
            if missing:
                sample = sorted(missing)[:10]
                errors.append(f"{len(missing)} expected PKs missing "
                              f"(sample: {sample})")
            if still_present:
                sample = sorted(still_present)[:10]
                errors.append(f"{len(still_present)} deleted PKs still present "
                              f"(sample: {sample})")
            error_msg = "; ".join(errors)
            print(f"  [FAIL] {dataset}: {error_msg}")
            return TestResult(
                part="Part4", test_name="verify",
                status="FAIL", error=error_msg,
                details={
                    "expected_count": len(expected_pks),
                    "actual_count": len(actual_pks),
                },
            )

        print(f"  [PASS] {dataset}: All {len(expected_pks)} expected PKs present, "
              f"all {len(deleted_pks)} deleted PKs absent")
        return TestResult(
            part="Part4", test_name="verify",
            status="PASS",
            details={
                "expected_count": len(expected_pks),
                "actual_count": len(actual_pks),
            },
        )
