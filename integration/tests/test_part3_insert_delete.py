"""Part 3: Insert/Delete Correctness Tests.

Creates a fresh dataverse, loads initial data, creates vector index,
then tests insert and delete operations with PK verification.
Runs for each target listed under part3_insert_delete.targets.
"""

import os
import time

from report import TestResult
from asterixdb_client import AsterixDBClient, prepare_limited_dataset


class TestPart3InsertDelete:
    """Test suite for insert/delete correctness on vector-indexed datasets."""

    def __init__(self, client: AsterixDBClient, config: dict, datasets_dir: str):
        self.client = client
        self.config = config
        self.datasets_dir = datasets_dir

    def run(self):
        """Run insert/delete test for each target. Returns list of TestResult."""
        results = []
        p3_cfg = self.config["part3_insert_delete"]
        source_key = p3_cfg["source_dataset"]
        ds_cfg = self.config["datasets"][source_key]

        dataverse = p3_cfg["dataverse"]
        staging = p3_cfg["staging_dataset"]
        pk_field = ds_cfg["pk_field"]

        print(f"\n{'='*60}")
        print(f"  Part 3: Insert/Delete Correctness")
        print(f"{'='*60}")

        # Step 1: Setup — create dataverse, load staging (shared across targets)
        t0 = time.time()
        try:
            result = self._setup(p3_cfg, ds_cfg, dataverse, staging, pk_field)
            if result:
                results.append(result)
                return results
        except Exception as e:
            results.append(TestResult(
                part="Part3", test_name="setup",
                status="FAIL", elapsed_seconds=time.time() - t0,
                error=str(e),
            ))
            return results

        # Step 2: Run insert/delete/verify for each target
        targets = p3_cfg.get("targets", [{"dataset": "MovieDataset"}])
        for target_cfg in targets:
            target = target_cfg["dataset"]
            print(f"\n  --- Target: {target} ---")

            t_target = time.time()
            try:
                target_results = self._run_target(
                    p3_cfg, ds_cfg, dataverse, staging,
                    target, pk_field)
                for r in target_results:
                    if r.elapsed_seconds is None or r.elapsed_seconds == 0:
                        r.elapsed_seconds = time.time() - t_target
                results.extend(target_results)
            except Exception as e:
                results.append(TestResult(
                    part="Part3",
                    test_name=f"insert_delete_{target}",
                    status="FAIL", elapsed_seconds=time.time() - t_target,
                    error=str(e),
                ))

        return results

    def _setup(self, p3_cfg, ds_cfg, dataverse, staging, pk_field):
        """Create dataverse and load staging dataset."""
        source_file = os.path.join(self.datasets_dir, ds_cfg["file"])
        if not os.path.exists(source_file):
            return TestResult(
                part="Part3", test_name="setup", status="FAIL",
                error=f"Dataset file not found: {source_file}",
            )

        print(f"  [step] Creating dataverse {dataverse}...")
        self.client.create_dataverse(dataverse)

        print(f"  [step] Creating Staging dataset...")
        self.client.create_type_and_dataset(
            dataverse, "StagingType", staging,
            pk_field, ds_cfg["type_schema"]
        )

        print(f"  [step] Loading all records into Staging...")
        self.client.load_dataset(dataverse, staging, source_file)
        count = self.client.count_records(dataverse, staging)
        print(f"  [done] Staging: {count} records")
        return None  # success

    def _run_target(self, p3_cfg, ds_cfg, dataverse, staging, target,
                    pk_field):
        """Run the full insert/delete/verify cycle for one target dataset."""
        # Create target dataset
        type_name = f"{target}Type"
        print(f"  [step] Creating target dataset {target}...")
        self.client.create_type_and_dataset(
            dataverse, type_name, target,
            pk_field, ds_cfg["type_schema"]
        )

        # Seed with initial records
        initial_limit = p3_cfg["initial_load_limit"]
        print(f"  [step] Seeding {target} with first {initial_limit} records...")
        stmt = f"""
        USE {dataverse};
        INSERT INTO {target} (
          SELECT VALUE s FROM {staging} s
          WHERE s.{pk_field} >= 0 AND s.{pk_field} < {initial_limit}
        );
        """
        self.client.execute(stmt, f"seed_{target}")
        count = self.client.count_records(dataverse, target)
        print(f"  [info] {target} seeded: {count} records")

        # ANALYZE is required before creating any vector index
        print(f"  [step] Running ANALYZE DATASET on {target}...")
        self.client.analyze_dataset(dataverse, target)

        # Create vector index. cross_pollination_m>1 puts each record in M leaf
        # clusters; the insert/delete cycle below then exercises multi-cluster
        # placement, and the index-only ANN verify (PK-only projection) would
        # surface any replica a broken multi-cluster delete failed to cancel.
        cross_pollination_m = p3_cfg.get("cross_pollination_m", 1)
        rng_factor = p3_cfg.get("rng_factor", 1.0)
        print(f"  [step] Creating vector index on {target} "
              f"(cross_pollination_m={cross_pollination_m}, rng_factor={rng_factor})...")
        self.client.create_vector_index(
            dataverse=dataverse,
            dataset_name=target,
            index_name=f"ix_{target}",
            vector_field=ds_cfg["vector_field"],
            dimension=ds_cfg["dimension"],
            similarity=ds_cfg["similarity"],
            train_list_fraction=p3_cfg["train_list_fraction"],
            epsilon=p3_cfg["epsilon"],
            cross_pollination_m=cross_pollination_m,
            rng_factor=rng_factor,
        )
        print(f"  [done] Vector index created")

        # Insert records
        insert_range = p3_cfg["insert_range"]
        print(f"  [step] Inserting records [{insert_range[0]}, {insert_range[1]}]...")
        self.client.insert_from_staging(
            dataverse, staging, target, pk_field,
            insert_range[0], insert_range[1]
        )
        count = self.client.count_records(dataverse, target)
        print(f"  [done] Insert complete. Count: {count}")

        target_results = []

        # === PRE-DELETE ANN VERIFICATION ===
        # Run ANN query BEFORE deletes to verify all records are reachable
        ann_cfg = p3_cfg.get("ann_verify")
        if ann_cfg:
            pre_delete_result = self._verify_ann_pre_delete(
                p3_cfg, ds_cfg, dataverse, target, initial_limit,
                insert_range, ann_cfg)
            if pre_delete_result:
                target_results.append(pre_delete_result)

        # Delete records
        for del_range in p3_cfg["delete_ranges"]:
            print(f"  [step] Deleting records [{del_range[0]}, {del_range[1]}]...")
            self.client.delete_by_pk_range(
                dataverse, target, pk_field, del_range[0], del_range[1]
            )
        count = self.client.count_records(dataverse, target)
        print(f"  [done] Deletes complete. Count: {count}")

        # Verify PKs
        pk_result = self._verify_pks(
            dataverse, target, pk_field,
            p3_cfg["verify_expected_ranges"],
            p3_cfg["verify_deleted_ranges"],
        )
        target_results.append(pk_result)

        # Verify ANN query (deleted PKs must not appear)
        ann_cfg = p3_cfg.get("ann_verify")
        if ann_cfg:
            ann_result = self._verify_ann_query(
                dataverse, target,
                p3_cfg["verify_deleted_ranges"],
                ann_cfg,
            )
            target_results.append(ann_result)

        return target_results

    def _verify_ann_query(self, dataverse, target, deleted_ranges, ann_cfg):
        """Verify that deleted PKs do not appear in ANN query results."""
        test_name = f"ann_verify_{target}"
        query_pk = ann_cfg["query_pk"]
        vector_field = ann_cfg["vector_field"]
        similarity = ann_cfg["similarity"]
        min_probe_fraction = ann_cfg["min_probe_fraction"]
        k_multiplier = ann_cfg["k_multiplier"]
        limit = ann_cfg["limit"]

        print(f"  [step] ANN verify for {target}: fetching query vector "
              f"(pk={query_pk})...")
        qvec = self.client.fetch_embedding(
            dataverse, target, "idx", query_pk, vector_field)
        if qvec is None:
            msg = f"Could not fetch embedding for pk={query_pk}"
            print(f"  [FAIL] {target}: {msg}")
            return TestResult(
                part="Part3", test_name=test_name,
                status="FAIL", error=msg,
            )

        print(f"  [step] Running ANN query (limit={limit}, "
              f"min_probe_fraction={min_probe_fraction}, "
              f"k_multiplier={k_multiplier})...")
        ann_pks = self.client.run_ann_query_pks(
            dataverse, target, vector_field, qvec,
            similarity, min_probe_fraction, k_multiplier, limit)

        deleted_pks = set()
        for r in deleted_ranges:
            deleted_pks.update(range(r[0], r[1] + 1))

        leaked = deleted_pks & ann_pks
        if leaked:
            sample = sorted(leaked)[:10]
            msg = (f"{len(leaked)} deleted PKs found in ANN results "
                   f"(sample: {sample})")
            print(f"  [FAIL] {target}: {msg}")
            return TestResult(
                part="Part3", test_name=test_name,
                status="FAIL", error=msg,
                details={
                    "ann_result_count": len(ann_pks),
                    "leaked_count": len(leaked),
                },
            )

        # Check for missing expected PKs
        expected_pks = set()
        for r in self.config["part3_insert_delete"]["verify_expected_ranges"]:
            expected_pks.update(range(r[0], r[1] + 1))
        missing = expected_pks - ann_pks
        if missing:
            initial_limit = self.config["part3_insert_delete"]["initial_load_limit"]
            from_disk = sorted(pk for pk in missing if pk < initial_limit)
            from_mem = sorted(pk for pk in missing if pk >= initial_limit)
            msg = (f"ANN post-delete missing {len(missing)}/{len(expected_pks)} "
                   f"expected PKs (disk: {len(from_disk)}, mem: {len(from_mem)})")
            print(f"  [FAIL] {target}: {msg}")
            print(f"         From disk (pk<{initial_limit}): "
                  f"{from_disk[:20]}")
            print(f"         From memory (pk>={initial_limit}): "
                  f"{from_mem[:20]}")
            return TestResult(
                part="Part3", test_name=test_name,
                status="FAIL", error=msg,
                details={
                    "expected_count": len(expected_pks),
                    "ann_result_count": len(ann_pks),
                    "missing_count": len(missing),
                },
            )

        print(f"  [PASS] {target}: ANN returned {len(ann_pks)} PKs, "
              f"0 deleted PKs leaked, all {len(expected_pks)} expected PKs present")
        return TestResult(
            part="Part3", test_name=test_name,
            status="PASS",
            details={
                "expected_count": len(expected_pks),
                "ann_result_count": len(ann_pks),
            },
        )

    def _verify_ann_pre_delete(self, p3_cfg, ds_cfg, dataverse, target,
                               initial_limit, insert_range, ann_cfg):
        """Run ANN query BEFORE deletes to verify all records are reachable.
        Returns TestResult — FAIL if ANN result count doesn't match expected."""
        test_name = f"ann_pre_delete_{target}"
        query_pk = ann_cfg["query_pk"]
        vector_field = ann_cfg["vector_field"]
        similarity = ann_cfg["similarity"]
        min_probe_fraction = ann_cfg["min_probe_fraction"]
        k_multiplier = ann_cfg["k_multiplier"]
        limit = ann_cfg["limit"]

        print(f"\n  === PRE-DELETE ANN VERIFICATION for {target} ===")
        print(f"  [step] Fetching query vector (pk={query_pk})...")
        qvec = self.client.fetch_embedding(
            dataverse, target, "idx", query_pk, vector_field)
        if qvec is None:
            msg = f"Could not fetch embedding for pk={query_pk}"
            print(f"  [FAIL] {target}: {msg}")
            return TestResult(
                part="Part3", test_name=test_name,
                status="FAIL", error=msg,
            )

        print(f"  [step] Running ANN query (limit={limit}, "
              f"min_probe_fraction={min_probe_fraction}, "
              f"k_multiplier={k_multiplier})...")
        ann_pks = self.client.run_ann_query_pks(
            dataverse, target, vector_field, qvec,
            similarity, min_probe_fraction, k_multiplier, limit)

        # At this point, ALL records should be present (no deletes yet)
        # Expected: 0..initial_limit-1 (disk) UNION insert_range[0]..insert_range[1] (memory)
        all_expected = set(range(0, initial_limit)) | set(range(insert_range[0], insert_range[1] + 1))

        missing = all_expected - ann_pks
        extra = ann_pks - all_expected

        print(f"  [info] PRE-DELETE: ANN returned {len(ann_pks)} PKs, "
              f"expected {len(all_expected)}")

        if missing:
            from_disk = sorted(pk for pk in missing if pk < initial_limit)
            from_mem = sorted(pk for pk in missing if pk >= initial_limit)
            msg = (f"ANN pre-delete missing {len(missing)}/{len(all_expected)} PKs "
                   f"(disk: {len(from_disk)}, mem: {len(from_mem)})")
            print(f"  [FAIL] {target}: {msg}")
            print(f"         From disk component (pk<{initial_limit}): "
                  f"{len(from_disk)} {from_disk[:20]}")
            print(f"         From memory component (pk>={initial_limit}): "
                  f"{len(from_mem)} {from_mem[:20]}")
            return TestResult(
                part="Part3", test_name=test_name,
                status="FAIL", error=msg,
                details={
                    "expected_count": len(all_expected),
                    "ann_result_count": len(ann_pks),
                    "missing_count": len(missing),
                },
            )

        if extra:
            print(f"  [WARN] PRE-DELETE EXTRA: {len(extra)} unexpected PKs in ANN results")

        print(f"  [PASS] {target}: ANN pre-delete returned all "
              f"{len(all_expected)} expected PKs")
        return TestResult(
            part="Part3", test_name=test_name,
            status="PASS",
            details={
                "expected_count": len(all_expected),
                "ann_result_count": len(ann_pks),
            },
        )

    def _verify_pks(self, dataverse, target, pk_field, expected_ranges,
                    deleted_ranges):
        """Verify that expected PKs are present and deleted PKs are absent."""
        print(f"  [step] Verifying primary keys for {target}...")
        actual_pks = self.client.fetch_all_pks(dataverse, target, pk_field)

        # Build expected sets
        expected_pks = set()
        for r in expected_ranges:
            expected_pks.update(range(r[0], r[1] + 1))

        deleted_pks = set()
        for r in deleted_ranges:
            deleted_pks.update(range(r[0], r[1] + 1))

        # Check for missing expected PKs
        missing = expected_pks - actual_pks
        # Check for present deleted PKs
        still_present = deleted_pks & actual_pks

        test_name = f"insert_delete_{target}"

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
            print(f"  [FAIL] {target}: {error_msg}")
            return TestResult(
                part="Part3", test_name=test_name,
                status="FAIL", error=error_msg,
                details={
                    "expected_count": len(expected_pks),
                    "actual_count": len(actual_pks),
                    "missing_count": len(missing),
                    "still_present_count": len(still_present),
                },
            )

        print(f"  [PASS] {target}: All {len(expected_pks)} expected PKs present, "
              f"all {len(deleted_pks)} deleted PKs absent. "
              f"Actual count: {len(actual_pks)}")
        return TestResult(
            part="Part3", test_name=test_name,
            status="PASS",
            details={
                "expected_count": len(expected_pks),
                "actual_count": len(actual_pks),
            },
        )
