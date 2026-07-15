"""Part 1: Index Creation Tests.

Creates one dataverse per source dataset, loads data once into Staging,
then creates multiple target datasets with different index configurations.
"""

import os
import time

from report import TestResult
from asterixdb_client import AsterixDBClient, prepare_limited_dataset


class TestPart1IndexCreation:
    """Test suite for vector index creation across multiple datasets."""

    def __init__(self, client: AsterixDBClient, config: dict, datasets_dir: str):
        self.client = client
        self.config = config
        self.datasets_dir = datasets_dir
        self.datasets_config = config["datasets"]

    def run(self):
        """Run all index creation tests. Returns list of TestResult."""
        results = []
        groups = self.config["part1_index_creation"]["dataverse_groups"]

        for group in groups:
            group_results = self._run_dataverse_group(group)
            results.extend(group_results)

        return results

    def _run_dataverse_group(self, group):
        """Process one dataverse group: load data once, create multiple indexes."""
        results = []
        dataverse = group["dataverse"]
        source_key = group["source_dataset"]
        ds_cfg = self.datasets_config[source_key]
        train_list_fraction = group["train_list_fraction"]
        epsilon = group["epsilon"]

        print(f"\n{'='*60}")
        print(f"  Part 1: Dataverse {dataverse} (source: {source_key})")
        print(f"{'='*60}")

        # Step 1: Prepare limited dataset file if needed
        source_file = os.path.join(self.datasets_dir, ds_cfg["file"])
        if not os.path.exists(source_file):
            for target in group["targets"]:
                results.append(TestResult(
                    part="Part1",
                    test_name=f"{dataverse}/{target['dataset']}",
                    status="FAIL",
                    error=f"Dataset file not found: {source_file}",
                ))
            return results

        load_path = prepare_limited_dataset(source_file, ds_cfg.get("record_limit"))

        # Step 2: Create dataverse and staging dataset
        t0 = time.time()
        try:
            print(f"  [step] Creating dataverse {dataverse}...")
            self.client.create_dataverse(dataverse)

            print(f"  [step] Creating Staging dataset...")
            self.client.create_type_and_dataset(
                dataverse, "StagingType", "Staging",
                ds_cfg["pk_field"], ds_cfg["type_schema"]
            )

            print(f"  [step] Loading data into Staging from {os.path.basename(load_path)}...")
            self.client.load_dataset(dataverse, "Staging", load_path)
            staging_count = self.client.count_records(dataverse, "Staging")
            print(f"  [done] Staging loaded: {staging_count} records "
                  f"({time.time() - t0:.1f}s)")
        except Exception as e:
            for target in group["targets"]:
                results.append(TestResult(
                    part="Part1",
                    test_name=f"{dataverse}/{target['dataset']}",
                    status="FAIL",
                    error=f"Staging setup failed: {e}",
                ))
            return results

        # Step 3: Create each target dataset and index
        for target in group["targets"]:
            result = self._create_target_index(
                dataverse, target, ds_cfg, train_list_fraction, epsilon
            )
            results.append(result)

        return results

    def _create_target_index(self, dataverse, target, ds_cfg,
                             train_list_fraction, epsilon):
        """Create one target dataset and its vector index."""
        dataset_name = target["dataset"]
        include_fields = target.get("include_fields")
        test_name = f"{dataverse}/{dataset_name}"
        t0 = time.time()

        try:
            # Create target type + dataset
            type_name = f"{dataset_name}Type"
            print(f"\n  [step] Creating {test_name}...")
            self.client.create_type_and_dataset(
                dataverse, type_name, dataset_name,
                ds_cfg["pk_field"], ds_cfg["type_schema"]
            )

            # INSERT INTO target (SELECT FROM Staging)
            print(f"  [step] Inserting data from Staging...")
            stmt = f"""
            USE {dataverse};
            INSERT INTO {dataset_name} (SELECT VALUE s FROM Staging s);
            """
            self.client.execute(stmt, f"insert_{dataset_name}")

            count = self.client.count_records(dataverse, dataset_name)
            print(f"  [info] {dataset_name}: {count} records")

            # ANALYZE is required before creating any vector index
            print(f"  [step] Running ANALYZE DATASET...")
            self.client.analyze_dataset(dataverse, dataset_name)

            # Create vector index
            incl_label = f"include={include_fields}" if include_fields else "no-include"
            print(f"  [step] Creating vector index ({incl_label})...")

            self.client.create_vector_index(
                dataverse=dataverse,
                dataset_name=dataset_name,
                index_name=f"ix_{dataset_name}",
                vector_field=ds_cfg["vector_field"],
                dimension=ds_cfg["dimension"],
                similarity=ds_cfg["similarity"],
                train_list_fraction=train_list_fraction,
                epsilon=epsilon,
                include_fields=include_fields,
            )

            elapsed = time.time() - t0
            print(f"  [done] {test_name} index created ({elapsed:.1f}s)")

            return TestResult(
                part="Part1",
                test_name=test_name,
                status="PASS",
                elapsed_seconds=elapsed,
                details={"count": count},
            )

        except Exception as e:
            elapsed = time.time() - t0
            return TestResult(
                part="Part1",
                test_name=test_name,
                status="FAIL",
                elapsed_seconds=elapsed,
                error=str(e),
            )
