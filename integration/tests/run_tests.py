#!/usr/bin/env python3
"""
Integration Test Suite for AsterixDB Vector Index Extension.

Usage:
  python run_tests.py                          # Run all parts
  python run_tests.py --parts 1 2              # Run only parts 1 and 2
  python run_tests.py --config my_config.yaml  # Use custom config
  python run_tests.py --skip-lifecycle         # Assume AsterixDB already running
  python run_tests.py --output results.json    # Save results to JSON
"""

import argparse
import os
import sys

import yaml

from report import TestReporter
from asterixdb_client import AsterixDBClient
from asterixdb_lifecycle import AsterixDBLifecycleManager
from test_part1_index_creation import TestPart1IndexCreation
from test_part2_ann_query import TestPart2AnnQuery
from test_part3_insert_delete import TestPart3InsertDelete
from test_part4_flush_compact import TestPart4FlushCompact


def load_config(config_path):
    """Load and resolve paths in config."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    config_dir = os.path.dirname(os.path.abspath(config_path))
    config["_config_dir"] = config_dir

    # Resolve datasets_dir relative to config file
    datasets_dir = config.get("datasets_dir", "../datasets")
    config["_datasets_dir"] = os.path.normpath(
        os.path.join(config_dir, datasets_dir)
    )

    return config


def main():
    parser = argparse.ArgumentParser(
        description="AsterixDB Vector Index Integration Test Suite"
    )
    parser.add_argument(
        "--config", default=None,
        help="Path to config YAML (default: config.yaml in script dir)"
    )
    parser.add_argument(
        "--parts", nargs="+", type=int, default=None,
        help="Run only specified parts (e.g., --parts 1 2)"
    )
    parser.add_argument(
        "--skip-lifecycle", action="store_true",
        help="Skip AsterixDB start/stop (assume already running)"
    )
    parser.add_argument(
        "--output", default=None,
        help="Save results to JSON file"
    )
    args = parser.parse_args()

    # Resolve config path
    if args.config:
        config_path = args.config
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "config.yaml")

    if not os.path.exists(config_path):
        print(f"Error: config file not found: {config_path}")
        sys.exit(1)

    config = load_config(config_path)
    datasets_dir = config["_datasets_dir"]
    config_dir = config["_config_dir"]

    parts_to_run = set(args.parts) if args.parts else {1, 2, 3, 4}

    print("=" * 60)
    print("  AsterixDB Vector Index Integration Test Suite")
    print("=" * 60)
    print(f"  Config:      {config_path}")
    print(f"  Datasets:    {datasets_dir}")
    print(f"  Parts:       {sorted(parts_to_run)}")
    print(f"  Lifecycle:   {'skip' if args.skip_lifecycle else 'managed'}")
    print("=" * 60)

    # Initialize components
    ax_cfg = config["asterixdb"]
    client = AsterixDBClient(ax_cfg["url"])
    reporter = TestReporter()
    lifecycle = None

    # Start AsterixDB if needed
    if not args.skip_lifecycle:
        lifecycle = AsterixDBLifecycleManager(ax_cfg, config_dir)
        try:
            lifecycle.start(cleanup=True)
            client.wait_for_ready(ax_cfg["health_url"], ax_cfg["startup_timeout"])
        except Exception as e:
            print(f"\nFATAL: Could not start AsterixDB: {e}")
            sys.exit(1)
    else:
        # Verify connectivity
        try:
            client.wait_for_ready(ax_cfg["health_url"], timeout=10)
        except Exception as e:
            print(f"\nFATAL: AsterixDB not reachable: {e}")
            sys.exit(1)

    try:
        # Part 1: Index Creation
        if 1 in parts_to_run:
            print(f"\n{'#'*60}")
            print(f"  PART 1: INDEX CREATION")
            print(f"{'#'*60}")
            test = TestPart1IndexCreation(client, config, datasets_dir)
            for result in test.run():
                reporter.add_result(result)

        # Part 2: ANN Query Recall
        if 2 in parts_to_run:
            print(f"\n{'#'*60}")
            print(f"  PART 2: ANN QUERY RECALL")
            print(f"{'#'*60}")
            test = TestPart2AnnQuery(client, config, datasets_dir)
            for result in test.run():
                reporter.add_result(result)

        # Part 3: Insert/Delete Correctness
        if 3 in parts_to_run:
            print(f"\n{'#'*60}")
            print(f"  PART 3: INSERT/DELETE CORRECTNESS")
            print(f"{'#'*60}")
            test = TestPart3InsertDelete(client, config, datasets_dir)
            for result in test.run():
                reporter.add_result(result)

        # Part 4: Flush + Compact
        if 4 in parts_to_run:
            print(f"\n{'#'*60}")
            print(f"  PART 4: FLUSH + COMPACT")
            print(f"{'#'*60}")
            test = TestPart4FlushCompact(client, config, lifecycle)
            for result in test.run():
                reporter.add_result(result)

    finally:
        # Stop AsterixDB
        if lifecycle and not args.skip_lifecycle:
            try:
                lifecycle.stop()
            except Exception as e:
                print(f"Warning: Error stopping AsterixDB: {e}")

    # Report
    reporter.print_summary()
    if args.output:
        reporter.save_json(args.output)

    sys.exit(reporter.exit_code())


if __name__ == "__main__":
    main()
