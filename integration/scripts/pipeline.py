import os
import sys
import requests
import subprocess


BASE_URL = "https://ann-benchmarks.com"


def run_subprocess(cmd, cwd=None):
    """Run a shell command and stream its output live."""
    print(f"[run] {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd)
    if result.returncode != 0:
        print(f"[run] FAILED (exit code {result.returncode})")
        sys.exit(result.returncode)


def download_hdf5(dataset_name, raw_dir):
    """Download HDF5 file if it does not exist."""
    os.makedirs(raw_dir, exist_ok=True)
    hdf5_path = os.path.join(raw_dir, f"{dataset_name}.hdf5")

    if os.path.exists(hdf5_path):
        print(f"[download] Already exists: {hdf5_path}")
        return hdf5_path

    url = f"{BASE_URL}/{dataset_name}.hdf5"
    print(f"[download] Downloading: {url}")
    print(f"[download] -> {hdf5_path}")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(hdf5_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    print("[download] Done.")
    return hdf5_path


def clean_dataset(dataset_name, base_dir):
    """Remove all generated files for a dataset, including subdatasets."""
    files = [
        os.path.join(base_dir, "raw", f"{dataset_name}.hdf5"),
        os.path.join(base_dir, "datasets", f"{dataset_name}_train.jsonl"),
        os.path.join(base_dir, "tests", f"{dataset_name}_test.jsonl"),
        os.path.join(base_dir, "neighbors", f"{dataset_name}_neighbors.jsonl"),
    ]

    print("\n==============================================")
    print(f"[CLEAN] Removing dataset: {dataset_name}")
    print("==============================================")

    removed = False
    for f in files:
        if os.path.exists(f):
            print(f"[CLEAN] Removing: {f}")
            os.remove(f)
            removed = True

    # Also clean up any subdataset files (pattern: *_train_*.jsonl)
    datasets_dir = os.path.join(base_dir, "datasets")
    if os.path.exists(datasets_dir):
        for filename in os.listdir(datasets_dir):
            if filename.startswith(f"{dataset_name}_train_") and filename.endswith(".jsonl"):
                filepath = os.path.join(datasets_dir, filename)
                print(f"[CLEAN] Removing subdataset: {filepath}")
                os.remove(filepath)
                removed = True

    if not removed:
        print("[CLEAN] No files found for this dataset.")

    print("[CLEAN] Done.\n")


def main():
    # ------------------------------------------------------
    # Arguments
    # ------------------------------------------------------
    if len(sys.argv) < 3:
        print("Usage:")
        print("  python pipeline.py <dataset_name> <num_k> <num_queries> [num_records]")
        print("  python pipeline.py <dataset_name> clean")
        print("")
        print("Examples:")
        print("  python pipeline.py fashion-mnist-784-euclidean 256 1000")
        print("  python pipeline.py fashion-mnist-784-euclidean 256 1000 20000")
        sys.exit(1)

    dataset_name = sys.argv[1]

    # Determine paths
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(scripts_dir)
    raw_dir = os.path.join(base_dir, "raw")

    # ------------------------------------------------------
    # CLEAN MODE
    # ------------------------------------------------------
    if sys.argv[2] == "clean":
        clean_dataset(dataset_name, base_dir)
        sys.exit(0)

    # ------------------------------------------------------
    # PIPELINE MODE
    # ------------------------------------------------------
    num_k = sys.argv[2]
    num_queries = sys.argv[3]
    num_records = sys.argv[4] if len(sys.argv) > 4 else None

    print("==============================================")
    print("ANN PIPELINE START")
    print(f"Dataset:      {dataset_name}")
    print(f"num_k:        {num_k}")
    print(f"num_queries:  {num_queries}")
    if num_records:
        print(f"num_records:  {num_records} (subdataset)")
    print("==============================================\n")

    # Step 1: Download HDF5
    download_hdf5(dataset_name, raw_dir)

    # Step 2: Convert HDF5 → JSON
    print("\n==============================================")
    print("[step] Converting HDF5 to JSON")
    print("==============================================")
    run_subprocess([
        sys.executable,
        os.path.join(scripts_dir, "hdf5_to_jsonl.py"),
        dataset_name
    ], cwd=base_dir)

    # Step 2.5: Create subdataset if num_records is specified
    if num_records:
        print("\n==============================================")
        print("[step] Creating subdataset")
        print("==============================================")
        run_subprocess([
            sys.executable,
            os.path.join(scripts_dir, "create_subdataset.py"),
            dataset_name,
            num_records
        ], cwd=base_dir)

    # Step 3: Load dataset
    print("\n==============================================")
    print("[step] Loading dataset to AsterixDB")
    print("==============================================")
    load_cmd = [
        sys.executable,
        os.path.join(scripts_dir, "load_dataset.py"),
        dataset_name
    ]
    if num_records:
        load_cmd.append(num_records)
    run_subprocess(load_cmd, cwd=base_dir)

    # Step 4: Create index
    print("\n==============================================")
    print("[step] Creating vector index")
    print("==============================================")
    index_cmd = [
        sys.executable,
        os.path.join(scripts_dir, "create_index.py"),
        dataset_name,
        num_k
    ]
    if num_records:
        index_cmd.append(num_records)
    run_subprocess(index_cmd, cwd=base_dir)

    # Step 5: Run recall evaluation (comparing ANN vs exact)
    print("\n==============================================")
    print("[step] Running recall evaluation (ANN vs Exact)")
    print("==============================================")
    query_cmd = [
        sys.executable,
        os.path.join(scripts_dir, "run_query_compare.py"),
        dataset_name,
        num_queries
    ]
    if num_records:
        query_cmd.append(num_records)
    run_subprocess(query_cmd, cwd=base_dir)

    print("\n==============================================")
    print("ANN PIPELINE DONE")
    print("==============================================\n")


if __name__ == "__main__":
    main()
