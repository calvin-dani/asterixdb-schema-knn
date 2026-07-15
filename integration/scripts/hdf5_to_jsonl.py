import h5py
import json
import numpy as np
import sys
import os
from tqdm import tqdm

# Base project directory (scripts/convert_hdf5_to_json.py -> parent)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DIR = os.path.join(BASE_DIR, "raw")
DATASETS_DIR = os.path.join(BASE_DIR, "datasets")
TESTS_DIR = os.path.join(BASE_DIR, "tests")
NEIGHBORS_DIR = os.path.join(BASE_DIR, "neighbors")

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def write_jsonl(output_path, array):
    with open(output_path, "w") as f:
        for i in tqdm(range(array.shape[0])):
            obj = {
                "idx": int(i),
                "embedding": array[i].tolist()
            }
            f.write(json.dumps(obj) + "\n")

def write_neighbors(output_path, array):
    # neighbors is usually shape (queries, 1 or k)
    with open(output_path, "w") as f:
        for i in tqdm(range(array.shape[0])):
            obj = {
                "idx": int(i),
                "neighbors": array[i].tolist()
            }
            f.write(json.dumps(obj) + "\n")

def main():
    if len(sys.argv) != 2:
        print("Usage: python convert_hdf5_to_json.py <dataset_name>")
        print("Example: python convert_hdf5_to_json.py glove-100-angular")
        sys.exit(1)

    dataset_name = sys.argv[1]
    input_path = os.path.join(RAW_DIR, dataset_name + ".hdf5")

    if not os.path.exists(input_path):
        print(f"Error: raw dataset not found: {input_path}")
        sys.exit(1)

    print(f"Loading HDF5 file: {input_path}")
    f = h5py.File(input_path, "r")

    # Ensure target dirs exist
    ensure_dir(DATASETS_DIR)
    ensure_dir(TESTS_DIR)
    ensure_dir(NEIGHBORS_DIR)

    # TRAIN vectors
    if "train" in f:
        train = f["train"]
        train_output = os.path.join(DATASETS_DIR, f"{dataset_name}_train.jsonl")
        if os.path.exists(train_output):
            print(f"Train dataset already exists: {train_output} (skipping)")
        else:
            print(f"Converting train dataset → {train_output}...")
            write_jsonl(train_output, train)
    else:
        print("No 'train' dataset found inside HDF5.")

    # TEST vectors
    if "test" in f:
        test = f["test"]
        test_output = os.path.join(TESTS_DIR, f"{dataset_name}_test.jsonl")
        if os.path.exists(test_output):
            print(f"Test vectors already exist: {test_output} (skipping)")
        else:
            print(f"Converting test vectors → {test_output}...")
            write_jsonl(test_output, test)
    else:
        print("No 'test' dataset found inside HDF5.")

    # NEIGHBORS ground truth
    if "neighbors" in f:
        neighbors = f["neighbors"]
        neighbors_output = os.path.join(NEIGHBORS_DIR, f"{dataset_name}_neighbors.jsonl")
        if os.path.exists(neighbors_output):
            print(f"Neighbors already exist: {neighbors_output} (skipping)")
        else:
            print(f"Converting neighbors → {neighbors_output}...")
            write_neighbors(neighbors_output, neighbors)
    else:
        print("No 'neighbors' ground-truth found inside HDF5.")

    f.close()
    print("\nAll done!")

if __name__ == "__main__":
    main()

