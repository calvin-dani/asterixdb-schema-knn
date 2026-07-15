import os
import sys
import json


def create_subdataset(input_path, output_path, num_records):
    """
    Create a subdataset by taking the first num_records from input_path.
    
    Args:
        input_path: Path to the original _train.jsonl file
        output_path: Path to the output subdataset file
        num_records: Number of records to extract
    """
    if not os.path.exists(input_path):
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)
    
    # Check if subdataset already exists
    if os.path.exists(output_path):
        print(f"[subdataset] Subdataset already exists: {output_path}")
        print(f"[subdataset] Skipping creation")
        return num_records
    
    print(f"[subdataset] Creating subdataset with {num_records} records")
    print(f"[subdataset] Input:  {input_path}")
    print(f"[subdataset] Output: {output_path}")
    
    count = 0
    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        for line in infile:
            if count >= num_records:
                break
            outfile.write(line)
            count += 1
    
    print(f"[subdataset] Created subdataset with {count} records")
    
    if count < num_records:
        print(f"[subdataset] Warning: Only {count} records available, less than requested {num_records}")
    
    return count


def main():
    if len(sys.argv) != 3:
        print("Usage: python create_subdataset.py <dataset_name> <num_records>")
        print("Example: python create_subdataset.py fashion-mnist-784-euclidean 20000")
        sys.exit(1)
    
    dataset_name = sys.argv[1]
    num_records = int(sys.argv[2])
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    datasets_dir = os.path.join(base_dir, "datasets")
    
    input_path = os.path.join(datasets_dir, f"{dataset_name}_train.jsonl")
    output_path = os.path.join(datasets_dir, f"{dataset_name}_train_{num_records}.jsonl")
    
    create_subdataset(input_path, output_path, num_records)


if __name__ == "__main__":
    main()
