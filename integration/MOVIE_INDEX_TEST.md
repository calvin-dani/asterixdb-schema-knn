# Movie Index Test - Simplified Usage

## Overview
Simplified script to test vector index with movie dataset. The dataset file should be placed in the `datasets/` directory, and the dimension is automatically detected from the embeddings.

## Changes from Original
- ✅ **Simplified path**: Just provide filename, not full path
- ✅ **Auto-detect dimension**: Extracts dimension from first embedding
- ✅ **Fixed location**: Assumes dataset is in `./datasets/` directory
- ✅ **Flexible loading**: Support `--limit` flag to load only N records

## Usage

### Full Workflow (Create Everything + Query)
```bash
python scripts/movie_index_test.py <dataset_filename> <num_clusters> <k> <year_condition>
```

### With INCLUDE Field
```bash
python scripts/movie_index_test.py <dataset_filename> <num_clusters> <k> <year_condition> --include-field <field_name>
```

### With Limited Dataset (Load Only N Records)
```bash
python scripts/movie_index_test.py <dataset_filename> <num_clusters> <k> <year_condition> --limit <num_records>
```

### Create Index Only (Skip Query Execution)
```bash
# Note: k and year_condition are not required when using --no-query
python scripts/movie_index_test.py <dataset_filename> <num_clusters> --no-query
```

### Query Only (Skip Setup, Use Existing Index)
```bash
# Note: num_clusters is not required when using --query-only
python scripts/movie_index_test.py <dataset_filename> <k> <year_condition> --query-only
```

## Arguments

### Full Workflow Mode
1. **dataset_filename** - Filename in `datasets/` directory (e.g., `movie_embeddings_384d.json`)
2. **num_clusters** - Number of clusters for vector index (e.g., `100`)
3. **k** - Number of results to return (e.g., `50`)
4. **year_condition** - Year threshold for filtering (e.g., `2000`)
5. **--include-field** *(optional)* - Field to include in vector index (e.g., `year`)
6. **--limit** *(optional)* - Load only first N records from dataset (e.g., `1000`)

### Index-Only Mode (`--no-query`)
1. **dataset_filename** - Filename in `datasets/` directory
2. **num_clusters** - Number of clusters for vector index
3. **--no-query** - Flag to skip query execution
4. **--include-field** *(optional)* - Field to include in vector index
5. **--limit** *(optional)* - Load only first N records from dataset

### Query-Only Mode (`--query-only`)
1. **dataset_filename** - Filename in `datasets/` directory (only used to load query vector)
2. **k** - Number of results to return
3. **year_condition** - Year threshold for filtering
4. **--query-only** - Flag to skip dataverse/dataset/index creation

## Examples

### Without INCLUDE Field
```bash
# Test with 100 clusters, return 50 results, filter year > 2000
python scripts/movie_index_test.py movie_embeddings_384d.json 100 50 2000
```

### With INCLUDE Field
```bash
# Same as above, but with year field included in index
python scripts/movie_index_test.py movie_embeddings_384d.json 100 50 2000 --include-field year
```

### With Limited Dataset
```bash
# Load only first 1000 records for quick testing
python scripts/movie_index_test.py movie_embeddings_384d.json 100 50 2000 --limit 1000

# Combine with INCLUDE field
python scripts/movie_index_test.py movie_embeddings_384d.json 100 50 2000 --include-field year --limit 5000
```

### Different Parameters
```bash
# More clusters, fewer results, different year threshold
python scripts/movie_index_test.py movie_embeddings_384d.json 200 20 2010 --include-field year
```

### Create Index Only (No Query)
```bash
# Just create the index and dataverse, skip query execution
# Note: k and year_condition are NOT required
python scripts/movie_index_test.py movie_embeddings_384d.json 100 --no-query

# With limited dataset for faster index creation
python scripts/movie_index_test.py movie_embeddings_384d.json 100 --no-query --limit 1000

# Combine with INCLUDE field
python scripts/movie_index_test.py movie_embeddings_384d.json 100 --no-query --include-field year
```

### Query Only (Use Existing Index)
```bash
# Just execute query against existing index (dataverse/dataset/index must already exist)
# Note: num_clusters is NOT required
python scripts/movie_index_test.py movie_embeddings_384d.json 50 2000 --query-only

# Try different k values
python scripts/movie_index_test.py movie_embeddings_384d.json 100 2000 --query-only

# Try different year conditions
python scripts/movie_index_test.py movie_embeddings_384d.json 50 2010 --query-only
```

## Dataset Location

Place your movie dataset file in:
```
ANN-benchmarks/
  datasets/
    movie_embeddings_384d.json  ← Put file here
```

## Expected Dataset Format

JSON Lines format with records like:
```json
{"idx": 0, "title": "Movie Title", "year": 2001, "embedding": [0.1, 0.2, ...]}
{"idx": 1, "title": "Another Movie", "year": 2005, "embedding": [0.3, 0.4, ...]}
```

Required fields:
- `idx` (int) - Record identifier
- `title` (string) - Movie title
- `year` (int) - Release year (integer, not string)
- `embedding` (array) - Vector embedding (dimension auto-detected)

## Flexible Dataset Loading with `--limit`

The `--limit` flag allows you to load only the first N records from the dataset, which is useful for:
- **Quick testing**: Test with a small subset before running on full dataset
- **Development**: Faster iteration during development
- **Benchmarking**: Compare performance with different dataset sizes

### How It Works

When `--limit N` is specified:
1. Script creates a temporary limited dataset file: `<original_file>.limited_N`
2. Copies only the first N records from the original file
3. Loads the limited file into AsterixDB
4. The limited file is retained for inspection if needed

### Example Use Cases

```bash
# Quick test with 100 records
python scripts/movie_index_test.py movie_embeddings_384d.json 10 20 2000 --limit 100

# Development with 1000 records
python scripts/movie_index_test.py movie_embeddings_384d.json 50 50 2000 --limit 1000 --include-field year

# Benchmark different sizes
python scripts/movie_index_test.py movie_embeddings_384d.json 100 --no-query --limit 1000
python scripts/movie_index_test.py movie_embeddings_384d.json 100 --no-query --limit 5000
python scripts/movie_index_test.py movie_embeddings_384d.json 100 --no-query --limit 10000
```

**Note**: The `--limit` flag is only applicable in full workflow and index-only modes. It is ignored in query-only mode since that mode doesn't load data.

## What the Script Does

### Full Workflow Mode (Default)
1. **Creates Dataverse**
   ```sql
   DROP DATAVERSE VectorTest IF EXISTS;
   CREATE DATAVERSE VectorTest;
   ```

2. **Creates Dataset**
   ```sql
   CREATE TYPE MovieType AS {
     idx: int,
     title: string,
     year: string
   };
   
   CREATE DATASET MovieDataset (MovieType)
   PRIMARY KEY year WITH {
     "storage-format":{"format":"column"}
   };
   ```

3. **Loads Data**
   ```sql
   LOAD DATASET MovieDataset USING localfs (
     ("path" = "localhost:///path/to/datasets/movie_embeddings_384d.json"),
     ("format" = "json")
   );
   ```

4. **Drops Existing Index** (if any)
   ```sql
   DROP INDEX MovieDataset.ix1 IF EXISTS;
   ```

5. **Creates Vector Index**
   
   Without INCLUDE:
   ```sql
   CREATE VECTOR INDEX ix1 ON MovieDataset(embedding) 
   WITH { 
     "dimension": 384,  -- auto-detected
     "train_list": 10000,
     "num_clusters": 100,
     "similarity": "Euclidean"
   };
   ```
   
   With INCLUDE:
   ```sql
   CREATE VECTOR INDEX ix1 ON MovieDataset(embedding) 
   INCLUDE (year)  -- added when --include-field year is specified
   WITH { 
     "dimension": 384,
     "train_list": 10000,
     "num_clusters": 100,
     "similarity": "Euclidean"
   };
   ```

6. **Executes ANN Query** (skipped if `--no-query` is set)
   ```sql
   LET qvec = [0.1, 0.2, ...]  -- embedding from idx=0
   FROM MovieDataset row
   LET dist = ann_distance(row.embedding, qvec, "Euclidean")
   SELECT row.idx, row.year, dist
   WHERE row.year > "2000"
   ORDER BY dist
   LIMIT 50;
   ```

7. **Verifies Results** (skipped if `--no-query` is set)
   - Checks that exactly K results are returned
   - Validates all year values are > year_condition

### Index-Only Mode (`--no-query`)
When `--no-query` flag is set, the script stops after creating the index (step 5) and does not execute the query or verification steps. This is useful when you want to:
- Just set up the index without testing queries
- Benchmark index creation time separately
- Prepare the index for manual testing

### Query-Only Mode (`--query-only`)
When `--query-only` flag is set, the script skips steps 1-5 (dataverse/dataset/index creation) and only executes steps 6-7 (query and verification). This is useful when you want to:
- Test different query parameters (k, year_condition) against an existing index
- Benchmark query performance without index creation overhead
- Run multiple queries quickly without recreating the index
- **Note**: The dataverse, dataset, and index must already exist (created in a previous run)

## Output Example

### Standard Mode (With Query)
```
============================================================
Movie Dataset Vector Index Test
============================================================
Dataset: movie_embeddings_384d.json
Location: /Users/hongyu/Projects/dev/ANN-benchmarks/datasets/movie_embeddings_384d.json
Num Clusters: 100
K: 50
Year Condition: > 2000
Include Field: year
Skip Query: False
============================================================

[step] Creating dataverse and loading dataset
[info] Loading from: localhost:///Users/hongyu/Projects/dev/ANN-benchmarks/datasets/movie_embeddings_384d.json
[done] Dataverse and dataset created

[step] Dropping existing index (if any)
[done] Index dropped

[step] Loading query vector from dataset (idx=0)
[info] Loaded embedding with dimension 384
[info] Auto-detected dimension: 384

[step] Creating vector index (num_clusters=100)
[info] Including field: year
[done] Vector index created

[query] Executing ANN query with K=50, year > 2000
[info] Received 50 results

[step] Verifying results...

============================================================
✓ VERIFICATION PASSED
✓ All 50 results have year > 2000
============================================================

Sample of results (first 10):
  1. idx=0, year=2001, dist=0.0
  2. idx=15, year=2005, dist=1.234
  3. idx=23, year=2003, dist=1.456
  ...
```

### Index-Only Mode (With `--no-query`)
```
============================================================
Movie Dataset Vector Index Test - INDEX ONLY MODE
============================================================
Dataset: movie_embeddings_384d.json
Location: /Users/hongyu/Projects/dev/ANN-benchmarks/datasets/movie_embeddings_384d.json
Num Clusters: 100
Include Field: None
============================================================

[step] Creating dataverse and loading dataset
[info] Loading from: localhost:///Users/hongyu/Projects/dev/ANN-benchmarks/datasets/movie_embeddings_384d.json
[done] Dataverse and dataset created

[step] Dropping existing index (if any)
[done] Index dropped

[step] Loading query vector from dataset (idx=0)
[info] Loaded embedding with dimension 384
[info] Auto-detected dimension: 384

[step] Creating vector index (num_clusters=100)
[info] No INCLUDE fields
[done] Vector index created

============================================================
✓ INDEX CREATION COMPLETE
Query execution skipped (--no-query flag set)
============================================================
```

### Query-Only Mode (With `--query-only`)
```
============================================================
Movie Dataset Vector Index Test - QUERY ONLY MODE
============================================================
Dataset: movie_embeddings_384d.json
Location: /Users/hongyu/Projects/dev/ANN-benchmarks/datasets/movie_embeddings_384d.json
K: 50
Year Condition: > 2000
Include Field: None
============================================================

[mode] Query-only: skipping dataverse/dataset/index creation

[step] Loading query vector from dataset (idx=0)
[info] Loaded embedding with dimension 384
[info] Auto-detected dimension: 384

[query] Executing ANN query with K=50, year > 2000
[info] Received 50 results

[step] Verifying results...

============================================================
✓ VERIFICATION PASSED
✓ All 50 results have year > 2000
============================================================

Sample of results (first 10):
  1. idx=0, year=2001, dist=0.0
  2. idx=15, year=2005, dist=1.234
  3. idx=23, year=2003, dist=1.456
  ...
```

## Troubleshooting

### Dataset File Not Found
```
Error: dataset file not found: /path/to/datasets/movie_embeddings_384d.json
Expected location: datasets/movie_embeddings_384d.json
```

**Solution**: Make sure the file is in the `datasets/` directory:
```bash
ls datasets/movie_embeddings_384d.json
```

### Could Not Find Embedding for idx=0
```
Error: Could not find embedding for idx=0 in /path/to/dataset
```

**Solution**: Ensure your dataset has a record with `idx: 0` and an `embedding` field.

### AsterixDB Not Running
```
HTTP error from AsterixDB: ...
Connection refused
```

**Solution**: Start AsterixDB before running the script.

### Invalid Year Format
```
✗ VERIFICATION FAILED
Found N records with invalid year values
```

**Solution**: Check that `year` field is a string and all records have year > condition.

## Comparison: INCLUDE vs No INCLUDE

| Feature | Without INCLUDE | With INCLUDE (year) |
|---------|----------------|---------------------|
| **Index Size** | Smaller | Larger (stores year in index) |
| **Query Performance** | Standard | Potentially faster for filtered queries |
| **Storage** | Embedding only | Embedding + year field |
| **Use Case** | General ANN queries | ANN queries with year filtering |

## Best Practices

### Typical Workflow

**1. Initial Setup (Full Workflow)**
```bash
# Create everything and verify with one query
python scripts/movie_index_test.py movie_embeddings_384d.json 100 50 2000 --include-field year
```

**2. Test Multiple Query Parameters (Query-Only)**
```bash
# Now that index exists, test different k values quickly
python scripts/movie_index_test.py movie_embeddings_384d.json 10 2000 --query-only
python scripts/movie_index_test.py movie_embeddings_384d.json 50 2000 --query-only
python scripts/movie_index_test.py movie_embeddings_384d.json 100 2000 --query-only

# Test different year conditions
python scripts/movie_index_test.py movie_embeddings_384d.json 50 1990 --query-only
python scripts/movie_index_test.py movie_embeddings_384d.json 50 2000 --query-only
python scripts/movie_index_test.py movie_embeddings_384d.json 50 2010 --query-only
```

**3. Benchmark Index Creation Only (Index-Only)**
```bash
# Just create index without query overhead
python scripts/movie_index_test.py movie_embeddings_384d.json 100 --no-query
```

### When to Use Each Mode

| Mode | Use Case | Creates Index | Runs Query |
|------|----------|--------------|------------|
| **Full Workflow** | First-time setup and end-to-end test | ✅ | ✅ |
| **Index-Only** (`--no-query`) | Benchmark index creation time | ✅ | ❌ |
| **Query-Only** (`--query-only`) | Test different query parameters quickly | ❌ | ✅ |

### General Tips

1. **Start Small**: Test with fewer clusters first to verify setup
   ```bash
   python scripts/movie_index_test.py movie_embeddings_384d.json 10 10 2000
   ```

2. **Use Query-Only for Iteration**: Once index is created, use `--query-only` for fast experimentation
   ```bash
   # Much faster than full workflow!
   python scripts/movie_index_test.py movie_embeddings_384d.json 50 2000 --query-only
   ```

3. **Use INCLUDE for Filtered Queries**: If you frequently filter by year, use `--include-field year`

4. **Check Dataset First**: Verify your dataset file before running:
   ```bash
   head -1 datasets/movie_embeddings_384d.json | python -m json.tool
   ```

5. **Monitor AsterixDB**: Check AsterixDB logs if queries fail or are slow

6. **Experiment with Clusters**: Try different num_clusters values (10, 50, 100, 200) to see impact on performance

## Integration with Other Scripts

This script is standalone but follows the same patterns as:
- `verify_query_intervals.py` - Similar query verification approach
- `extend_dataset.py` - Similar AsterixDB interaction patterns
- `movie_index_test.py` - Can be used in integration test suites
