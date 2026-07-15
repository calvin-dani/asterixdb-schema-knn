# Movie Dataset Integration Test Guide

## Overview

This guide describes the integration testing framework for vector indexes with the movie dataset, supporting both standard and quantized indexes.

## Essential Scripts

### 1. Core Test Script: `movie_index_test.py`

**Purpose**: Main Python script that handles all database operations and testing scenarios.

**Capabilities**:
- ✅ Dataset loading (full or limited)
- ✅ Vector index creation (with/without quantization)
- ✅ Insert operations (batch record insertion)
- ✅ Delete operations (range-based deletion)
- ✅ Query execution (with/without filters)
- ✅ Result verification
- ✅ Multiple operation modes

**Key Features**:
- Quantization support (SQ7, SQ8)
- Custom similarity metrics (Euclidean, euclidean-squared, cosine)
- Configurable training list size
- INCLUDE field support
- Flexible operation modes (full workflow, index-only, query-only, etc.)

### 2. Comprehensive Test Suite: `integration_test.sh`

**Purpose**: Automated test suite that runs complete integration scenarios.

**Test Coverage**:
1. Standard index creation and data loading
2. Insert operations (standard index)
3. Delete operations (standard index)
4. Query verification without year filter
5. Query verification with year filter
6. Quantized index creation (SQ7)
7. Insert operations (quantized index)
8. Delete operations (quantized index)
9. Query verification (quantized index)
10. Full workflow (standard index)
11. Full workflow with quantization (SQ8)
12. Quantized index with INCLUDE field

**Usage**:
```bash
cd scripts
chmod +x integration_test.sh
./integration_test.sh
```

### 3. Quick Test Runner: `quick_test.sh`

**Purpose**: Fast testing for specific scenarios during development.

**Available Tests**:
1. Standard index test (no quantization)
2. Quantized index test (SQ7)
3. Quantized index test (SQ8)
4. Insert/delete/verify workflow (standard)
5. Insert/delete/verify workflow (quantized SQ7)

**Usage**:
```bash
cd scripts
chmod +x quick_test.sh

# Run specific test (1-5)
./quick_test.sh 1  # Standard index test
./quick_test.sh 2  # Quantized index (SQ7)
./quick_test.sh 5  # Insert/delete/verify (quantized)

# Default (no argument) runs test 1
./quick_test.sh
```

## Test Scenarios

### Scenario 1: Standard Index Full Workflow

**Command**:
```bash
python movie_index_test.py movie_embeddings_384d.json 153 50 2000 --limit 3000
```

**What it does**:
1. Loads first 3000 records
2. Creates vector index with 153 clusters
3. Executes ANN query (K=50, year > 2000)
4. Verifies results

### Scenario 2: Quantized Index with SQ7

**Command**:
```bash
python movie_index_test.py movie_embeddings_384d.json 153 50 2000 \
    --limit 3000 \
    --quantization SQ7 \
    --similarity euclidean-squared \
    --train-list 3000
```

**What it does**:
1. Loads first 3000 records
2. Creates quantized vector index (SQ7) with euclidean-squared similarity
3. Uses 3000 records for training
4. Executes ANN query with quantization parameters (nprobe=20, epsilon=0.05, search_method=3)
5. Verifies results

### Scenario 3: Insert/Delete/Verify Workflow

**Step 1 - Load initial dataset**:
```bash
python movie_index_test.py movie_embeddings_384d.json 153 --no-query --limit 3000
```
Creates index with records [0, 2999]

**Step 2 - Insert additional records**:
```bash
python movie_index_test.py movie_embeddings_384d.json --insert 3000 5999
```
Adds records [3000, 5999]

**Step 3 - Delete records**:
```bash
python movie_index_test.py movie_embeddings_384d.json --delete 2000 3999
```
Removes records [2000, 3999]

**Step 4 - Verify remaining records**:
```bash
python movie_index_test.py movie_embeddings_384d.json --verify 400 0 1999 4000 5999
```
Verifies query returns results from intervals [0, 1999] and [4000, 5999]

### Scenario 4: Index Creation Only

**Standard Index**:
```bash
python movie_index_test.py movie_embeddings_384d.json 153 --no-query --limit 3000
```

**Quantized Index**:
```bash
python movie_index_test.py movie_embeddings_384d.json 153 --no-query \
    --limit 3000 \
    --quantization SQ7 \
    --similarity euclidean-squared \
    --train-list 3000
```

### Scenario 5: Query Only (Existing Index)

```bash
python movie_index_test.py movie_embeddings_384d.json 50 2000 --query-only
```

Uses existing index to execute query without recreating dataset/index.

## Command-Line Options

### Required Arguments

- `<dataset_filename>`: Name of dataset file (e.g., `movie_embeddings_384d.json`)
- `<num_clusters>`: Number of clusters for vector index (e.g., `153`)
- `<k>`: Number of results to return in queries (e.g., `50`)
- `<year_condition>`: Year filter condition (e.g., `2000` for year > 2000)

### Optional Flags

| Flag | Argument | Description | Example |
|------|----------|-------------|---------|
| `--limit` | `<number>` | Load only first N records | `--limit 3000` |
| `--quantization` | `<type>` | Enable quantization | `--quantization SQ7` |
| `--similarity` | `<metric>` | Similarity metric | `--similarity euclidean-squared` |
| `--train-list` | `<number>` | Training list size | `--train-list 3000` |
| `--include-field` | `<field>` | Include field in index | `--include-field year` |
| `--no-query` | - | Skip query execution | `--no-query` |
| `--no-index` | - | Skip index creation | `--no-index` |
| `--query-only` | - | Query only mode | `--query-only` |
| `--insert` | `<start> <end>` | Insert records | `--insert 3000 5999` |
| `--delete` | `<start> <end>` | Delete records | `--delete 2000 3999` |
| `--verify` | `<k> <intervals>` | Verify query results | `--verify 400 0 1999 4000 5999` |
| `--year` | `<condition>` | Year filter (with --verify) | `--year 2000` |

### Quantization Options

**Quantization Types**:
- `SQ7`: Scalar quantization with 7 bits
- `SQ8`: Scalar quantization with 8 bits

**Similarity Metrics**:
- `Euclidean`: Standard Euclidean distance
- `euclidean-squared`: Squared Euclidean distance
- `cosine`: Cosine similarity

**Query Parameters** (for quantized indexes):
- `nprobe`: Number of clusters to probe (default: 20)
- `epsilon`: Error tolerance (default: 0.05)
- `search_method`: Search method indicator (default: 3)

## Test Workflow Examples

### Example 1: Complete Standard Index Test

```bash
# 1. Load dataset and create index
python movie_index_test.py movie_embeddings_384d.json 153 --no-query --limit 3000

# 2. Query the index
python movie_index_test.py movie_embeddings_384d.json 50 2000 --query-only

# 3. Insert more records
python movie_index_test.py movie_embeddings_384d.json --insert 3000 5999

# 4. Query again
python movie_index_test.py movie_embeddings_384d.json 50 2000 --query-only
```

### Example 2: Complete Quantized Index Test

```bash
# 1. Load dataset and create quantized index
python movie_index_test.py movie_embeddings_384d.json 153 --no-query \
    --limit 3000 \
    --quantization SQ7 \
    --similarity euclidean-squared \
    --train-list 3000

# 2. Query the quantized index
python movie_index_test.py movie_embeddings_384d.json 50 2000 --query-only

# 3. Insert/delete operations
python movie_index_test.py movie_embeddings_384d.json --insert 3000 5999
python movie_index_test.py movie_embeddings_384d.json --delete 2000 3999

# 4. Verify final state
python movie_index_test.py movie_embeddings_384d.json --verify 400 0 1999 4000 5999
```

### Example 3: Comparison Test (Standard vs Quantized)

```bash
# Test 1: Standard index
python movie_index_test.py movie_embeddings_384d.json 153 50 2000 --limit 3000

# Test 2: Quantized SQ7
python movie_index_test.py movie_embeddings_384d.json 153 50 2000 \
    --limit 3000 \
    --quantization SQ7 \
    --similarity euclidean-squared \
    --train-list 3000

# Test 3: Quantized SQ8
python movie_index_test.py movie_embeddings_384d.json 153 50 2000 \
    --limit 3000 \
    --quantization SQ8 \
    --similarity Euclidean \
    --train-list 3000
```

## Scripts to Keep

### Minimum Required:
1. ✅ `movie_index_test.py` - Core testing script
2. ✅ `quick_test.sh` - Quick testing during development

### Recommended:
3. ✅ `integration_test.sh` - Full automated test suite

### Optional Supporting Scripts:
- Dataset conversion/preparation scripts (if needed)
- Performance benchmarking scripts
- Result analysis scripts

## File Organization

```
ANN-benchmarks/
├── datasets/
│   └── movie_embeddings_384d.json         # Main dataset
├── scripts/
│   ├── movie_index_test.py                # ✅ KEEP - Core test script
│   ├── integration_test.sh                # ✅ KEEP - Full test suite
│   └── quick_test.sh                      # ✅ KEEP - Quick tests
└── INTEGRATION_TEST_GUIDE.md              # This guide
```

## Troubleshooting

### Common Issues

**1. Dataset not found**
```
Error: dataset file not found: /path/to/datasets/movie_embeddings_384d.json
```
**Solution**: Ensure dataset is in `datasets/` directory relative to script location.

**2. AsterixDB connection error**
```
HTTP error from AsterixDB: Connection refused
```
**Solution**: Ensure AsterixDB is running on localhost:19002.

**3. Quantization query parameters**
```
For quantized indexes, queries automatically use:
- nprobe: 20
- epsilon: 0.05
- search_method: 3
```
These are currently hardcoded but can be made configurable if needed.

## Performance Considerations

### Dataset Size Recommendations

| Test Type | Recommended Limit | Purpose |
|-----------|------------------|---------|
| Quick tests | 1000-3000 records | Fast iteration |
| Integration tests | 3000-10000 records | Full feature testing |
| Performance tests | 10000+ records | Benchmarking |

### Quantization Trade-offs

**SQ7**:
- ✅ Smaller index size
- ✅ Faster queries
- ⚠️ Lower precision

**SQ8**:
- ✅ Better precision than SQ7
- ⚠️ Larger index size than SQ7
- ⚠️ Slower than SQ7 (but faster than no quantization)

**No Quantization**:
- ✅ Highest precision
- ⚠️ Largest index size
- ⚠️ Slowest queries

## Next Steps

1. **Run quick tests** to verify setup:
   ```bash
   ./quick_test.sh 1
   ```

2. **Run full integration suite**:
   ```bash
   ./integration_test.sh
   ```

3. **Customize tests** for your specific use cases by modifying the test scripts or creating new ones based on the examples above.

4. **Monitor results** and adjust parameters (num_clusters, train_list, etc.) based on your performance requirements.
