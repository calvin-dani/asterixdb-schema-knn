# Hierarchical K-Means++ Test Suite

This directory contains comprehensive test cases for the `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor` operator in AsterixDB.

## Test Structure

The test suite follows the standard AsterixDB integration test pattern with the following files:

### Basic Hierarchical K-Means Tests (`hierarchical_kmeans.*`)

- **`hierarchical_kmeans.1.ddl.sqlpp`** - Sets up test dataverse and dataset
- **`hierarchical_kmeans.2.update.sqlpp`** - Loads test data
- **`hierarchical_kmeans.3.query.sqlpp`** - Main test queries
- **`hierarchical_kmeans.4.ddl.sqlpp`** - Cleanup
- **`hierarchical_kmeans.3.adm`** - Expected results

### Multilevel Index Tests (`multilevel_index_creation.*`)

- **`multilevel_index_creation.1.ddl.sqlpp`** - Sets up column dataset for index testing
- **`multilevel_index_creation.2.update.sqlpp`** - Loads test data
- **`multilevel_index_creation.3.query.sqlpp`** - Index creation and validation tests
- **`multilevel_index_creation.4.ddl.sqlpp`** - Cleanup
- **`multilevel_index_creation.3.adm`** - Expected results

## Test Categories

### 1. Centroid Update Validation
**Purpose**: Verify that centroids are updated correctly based on data assignments
**Test**: `centroid_update_test`
**Validates**:
- Centroids converge to the mean of their assigned data points
- Data-weighting and division errors are detected
- Average and standard deviation of distances to centroids

### 2. Convergence Criterion Testing
**Purpose**: Test that algorithm stops when centroid movement < tolerance
**Test**: `convergence_test`
**Validates**:
- Algorithm stops within reasonable iterations
- Tighter tolerance leads to more iterations
- Prevents infinite loops

### 3. Initialization Reproducibility
**Purpose**: Test that KMeans++ produces reproducible centroids when seed is fixed
**Test**: `reproducibility_test`
**Validates**:
- Same seed produces identical initial centroids
- Different seeds produce different results
- Ensures determinism and reproducibility

### 4. Edge Cases Testing
**Purpose**: Test robustness of algorithm with challenging inputs
**Tests**:
- `edge_case_few_points`: Fewer points than clusters
- `edge_case_identical_points`: Identical/duplicate points
- `edge_case_nan_values`: NaN, null, and invalid values
- `edge_case_empty_partitions`: Empty clusters during iteration

### 5. Multilevel Index Creation
**Purpose**: Test creation and validation of hierarchical cluster index
**Tests**:
- `index_creation_test`: Basic index creation validation
- `level_distribution_test`: Cluster distribution across levels
- `parent_child_validation_test`: Parent-child relationships
- `cluster_quality_test`: Centroid quality and convergence
- `range_query_test`: Index performance for range queries
- `index_metadata_test`: Index metadata and statistics

### 6. Performance and Scalability
**Purpose**: Test algorithm performance with larger datasets
**Test**: `performance_test`
**Validates**:
- Completes within reasonable time and memory bounds
- Maintains quality with larger datasets
- Iteration counts are reasonable

## Test Data Requirements

The tests expect a dataset with the following structure:
```json
{
  "id": "uuid",
  "embedding": [double, double, ...]  // Vector of doubles
}
```

**Dataset**: `/home/calvin-dani/Documents/Datasets/10K256emb.json`
- 10,000 records
- 256-dimensional embedding vectors
- JSON format

## Key Test Parameters

### Hierarchical K-Means Parameters
- **K**: Number of clusters (varies by test: 3-20)
- **max_iterations**: Maximum Lloyd's iterations (10-20)
- **tolerance**: Convergence threshold (0.001-0.1)
- **seed**: Random seed for reproducibility (12345, 67890)
- **max_levels**: Maximum tree levels (5)

### Index Creation Parameters
- **k**: 8 clusters
- **max_iterations**: 20
- **tolerance**: 0.01
- **seed**: 12345
- **max_levels**: 5
- **min_cluster_size**: 10

## Expected Behaviors

### Centroid Update
- Centroids should converge to the mean of assigned points
- Average distance to centroids should decrease over iterations
- Standard deviation should be reasonable

### Convergence
- Algorithm should stop when centroid movement < tolerance
- Tighter tolerance should require more iterations
- Should not exceed maximum iterations

### Initialization
- Same seed should produce identical initial centroids
- Different seeds should produce different initial centroids
- Initial centroids should be well-separated

### Edge Cases
- Fewer points than clusters: Should use all available points
- Identical points: Should cluster identical points together
- NaN values: Should skip invalid points gracefully
- Empty partitions: Should handle empty clusters

### Multilevel Index
- Should create proper tree structure with multiple levels
- Parent-child relationships should be correct
- Higher levels should have fewer clusters
- Index should support efficient range queries

## Running the Tests

The tests are integrated into the AsterixDB test framework and can be run using:

```bash
mvn test -Dtest=HierarchicalKMeansTests
```

Or as part of the full test suite:

```bash
mvn test
```

## Test Validation

Each test includes validation logic that returns "PASS" or "FAIL" based on:
- Expected numerical ranges
- Structural properties (tree hierarchy)
- Performance metrics
- Edge case handling

The comprehensive validation test aggregates all individual test results to provide an overall assessment.
