# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache AsterixDB is a Big Data Management System (BDMS) with a semistructured NoSQL data model (ADM), SQL++ query language, and parallel runtime execution engine built on Apache Hyracks. This fork focuses on vector indexing and k-nearest neighbor (KNN) search capabilities using VCTree (Vector Clustering Tree) structures with LSM-based storage.

## Build Commands

### Building the Project
```bash
# Full build (skip tests)
mvn clean package -DskipTests

# Full build with tests
mvn clean package

# Build specific module
cd <module-directory>
mvn clean install
```

### Running Tests
```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=LSMVCTreeSearchTest

# Run specific test method
mvn test -Dtest=LSMVCTreeSearchTest#testBasicSearch

# Run tests in specific module
cd hyracks-fullstack/hyracks/hyracks-tests/hyracks-storage-am-lsm-btree-test
mvn test
```

### Running AsterixDB
```bash
# Navigate to build output
cd asterixdb/asterix-server/target/asterix-server-*-binary-assembly/apache-asterixdb-*-SNAPSHOT

# Start single-machine cluster
./opt/local/bin/start-sample-cluster.sh

# Access web interface at http://localhost:19006

# Stop cluster
./opt/local/bin/stop-sample-cluster.sh
```

## Architecture Overview

### Two-Layer Architecture

**AsterixDB Layer** (`asterixdb/` directory)
- High-level query processing, SQL++ language support, metadata management
- Key modules: asterix-algebra, asterix-runtime, asterix-app, asterix-metadata
- Translates SQL++ queries into Algebricks logical plans, then to Hyracks jobs

**Hyracks Layer** (`hyracks-fullstack/` directory)
- Low-level parallel runtime, storage engines, dataflow execution
- Key modules: hyracks-api, hyracks-dataflow-*, hyracks-storage-am-*
- Executes physical operators across distributed cluster nodes

### Vector Indexing Architecture - Hierarchical IVF (Under Development)

The codebase implements **LSMVCTree** (LSM Vector Clustering Tree), a hierarchical IVF (Inverted File Index) for approximate nearest neighbor search. This is the primary research focus.

#### Query Flow: SQL++ to Hyracks Execution

1. **SQL++ Layer**: User creates vector index with syntax:
   ```sql
   CREATE VECTOR INDEX ix1 ON movie(reviewEmbedding)
   WITH {
     "dimension": 1024,
     "train_list": 10000,
     "similarity": "Euclidean"
   };
   ```

2. **Parser**: `SQLPP.jj` (JavaCC grammar) parses CREATE VECTOR INDEX statements

3. **QueryTranslator**: Acts as coordinator for different statement types, builds metadata and creates JobSpecification for index creation

4. **Algebricks**: Compiles and rewrites query plans (logical → optimized → physical)

5. **Hyracks**: Executes physical operators in distributed dataflow engine

#### Two-Level Implementation Architecture

**VectorClusteringTree** (`hyracks-storage-am-btree/src/main/java/org/apache/hyracks/storage/am/vector/`)
- In-memory tree structure (analogous to BTree in LSM pattern)
- Implements hierarchical k-means clustering with 4 frame types:
  - **Interior frames**: Store cluster centroids and child page pointers for navigation
  - **Leaf frames**: Store cluster centroids and metadata page pointers
  - **Metadata frames**: Store maximum distances and data page pointers
  - **Data frames**: Store actual vectors with distance-to-centroid, cosine similarity, and primary keys
- Handles insert/delete/search operations on a single tree component
- Used as mutable memory component in LSMVCTree

**LSMVCTree** (`hyracks-storage-am-lsm-btree/src/main/java/org/apache/hyracks/storage/am/lsm/vector/`)
- LSM coordinator (analogous to LSMBTree in LSM pattern)
- Creates one VectorClusteringTree per virtual buffer cache as memory components
- Manages multiple immutable disk components (flushed VectorClusteringTrees)
- Coordinates flush/merge operations via inherited LSM infrastructure
- Delegates operations to LSMHarness abstraction layer

**Static Structure Initialization**
- `VCTreeStaticStructureBuilder`: Builds the multi-level k-means hierarchy during index creation
- **Static structure**: Pre-computed hierarchical clustering built from `train_list` sample records
- Leaf-level centroids are trained using k-means++ on sample data
- Upper levels built hierarchically from leaf centroids upward
- This structure enables efficient navigation during ANN search

**Search Predicates and Cursors**
- `VectorAnnPredicate`: Contains query vector, k value, and distance metric
- `VectorClusteringAnnCursor`: ANN search on single VCTree component with triangle inequality pruning
- `LSMVCTreeSearchCursor`: Coordinates search across all LSM components (memory + disk)

### Storage Architecture Patterns

**LSM (Log-Structured Merge) Pattern**
- **Memory component**: Fast in-memory writes using VectorClusteringTree
- **Disk components**: Immutable VectorClusteringTree instances flushed to disk
- **Merge operations**: Background consolidation of disk components
- **LSMHarness**: Abstraction layer that orchestrates index operations
  - Manages component lifecycle (memory → disk transitions)
  - Coordinates search across multiple components
  - Handles flush/merge scheduling via ILSMIOOperationScheduler
  - Delegates specific operations to LSM index implementations (LSMBTree, LSMVCTree, etc.)
- All indexes (B+Trees, R-Trees, VCTrees) follow this pattern

**Buffer Cache Management**
- `IBufferCache`: Page-level caching with pinning/unpinning
- `ICachedPage`: Individual cached pages
- Always pin pages before access, unpin after use
- Use try-finally blocks for proper cleanup

**Operator Descriptors**
- Implement `IOperatorDescriptor` from Hyracks API
- Define dataflow operators: `createPushRuntime()`, `contributeActivities()`
- Handle record schemas via `IRecordDescriptorProvider`
- Propagate `HyracksDataException` for error handling

## Key Design Patterns

### Tuple Access Pattern
```java
// Use IPointable for efficient zero-copy data access
ITupleReference tuple = ...;
IPointable pointable = ...;
tuple.getFieldData(fieldIdx, pointable);
```

### Index Operations
```java
// Always use operation contexts for state management
ILSMIndexOperationContext opCtx = index.createOpContext(...);
try {
    index.search(cursor, searchPred, opCtx);
    while (cursor.hasNext()) {
        cursor.next();
        // Process tuple
    }
} finally {
    cursor.close();
}
```

### Data Serialization
- Use `ITypeTraits` for type metadata
- Use `ISerializerDeserializer` for field serialization
- Byte arrays are prefixed with length in many tuple formats

## Module Locations

### Storage Modules
- **B+Tree**: `hyracks-storage-am-btree/`
- **LSM B+Tree**: `hyracks-storage-am-lsm-btree/`
- **VCTree**: `hyracks-storage-am-btree/src/main/java/org/apache/hyracks/storage/am/vector/`
- **LSM VCTree**: `hyracks-storage-am-lsm-btree/src/main/java/org/apache/hyracks/storage/am/lsm/vector/`
- **Storage Common**: `hyracks-storage-common/`

### Test Modules
- **LSM B+Tree Tests**: `hyracks-tests/hyracks-storage-am-lsm-btree-test/`
- **VCTree Tests**: Located in same test module (e.g., `LSMVCTreeSearchTest.java`)

### AsterixDB Modules
- **SQL++ Parser**: `asterixdb/asterix-lang-sqlpp/` - Contains `SQLPP.jj` JavaCC grammar
- **Query Translator**: `asterixdb/asterix-app/src/main/java/org/apache/asterix/app/translator/QueryTranslator.java`
- **Query Runtime**: `asterixdb/asterix-runtime/`
- **Algebricks**: `hyracks-fullstack/algebricks/` (logical plan optimization)
- **Metadata**: `asterixdb/asterix-metadata/`
- **Index Utilities**: `asterixdb/asterix-app/src/main/java/org/apache/asterix/utils/IndexUtil.java`

## Package Naming Conventions

- AsterixDB packages: `org.apache.asterix.*`
- Hyracks packages: `org.apache.hyracks.*`
- Storage: `org.apache.hyracks.storage.am.*`
- Vector indexing: `org.apache.hyracks.storage.am.vector.*` and `org.apache.hyracks.storage.am.lsm.vector.*`

## Code Style Requirements

- **License**: Include Apache License header in all new files
- **Indentation**: 4 spaces (no tabs)
- **Line length**: 120 characters max
- **Imports**: Group as java.*, javax.*, third-party, org.apache.*
- **Naming**: camelCase for methods/variables, PascalCase for classes
- **Annotations**: Use `@Override` for overridden methods
- **Error handling**: Throw `HyracksDataException` for storage/runtime errors

## Testing Patterns

Tests extend base classes like `AbstractLSMIndexTest` or use utilities from `*TestHelper` classes. Test naming:
- `*Test.java`: Unit tests
- `*InsertTest`, `*SearchTest`, `*MergeTest`: Operation-specific tests
- Pattern: Create index → Perform operations → Verify results → Cleanup

## Vector Index Implementation Notes

The VCTree implementation uses:
- **k-means++ clustering**: Generates centroids with probabilistic sampling for better cluster quality
- **Triangle inequality pruning**: Eliminates pages where |d(q,c) - d(v,c)| > k-th distance
- **Multi-level navigation**: Traverses interior pages to find closest clusters
- **Distance metadata**: Stores d(v,c) with each vector for pruning without full vector comparison
- **Top-K queue**: Maintains k best candidates during search

Current development focus (based on recent commits): Multi-level k-means++ structure, bulk loading, clustering navigation optimization.

## Vector Index Initialization and Structure

### Understanding the Test Flow (LSMVCTreeBuildTest)

The test demonstrates the complete lifecycle of vector index initialization:

```java
// 1. Setup: Create LSM infrastructure
LSMVCTreeTestHarness harness = new LSMVCTreeTestHarness();
harness.setUp();
AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, vectorDimensions);

// 2. Create and activate index
ctx.getIndex().create();
ctx.getIndex().activate();

// 3. Build static structure (hierarchical k-means)
testUtils.buildStaticStructure(ctx);

// 4. Bulk load data records
testUtils.bulkLoadRecords(ctx);

// 5. Test search functionality
testUtils.scanClosestLeafCluster(ctx);  // or topKSearch(ctx)

// 6. Validate and cleanup
ctx.getIndex().validate();
ctx.getIndex().deactivate();
```

**Key Test Files:**
- `LSMVCTreeBuildTest.java`: Main test orchestrator
- `VectorIndexTestDriver.java`: Generates test data (centroids and records)
- `VectorTreeTestUtils.java`: Utility methods for structure building and search

### Static Structure Building

**Process** (in `VectorTreeTestUtils.buildStaticStructure()`):

```java
IIndexBulkLoader ssBuilder = lsmvcTree.createStaticStructureBulkLoader(
    numLevels,              // e.g., 3 levels
    clustersPerLevel,       // [1, 2, 8] - clusters at each level
    centroidsPerCluster,    // [[2], [4,4], [3,3,3,3,3,3,3,3]] - centroids per cluster
    maxEntriesPerPage       // e.g., 5 - tuples per page
);

// Add centroids level by level
for (ITupleReference centroid : centroids) {
    ssBuilder.add(centroid);  // Format: <centroid_id, vector>
}
ssBuilder.end();  // Finalize and persist static structure
```

**Example 3-Level Hierarchy** (from `VectorIndexTestDriver.threeDimensionThreeLevels()`):

```
Level 0 (Root):     1 cluster with 2 centroids
                    c0: [0, 0, 50]    (positive z)
                    c1: [0, 0, -50]   (negative z)

Level 1 (Interior): 2 clusters with 4 centroids each
                    Cluster 1.1: c2-c5   (positive z quadrants)
                    Cluster 1.2: c6-c9   (negative z quadrants)

Level 2 (Leaf):     8 clusters with 3 centroids each
                    Cluster 2.1: c10-c12  (+x, +y, +z)
                    Cluster 2.2: c13-c15  (-x, +y, +z)
                    ...
                    Cluster 2.8: c31-c33  (+x, -y, -z)
```

**Centroid Tuple Format:** `<centroid_id: int, vector: double[]>`

### Bulk Loading Data Records

**Process** (in `VectorTreeTestUtils.bulkLoadRecords()`):

```java
// 1. Extract metadata from static structure
metaFrame.get("num_leaf_centroids") → 24 centroids (c10-c33)
metaFrame.get("first_leaf_centroid_id") → 10

// 2. Create bulk loader
LSMVCTreeBulkLoader bulkLoader = lsmvcTree.createBulkLoader(
    numLeafCentroids,
    firstLeafCentroidId,
    dataFrameSerdes  // [double, double, double[], UTF8String]
);

// 3. Copy static structure pages from builder
for (pageId = 1; pageId <= maxPageId; pageId++) {
    bulkLoader.copyPage(sourcePage);
}

// 4. Load data records for each leaf cluster
for (List<ITupleReference> clusterRecords : dataRecords) {
    for (ITupleReference record : clusterRecords) {
        bulkLoader.add(record);  // Format: <distance, cos(θ), vector, pk>
    }
    bulkLoader.next();  // Move to next cluster
}

bulkLoader.end();  // Finalize first disk component
```

**Data Record Tuple Format:** `<distance_to_centroid: double, cosine_similarity: double, vector: double[], primary_key: string>`

**Test Data Generation** (`VectorIndexTestDriver.generateDataRecords()`):
- Generates 300 records per leaf centroid (24 centroids × 300 = 7,200 total records)
- Records arranged in concentric rings around centroids
- 6 directions per ring: ±x, ±y, ±z
- Distances increase: 0.1, 0.3, 0.5, 0.7, ...
- Primary key format: `"pk_c_<centroid_id>_<record_num>"` (e.g., `"pk_c_10_0"`)

### Frame (Page) Formats

The vector index uses **four distinct frame types**, each stored as pages in the buffer cache:

#### 1. Interior Frame (`VectorClusteringInteriorFrame`)

**Purpose:** Navigation nodes in tree hierarchy (root and interior levels)

**Tuple Format:** `<cid: int, full_precision_centroid: double[], child_page_pointer: int>`

**Page Header:**
- Standard fields (tuple count, free space, etc.)
- `next_page_offset`: For overflow chaining
- `overflow_flag`: Indicates if page has overflow

**Key Methods:**
- `getChildPageId(tupleIndex)`: Get child page for navigation
- `setChildPageId(tupleIndex, pageId)`: Update child pointer
- Location: `hyracks-storage-am-btree/src/main/java/org/apache/hyracks/storage/am/vector/frames/VectorClusteringInteriorFrame.java`

#### 2. Leaf Frame (`VectorClusteringLeafFrame`)

**Purpose:** Bottom level of static structure, points to metadata pages

**Tuple Format:** `<cid: int, full_precision_centroid: double[], metadata_page_pointer: int>`

**Page Header:**
- Standard fields
- `next_leaf_offset`: Pointer to next sibling leaf page
- `overflow_flag`: Indicates if page has overflow

**Key Methods:**
- `getMetadataPagePointer(tupleIndex)`: Get metadata page for cluster
- `setMetadataPagePointer(tupleIndex, pageId)`: Update metadata pointer
- `getCentroidId(tupleIndex)`: Extract cluster ID
- Location: `hyracks-storage-am-btree/src/main/java/org/apache/hyracks/storage/am/vector/frames/VectorClusteringLeafFrame.java`

#### 3. Metadata Frame (`VectorClusteringMetadataFrame`)

**Purpose:** Indirection layer for finding data pages by distance ranges

**Tuple Format:** `<max_distance: double, data_page_pointer: int>`

**Sorting:** Tuples sorted by `max_distance` in ascending order

**Page Header:**
- Standard fields
- `next_page_offset`: For chaining multiple metadata pages

**Key Methods:**
- `findDataPageForDistance(distance)`: Binary search to find appropriate data page
- `getMaxDistance(tupleIndex)`: Extract max distance for entry
- `getDataPagePointer(tupleIndex)`: Get data page pointer
- Location: `hyracks-storage-am-btree/src/main/java/org/apache/hyracks/storage/am/vector/frames/VectorClusteringMetadataFrame.java`

**Example Metadata Entries:**
```
Entry 0: max_distance=2.5  → data_page=100  (vectors with distance 0-2.5)
Entry 1: max_distance=5.0  → data_page=101  (vectors with distance 2.5-5.0)
Entry 2: max_distance=10.0 → data_page=102  (vectors with distance 5.0-10.0)
```

#### 4. Data Frame (`VectorClusteringDataFrame`)

**Purpose:** Actual vector storage with distance metadata

**Tuple Format:** `<distance_to_centroid: double, cos(θ): double, vector: double[], include_fields: ?, primary_key: string>`

**Sorting:** Tuples sorted by `distance_to_centroid` in ascending order

**Page Header:**
- Standard fields
- `next_page_offset`: For overflow chaining when cluster data spans multiple pages

**Key Methods:**
- `getDistanceToCentroid(tupleIndex)`: Extract distance for tuple
- `getCosineValue(tupleIndex)`: Extract cosine similarity
- `findDistanceRange(minDist, maxDist)`: Find tuple index range for distance bounds
- `searchByDistance(targetDist, tolerance)`: Search vectors within distance tolerance
- Location: `hyracks-storage-am-btree/src/main/java/org/apache/hyracks/storage/am/vector/frames/VectorClusteringDataFrame.java`

### Disk Component Structure

After `bulkLoadRecords()`, the first disk component contains:

**Static Structure Pages** (copied from builder):
```
Root Page(s):     Interior frames with level-0 centroids
Interior Page(s): Interior frames with level-1 centroids
Leaf Page(s):     Leaf frames with level-2 centroids + metadata pointers
```

**Dynamic Data Pages** (added by bulk loader):
```
Metadata Pages:   Max distance → data page mappings (one per leaf cluster)
Data Pages:       Actual vector records sorted by distance (chained via next_page)
```

**Page Hierarchy Example for Cluster c10:**
```
Leaf Frame (c10)
    → metadata_page_pointer: 200

Metadata Frame (page 200)
    Entry 0: max_distance=2.5  → data_page: 300
    Entry 1: max_distance=5.0  → data_page: 301
    Entry 2: max_distance=10.0 → data_page: 302

Data Frame (page 300): 100 vectors with distance 0-2.5
    next_page: 301
Data Frame (page 301): 100 vectors with distance 2.5-5.0
    next_page: 302
Data Frame (page 302): 100 vectors with distance 5.0-10.0
    next_page: -1
```

### Search Flow Example

**Point Search** (`VectorTreeTestUtils.scanClosestLeafCluster()`):

```java
double[] queryVector = {20.0, 30.0, 15.0};
VectorPointPredicate predicate = new VectorPointPredicate(queryVector);

accessor.search(cursor, predicate);
// Internally:
// 1. Navigate static structure (root → interior → leaf) to find closest centroid
// 2. Follow metadata_page_pointer from leaf frame
// 3. Scan metadata entries (no distance filtering for point predicate)
// 4. Traverse data pages via next_page chaining
// 5. Return all tuples in closest cluster sorted by distance

while (cursor.hasNext()) {
    cursor.next();
    ITupleReference tuple = cursor.getTuple();
    // Tuple format: <distance, cos(θ), vector, pk>
}
```

**Key Search Files:**
- `VectorPointPredicate.java`: Finds all records in closest cluster
- `VectorAnnPredicate.java`: ANN search with k and distance metric
- `VectorClusteringSearchCursor.java`: Base cursor for VCTree component
- `LSMVCTreeSearchCursor.java`: Coordinates search across LSM components

## Query Processing Flow

### Statement Handling in QueryTranslator

The `QueryTranslator` class (`asterixdb/asterix-app/src/main/java/org/apache/asterix/app/translator/QueryTranslator.java`) acts as the central coordinator for all SQL++ statements.

**Main Entry Point:** `QueryTranslator.compileAndExecute()`

Statement routing happens via a large switch statement on `Statement.Kind`:

```java
switch (stmt.getKind()) {
    case QUERY:
        handleQuery(metadataProvider, (Query) stmt, hcc, resultSet,
                    resultDelivery, outMetadata, stats, requestParameters,
                    stmtParams, stmtRewriter);
        break;
    case CREATE_VECTOR_INDEX:
        handleCreateVectorIndexStatement(metadataProvider, stmt, hcc,
                    requestParameters, Creator.DEFAULT_CREATOR);
        break;
    case CREATE_INDEX:
        handleCreateIndexStatement(metadataProvider, stmt, hcc,
                    requestParameters, Creator.DEFAULT_CREATOR);
        break;
    // ... other statement types
}
```

### Query Statement Processing

**Flow for SELECT queries** (`QueryTranslator.handleQuery()` at line 6017):

```java
protected void handleQuery(MetadataProvider metadataProvider, Query query,
                          IHyracksClientConnection hcc, ...) {
    // 1. Create metadata locker for compilation
    final IMetadataLocker locker = ...;

    // 2. Define statement compiler
    final IStatementCompiler compiler = () -> {
        // Begin metadata transaction
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Rewrite and compile query
        final JobSpecification jobSpec = rewriteCompileQuery(hcc, metadataProvider,
                                                             query, null, stmtParams, requestParameters);

        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        return query.isExplain() || isCompileOnly() ? null : jobSpec;
    };

    // 3. Deliver result based on delivery mode
    deliverResult(hcc, resultSet, compiler, metadataProvider, locker,
                  resultDelivery, outMetadata, stats, requestParameters, ...);
}
```

**Compilation Steps** (`QueryTranslator.rewriteCompileQuery()` at line 4725):

```java
public JobSpecification rewriteCompileQuery(...) {
    // 1. Query Rewriting (under ongoing metadata transaction)
    LangRewritingContext langRewritingContext = createLangRewritingContext(...);
    Pair<IReturningStatement, Integer> rewrittenResult =
        apiFramework.reWriteQuery(langRewritingContext, query, ...);

    // 2. Query Compilation (under same metadata transaction)
    return apiFramework.compileQuery(clusterInfoCollector, metadataProvider,
                                     (Query) rewrittenResult.first, ...);
}
```

**APIFramework Compilation:**
- `apiFramework.reWriteQuery()`: Applies SQL++ rewrites (FROM clause normalization, subquery decorrelation, etc.)
- `apiFramework.compileQuery()`: Translates SQL++ to Algebricks logical plan, optimizes, generates Hyracks physical plan
  - Creates `ILangExpressionToPlanTranslator` to convert SQL++ expressions to logical operators
  - Applies Algebricks optimization rules (predicate pushdown, join reordering, etc.)
  - Converts optimized logical plan to Hyracks `JobSpecification`

### CREATE VECTOR INDEX Processing

**Handler Flow** (`QueryTranslator.handleCreateVectorIndexStatement()` at line 1330):

```java
public void handleCreateVectorIndexStatement(MetadataProvider metadataProvider,
                                            Statement stmt, ...) {
    CreateIndexStatement stmtCreateIndex = (CreateIndexStatement) stmt;

    // 1. Extract statement parameters
    String datasetName = stmtCreateIndex.getDatasetName().getValue();
    String indexName = stmtCreateIndex.getIndexName().getValue();

    // 2. Acquire metadata locks
    lockUtil.createIndexBegin(lockManager, metadataProvider.getLocks(),
                             databaseName, dataverseName, datasetName, ...);

    // 3. Delegate to implementation
    doCreateVectorIndex(metadataProvider, stmtCreateIndex, databaseName,
                       dataverseName, datasetName, hcc, requestParameters, creator);
}
```

**Implementation: Three-Job Pattern** (`QueryTranslator.doCreateIndexImpl()` at line 1954):

Vector indexes use a specialized three-job execution pattern:

```java
if (index.getIndexType() == IndexType.VECTOR) {
    // === JOB 1: Create Empty Index Files ===
    // Build job spec to create LSMVCTree structure on each NC
    spec = IndexUtil.buildSecondaryIndexCreationJobSpec(ds, index,
                                                         metadataProvider, sourceLoc);
    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    runJob(hcc, spec, jobFlags);

    // Flush dataset to ensure consistency
    FlushDatasetUtil.flushDataset(hcc, metadataProvider, databaseName,
                                  dataverseName, datasetName);

    // === JOB 2: Create Static Structure ===
    // Build hierarchical k-means clustering structure
    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
    metadataProvider.setMetadataTxnContext(mdTxnCtx);
    spec = IndexUtil.buildSecondaryIndexStaticStructureJobSpec(ds, index,
                                                                metadataProvider, sourceLoc);
    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    runJob(hcc, spec, jobFlags);

    // === JOB 3: Bulk Load Data ===
    // Load actual vector data into the clustered structure
    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
    metadataProvider.setMetadataTxnContext(mdTxnCtx);
    spec = IndexUtil.buildSecondaryIndexLoadingJobSpec(ds, index,
                                                       metadataProvider, sourceLoc);
    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    runJob(hcc, spec, jobFlags);

    // === Final Cleanup ===
    // Update index metadata: PendingAddOp → PendingNoOp
    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
    MetadataManager.INSTANCE.dropIndex(..., index.getIndexName());
    index.setPendingOp(MetadataUtil.PENDING_NO_OP);
    MetadataManager.INSTANCE.addIndex(..., index);
    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
}
```

**JobSpecification Creation** (via `IndexUtil` at `asterixdb/asterix-app/src/main/java/org/apache/asterix/utils/IndexUtil.java`):

- **Job 1** (`buildSecondaryIndexCreationJobSpec()`):
  - Creates `IIndexBulkLoadOperatorDescriptor` nodes
  - Sets up dataflow: one empty index creator per NC partition
  - Returns JobSpec that creates LSMVCTree files without data

- **Job 2** (`buildSecondaryIndexStaticStructureJobSpec()`):
  - Creates operators to:
    - Sample `train_list` records from dataset
    - Run hierarchical k-means++ clustering
    - Build interior/leaf frame hierarchy
    - Persist static structure to disk
  - Returns JobSpec for structure building

- **Job 3** (`buildSecondaryIndexLoadingJobSpec()`):
  - Creates dataflow: Dataset Scan → BulkLoad → Index
  - `LSMVCTreeBulkLoader` handles:
    - Copying static structure pages
    - Computing distances to leaf centroids
    - Assigning vectors to clusters
    - Creating metadata and data frames
  - Returns JobSpec for bulk loading

### Regular Index Creation (B+Tree, R-Tree)

Regular indexes use a **two-job pattern**:

```java
// JOB 1: Create empty index structure
spec = IndexUtil.buildSecondaryIndexCreationJobSpec(ds, index, metadataProvider, sourceLoc);
runJob(hcc, spec, jobFlags);
FlushDatasetUtil.flushDataset(...);

// JOB 2: Load data into index
spec = IndexUtil.buildSecondaryIndexLoadingJobSpec(ds, index, metadataProvider, sourceLoc);
runJob(hcc, spec, jobFlags);
```

### Key Differences: Query vs DDL Statements

**Queries (SELECT):**
- Single-job execution
- Compilation: SQL++ → Algebricks logical plan → Optimized plan → Hyracks JobSpec
- Result delivery: IMMEDIATE, DEFERRED, or ASYNC
- Uses `apiFramework.compileQuery()`

**DDL Statements (CREATE VECTOR INDEX):**
- Multi-job execution (3 jobs for vector indexes)
- Direct JobSpec generation via `IndexUtil` builders
- Metadata transaction coordination between jobs
- Manual index lifecycle management (PendingAddOp → PendingNoOp)

## Vector Index Query Optimization (Top-K ANN Search)

### Overview

Vector index optimization for ANN (Approximate Nearest Neighbor) queries follows a different pattern than traditional SELECT-based optimizations:

**Query Pattern:**
```sql
SELECT id, year FROM movie
WHERE year > 2000  -- Optional filter
ORDER BY ANN_DISTANCE(reviewEmbedding, [1.0, 2.0, ...], "Euclidean")
LIMIT 10
```

**Key Components:**

### 1. VectorIndexAccessMethod

**Location:** `asterixdb/asterix-algebra/src/main/java/org/apache/asterix/optimizer/rules/am/VectorIndexAccessMethod.java`

**Responsibilities:**
- Implements `IAccessMethod` interface for vector indexes
- Registers `ANN_DISTANCE` as optimizable function
- Validates ANN_DISTANCE function arguments
- Matches vector index type (IndexType.VECTOR)

**Key Methods:**
- `getOptimizableFunctions()`: Returns `ANN_DISTANCE` function identifier
- `analyzeFuncExprArgsAndUpdateAnalysisCtx()`: Validates function arguments
  - arg0: Vector field reference (variable)
  - arg1: Query vector (constant or parameter)
  - arg2: Distance metric (string constant: "Euclidean", "Cosine", etc.)
- `matchIndexType()`: Returns true for IndexType.VECTOR
- `createTopKIndexSearchPlan()`: **Custom method** for creating vector index search plan
  - Similar to how RTree/BTree call `createIndexSearchPlan()` from `applySelectPlanTransformation()`
  - Adapted for ORDER BY + LIMIT pattern instead of SELECT pattern
  - Accepts LIMIT operator, ORDER operator, ANN_DISTANCE expression
  - Creates UNNEST-MAP operator with vector index search function
  - Returns transformed plan rooted at UNNEST-MAP operator

### 2. IntroduceTopKAccessMethodRule

**Location:** `asterixdb/asterix-algebra/src/main/java/org/apache/asterix/optimizer/rules/am/IntroduceTopKAccessMethodRule.java`

**Responsibilities:**
- Optimization rule for top-k ANN queries
- Matches pattern: `LIMIT k → ORDER BY ANN_DISTANCE(...) → ... → DATASOURCE_SCAN`
- Checks for available vector indexes on the accessed field
- Transforms plan to use vector index top-k search

**Optimization Flow:**

```
1. Pattern Matching (checkAndApplyTopKTransformation)
   - Find LIMIT operator
   - Check child is ORDER operator
   - Verify ORDER BY uses ANN_DISTANCE function

2. Analysis (analyzeAndTransform)
   - Initialize subtree from ORDER to DATASOURCE_SCAN
   - Load dataset metadata and vector indexes
   - Analyze ANN_DISTANCE function arguments
   - Map variables to applicable vector indexes

3. Index Selection (chooseVectorIndex)
   - Iterate over candidate vector indexes
   - Select best index (currently: first match)
   - TODO: Add cost-based selection

4. Plan Transformation (applyTopKPlanTransformation)
   - Calls VectorIndexAccessMethod.createTopKIndexSearchPlan()
   - This delegates to the custom method similar to how BTree/RTree use createIndexSearchPlan()
   - If transformation succeeds, replaces ORDER operator with UNNEST-MAP
   - Maintains LIMIT → UNNEST-MAP → (rest of plan) structure
```

**Transformed Plan:**

```
Before Optimization:
  LIMIT 10
    ↓
  ORDER BY ANN_DISTANCE(reviewEmbedding, qvec, "Euclidean")
    ↓
  SELECT (year > 2000)
    ↓
  DATASOURCE_SCAN

After Optimization:
  LIMIT 10
    ↓
  UNNEST-MAP (vector index top-k search)
    ↓
  SELECT (year > 2000)  -- Post-filter if needed
    ↓
  (index scan returns sorted top-k results)
```

### Integration with Other Indexes

Vector index optimization works alongside other index optimizations:

**Example Query:**
```sql
SELECT id, year FROM movie
WHERE year > 2000  -- B-tree index on year
ORDER BY ANN_DISTANCE(reviewEmbedding, qvec, "Euclidean")  -- Vector index
LIMIT 10
```

**Optimization Pipeline:**
1. `IntroduceSelectAccessMethodRule` runs first → Optimizes `year > 2000` with B-tree
2. `IntroduceTopKAccessMethodRule` runs next → Optimizes ANN search with vector index

**Combined Plan:**
```
LIMIT 10
  ↓
UNNEST-MAP (vector index top-k search on reviewEmbedding)
  ↓
SELECT (year > 2000)  -- Or push filter into index if supported
  ↓
UNNEST-MAP (B-tree index search on year > 2000)
```

### Registration in Optimization Pipeline

**Location:** Add to physical optimization rules in compilation provider (e.g., `SqlppCompilationProvider.java`)

```java
// Add after IntroduceSelectAccessMethodRule
physicalRewriteRules.add(new IntroduceTopKAccessMethodRule());
```

### ANN_DISTANCE Function Definition

**Location:** `asterixdb/asterix-om/src/main/java/org/apache/asterix/om/functions/BuiltinFunctions.java`

Add function identifier:
```java
public static final FunctionIdentifier ANN_DISTANCE =
    new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ann-distance", 3);
```

Register in static initialization block with appropriate type computer.

### Current Status

**Implemented:**
- ✅ VectorIndexAccessMethod class with custom `createTopKIndexSearchPlan()` method
- ✅ IntroduceTopKAccessMethodRule class
- ✅ Pattern matching (LIMIT → ORDER BY ANN_DISTANCE)
- ✅ Vector index discovery and selection
- ✅ Analysis context setup
- ✅ Rule registered in RuleCollections.buildAccessMethodRuleCollection()
- ✅ Delegation from IntroduceTopKAccessMethodRule to VectorIndexAccessMethod.createTopKIndexSearchPlan()

**TODO:**
- ⚠️ Complete `VectorIndexAccessMethod.createTopKIndexSearchPlan()` implementation
  - Extract k from LIMIT operator
  - Extract query vector and distance metric from ANN_DISTANCE expression
  - Create AssignOperator for search key (query vector) if needed
  - Create UNNEST-MAP operator with vector index search function
  - Generate logical variables for results (distance, primary_key)
  - Connect operators properly: UNNEST-MAP → (rest of plan below ORDER)
  - Update type environment with new variables
- ⚠️ Define ANN_DISTANCE function in BuiltinFunctions (user completed TODO-1)
- ⚠️ Implement vector index search runtime operator at Hyracks layer
- ⚠️ Add cost-based index selection for multiple vector indexes

**Architecture Notes:**
- Following RTreeAccessMethod pattern: custom method for plan creation
- VectorIndexAccessMethod.createTopKIndexSearchPlan() is analogous to RTree/BTree's createIndexSearchPlan()
- Difference: Called for ORDER BY pattern instead of SELECT pattern
- Method will use AccessMethodUtils helper functions for creating UNNEST-MAP operators

### Testing

**Test queries:**
```sql
-- Basic top-k query
SELECT id FROM movie
ORDER BY ANN_DISTANCE(reviewEmbedding, [1.0, 2.0, 3.0], "Euclidean")
LIMIT 10;

-- Top-k with filter
SELECT id FROM movie
WHERE year > 2000
ORDER BY ANN_DISTANCE(reviewEmbedding, [1.0, 2.0, 3.0], "Euclidean")
LIMIT 10;

-- Fallback to data scan (no vector index)
SELECT id FROM movie2
ORDER BY ANN_DISTANCE(embedding, [1.0, 2.0], "Cosine")
LIMIT 5;
```

## ANN Top-K Query Data Flow

### Complete Index Structure

The VCTree index has a **three-tier structure**:

```
┌─────────────────────────────────────────────────────────┐
│              TIER 1: STATIC STRUCTURE                    │
│  Root Page → Interior Pages → Leaf Pages                │
│  Leaf tuple: <cid, centroid, metadata_page_ptr>         │
└─────────────────────────────────────────────────────────┘
                        ↓ metadata_page_ptr
┌─────────────────────────────────────────────────────────┐
│         TIER 2: METADATA/DIRECTORY PAGES                 │
│  One metadata page per leaf centroid                     │
│  Tuple: <max_distance, data_page_id>                    │
│  Purpose: Triangle inequality pruning (future)          │
└─────────────────────────────────────────────────────────┘
                        ↓ data_page_id
┌─────────────────────────────────────────────────────────┐
│              TIER 3: DATA PAGES                          │
│  Actual vector records for this cluster                  │
│  Tuple: <distance, cos(θ), vector, pk>                  │
│  Chained: p100 → p101 → p102 → -1                       │
└─────────────────────────────────────────────────────────┘
```

### Query Example

```sql
SELECT row.idx, row.title, row.year
FROM Movie1 row
ORDER BY ann_distance(embedding, query_vector(ignored), "euclidean")
LIMIT 10;
```

### Layer 1: AsterixDB Operator Layer

**VectorSearchOperatorNodePushable**
- Entry point for vector search queries
- Creates `VectorPointPredicate` with query tuple reference (no deserialization yet)
- Adds `vectorAccessorFactory` to index access parameters
- Returns only primary keys (no distance values in current implementation)

**Key Methods:**
- `createSearchPredicate()`: Returns simple `VectorPointPredicate()`
- `resetSearchPredicate(tupleIndex)`: Updates predicate with current tuple reference
- `addAdditionalIndexAccessorParams()`: Stores vector accessor factory in params

### Layer 2: LSM Coordination Layer

**LSMVCTreeSearchCursor**
- Coordinates search across multiple LSM components: `[memory, disk1, disk2, ...]`
- Creates `VectorClusteringTreeAccessor` for each component
- Opens one `VectorClusteringSearchCursor` per component
- **Sequential iteration** through components (no priority queue merge yet)

**Key Flow:**
```java
doOpen():
    for each component:
        vcTreeAccessors[i] = createAccessor(component)
        rangeCursors[i] = createCursor(accessor)
    IndexCursorUtils.open(vcTreeAccessors, rangeCursors, searchPred)
    currentComponentIndex = 0

doHasNext():
    return currentComponentCursor.hasNext()

checkPriorityQueue():
    while currentComponentIndex < components.length:
        if currentComponentCursor.hasNext():
            return
        currentComponentIndex++  // Move to next component
```

**Memory Component Replacement:**
- Every `SWITCH_COMPONENT_CYCLE` calls, checks if memory components were flushed
- Replaces with disk components and reopens searches

### Layer 3: Single Component Search

**VectorClusteringSearchCursor**
- Searches a single VCTree component (memory or disk)
- After `open()`, cursor is positioned at **first tuple of closest cluster's first data page**

**Critical Flow in `open()`:**

```java
// STEP 1: Find closest leaf centroid via tree traversal
ClusterSearchResult clusterResult =
    accessor.findClosestLeafCentroid(queryVector);
// Returns: {leafPageId=8, clusterIndex=4, centroidId=12, distance=5.3}

// STEP 2: Extract metadata page pointer from leaf frame
targetMetadataPageId = getMetadataPageIdFromCluster(clusterResult);
// Pin leaf page 8 → get tuple 4 → extract metadata_page_ptr → returns 204

// STEP 3: Get first data page ID from metadata page
currentDataPageId = getFirstDataPageFromMetadata();
// Pin metadata page 204 → get tuple 0: <max_distance=2.5, data_page_id=300>

// STEP 4: Open first data page and position cursor
openDataPage(currentDataPageId);
// Pin data page 300 → initialize dataFrame → currentTupleIndex = 0
// Cursor is now READY TO ITERATE from first tuple!
```

**Page Navigation:**
```java
getMetadataPageIdFromCluster(clusterResult):
    leafPage = bufferCache.pin(clusterResult.leafPageId)
    leafFrame.setPage(leafPage)
    return leafFrame.getMetadataPagePointer(clusterResult.clusterIndex)

getFirstDataPageFromMetadata():
    metadataPage = bufferCache.pin(targetMetadataPageId)
    metadataFrame.setPage(metadataPage)
    return metadataFrame.getDataPagePointer(0)  // First entry

moveToNextDataPage():
    nextDataPageId = dataFrame.getNextPage()  // Follow chain
    if (nextDataPageId == -1): return false
    openDataPage(nextDataPageId)
    return true
```

**Data Page Chain Example:**
```
Data Page 300: tuples 0-99   (next_page=301)
    ↓ moveToNextDataPage()
Data Page 301: tuples 0-99   (next_page=302)
    ↓ moveToNextDataPage()
Data Page 302: tuples 0-50   (next_page=-1)
    ↓ moveToNextDataPage()
return false; // Chain exhausted
```

### Layer 4: Tree Navigation (Static Structure)

**VCTreeNavigationUtils.findClosestCentroid()**
- Traverses static structure: root → interior pages → leaf pages
- **CRITICAL FIX**: Handles overflow pages correctly in both interior and leaf levels

**Key Methods:**

1. **`findClosestInLeafPage()` - WITH OVERFLOW FIX**
   ```java
   // Unified loop through overflow chain: p10 → p20 → p21
   while (currentPageId != -1) {
       // Pin overflow pages (not first page - already pinned by caller)
       if (!isFirstPage) {
           currentPage = bufferCache.pin(...getDiskPageId(fileId, currentPageId))
           currentPage.acquireReadLatch()
       }

       // Check overflow flag before following chain
       boolean hasOverflow = currentFrame.getOverflowFlagBit()
       int nextPageId = hasOverflow ? currentFrame.getNextLeaf() : -1

       // Search all centroids in this page
       for (int i = 0; i < tupleCount; i++) {
           double distance = calculateDistance(queryVector, centroid)
           if (distance < bestDistance) {
               bestDistance = distance
               bestTupleIndex = i
               bestPageId = currentPageId  // Track which page!
               bestCentroidId = getCentroidId(i)
           }
       }

       currentPageId = nextPageId
       isFirstPage = false
   }
   ```

2. **`findClosestInInteriorPage()` - WITH OVERFLOW FIX**
   - Same pattern as leaf page traversal
   - Returns child page ID from best centroid found across all overflow pages

**Returns ClusterSearchResult:**
```java
{
    leafPageId: 8,         // Page where closest centroid found
    clusterIndex: 4,       // Tuple index within that page
    centroid: [1.2, ...],  // Centroid vector
    distance: 5.3,         // Distance to centroid
    centroidId: 12         // Centroid ID
}
```

### Complete Data Flow Summary

```
SQL Query: SELECT ... ORDER BY ann_distance(...) LIMIT 10
    ↓
┌─────────────────────────────────────────────┐
│ VectorSearchOperatorNodePushable            │ ← AsterixDB Layer
│ - Creates VectorPointPredicate              │
│ - Adds vectorAccessorFactory to params      │
└─────────────────────────────────────────────┘
    ↓ accessor.search(cursor, pred, opCtx)
┌─────────────────────────────────────────────┐
│ LSMVCTreeSearchCursor                        │ ← LSM Layer
│ - Iterates: [memory, disk1, disk2, ...]    │
│ - Sequential iteration (no merge yet)       │
└─────────────────────────────────────────────┘
    ↓ open each component cursor
┌─────────────────────────────────────────────┐
│ VectorClusteringSearchCursor.open()         │ ← Component Layer
│ Step 1: findClosestLeafCentroid(queryVec)   │
│         Returns ClusterSearchResult         │
│ Step 2: getMetadataPageId(clusterResult)    │
│         Pin leaf page → extract ptr → 204   │
│ Step 3: getFirstDataPage(metadataPageId)    │
│         Pin metadata page → get ptr → 300   │
│ Step 4: openDataPage(300)                   │
│         Pin data page → cursor READY!       │
└─────────────────────────────────────────────┘
    ↓ findClosestLeafCentroid()
┌─────────────────────────────────────────────┐
│ VCTreeNavigationUtils                        │ ← Navigation Layer
│ - Traverse: root → interior → leaf          │
│ - WITH OVERFLOW FIX for both levels         │
│ - Returns ClusterSearchResult               │
└─────────────────────────────────────────────┘
    ↓
Results: Iterate through data pages 300→301→302
         Returns: [(pk1), (pk2), ..., (pk10)]
```

### Triangle Inequality Optimization (Future)

Metadata pages enable triangle inequality pruning:

**Given:**
- `d(q, c)` = distance from query to centroid
- `max_distance` = maximum distance of vectors in a data page to centroid

**Triangle Inequality:**
```
d(q, v) ≥ |d(q, c) - d(v, c)|

If |d(q, c) - max_distance| > k-th_distance:
  → Skip this data page (all vectors are too far)
```

**Current:** Iterates all data pages sequentially (no pruning yet)
**Future:** Check metadata entries and skip data pages based on inequality

### Key Implementation Notes

1. **Overflow Page Handling:** Fixed in PR (see fixes below)
2. **No Distance Values:** Current implementation only returns primary keys
3. **Sequential Component Search:** No priority queue merge across LSM components yet
4. **Single Cluster Search:** Each component returns all records from closest cluster
5. **Cursor Positioning:** After `open()`, cursor is ready at first tuple

### Recent Bug Fixes

**1. Overflow Page Traversal (VCTreeNavigationUtils)**
- **Bug:** `findClosestInLeafPage()` and `findClosestInInteriorPage()` only searched first page
- **Fix:** Unified loop iterates through overflow chain using `getOverflowFlagBit()`
- **Impact:** Ensures correct closest centroid even when clusters span multiple pages

**2. Skipped Centroid IDs (VCTreeBulkLoader)**
- **Bug:** When centroid IDs had gaps (e.g., 10, 11, skip 12, 13), data written to wrong clusters
- **Fix:** Calculate target cluster index: `targetClusterIndex = tupleCentroidId - firstLeafCentroidId`
- **Impact:** Ensures data loaded into correct cluster even with non-uniform distribution

**3. Dot Product Distance Sign Mismatch — Correctness Bug (VectorDistanceArrScalarEvaluator)**
- **Bug:** The SQL++ functions `ann_distance` and `vector_distance` mapped "dot" to raw `dot(a,b)` (positive), while the index internally uses `-dot(a,b)` (negated for MIPS convention: smaller = more similar). This caused the index to pre-select the most similar candidates, but `ORDER BY dist ASC` then returned the least similar ones — a complete inversion. Quantized dot product queries returned recall=0. Non-quantized was unaffected because `LSMVCTreeSearchCursor` returns all tuples from the closest cluster without pre-ranking.
- **Fix:** Negated dot product in `DISTANCE_MAP` in `VectorDistanceArrScalarEvaluator.java`: `(a, b) -> -VectorDistanceArrCalculation.dot(a, b)`
- **Files:** `asterixdb/asterix-runtime/src/main/java/org/apache/asterix/runtime/evaluators/functions/vector/VectorDistanceArrScalarEvaluator.java`
- **Impact:** Both `ann_distance` and `vector_distance` now return `-dot(a,b)`, consistent with the index convention. Quantized dot product recall went from 0.00 to functional.

**4. Epsilon Threshold Ineffective for Dot Product — Fundamental Cause of Low Recall (VCTreeNavigationUtils)**
- **Bug:** The cluster selection logic in `findCloseCentroidsLevelWiseGlobalSort()` uses `threshold = closestDistance + epsilon` at both interior and leaf levels to decide which clusters to explore. For euclidean (positive distances), additive epsilon opens a meaningful window (e.g., `5.0 + 0.15 = 5.15`). For dot product (negative distances), the window is negligible (e.g., `-150.0 + 0.15 = -149.85`, a 0.1% window). This meant the search explored only ~1 cluster regardless of nprobe/epsilon settings, capping recall at 0.60-0.73.
- **Fix:** Use multiplicative threshold when distances are negative: `closestDistance < 0 ? closestDistance * (1.0 - epsilon) : closestDistance + epsilon`. For dot product, epsilon=0.15 now gives a 15% window (e.g., `-150.0 * 0.85 = -127.5`). Applied at both interior level (line 673) and leaf level (line 705).
- **Files:** `hyracks-fullstack/hyracks/hyracks-storage-am-btree/src/main/java/org/apache/hyracks/storage/am/vector/utils/VCTreeNavigationUtils.java`
- **Impact:** Quantized dot product recall improved from 0.60-0.73 to 1.00. Non-quantized (euclidean, cosine) behavior unchanged since their distances are non-negative.

**5. Dequantization Missing Inverse Mapping (OptimizedScalarQuantizationSampleFile)**
- **Bug:** `dequantizeToDoubleArray()` performed only a simple type cast (`(double)(bytes[i] & 0xFF)`) instead of applying the inverse quantization formula. This left dequantized values in "quantized integer space" rather than approximating the original value space. For euclidean, ranking was preserved because the shift cancels in subtraction. For dot product, the `minQuantile` bias term varies per vector and corrupts ranking.
- **Fix:** Applied inverse formula: `result[i] = ((double)(bytes[i] & 0xFF)) / params.alpha + params.minQuantile` for all bit-width cases.
- **Files:** `asterixdb/asterix-common/src/main/java/org/apache/asterix/common/storage/OptimizedScalarQuantizationSampleFile.java`
- **Impact:** Dequantized values now approximate original vector values, preserving ranking for all distance metrics.

## Multi-Cluster Search Design (Future Work)

### Problem Statement

**Current limitation:** The single-cluster search approach fails when the closest cluster doesn't contain enough records to satisfy the LIMIT K requirement.

**Scenario:**
```sql
SELECT * FROM dataset
ORDER BY ann_distance(vector, [8, 2, 0], "euclidean")
LIMIT 10;
```

**Failure case with non-uniform distribution:**
```
Cluster c11 (closest, d=2.83):  3 records  ❌ Not enough!
Cluster c10 (2nd, d=8.25):     150 records
Cluster c12 (3rd, d=11.31):     80 records
Cluster c13 (4th, d=12.96):      1 record

Current: Returns only 3 records (fails to meet K=10)
Needed:  Scan multiple clusters to collect ≥ K records
```

### Proposed Solution: Distance-Based Multi-Cluster Probing with Triangle Inequality

**Algorithm Overview:**
1. Find closest cluster and calculate initial search radius
2. Identify candidate clusters within radius using triangle inequality
3. Probe clusters in distance order, maintaining top-K heap
4. Early termination when remaining clusters can't improve results

### Detailed Algorithm

#### **Step 1: Find Closest Cluster**

```java
// Navigate tree to find closest leaf centroid
ClusterSearchResult closest = findClosestLeafCentroid(queryVector);
// closest = {centroidId: 11, distance: 2.83, ...}
```

#### **Step 2: Calculate Search Radius**

Use metadata to estimate maximum distance within closest cluster:

```java
// Pin metadata page for closest cluster
IVectorClusteringMetadataFrame metadataFrame = getMetadataFrame(closest);

// Get maximum distance from metadata entries
// Metadata tuple format: <max_distance, data_page_id>
double maxDistInCluster = 0.0;
for (int i = 0; i < metadataFrame.getTupleCount(); i++) {
    double maxDist = metadataFrame.getMaxDistance(i);
    maxDistInCluster = Math.max(maxDistInCluster, maxDist);
}

// Calculate initial search radius using triangle inequality
// For any vector v in closest cluster:
//   d(q, v) ≤ d(q, c) + d(v, c) ≤ closest.distance + maxDistInCluster
double searchRadius = closest.distance + maxDistInCluster;
```

**Triangle Inequality Insight:**
```
Query q = [8, 2, 0]
Closest centroid c11 = [10, 0, 0]
d(q, c11) = 2.83
maxDistInCluster = 2.0  (from metadata)

Worst-case distance to any vector v in c11:
  d(q, v) ≤ d(q, c11) + d(v, c11)
  d(q, v) ≤ 2.83 + 2.0 = 4.83

Therefore: searchRadius = 4.83
```

#### **Step 3: Find Candidate Clusters Within Radius**

Scan all leaf-level centroids and collect those within search radius:

```java
List<ClusterSearchResult> findClustersWithinRadius(
    double[] queryVector, double radius) {

    List<ClusterSearchResult> candidates = new ArrayList<>();

    // Traverse to leaf level (same as current implementation)
    int leafPageId = navigateToLeafLevel(queryVector);

    // Modified: Scan ALL leaf pages (including overflow chains)
    int currentPageId = leafPageId;
    while (currentPageId != -1) {
        IVectorClusteringLeafFrame leafFrame = pinAndLatchLeafPage(currentPageId);

        // Check overflow flag
        boolean hasOverflow = leafFrame.getOverflowFlagBit();
        int nextPageId = hasOverflow ? leafFrame.getNextLeaf() : -1;

        // Evaluate all centroids in this page
        for (int i = 0; i < leafFrame.getTupleCount(); i++) {
            double[] centroid = extractCentroid(leafFrame, i);
            double distance = calculateEuclideanDistance(queryVector, centroid);

            if (distance <= radius) {
                candidates.add(new ClusterSearchResult(
                    leafFrame.getCentroidId(i),
                    distance,
                    currentPageId,
                    i
                ));
            }
        }

        // Move to next leaf page
        currentPageId = getNextLeafPageInTree(currentPageId, nextPageId);
    }

    // Sort by distance to query
    candidates.sort(Comparator.comparingDouble(c -> c.distance));
    return candidates;
}
```

**Adaptive Radius Expansion:**

If initial radius yields insufficient candidate clusters:

```java
// Check if we need more candidates
int estimatedRecords = estimateTotalRecords(candidates);
if (estimatedRecords < K) {
    // Expand radius to include next closest cluster
    double nextClusterDist = findNextClosestCluster(candidates, allClusters);
    searchRadius = nextClusterDist + avgMaxDistInCluster;

    // Re-scan with expanded radius
    candidates = findClustersWithinRadius(queryVector, searchRadius);
}
```

#### **Step 4: Probe Clusters and Maintain Top-K**

```java
PriorityQueue<Result> probeMultipleClusters(
    double[] queryVector,
    List<ClusterSearchResult> candidateClusters,
    int K) {

    // Max-heap: largest distance at top for easy replacement
    PriorityQueue<Result> topK = new PriorityQueue<>(K,
        Comparator.comparingDouble(r -> -r.distance));

    for (ClusterSearchResult cluster : candidateClusters) {
        // Early termination check using triangle inequality
        if (topK.size() >= K) {
            double kthDistance = topK.peek().distance;

            // For any vector v in this cluster:
            //   d(q, v) ≥ |d(q, c) - d(v, c)| ≥ |d(q, c) - maxDistInCluster|
            double minPossibleDist = cluster.distance - maxDistInCluster;

            if (minPossibleDist > kthDistance) {
                // All remaining clusters are too far
                break;
            }
        }

        // Scan this cluster and update top-K
        scanClusterAndUpdateTopK(cluster, queryVector, topK, K);
    }

    return topK;
}

void scanClusterAndUpdateTopK(ClusterSearchResult cluster,
                              double[] queryVector,
                              PriorityQueue<Result> topK,
                              int K) {
    // Get metadata page pointer from leaf frame
    long metadataPageId = getMetadataPageId(cluster);

    // Get first data page from metadata
    long firstDataPageId = getFirstDataPage(metadataPageId);

    // Scan all data pages in chain
    long currentDataPageId = firstDataPageId;
    while (currentDataPageId != -1) {
        IVectorClusteringDataFrame dataFrame = pinAndLatchDataPage(currentDataPageId);

        // Process all tuples in this data page
        for (int i = 0; i < dataFrame.getTupleCount(); i++) {
            double[] vector = extractVector(dataFrame, i);
            String pk = extractPrimaryKey(dataFrame, i);
            double distance = calculateEuclideanDistance(queryVector, vector);

            // Update top-K heap
            if (topK.size() < K) {
                topK.offer(new Result(pk, distance));
            } else if (distance < topK.peek().distance) {
                topK.poll();  // Remove worst
                topK.offer(new Result(pk, distance));
            }
        }

        // Move to next data page in chain
        currentDataPageId = dataFrame.getNextPage();
    }
}
```

### Example Walkthrough

**Setup:**
```
Query: q = [8, 2, 0], K = 10

Leaf centroids:
- c10: [0, 0, 0]    → d(q, c10) = 8.25
- c11: [10, 0, 0]   → d(q, c11) = 2.83  ← Closest
- c12: [0, 10, 0]   → d(q, c12) = 11.31
- c13: [0, 0, 10]   → d(q, c13) = 12.96

Data distribution:
- c11: 3 records    (max_dist_in_cluster = 2.0)
- c10: 150 records  (max_dist_in_cluster = 5.0)
- c12: 80 records   (max_dist_in_cluster = 4.0)
- c13: 1 record     (max_dist_in_cluster = 1.0)
```

**Execution:**

1. **Find closest cluster:**
   - `closest = c11 (d=2.83)`

2. **Calculate search radius:**
   - `maxDistInCluster = 2.0` (from c11's metadata)
   - `searchRadius = 2.83 + 2.0 = 4.83`

3. **Find candidates within radius 4.83:**
   - `c11 (d=2.83)` ✓ Add
   - `c10 (d=8.25)` ✗ Skip (too far)
   - Only 1 candidate with ~3 records < K=10

4. **Expand radius:**
   - `searchRadius = 8.25 + 5.0 = 13.25`

5. **Find candidates within radius 13.25:**
   - `c11 (d=2.83)` ✓
   - `c10 (d=8.25)` ✓
   - `c12 (d=11.31)` ✓
   - `c13 (d=12.96)` ✓
   - Candidates sorted: `[c11, c10, c12, c13]`

6. **Probe clusters:**

   **a. Scan c11 (d=2.83, 3 records):**
   ```
   v1: d(q, v1) = 2.65
   v2: d(q, v2) = 2.11
   v3: d(q, v3) = 3.71

   topK = [v3(3.71), v1(2.65), v2(2.11)]
   topK.size = 3 < K=10 → Continue
   ```

   **b. Scan c10 (d=8.25, 150 records):**
   ```
   v4: d(q, v4) = 0.54
   v5: d(q, v5) = 0.25
   v6: d(q, v6) = 2.35
   ... (scan 7 more records) ...

   topK = [v11(2.50), v7(2.35), v2(2.11), v9(1.14), v8(1.12),
           v10(0.58), v4(0.54), v12(0.45), v13(0.32), v5(0.25)]
   topK.size = 10 ≥ K=10 ✓
   k-th distance = 2.50
   ```

   **c. Early termination check for c12:**
   ```
   Triangle inequality:
   For any v in c12: d(q, v) ≥ |d(q, c12) - d(v, c12)|
                     d(q, v) ≥ |11.31 - 4.0| = 7.31

   Since 7.31 > 2.50 (k-th distance):
     → All vectors in c12 are too far
     → Skip c12 and c13 ✓
   ```

7. **Return top-K:**
   ```
   Results: 10 records (9 from c10, 1 from c11)
   Clusters scanned: 2 (c11, c10)
   Clusters skipped: 2 (c12, c13) via early termination
   ```

### Implementation Considerations

**Changes to VCTreeNavigationUtils:**
- Add `findClustersWithinRadius(queryVector, radius)` method
- Modify `findClosestCentroid()` to scan all leaf pages (not just path to closest)
- Handle overflow pages at leaf level during candidate collection

**Changes to VectorClusteringSearchCursor:**
- Support multiple cluster iteration instead of single cluster
- Add `openNextCluster()` method for cluster transitions
- Maintain state: `List<ClusterSearchResult> remainingClusters`

**Changes to LSMVCTreeSearchCursor:**
- Merge results from multiple components using global top-K heap
- Coordinate multi-cluster search across memory and disk components
- Handle component switching during multi-cluster probing

**Metadata Usage:**
- Use `max_distance` from metadata frames for radius calculation
- Enables cheap cluster size estimation without scanning data pages
- Critical for early termination via triangle inequality

### Alternative Approaches

#### **Option 2: Beam Search with Fixed Probes** (Simpler Initial Implementation)

**Concept:** Always probe a fixed number of top-N closest clusters

**Algorithm:**
```java
public List<Result> beamSearch(double[] queryVector, int K) {
    // Step 1: Calculate number of clusters to probe
    int numProbes = calculateNumProbes(K);
    // Example heuristics:
    // - numProbes = Math.max(5, K / 100)
    // - numProbes = Math.min(10, numLeafCentroids / 10)
    // - numProbes = 5 (fixed)

    // Step 2: Find top-N closest clusters
    List<ClusterSearchResult> topClusters =
        findTopNClosestClusters(queryVector, numProbes);

    // Step 3: Scan all probe clusters and collect results
    PriorityQueue<Result> topK = new PriorityQueue<>(K,
        Comparator.comparingDouble(r -> -r.distance));

    for (ClusterSearchResult cluster : topClusters) {
        scanClusterAndUpdateTopK(cluster, queryVector, topK, K);
    }

    // Step 4: Return top-K results
    return extractTopK(topK, K);
}

List<ClusterSearchResult> findTopNClosestClusters(
    double[] queryVector, int N) {

    PriorityQueue<ClusterSearchResult> topN =
        new PriorityQueue<>(N, Comparator.comparingDouble(c -> -c.distance));

    // Scan all leaf pages (including overflow chains)
    int currentPageId = navigateToLeafLevel(queryVector);
    while (currentPageId != -1) {
        IVectorClusteringLeafFrame leafFrame = pinAndLatchLeafPage(currentPageId);

        for (int i = 0; i < leafFrame.getTupleCount(); i++) {
            double[] centroid = extractCentroid(leafFrame, i);
            double distance = calculateEuclideanDistance(queryVector, centroid);

            if (topN.size() < N) {
                topN.offer(new ClusterSearchResult(..., distance));
            } else if (distance < topN.peek().distance) {
                topN.poll();
                topN.offer(new ClusterSearchResult(..., distance));
            }
        }

        currentPageId = getNextLeafPage(currentPageId);
    }

    // Sort by distance (ascending)
    List<ClusterSearchResult> result = new ArrayList<>(topN);
    result.sort(Comparator.comparingDouble(c -> c.distance));
    return result;
}
```

**Example Execution:**

Setup (same as before):
```
Query: q = [8, 2, 0], K = 10, numProbes = 5

Leaf centroids:
- c10: [0, 0, 0]    → d(q, c10) = 8.25
- c11: [10, 0, 0]   → d(q, c11) = 2.83
- c12: [0, 10, 0]   → d(q, c12) = 11.31
- c13: [0, 0, 10]   → d(q, c13) = 12.96
- c14: [15, 15, 0]  → d(q, c14) = 14.76

Data distribution:
- c11: 3 records
- c10: 150 records
- c12: 80 records
- c13: 1 record
- c14: 200 records
```

**Step 1:** Calculate numProbes
```java
numProbes = Math.max(5, 10 / 100) = 5
```

**Step 2:** Find top-5 closest clusters
```java
// Scan all leaf centroids
topClusters = [c11(2.83), c10(8.25), c12(11.31), c13(12.96), c14(14.76)]
```

**Step 3:** Scan all 5 clusters
```java
Scan c11: Add 3 records to topK
Scan c10: Add best 7 records (replace worse ones), topK.size = 10
Scan c12: Check each record, may replace some in topK
Scan c13: Check 1 record, may replace some in topK
Scan c14: Check records, may replace some in topK

Final topK: 10 best records from all scanned clusters
```

**Advantages:**
- ✅ Very simple to implement (no radius calculation, no early termination logic)
- ✅ Predictable I/O cost (always scan exactly N clusters)
- ✅ Works well for uniform distributions
- ✅ Guaranteed to return K results if scanned clusters have ≥ K total records

**Disadvantages:**
- ❌ May scan unnecessary clusters (if first cluster already has K records)
- ❌ May still miss results (if numProbes too small for very skewed distributions)
- ❌ Requires tuning `numProbes` parameter (dataset-dependent)
- ❌ No early termination (always scans all N clusters even if not needed)

**When to Use:**
- Initial implementation for quick deployment
- Datasets with relatively uniform cluster sizes
- When simplicity is more important than optimal I/O

---

#### **Option 3: Adaptive Multi-Cluster with Metadata Pre-check** (Most I/O Efficient)

**Concept:** Use metadata pages to estimate cluster sizes before scanning, enabling smarter probing decisions

**Algorithm:**
```java
public List<Result> adaptiveSearch(double[] queryVector, int K) {
    // Step 1: Find all leaf clusters sorted by distance to query
    List<ClusterSearchResult> sortedClusters =
        findAllLeafClustersSorted(queryVector);

    // Step 2: Estimate records needed and probe adaptively
    PriorityQueue<Result> topK = new PriorityQueue<>(K,
        Comparator.comparingDouble(r -> -r.distance));

    int recordsCollected = 0;
    int clustersProbed = 0;

    for (ClusterSearchResult cluster : sortedClusters) {
        // Pre-check: Estimate cluster size from metadata
        int estimatedRecords = estimateClusterSize(cluster);

        // Early termination check (only if we have enough records)
        if (recordsCollected >= K) {
            double kthDistance = topK.peek().distance;

            // Use metadata to get max distance in this cluster
            double maxDistInCluster = getMaxDistanceFromMetadata(cluster);

            // Triangle inequality: Can this cluster improve results?
            double minPossibleDist = cluster.distance - maxDistInCluster;

            if (minPossibleDist > kthDistance) {
                // All remaining clusters are too far
                break;
            }
        }

        // Scan this cluster
        scanClusterAndUpdateTopK(cluster, queryVector, topK, K);
        recordsCollected = topK.size();
        clustersProbed++;

        // Log probing decision
        logProbingDecision(cluster, estimatedRecords, recordsCollected, clustersProbed);
    }

    return extractTopK(topK, K);
}

int estimateClusterSize(ClusterSearchResult cluster) {
    // Pin metadata page for this cluster (cheap operation)
    long metadataPageId = getMetadataPageId(cluster);
    ICachedPage metadataPage = bufferCache.pin(...getDiskPageId(fileId, metadataPageId));

    try {
        metadataPage.acquireReadLatch();
        IVectorClusteringMetadataFrame metadataFrame = createMetadataFrame();
        metadataFrame.setPage(metadataPage);

        // Count number of data pages referenced
        int numDataPages = metadataFrame.getTupleCount();

        // Estimate: numDataPages × average tuples per page
        // Use global statistics or default estimate (e.g., 100)
        int avgTuplesPerPage = getAverageTuplesPerPage();  // e.g., 100

        return numDataPages * avgTuplesPerPage;

    } finally {
        metadataPage.releaseReadLatch();
        bufferCache.unpin(metadataPage);
    }
}

double getMaxDistanceFromMetadata(ClusterSearchResult cluster) {
    // Pin metadata page
    long metadataPageId = getMetadataPageId(cluster);
    ICachedPage metadataPage = bufferCache.pin(...getDiskPageId(fileId, metadataPageId));

    try {
        metadataPage.acquireReadLatch();
        IVectorClusteringMetadataFrame metadataFrame = createMetadataFrame();
        metadataFrame.setPage(metadataPage);

        // Get maximum distance from last metadata entry
        // Metadata tuples sorted by max_distance ascending
        int lastIndex = metadataFrame.getTupleCount() - 1;
        return metadataFrame.getMaxDistance(lastIndex);

    } finally {
        metadataPage.releaseReadLatch();
        bufferCache.unpin(metadataPage);
    }
}

List<ClusterSearchResult> findAllLeafClustersSorted(double[] queryVector) {
    List<ClusterSearchResult> allClusters = new ArrayList<>();

    // Scan all leaf pages (including overflow chains)
    int currentPageId = navigateToLeafLevel(queryVector);
    while (currentPageId != -1) {
        IVectorClusteringLeafFrame leafFrame = pinAndLatchLeafPage(currentPageId);

        for (int i = 0; i < leafFrame.getTupleCount(); i++) {
            double[] centroid = extractCentroid(leafFrame, i);
            double distance = calculateEuclideanDistance(queryVector, centroid);

            allClusters.add(new ClusterSearchResult(
                leafFrame.getCentroidId(i),
                distance,
                currentPageId,
                i
            ));
        }

        currentPageId = getNextLeafPage(currentPageId);
    }

    // Sort by distance to query
    allClusters.sort(Comparator.comparingDouble(c -> c.distance));
    return allClusters;
}
```

**Example Execution:**

Setup (same as before):
```
Query: q = [8, 2, 0], K = 10

Leaf centroids sorted by distance:
- c11: d=2.83
- c10: d=8.25
- c12: d=11.31
- c13: d=12.96
- c14: d=14.76
```

**Step 1:** Sort all leaf clusters by distance
```java
sortedClusters = [c11(2.83), c10(8.25), c12(11.31), c13(12.96), c14(14.76)]
```

**Step 2:** Probe clusters adaptively

**Iteration 1: c11 (d=2.83)**
```java
// Pre-check: Estimate cluster size
metadataPage = pin(c11.metadataPageId)
numDataPages = 1  (metadata has 1 entry)
estimatedRecords = 1 × 100 = 100  (overestimate for small cluster)

// No early termination check (recordsCollected=0 < K=10)

// Scan cluster
Scan c11: Add 3 records
topK = [v3(3.71), v1(2.65), v2(2.11)]
recordsCollected = 3
clustersProbed = 1

Log: "Probed c11: estimated=100, actual=3, collected=3, probed=1"
```

**Iteration 2: c10 (d=8.25)**
```java
// Pre-check: Estimate cluster size
metadataPage = pin(c10.metadataPageId)
numDataPages = 3  (metadata has 3 entries for distance ranges)
estimatedRecords = 3 × 100 = 300

// No early termination check (recordsCollected=3 < K=10)

// Scan cluster
Scan c10: Add best records, update topK
topK = [v11(2.50), v7(2.35), ..., v5(0.25)]  (10 records)
recordsCollected = 10
clustersProbed = 2
kthDistance = 2.50

Log: "Probed c10: estimated=300, actual=150, collected=10, probed=2"
```

**Iteration 3: c12 (d=11.31)**
```java
// Pre-check: Estimate cluster size
metadataPage = pin(c12.metadataPageId)
numDataPages = 2
estimatedRecords = 2 × 100 = 200

// Early termination check (recordsCollected=10 ≥ K=10)
kthDistance = 2.50
maxDistInCluster = getMaxDistanceFromMetadata(c12) = 4.0
minPossibleDist = |11.31 - 4.0| = 7.31

if (7.31 > 2.50):  ✓ TRUE
    → EARLY TERMINATION
    → Skip c12, c13, c14

Log: "Skipped c12: minDist=7.31 > kthDist=2.50, saved I/O for 3 clusters"
```

**Step 3:** Return top-K
```java
Results: 10 records (9 from c10, 1 from c11)
Clusters probed: 2 (c11, c10)
Clusters skipped: 3 (c12, c13, c14)
Metadata pages accessed: 3 (c11, c10, c12 for early termination check)
Data pages accessed: Only for c11 and c10
```

**Advantages:**
- ✅ Minimal I/O cost (uses metadata for cheap size estimation)
- ✅ Early termination with metadata-based pruning
- ✅ Adapts to actual data distribution
- ✅ No need to tune parameters (works for all distributions)
- ✅ Most efficient for skewed distributions

**Disadvantages:**
- ⚠️ More complex implementation (metadata pre-checks, estimation logic)
- ⚠️ Requires accurate metadata (max_distance values must be up-to-date)
- ⚠️ Overhead of metadata page accesses (small but non-zero)
- ⚠️ Estimation errors may cause suboptimal decisions

**When to Use:**
- Production deployments with diverse workloads
- Datasets with highly skewed cluster sizes
- When I/O cost is critical (e.g., cloud storage, SSDs)
- After Option 1 or 2 proves successful

**Key Optimization:**
Cache metadata information to avoid repeated metadata page accesses:
```java
// Cache metadata stats per cluster
Map<Integer, ClusterMetadata> metadataCache = new HashMap<>();

class ClusterMetadata {
    int estimatedRecords;
    double maxDistInCluster;
    long timestamp;  // For cache invalidation
}
```

---

### Comparison of All Three Options

| Feature | Option 1: Distance-Based | Option 2: Beam Search | Option 3: Adaptive Metadata |
|---------|-------------------------|----------------------|----------------------------|
| **Complexity** | High | Low | Very High |
| **I/O Efficiency** | High | Medium | Very High |
| **Accuracy** | High | Medium | High |
| **Parameter Tuning** | Minimal (radius expansion) | Required (numProbes) | None |
| **Early Termination** | Yes (triangle inequality) | No | Yes (metadata + triangle) |
| **Metadata Usage** | Radius calculation | None | Estimation + pruning |
| **Best For** | General-purpose | Quick deployment | Production optimization |

**Recommended Implementation Path:**
1. **Start with Option 2** (Beam Search) for quick wins
2. **Evolve to Option 1** (Distance-Based) for better accuracy
3. **Optimize with Option 3** (Adaptive Metadata) for production efficiency

### Future Optimizations

1. **Parallel cluster probing:** Scan multiple clusters concurrently
2. **Adaptive probe count:** Adjust based on cluster sizes from metadata
3. **Distance-based data page pruning:** Use metadata entries to skip far data pages
4. **Inter-component deduplication:** Handle duplicate records across LSM components

## Requirements

- JDK 11 or newer
- Maven 3.3.9 or newer
- Python 3.6+ with pip and venv (for documentation generation)
- Unix-like environment (Linux, macOS)