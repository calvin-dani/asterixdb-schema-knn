# SPANN in AsterixDB: Current Algorithm Report

## Scope

This report describes the current SPANN-oriented VTREE implementation in this branch, including:

- Build-time top-down tree creation (`SelectHead` + `BuildHead`)
- Static-structure materialization and data loading
- Query-time ANN top-k execution path
- Relevant configuration knobs and operational assumptions

## Core Components

### Build orchestration

- `asterixdb/asterix-metadata/src/main/java/org/apache/asterix/metadata/utils/SecondaryVectorOperationsHelper.java`
  - Creates job specs for:
    - index creation
    - static-structure build
    - data loading
  - Resolves top-down and SelectHead tuning via `resolveTopDownTuning()` and `resolveSelectHeadTuning()`.

### Top-down hierarchical clustering and SPANN head selection

- `asterixdb/asterix-runtime/src/main/java/org/apache/asterix/runtime/operators/HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.java`
  - Main phases:
    - `runSelectHeadPhase(...)`
    - `materializeHeadRunFile(...)`
    - `buildTopDownHierarchicalKMeans(...)`
    - `outputBottomUpForStaticStructure(...)`
  - Internal representation:
    - `HierarchicalClusterStructure`
    - `CentroidInfo`
    - level map where level `0` is root in top-down flow.

### Static structure consumer and builder handoff

- `asterixdb/asterix-runtime/src/main/java/org/apache/asterix/runtime/operators/VTreeStaticStructureCreatorOperatorDescriptor.java`
  - Consumes hierarchical tuples:
    - `[treeLevel, centroidId, parentClusterId, embedding]`
  - Infers structure info (`clustersPerLevel`, `centroidsPerCluster`, `numLevels`)
  - Converts tuple format for builder and initiates static bulk load.

### Storage-side static builder and search

- `hyracks-fullstack/hyracks/hyracks-storage-am-vtree/.../VTreeStaticStructureBuilder.java`
- `hyracks-fullstack/hyracks/hyracks-storage-am-lsm-vtree/.../LSMVTreeTopKSearchCursor.java`
- `hyracks-fullstack/hyracks/hyracks-storage-am-lsm-vtree/.../NprobeClusterSelectionStrategy.java`

These classes materialize and query the VTREE structure that SPANN build emits.

## Build-Time Algorithm (Current)

## 1) Static-structure build pipeline

`SecondaryVectorOperationsHelper.buildLoadingJobSpecForStructureCreation()` wires:

1. Primary/sample scan and assign
2. `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor` (cluster structure producer)
3. `VTreeStaticStructureCreatorOperatorDescriptor` (structure sink/builder handoff)

## 2) SelectHead + BuildHead behavior

Within `FindCandidatesActivity.initialize()`:

- Reads materialized sample and total sample count.
- If SelectHead enabled:
  - executes `runSelectHeadPhase(...)`,
  - materializes selected heads into a compact run file,
  - builds structure from head-only source with `buildTopDownHierarchicalKMeans(..., headOnlyBuild=true, ...)`.
- Else if top-down enabled:
  - runs full-sample top-down build.
- Else:
  - runs legacy bottom-up path.

## 3) Top-down split logic

`buildTopDownHierarchicalKMeans(...)` performs level-wise splitting:

- initializes root-level clusters from streamed input
- repeatedly splits batches using dynamic `k` decisions
- stops based on:
  - leaf-page capacity constraints
  - head-only stop conditions
  - configured `maxLevel`
  - empty/degenerate split outcomes

## 4) Mode-height cutoff (current branch behavior)

Before emitting top-down tuples to the next operator:

- branch leaf-depth histogram is computed from `HierarchicalClusterStructure`
- modal leaf depth is selected (ties prefer deeper depth)
- levels deeper than the modal depth are pruned
- remaining levels are emitted leaf-first via `outputBottomUpForStaticStructure(...)`

This cutoff is intentionally applied at handoff time to minimize impact on clustering internals and preserve downstream tuple contracts.

## 5) Static-structure tuple contract

Emitted tuple format from hierarchical operator:

- field 0: `treeLevel`
- field 1: `centroidId`
- field 2: `parentClusterId`
- field 3: `embedding`

`VTreeStaticStructureCreatorOperatorDescriptor` depends on this format to:

- compute per-level distributions
- detect leaf level
- create builder tuples for interior vs leaf nodes

## Data Loading

`SecondaryVectorOperationsHelper.buildLoadingJobSpec()` continues after static structure creation:

- assigns data tuples to nearest centroids (`VTreeBulkLoaderAndGroupingOperatorDescriptor`)
- sorts by centroid and distance
- bulk-loads VTREE data component

This acts as the posting/data placement phase for vector search.

## Query-Time Algorithm (Current)

## 1) Optimizer rewrite

- `IntroduceTopKAccessMethodRule` and `VectorIndexAccessMethod`
  - detect ANN order-by + limit patterns
  - choose vector index access plan

## 2) Physical/runtime lowering

- `VectorSearchPOperator` and `MetadataProvider.getVectorSearchRuntime()`
  - create runtime descriptor
  - pass query vector, `k`, metric, probing knobs (`min_probe_fraction`, `k_multiplier`, search approach)

## 3) Cursor execution

- `VectorSearchOperatorNodePushable` builds predicate/accessor params
- `LSMVTreeTopKSearchCursor`:
  - initializes nprobe cluster traversal strategy
  - synchronizes cluster advancement across components
  - accumulates top-k candidates (spillable buffer)
  - reconciles include/deletes and returns ranked results

## Configuration Knobs

Primary top-down/SPANN knobs flow from compiler/session properties and WITH options:

- top-down toggle (`top_down`)
- top-down lambda factor
- top-down max level
- SelectHead enable/ratio/count/type
- quantization mode and bits

User-visible parameters are documented in:

- `asterixdb/asterix-doc/src/main/markdown/sqlpp/appendix_2_parameters.md`
- `docs/top-down-hierarchical-kmeans.md`

## Operational Assumptions and Edge Cases

- Empty sample/head output in SelectHead path logs warnings and can lead to zero emitted tuples.
- Static-structure creator has best-effort metadata reads and defaults quantization parameters when unavailable.
- Correct parent-child consistency relies on stable level-local parent index mapping between adjacent levels.
- Cutoff-at-mode retains all levels `<= modalDepth`; deeper levels are dropped at handoff only.

## Summary

Current SPANN behavior is implemented as a top-down VTREE structure build with optional SelectHead preselection and a pre-handoff mode-depth truncation step. The runtime ANN path remains the existing vector index rewrite plus LSM VTREE top-k cursor traversal. The cleanup in this branch focuses on readability and comments without changing algorithmic semantics beyond the explicit modal depth cutoff behavior.
