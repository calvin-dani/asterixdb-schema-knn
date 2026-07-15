# Patch 3760 — Training VTree index

> **Status:** current
> **Verified against:** `15c888f490` on branch `integrate-newbase` (2026-07-02)
> **Scope:** what layer patch [ASTERIXDB-3760] adds, module by module, with a data-stream
> walkthrough of its five new runtime operators.

## Commit metadata

- Commit: `15c888f49006c212e8c24147ce47c425eb6ffcdd`, authored 2026-04-24 by Calvin Thomas Dani.
- 29 files, ~6,800 insertions, spanning `asterix-common`, `asterix-om`, `asterix-metadata`,
  `asterix-runtime`, `asterix-app` (registry only), and two small `hyracks-dataflow-std` helpers.
- Touches **zero** `hyracks-storage-am-*` files and **no** grammar/QueryTranslator files.

## What layer this patch is

The 3754 patch series built the storage *engine* (VTree/LSMVTree, frames, bulk loaders,
`QuantizedIndexCreateOperatorDescriptor` — all in `hyracks-storage-am-vtree` /
`hyracks-storage-am-lsm-vtree`). Patch 3760 builds the **AsterixDB-side index-construction
layer on top of it**: everything needed to go from "an empty VTree resource can exist" to
"CREATE INDEX actually trains, quantizes, and populates one from a real dataset." The DDL
surface (grammar, `QueryTranslator` handling) and the query-side optimizer (3771) live in
adjacent patches of the stack; 3760 is precisely the middle.

The result is a **three-job creation pipeline**, orchestrated by
`QueryTranslator#doCreateIndexImpl` (metadata transaction committed between jobs, index
invisible under `PENDING_ADD_OP` until all three succeed):

```
Job 1 — train quantization constants + create empty LSM files
  sample scan → VectorComponentExtractor → QuantizationAgg(local) → QuantizationAgg(global)
             → QuantizedIndexCreate                        [params persisted on resource JSON]

Job 2 — train clustering + build static navigation tree
  sample scan → assign → HierarchicalKMeans++ → VTreeStaticStructureCreator
             → .staticstructure pages (leaves at low page ids, root last/highest)

Job 3 — route every record, sort, bulk-load first disk component
  full scan → assign → VTreeBulkLoaderAndGrouping → external sort {1,0}
            → LSMIndexBulkLoad → VTreeBulkLoader (data + directory pages, static pages copied)
```

Cross-job dependencies: Job 1's `float[6]` quantization params (on the
`LSMVTreeLocalResource` JSON) are read by Job 2's static-structure creator (to quantize leaf
centroids) **and** Job 3's grouping operator (to quantize records). Job 2's static tree is
what Job 3 navigates to route each record.

## Module-by-module breakdown

### 1. Quantization math (`asterix-common`) — the numeric core

OSQ (optimized scalar quantization), SQ4/SQ8:

- `OptimizedScalarQuantizationSampleFile` — the math. `Params` record (bits, dimension,
  minQuantile, maxQuantile, alpha, confidenceInterval, sampleCount); `quantizeVector()` =
  clamp to `[minQ, maxQ]`, scale by `alpha`, round to a code in `[0, 2^bits−1]`, one byte per
  dimension; `dequantizeToDoubleArray()` = `code/alpha + minQ`; `SimilarityFunction` mapping
  from metric strings.
- `QuantizationConstants` — immutable DTO; `alpha = (2^bits − 1)/(maxQ − minQ)`.
- `ScalarVectorQuantizer` + `OptimizedScalarQuantizerFactory` — implement the Hyracks-side
  interfaces `IVTreeQuantizer` / `IVTreeQuantizerFactory`. The factory javadoc states it
  explicitly: this **replaces a `Class.forName` reflection block that used to live in
  `VTree#search`**, because the storage module cannot import AsterixDB types. It reconstructs
  a quantizer from the `float[6]` stored on the index (layout: minQ, maxQ, alpha, CI, bits,
  sampleCount).

Also: `IndexType.VTREE` added to `DatasetConfig`; nine new compile-time error codes
(1256–1264) for WITH-clause validation in `ErrorCode`.

### 2. The ADM↔Hyracks bridge (`asterix-om`)

- `AOrderedListVectorBinaryAccessor(+Factory)` implements Hyracks' `IVTreeBinaryAccessor`:
  raw bytes of an ADM ordered list `[1.0, 2.0, ...]` → `double[]`, handling homogeneous and
  heterogeneous (ANY-typed) lists, coercing double/float/int/bigint. This is what lets a
  `VTree` deep inside Hyracks read query/insert vectors without knowing ADM serialization.
- `BuiltinFunctions` gains `QUANTIZATION_CONSTANTS` (`agg-quantization-constants`) and
  k-means accumulate aggregate identifiers.

This is the same **dependency-inversion pattern repeated three times** in this patch —
storage defines the interface, AsterixDB supplies the implementation, and the implementation
is registered in `PersistedResourceRegistry` so it JSON-round-trips on the local resource
across NC restarts:

| Hyracks interface | AsterixDB implementation |
|---|---|
| `IVTreeBinaryAccessor` | `AOrderedListVectorBinaryAccessor` |
| `IVTreeQuantizerFactory` | `OptimizedScalarQuantizerFactory` |
| `IVTreeDistanceFunctionFactory` | `VectorDistanceFunctionFactory` (asterix-runtime) |

### 3. Metadata + job orchestration (`asterix-metadata`) — the brain

- `Index.VectorIndexDetails` — the metadata representation: vector key field, INCLUDE fields
  (names/types/source indicators), the whole validated `withObjectNode`, plus six nullable
  slots for trained quantization constants (`hasQuantizationConstants()`). Note: `VTREE`
  maps to `ResourceType.LSM_BTREE` as its base resource type.
- `SecondaryVectorOperationsHelper` (~970 lines, the heart of the patch) — builds all three
  job specs: `buildCreationJobSpec()` (empty files + quantization training),
  `buildStaticStructureJobSpec()` (sample → k-means → tree), `buildLoadingJobSpec()`
  (scan → route → sort → bulk load). The `buildStaticStructureJobSpec()` hook is added to
  `ISecondaryIndexOperationsHelper` and `IndexUtil`; `SampleOperationsHelper` throws
  `UnsupportedOperationException` for it. Sampling policy: `train_list_fraction` ×
  cardinality clamped to [10,000, 1,000,000]; below 10,000 → full scan instead of sample scan.
- `VTreeResourceFactoryProvider` — translates the metadata `Index` into the physical
  `LSMVTreeLocalResourceFactory`. This is where the on-disk data-tuple schema is *decided*:
  type traits and comparators for
  `[distance (raw 8B double), centroidId (raw 4B int), qDist, qEmbed, pk..., includes...]` —
  hot fields are **raw untagged binary** (`FixedLengthTypeTrait`), not ADM-tagged values. It
  also threads the distance metric string, `VectorDistanceFunctionFactory`, the vector
  accessor factory, and the `CrossPollinationConfig` onto the resource. An in-code comment
  documents a real invariant: the cross-pollination parameter reads here (DML path) must stay
  **in lock-step** with the same reads in `buildLoadingJobSpec` (bulk-load path), or deletes
  miss replicas (leaked deletes).

### 4. Runtime operators (`asterix-runtime`) — the muscle

Five new operators (walkthrough below), plus `KMeansUtils` (ADM-list → `double[]` coercion)
and `VectorDistanceFunctionFactory` (JSON-serializable metric-string → distance-function
factory, injected into storage; supports euclidean/l2, euclidean_squared/l2_squared, cosine,
dot — dot negated for MIPS convention).

| Operator | Job | Role |
|---|---|---|
| `VectorComponentExtractorOperatorDescriptor` | 1 | Flatten each vector into one tuple per scalar component |
| `QuantizationConstantsAggregateDescriptor` | 1 | Local/global aggregate: quantiles + alpha → `float[6]` |
| `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor` | 2 | Two-activity k-means‖ + Lloyd's; bottom-up hierarchy emission |
| `VTreeStaticStructureCreatorOperatorDescriptor` | 2 | Consume centroid stream, drive `VTreeStaticStructureBuilder` |
| `VTreeBulkLoaderAndGroupingOperatorDescriptor` | 3 | Route each record to leaf centroid(s), compute distance, quantize |

### 5. Hyracks plumbing (`hyracks-dataflow-std`) — two tiny enablers

`MaterializerTaskState` gains `createReader()` and a constructor keyed by the new
`PartitionedUUID` `(UUID, partition)`. This exists solely so the k-means operator's second
activity can **re-read the materialized training sample multiple times** (k-means‖ needs
several passes; a normal dataflow stream is single-pass) and find the right state object per
partition.

## Data-stream walkthrough

![Three-job pipelines with the five new operators and per-edge tuple formats](../_assets/3760-three-job-datastream.svg)

Toy dataset (4-dim embeddings, `WITH {"dimension": 4, "similarity": "euclidean",
"quantization": "SQ8"}`). Numbers are illustrative but computed with the real formulas; in
reality this dataset is far below the 10,000-sample clamp, so both "sample" scans would fall
back to full scans.

| pk | year | embedding |
|---|---|---|
| 1 | 1999 | [0.10, 0.20, 0.90, 0.80] |
| 2 | 2005 | [0.12, 0.18, 0.85, 0.75] |
| 3 | 2011 | [0.90, 0.85, 0.10, 0.05] |
| 4 | 2018 | [0.88, 0.90, 0.15, 0.10] |
| 5 | 2001 | [0.50, 0.45, 0.55, 0.50] |
| 6 | 2020 | [0.05, 0.15, 0.95, 0.85] |

### Job 1 — the scalar stream

**`VectorComponentExtractor`** destroys record structure entirely: pk=1 becomes four
single-field tuples `(0.10) (0.20) (0.90) (0.80)`. The dataset becomes a stream of 24
anonymous scalars; null/missing embeddings and non-numeric items are silently skipped.

**`QuantizationConstantsAggregate` (local step)**, per partition, hoards values and emits one
BINARY blob laid out `[count=24][24 raw doubles]`. It ships raw values instead of partial
results because **quantiles don't compose** — you can't merge two medians.

**`QuantizationConstantsAggregate` (global step)** detects the BINARY tag, merges all
partitions' blobs, sorts, and takes confidence-interval quantiles: with CI = 0.99 over 24
values, `minQ = 0.05`, `maxQ = 0.95`, `alpha = (2^8−1)/(0.95−0.05) ≈ 283.33`. One tuple
leaves: `float[6] = [0.05, 0.95, 283.33, 0.99, 8, 24]`, which
`QuantizedIndexCreateOperatorDescriptor` (from 3754 p2) writes into the
`LSMVTreeLocalResource` JSON next to `"euclidean"` and the serialized factories. Nothing about
individual vectors survives this job — only six floats.

### Job 2 — the centroid stream

Assign reshapes each record to `[embedding, year, pk]`. K-means **activity 1** writes all
tuples to a run file keyed by `PartitionedUUID` (this is why `MaterializerTaskState` was
patched). **Activity 2** re-reads it across k-means‖ seeding rounds (5 rounds of
probabilistic oversampling at ~2k per round) and Lloyd's refinement (convergence 1e-4;
centroids L2-normalized when metric is cosine). Say it converges on 3 leaf centroids, then
reclusters those into 1 root (`maxLevel = 1`):

- `cA ≈ [0.09, 0.18, 0.90, 0.80]` ← pks 1, 2, 6
- `cB ≈ [0.89, 0.88, 0.13, 0.08]` ← pks 3, 4
- `cC = [0.50, 0.45, 0.55, 0.50]` ← pk 5
- root `r ≈ [0.49, 0.50, 0.53, 0.46]`

`outputHierarchicalStructure()` emits **leaves first but with BFS-from-root centroid IDs**,
via a precomputed per-level offset table (root level starts at 0, leaf level at root-size):

```
(treeLevel=1, cid=1, parent=0,  cA)   ← emitted 1st
(treeLevel=1, cid=2, parent=0,  cB)
(treeLevel=1, cid=3, parent=0,  cC)
(treeLevel=0, cid=0, parent=-1, r)    ← root emitted LAST
```

**`VTreeStaticStructureCreator`** consumes this in order, so the storage builder can be
append-only: page 1 is the leaf page holding `<cid=1, cA, quantize(cA), metaPtr=-1>` … (leaf
centroids are quantized with Job-1 params read off the resource), and the root interior page
is written last at the highest page id with `<cid=0, r, childPage=1>`. The `-1` metadata
pointers are placeholders for Job 3.

### Job 3 — the routed stream

Same scan+assign shape. **`VTreeBulkLoaderAndGrouping`** does four things per tuple; for pk=1:

1. **Quantize** with Job-1 params: `round((v − 0.05) × 283.33)` per dimension →
   `qv = [14, 43, 241, 213]` (one byte each).
2. **Route** via `findCloseCentroidsLevelWiseGlobalSort` on the Job-2 tree with the DDL
   epsilon: distances to leaves are cA = 0.022, cC = 0.62, cB = 1.31 → candidates headed by
   `cid=1`.
3. **Thin** with `RngAcceptanceFilter`: at `cross_pollination_m = 1` only the closest
   survives; at M = 3 the record is emitted up to 3 times, once per accepted diverse cluster
   (SPTAG-style RNG rule thins near-duplicate clusters).
4. **Quantized distance**: dequantize `qv` and `quantize(cA)`, apply euclidean between the
   approximations → ≈ 0.021.

Output tuple: `[0.0224, 1, 0.021, bytes[14,43,241,213], "1", 1999]`. Full stream (scan order):

| distance (f0) | cid (f1) | pk | year |
|---|---|---|---|
| 0.022 | 1 | 1 | 1999 |
| 0.077 | 1 | 2 | 2005 |
| 0.044 | 2 | 3 | 2011 |
| 0.044 | 2 | 4 | 2018 |
| 0.000 | 3 | 5 | 2001 |
| 0.087 | 1 | 6 | 2020 |

**External sort on `{1, 0}`** (int comparator on cid, double on distance — both raw untagged
binary; the operator asserts cid sits at field 1) produces contiguous cluster runs:
`(1: pk1, pk2, pk6) (2: pk3, pk4) (3: pk5)`.

**`LSMIndexBulkLoad` → `VTreeBulkLoader`** (storage layer, 3754) consumes the sorted run
cluster-at-a-time: reads cid from field 1; on a cid flip it finalizes cluster
`cid − firstLeafCentroidId` — writes its data pages (tuples insert-sorted by distance),
emits directory entries `<maxDist, dataPageId>`, chains and flushes directory pages — then
starts the next cluster. In `end()`, the Job-2 static pages are copied to the tail of the
component with child/next-leaf pointers offset by the new base page id, and each leaf tuple's
`metaPtr=-1` is patched to its cluster's first directory page. The first disk component is
complete.

## Design theses

1. **Training is a query job, not a storage feature.** K-means, quantile estimation, and
   cluster routing are ordinary Hyracks operators composed by a metadata-layer helper — they
   parallelize across NCs, spill via sort/materialization, and reuse the sample-index
   machinery, instead of being buried in the storage engine.
2. **The storage engine stays ADM-ignorant via injected interfaces**, all persisted through
   `PersistedResourceRegistry` so a restarted node reconstructs identical behavior. This
   patch is where that inversion replaced reflection.
3. **Hot-path fields are schema-less raw binary.** Distance and centroidId are untagged
   8/4-byte values with raw comparators — decided once in `VTreeResourceFactoryProvider`,
   relied on by the sort keys `{1,0}` and the bulk loader downstream.
4. **One config source, two consumers.** The WITH-clause `AdmObjectNode` on
   `VectorIndexDetails` is the single source of truth read by both the bulk-load job builder
   and the DML resource factory — drift between the two produces leaked deletes.
5. **Each job compresses the dataset into the next job's configuration.** Job 1 reduces all
   vectors to six floats; Job 2 reduces them to a page-resident centroid tree; only Job 3's
   output scales with the data, and by then each record needs one cheap tree descent to know
   where it lives.

## Known bugs in this patch

- **OPEN: hierarchical k-means drops the K trained leaf centroids whenever K ≥ 4** — the
  emitted tree's leaf level is the first parent level (~√K clusters), so `num_clusters` is
  not honored. Found 2026-07-03 by the layer-2 unit tests. Details and fix sketch:
  [bug-archive.md](../60-quality/bug-archive.md).
- **FIXED 2026-07-03: materialized training sample leaked run files on every build** — same
  archive entry.

## Caveats

- This document describes the commit **as it sits on `integrate-newbase`** — the rebased
  version. The original Gerrit change went through many patchsets; an external amendment
  bundle was absorbed into ps20, and the commit was then ported onto the reorganized
  `double[]`-only vector-distance API. Some in-code comments (the lock-step warning, the
  reflection-replacement javadoc) reflect fixes that landed after the original patch.
- `QuantizedIndexCreateOperatorDescriptor` is used by Job 1 but is **not** part of this
  commit — it lives in `hyracks-storage-am-lsm-vtree` from 3754 p2.
- The toy numbers in the walkthrough are illustrative; real defaults:
  `K = sqrt(cardinality / numPartitions)` unless `num_clusters` is set, quantization defaults
  to SQ8, `train_list_fraction` defaults to 0.1.
