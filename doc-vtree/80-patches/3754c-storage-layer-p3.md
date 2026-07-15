# Patch 3754 p3 — Storage layer of VTree index, patch 3

> **Status:** current
> **Verified against:** `8d911ed996` on branch `integrate-newbase` (2026-07-02)
> **Scope:** what patch 3 of [ASTERIXDB-3754] adds — the `hyracks-storage-am-lsm-vtree-test`
> unit-test module and shared test-support classes — including what the tests reveal about
> intended semantics.

## Commit metadata

- Commit: `8d911ed9961d22d64d536b59fc071f6c87c4090` (short `8d911ed996`), authored 2026-05-17
  by Le0shy.
- 26 files, ~5,930 insertions. No production code: one new test module
  (`hyracks-tests/hyracks-storage-am-lsm-vtree-test`) plus five shared classes in
  `hyracks-test-support` (`org.apache.hyracks.storage.am.vector.*`).

## What layer this patch is

The proof harness for patches 1–2. Because 3760's AsterixDB pipeline didn't exist yet when
the storage layer was written, this module is what exercises the storage APIs end-to-end —
**playing the role of the k-means job and the bulk-load job by hand**, with deterministic
geometric data instead of trained centroids. It's also the fastest place to reproduce
storage-level bugs without booting a cluster (cf. the integration tests, which run the full
engine).

## Infrastructure: `LSMVTreeTestHarness`

Stands up a miniature NC: disk buffer cache (512-byte pages, ~10K pages), a list of virtual
buffer caches (default 4 mutable components, ~50 tiny pages each — deliberately small so
overflow paths trigger fast), IO manager on a timestamped test directory,
**synchronous** IO scheduler, `ThreadCountingTracker`, **`NoMergePolicy`** (merges happen only
when a test schedules them), counting IO callbacks, append-only linked metadata page manager,
and `RANDOM_SEED = 50` for reproducibility. `LSMVTreeTestContext` then builds the real index
via `LSMVTreeUtils.createLSMTree()` — metric "euclidean", bloom filter off, with overloads for
custom data-tuple creators (quantized tests), include fields, and the neighbor-capable
quantized leaf frame.

Test-support bridges substitute for AsterixDB's injections, same interfaces, trivial
implementations: `TestDoubleArrayVectorAccessor` (reads a raw `double[]` field — no ADM) and
`TestVTreeDistanceFunctionFactory`.

## Test data: `VectorTestStructure` (~790 lines)

The keystone utility — a *declarative* index shape plus generators, decoupled from test
operations:

- **Canonical hierarchies:** `threeDim3Level()` (1×2 root / 2×4 interior / 8×3 leaves = 34
  centroids on octant geometry), `twoDim2Level()` (4 quadrant centroids over 16 leaves),
  `threeDim1Centroid()` (single leaf at the origin, for pruning tests with a linear record
  layout).
- **Bulk-load records** in three formats — `NAIVE` `<distance, cid, pk>`,
  `NAIVE_WITH_INCLUDES` (+ include fields), `QUANTIZED`
  `<distance, cid, qDist, qEmbed, pk>` — generated as **concentric rings** around each leaf
  centroid (base 0.2, step 0.2, ±axis directions), with the stored distance computed from the
  actual generated vector so float rounding can't desynchronize data from assertions.
- **Insert records** `<vector, (includes), pk>` — note: *no distance, no centroid id*; those
  are the index's job. Deterministic PK schemes (`pk_c_10_0`, `pk_ins_c10_0`, `mt_t0_0`)
  encode provenance so assertions can tell disk-loaded from inserted records by prefix.
- **Include-field values** come from a pure function of `(centroidId, recordIndex)`
  (`(cid·31 + i·17) mod 10`), so they're checkable without side tables.
- **Centroid tuples** are emitted bottom-up (leaf level first) — the same contract 3760's
  k-means honors — and `buildCentroidTuplesWithLeafNeighbors()` encodes provisional neighbor
  entries `[centroidId, SENTINEL]` for the graph-leaf-neighbors feature.

`VectorTreeTestUtils` (hyracks-test-support) provides the verbs: `buildStaticStructure`,
`bulkLoadRecords`, `insertRecordsIntoMemoryComponent`, `deleteRecordsFromIndex`,
`topKSearch` / `naiveBlockedSearch` / `verifyRecordsWithSearch`, and expected-set checking
via `VectorCheckTuple`.

## The test matrix

| Test | Scenario | Key assertion |
|---|---|---|
| `LSMVTreeBuildTest` | create → build static → bulk load → scan closest cluster | structure is navigable end-to-end |
| `LSMVTreeBulkLoadTest` | bulk load into first disk component | every record lands in the geometrically correct leaf cluster (exact PK sets) |
| `LSMVTreeInsertTest` | bulk load (disk) + inserts (memory), search both | results mix `pk_c_*` and `pk_ins_*`; counts exact |
| `LSMVTreeInsertIncludeTest` | same, with include fields | include values match the deterministic formula |
| `LSMVTreeDeleteTest` | delete 5 known PKs near c10 via antimatter | deleted PKs absent, all others present |
| `LSMVTreeDeleteIncludeTest` | delete with include fields | absence + surviving includes intact |
| `LSMVTreeMergeTest` | bulk load → insert → flush → `scheduleMerge` (sync) | exactly 1 component after merge; union of PKs preserved |
| `LSMVTreeLeafNeighborTest` | neighbor lists through build + bulk load | provisional `[cid, SENTINEL]` entries resolved to real `[pageId, slot]`; neighbor-less leaves have well-formed empty lists |
| `quantized/LSMVTreeInsert-/DeleteQuantizedTest` | same lifecycles with quantized tuple creator | exact PK matching still holds under quantization |
| `quantized/LSMVTreeQuantizedSearchTest` + `QuantizedSearchTestDriver` | pruning-oriented query cases (`QueryCase(queryVector, K, expectedPKs, excludedPKs)`) on 1-centroid-3D and 2-level-2D structures | pre-computed top-K expected/excluded sets |
| `multithread/LSMVTreeMultiThreadTest` | all threads hammer one leaf cluster (c10): 1×400 single-thread stress; N concurrent inserters; 4·cores high contention; inserts+searches mixed | every inserted PK retrievable, cid == 10 for all, no worker exceptions, `index.validate()` passes; concurrent searches never see fewer than the bulk-load floor |
| `RngAcceptanceFilterTest` | unit test of the SPTAG acceptance rule | veto condition `rngFactor·d(c_i, r) < d(x, c_i)` |

Two design points about the assertions: correctness is **exact-set PK comparison, never
recall** — at this layer, with hand-placed geometry and full scans (large K), the right answer
is knowable, so approximation is not tolerated; and quantized tests assert the *same* exact PK
sets, i.e. quantization may distort distances but must not lose or invent records.

## Real APIs, not shortcuts

The lifecycle each test drives is the production one: `VTreeStaticStructureBuilder` fed
bottom-up centroid tuples (standing in for Job 2), `VTreeBulkLoader` fed
cluster-sorted records (standing in for Job 3), `IIndexAccessor.insert/delete` for DML
(distance and centroid computed by the index, exactly as in the engine),
`VTreeSearchPredicate` + cursors for reads, and `ILSMIndexAccessor.scheduleMerge(...).sync()`
for compaction. The only test-only substitutions are the two injected bridges (raw-`double[]`
accessor and distance factory) — which is itself a demonstration that patch 1's dependency
inversion works.

## Semantics the tests pin down (not obvious from main code)

1. **PK is identity.** Deletes match by PK; the vector only routes to the right clusters.
2. **Distance is never client-supplied on DML.** Insert tuples carry `<vector, pk>` only —
   whatever distance/centroid the index computes is authoritative.
3. **Delete = antimatter insert, reconciled lazily.** Post-delete searches must not see the
   PK even *before* any merge (read-time cancellation), and the merge test confirms physical
   removal after compaction.
4. **Include fields are opaque payload** — stored, returned, never indexed or filtered at
   this layer.
5. **Neighbor lists are two-phase**: provisional by centroid-id at build time, physical
   `(pageId, slot)` after bulk load — and the empty-list case is a first-class contract.
6. **Deliberately tiny pages (512 B)** make page overflow/chaining the common case rather
   than the rare one, so overflow bugs surface in unit tests.
7. **Gap in coverage worth knowing:** all canonical structures use contiguous centroid ids —
   sparse/skipped-id layouts (a past bug class in the bulk loader) are not exercised here.

## Design theses

1. **Declarative geometry beats random data.** Centroids on axes/quadrants and records on
   concentric rings make every expected result derivable by inspection — failures point at
   the code, not the fixture.
2. **The test module doubles as the storage layer's reference client.** Until 3760 existed,
   this was the executable specification of the builder/loader input contracts (bottom-up
   emission, cluster-sorted records) that the real pipeline later had to honor.
3. **Contention is aimed, not sprayed.** The multithread suite targets one cluster to
   maximize page-level interleaving where the frames are mutable — the place races actually
   live.

## Caveats

- Rebased commit: the neighbor-list tests and quantized drivers reflect features
  (graph-leaf-neighbors, quantized-only formats) that post-date the original patchset.
- `RngAcceptanceFilterTest` currently also exists as a stray untracked copy at the repo-root
  level of the module (see `git status`) — the tracked one under the test module is
  authoritative.
- These are single-process unit tests; cluster-level behavior (partitioning, recovery, SQL++)
  is covered by `integration/tests/run_tests.py` instead. Per project experience, dropping
  "dead" fields based on unit tests alone is unsafe — framework-level readers only show up in
  integration runs.
