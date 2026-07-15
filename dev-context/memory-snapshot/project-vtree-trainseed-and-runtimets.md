---
name: project-vtree-trainseed-and-runtimets
description: compiler.vector.trainseed knob + vector/create-index-vtree runtimets case (2026-07-03); k-means materializer run-file leak found & fixed
metadata: 
  node_type: memory
  type: project
  originSessionId: f0dbccc0-9888-49c9-9fe5-a679580772e1
---

Added 2026-07-03 (integrate-newbase, on top of 3760/3771):

- **`compiler.vector.trainseed`** compiler property (SET-able per request) seeds the
  k-means training RNG: `new Random(trainSeed * 31 + partition)` in
  `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor` (was unseeded — the ONLY
  randomness in the whole creation pipeline; "RNG" in RngAcceptanceFilter is
  relative-neighborhood-graph, deterministic). Read in
  `SecondaryVectorOperationsHelper#buildStaticStructureJobSpec`; defaults to nanoTime.
- **runtimets case `vector/create-index-vtree`** (registered in VectorQueries.xml, runs in
  CI): seeded ANALYZE (sample-seed 1000) → seeded CREATE INDEX TYPE VTREE → metadata golden
  → exact top-k golden (min_probe_fraction 1.0 makes it clustering-independent) → compile-only
  `optimized-logical-plan` regex golden proving the index rewrite fired. NOTE: plain
  `logical-plan` is PRE-optimization (no rewrite visible); compile-only CREATE INDEX
  short-circuits at QueryTranslator ~line 1429 before job building.
- **Bug found & fixed by the test:** k-means materialized-sample run file was never deleted
  (read via `MaterializerTaskState.createReader()`, bypassing writeOut's last-consumer
  delete) — every vector index build leaked run files (46 in one tiny test). Fix: new
  `MaterializerTaskState.deleteFile()` + call in the FindCandidates finally block.
- **Layer-2 JUnit tests added (2026-07-03), 17/17 green** in asterix-runtime/src/test:
  VectorComponentExtractor (5), QuantizationConstantsAggregate (8), HierarchicalKMeans (4 —
  full two-activity flow with mocked state-object map + real IOManager over TemporaryFolder;
  seed-42-twice byte-identical determinism).
- **OPEN BUG found by the k-means unit test:** whenever a hierarchy is built (K>=4), the K
  finest-level centroids are stored at levelCentroids[0], then OVERWRITTEN by
  initializeParentLevel(0) and re-keyed to -1 by buildLevelFromAssignments(child=-1);
  outputHierarchicalStructure iterates 0..maxLevel so key -1 never emits → index gets
  ~sqrt(K) leaf clusters, not num_clusters. K<=3 unaffected. Verified: K=36/80 vecs → 8
  tuples emitted. Also minor: resetRunFileReader leaks reader handles per pass; mixed
  local+global aggregate state silently prefers local branch.
- Gerrit placement TBD: leak fix + seed plumbing belong as 3760 amendments (see
  [[project-gerrit-stack-handoff-crosspollination]]).
- **Two real-data cases added (2026-07-03), both green:** `vector/create-index-vtree-movie`
  (100-rec subset of movie_filtered_indexed, 384-dim, l2_squared) and
  `vector/create-index-vtree-glove` (150-rec subset of glove-100-angular, 100-dim, cosine).
  Subsets live in `asterixdb/asterix-app/data/vector/*.adm`. Goldens brute-forced in Python
  with the exact VectorDistanceCalculation formulas. Key technique: SELECT a non-PK field
  (forces lookup-and-rerank → exact ordering, not quantized dqx) + kmultiplier 30 +
  min_probe_fraction 1.0 → candidate set covers the subset → golden is exact by construction.
- To run one runtimets case: add it to `runtimets/only_sqlpp.xml`, `mvn test
  -Dtest=SqlppExecutionTest` in asterix-app (restore only_sqlpp.xml after). Surefire needs
  `mvn install` of edited upstream modules first (see [[feedback-surefire-needs-install]]).
