---
name: vtree-3760-test-plan
description: Deferred unit-test plan for the VTree 3760 review round; Milestone B validated via existing CI regression net first
metadata: 
  node_type: memory
  type: project
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

For the VTree **3760 (ASTERIXDB-3760, Gerrit 21159)** review round, decision (2026-07-29): validate the
Milestone-B code changes via the **existing CI/CD regression net first**, and defer adding new tests.

**Regression net already covering the changes:** `hyracks-storage-am-lsm-vtree-test` (incl. `quantized/`
subpackage) covers :319/:569/:702/:593 in storage; `SqlppExecutionTest` vector goldens
(`asterix-app/.../runtimets/queries_sqlpp/vector/`, negative-DDL pattern = `create-index-vtree-invalid-field-type`)
covers DDL knobs :224/:386/:388/:582; external glove/movie integration = end-to-end recall (:319/:553);
`InstallerFootprintIT` + `mvn formatter:validate`.

**New tests to add LATER (none exist today for these classes):**
- P0 characterization (test existing behavior → prove :319/:553 refactors are behavior-preserving):
  `OptimizedScalarQuantizationSampleFileTest` (round-trip byte/short/int, clamp, 32-bit overflow guard),
  `VectorDistanceFunctionFactoryTest` (per-metric + equivalence to current DISTANCE_MAP + Serializable).
- P0 regression (fails pre-fix): extend `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptorTest` for the
  :1517 low-cardinality padding path (padded centroids from correct absolute positions).
- P1: `VectorIndexDeclUtilTest` (:224/:582 rejected, :388 epsilon 0.25, :386 dimension required);
  `Index.resourceType()`→LSM_VTREE + `IndexTupleTranslator` round-trip (:310/:485).
- P2: :569 throw-on-unreadable-params (resource mock); :593 page-count (extend `LSMVTreeBuildTest`).

**Milestone-B verification (2026-07-29, all local tiers GREEN):** compile + `formatter:validate` pass;
Tier-1 storage vtree suite 26/26; Tier-2 HierarchicalKMeans unit 5/5; a throwaway
VectorDistanceFunctionFactory equivalence test passed before AND after the :553 dedup; **Tier-3 vector
goldens (`SqlppExecutionTest` vector group) 8/8**. Tier-4 external integration is user-driven (remote
cluster + SSH tunnel), not run.

**CI-scope correction:** the vector goldens are Tier 3 and run **in-JVM LOCALLY** (no external CI to execute
them). NB the first local run crashed at surefire fork startup (exit 11; the x86 `fake-gcs-server` ELF can't
run on this arm64 Mac, but that's non-fatal) — the fix was following the recipe exactly: `mvn -pl asterix-app
-am install -DskipTests` FIRST (not just compile), then run; do NOT use `-o` off a compile-only build.
When these gate a change it's the **ASF Gerrit CV → ASF Jenkins (asterix-jenkins.ics.uci.edu)** for the
`asterixdb` repo — **NOT** `analytics.jenkins.couchbase.com` (that's the cbas/columnar scope; the cbas gate
MB-72975 builds there and pulls asterixdb via the manifest bridge, it does not run these goldens).

Full detail lives in `asterixdb/.vtree/gerrit-notes/design-followups.md`. See [[vtree-ci-verification-workflow]],
[[vtree-infra-access-cheatsheet]], [[clusterby-branch-model]].
