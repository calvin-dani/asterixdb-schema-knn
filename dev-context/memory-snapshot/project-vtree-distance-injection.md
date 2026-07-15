---
name: project-vtree-distance-injection
description: "Storage layer made distance-math-free — VectorUtils deleted, canonical distance factory injected + persisted (full dependency inversion)"
metadata: 
  node_type: memory
  type: project
  originSessionId: f918823a-9e3b-4f5f-a2b3-6f30a6442e6e
---

**Done 2026-07-01 on `gerrit/storage-wrap-up-integrate`.** Removed the storage layer's duplicated distance math so there is ONE distance implementation in the codebase — asterix-runtime's `VectorDistanceArrCalculation`/`VectorDistanceFunctionFactory` (the 3676 vector-distance patch). Storage `hyracks-storage-am-vtree/.../utils/VectorUtils.java` is **DELETED**.

**Design (full injection, "Option A"):** `IVTreeDistanceFunctionFactory` now `extends IJsonSerializable`. The asterix `VectorDistanceFunctionFactory` is created in `VTreeResourceFactoryProvider` (the [PENDING] amendment / Calvin's 3760) and threaded — same rails as `distanceMetric`/`CrossPollinationConfig` — through `LSMVTreeLocalResourceFactory → LSMVTreeLocalResource → LSMVTreeUtils.createLSMTree → VTreeFactory/LSMVTree → VTree`. It is **persisted on the local resource** (JSON key `distanceFunctionFactory`, registered in asterix-app `PersistedResourceRegistry`) exactly like `vectorAccessorFactory`, so a restarted index reconstructs the same metric impl in `createInstance` (throws if the key is missing, mirroring the accessor-factory invariant). VTree builds `this.distanceFunction = factory.createDistanceFunction(metric)` at construction (write/clustering path); search uses the IAP factory if present else `tree.distanceFunctionFactory`.

**Tests:** hyracks tests can't depend on asterix, so a minimal `TestVTreeDistanceFunctionFactory` fixture lives in `hyracks-test-support` (euclidean/euclidean_squared/cosine/dot). Wired into `LSMVTreeTestContext` (2 createLSMTree sites) + `VectorTreeTestUtils`. Product ships one impl; tests carry a fixture as tests always do.

**Regression caught + fixed:** the MERGE full-scan predicate carries a **null** distance metric (merge doesn't rank). Old `VectorUtils.forMetric(null)` defaulted to euclidean; the asterix factory does `null.toLowerCase()` → NPE, so every merge failed (data still correct → tests falsely passed; unit `LSMVTreeMergeTest` passed only because the test fixture null-guards). Fix in `VTree.search`: `if (distanceMetric == null) distanceMetric = tree.distanceMetric` (index's own metric, never null) before calling the factory. **Any factory contract must tolerate/guard null, or callers must default first.**

**Also:** `VTreeStaticStructureCreatorOperatorDescriptor` (asterix-runtime) stopped reaching down into storage `VectorUtils.calculateEuclideanDistance` — now uses its own `VectorDistanceArrCalculation.euclidean`.

**Verified:** hyracks-fullstack + asterix-app builds green; 32/32 vtree unit tests; integration Parts 3+4 all pass with 0 merge NPEs (Part 4 restart validates the JSON round-trip of the persisted factory). Files touched span storage patch 1 (`VTree`, `IVTreeDistanceFunctionFactory`, delete VectorUtils), patch 2 (`VTreeFactory`/`LSMVTreeUtils`/`LSMVTree`/`LSMVTreeLocalResource`/`LSMVTreeLocalResourceFactory`), Calvin's amendment (`VTreeResourceFactoryProvider`), 3771/3760 (`VectorDistanceFunctionFactory`, `PersistedResourceRegistry`, `VTreeStaticStructureCreatorOperatorDescriptor`), tests. NOTE: on-disk resource JSON format changed (new key) — pre-existing indexes fail on restart, fine for dev. See [[project-vtree-quantized-only]].
