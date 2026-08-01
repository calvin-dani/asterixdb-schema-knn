---
name: vtree-ann-query-conditions
description: "Exact conditions under which a VTREE vector query uses the index (ANN) vs a full scan (KNN), and how the integration tests write them"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

VTree/ANN query behavior on columnar (see [[vtree-branch-model]]). The optimizer rule is
`IntroduceTopKAccessMethodRule` + `VectorIndexAccessMethod` (asterix-algebra `optimizer/rules/am/`).

**`vector_distance()` NEVER uses the vector index — by design.** It is public sugar rewritten at
COMPILE time (`SqlppVectorDistanceRewriteVisitor`) into a concrete 2-arg per-metric scalar
(`euclidean-squared-distance`, `cosine-distance`, ...). The index rule only ever fires on the
`ann-distance` function identifier (`resolveAnnDistanceExpr` requires `BuiltinFunctions.ANN_DISTANCE`).
So `vector_distance ... ORDER BY ... LIMIT k` is always a `DATASOURCE_SCAN` = legitimate exact/brute-force
KNN baseline. `approx_vector_distance` maps to `ann-distance` (`CommonFunctionMapUtil`).

**For `ann_distance()` to actually hit the index (VECTOR_SEARCH), ALL must hold:**
1. Query vector is a COMPILE-TIME CONSTANT. Inline literal array works; `LET qv = [literal]` /
   `WITH qv AS ([literal])` works (constant-folded). A runtime `LET qv = (SELECT ...)[0]` subquery
   does NOT — falls back to DATASOURCE_SCAN.
2. Metric string matches the index's declared `similarity` EXACTLY (aliases ok: l2_squared==euclidean_squared).
   `'l2'`/`'euclidean'` against an `euclidean_squared` index = mismatch = silent full scan.
3. Plain `ORDER BY ann_distance(...) LIMIT k` — **NO `NULLS LAST`** (and no OFFSET). `ASC NULLS LAST`
   changes the order spec and disables the rewrite. This is why the index test suites omit NULLS LAST
   while the un-indexed `distance-functions` suite uses it.

Ordering by the projected alias (`... AS dist ... ORDER BY dist`) is fine. Projecting a non-PK field
(dist/title) adds a BTREE_SEARCH rerank on top of VECTOR_SEARCH (the "lookup-and-rerank" plan) — still
index-based.

**4th arg = min_probe_fraction** (default 0.1). `ann_distance(emb, qv, "metric", 1.0)` probes every
cluster ⇒ result == brute force ("exact by construction"). This is how `create-index-vtree*` golden tests
validate recall (+ `SET compiler.vector.kmultiplier "30"`). Verified: 3-arg ann recall@10 ≈ 5/10 on the
10k movie set; 4-arg 1.0 == vector_distance exactly.

Test suites: `asterixdb/asterix-app/src/test/resources/runtimets/queries_sqlpp/vector/` —
`create-index-vtree*` (index/ANN), `distance-functions/` (scalar/KNN). Index DDL needs
`EXCLUDE UNKNOWN KEY` + `ANALYZE DATASET` first. Ready-made queries: `~/vector-queries/{ann,ann_exact,knn}_query.sql`.
