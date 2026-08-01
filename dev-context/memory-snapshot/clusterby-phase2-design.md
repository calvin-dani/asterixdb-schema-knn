---
name: clusterby-phase2-design
description: "CLUSTER BY Phase 2 distributed k-means design — architecture, realization, new code, open decisions"
metadata: 
  node_type: memory
  type: project
  originSessionId: 2147a65d-7f57-419c-ad59-1b3f153979ce
---

CLUSTER BY (ASTERIXDB-3783) Phase 2 = **true distributed, globally-synced k-means** (architecture A, not
gather-to-one/per-partition). v1 is brute-force: trivial seed (first/sample k), **3 hardcoded Lloyd
iterations, unrolled** into the plan, one default distance (Euclidean-squared).

**Realization = B (SQL++ desugar) — VALIDATED 2026-07-01.** The raw-operator approach (A: hand-wire a
Replicate in the translator) FAILED `FixReplicateOperatorOutputsRule` (ASX1054). Pivoted to B: express
CLUSTER BY as SQL++ and let the proven translator/optimizer build the distributed plan. A hand-written
k-means query (`optimizerts/cluster-by/kmeans-validate.sqlpp`) compiles to the target plan:
LOCAL agg-local-sql-centroid → HASH_PARTITION → GLOBAL agg-global-sql-centroid → listify →
BROADCAST_EXCHANGE → NESTED_LOOP join → nearest-centroid labeling. No manual Replicate. Template query:
`select ..., nearest_centroid(r.vec, (select value centroid(r2.vec) from T r2 group by 0)) from T r`.
Making `centroid` work in SQL++ needed the SQL/scalar variants (SQL_CENTROID family + SCALAR_[SQL_]CENTROID
collection descriptors; group-by sugar resolves centroid→array_centroid→sql-centroid). Commits: 1a94e422e9
(functions), 5a50bf6b57 (B validated). Full writeup: doc-clusterby/design/distributed-kmeans-design/PHASE2_B_VALIDATED.md.
Runtime (2026-07-01, commit 7145912871): **distributed k-means RUNS END-TO-END and recovers correct
clusters** via SqlppExecutionTest. The CENTROID two-step aggregate bug is FIXED: replaced the packed-list
intermediate (didn't round-trip local→global) with the AVG-style record {count:int64, sum:[double]}
(ClosedRecordConstructorEval emit + ARecordSerializerDeserializer.getFieldOffsetById read). Two byte-level
gotchas found by tracing: (1) a CLOSED record STRIPS a list field's outer ORDEREDLIST type tag on store — so
put sum LAST and prepend the tag back (sumLen = recordEnd - sumOff) before ListAccessor.reset; (2) the
provider's ADouble serializer ALREADY writes the DOUBLE tag, so a manual writeByte(tag) made double-tagged
10-byte items that misaligned the reader (the 1.1e-249 garbage) — drop the manual tag, use a TYPED [double]
list (matches declared output type) for both sum field and final centroid. Committed regression:
runtimets/cluster-by/kmeans-exec (tiny 2-D, k=2, seed id%2 + 3 unrolled Lloyd; registered in ClusterByQueries.xml).
SCALE-TESTED (2026-07-01) on the full movie dataset (clusterbyTests/movie_filtered_indexed.jsonl, 99936 rows,
384-dim embedding, id bigint PK) via the desugar form (k=8, seed id%8, 3 Lloyd): 8 balanced clusters, sizes sum to
99936, every centroid dim=384, ~73s embedded (localfs load, OPEN type {id,embedding:[double]}). So the 384-elem sum
list round-trips at scale. Working query saved (untracked) at clusterbyTests/e2e/movies_kmeans_desugar_query.sqlpp;
the movie-scale runtimets fixture was scratch-only (data dep too big for CI) and removed. cluster_radius not yet
produced (belongs to CLUSTER BY sc tail).
CLUSTER BY SYNTAX REWRITE DONE (2026-07-01, commit 62780da701): SqlppClusterByVisitor now desugars a SELECT with
CLUSTER BY into the distributed k-means SQL++ (query-level LET C0..C3 + GROUP BY nearest_centroid(vec,C3)); runs
end-to-end incl. CLUSTER AS members. Key impl points: (1) pass moved BEFORE substituteGroupbyKeyExpression()/
rewriteGroupBys() so the emitted GROUP BY rides the normal pipeline; (2) sc is NOT a materialized var/record —
substitute the FIELD ACCESSES directly: sc.cluster_id→grouping-key expr, sc.centroid→centroid(vec) as a per-cluster
aggregate, sc.cluster_radius→0.0. Building a record OR indexing C3[cluster_id] triggers an optimizer type-inference
NPE (OpenRecordConstructorResultType/RemoveRedundantVariablesRule) when members is also referenced — keeping every
post-group field on the group-aggregation path avoids it. v1: single FROM term (throws otherwise), k-means, first-k
seed, 3 Lloyd. Tests: runtimets/cluster-by/kmeans-syntax (sc + members); kmeans-exec reworked LABEL-INVARIANT (see
[[clusterby-tests-label-invariance]]). Removed 4 obsolete Phase A optimizer plan tests (asserted the old
ClusterAssignOperator EXPLAIN). Debugging note: asterix-app `mvn install` silently no-ops if apache-rat fails on a
license-header-less file — my asterix-app instrumentation didn't compile for many iterations until I found it; use
`-Drat.skip=true` + `clean` and `-XX:-OmitStackTraceInFastThrow` to surface swallowed compile NPEs.
Remaining follow-ups: real cluster_radius (max member euclidean_distance to centroid; EUCLIDEAN_DISTANCE builtin
exists); multi-FROM/join support; retire dead ClusterAssignOperator translator path (D1=a); centroid semantics
(currently mean-of-members = C4, vs C3[cid] the assignment centroid — chose members-mean to avoid the optimizer NPE).

(Superseded design notes below — logical-plan expansion in the translator; kept for context.) Per Lloyd iteration:
`broadcast centroids (one array value) → cross-join → AssignOperator nearest_centroid → GroupBy + CENTROID
two-step aggregate → LISTIFY → next centroids`. Unroll 3×, then final-label assign → existing GROUP BY.

New runtime code: (1) **CENTROID aggregate** = AVG clone with `double[] vectorSum`+count→divide (~8 classes
std + serializable, model `aggregates/std/AbstractAvgAggregateFunction`, register two-step in
`BuiltinFunctions`); (2) **`nearest_centroid(vec, centroidArray)`** scalar (argmin, reuses 3676
EuclideanSquared). Assignment/labeling need NO bespoke operator. Translator injection point:
`SqlppExpressionToPlanTranslator.visit(ClusterbyClause)` lines 290–308; thread centroid array as explicit var.

Decisions: **D1 RESOLVED = keep `ClusterAssignOperator` as the final labeler** (build 3 iterations
upstream as standard ops; give `ClusterAssignRuntimeFactory` a real label impl fed broadcast final
centroids; Phase A visitor wiring stays valid). Defaults: D2 seeding = first-k; D3 std-aggregate first
then serializable; D4 cross-join for centroid broadcast (validate via EXPLAIN early, ForwardOperator fallback).

Only hard dep for v1 is 3676 (distance fns). Full doc: `asterixdb/doc-clusterby/design/distributed-kmeans-design/PHASE2_DESIGN.md`.
Build/iterate on the `clusterby-dev` branch — see [[clusterby-branch-model]].
