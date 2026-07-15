---
name: project-clusterby-architecture
description: "CLUSTER BY feature — chosen logical-plan architecture (Path B: assign-then-groupby)"
metadata: 
  node_type: memory
  type: project
  originSessionId: c0c8173f-b5b9-424d-b467-255c9cc39736
---

The CLUSTER BY SQL++ feature (fuzzy/similarity analog of GROUP BY for vector embeddings;
K-Means in v1) will use **Path B: assign-then-groupby**, decided 2026-06-17.

Plan shape:
1. A NEW blocking `ClusterAssign` logical operator (its own LogicalOperatorTag) — buffers
   all input vectors, runs K-Means, emits N tuples each labeled with `cluster_id` +
   `centroid` + `cluster_radius`. Physical op modeled on `ExternalSortOperatorDescriptor`
   (two activities + AbstractStateObject buffer + framesLimit spill). Cardinality-preserving
   like `RunningAggregateOperator extends AbstractAssignOperator`.
2. A STOCK, unmodified `GROUP BY cluster_id` GROUP AS rvc (LISTIFY) — produces the k cluster
   rows `{cluster_id, centroid, cluster_radius, rvc:[members]}`. centroid/radius ride as
   decor (functionally determined by cluster_id; uses existing null-first decor pass-through).

**Why not the alternatives:**
- Path A (annotate a group-all GroupByOperator): rejected. Forces guarding ~5-10 GBY rules
  (combiner family x3, EnforceStructuralProperties, physical-op selection, etc.) that are
  NOT compile-checked — a missed guard = silent wrong clusters. Also a semantic lie
  (group-all logically yields 1 row, K-Means emits k). High silent-failure risk.
- Path C (monolithic new ClusterByOperator, the spec's original assumption): same ~27
  visitor-edit cost as B but a far more complex operator and reuses none of group-by's
  grouping machinery — strictly more work than B.

**Path B cost:** ~27 `ILogicalOperatorVisitor` implementors each need a
`visitClusterAssignOperator` — but mechanical, compile-enforced (loud failure), mostly
mirroring the trivial AssignOperator case. Because step 2 is a genuine group-by, ZERO
optimizer rules need guarding (combiner/sort/partition rules become correct/desirable).

Docs live under `doc-clusterby/` (see `doc-clusterby/README.md` for the map):
`design/SPEC_VALIDATION.md`, `design/REUSE_ANALYSIS.md`, `plan/IMPLEMENTATION_PLAN.md`,
`bootstrap/{ORIGINAL_APE_PROPOSAL,CLUSTER_BY_IMPLEMENTATION_SPEC_INITIAL}.md`,
per-phase notes in `phases/phase-N/`.
