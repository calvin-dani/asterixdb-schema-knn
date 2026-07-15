---
name: todo-crosspollination-regression-test
description: "TODO — add a real cross-pollination regression test (current Part 3 doesn't assert the feature)"
metadata: 
  node_type: memory
  type: project
  originSessionId: f918823a-9e3b-4f5f-a2b3-6f30a6442e6e
---

**TODO (deferred 2026-06-30):** the integration suite has NO real regression for cross-pollination. We bolted `cross_pollination_m=3` onto the shared Part-3 insert/delete test, but its assertions (`0 leaked`, `0 missing`, `_verify_pks`) ALL pass at M=1 too — so a silent disable of cross-pollination would stay green. Also overwrote Part-3's old M=1 coverage.

A real regression must assert two independent things:
1. **Replication actually happened** — catches "cross-pollination silently off". (The fan-out telemetry that signalled this was REVERTED as debug logging, so the test needs its own signal: a secondary-tuple-count query, or a recall-vs-M=1 delta, or re-add a narrow assertable counter.)
2. **Index-only dedup works** (the [[project-crosspollination-dml-multicluster]] Distinct fix) — assert the index-only ANN returns `count == unique` (dupes == 0). Already computed in `run_ann_query_pks`, just not asserted. Guards removal of the index-only DistinctOperator.
Plus the existing `0 leaked` / `all present` (meaningful only once replication is asserted).

Suggested shape: dedicated `test_part5_cross_pollination.py` + config block, own dataverse, params pinned to GUARANTEE replication (M=3, rng=3, eps wide). Revert Part-3 to M=1 to restore baseline insert/delete coverage. Optional: add a cosine case.
