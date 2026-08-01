---
name: clusterby-tests-label-invariance
description: "CLUSTER BY / k-means test outputs: cluster_id LABELS are arbitrary; assert the partition, not labels"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 2147a65d-7f57-419c-ad59-1b3f153979ce
---

k-means cluster **labels** (cluster_id 0/1/2…) are arbitrary — only the **partition** (which points group together)
is meaningful. Which physical cluster receives label 0 vs 1 depends on the order the seed groups come out of the
GROUP BY, which is not guaranteed stable across builds/environments. The clustering is otherwise deterministic
(fixed first-k seed + fixed 3 Lloyd iterations → same partition every run).

**Why:** A test that prints raw `cluster_id` per point (exact-text compare) is fragile — a label swap 0↔1 breaks it
even though the partition is identical. This bit the committed `runtimets/cluster-by/kmeans-exec` golden (labels
flipped after the CLUSTER-BY-syntax rewrite work, same partition).

**How to apply:** Make CLUSTER BY / k-means test assertions **label-invariant** — assert per-cluster counts
(`array_count(members)`), or each cluster as the sorted list of its member ids ordered canonically (e.g.
`order by members`), NOT the exact cluster_id values. `kmeans-syntax` was already robust (per-cluster counts);
`kmeans-exec` was changed to report canonical cluster membership. See [[clusterby-phase2-design]].
