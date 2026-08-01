---
name: clusterby-runtime-dataflow
description: "CLUSTER BY end-to-end runtime dataflow — the two-phase (tower→label) model, why full records never enter the k-means tower, kmeansPP-exact mechanics, and known perf TODOs"
metadata:
  node_type: memory
  type: reference
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

CLUSTER BY (ASTERIXDB-3785) runtime dataflow, verified against code 2026-07-22 (branch `clusterby-gerrit`,
`KMeansInitCandidatesOperatorDescriptor`, `SqlppClusterByVisitor`, optimizerts plans). Builds on
[[clusterby-vector-materialization-layers]] and [[clusterby-phase2-design]].

**Two disjoint phases — the key mental model:**
1. **k-means tower = pure `vectors → Cfinal`.** Seed → oversample → weigh+recluster → Lloyd. It reads ONLY
   the projected embeddings and emits ONLY the k final centroids `Cfinal`. **No records anywhere in the tower,
   not even in Lloyd's last iteration** — Lloyd's per-point assignments are computed only to move centroids,
   then discarded. The tower never outputs "vector→cluster" pairs and never tracks record identity.
2. **label + group = `records + Cfinal → result`.** Runs strictly AFTER the tower. Each original row
   **self-labels**: `cluster_id = nearest_centroid(row.embedding, Cfinal)`. There is **no join / no
   back-reference** from a clustered vector to its record — the row re-derives its own label from its own
   embedding + the broadcast Cfinal. Then `GROUP BY cluster_id` → `{centroid, cluster_radius, members}`.

**Source is scanned ONCE and teed by the optimizer's common-subtree REPLICATE — NOT re-scanned.** One tee
output → project embedding → materialized vectors run file → tower; the other tee output → the label/group
branch, buffered at the replicate until `Cfinal` is ready, then reconsumed. So **full records never enter the
tower — only embeddings are materialized for clustering** (the efficiency property that matters); the
label branch carries the projected record columns, and only downstream of the tower.

**Plan-reading gotchas (cost me two wrong answers — DON'T repeat):**
- `optimizerts/.../cluster-by/kmeans-*-validate.plan` = HAND-WRITTEN pure-SQL++ reference oracle (nested
  `let C0..C3 = select value centroid(..) group by nearest_centroid(..)`). It genuinely has many independent
  `data-scan`s and 0 replicate — it is NOT the CLUSTER BY operator path. **`kmeans-syntax-*.plan` = the actual
  CLUSTER BY desugar** (shows the REPLICATE sharing). Diagnose the real path from `-syntax-`, never from `-validate`.
- Algebricks reprints a REPLICATE's whole input subtree once per output edge → identical-var `data-scan` lines
  are the SAME shared scan reprinted, not separate scans (see [[clusterby-vector-materialization-layers]]).

**kmeansPP-exact mode (de-facto PRODUCTION target, decided 2026-07-22; but still OPT-IN — default is
`kmeansPP`).** Oversample round = 2 passes / 2 merges: ① broadcast pool C → ② COST (each partition sums local
`Σd²(x,C)`) → ③ merge₁ reduce → global potential `φ=Σd²`, broadcast back (barrier) → ④ SAMPLE (each node draws
every point independently with `p = ℓ·d²/φ`, ℓ=2k, seeded RNG) → ⑤ merge₂ UNION `C←C∪C′` (keep every draw, no
re-limit; per-round count varies, E=ℓ). Fixed 5 rounds. Recluster = k heaviest weighted means (pad to k, an
approximation of weighted k-means++, NOT real k-means++). Lloyd ×3 fixed. Default `kmeansPP` = deterministic
top-2k, φ skipped (the older animation). Third mode `random` = k lexicographically-smallest. (This supersedes
the stale "ψ remains future work" note in [[clusterby-vector-materialization-layers]] — exact mode implements it.)
Operator: `KMeansInitCandidatesOperatorDescriptor`, `Mode ∈ {ROUND,COST,SAMPLE,WEIGH,RECLUSTER,LLOYD,FINALIZE}`;
label fn `nearest_centroid` / score fn `nearest_centroid_distance` (squared-Euclidean).

**cluster_radius nuance:** `sqrt(max(nearest_centroid_distance(embedding, Cfinal)))` — measured to the
ASSIGNMENT centroid, which MAY differ from the members-mean `centroid` also reported (two reference points).

**Known perf TODOs on the exact path (candidate WIP optimizations):**
1. **d²(x,C) computed twice per round** — COST computes each point's nearest-pool d² to sum φ then discards it;
   SAMPLE re-scans the whole pool to recompute the same d² for the coin flip. Fix: COST emits per-vector d² so
   SAMPLE reuses it (the φ barrier makes the 2 passes unavoidable, but the distance recompute is not).
2. **ADM re-deserialization every pass** — vectors are stored as tagged ADM ordered-lists and decoded to
   `double[]` on every pass; exact does ~15 full run-file scans vs ~10 for default (extra COST+SAMPLE). Fix:
   materialize/stream decode-free packed `double[]`.

Animations: `407436d4` (CLUSTER BY — exact k-means‖ dataflow) is aligned with the exact path;
`f5fbf7bc` (animated dataflow) depicts the DEFAULT deterministic top-2k. See [[artifact-sql-waf-reset]].
