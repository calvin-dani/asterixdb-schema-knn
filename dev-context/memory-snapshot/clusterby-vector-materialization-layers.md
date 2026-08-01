---
name: clusterby-vector-materialization-layers
description: "How the input vectors (__vecs) get materialized/shared in CLUSTER BY desugar (G2) vs KMeans operator (G3), and what Approach A actually changed"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

CLUSTER BY k-means‖ input-vector (`__vecs`) materialization — verified against code 2026-07-19 (asterix-runtime
`KMeansInitCandidatesOperatorDescriptor`, asterix-algebra `SqlppExpressionToPlanTranslator`). See [[clusterby-phase2-design]]
and [[clusterby-runtime-dataflow]] (the end-to-end tower→label picture; NB the "psi remains future work" note below
is now superseded — the `kmeansPP-exact` mode implements the potential φ + Bernoulli sampling).

**Three distinct layers — don't conflate:**
1. **Datasource scan** — `__vecs` read from disk once.
2. **Materializing REPLICATE** (optimizer-inserted, plan level) — when `__vecs` feeds multiple consumers
   (each Lloyd pass's GROUP BY key+aggregate / each WEIGH stage), the optimizer tees it via ONE REPLICATE,
   materializing ONE run file so branches don't recompute. **The vectors ARE cleanly single-shared** —
   directly analogous to G3's one shared run file. CAUTION: the Algebricks plan-TEXT printer re-prints a
   REPLICATE's whole input subtree once per output edge, so the reference desugar plan SHOWS "14
   DATASOURCE_SCAN / 32 REPLICATE" but there are really only **3 physical scans** (one `Reviews` scan
   printed 12× with identical vars `$$976,$$975` = one shared scan teed to 12 consumers). Do NOT read the
   line-counts as independent scans — identical variable numbers across repeated lines = same operator
   reprinted. (Earlier "fragmented, many replicate points" framing was WRONG — it's one clean tee.)
3. **Operator-internal materialization** (G3 only) — each KMeans operator instance's `StoreVectors` activity
   (input 0) materializes its branch into a `MaterializerTaskState` run file for re-scan by its Score activity.

**Approach A (commit a1726994f1) changed ONLY layer 3 — REPLICATE (layer 2) is untouched.**
- Before: every ROUND/WEIGH stage's StoreVectors materialized its OWN run file → ~9 internal copies (on top
  of the REPLICATE file).
- After: exactly ONE stage is the **writer** (`vectorsWriter=true`) — materializes into a joblet-scoped state
  keyed `sharedVectorsKey#vec#<partition>`, constructed with refcount `sharedConsumerCount` (self-deletes after
  that many reads). Every other ROUND/WEIGH stage is a **reader**: its StoreVectors is a no-op sink that just
  **drains** input 0 (empty open/nextFrame/close, to satisfy the blocking edge) while its Score reads the
  writer's shared file. Net ~9→1 internal run files. Threading: translator recurses the pool-arg chain with one
  `KMeansTowerShared`; every ROUND+WEIGH gets the shared key (incl. all 3 Lloyd WEIGH passes). RECLUSTER/LLOYD/
  FINALIZE read a LIMIT-1 dummy, so they're NOT vector readers and keep their own trivial materialization.

Per partition: desugar = scan1 + REPLICATE1; G3 pre-A = scan1 + REPLICATE1 + ~9 internal; G3 post-A =
scan1 + REPLICATE1 + 1 internal(writer)+drains. So there are STILL two materialized vector copies in G3
(REPLICATE + operator-internal) — that redundancy is what the open **"A′"** idea targets: drop the REPLICATE
fan-out so the operator's own shared file is the single materialization.

Lloyd per-iteration split: **WEIGH** re-reads the shared vectors, assigns each point to nearest current
centroid, emits per-partition `{count(scalar), sum(double[dim])}` partials; **LLOYD_MERGE** ignores vectors
(LIMIT-1 dummy), folds broadcast partials by centroid (`weights[seq]+=count`, `sums[seq]+=sumvec`) → new
centroid = sum/count. Partial carried in a `Row{kind,partition,seq,score,vec}`: score=count, vec=sum, seq=centroid.
The LLOYD merge holds all partials in one partition's heap ArrayList = O(P·k·dim), uncapped/no-spill (large-k
hazard, NOT large-N — vectors never accumulate). Same {count,sum} as the desugar's two-step CENTROID aggregate.
