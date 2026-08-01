---
name: clusterby-exact-sampling-desugar-bench
description: Result of prototyping/benchmarking the exact-sampling (Bernoulli phi) k-means|| init as a hand-written desugar vs top-2k
metadata: 
  node_type: memory
  type: project
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

Prototyped (2026-07-19/20) the paper-exact k-means|| oversampling as hand-written SQL++ (no product code change):
per round `phi = SUM(nearest_centroid_distance(v, pool))`, `WHERE random(v.id+base) < l*d2/phi`, `pool = array_concat`.
Files: ~/.claude/jobs/7215a24d/tmp/bench_{top2k,exact}.sqlpp. Ran on embedded Apache-asterix cluster
(AsterixHyracksIntegrationUtil, port 19002) with the full movie fixture (99,936 x 384, clusterbyTests/
movie_filtered_indexed.jsonl), k=8, l=16, 5 rounds. See [[clusterby-vector-materialization-layers]].

**VERDICT: a clean exact-vs-top2k desugar timing is CONFOUNDED — the dominant cost is STRUCTURAL, not the
sampling method.** The desugar carries the oversampled `pool` as a single LIST VALUE. As it grows (~250KB at
5 rounds) it blows the 32KB frame + 96KB group budget → must SET compiler.framesize=512KB + all mem knobs
(joinmemory/groupmemory/sortmemory/windowmemory/textsearchmemory, each >=5x framesize) to 64MB. That large
frame makes even a 2k-row run take ~24s (both queries, near-identical — pure overhead) and garbles result
printing (NUL bytes; metrics.elapsedTime still valid). TOP2K at full scale never completed (run1 empty,
run2 >4min, killed) — re-confirms the prior "499s / unrunnable" result. Exact sampling removes the SORT but
NOT the pool-as-list framing cost (SUM+filter still broadcasts the growing pool list each round).

**IMPLEMENTED 2026-07-20 (user overrode the "shelved" decision below — this is a SPEC-COMPLIANCE task, follow
the paper; the naive 2k inertia test can't overturn the paper's guarantees).** Route B1 built + validated
end-to-end on the embedded cluster: `initMode="kmeansPP-exact"` → operator tower `seed → 5×(COST+SAMPLE) →
WEIGH → RECLUSTER → 3×(WEIGH+LLOYD)`. COST emits per-partition local Σd² partial → broadcast → global φ;
SAMPLE draws each vector p_x=ℓ·d²/φ (per-partition seeded RNG), keeps every draw (keepAllCandidates skips the
top-l re-limit). Tiny 6-pt test → correct [[1,2,5],[3,4,6]], deterministic across runs. Full 99,936×384 movie
→ 8 balanced clusters (sum 99,936) in ~10s — SCALES where the desugar couldn't. Negative: exact+runtime_init=false
→ clear error. 9 files: BuiltinFunctions (KMEANS_COST/KMEANS_SAMPLE fids), KMeansInitCandidatesOperator (+Mode.COST/
SAMPLE, seed/keepAllCandidates + 2 deep-copy visitors), POperator, descriptor (emitCost/emitSample), translator
(dispatch+seed+keepAll+shared-set), SqlppClusterByVisitor (exact tower + validation). Test: runtimets cluster-by/
kmeans-exact (+golden +ClusterByQueries.xml). Full hyracks+asterix build SUCCESS. On clusterby-gerrit, uncommitted,
NOT pushed. Seed base fixed (EXACT_SEED_BASE=1000003); user-configurable seed is a follow-up. Design doc:
doc-clusterby/design/exact-sampling-operator-design.md.

**(Superseded) earlier conclusion — exact Bernoulli sampling at scale belongs in the OPERATOR (Route B, pool in-heap double[][]),
NOT the desugar.** The exact desugar is only viable as a small-scale reference/spec. This FLIPS the earlier
"prototype the desugar, it may be fast without the sort" lean — the prototype exposed the desugar's structural
ceiling (pool framing) that sits beneath the sort-vs-filter question. random(seed) is arity-1 (RANDOM_WITH_SEED),
seed per-point via v.id for reproducible+partition-invariant draws.

**PAYOFF VALIDATED 2026-07-20 — DECISION: DO NOT IMPLEMENT exact sampling (quality tie).** Before building
the operator (Route B1), measured init quality = inertia (Σ d²(x, C0), lower=better) on a 2k sample, k=8, via
the desugar (compute C0 → INSERT to scratch dataset with big frame [dodges the NUL-result garble] → read C0
back + inertia with default frame). Result: random init 2880.2; **top-2k (current) 1616.9; exact (paper
Bernoulli) 1603.1**. Both oversampling inits beat random by ~44%, but exact beats the deterministic top-2k by
only **0.9% — essentially a tie.** So the deterministic top-2k already captures ~99% of the paper sampling's
benefit; the substantial operator work (two-phase COST/SAMPLE + φ-reduce + seeded RNG + threading + tests)
would buy <1% tighter clusters. NOT justified on quality; only paper-faithfulness would justify it. Design
doc kept at doc-clusterby/design/exact-sampling-operator-design.md for if it's ever revisited. Caveats: 2k
scale, one seed, φ before Lloyd — a bigger gap COULD appear at larger k/data, but <1% is a strong signal.
