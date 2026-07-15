# Operations — DML, lifecycle, and limitations from the user's seat

> **Status:** current
> **Verified against:** `9c5fd8c704` (2026-07-07)
> **Scope:** what INSERT/DELETE/UPSERT, flush, COMPACT, and restart mean for a dataset with
> a VTree index, plus the current limitation list.

Companion files: [ddl.md](ddl.md), [querying.md](querying.md). Engine internals:
[30-storage-engine/dml.md](../30-storage-engine/dml.md) and
[30-storage-engine/lsm-lifecycle.md](../30-storage-engine/lsm-lifecycle.md).

## 1. DML is transparent

Once the index exists, plain SQL++ DML maintains it — no user action required. The
optimizer (`IntroduceSecondaryIndexInsertDeleteRule`, `case VTREE:`) splices an index
maintenance operator into every DML plan that projects `[vector, include…, pk…]` per
mutated record. Full trace: [dml.md](../30-storage-engine/dml.md).

What each statement does inside the index:

- **INSERT** — the new vector is routed to its closest leaf cluster(s) in the in-memory
  component and inserted in distance-sorted position. With `cross_pollination_m: M > 1`,
  the record is replicated into the **same M clusters** the bulk loader would have chosen
  (`findReplicaClusters` runs the identical eps + RNG pipeline), so DML'd and bulk-loaded
  records behave identically at query time.
- **DELETE** — deletes are **logical**: an *antimatter* tuple (same cluster/distance/PK,
  antimatter bit set) is inserted into the memory component for each of the M replica
  clusters. The matter tuple in older disk components is not touched; query-time
  reconciliation cancels the pair, and the pair is only **physically removed at merge**
  (see §3). Until a merge, deleted records still occupy space and candidate-scan effort.
- **UPSERT** — decomposed per record by `LSMSecondaryUpsertOperatorNodePushable` into
  *delete-old-entry + insert-new-entry* against the index (the plan carries both the old
  and new `[vector, include…]` projections). An upsert that doesn't change the vector or
  INCLUDE fields still performs both operations.

Implication of cross-pollination for write cost: every insert/delete does M cluster
insertions, and index size grows accordingly.

## 2. Flush — memory to disk

New/deleted entries first live in the index's in-memory LSM component, which becomes a disk
component on flush. Flushes happen without user involvement:

- **memory component full** (the organic path — write budget exhausted),
- **explicit dataset flush** (internal `FlushDatasetUtil`, e.g. during index build),
- **shutdown** — components are flushed so a clean restart loses nothing.

For VTree the flush is an identity copy of cluster contents **preserving antimatter**
(deletes stay logical across flushes). Triggers, mechanics, and file naming:
[lsm-lifecycle.md §1–2](../30-storage-engine/lsm-lifecycle.md).

**Restart durability:** after a restart, flushed components are reloaded and queries see
exactly the pre-shutdown state — including still-pending logical deletes. (A 2026-07-04 bug
where flushed-but-unmerged components became partially invisible after restart is FIXED —
[bug-archive.md](../60-quality/bug-archive.md) "Flush persists a LEAF page as the component
root".)

## 3. COMPACT DATASET

```sql
COMPACT DATASET test.Movie;
```

Schedules a **full merge** of every disk component of the dataset — primary and all
secondary indexes, VTree included — into one component per index
([lsm-lifecycle.md §3](../30-storage-engine/lsm-lifecycle.md)). For a VTree index this is
when matter/antimatter pairs are physically cancelled: space is reclaimed and deleted
records stop consuming query-time candidate budget. The statement returns when the merge is
scheduled/completed per the merge policy; background merges (`concurrent` policy by
default) also run automatically as components accumulate — COMPACT just forces a full one.

Note one OPEN observation on quantized indexes: an ANN recall anomaly after COMPACT at a
specific probe fraction has been seen and is under investigation (session record; the two
COMPACT-related delete-leak bugs previously in that area were FIXED 2026-07-07 — see
[bug-archive.md](../60-quality/bug-archive.md), "Partial merge re-encodes preserved
antimatter" and "Merge cancellation key includes quantized fields").

## 4. Rebuilding / dropping

- `DROP INDEX Movie.idx_emb;` works normally.
- There is no incremental retrain: the static clustering structure is fixed at CREATE
  INDEX time. If the data distribution drifts far from the training sample, recall degrades
  and the remedy is drop + re-ANALYZE + re-create.
- Records inserted after index creation are absorbed into the *existing* cluster structure
  (nearest existing centroid) — clusters are never split or re-trained online.

## 5. Limitations (current release)

- **Quantized-only.** Every index is SQ4/SQ8-quantized; there is no full-precision storage
  option (the non-quantized tuple format and its streaming cursor are deprecated — the
  top-K cursor is the only search path). Index-only query shapes return quantized-distance
  ordering; exact ordering requires the lookup-and-rerank shape
  ([querying.md §5](querying.md)).
- **One vector field per index, no composite vector keys** — grammar-enforced
  ([ddl.md §2](ddl.md)). Multiple vector indexes on different fields of one dataset are fine.
- **Metrics:** euclidean (/l2), euclidean_squared (/l2_squared), cosine, dot.
  **No manhattan** — `manhattan_distance` passes DDL validation but the build fails in
  Job 2 ([ddl.md §3](ddl.md), "ghost value").
- **`num_clusters` is ignored** for `creation_mode: "top-down"` unless SelectHead is
  explicitly disabled; it is also unvalidated at DDL time ([ddl.md §3](ddl.md)).
- **ORDER BY + LIMIT is the only accelerated pattern.** No `WHERE ann_distance(...) < r`
  range search, no join acceleration, no `vector_distance`-driven plans yet.
- **Open bugs a user can hit** (details in [bug-archive.md](../60-quality/bug-archive.md)
  unless noted):
  - index-only plan + WHERE on INCLUDE field → silent empty result (OPEN, workaround in
    [querying.md §6](querying.md); not yet archived);
  - bottom-up training builds ~`sqrt(K)` leaf clusters instead of `num_clusters` (OPEN) —
    affects recall/latency tuning;
  - top-K candidate budget consumed by cross-pollination replicas — by-design contract,
    requires `k_multiplier` headroom (DEFERRED BY DESIGN);
  - `quantized_distance` field ambiguity (OPEN, LOW — no user-visible effect known).

## Related

- [30-storage-engine/dml.md](../30-storage-engine/dml.md) — the full insert/delete/upsert trace
- [30-storage-engine/lsm-lifecycle.md](../30-storage-engine/lsm-lifecycle.md) — flush/merge mechanics
- [60-quality/bug-archive.md](../60-quality/bug-archive.md) — status of every known defect
