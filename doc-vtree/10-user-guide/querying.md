# Querying — top-k ANN search with `ann_distance`

> **Status:** current
> **Verified against:** `9c5fd8c704` (2026-07-07)
> **Scope:** the query-time user surface — the optimizable ORDER BY/LIMIT pattern, every
> `ann_distance` argument, query-time SET knobs, the plan shapes you get, and the caveats.

Companion files: [ddl.md](ddl.md) and [operations.md](operations.md). The internals live in
[40-query-path/optimizer.md](../40-query-path/optimizer.md) (plan rewrite),
[search-cursors.md](../40-query-path/search-cursors.md) (runtime), and
[navigation.md](../40-query-path/navigation.md) (single-component search).

## 1. The optimizable pattern

```sql
SELECT m.id, m.title
FROM Movie m
[ WHERE <predicate> ]
ORDER BY ann_distance(m.embedding, <query-vector>, <metric> [, <min_probe_fraction> [, <k_multiplier>]])
LIMIT <k>;
```

`IntroduceTopKAccessMethodRule`
(`asterixdb/asterix-algebra/.../optimizer/rules/am/IntroduceTopKAccessMethodRule.java`)
matches **LIMIT → ORDER BY ann_distance(...) → … → DATASOURCE_SCAN** (intermediate ASSIGN /
EXCHANGE / nested LIMIT operators are skipped), checks for a usable VTree index on the
field, and swaps the scan for a vector index search. **Both LIMIT and ORDER BY are
required** — no LIMIT (or an ORDER BY on anything but a single `ann_distance` call) means no
index use.

Example (exact top-3 by probing every cluster):

```sql
SELECT VALUE m.id
FROM MovieSmall m
ORDER BY ann_distance(m.embedding, [0.0, 0.0, 0.0, 0.0], "euclidean", 1.0)
LIMIT 3;
```

## 2. `ann_distance` arguments

Registered as `ann-distance` with VARARGS (`BuiltinFunctions`); the evaluator
(`ANNDistanceDescriptor`, `asterixdb/asterix-runtime/.../functions/vector/ANNDistanceDescriptor.java`)
enforces 3–5 arguments and returns a `double`.

| # | argument | type | required | default | semantics |
|---|---|---|---|---|---|
| 1 | vector field | field reference | yes | — | must be the exact field the index is declared on |
| 2 | query vector | array constant/parameter | yes | — | same dimension as the index |
| 3 | metric | **constant string** | yes | — | `euclidean`/`l2`, `euclidean_squared`/`l2_squared`, `cosine`/`"cosine similarity"`, `dot` — anything else is a runtime error; a non-constant metric is a compile-time error |
| 4 | `min_probe_fraction` | numeric constant | no | `0.1` | fraction of leaf clusters to probe, `[0,1]`; converted at runtime to `nprobe = max(1, floor(totalLeafClusters × fraction))`; `0` and `1.0` mean "use default" and "probe everything (exact within the index)" respectively; out-of-range constant → compile error |
| 5 | `k_multiplier` | integer constant | no | `1` | per-partition candidate budget = `K × k_multiplier`; extra candidates are collected and reranked before LIMIT; a double literal is a compile error; runtime clamps to ≥ 1 |

Validation quirks (from `VectorIndexAccessMethod#ensureDoubleConstant/ensureInt32Constant`):
the range/type checks for args 4–5 only fire for *constant* expressions; a non-constant
expression is passed through unchecked. Args 4–5 have **no effect when the query falls back
to an exact scan** (no index / metric mismatch) — the scalar evaluator ignores them.

### `dot` sign convention (hazard)

`ann_distance(..., "dot")` returns **`-dot(a, b)`** — negated so that smaller "distance"
means more similar, matching the index's MIPS convention. `ORDER BY ... ASC` (the default)
therefore correctly returns the *most* similar vectors first. Don't add `DESC` expecting
"highest dot product first", and don't compare the returned values against raw dot products
you computed elsewhere. `vector_distance` follows the same convention. Background and the
sign-mismatch bug this fixed:
[30-storage-engine/distance-and-quantization.md](../30-storage-engine/distance-and-quantization.md).

## 3. Query-time SET knobs

Both are registered in the compiler's SET-allowlist (`SqlppCompilationProvider`), so they
are valid alongside queries.

| knob | type | default | effect |
|---|---|---|---|
| `compiler.vector.kmultiplier` | int ≥ 1 | 1 | Session-level candidate-budget multiplier. **Precedence: when set to a value > 1 it OVERRIDES the query's 5th argument** — `VectorSearchOperatorNodePushable#resetSearchPredicate` applies the query arg first, then unconditionally overwrites it with the session value if that is > 1. Set to 1 (or leave unset) to let the per-query argument win. |
| `compiler.vector.prunedsearch` | — | — | ⚠ **No-op.** Whitelisted in `SqlppCompilationProvider` but read by nothing in the codebase. Setting it changes no behavior. |

## 4. Index matching and the silent full-scan fallback

The rule picks an index whose **field matches arg 1** and whose **`similarity` matches
arg 3 after normalization** (`VectorIndexAccessMethod#normalizeDistanceMetric`: case- and
alias-insensitive — `l2` ≡ `euclidean`, `l2_squared` ≡ `euclidean_squared`,
`"cosine similarity"` ≡ `cosine`).

If the metrics don't match — or there is no VTree index on the field at all — **the query
still runs, silently, as an exact scan**: `ann_distance` is evaluated as a plain scalar over
every record, ORDER BY sorts all of them, LIMIT truncates. The only trace is a server-side
log line ("Distance metric mismatch … Falling back to full scan (KNN)"). Correct answers,
no index acceleration. Check the query plan (`EXPLAIN`/plan format) for a
`VECTOR_INDEX_SEARCH`/unnest-map over the index if you need to confirm the index fired.

## 5. Plan shapes

Detailed listings in [optimizer.md §5](../40-query-path/optimizer.md); the user-visible
summary:

- **Index-only** — chosen automatically when everything projected above the LIMIT is
  primary-key-only (`isProjectionPkOnly`). The secondary index emits `(pk…, dist)` directly,
  ORDER BY is rebound to the emitted distance, and **no primary-index lookup or rerank
  happens**. Fastest shape; distances are computed from *quantized* vectors.
- **Lookup-and-rerank** (default) — any non-PK projection forces a primary-index lookup of
  the candidate PKs; `ann_distance` is then re-evaluated on the **full-precision** records
  and ORDER BY/LIMIT rerank exactly. More I/O, exact final ordering over the candidates.
- **DISTINCT appears** in either shape when the index was created with
  `cross_pollination_m > 1`: the cursor can emit up to M copies of a record (one per
  replica cluster), so the optimizer splices a `DISTINCT` on the PKs above the index search
  (index-only) or above the primary lookup (rerank). With M = 1 the plan is unchanged.

### Filter pushdown on INCLUDE fields

A `WHERE` predicate that references **only INCLUDE fields** of the chosen index is pushed
into the index search by `PushFilterIntoVectorSearchRule`
(`asterixdb/asterix-algebra/.../optimizer/rules/PushFilterIntoVectorSearchRule.java`): the
filter is evaluated per candidate *inside* the cursor, so K counts only surviving rows.
Predicates on non-INCLUDE fields are evaluated the normal way (after lookup). Composite
primary keys are supported (offset bug fixed 2026-07-07 — see
[bug-archive.md](../60-quality/bug-archive.md)).

## 6. Caveats — read before relying on results

1. ⚠ **OPEN BUG — index-only + WHERE returns silently empty results.** When the projection
   is PK-only (index-only shape) *and* the query has a `WHERE` on an INCLUDE field, the
   index-only rewrite neutralizes the record-based predicate to `select (missing)` — every
   row is filtered out and the query returns **zero rows with no error** (found 2026-07-07
   during test design; not yet in the bug archive). **Workaround:** project at least one
   non-indexed field (e.g. `SELECT m.id, m.title`) to force the lookup-and-rerank shape,
   where the WHERE works (and the INCLUDE pushdown fires correctly).

2. **`k_multiplier` × `cross_pollination_m` contract.** With replication M > 1, replicas
   consume the per-partition candidate budget *before* dedup; deduplication only happens in
   the DISTINCT above. Size `k_multiplier` with replica headroom (worst case ×M) or the
   query can return fewer than K distinct rows. Deliberate design decision — see
   [bug-archive.md](../60-quality/bug-archive.md) "Top-K budget consumed by
   cross-pollination replicas — DEFERRED BY DESIGN".

3. **Recall knobs and their semantics.**
   - `min_probe_fraction` (arg 4) — how many leaf clusters are probed. The dominant recall
     lever: `1.0` = probe everything (exact w.r.t. index contents), default `0.1`.
   - `epsilon` — declared in the index WITH clause (default 0.25), applied at query time as
     a *relative* widening of the cluster-candidate window (`d ≤ closest + |closest|·ε`,
     multiplicative so it also works for negative dot-product distances). You cannot change
     it per query without recreating the index.
   - `k_multiplier` (arg 5 / session knob) — widens the candidate pool per partition to
     absorb cross-partition ranking error and replica duplicates.
   Details: [navigation.md](../40-query-path/navigation.md) parameter table.

4. **Results are approximate by default.** Fraction 0.1 probing + quantized distances means
   missing true neighbors is expected behavior, and index-only distances are quantized
   approximations. Known recall-affecting open defects (k-means leaf drop, post-compact
   recall anomaly) are tracked in [bug-archive.md](../60-quality/bug-archive.md).

5. **Cosine assumes normalized vectors** — the engine never normalizes; un-normalized
   inserts silently degrade cosine results ([ddl.md §3](ddl.md)).

6. **Session `compiler.vector.kmultiplier` > 1 silently overrides the per-query arg** (§3) —
   remember to unset it.

## Related

- [ddl.md](ddl.md) — declaration-time parameters that shape query behavior (similarity,
  epsilon, cross_pollination_m)
- [operations.md](operations.md) — how DML/COMPACT affect query results
- [40-query-path/optimizer.md](../40-query-path/optimizer.md) → [search-cursors.md](../40-query-path/search-cursors.md) → [navigation.md](../40-query-path/navigation.md) — the full query-path chain
