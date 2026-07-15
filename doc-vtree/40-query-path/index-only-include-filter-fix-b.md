# Design: index-only ANN with an INCLUDE-field filter ("fix B")

> **Status:** draft (deferred follow-up)
> **Verified against:** `1e555cc896` (2026-07-21)
> **Scope:** how to serve `SELECT VALUE m.pk … WHERE m.<include> … ORDER BY ann_distance(...) LIMIT k`
> as an index-only plan (no primary lookup) with the filter evaluated inside the vector cursor — the
> optimization deferred after the correctness fix ("A") landed. See the bug-archive entry
> "Index-only ANN + WHERE on an INCLUDE field" for the original defect.

## Where we are today (after fix A)

- **Correctness is solved.** A PK-only projection with a WHERE on a non-PK field no longer takes the
  index-only branch (`IntroduceTopKAccessMethodRule.isProjectionPkOnly` also checks below-LIMIT SELECT
  vars). It falls back to **lookup-and-rerank**.
- **The filter is already in the cursor even in the fallback.** In the lookup-and-rerank plan, the
  physical rule `PushFilterIntoVectorSearchRule` pushes the INCLUDE predicate into the `VECTOR_SEARCH`
  unnest-map (`condition (gt($$x, <lit>))`), so it is evaluated **before the top-K cut** — recall is
  correct. Confirmed by EXPLAIN.
- **The only thing missing is skipping the primary BTree lookup** for the PK projection. That is a
  perf optimization, not a correctness or recall gain. This is what "fix B" would recover.

## Why the obvious B does not work

The tempting approach — embed the filter into the index-only vector unnest during the **logical**
access-method rule (`VectorIndexAccessMethod`), reusing the pushdown's embed logic — fails at
compile time:

```
ASX1079: Could not infer type for variable '$$N'
```

The embedded INCLUDE filter variable is an **output** of the vector unnest-map that is referenced
**only** by the operator's `selectCondition` annotation (evaluated at runtime), not by any logical
operator above it. During logical type-environment recomputation, that variable has no consumer that
establishes its type in scope, so inference fails. This is exactly why `PushFilterIntoVectorSearchRule`
is registered in `physicalRewritesTopLevel` and not in the logical phase.

Empirically verified: forcing `indexOnly = true` for this query shape and running EXPLAIN produces the
`Could not infer type` error, not a plan.

## The real fix: a dedicated physical-phase rule

Do the embed **after** the index-only plan is set, in the physical phase — mirroring how the existing
pushdown already works for lookup-and-rerank. Sketch:

1. **Let the index-only plan form as usual, keeping the SELECT.** In `isProjectionPkOnly`, permit the
   index-only branch when the below-LIMIT WHERE references only PK or INCLUDE fields (PK is rewritten
   as today; INCLUDE will be served by the new rule). The index-only branch must **not** drop or
   neutralize the SELECT's INCLUDE predicate — leave the SELECT in place above the vector unnest, and
   ensure `neutralizeDanglingExpressions` does not collapse it (e.g. skip neutralize for a SELECT whose
   only record-var uses are INCLUDE field-accesses, so the field-access survives to the physical phase).
   The record var is otherwise dead, so this is the delicate part: the field-access must remain typeable
   until the physical rule consumes it. One option is to keep the INCLUDE field-access alive by having
   the index-only branch project the INCLUDE column as an unnest output var **and** rewrite the SELECT's
   field-access to that var; then the physical rule embeds from the var (needs the physical rule taught
   to accept a var-ref condition, not just field-access).

2. **New physical rule `PushFilterIntoIndexOnlyVectorSearchRule`** (or extend the existing one to run in
   both phases): match a SELECT directly above an **index-only** `VECTOR_SEARCH` unnest-map, call the
   shared `embedIncludeFilterIntoVectorUnnest` helper (extract it from `PushFilterIntoVectorSearchRule`
   as originally planned — single source of truth for the physical-tuple offset math), then drop the
   SELECT. Running in the physical phase means the embedded filter var types correctly (as it already
   does for lookup-and-rerank).

3. **Reuse the shared embed helper.** The offset math (`numSecondaryKeys + numPrimaryKeys + includePos`)
   and annotation wiring (`VECTOR_FILTER_VAR_MAPPING` / `VECTOR_FILTER_VAR_TYPES` + `setSelectCondition`)
   must stay identical to the lookup-and-rerank path. Extract once, call from both.

## Constraints and risks to honor

- **Keep index-only off the streaming cursor.** `ClusterSearchResult.quantizedDistance` is always `NaN`
  on the query path (see [bug-archive](../60-quality/bug-archive.md) latent-risks). A NaN distance from
  the non-top-K streaming cursor is a hard `IOException`. Whatever B lands must route index-only onto the
  top-K cursor only.
- **FIELD_ACCESS_BY_INDEX.** By the physical phase the predicate is `field-access-by-index($$m, i)`
  (record constructors closed). The guard and any field-name matching must resolve the index → name via
  the record type (as `PushFilterIntoVectorSearchRule.extractFieldNames` already does), not assume
  by-name access.
- **Composite PK.** Keep the `numPrimaryKeys`-aware offset (the composite-PK fix); do not regress it.
- **LIMIT semantics.** The filter must remain **before** the top-K cut (inside the cursor). A
  filter-after-unnest plan would return fewer than `k` rows — do not do that.

## Verification when B is built

- Extend runtimets `vector/create-index-vtree-include-filter`: keep the correctness assertion
  (returns `6,7,8`) and add a `.plans`/EXPLAIN assertion that the plan is **index-only** (no
  `BTREE_SEARCH` / primary `index-search`) while the `VECTOR_SEARCH` carries the `condition (...)`.
- A WHERE on a genuinely non-indexed field must still fall back to lookup-and-rerank and return correct
  rows.
- The composite-PK pushdown test must still pass.
