# Optimizer — from SQL++ text to the physical ANN plan

> **Status:** current
> **Verified against:** `56881406e7` (2026-07-05)
> **Scope:** how `ORDER BY ann_distance(...) LIMIT k` becomes a `VECTOR_SEARCH` physical operator —
> every rule, every plan shape, every knob, down to the jobgen call.

First file of the query-path chain: **optimizer.md → [search-cursors.md](search-cursors.md) →
[navigation.md](navigation.md)**. For the patch-level overview and design theses see
[3771 — ANN optimizer rule](../80-patches/3771-ann-optimizer-rule.md); this doc goes below that
altitude and stays there.

All paths below are under
`asterixdb/asterix-algebra/src/main/java/org/apache/asterix/` unless noted.

## 0. Where the rules sit in the pipeline

`optimizer/base/RuleCollections.java`:

| Phase | Rule | Neighbors |
|---|---|---|
| `buildAccessMethodRuleCollection()` (logical) | `IntroduceTopKAccessMethodRule` | after `IntroduceSelectAccessMethodRule`, before `IntroduceJoinAccessMethodRule` |
| `buildPhysicalRewritesTopLevelRuleCollection()` | `PushFilterIntoVectorSearchRule` | after `PushLimitIntoPrimarySearchRule`, before `RemoveUnusedAssignAndAggregateRule` (which cleans up ASSIGNs the pushdown orphans) |
| physical-operator assignment | `SetAsterixPhysicalOperatorsRule` `case VTREE:` → `new VectorSearchPOperator(dsi, storageDomain, requiresBroadcast)` | dispatched on the `INDEX_SEARCH` unnest function's `AccessMethodJobGenParams.indexType` |

Ordering consequences: a `WHERE year > 2000` on a B-tree-indexed field is optimized *first*
(`IntroduceSelectAccessMethodRule`); the top-k rule then sees the rewritten subtree. The filter
pushdown runs much later, after `SetClosedRecordConstructorsRule`, because it must inline
field-access expressions into a condition whose record constructors are already closed.

## 1. `IntroduceTopKAccessMethodRule` (`optimizer/rules/am/`)

Extends `AbstractIntroduceAccessMethodRule`; registers exactly one access method
(`VectorIndexAccessMethod.INSTANCE`) keyed on `ANN_DISTANCE`.

### 1.1 Entry and traversal — `rewritePre` / `checkAndApplyTopKTransformation`

`rewritePre` fires only when the visited operator is a plan root
(`DISTRIBUTE_RESULT`, `SINK`, or `DELEGATE_OPERATOR`) and recurses downward itself. During the
recursion every non-matching ancestor is pushed onto **`aboveLimitOps`** (and popped in a
`finally`), so when a `LIMIT` is finally matched, the rule holds the exact ancestor chain from the
root down to (not including) the LIMIT. That chain is what makes the index-only analysis and
rewrite possible — the result projection (`SELECT VALUE m.idx`) lives *above* the LIMIT.

### 1.2 `findOrderOperator` — the LIMIT→ORDER walk

From `limitOp.getInputs().get(0)`, loop:

- tag == `ORDER` → found, return `(ref, op)`;
- tag ∈ {`ASSIGN`, `EXCHANGE`, `LIMIT`} → skip through input 0 (these are what
  `CopyLimitDownRule`/distributed-top-k rewrites interleave);
- anything else → `null`, no match.

`matchesAnnDistancePattern()` then only checks the ORDER has **exactly one** order expression.
Resolving that expression to `ann_distance` is deferred to after subtree init, because the ORDER
key is frequently a variable (`$$dist := ann_distance(...)` hoisted into an ASSIGN):
`resolveAnnDistanceExpr` handles both the direct-call and the variable case (linear scan of the
subtree's assigns/unnests for the defining ASSIGN).

### 1.3 `analyzeAndTransform` — the eight steps

1. `initializeSubTree()` — `OptimizableOperatorSubTree.initFromSubTree(orderOp.getInputs().get(0))`.
   The subtree starts at ORDER's *child* because `OptimizableOperatorSubTree` does not accept
   ORDER as a top operator. Must end at a `DATASOURCESCAN`.
2. Type environment of the ORDER operator captured.
3. `subTree.setDatasetAndTypeMetadata(metadataProvider)` — **must precede** filter-field
   extraction: `field-access-by-index` can only be turned into a field *name* with the record
   type in hand.
4. `findSelectOperatorInSubTree()` — walks ORDER→…→scan, stops at the first SELECT, and populates
   `filterFieldNames` via `extractFilterFieldsFromCondition`:
   - recursive over the condition; `field-access-by-name`/`-by-index` yield a field path
     (nested paths supported via `extractFieldPathRecursive`);
   - a bare variable reference is traced to its defining ASSIGN — first in
     `subTree.getAssignsAndUnnests()`, then (fallback) a linear walk from ORDER down
     (`searchAssignsInPlan`) — and the RHS is recursed into.
5. `analyzeAnnDistanceFunction` — resolves the ORDER key to the `ann_distance` call, extracts the
   metric from **arg 2** (string constant) and normalizes it (§1.5), then delegates argument
   validation to `VectorIndexAccessMethod.analyzeFuncExprArgsAndUpdateAnalysisCtx`: 3–5 args for
   `ANN_DISTANCE`; arg0 must be a variable/field reference and arg1 a constant
   (`AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVarAndUpdateAnalysisCtx`).
6. `fillSubTreeIndexExprs(...)` — standard access-method machinery matches the field against the
   dataset's indexes; `VectorIndexAccessMethod.exprIsOptimizable` accepts only `IndexType.VTREE`
   with a single key field equal to the accessed field.
7. `chooseVectorIndex` (§1.4).
8. `applyTopKPlanTransformation` — computes `indexOnly = isProjectionPkOnly()` (§1.6), calls
   `VectorIndexAccessMethod.createIndexSearchPlan(...)` (§2), and on success replaces **only**
   `subTree.getDataSourceRef()` with the returned operator. The LIMIT is added to the
   dont-apply set either way, so the rule never re-fires on this LIMIT.

### 1.4 Index choice — filter fields and metric identity

`chooseVectorIndex` iterates the candidate VTREE indexes from the analysis context:

- **Filter gate.** If a SELECT with filter fields was found, `indexHasIncludeFields` requires
  *every* filter field path to appear (list-equality) in the index's
  `VectorIndexDetails.getIncludeFieldNames()`. Indexes without the needed INCLUDEs are skipped
  entirely — they could serve the ANN but the post-filter would then run above the primary
  lookup with no recall protection.
- **Metric gate.** With a query metric available: an index whose normalized `similarity` equals
  the normalized query metric is an *exact match* (chosen, break). A field-only match is
  remembered but, if no exact match exists, the rule logs
  `"Distance metric mismatch … Falling back to full scan (KNN)"` and returns nothing — the query
  silently keeps the scan + exact `ann_distance` sort rather than producing wrong-ranked results.
- No query metric extractable → first field match wins (backward compatibility).

There is no cost model; the first exact match wins.

### 1.5 Metric normalization

`VectorIndexAccessMethod.normalizeDistanceMetric` (used for both the query string and the index
`similarity` WITH-field, case-insensitive, trimmed):

| input | canonical |
|---|---|
| `l2` | `euclidean` |
| `l2_squared` | `euclidean_squared` |
| `l1`, `manhattan_distance` | `manhattan` |
| `cosine similarity` | `cosine` |
| `euclidean`, `euclidean_squared`, `manhattan`, `cosine`, `dot` | unchanged |
| null/empty | `euclidean` (default) |
| anything else | passed through as-is |

### 1.6 `isProjectionPkOnly` — the index-only detector

Gated by `INDEX_ONLY_ENABLED = true` — a compile-time kill switch, not a correctness guard (the
delete-leak that once forced it off was a storage bug, fixed by the sorted directory insert; see
[dml.md §6](../30-storage-engine/dml.md)). The algorithm:

1. **Partition the scan's variables.** `dsVars` = the `DataSourceScan`'s variable list; the first
   `numPK` are `pkVars`, everything after (dataset record + optional meta record) is
   `recordVars`. Bail (return false) if any PK path is composite/nested (`p.size() != 1`) or the
   scan produces fewer than `numPK + 1` variables.
2. **Collect bindings.** `collectAssignBindings` walks the entire subtree below the LIMIT and
   maps every ASSIGN variable → its defining expression.
3. **Compute live-out.** `VariableUtilities.getUsedVariables` over every operator in
   `aboveLimitOps` — everything any ancestor of the LIMIT reads.
4. **Trace each live-out variable** with `isVarPkSafe` / `isExprPkSafe` (mutually recursive, with
   a `visiting` set to reject binding cycles):
   - a PK variable → safe; a direct record/meta variable reference → unsafe;
   - a constant → safe;
   - `field-access-by-name($$rec, "f")` where `$$rec ∈ recordVars` → safe **iff** `f` is a PK
     column name; any other field → unsafe;
   - any other function call → safe iff all arguments are safe;
   - a variable with no tracked binding (produced by an operator we don't model) → unsafe.

Conservative by construction: meta records fall into `recordVars` so any use is unsafe; external
datasets fail the shape checks. One `false` anywhere → the legacy lookup-and-rerank plan.

## 2. `VectorIndexAccessMethod#createIndexSearchPlan` — building the replacement subplan

Note this is a *custom* overload `(limitRef, orderRef, annDistanceExpr, subTree, chosenIndex,
analysisCtx, context, selectOp, indexOnly, aboveLimitOps)`; the `IAccessMethod` interface method
of the same name returns `null` (vector indexes don't do SELECT/join transformations).

### 2.1 The search-key ASSIGN

Five fresh variables, evaluated below the unnest-map:

| slot | variable | source | coercion |
|---|---|---|---|
| 0 | `$$qv` | `ann_distance` arg 1 (query vector), cloned | none |
| 1 | `$$k` | `limitOp.getMaxObjects()`, cloned | none |
| 2 | `$$metric` | `ann_distance` arg 2 | none |
| 3 | `$$mpf` | arg 3 if present, else constant `ADouble(0.1)` | `ensureDoubleConstant`: int/long/float constants → ADouble; null/missing or out of [0,1] → compile error |
| 4 | `$$kmult` | arg 4 if present, else constant `AInt32(1)` | `ensureInt32Constant`: AInt64 → AInt32 (the runtime reads with `IntegerPointable.getInteger`, which consumes 4 bytes — the first half of a big-endian AInt64 is 0); float/null/missing → compile error |

The ASSIGN's input is a **deep copy** of the scan's input (usually ETS + EXCHANGE), so the new
subplan is self-contained. Args 3/4 are ignored for the (recognized-but-unwired)
`vector_distance` function family — `isVectorDistance` forces the defaults there.

### 2.2 `VectorJobGenParams` — the optimizer↔jobgen wire format

`optimizer/rules/am/VectorJobGenParams.java`, extends `AccessMethodJobGenParams`. Serialized into
the `INDEX_SEARCH` unnest function's argument list:

| position in funcArgs | content |
|---|---|
| `[0 .. getNumParams())` | base params: index name, index type (VTREE), database/dataverse/dataset, retainInput=false, requiresBroadcast=false |
| `[getNumParams()]` | **`indexOnly` boolean** — deliberately placed *before* the varlist so readers can find it positionally without knowing the list length |
| after that | the 5-variable `queryVarList` (length-prefixed varlist encoding) |

The class comment documents a **slot [5] `search_approach`** (0 = ann_distance, 4 =
vector_distance) in `queryVarList`, but `createIndexSearchPlan` only ever appends five variables —
slot 5 is a reserved placeholder for the dual-navigation experiment, **never written and never
read** anywhere in-tree today.

### 2.3 The secondary unnest-map

`AccessMethodUtils.createSecondaryIndexUnnestMap(...)` builds
`UNNEST_MAP [$$pk…] ← index-search(<funcArgs>)` over the ASSIGN. For VTREE,
`appendSecondaryIndexOutputVars`/`appendSecondaryIndexTypes` take the `primaryKeysOnly` branch
(shared with inverted indexes): **only PK variables are emitted** — the embedding, distance and
centroid fields never surface as logical variables (comment: avoids shipping large embeddings).
The unnest function is marked `setReturnsUniqueValues(true)`.

A vestigial in-method filter hook, `addIncludeFieldsAndSetSelectCondition`, is called when a
SELECT exists — but it **always returns false** (block comment "FILTER PUSHDOWN DISABLED": the
inlined condition would reference `$row`, which is produced *above* the vector unnest by the
primary lookup, so the type environment can't see it). Real pushdown happens later in
`PushFilterIntoVectorSearchRule` (§3), which creates fresh variables instead of reusing `$row`.

### 2.4 Index-only branch

Entered when `indexOnly` (from §1.6). If the unnest isn't an `UnnestMapOperator` or the ORDER has
≠ 1 expression the branch **rolls itself back** (removes the added variable, clears the flag) and
falls through to the legacy shape. Otherwise:

1. **`$$dist` appended** to the unnest's variables with type `BuiltinType.ADOUBLE` — the runtime
   will emit `[pk…, dist]` (see [search-cursors.md §2](search-cursors.md)).
2. **ORDER rewritten**: the single order expression (the `ann_distance` call or its variable) is
   replaced by `VarRef($$dist)`, same direction. No re-evaluation of distances downstream.
3. **PK substitution.** The secondary unnest allocated *fresh* PK variables; the scan (about to be
   deleted) owned the old ones. `substituteVariablesInDescendantsAndSelf` runs from
   `rewriteRoot = aboveLimitOps.get(0)` (the outermost ancestor, e.g. DISTRIBUTE_RESULT) — not
   from the LIMIT — so references above the LIMIT are also redirected.
4. **`rewriteRecordFieldAccessToPk`**: every `field-access-by-name($$rec, "id")` where `id` is a
   PK column becomes `VarRef($$pk_new)` — this repairs the projection ASSIGNs
   (`$$idx := $$m.getField("idx")`).
5. **`neutralizeDanglingExpressions`**: fixpoint pass seeded with the dead record variable; any
   ASSIGN expression whose subtree (transitively) references a dead variable is replaced with a
   `MISSING` constant, and its own variable joins the dead set. This repairs chains like
   `$$237 := field-access($$rec, "embedding")` → `$$dist0 := ann_distance($$237, …)` that nothing
   above LIMIT consumes but the type system still walks.
6. **Re-typing, bottom-up**: `computeAndSetTypeEnvironmentForOperator(unnest)`,
   `typeOpRec(limitRef)`, then each `aboveLimitOps[i]` from the one nearest the LIMIT up to the
   root (the list is root-first, so it's iterated in reverse).
7. **DISTINCT for cross-pollination**: iff `extractCrossPollinationM(index) > 1` (reads
   `cross_pollination_m` from the index's WITH object, default 1), a `DistinctOperator` keyed on
   the new PK variables is spliced directly above the unnest — up to M replica copies of a record
   would otherwise each surface as a `(pk, dist)` candidate. `DistinctOperator` propagates all
   input variables, so `$$dist` survives for the ORDER above. For M = 1 the bare unnest is
   returned so the plan stays byte-identical to the legacy path.

### 2.5 Lookup-and-rerank branch (default)

`AccessMethodUtils.createRestOfIndexSearchPlan(...)` with `sortPrimaryKeys = true`: an ORDER on
the candidate PKs, then a primary-index `UNNEST_MAP` that fetches `[$$pk…, $$rec]`. The original
ORDER BY `ann_distance` and LIMIT above are untouched — the ORDER re-evaluates **exact**
distances on the fetched records (via the scalar `ANNDistanceDescriptor` fallback function), so
the index's approximate ranking only affects recall, never final ordering.

DISTINCT for M > 1 goes **above** the primary unnest here (can't sit between the two unnests:
`createRestOfIndexSearchPlan` casts its input to `AbstractUnnestMapOperator`), keyed on the
**data-source operator's PK variables** — `createRestOfIndexSearchPlan` emits the primary unnest
reusing the scan's variable IDs, so the secondary unnest's PK vars are *not* in scope above it
(using them yields "Could not infer type for variable").

## 3. `PushFilterIntoVectorSearchRule` (`optimizer/rules/`)

Runs on SELECT operators in `physicalRewritesTopLevel`. Shape:

```
SELECT (cond on INCLUDE fields)          ASSIGN* (SELECT removed)
  └─ ASSIGN*                       ⇒       └─ … PRIMARY_INDEX_UNNEST
      └─ … PRIMARY_INDEX_UNNEST                  └─ VECTOR_INDEX_UNNEST [$pk…, $inc1…]
            └─ VECTOR_INDEX_UNNEST [$pk…]              selectCondition: cond($inc1…)
```

Mechanics:

1. `findVectorIndexUnnest`: skip ASSIGNs below the SELECT, then recursive descent for an
   `UNNEST_MAP` whose expression is `INDEX_SEARCH` with `indexType == VTREE`. Declines if a
   `selectCondition` is already set (one pushdown per unnest).
2. **Inline** the SELECT condition: clone it, then walk the ASSIGN chain below the SELECT,
   inlining any referenced, `isFunctional()` ASSIGN definitions via
   `InlineVariablesRule.InlineVariablesVisitor` — after this the condition references
   field-access expressions on the source record, not ASSIGN variables.
3. Extract the referenced field names; **bail if any is not in the index INCLUDE list**
   (matching on the *last* path segment).
4. For each referenced INCLUDE field allocate a fresh variable and record its **physical field
   index**: `numSecondaryKeys + numPrimaryKeys + positionInIncludeList`, where
   `numSecondaryKeys` is 4 (quantized) or 2 (non-quantized) and `numPrimaryKeys` is
   `dataset.getPrimaryKeys().size()` (composite-PK fix, 2026-07-07) — the counter increments
   across *all* INCLUDE fields, referenced or not, so unreferenced ones just leave gaps. Field
   types come from `recordType.getSubFieldType(path)`, defaulting to `ANY` for open fields.
5. Two annotations are attached to the unnest-map for the physical layer:
   `VECTOR_FILTER_VAR_MAPPING` (var → physical field index) and `VECTOR_FILTER_VAR_TYPES`
   (var → `IAType`). The fresh variables and their types are also appended to the unnest's
   variables/variableTypes "so sanity checks recognize them".
6. The condition's field accesses are rewritten to the fresh variables
   (`rewriteFieldAccess`), the result is set as the unnest's `selectCondition`, and the SELECT is
   deleted from the plan.

Caveats pinned by the code: the layout comments in this rule and in
`VectorIndexFilterSchema` list the quantized secondary fields as
`[distance, qDist, qEmbed, centroidId]`, but the authoritative
`VTreeDataTupleConstants` order is `[distance, centroidId, qDist, qEmbed]` — harmless today
because only the *count* (4) enters the offset math, but the comments mislead.

## 4. Codegen — `VectorSearchPOperator` and `getVectorSearchRuntime`

### 4.1 `VectorSearchPOperator` (`algebra/operators/physical/`)

Tag `PhysicalOperatorTag.VECTOR_SEARCH`, extends `IndexSearchPOperator`.
`contributeRuntimeOperator`:

1. Re-reads `VectorJobGenParams` from the unnest function args.
2. `getKeyIndexes(queryVarList, inputSchemas)` → `queryFields` — the *column positions* of the
   five search-key variables in the input (the ASSIGN's output frame). These positions travel to
   the runtime, which re-extracts the values per input tuple.
3. Decides `numSecondaryKeys` (4/2) from the index's `quantization` WITH-field — the same
   dichotomy as the filter rule.
4. If the unnest has a `selectCondition`, compiles it into an `ITupleFilterFactory` via
   `mp.createTupleFilterFactory` using two wrappers:
   - **`VectorIndexFilterSchema`** — an `IOperatorSchema` view of the *storage tuple*: variables
     found in the delegate op-schema (the PKs) are offset by `+ numSecondaryKeys`; filter-only
     variables resolve through the `VECTOR_FILTER_VAR_MAPPING` annotation to their absolute
     physical index; `getVariable(index < fieldOffset)` returns null (the distance/centroid
     fields have no logical variables).
   - **`VectorIndexFilterTypeEnvironment`** — delegates to the operator's type env but answers
     for the filter-only variables from `VECTOR_FILTER_VAR_TYPES`.
   The stock scalar-evaluator machinery then compiles the predicate against storage-tuple
   offsets — this is why the cursor can filter *before* the top-K buffer.
5. Calls `MetadataProvider.getVectorSearchRuntime(jobSpec, outputVars, opSchema, typeEnv, ctx,
   retainInput, dataset, indexName, queryFields, tupleFilterFactory, jobGenParams.isIndexOnly())`.

### 4.2 `MetadataProvider#getVectorSearchRuntime` (`asterixdb/asterix-metadata/.../declared/MetadataProvider.java`)

- Looks the index up again via `MetadataManager`, builds the output `RecordDescriptor` from the
  op-schema, and resolves partitioning (`partitionsMap = computeStorageMap`).
- **`indexEpsilon`** = index WITH `epsilon`, default **0.3** — the level-wise navigation window
  ([navigation.md §4](navigation.md)); a per-index DDL knob, not a query knob.
- **`kMultiplier`** = session config `compiler.vector.kmultiplier` (parsed, `max(1, …)`,
  default 1). This is the *session-level override*; the query-level `k_multiplier` (arg 4) rides
  separately in the search-key tuple, and the runtime lets the session value win when > 1.
- `numSecondaryKeys` from `VTreeDataTupleConstants.Q_NUM_SECONDARY_FIELDS` (4) /
  `NQ_NUM_SECONDARY_FIELDS` (2).
- Instantiates the three injected factories — `AOrderedListVectorBinaryAccessorFactory` (decode
  ADM ordered-list → `double[]`), `VectorDistanceFunctionFactory` (metric string →
  `IVTreeDistanceFunction`), `OptimizedScalarQuantizerFactory` (float[6] params → quantizer) —
  and builds the `VectorSearchOperatorDescriptor` with
  `(…, queryFields, …, partitionsMap, numPrimaryKeys, numSecondaryKeys, tupleFilterFactory,
  kMultiplier, indexEpsilon, indexOnly)`.

**Knob inventory:** `compiler.vector.kmultiplier` is read here;
`compiler.vector.prunedsearch` is *declared* (`CompilerProperties.COMPILER_VECTOR_PRUNEDSEARCH_KEY`,
whitelisted in `SqlppCompilationProvider`) **but has no reader anywhere** — setting it is
currently a no-op. (`compiler.vector.trainseed` belongs to the creation pipeline.)

## 5. Full before/after plan listings

Setup: dataset `Movie(idx int64 PK, title, year, embedding)`, index
`ix_emb` = VTREE on `embedding`, `similarity: "euclidean"`, quantized (SQ8), INCLUDE `[year]`,
`cross_pollination_m = 1`.

### 5.1 Index-only shape

```sql
SELECT VALUE m.idx FROM Movie m
ORDER BY ann_distance(m.embedding, [0.1, 0.2, 0.9, 0.8], "euclidean")
LIMIT 10;
```

Before (simplified logical plan; `$$m` = record, `$$21` = PK from scan):

```
DISTRIBUTE_RESULT [$$26]
└─ ASSIGN $$26 ← field-access-by-name($$m, "idx")        ← above LIMIT (aboveLimitOps)
   └─ LIMIT 10
      └─ ORDER (ASC) $$24
         └─ ASSIGN $$24 ← ann-distance($$23, [0.1,…], "euclidean")
            └─ ASSIGN $$23 ← field-access-by-name($$m, "embedding")
               └─ DATASOURCE_SCAN Movie → [$$21, $$m]
```

`isProjectionPkOnly`: live-out = {`$$26`}; `$$26` traces to
`field-access($$m, "idx")` with `idx` ∈ PK names → **index-only**. After:

```
DISTRIBUTE_RESULT [$$26]
└─ ASSIGN $$26 ← $$27                                    ← rewritten by step 4 (§2.4)
   └─ LIMIT 10
      └─ ORDER (ASC) $$28                                ← rewritten from ann-distance
         └─ ASSIGN $$24 ← MISSING                        ← neutralized (referenced $$23)
            └─ ASSIGN $$23 ← MISSING                     ← neutralized (referenced $$m)
               └─ UNNEST_MAP [$$27, $$28] ← index-search("ix_emb", VTREE, "Movie",
                        indexOnly=true, $$29, $$30, $$31, $$32, $$33)
                  └─ ASSIGN $$29 ← [0.1,0.2,0.9,0.8], $$30 ← 10,
                            $$31 ← "euclidean", $$32 ← 0.1, $$33 ← 1
                     └─ (deep copy of scan input: EXCHANGE ─ EMPTY_TUPLE_SOURCE)
```

`$$27` = fresh PK var, `$$28` = `$$dist` (ADOUBLE). No primary lookup, no rerank; runtime emits
`[pk, dqx]` already sorted by approximate distance, ORDER re-sorts (stable no-op modulo
cross-partition merge), LIMIT trims. With `cross_pollination_m = 3` a
`DISTINCT [$$27]` would sit between the ORDER and the UNNEST_MAP.

### 5.2 Lookup-and-rerank shape (with pushed filter)

```sql
SELECT m.idx, m.title FROM Movie m
WHERE m.year > 2000
ORDER BY ann_distance(m.embedding, [0.1, 0.2, 0.9, 0.8], "euclidean")
LIMIT 10;
```

`m.title` is not PK-derivable → `isProjectionPkOnly()` = false. After the access-method rule
**and** `PushFilterIntoVectorSearchRule` (which later removes the SELECT):

```
DISTRIBUTE_RESULT [$$rec']
└─ ASSIGN … projection ($$m.idx, $$m.title) …
   └─ LIMIT 10
      └─ ORDER (ASC) ann-distance(field-access($$m,"embedding"), [0.1,…], "euclidean")
         └─ ASSIGN* (year/embedding accesses; SELECT year>2000 was here, now removed)
            └─ UNNEST_MAP Movie.Movie [$$21, $$m] ← index-search("Movie", BTREE, primary,
                     lowKey=$$21, highKey=$$21)                    ← primary lookup
               └─ ORDER (ASC) $$21                                 ← sortPrimaryKeys
                  └─ UNNEST_MAP [$$21] ← index-search("ix_emb", VTREE, indexOnly=false,
                           $$29…$$33)
                     selectCondition: gt($$34, 2000)               ← pushed filter
                     annotations: VECTOR_FILTER_VAR_MAPPING {$$34 → 5},
                                  VECTOR_FILTER_VAR_TYPES {$$34 → AInt64}
                     └─ ASSIGN $$29 ← [0.1,…], $$30 ← 10, $$31 ← "euclidean",
                               $$32 ← 0.1, $$33 ← 1
                        └─ (EXCHANGE ─ EMPTY_TUPLE_SOURCE)
```

`$$34`'s physical index 5 = 4 secondary fields + 1 PK + position 0 in the INCLUDE list. The
filter runs inside the storage cursor *before* the top-K buffer, so the K best *year > 2000*
candidates are collected — a post-filter above the ORDER would instead silently shrink recall.
The ORDER recomputes exact distances on the fetched records; the index only proposed candidates.

## 6. Where control goes next

At runtime the `VectorSearchOperatorDescriptor` built in §4.2 creates a
`VectorSearchOperatorNodePushable`, which per input tuple opens the LSM top-K cursor —
continue in **[search-cursors.md](search-cursors.md)**.

## Known gaps / discrepancies (as of `56881406e7`)

- `queryVarList` slot [5] (`search_approach`) is documented in `VectorJobGenParams` but never
  written or read.
- `compiler.vector.prunedsearch` is whitelisted but unread — a no-op knob.
- ~~`PushFilterIntoVectorSearchRule` offset math assumes a single PK column
  (`numSecondaryKeys + 1`)~~ — **FIXED 2026-07-07** (amended into the 3771 commit): now
  `numSecondaryKeys + numPrimaryKeys + positionInIncludeList`; regression test
  `vector/create-index-vtree-composite-pk`. See
  [bug-archive.md](../60-quality/bug-archive.md).
- Quantized field-order comments in `VectorIndexFilterSchema` transpose `centroidId` and the
  quantized fields relative to `VTreeDataTupleConstants`; only the count is used, so behavior
  is unaffected (the layout comment in `PushFilterIntoVectorSearchRule` was corrected with the
  composite-PK fix).
- `PushFilterIntoVectorSearchRule` appends the filter variables to the unnest-map's output
  variable list, but the runtime pushable adds only `[pk…(, dist)]` fields
  ([search-cursors.md §2](search-cursors.md)) — **verified benign**: the generic pushable
  sizes its tuple builder from the declared output RecordDescriptor, so emitted tuples carry
  the full slot count with never-written trailing slots (phantom zero-end-offset fields that
  nothing reads); see [bug-archive.md](../60-quality/bug-archive.md) latent-risks
  (2026-07-05).
- `VectorIndexAccessMethod` recognizes the `euclidean/cosine/dot` scalar functions
  (`vector_distance` family, "index-driven KNN") in analysis but no rule wires them to a plan.
