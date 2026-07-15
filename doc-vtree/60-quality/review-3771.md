# Code Review — ASTERIXDB-3771 (ANN query optimizer rule), Gerrit change 21287

Reviewer: Shahrzad Shirazi (all 10 unresolved threads on PS22/PS23). Pulled 2026-07-22.

## Fixed (working tree on `vtree-split`, pending fold into the 3771 commit)

| # | Location | Fix |
|---|----------|-----|
| U1 | `QueryTranslator.java:1774` | `excludeUnknownKey`/`castDefaultNull` are BTree-only DDL options, always empty for a vector index; pass `OptionalBoolean.empty()` directly (no behavior change) + comment. |
| U2 | `CompilerProperties` + `SqlppCompilationProvider` + `MetadataProvider` | `compiler.vector.prunedsearch` was a no-op (registered, never read) → removed entirely. `compiler.vector.kmultiplier` is live → constant renamed `COMPILER_VECTOR_K_MULTIPLIER_KEY`, ini string kept (used by golden tests). |
| U5/U6 | `BTreeResourceFactoryProvider.java:157/207/258` | Dead VTREE branches removed — `Dataset` routes VTREE to `VTreeResourceFactoryProvider`, never here. |
| U9 | `VectorSimilarityMetric.java` | Constructor → `String... aliases`; dropped redundant `canonical` field + `System.arraycopy`; `canonical()` = `aliases.get(0)`. |
| U10 | `VectorSimilarityMetric.java` | Removed unused `allAliases()` (+ now-dead `Arrays`/`Set` imports). |

## Fixed — second round (U7, U8)

### U8 — `VectorDistanceFunctionFactory.createDistanceFunction` — DONE (folded into 3760)
- Replaced the `func == null` warn+euclidean branch with `throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, ...)`. The metric is validated at CREATE INDEX, so this is unreachable in normal operation and now fails loudly if the DDL allow-set and the runtime `DISTANCE_MAP` ever drift, instead of silently ranking by euclidean. Removed the now-dead `LOGGER`/log4j imports. Interface already declared `throws HyracksDataException`, so callers unaffected. Golden suite 7/7.
- **Follow-up (still open, larger):** unify the DDL `ALLOWED_VECTOR_DISTANCE_METRICS` literal with the runtime `VectorSimilarityMetric` taxonomy so the two can't drift at all (would reintroduce an `allAliases()`-style accessor; needs `asterix-lang-common` → `asterix-runtime` visibility check).

### U7 — vector field element type not validated at DDL — DONE (folded into 3771)
- Added a check in `QueryTranslator` right after the field-existence check: a declared vector field must be an ordered list of a numeric item type (`[double]`/`[float]`/…); ANY/open fields defer to runtime; a declared scalar, record, or non-numeric list is rejected with `ErrorCode.COMPILATION_ILLEGAL_KEY_TYPE` → `ASX1022: Field of type <t> cannot be used as a vector key field`. `getVectorIndexKeyTypes`'s `FULL_OPEN_ORDEREDLIST_TYPE` assumption is now guaranteed valid by this gate.
- Covered by new negative runtimet `vector/create-index-vtree-invalid-field-type` (verified: raises ASX1022 with source location).

## TODO — open items

### U3 — single-use static factories in `CreateIndexStatement`
- `CreateNonVectorIndexStatement` / `CreateVectorIndexStatement` are thin single-use wrappers over the constructor. Judgment: keep (the vector one hides a run of null/false/emptyList args, mild readability value) or inline per reviewer. Low stakes.

### U4 — `VECTOR_INDEX_MAX_CROSS_POLLINATION_M = 1024`
- Reply-only. Arbitrary sanity ceiling (realistic M is 1-8); a guardrail against fat-finger errors, not an architectural limit. Optionally lower it or spell out the rationale in the constant's javadoc.
