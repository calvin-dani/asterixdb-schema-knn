# Distance functions and quantization — the end-to-end reference

> **Status:** current
> **Verified against:** `1f89f64f25` (2026-07-07)
> **Scope:** every distance-function and quantizer mechanism in the VTree project — the hyracks
> abstraction interfaces, the asterix implementations, every injection path, every use site, and
> the invariants a reviewer must check when any of it changes.

This doc absorbs the scope planned for `resource-and-injection.md` (resource JSON + injected
factories) and extends it into a review reference. Companion docs it links to instead of
duplicating: [index-instance-anatomy.md](index-instance-anatomy.md) §5 (resource JSON key
table), [page-formats.md](page-formats.md) (tuple byte layouts),
[dml.md](dml.md) (insert/delete flow), [navigation.md](../40-query-path/navigation.md)
(traversal algorithms), [search-cursors.md](../40-query-path/search-cursors.md) (top-K cursor),
[optimizer.md](../40-query-path/optimizer.md) (metric matching at plan time),
[creation-pipeline overview](../20-creation-pipeline/overview.md) (three-job orchestration),
[3760 patch walkthrough](../80-patches/3760-training-vtree-index.md).

---

## 1. The two-and-a-half interface families (hyracks side)

All three abstractions live in
`hyracks-fullstack/hyracks/hyracks-storage-am-vtree/src/main/java/org/apache/hyracks/storage/am/vector/api/`.

**Module-boundary rationale:** `hyracks-storage-am-vtree` cannot import AsterixDB types
(dependency direction is asterix → hyracks). The storage layer therefore only ever sees these
interfaces; the concrete implementations (which wrap `VectorDistanceCalculation`, ADM list
parsing, and OSQ math) live in asterix modules and are *injected* — via constructor, via
persisted-resource JSON, or via index-access parameters (IAP). This replaced an earlier
`Class.forName` reflection block in `VTree#search` (see `OptimizedScalarQuantizerFactory`
javadoc) and the deleted storage-side `VectorUtils` (full dependency inversion — see memory of
the distance-injection project and [3754a patch doc](../80-patches/3754a-storage-layer-p1.md)).

| Interface | Abstracts | Key members | Serializable? |
|---|---|---|---|
| `IVTreeDistanceFunction` | one `double apply(double[] a, double[] b)` — the metric | `@FunctionalInterface` | not itself; concrete impls add `Serializable` so they can ride inside a serialized factory |
| `IVTreeDistanceFunctionFactory` | metric-string → function resolution | `IAP_KEY = "VECTOR_DISTANCE_FUNCTION_FACTORY"`, `createDistanceFunction(String)` | `Serializable` **and** `IJsonSerializable` — it crosses both the job-serialization boundary (operator ctor field) and the resource-JSON boundary (`.metadata` file) |
| `IVTreeQuantizer` | encode/decode of one vector: `double[] quantize(double[])`, `double[] dequantize(byte[])` | `IAP_KEY = "VECTOR_QUANTIZER"` (test-only injection slot) | no — created per query/operator, never persisted |
| `IVTreeQuantizerFactory` | (metric, dims, `float[6]` params) → quantizer | `IAP_KEY = "VECTOR_QUANTIZER_FACTORY"`, `createQuantizer(String, int, float[])` | `Serializable` only — travels in the job, is **not** persisted to resource JSON (the `float[6]` params are persisted instead; the factory is stateless) |
| `IVTreeBinaryAccessor(Factory)` | extracting a `double[]` from the serialized query-vector bytes in a search-predicate tuple (ADM ordered-list decoding without asterix imports) | factory `IAP_KEY = "VECTOR_QUERY"`; accessor: `reset(byte[],int,int)`, `getVector()`, `getDimension()` | factory is `Serializable` + `IJsonSerializable` (persisted to resource JSON, like the distance factory) |

Note the asymmetry: the **distance factory** and **vector-accessor factory** are persisted on
the resource JSON (they are needed by DML on a restarted NC with no job context), while the
**quantizer factory** is not — only its *inputs* (the `float[6]` params + metric string) are
persisted, and each consumer reconstructs a quantizer locally.

---

## 2. Concrete implementations (asterix side, plus one test stub)

### 2.1 `VectorDistanceCalculation` — the single formula source

`asterixdb/asterix-runtime/src/main/java/org/apache/asterix/runtime/utils/VectorDistanceCalculation.java`
— pure static math, `double[]`-only (the post-migration API; the old `VectorDistanceArrCalculation`
/ `VectorDistanceArrScalarEvaluator` names are gone on this branch):

| Method | Formula | NaN guards |
|---|---|---|
| `euclidean` | `sqrt(euclideanSquared)` | length mismatch → NaN |
| `euclideanSquared` | `Σ(aᵢ−bᵢ)²` | length mismatch → NaN |
| `cosineSimilarity` | `dot/(‖a‖·‖b‖)` | length mismatch, zero norm, NaN norm/dot → NaN |
| `cosineDistance` | `1 − cosineSimilarity` | propagates NaN |
| `dotProduct` | `Σaᵢbᵢ` (**raw, positive**) | length mismatch, NaN sum → NaN |

Every distance implementation in the tree delegates to these five methods. **The dot negation
is applied by the wrappers, not here** — anything calling `dotProduct` directly gets the raw
(larger-is-more-similar) value.

### 2.2 `VectorDistanceFunctionFactory` — the canonical injected factory

`asterixdb/asterix-runtime/src/main/java/org/apache/asterix/runtime/utils/VectorDistanceFunctionFactory.java`
implements `IVTreeDistanceFunctionFactory`. Its `DISTANCE_MAP` (UTF8 lowercase-hash keyed)
resolves **7 aliases**: `euclidean`, `l2` → euclidean; `euclidean_squared`, `l2_squared` →
euclidean-squared; `cosine`, `"cosine similarity"` → `cosineDistance`; `dot` →
**`-dotProduct`** (MIPS convention: smaller = more similar — see §5, invariant 1).
Unknown metric → **WARN + default to euclidean** (no throw). JSON round-trip is just the class
identifier (stateless factory); `fromJson` returns `new VectorDistanceFunctionFactory()`.

### 2.3 `ANNDistanceDescriptor` — the SQL-function side

`asterixdb/asterix-runtime/src/main/java/org/apache/asterix/runtime/evaluators/functions/vector/ANNDistanceDescriptor.java`
backs `ann_distance(vec, qvec, metric [, min_probe_fraction, k_multiplier])`
(`BuiltinFunctions.ANN_DISTANCE`, arity 3–5). It has its **own** `DISTANCE_MAP` with the same
7 aliases and the same dot negation (`(a, b) -> -VectorDistanceCalculation.dotProduct(a, b)`).
Unknown metric → compile-time `RuntimeDataException`. The metric argument must be a constant
string, resolved once in `createEvaluatorFactory` → `resolveDistanceFunctionDouble`; the
per-tuple work happens in `VectorDistanceScalarEvaluator` (same package), which takes a
`ToDoubleBiFunction<double[],double[]>`.

The in-code comments in both classes declare the sync contract: *every alias accepted by
`ANNDistanceDescriptor` must resolve in `VectorDistanceFunctionFactory`, or the index search
path NPEs.* Currently they match exactly.

**Per-metric scalar descriptors** (same package): `EuclideanDistanceDescriptor`,
`EuclideanSquaredDistanceDescriptor`, `CosineDistanceDescriptor`, and `DotProductDescriptor`.
⚠️ `DotProductDescriptor` wires `VectorDistanceCalculation::dotProduct` — **raw, un-negated**.
That is correct for a plain scalar function, but `VectorIndexAccessMethod` also accepts
`BuiltinFunctions.DOT_PRODUCT` as an index-optimizable ORDER-BY function (see §5, invariant 2
— this is a live semantic-drift hazard).

### 2.4 Operator-local `DISTANCE_MAP` copies — three of them (drift check)

Three 3760/SPANN operators each embed a private copy of the wrapper classes and the map,
because they were written before the injected factory existed and take a metric *string* in
their constructor:

| Operator | Map contents | Unknown metric | Empty/null metric |
|---|---|---|---|
| `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor` (`asterix-runtime/.../operators/`, top-of-file wrapper classes + `buildDistanceMap()` ~line 540) | 6 keys: euclidean, l2, euclidean_squared, l2_squared, cosine, dot | **throws `IllegalArgumentException`** | defaults to **EuclideanSquared** |
| `SpannTopDownCentroidsOperatorDescriptor` (same package, `buildDistanceMap()` ~line 817) | same 6 keys | throws `IllegalArgumentException` | normalizes to `"l2_squared"` in ctor (~line 959) — same function, different label |
| `VTreeBulkLoaderAndGroupingOperatorDescriptor` (same package, `DISTANCE_MAP` ~line 170) | same 6 keys | **returns `null` silently** → later `IllegalStateException("DistanceFunction not initialized")` in `findCloseCentroidsLevelWiseGlobalSort` | NPE in `getDistanceFunction` (calls `.toLowerCase()` on null) — never happens in practice because DDL requires `similarity` |

**Verified: no formula or sign drift.** All three copies delegate to the same
`VectorDistanceCalculation` methods and all three negate dot
(`-VectorDistanceCalculation.dotProduct(a, b)`) — byte-for-byte the same wrapper bodies as the
canonical factory. The drift that *does* exist:

1. **Alias set:** the three operator maps lack the `"cosine similarity"` alias that the two
   canonical maps have. Latent, not live — operators only ever receive DDL-validated strings,
   and `VectorIndexDeclUtil`'s allowed set doesn't include `"cosine similarity"` either. It
   becomes live the moment someone widens the DDL set without touching the operator maps.
2. **Unknown-metric behavior:** warn-and-default (factory) vs throw (k-means/SPANN) vs
   silent-null (bulk-load op) vs compile error (ann_distance). Four different failure modes for
   the same input.
3. **`manhattan_distance` is a ghost metric** — see §5, invariant 3.

`HierarchicalKMeans...` and `SpannTopDown...` additionally share (as private copies, one each)
`clipCentroid` (clamps every centroid coordinate to ±1e3, NaN/Inf → 0.0 — silently),
`requiresNormalizedCentroids()` (`true` only for cosine → spherical k-means, matching
FAISS/Spark; dot is *not* normalized), and `normalizeL2` (local copy of the helper the
double[]-only `VectorDistanceCalculation` no longer exposes).

### 2.5 Quantization: `OptimizedScalarQuantizationSampleFile` + friends

All in `asterixdb/asterix-common/src/main/java/org/apache/asterix/common/storage/`.

**`OptimizedScalarQuantizationSampleFile`** (static utility, "OSQ"):

- **`Params`** (final class, all-final fields): `bits` (4 = SQ4, 8 = SQ8 in practice),
  `vectorDimensions`, `sampleCount`, `confidenceInterval`, `minQuantile`, `maxQuantile`,
  `alpha = (2^bits − 1)/(maxQuantile − minQuantile)`, plus derived Lucene-style constants
  (`alphaSquared`, `minQuantileAlpha`, `minQuantileSquared` — currently unused downstream).
  Populated at index creation by `QuantizationConstantsAggregate`
  (`asterix-runtime/.../aggregates/std/QuantizationConstantsAggregateDescriptor`) — see
  [creation overview §Job 1](../20-creation-pipeline/overview.md).
- **`quantizeVector(double[], Params, SimilarityFunction)`** → `QuantizedVector`. Per
  dimension: `v = clamp(x[i], minQ, maxQ)`, then `q = clamp(round((v − minQ)·α), 0, 2^bits−1)`.
  Storage width by bits: ≤8 → `byte[]` (**SQ4 stores codes 0–15 one per byte, not
  nibble-packed**), ≤16 → `short[]`, ≤32 → `int[]`. Codes are unsigned — consumers must read
  with `& 0xFF` / `& 0xFFFF`.
- **`dequantizeToDoubleArray(Object, Params)`** — the inverse: `x̂[i] = q/α + minQuantile`.
  This inverse mapping is itself a historical bug fix — the original code did only
  `(double)(bytes[i] & 0xFF)`, leaving values in code space, which preserved euclidean ranking
  (shift cancels in subtraction) but corrupted dot-product ranking (per-vector `minQuantile`
  bias). See §5, invariant 8.
- **`SimilarityFunction`** enum (`DOT_PRODUCT`, `COSINE`, `EUCLIDEAN`, `EUCLIDEAN_SQUARED`)
  and **`fromDistanceMetric(String)`** — yet another metric-string switch (aliases: euclidean/
  l2, euclidean_squared/l2_squared, cosine/"cosine similarity", dot/"dot product";
  **unknown/empty → `DOT_PRODUCT` silently**). Today the enum only *labels* the
  `QuantizedVector`; it changes no numeric behavior because…
- **`correctiveMultiplier` is disabled**: `ENABLE_CORRECTIVE_MULTIPLIER = false`, so it is
  always `0.0f` and is never persisted (`calculateCorrectiveMultiplier` — Lucene's exact
  formula for DOT/COSINE, 0 for euclidean — is dead code until the flag flips).
- Cosine contract (class javadoc): vectors **must arrive L2-normalized**; the engine never
  re-normalizes during quantization.

**`ScalarVectorQuantizer implements IVTreeQuantizer`** — the production quantizer.
Critical semantic: `quantize(double[])` is **encode-then-decode** (round trip through
`quantizeVector` + `dequantizeToDoubleArray`), returning a *lossy approximation in original
value space*, so unchanged `double[]` distance functions work on it. "Quantized distance"
throughout this project therefore means *distance between reconstructed vectors*, never
distance between integer codes. `dequantize(byte[])` decodes stored leaf/data-page bytes with
the same params.

**`OptimizedScalarQuantizerFactory implements IVTreeQuantizerFactory`** — builds a
`ScalarVectorQuantizer` from the wire `float[6]`. **The `float[6]` layout is a cross-module
contract**: `{0: minQuantile, 1: maxQuantile, 2: alpha, 3: confidenceInterval, 4: bits,
5: sampleCount}` — produced by `LSMVTreeLocalResource#createInstance` (same order) and consumed
here and by `VTreeDataTupleCreator#quantizeVector`. Metric string →
`fromDistanceMetric` → similarity label.

**`NoOpVectorQuantizer`** (`hyracks-storage-am-vtree/.../utils/`) — test-only identity
quantizer: `quantize` returns input, `dequantize` reads raw big-endian doubles. Injected by
unit tests directly under `IVTreeQuantizer.IAP_KEY` (the pre-built-instance slot in
`VTree.VTreeAccessor` — see §3b).

⚠️ **Third copy of the encode formula:** `VTreeDataTupleCreator#quantizeVector`
(`hyracks-storage-am-vtree/.../impls/VTreeDataTupleCreator.java` ~line 114) hand-inlines the
clamp-scale-round byte encode (it cannot import asterix-common). It matches `quantizeToByte`
today but is byte-only (assumes `bits ≤ 8`) and will drift silently if the OSQ encode ever
changes. It reads the same `float[6]` (indices 0/1/2/4).

### 2.6 The vector accessor

`AOrderedListVectorBinaryAccessor(Factory)`
(`asterixdb/asterix-om/src/main/java/org/apache/asterix/dataflow/data/common/`) — decodes an
ADM ordered list (float/double elements) into `double[]`. Used by the storage layer to extract
the query vector from `VTreeSearchPredicate`'s tuple and by DML to extract the record's
embedding (field 0 of the maintenance tuple — [dml.md §3.1](dml.md)).

---

## 3. Injection paths

| # | Path | Carrier | What travels | Consumer |
|---|---|---|---|---|
| a | DDL → resource JSON → `createInstance` | `.metadata` file via `PersistedResourceRegistry` | metric string, distance factory, accessor factory, cross-pollination `{m, rngFactor, epsilon}`, quantization `float[6]` (as 6 scalar keys) | `VTree` fields (permanent, per index instance) |
| b | Query-time IAP | `IIndexAccessParameters` map, per search | distance factory, quantizer **factory**, accessor factory, task ctx, top-K flag | `VTree.VTreeAccessor#createSearchOperationContext` → cursors |
| c | Job-time ctor params (3760/SPANN ops) | serialized operator descriptor | metric **string** (+ epsilon, M, rngFactor, dims, bits…) | each operator's private `DISTANCE_MAP` + locally built `ScalarVectorQuantizer` |
| d | Defaults / back-compat fallbacks | in code | see prose below | various |

### (a) DDL → resource → tree fields

1. `CREATE INDEX … TYPE VTREE WITH {…}` → `VectorIndexDeclUtil#validateAndGetWithObjectNode`
   (`asterixdb/asterix-lang-common/.../util/VectorIndexDeclUtil.java`) validates and
   canonicalizes the WITH record. `similarity` is **required**, checked against
   `ALLOWED_VECTOR_DISTANCE_METRICS = {euclidean, l2, euclidean_squared, l2_squared,
   manhattan_distance, cosine, dot}` (case-insensitively) — but note the *original* casing is
   left in the node (unlike `quantization`/`creation_mode`, which are rewritten normalized);
   every downstream consumer lowercases again. `quantization` defaults to `"SQ8"` (allowed:
   SQ4/SQ8); `epsilon` defaults 0.25; `cross_pollination_m` defaults 1 (cap 1024);
   `rng_factor` defaults 1.0.
2. `VTreeResourceFactoryProvider#getResourceFactory`
   (`asterixdb/asterix-metadata/.../utils/VTreeResourceFactoryProvider.java` ~line 177) reads
   `similarity` off the WITH node, instantiates `new VectorDistanceFunctionFactory()` and
   `new AOrderedListVectorBinaryAccessorFactory()`, reads the cross-pollination triple **with
   the same defaults as the bulk-load job** (in-code comment: drift here ⇒ leaked deletes —
   [dml.md §3.2](dml.md)), and hands everything to `LSMVTreeLocalResourceFactory`.
3. `LSMVTreeLocalResource` (`hyracks-storage-am-lsm-vtree/.../dataflow/`) persists to JSON:
   `distanceMetric` (string), `distanceFunctionFactory` + `vectorAccessorFactory` (registry
   class identifiers — both classes are registered in
   `asterix-app/.../io/PersistedResourceRegistry.java` ~lines 167–174), cross-pollination keys
   (only when M > 1), and the six quantization keys (`minQuantile`, `maxQuantile`, `alpha`,
   `bits`, `confidenceInterval`, `sampleCount`) — full key table in
   [index-instance-anatomy.md §5](index-instance-anatomy.md).
4. **Quantization params arrive later, from Job 1**: the resource implements
   `IQuantizedResource` (`hyracks-storage-am-common/.../api/IQuantizedResource.java`);
   `QuantizedIndexCreateOperatorDescriptor` (`hyracks-storage-am-lsm-vtree/.../dataflow/`)
   receives the single aggregate tuple from `QuantizationConstantsAggregate`, unpacks it in
   `extractQuantizationParams`, and calls
   `IndexBuilder#setQuantizationParameters` → resource `setQuantizationParameters(Map)` before
   the resource file is written. The DDL-time resource has null quantization fields until then.
5. `LSMVTreeLocalResource#createInstance` null-checks both factories (missing ⇒ "corrupted
   resource" `HyracksDataException`), packs the `float[6]`
   `{minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount}` iff
   `hasQuantizationParams()`, and calls `LSMVTreeUtils.createLSMTree` →
   `LSMVTree`/`VTreeFactory` → **`VTree` fields**: `quantizationParams` (float[6] or null =
   non-quantized), `distanceMetric`, `distanceFunctionFactory`, and the **eagerly created**
   `distanceFunction` (`VTree` ctor line ~138:
   `distanceFunctionFactory.createDistanceFunction(distanceMetric)`) used by all DML routing.

### (b) Query-time IAP injection

`MetadataProvider#getVectorSearchRuntime`
(`asterixdb/asterix-metadata/.../declared/MetadataProvider.java` ~line 825) constructs — fresh,
not from the resource — `AOrderedListVectorBinaryAccessorFactory`,
`VectorDistanceFunctionFactory`, and `OptimizedScalarQuantizerFactory`, plus epsilon
(WITH-clause, default 0.3 here), `kMultiplier` (session config), quantized-vs-not field counts
(`VTreeDataTupleConstants.Q_NUM_SECONDARY_FIELDS`), and builds
`VectorSearchOperatorDescriptor`. At runtime,
`VectorSearchOperatorNodePushable#addAdditionalIndexAccessorParams`
(`hyracks-storage-am-lsm-vtree/.../dataflow/`, ~line 340) puts into the IAP:

- `IVTreeBinaryAccessorFactory.IAP_KEY` — query-vector extraction;
- `IVTreeDistanceFunctionFactory.IAP_KEY` — distance factory;
- `IVTreeQuantizerFactory.IAP_KEY` — quantizer factory (skipped if null);
- `HYRACKS_TASK_CONTEXT` — for the spillable top-K buffer;
- `LSMVTreeTopKSearchCursor.IAP_KEY = TRUE` — cursor selection
  ([search-cursors.md §1.2/§2](../40-query-path/search-cursors.md)).

Consumption is in `VTree.VTreeAccessor` (~lines 1185–1252): metric string comes **from the
predicate** (`VTreeSearchPredicate#getDistanceMetric`, which the optimizer normalized — §5
invariant 4) with fallback to the tree's persisted metric; the factory comes from the IAP with
fallback to the tree's persisted factory; then
`distanceFunction = factory.createDistanceFunction(metric)` per operation. The quantizer is
built **lazily per query**: iff `tree.getQuantizationParams() != null`,
`quantizerFactory.createQuantizer(metric, dims, float[6])`, then
`quantizedQueryVector = quantizer.quantize(queryVector)` (encode-decode roundtrip) — both
stored on `VTreeCursorInitialState`. Test fallback: a pre-built `IVTreeQuantizer` under
`IVTreeQuantizer.IAP_KEY` (how `NoOpVectorQuantizer` gets in).

### (c) Job-time constructor params (creation pipeline)

`SecondaryVectorOperationsHelper` (`asterixdb/asterix-metadata/.../utils/`, ~lines 454–682)
reads `similarity` off the WITH node (`getOptionalString("similarity", "")`) and passes the
**string** into the constructors of `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor`,
`SpannTopDownCentroidsOperatorDescriptor`, `VTreeStaticStructureCreatorOperatorDescriptor`, and
`VTreeBulkLoaderAndGroupingOperatorDescriptor`. Each resolves it against its private map (§2.4)
or `fromDistanceMetric` (quantizer side). The bulk-load op additionally re-reads the persisted
`float[6]`-equivalents from the `LSMVTreeLocalResource` at open()
(`readQuantizationParamsFromMetadata`) and builds its own `ScalarVectorQuantizer`; the
static-structure creator does the same to quantize **leaf centroids**
(`createLeafTupleWithQuantization`).

### (d) Defaults and fallbacks (complete list)

| Site | Condition | Default |
|---|---|---|
| `VectorDistanceFunctionFactory#createDistanceFunction` | unknown metric | WARN + euclidean |
| k-means op `getDistanceFunctionDouble` | null/empty metric | EuclideanSquared |
| SPANN op ctor | null/empty metric | `"l2_squared"` string → EuclideanSquared |
| `LSMVTreeLocalResource#fromJson` | missing `distanceMetric` key (pre-fix resource) | `LEGACY_DEFAULT_DISTANCE_METRIC = "euclidean"` |
| `LSMVTreeLocalResource#createInstance` | null `distanceMetric` field | same legacy euclidean |
| `VTree` ctor | null metric arg | `"euclidean"` |
| `VectorIndexAccessMethod#normalizeDistanceMetric` | null/empty query metric | `"euclidean"` |
| `OptimizedScalarQuantizationSampleFile#fromDistanceMetric` | unknown/empty | **`DOT_PRODUCT`** (label only while corrective is disabled) |
| bulk-load op `readQuantizationParamsFromMetadata` | resource unreadable | hardcoded fallback Params (8 bits, ±10 quantiles, α=6.35) — masks a broken resource with silently wrong quantization |
| `LSMVTreeLocalResource#fromJson` cross-pollination | missing keys | `CrossPollinationConfig.LEGACY` (M=1); `rngFactor` 1.0, `epsilon` 0.3 |

---

## 4. Use-site inventory (review-critical)

Every place a distance or a quantizer is *applied*. "FP" = full-precision `double[]`;
"RQ" = reconstructed-quantized (encode→decode double[], §2.5). All comparisons listed are
FP-vs-FP or RQ-vs-RQ unless flagged.

| # | Site (file → method) | Function/quantizer instance | Operands | Semantics / notes |
|---|---|---|---|---|
| 1 | k-means training — `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor#calculateDistance` (used by k-means‖ candidate weighting, Lloyd's assignment, hierarchy levels) | op-local map from ctor metric string | FP record ↔ FP centroid | smaller-is-better everywhere incl. −dot; cosine triggers spherical normalization of centroids; `clipCentroid` ±1e3 applied to outputs |
| 2 | SPANN BKT / SelectHead scoring — `SpannTopDownCentroidsOperatorDescriptor#kmeansAssign` (~line 1979) | op-local map | FP ↔ FP | **lambda-balanced**: `score = dist + λ·priorCounts[c]`; λ from `lambdaFactor` knob or `DynamicFactorSelect` (negative = auto). λ is an additive count penalty on top of the metric — for dot (negative distances) the balance pressure is relatively stronger; nobody has audited that |
| 3 | Bulk-load routing — `VTreeBulkLoaderAndGroupingOperatorDescriptor#nextFrame` → `VTree.VTreeAccessor#findCloseCentroidsLevelWiseGlobalSort` | op-local map (`hyracksDistanceFunction`) | FP embedding ↔ FP leaf/interior centroids | epsilon-windowed level-wise probe ([navigation.md §3.4](../40-query-path/navigation.md)); result's `.distance` becomes **stored field-0** |
| 4 | Bulk-load qDist — same method, ~line 698 | same map + op-local `ScalarVectorQuantizer` | RQ embedding ↔ RQ centroid (`quantizer.quantize` both) | fills field-2 `quantized_distance` when navigation didn't provide one — one of the three field-2 semantics ([bug-archive: field-2 OPEN](../60-quality/bug-archive.md)) |
| 5 | RNG diversity filter — `RngAcceptanceFilter#accept` (`hyracks-storage-am-vtree/.../utils/`) | whichever function the caller routed with (bulk-load: op map; DML: tree's persisted function) | FP centroid ↔ FP centroid (+ FP record↔centroid `cand.distance` from navigation) | reject iff `rngFactor·dist(cᵢ, r) < dist(x, cᵢ)`; deliberately FP even on quantized indexes ("diversity should reflect geometric truth"); defensive NaN handling = never veto |
| 6 | DML insert routing + stored distance — `VTree#insertVector` (~line 213) / `deleteVector` (~line 522) | **tree's eager `distanceFunction`** (persisted metric) | FP vector ↔ FP `clusterResult.centroid` | per-replica `distance = d(v, c_replica)` becomes stored field-0 (and the search key for physical delete / antimatter placement — [dml.md §3.4–3.5](dml.md)) |
| 7 | DML data-tuple quantized fields — `VTreeDataTupleCreator#writeQuantizedFields` | hand-inlined encode (§2.5 ⚠️) | encodes FP vector | field-2 written as **the FP field-0 distance again** (second of the three semantics); field-3 = encoded bytes |
| 8 | In-tree navigation — `VTreeNavigationUtils` (greedy descent, iterative DFS, level-wise; ~lines 203, 276) | caller-supplied function | FP query ↔ FP centroid (interior + leaf) | **navigation never ranks on quantized values**; leaf pages carry both FP centroid and quantized bytes, but the quantized bytes feed only the optional side-channel ↓ |
| 9 | Navigation quantized side-channel — `VTreeNavigationUtils` ~line 280 | same function + query's quantizer | RQ query ↔ RQ leaf centroid (`dequantize(getQuantizedCentroidBytes(i))`) | fills `ClusterSearchResult.quantizedDistance` (NaN if quantizer null). At **query** time it *is* null — `NprobeClusterSelectionStrategy#setQuantizer` has no callers — so query-time results carry NaN here; harmless, because dqx is recomputed per tuple (site 11). Bulk-load passes null too and patches qDist afterwards (site 4) |
| 10 | Static-structure leaf quantization — `VTreeStaticStructureCreatorOperatorDescriptor#createLeafTupleWithQuantization` | canonical OSQ `quantizeVector` | encodes FP trained centroid | writes the quantized-centroid bytes that site 9 later dequantizes; params read from the resource (Job 1 output), similarity via `fromDistanceMetric` |
| 11 | Top-K cursor dqx — `LSMVTreeTopKSearchCursor#computeApproximateDistance` (~line 608) | distance function + quantizer propagated from the first component cursor's initial state | RQ query (`quantizedQueryVector`) ↔ RQ stored embedding (`dequantize(field 3)`) | the ranking key of production ANN: inserted into `SpillableTopKBuffer`, drain sorts ascending, exposed as `getCurrentDistance()` for index-only output ([search-cursors.md §3.6](../40-query-path/search-cursors.md)). Consistent RQ-vs-RQ. Replicas of one record share identical field-3 bytes ⇒ identical dqx (what makes adjacency/DISTINCT reconciliation safe) |
| 12 | ORDER BY `ann_distance` exact rerank (lookup-and-rerank plan shape) | `ANNDistanceDescriptor` map | FP stored record vector ↔ FP query constant | exact FP rerank of the index's RQ-preselected candidates; sign convention matches the index because both maps negate dot ([optimizer.md §2](../40-query-path/optimizer.md)) |
| 13 | Directory (metadata-page) maxDistance bookkeeping — `VTree#forceUpdateMetadataMaxDistance` / `updateMetadataWithNewDataPage`; consulted by `findDataPageInMetadataPage` on every insert/delete | none applied — stores/compares **field-0 FP distances** | FP scalar thresholds | sorted-ascending invariant (the fixed unsorted-append bug — [bug-archive](../60-quality/bug-archive.md), [dml.md §3.4](dml.md)). Mixing: routing distance (FP) also selects the page bucket for *antimatter* placement; the top-K read side ranks by RQ dqx — the FP/RQ gap is why reconciliation is keyed on stored FP field-0 + PK, not on dqx |

**Where semantics could drift (the mixed-space edges):** sites 4/9/11 are the only
quantized-space computations, and all three are RQ-vs-RQ with the *same* quantizer params —
safe. The structural hazards are (i) field-2, which mixes FP (DML write) and RQ (bulk-load
write) values in one column — currently unread, but any future reader inherits the mix; and
(ii) any future "prune data pages by dqx vs directory maxDistance" optimization, which would
compare an RQ quantity against FP bookkeeping.

---

## 5. Invariants & conventions — the review checklist

1. **Smaller-is-more-similar, everywhere.** Hence dot product is negated in **all five**
   distance maps (canonical factory, ANNDistanceDescriptor, three operator copies). Historical
   precedent: the evaluator once mapped `"dot"` to raw `+dot` while the index used `−dot` —
   quantized dot recall was 0.00 (fixed on the pre-rename branch; the negation now lives in
   `ANNDistanceDescriptor.DISTANCE_MAP`). *Reviewer:* any new map entry or new metric must
   return values where ascending sort = most-similar-first; grep for `-VectorDistanceCalculation`
   to compare all copies.
2. **⚠️ OPEN sign hazard — `dot-product()` as an ORDER BY key.** `DotProductDescriptor` is raw
   `+dot`, yet `VectorIndexAccessMethod` accepts `BuiltinFunctions.DOT_PRODUCT` for
   index-driven KNN (quantized indexes only, ~line 610). The index preselects by `−dot`
   (most similar); a surviving `ORDER BY dot_product(...) ASC` rerank then puts the *least*
   similar candidates first — the same inversion class as invariant 1's historical bug.
   *Reviewer:* before enabling/using this path, pin the intended semantics with a runtimets
   case; either negate at plan time or reject `DOT_PRODUCT` as an ORDER BY access-method
   trigger.
3. **⚠️ Ghost metric `manhattan_distance`.** Allowed by `VectorIndexDeclUtil`, normalized to
   `"manhattan"` by `VectorIndexAccessMethod#normalizeDistanceMetric` (which also accepts
   `"l1"`), but implemented by **no map anywhere**. Creating such an index passes DDL
   validation, then Job 2 dies in the k-means op with `IllegalArgumentException: Unsupported
   distance function`. *Reviewer:* either implement manhattan in `VectorDistanceCalculation` +
   all five maps, or drop it from the DeclUtil allowed set and the normalizer.
4. **Index metric == query metric, enforced at plan time.** Both sides pass through
   `VectorIndexAccessMethod#normalizeDistanceMetric` (`getIndexDistanceMetric` normalizes the
   WITH value; the rule normalizes the `ann_distance` arg2) before the equality check in index
   selection; mismatch ⇒ fall back to full-scan KNN with a WARN
   ([optimizer.md §1.4–1.5](../40-query-path/optimizer.md)). The runtime therefore never mixes
   metrics between predicate and tree — the `VTreeAccessor` predicate-metric-overrides-tree
   fallback chain (§3b) is belt-and-braces, not a correctness mechanism. *Reviewer:* new
   aliases must be added to the normalizer too, or exact-match index selection silently
   degrades to full scan.
5. **Epsilon threshold is uniformly multiplicative:** `epsilonThreshold(d, ε) = d + |d|·ε`
   (`VTreeNavigationUtils` ~line 552), i.e. `(1+ε)d` for positive and `(1−ε)d` for negative
   (−dot) distances — the fix for the "additive epsilon is a no-op window for dot" recall bug
   ([navigation.md §3.4](../40-query-path/navigation.md)). *Reviewer:* any new pruning window
   must handle negative distances; never reintroduce `d + ε`.
6. **Quantization params are trained once (Job 1) and immutable afterwards.** `Params` fields
   are final; the resource's setters are only invoked by `QuantizedIndexCreateOperatorDescriptor`
   before the resource file is written. Every later consumer (static-structure creator,
   bulk-load op, DML tuple creator, query quantizer) reconstructs from the same persisted six
   values — so encode at load time and decode at query time cannot disagree. *Reviewer:* any
   path that re-derives params (e.g. the bulk-load op's hardcoded fallback Params, §3d) breaks
   this and must fail loudly instead.
7. **Replicas share the quantized embedding.** Cross-pollination writes M copies of the same
   record; field-3 bytes are identical across replicas (deterministic encode of the same FP
   vector with the same params), hence identical dqx (site 11) — the precondition for
   distance+PK adjacency reconciliation and for index-only DISTINCT dedup being lossless.
   *Reviewer:* anything that makes the stored embedding replica-dependent (e.g. per-cluster
   residual encoding) breaks reconciliation and the k_multiplier replica-budget contract.
8. **Dequantization must apply the inverse mapping** (`q/α + minQuantile`), not a cast —
   euclidean ranking survives a cast, dot ranking does not (§2.5; historical bug). *Reviewer:*
   round-trip test `dequantize(quantize(x)) ≈ x` on any change to either side, and remember
   `VTreeDataTupleCreator` holds a third, hand-inlined encode copy.
9. **Known open quirks a patch may collide with:**
   - **field-2 `quantized_distance`: three write semantics, zero readers**
     ([bug-archive, OPEN LOW](../60-quality/bug-archive.md)) — bulk load writes RQ distance,
     DML duplicates FP field-0, navigation fallback writes 0.0. Do not add a reader without
     first unifying the writers.
   - **`setQuantizer` never called** on `NprobeClusterSelectionStrategy` ⇒ query-time
     `ClusterSearchResult.quantizedDistance` is always NaN (site 9). Harmless today; a trap
     for anyone who starts consuming that field.
   - **`clipCentroid` clamps to ±1e3 silently** in both training operators — embeddings with
     legitimately larger coordinates get distorted centroids with no warning.
   - SPANN-specific: `num_clusters` ignored when selecthead is enabled; 0 heads hard-fails
     Job 2 by design (memory: SPANN op review).

---

## 6. Review guide — five questions for any distance/quantization patch

1. **Sign:** does every value the patch produces or consumes obey smaller-is-more-similar? If
   it touches dot product anywhere, trace the sign end-to-end (training → routing → stored
   field-0 → dqx → rerank) and check invariant 2's `dot-product()` hazard.
2. **All the maps:** a new metric or alias must land in **seven places** —
   `VectorIndexDeclUtil.ALLOWED_VECTOR_DISTANCE_METRICS`, `ANNDistanceDescriptor.DISTANCE_MAP`,
   `VectorDistanceFunctionFactory.DISTANCE_MAP`, the three operator-local maps
   (`HierarchicalKMeans…`, `SpannTopDown…`, `VTreeBulkLoaderAndGrouping…`), and
   `VectorIndexAccessMethod#normalizeDistanceMetric` — plus, if quantization behavior depends
   on it, `OptimizedScalarQuantizationSampleFile#fromDistanceMetric`. Missing any one gives a
   different failure mode per site (§2.4).
3. **Normalization:** does the metric need vector preprocessing like cosine (L2-normalized
   inputs, spherical k-means centroid renorm)? If so: `requiresNormalizedCentroids()` in *both*
   training ops, the OSQ cosine contract (callers normalize, engine never does), and user-facing
   docs.
4. **Write-path consistency:** are bulk-load (site 3/4) and DML (site 6/7) still storing the
   *same* field-0 semantics (FP distance to the owning replica's centroid, same metric) and
   compatible field-2/3 bytes? Directory maxDistance ordering (site 13) and antimatter
   distance-keyed lookup both assume it. Run the flush/merge + delete integration parts, not
   just unit tests.
5. **Persistence & registry:** new factory class or new JSON key ⇒ register the class in
   `PersistedResourceRegistry`, bump/handle `LSMVTreeLocalResource` serialVersionUID + fromJson
   back-compat default, and keep the `float[6]` layout in lock-step between
   `LSMVTreeLocalResource#createInstance`, `OptimizedScalarQuantizerFactory`, and
   `VTreeDataTupleCreator#quantizeVector`. A restarted NC must reconstruct byte-identical
   behavior from the `.metadata` file alone.
