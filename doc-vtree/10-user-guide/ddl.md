# DDL — creating a VTree vector index

> **Status:** current
> **Verified against:** `9c5fd8c704` (2026-07-07)
> **Scope:** the complete creation-time user surface — `CREATE INDEX ... TYPE VTREE` grammar,
> every WITH parameter, the ANALYZE prerequisite, and the creation-time compiler knobs.

Companion files: [querying.md](querying.md) (query-time surface) and
[operations.md](operations.md) (DML + lifecycle). For what actually happens under the hood
when the statement runs, see [20-creation-pipeline/overview.md](../20-creation-pipeline/overview.md).

## 1. Prerequisite — ANALYZE DATASET

A vector index **cannot be created until the dataset has a sample index**. Two build steps
depend on it: quantization training (Job 1) scans the sample, and the training-sample sizing
(Job 2) needs the dataset cardinality that ANALYZE records. Without it, CREATE INDEX fails
with *"Vector Index requires ANALYZE statement prior to CREATE INDEX DDL."*
(`SecondaryVectorOperationsHelper#buildStaticStructureJobSpec`,
`asterixdb/asterix-metadata/.../utils/SecondaryVectorOperationsHelper.java`).

```sql
ANALYZE DATASET test.MovieSmall WITH {"sample-seed": 1000};
```

Options (`AnalyzeStatement`, `asterixdb/asterix-lang-common/.../statement/AnalyzeStatement.java`):

| option | type | default | notes |
|---|---|---|---|
| `sample` | `"low"`\|`"medium"`\|`"high"` or int | `"low"` (1063) | medium = 4252, high = 68032; int must be in [1063, 272128] |
| `sample-seed` | int (or numeric string) | `nanoTime`-derived | fixes the sample for reproducible index builds |
| `full-scan` | boolean | `false` | sample from a full scan instead |

Set `sample-seed` whenever you want run-to-run reproducible index creation (pair it with
`compiler.vector.trainseed`, §5). Note that ANALYZE captures cardinality *at that moment*;
re-run it after large loads so `train_list_fraction` sizing is based on current data.

## 2. Statement syntax

```sql
CREATE INDEX <index-name> ON <dataset>(<field> VECTOR)
    [ INCLUDE (<field1>, <field2>, ...) ]
    TYPE VTREE
    WITH { "dimension": <int>, "similarity": <string>, ... }
    EXCLUDE UNKNOWN KEY;
```

Note the clause order: **`INCLUDE(...)` comes between the key list and `TYPE VTREE`** (the
grammar parses it right after the indexed-element list — `SQLPP.jj` `CreateIndexStatement`
production, `asterixdb/asterix-lang-sqlpp/src/main/javacc/SQLPP.jj` ~line 1608).

Grammar-level constraints (all enforced by the parser, `SQLPP.jj` ~lines 1655–1728):

| constraint | error if violated |
|---|---|
| Exactly **one** indexed field | "Vector index requires exactly one field" |
| The field must carry the `VECTOR` annotation, and `VECTOR` requires `TYPE VTREE` | "VECTOR field annotation requires TYPE VTREE" |
| `TYPE VTREE` requires the `VECTOR` annotation | "TYPE VTREE requires VECTOR field annotation" |
| `WITH {...}` is **mandatory** | "Vector index requires WITH clause specifying index configuration" |
| `EXCLUDE UNKNOWN KEY` is **mandatory** (INCLUDE UNKNOWN is rejected too) | "Vector index requires EXCLUDE UNKNOWN KEY" |
| `INCLUDE(fields)` is **VTREE-only** — using it on any other index type fails | "INCLUDE clause is only supported for vector indexes (TYPE VTREE)" |

`INCLUDE(...)` stores extra scalar fields inside each index entry so that WHERE predicates
on them can be evaluated inside the index search without fetching the record
(see [querying.md §6](querying.md) for how the filter pushdown behaves — including one open
bug in the index-only shape).

## 3. WITH parameters

Validated by `VectorIndexDeclUtil#validateAndGetWithObjectNode`
(`asterixdb/asterix-lang-common/.../util/VectorIndexDeclUtil.java`). Any key outside this
table is a compile error ("Unknown field \`x\` in WITH clause").

| parameter | type | required | default | valid range / values |
|---|---|---|---|---|
| `dimension` | int | **yes** | — | > 0 (int literal only; a double is rejected) |
| `similarity` | string | **yes** | — | `euclidean`, `l2`, `euclidean_squared`, `l2_squared`, `cosine`, `dot`, `manhattan_distance` (⚠ ghost, see below); case-insensitive |
| `quantization` | string | no | `"SQ8"` | `SQ4` or `SQ8`, case-insensitive, stored uppercase |
| `train_list_fraction` | double (int coerced) | no | `0.1` | `(0, 1]` |
| `epsilon` | double (int coerced) | no | `0.25` | `[0, 1]` |
| `num_clusters` | int | no | `sqrt(cardinality / numPartitions)` | ⚠ **not validated at DDL time** (see below) |
| `creation_mode` | string | no | `"bottom-up"` | `bottom-up` or `top-down`, case-insensitive; blank ⇒ default |
| `cross_pollination_m` | int | no | `1` | `[1, 1024]` |
| `rng_factor` | double (int coerced) | no | `1.0` | positive finite |

### `dimension`
Vector length. Every indexed value must be an ordered list of numbers of exactly this
length. No runtime resize — pick it to match your embedding model.

### `similarity`
The distance metric baked into the index (used for training, storage ordering, and search).
The ANN query's third argument must match it, or the index is silently skipped
([querying.md §4](querying.md)). Aliases: `l2` ≡ `euclidean`, `l2_squared` ≡
`euclidean_squared`. `dot` is stored/searched as **negated** dot product (smaller = more
similar) — see [30-storage-engine/distance-and-quantization.md](../30-storage-engine/distance-and-quantization.md).

⚠ **Ghost value `manhattan_distance`:** it is in the DDL allowlist
(`VectorIndexDeclUtil.ALLOWED_VECTOR_DISTANCE_METRICS`) but *not* in the error message, and —
more importantly — **no runtime supports it**. CREATE INDEX passes DDL validation, then
**Job 2 hard-fails** building the training operator
(`HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor#getDistanceFunctionDouble` throws
`IllegalArgumentException: Unsupported distance function`). Do not use it. The metric maps
that would need to learn it are inventoried in
[distance-and-quantization.md](../30-storage-engine/distance-and-quantization.md).

⚠ **Cosine requires pre-normalized embeddings:** the engine does **not** L2-normalize —
with `similarity: "cosine"` you must insert unit-length vectors yourself (comment in
`SecondaryVectorOperationsHelper#buildStaticStructureJobSpec`).

### `quantization`
Scalar quantization for stored vectors: `SQ8` (8-bit, default) or `SQ4` (4-bit). There is
**no unquantized option** — every VTree index is quantized at this release
(the DDL injects `SQ8` when the key is omitted; see
[operations.md §5](operations.md) limitations).

### `train_list_fraction`
Fraction of the dataset sampled to train the clustering (Job 2). The requested size
`cardinality × fraction` is then **clamped to [10 000, 1 000 000]** and capped at the
dataset cardinality (`SecondaryVectorOperationsHelper#clampTrainListSampleSize`).
Consequences:

- **cardinality < 10 000** → the clamp can't reach 10 000, so the build **falls back to a
  full scan**: every record is used for training (this is why small runtimets datasets use
  `train_list_fraction: 1.0` and still get deterministic structures).
- **fraction too small for a big dataset** → silently raised to 10 000 samples.
- **very large datasets** → capped at 1 000 000 samples no matter the fraction.

### `epsilon`
A *query-time* recall knob that happens to be declared at creation time: it widens the
cluster-candidate window during navigation (`d ≤ closest + |closest|·ε` — see
[40-query-path/navigation.md](../40-query-path/navigation.md)). Declared per-index in the
DDL, read back at query compile time by `MetadataProvider#getVectorSearchRuntime` and passed
into every search against this index. DDL default is `0.25`. (Trivia: the code fallback when
the WITH object is somehow absent is `0.3`, and the storage-layer predicate default is also
`0.3` — unreachable in practice since DDL always persists a value.)

### `num_clusters`
Target number of leaf clusters **per storage partition** for the bottom-up (k-means) build.
Default: `sqrt(cardinality / numPartitions)`.

- ⚠ **Not validated at DDL time** — `VectorIndexDeclUtil` has no validator for it. A wrong
  type (e.g. a string) is *silently ignored* at build time (`getOptionalInt` falls back to
  the default); a nonsensical int (0, negative) flows straight into the k-means operator.
- ⚠ **Ignored under top-down + SelectHead** (the default top-down configuration): when
  `creation_mode: "top-down"` and `compiler.vector.selecthead.enabled` is true (its
  default), the number of heads is decided by SelectHead/BuildHead (leaf-page fit), and the
  build logs *"WITH num_clusters=… is ignored"*. It is only honored top-down when SelectHead
  is explicitly disabled.
- Known related defect: the bottom-up build currently produces ~`sqrt(K)` leaves instead of
  `K` — see [bug-archive.md](../60-quality/bug-archive.md) "K trained leaf centroids
  dropped" (OPEN).

### `creation_mode`
Which **training algorithm** builds the static structure in Job 2 — not the page layout
(page emission is always bottom-up regardless):

- `"bottom-up"` (default) — hierarchical k-means++, leaves trained first
  (`HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor`).
- `"top-down"` — SPANN-style SelectHead + BKT recursive splitting
  (`SpannTopDownCentroidsOperatorDescriptor`), tunable via the `compiler.vector.selecthead.*`
  and `compiler.vector.topdown.*` knobs (§5).

### `cross_pollination_m`
Each record is written into its **M closest leaf clusters** (M = 1 disables replication).
Boosts recall for boundary vectors at the cost of index size and — importantly — of the
query-time candidate budget:

⚠ **The k_multiplier headroom contract:** replicas of the same record each consume a slot in
the top-K candidate buffer *before* deduplication. With `cross_pollination_m: M`, size the
query's `k_multiplier` (5th `ann_distance` argument, or `compiler.vector.kmultiplier`) with
replica headroom — worst case ×M — or results can silently fall short of K distinct rows.
This is a deliberate, documented contract rather than a fixed bug: see
[bug-archive.md](../60-quality/bug-archive.md) "Top-K budget consumed by cross-pollination
replicas — DEFERRED BY DESIGN (2026-07-07)" and [querying.md §7](querying.md).

DML (insert/delete) replicates into the same M clusters as bulk load — see
[operations.md §2](operations.md).

### `rng_factor`
SPTAG-style RNG (relative neighborhood graph) diversity filter applied on top of the
cross-pollination candidate list at load time: candidate cluster `c` is rejected iff some
already-accepted replica `r` satisfies `rng_factor · dist(c, r) < dist(x, c)`. `1.0` is the
canonical SPTAG rule; larger values loosen it (more replicas survive), and a very large
value effectively disables RNG (pure top-M slice). Irrelevant when `cross_pollination_m` is 1.

## 4. What the statement runs

`CREATE INDEX ... TYPE VTREE` executes **three Hyracks jobs** (not the usual two): Job 1
trains quantization parameters from the ANALYZE sample, Job 2 samples/scans the dataset,
trains the clustering (per `creation_mode`), and persists the static structure, Job 3 scans
the dataset and bulk-loads all vectors into the first disk component. Full dataflow:
[20-creation-pipeline/overview.md](../20-creation-pipeline/overview.md).

## 5. Creation-time compiler knobs (`SET` before `CREATE INDEX`)

These are read from the request config by `SecondaryVectorOperationsHelper` when building
Job 2. Keys are declared in `CompilerProperties`
(`asterixdb/asterix-common/.../config/CompilerProperties.java` ~lines 286–302). Invalid
values never fail the statement — they log a WARN and fall back to the default.

| knob | type | default | effect |
|---|---|---|---|
| `compiler.vector.trainseed` | long | fresh `nanoTime` | Seeds the training RNG for **both** creation modes (bottom-up k-means and top-down SelectHead+BKT, with per-partition/per-phase offsets). Set it for reproducible index builds. |
| `compiler.vector.selecthead.enabled` | boolean | `true` | Top-down only: run SPANN SelectHead + BuildHead (BKT routing tree over selected head vectors). When true, `num_clusters` is ignored (§3). |
| `compiler.vector.selecthead.headRatio` | double `(0,1]` | `0.12` | Fraction of the training sample selected as heads (per partition). |
| `compiler.vector.selecthead.headCount` | int > 0 | unset | Absolute head count; when set it overrides `headRatio` (converted to an effective ratio per partition). |
| `compiler.vector.selecthead.selectType` | `"bkt"` \| `"random"` | `"bkt"` | Head-selection strategy: BKT-tree-guided or uniform random. |
| `compiler.vector.selecthead.bktLeafSize` | int > 0 | unset | Scratch-BKT leaf stop threshold; when unset, the page-derived leaf capacity is used. |
| `compiler.vector.topdown.lambdaFactor` | double > 0 | `-1` (auto-tune) | BKT split balance factor; ≤ 0 or unset means auto-tuned per partition. |
| `compiler.vector.topdown.maxlevel` | int ≥ 0 | `5` | Maximum BKT recursion depth for the top-down build. |
| `compiler.vector.topdown.v` | — | — | ⚠ `@deprecated`, **dead** — declared but never read (FSCL path removed). |
| `compiler.vector.topdown.gamma` | — | — | ⚠ `@deprecated`, **dead** — declared but never read; use `lambdaFactor`. |

⚠ **Registration quirk:** none of these creation-time knobs is in the compiler's
SET-allowlist (`SqlppCompilationProvider#getCompilerOptions` registers only
`compiler.vector.prunedsearch` and `compiler.vector.kmultiplier`). They work with
`CREATE INDEX` because DDL statements never pass through `APIFramework#validateConfig` —
but a request that ALSO compiles a query (e.g. a `SELECT` in the same submission) will fail
with *"unsupported query parameter"* for the whole request. Keep `SET` +
`CREATE INDEX` in their own request.

⚠ **Sampling is not covered by `trainseed`:** when Job 2 uses a *sample* scan (dataset ≥
10 000 rows and fraction < 1 covers less), the sample-selection seed is
`System.currentTimeMillis()` — the code that would read a `sample_seed` WITH key is dead
because `VectorIndexDeclUtil` rejects that key. Fully deterministic builds therefore
currently require the full-scan training regime (small dataset or clamp-induced full scan)
plus `trainseed` — and a seeded `ANALYZE`.

## 6. Worked examples

Minimal (defaults: SQ8, fraction 0.1, epsilon 0.25, bottom-up, M=1):

```sql
ANALYZE DATASET test.Movie WITH {"sample-seed": 1000};

USE test;
CREATE INDEX idx_emb ON Movie(embedding VECTOR)
    TYPE VTREE
    WITH { "dimension": 768, "similarity": "cosine" }
    EXCLUDE UNKNOWN KEY;
```

Full-option bottom-up, with INCLUDE fields for filter pushdown and reproducible training:

```sql
ANALYZE DATASET test.Movie WITH {"sample-seed": 1000};

USE test;
SET `compiler.vector.trainseed` "42";
CREATE INDEX idx_emb ON Movie(embedding VECTOR) INCLUDE (year, popularity)
    TYPE VTREE
    WITH {
        "dimension": 768,
        "similarity": "euclidean",
        "quantization": "SQ8",
        "train_list_fraction": 0.2,
        "epsilon": 0.25,
        "num_clusters": 1000,
        "creation_mode": "bottom-up",
        "cross_pollination_m": 3,
        "rng_factor": 1.0
    }
    EXCLUDE UNKNOWN KEY;
```

Top-down (SPANN) build with SelectHead tuning:

```sql
ANALYZE DATASET test.Movie WITH {"sample-seed": 1000};

USE test;
SET `compiler.vector.trainseed` "42";
SET `compiler.vector.selecthead.headRatio` "0.10";
SET `compiler.vector.topdown.maxlevel` "6";
CREATE INDEX idx_emb_td ON Movie(embedding VECTOR)
    TYPE VTREE
    WITH { "dimension": 768, "similarity": "euclidean", "creation_mode": "top-down" }
    EXCLUDE UNKNOWN KEY;
-- num_clusters would be ignored here (SelectHead enabled by default).
```

## Related

- [querying.md](querying.md) — how to query the index you just built
- [operations.md](operations.md) — DML, COMPACT, restart behavior, limitations
- [20-creation-pipeline/overview.md](../20-creation-pipeline/overview.md) — the three-job dataflow
- [60-quality/bug-archive.md](../60-quality/bug-archive.md) — open build-side defects
  (k-means leaf drop; top-K replica budget contract)
