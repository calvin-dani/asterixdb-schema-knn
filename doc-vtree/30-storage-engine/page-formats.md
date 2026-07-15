# VTree page formats — byte-level reference

> **Status:** current
> **Verified against:** `111bfcd146` (2026-07-05)
> **Scope:** the exact on-page byte layout of every VTree page kind — headers, slot array,
> tuple encodings (non-quantized, quantized, neighbor-list, antimatter), sort invariants,
> chaining semantics, and the index metadata page.

Companion to [index-instance-anatomy.md](index-instance-anatomy.md) (which pages live where in a
component). The frame classes are in
`hyracks-fullstack/hyracks/hyracks-storage-am-vtree/src/main/java/org/apache/hyracks/storage/am/vector/frames/`;
line numbers below refer to the verified commit. All multi-byte integers/doubles are
**big-endian** (Java `ByteBuffer`/`DataOutput` defaults).

## 1. The common page header

Every VTree page (interior, leaf, directory, data) is an NSM page: header at offset 0, tuple
data growing **up** from the header end, slot array growing **down** from the page end. The
header is three layers of inheritance:

| Offset | Size | Field | Source | Meaning |
|---|---|---|---|---|
| 0 | 4 | `tuple_count` | `ITreeIndexFrame.Constants` (`hyracks-storage-am-common/.../api/ITreeIndexFrame.java` lines 43–46) | number of live tuples / slots |
| 4 | 4 | `free_space_offset` | same | byte offset of the first free data byte; initialized to the page-header size (`TreeIndexNSMFrame#resetSpaceParams`, line 240) |
| 8 | 1 | `level` | same | see §1.1 |
| 9 | 8 | `page_lsn` | `TreeIndexNSMFrame` lines 41–44 | recovery LSN (written 0 at init) |
| 17 | 4 | `total_free_space` | same | free bytes counting reclaimable holes; init `capacity − headerSize` (line 241) |
| 21 | 1 | `flags` | same | bit 0x1 = "small", bit 0x2 = "large" (large-tuple page chains; unused by VTree frames, whose generic `split` throws — `VTreeNSMFrame` lines 106–111) |
| 22 | 4 | `cluster_id` | `VTreeNSMFrame` lines 45–48, 58–62 | **initialized to −1 and never written afterwards; read only by the debug-only `VTreeNSMFrame#printHeader`** (lines 114–118) — dead in production paths |
| 26 | 4 | `centroid_id` | `VTreeNSMFrame` lines 47–48 | **never initialized or written; read only by the debug-only `printHeader`** — otherwise pure padding, only its end offset (30) is used to place subtype fields |

Subtype extensions (each overrides `getPageHeaderSize()`):

| Page kind | Offset 30 | Offset 34 | Header size | Source |
|---|---|---|---|---|
| interior | `next_page` (int, init −1) | `overflow_flag` (1 byte, init 0) | **35** | `VTreeInteriorFrame` lines 37–56 |
| leaf | `next_leaf` (int, init −1) | `overflow_flag` (1 byte, init 0) | **35** | `VTreeLeafFrame` lines 45–63 |
| directory ("metadata frame") | `next_page` (int, init −1) | — | **34** | `VTreeMetadataFrame` lines 40–58 |
| data | `next_page` (int, init −1) | — | **34** | `VTreeDataFrame` lines 48–65 |

### 1.1 The level byte is NOT the tree depth

- Leaf, directory, and data pages are all initialized with `initBuffer((byte) 0)` → level 0
  (`VTreeStaticStructureBuilder#confiscateAndInitFrame` lines 318–334, `VTreeBulkLoader` lines
  182–188 / 275–285, `VTree#setStaticStructure` line 985).
- **Every** interior page — whatever its true height — gets level 1
  (`VTreeStaticStructureBuilder` line 330).
- The index metadata page uses level −1, free pages −2 (`LIFOMetaDataFrame` lines 42–43).

Consequences: `isLeaf()` (`level == 0`) is only meaningful for static-structure pages reached by
navigation (`VTreeNavigationUtils` line 624 dispatches on it); a level-0 page could equally be a
directory or data page — page *kind* is established by pointer provenance (which field led you
there), not by self-description. The copy loops in `VTreeBulkLoader#end` (line 499) and
`VTreeFlushLoader#copyStaticStructure` (line 151) rely on exactly this `level > 0` test to tell
interior from leaf during pointer fix-up.

## 2. Slot array

All four frames use the B-tree `OrderedSlotManager` over `AbstractSlotManager`
(`hyracks-storage-am-common/.../frames/AbstractSlotManager.java` lines 30–66):

- Slot size = **4 bytes**; a slot holds the int byte-offset of its tuple's start.
- Slot for tuple index `i` lives at `capacity − 4 − i·4` (`getSlotOff`) — i.e. slot 0 is the
  **last** 4 bytes of the page and the array grows **downward** toward the data area.
- Free space on a page is the gap between `free_space_offset` (growing up) and
  `capacity − tuple_count·4` (growing down); `hasSpaceInsert` distinguishes contiguous space
  from space that needs `compact()` (`VTreeNSMFrame` lines 90–103, `VTreeDataFrame` lines
  87–99).

## 3. Tuple encoding — the two writers

### 3.1 Static/directory tuples: `VTreeTupleWriter` (plain `TypeAwareTupleWriter`)

`VTreeTupleWriter` (`.../vector/tuples/VTreeTupleWriter.java`) adds nothing to
`TypeAwareTupleWriter` (`hyracks-storage-am-common/.../tuples/TypeAwareTupleWriter.java` lines
73–101); it serves interior, leaf, and directory frames. A written tuple is:

```
[null-flag bytes: ceil(F / 8)]
[var-len slots: one varint per NON-fixed field, in field order]
[field data: concatenated field bytes, in field order]
```

- **Null flags**: one bit per field, field `i`'s bit is bit `7 − (i mod 8)` of byte `i / 8`
  (MSB-first; `setNullFlag`, lines 185–190). With a null `nullIntrospector` (VTree's case —
  `LSMVTreeUtils` lines 122–123) the bits stay 0.
- **Var-len slots**: `VarLenIntEncoderDecoder` high-bit-continuation varints
  (`hyracks-util/.../encoding/VarLenIntEncoderDecoder.java` lines 31–45): lengths < 128 take
  1 byte; 255 encodes as `0x81 0x7F`. Fixed-length fields get **no slot** — readers compute
  their offsets from the type traits (`TypeAwareTupleReference`).
- Field count on the wire is the *tuple's* field count; the traits array is the schema. Frames
  dispatch layout variants on `frameTuple.getFieldCount()` (e.g. `VTreeLeafFrame` line 113) —
  which works only because `TypeAwareTupleReference.fieldCount` defaults to the traits length;
  a shorter-variant tuple read through a longer-schema reference is a known hazard (see
  graph-leaf-neighbors notes).

### 3.2 Data tuples: `LSMVTreeDataTupleWriter` (antimatter-aware)

`hyracks-storage-am-lsm-vtree/.../lsm/vector/tuples/LSMVTreeDataTupleWriter.java` — same
physical layout, with **one extra flag bit** in front of the field bits:

- Null-flag byte count = `ceil((F + 1) / 8)` (lines 59–70).
- **Bit 7 (0x80) of null-flag byte 0 is the antimatter bit**: writer sets it after the base
  write when `isAntimatter` (lines 73–79, `ANTIMATTER_BIT_OFFSET = 7` in
  `ITreeIndexTupleReference` line 25). Matter and antimatter tuples are byte-identical except
  this bit.
- Field `i`'s null bit shifts to adjusted index `i + 1` (`getAdjustedFieldIdx`, lines 87–90) —
  so field 0's null bit is bit 6 (0x40) of byte 0.
- `LSMVTreeDataTupleReference` mirrors the adjustment on read and exposes `isAntimatter()`
  (`.../tuples/LSMVTreeDataTupleReference.java` lines 47–67).

The matter/antimatter pair is instantiated as two `LSMVTreeDataTupleWriterFactory`s → two
`VTreeDataFrameFactory`s (insert vs delete), selected per operation by the op-context
(`LSMVTreeUtils.java` lines 142–149, `LSMVTreeOpContext#setInsertMode/setDeleteMode`).
Disk components only ever use the insert (matter-capable) factory; antimatter bits survive in
the bytes regardless of which frame reads them.

## 4. Static-structure pages

### 4.1 Interior page

Schema is fixed, owned by `VTreeInteriorFrameFactory` (lines 43–47):

| Field | Type trait | Wire encoding |
|---|---|---|
| 0 `cid` | fixed 4 (`IntegerPointable`) | raw big-endian int32, no tag |
| 1 `centroid` | var-len | `[int32 n][n × float64]` — `DoubleArraySerializerDeserializer` format (count prefix; see `VTreeNavigationUtils#extractCentroid`, lines 156–161) |
| 2 `child_page_ptr` | fixed 4 | raw int32 page id, **always the last field** (read/written via `getFieldCount()−1`, `VTreeInteriorFrame` lines 79–93) |

Tuple bytes: `[1 null-flag byte][1+ varint slot for field 1][4][4+8n][4]`. The child pointer is
appended by `VTreeStaticStructureBuilder#createEntryTuple` (lines 256–282) as a raw
`writeInt`; it is rewritten in place (same width) when a component copy offsets it
(`VTreeBulkLoader#end` lines 499–508).

**Sort order: arrival order.** `VTreeNSMFrame#insertSorted` just appends at index
`tupleCount` (lines 85–87); k-means emission order is preserved.

**Chaining**: one *cluster* of centroids per page chain. When a cluster overflows a page,
`next_page` points to the overflow page and `overflow_flag = 1`
(`VTreeStaticStructureBuilder#createOverflowPage` lines 300–313); distinct interior clusters
are unlinked (fresh page, no pointer — `transitionToNextCluster` lines 450–455). Readers must
check `overflow_flag` before following `next_page`.

### 4.2 Leaf page

Two schemas, chosen by the `quantized` flag at frame-factory construction
(`VTreeLeafFrameFactory` lines 43–81):

**Quantized (production) — 5 fields** (`TYPE_TRAITS_QUANTIZED`, lines 55–60):

| Field | Type trait | Wire encoding |
|---|---|---|
| 0 `cid` | fixed 4 | raw int32 |
| 1 `centroid` | var-len | `[int32 n][n × float64]` full-precision |
| 2 `quantized_centroid` | var-len | `[varint contentLen][contentLen bytes]` — `ByteArraySerializerDeserializer` format, one byte per dimension for SQ8 (read at `VTreeLeafFrame#getQuantizedCentroidBytes`, lines 111–124; field index pinned by `VTreeStaticTupleConstants.LEAF_QUANTIZED_BYTES_FIELD = 2`) |
| 3 `neighbor_list` | var-len | `[varint contentLen][contentLen bytes]`, content = fixed 8-byte entries (§4.2.1); **always present, possibly empty** (factory comment lines 50–53) |
| 4 `metadata_page_ptr` | fixed 4 | raw int32 → the cluster's first directory page; **last field** (`VTreeLeafFrame` lines 87–101) |

**Non-quantized (test fixtures only) — 3 fields**: `[cid, centroid, metadata_page_ptr]`
(lines 67–70). `VTreeStaticTupleConstants` (`.../vector/api/VTreeStaticTupleConstants.java`)
documents all input variants; note interior tuples and non-quantized leaf tuples are both
3 fields — the builder distinguishes them by **build level**, never by field count (its javadoc
lines 44–46).

`metadata_page_ptr` is written as sentinel **−1** by the static-structure builder
(`LEAF_METADATA_PTR_SENTINEL`, `VTreeStaticStructureBuilder` lines 86–87) and patched to the
real directory page id per centroid by `VTreeBulkLoader#end` (lines 515–525, indexed
`cid − firstLeafCentroidId`, gap-tolerant) or by `VTreeFlushLoader#copyStaticStructure` from
the memory tree's `centroidDirPageMap` (lines 161–178).

**Sort order: arrival order** (same append-only `insertSorted`).

**Chaining — `next_leaf` is dual-purpose** (the one VTree frame where the overflow flag
changes the *meaning* of the pointer, not just its validity):

- `overflow_flag = 1`: `next_leaf` → intra-cluster overflow page
  (`createOverflowPage`, lines 300–313).
- `overflow_flag = 0`, `next_leaf ≥ 0`: **sibling link** to the next leaf cluster's first page
  (`transitionToNextCluster`, lines 440–449) — this is what makes the whole leaf level a
  scannable chain.
- `next_leaf = −1`: last leaf page of the level.

#### 4.2.1 Neighbor-list entry format

`VTreeLeafNeighborList` (`.../vector/utils/VTreeLeafNeighborList.java`): the field content is
`k` fixed **8-byte** entries (`ENTRY_SIZE = 8`, line 39), no count header — `k = contentLen/8`.
An entry is two int32s whose interpretation flips atomically:

| State | int 0 | int 1 |
|---|---|---|
| provisional | neighbor **centroid id** | `SENTINEL = Integer.MIN_VALUE` (line 45) |
| resolved | leaf **page id** | **slot** (tuple index in that page, always ≥ 0) |

Same width in both states, so the resolution pass overwrites in place without moving slots
(`writeResolved`, lines 99–103). Resolution happens in the builder's leaf pass
(`resolveAndUploadLeafNeighbors`) in scaffold page space; component copies then just *offset*
the page id by the copy base (`VTreeBulkLoader#resolveLeafNeighborPointers` lines 580–621,
`VTreeFlushLoader#offsetLeafNeighborPointers` lines 201–223).

## 5. Directory page (`VTreeMetadataFrame`)

The per-cluster distance directory. Fixed 2-field schema (`VTreeMetadataFrameFactory` lines
45–47):

| Field | Type trait | Wire encoding |
|---|---|---|
| 0 `max_distance` | fixed 8 (`DoublePointable`) | raw float64 — the largest `distance_to_centroid` in the referenced data page |
| 1 `data_page_ptr` | fixed 4 | raw int32 |

Tuple bytes: `[1 null-flag byte][8][4]` = **13 bytes** (+4-byte slot). Both fields fixed ⇒ no
var-len slots. Built via `DoubleSerializerDeserializer`/`IntegerSerializerDeserializer`
(`VTreeMetadataFrame#createMetadataTuple` lines 160–166, `VTreeBulkLoader#addDirectoryEntry`
lines 336–365) — both are tagless raw encoders.

- **Sort invariant: `max_distance` ascending.** `findInsertPosition` is a leftmost binary
  search (lines 96–109); `updateMaxDistance` overwrites field 0 in place (lines 88–91) — the
  caller must preserve the invariant. Sorted insertion here is the fix behind the antimatter
  reconciliation bug (an unsorted directory broke the k-way merge's sorted-input precondition —
  see [3754a](../80-patches/3754a-storage-layer-p1.md)).
- **Split**: copy-and-reinitialize halves, then the new tuple routes to whichever side covers
  its distance (lines 116–149) — used by the DML path's `handleMetadataPageOverflow`.
- **Chaining**: `next_page` only (no overflow flag); a cluster with more directory entries than
  one page holds chains dir pages `d0 → d1 → … → −1` (`VTreeBulkLoader#finalizeClusterDirectory`
  lines 397–412). One 32 KB page holds ≈ 2000 entries (comment, `VTreeBulkLoader` lines
  103–106), so production clusters almost always have exactly one.

## 6. Data page (`VTreeDataFrame`)

Header as §1 + `next_page` at 30. Tuples are written by the antimatter-aware writer (§3.2);
the schema is **caller-parameterized** — the type traits come from the resource JSON, built by
`VTreeResourceFactoryProvider#getTypeTraits`
(`asterixdb/asterix-metadata/.../utils/VTreeResourceFactoryProvider.java` lines 215–291):

**Quantized (production) layout** — `pkStartField = 4`
(`VTreeDataTupleConstants.Q_PK_START_FIELD`, `.../vector/utils/VTreeDataTupleConstants.java`):

| Field | Type trait (provider line) | Wire encoding | Written by |
|---|---|---|---|
| 0 `distance` | fixed 8 (line 234) | **raw float64, no ADM tag** — read directly with `buf.getDouble` (`VTreeDataFrame#getDistanceToCentroid` lines 79–84) | `writeDouble` (`VTreeDataTupleCreator` lines 67–69) |
| 1 `centroid_id` | fixed 4 (line 239) | raw int32 | `writeInt` (lines 71–73) |
| 2 `quantized_distance` | **var-len** (line 240) | 8 raw bytes (a float64) — declared variable-length despite always being 8 bytes, so it costs a 1-byte varint slot | DML: `writeDouble(distance)` — i.e. the **full-precision** distance, not a quantized one (`VTreeDataTupleCreator` lines 99–102); bulk load: real dequantized-space distance (`VTreeBulkLoaderAndGroupingOperatorDescriptor#createTransformedTuple` line 269) |
| 3 `quantized_embedding` | var-len (line 242) | `[varint contentLen][bytes]` — 1 byte/dim for SQ8; non-quantized fallback stores raw big-endian doubles (`VTreeDataTupleCreator` lines 104–136) |
| 4 … 4+P−1 | primary-key traits from the dataset (line 252) | **ADM-serialized, 1-byte type tag first** (e.g. int64 PK = 9 bytes: tag + 8) — copied verbatim from the operator tuple |
| … +I | INCLUDE-field traits (lines 256–287) | ADM-serialized, verbatim |

**Non-quantized layout** (deprecated, fixtures only): fields 2–3 absent, `pkStartField = 2`
(`VTreeDataTupleConstants.NQ_*`).

⚠ Layout-comment discrepancy: the block comment in `LSMVTreeUtils` (lines 139–141) claims
`[distance, qDist, qEmbed, centroidId, pk, …]`; the authoritative order — enforced by
`VTreeDataTupleConstants`, `VTreeDataTupleCreator`, the provider, and the Job-3 sort keys
`{1, 0}` — is `[distance, centroidId, qDist, qEmbed, pk…]`. Trust the constants class.

### 6.1 Sort invariants and DML behavior

- **Invariant: `distance` ascending, FIFO among equals.** `findInsertPosition` is a
  RIGHT-boundary binary search — new tuples land *after* all equal-distance tuples (lines
  106–127). Temporal order among equal distances is what lets antimatter reconciliation treat
  "rightmost match" as "most recent" — `findTupleByDistanceAndPrimaryKey` searches backwards
  from the right bound and compares PK bytes with a raw `Arrays.equals` (lines 140–164).
- **Split** mirrors `BTreeNSMLeafFrame.split`: mirror the buffer, shift the right half's slots,
  fix counts, `compact()` both, then re-derive the insert position from the new tuple's
  distance (lines 173–208).
- **Chaining**: `next_page` links a cluster's data pages in ascending distance ranges; −1
  terminates. The bulk loader pre-allocates the successor id so the forward pointer is set
  *before* the page is written (append-only-safe; `VTreeBulkLoader#finishCurrentDataPage`
  lines 293–330).

### 6.2 Worked example — one quantized data tuple

dim = 4, SQ8, one `int64` primary key, no INCLUDE columns, matter tuple with distance 2.5,
centroid 12, qDist 2.5, quantized bytes `[7, 200, 33, 5]`, PK 42. F = 5 fields ⇒ null-flag
bytes = ⌈(5+1)/8⌉ = 1; var-len slots for fields 2 (len 8 → `0x08`) and 3 (len 5 → `0x05`).

| Rel. offset | Bytes | Content |
|---|---|---|
| 0 | `00` | null/antimatter flags — `80` here instead if this were an antimatter tuple |
| 1 | `08` | varint slot: field 2 length = 8 |
| 2 | `05` | varint slot: field 3 length = 5 (1 meta byte + 4 content) |
| 3–10 | `40 04 00 00 00 00 00 00` | field 0 `distance` = 2.5 (raw float64 BE) |
| 11–14 | `00 00 00 0C` | field 1 `centroid_id` = 12 (raw int32) |
| 15–22 | `40 04 00 00 00 00 00 00` | field 2 `quantized_distance` = 2.5 (raw float64 inside a var-len field) |
| 23 | `04` | field 3: varint content length = 4 |
| 24–27 | `07 C8 21 05` | field 3: quantized embedding, 1 byte/dim |
| 28 | tag | field 4: ADM type tag for BIGINT (`SERIALIZED_INT64_TYPE_TAG`) |
| 29–36 | `00 00 00 00 00 00 00 2A` | field 4: PK value 42 (int64 BE) |

Total 37 bytes on the page, plus one 4-byte slot at the page tail holding this tuple's start
offset.

## 7. The index metadata page (`LIFOMetaDataFrame`)

Not a VTree frame — the page-manager page that anchors each component
(`hyracks-storage-am-common/.../frames/LIFOMetaDataFrame.java`). **Location**: for disk
components the `AppendOnlyLinkedMetadataPageManager` writes it as the **last physical page** of
the file at close (`getMetadataPageId()` returns `pages − 1`, lines 171–183; it also probes the
first page for downward compatibility). Memory components use `VirtualFreePageManager`, whose
`createMetadataFrame()` returns null — memory trees keep the equivalents in Java fields
(`numLeafCentroidMem` etc., `VTree.java` lines 114–117) and never materialize this page.

Header (shares offsets 0–8 with §1, then diverges — lines 42–51):

| Offset | Size | Field | Meaning |
|---|---|---|---|
| 0 | 4 | `tuple_count` | number of key–value entries |
| 4 | 4 | `free_space_offset` | append cursor for KV entries (init 33) |
| 8 | 1 | `level` | −1 = metadata page, −2 = free page |
| 9 | 4 | `max_page` | highest allocated page id (drives `takePage`) |
| 13 | 4 | `next_page` | next metadata page in chain (−1) |
| 17 | 4 | `valid` | magic `0x1B16DA7A` when the component is sealed |
| 21 | 4 | `storage_version` | on-disk format version |
| 25 | 4 | `root_page` | **the component's root page id** — a fixed header slot, *not* a KV entry (`setRootPageId`/`getRootPageId`, lines 149–156) |
| 29 | 4 | `free_page_count` | free-list length |
| 33 | … | KV storage | entries appended upward |

KV entry encoding (lines 173–260): `[int32 keyLen][key bytes][int32 valueLen][value bytes]`,
linearly scanned by key bytes; in-place overwrite requires the same value length.

### 7.1 VTree's keys (`VTreeMetadataKeys`)

`.../vector/utils/VTreeMetadataKeys.java` — the single source of truth for the key strings:

| Key (ASCII bytes) | Value | Written by | Read by |
|---|---|---|---|
| `num_leaf_centroids` | 8-byte long (`LongPointable`) | `VTreeStaticStructureBuilder#end` (lines 506–509), `VTreeBulkLoader#end` (lines 555–558), `VTreeFlushLoader#end` (lines 232–235) | `VTree#setStaticStructure` (lines 960–969), `VTreeBulkLoader` ctor (lines 140–149) |
| `first_leaf_centroid_id` | 8-byte long — BFS-from-root id of the first leaf centroid | same three writers | same two readers |

The **root page id** completes the trio but travels through
`IPageManager#setRootPageId`/`getRootPageId` (header offset 25 above) rather than the KV store:
the static builder records the true root (highest page id) at `end()` (line 511); the bulk
loader records `staticBasePageId + staticStructureRootPage` (`VTreeBulkLoader` lines 547–553);
the flush loader records the value returned by `VTreeFlushLoader#copyStaticStructure`, which is
`staticBasePageId + staticTree.getRootPageId()` (`VTreeFlushLoader.java` line 199) — the same
true-root convention as the bulk loader. (Historical note: this previously returned bare
`staticBasePageId`, the *first* copied static page, a bug fixed 2026-07-04 — see
[bug-archive.md](../60-quality/bug-archive.md).) The LSM framework additionally stores its own
KV entries (component id, filter, validity) in the same page through `DiskComponentMetadata`.

## 8. Raw vs ADM-tagged summary

| Data | Encoding |
|---|---|
| all page-header fields, slots, page pointers | raw big-endian ints/longs, no tags |
| `cid` / `centroid_id` / `child_ptr` / `metaPtr` / `data_page_ptr` tuple fields | raw int32 |
| `distance` / `max_distance` / `quantized_distance` | raw float64 |
| centroid embeddings (interior + leaf field 1) | `[int32 count][count × float64]` |
| quantized embeddings (leaf field 2, data field 3) | `[varint len][len bytes]` |
| neighbor list (leaf field 3) | `[varint len][len/8 × {int32, int32}]` |
| primary keys, INCLUDE fields (data fields 4+) | ADM-serialized: 1-byte type tag + value, copied verbatim from the operator tuple |
| metadata-page KV values (`num_leaf_centroids`, `first_leaf_centroid_id`) | raw 8-byte longs |

The raw (untagged) distance/cid fields are why the Job-3 external sort uses **raw** double/int
comparators on keys `{1, 0}`, and why `LSMVTreeOpContext` builds its `MultiComparator` from the
resource's `cmpFactories` (raw for fields 0–3, ADM-aware for PK fields) rather than a uniform
ADM comparator set.
