---
name: vtree-review-fixes-patchset-mapping
description: VTree storage-p1 review-fix commits → which Gerrit change each file folds into (p1/p2/p3/3760/3771)
metadata:
  node_type: memory
  type: project
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

Storage-p1 review round (Gerrit change 21099). Fixes made on branch `vtree-3754-p1-review-fixes`
(off `vtree-split`) as batch commits: batches 1–4, 5a (encapsulation), 5b-part1 (#25 merge
VTreeDataTupleConstants→VTreeDataTupleAccessor, #26 rename creator→builder). **#33 (VTreeSearchCursor →
EnforcedIndexCursor) still pending** → folds into p1 when done.

Gerrit chain (SHAs on `vtree-split`, verify before use): p1=`9fa222a5c9` (Id613553…/21099),
p2=`db55b6af52`, p3=`c491a62b68`, 3760=`bfed9f28b6` (Training), 3771=`7945fe838b` (ANN optimizer),
spann=`4fb8d71dfa` (DON'T REVIEW), docs=`72b20d1c19`.

**Fold mapping (verified by per-file diff-tree ownership; every fix file is single-owned):**
- **p1 (storage patch 1)** — ALL `hyracks-storage-am-vtree/*` (frames, impls, utils, api, tuples) +
  `hyracks-api/.../HyracksConstants.java` (#3). This is batches 1–5a + the hyracks half of #25/#26
  (accessor merge, type-def rename, the 4 renamed Builder files). The bulk of the work.
- **p2 (storage patch 2)** — 5 `hyracks-storage-am-lsm-vtree/*` files (`LSMVTree`, `LSMVTreeUtils`,
  `VTreeFactory`, `LSMVTreeLocalResource`, `LSMVTreeLocalResourceFactory`). **#26 rename ripple only.**
- **p3 (storage patch 3)** — 7 `hyracks-storage-am-lsm-vtree-test/*` files (quantized tests +
  `LSMVTreeTestContext` + `LSMVTreeTestHarness`). **#26 rename ripple only.**
- **3760 (Training)** — `asterix-metadata/.../VTreeResourceFactoryProvider.java`. #26 ripple.
- **3771 (ANN optimizer)** — `asterix-metadata/.../MetadataProvider.java`. #25 constant-ref + #26 ripple.

Everything outside p1 is pure #26 (creator→builder) rename ripple (plus one #25 constant ref in
MetadataProvider). Batches 1–5a were 100% p1.

**Push plan:** push p1/p2/p3 to Gerrit (stacked; pushing p3' tip creates patchsets for p1'/p2'/p3').
3760/3771 get a **bundle** (prepared, not pushed live) since they're downstream integration changes.
Fold technique used: sequential cherry-pick rebuild on branch `vtree-fold` (checkout fixed files from
the fix branch after each cherry-pick, amend). Verified by the **identical-tree invariant**
(`git diff vtree-fold <fixbranch>` empty). Watch the `while read` last-line-without-newline trap it
first hit. Commit identity author `Hongyu`/committer `Le0shy` (see [[vtree-gerrit-committer-email]]);
no @AiProvenance on review fixes (see [[vtree-review-fix-no-provenance]]).

## STATUS 2026-07-31 — 3760/3771 Ali+Shahrzad review batches FOLDED into vtree-dev (NOT pushed)
Folded the `vtree-3760-ali-batches` scratch branch (Batches 1,2a,2b,3a,3b,3c,3d,4,6 = Ali's 23 + Shahrzad's 3760 comments) into vtree-dev's 3760 + 3771. Fold branch `vtree-fold-r4`; **vtree-dev moved onto it** (backup `vtree-dev-prefold-r4-backup` = old tip `a2cc762cbd`). New chain (bottom→top): 3754-p3 `5088838b5b` → **3760'=`267dcc7e13`** (Calvin) → **3771'=`8cac15f861`** (Le0shy) → docs `01013bf773` → storage-r2 `1d682e6a0e` → **spann `8afd6abb42`** (Calvin, now TOPMOST). Change-Ids + authors preserved; committer=Le0shy.
- **spann moved to the TOP** (was sandwiched between 3771 and batch-base). User: spann is standalone don't-ship, "never directly touch spann commit; fine if it shifts as long as it builds."
- **Per-file ownership** (verified by diff-tree): 3760 = Index/IndexUtil/VTreeResourceFactoryProvider/VectorDistanceFunctionFactory/asterix-om pom/ErrorCode/en.properties/QuantizationConstantsAggregateDescriptor/HierarchicalKMeans/VTreeBulkLoader/VTreeStaticStructureCreator + OSQ moves (SampleFile→Codec, ScalarVectorQuantizer→OptimizedScalarQuantizer, OptimizedScalarQuantizerFactory move, **VectorSimilarityMetric move to common.vector — assigned wholly to 3760**) + QuantizationConstants delete. 3771 = VectorIndexAccessMethod/QueryTranslator/MetadataProvider/IndexTupleTranslator/VectorIndexDeclUtil.
- **Two spann-overlap files** (spann also edits them): `SecondaryVectorOperationsHelper` (3760) and `VectorIndexDeclUtil` (3771). Handled so spann's dev-only additions do NOT leak into shipped patches: at the owning patch, reverse-apply spann (SVOH, clean) OR apply batch delta with `git apply --reject` (VIDU: creation_mode/validateCreationMode hunks reject → stay in spann; also had to `sed`-drop the stale `runtime.utils.VectorSimilarityMetric` import since its removal-hunk rejected on spann's AiProvenance-import context). At the spann commit, `git checkout scratch -- <both>` → final content = scratch. `creation_mode` DDL kept ENTIRELY in spann.
- **Gates all green:** identical-tree invariant (fold tip tree == scratch tree) ✔; per-patch compile (asterix-app -am) at 3760' AND 3771' both BUILD SUCCESS ✔; authors/Change-Ids ✔.
- **Validation on the folded tip:** full golden suite (SqlppExecutionTest → testsuite_sqlpp.xml) = **3171 run, 0 Failures, 1 Error** — the lone error is `cbo-join: ch2` `ASX3077 target/data/ch2/customer-0-0.json path not found` = missing TPC-CH data file (infra/data-gen, pre-existing, unrelated). Vector goldens + all error-message goldens pass (the en.properties/ErrorCode 1258 consolidation broke nothing). Integration parts 1-4 = green (see run).
- **NOT pushed** (ask before Gerrit push). Reply drafts (28 on 3760 + the :42/:92 "done" flip) still UNPUBLISHED. Fold script: `~/.claude/jobs/7215a24d/tmp/fold_r4.sh`.
- **SYNCED onto latest ASF master (2026-07-31):** rebased the whole chain `--onto` `gerrit/master` tip **`c40857b78f`** ([NO ISSUE][MISC] Update Dependencies to address CVEs) so Gerrit shows no Merge-Conflict mark. Rebase was CLEAN (the only content delta vs pre-sync = that deps commit's 3 pom lines: asterixdb/pom.xml, asterix-dashboard/pom.xml, hyracks-fullstack/pom.xml; the 3760 dep-block add on asterixdb/pom.xml is a different region → no conflict). New SHAs: 3754 p1=`38e1ca2d84` p2=`c641b2641d` p3=`02c79a4be0`, 3760'=`65800499e8`, 3771'=`1b3e2f23f6`, docs=`d3e749df65`, storage-r2=`9d9067effd`, spann=`2b7d8a1dcb`. Authors/committer(Le0shy)/Change-Ids preserved; synced chain BUILD SUCCESS. Backups: `vtree-dev-presync-backup` (folded, pre-sync `8afd6abb42`), `vtree-dev-prefold-r4-backup` (pre-fold `a2cc762cbd`).
- **Ext-ref added to 3760 + 3771 (2026-07-31):** these two lacked `Ext-ref: MB-72975` (the 3 storage patches already had it). Added via `git filter-branch --msg-filter` matching the two Change-Ids (I1264840ffd / Iaa502b5791), inserting `Ext-ref: MB-72975` before `Change-Id` (after 3760's `Co-authored-by`). Message-only → tree IDENTICAL; 3754 SHAs unchanged, spann NOT given Ext-ref. **Current SHAs:** 3754 p1=`38e1ca2d84` p2=`c641b2641d` p3=`02c79a4be0`, 3760=`867f17dd6c`, 3771=`75c8422c7c`, docs=`02b4c2346c`, storage-r2=`8ed5df6cfb`, spann=`19abb130bd`. Backup `vtree-dev-preextref-backup`=`2b7d8a1dcb`.
- **Gerrit state check (2026-07-31, before any push):** current patchsets still at PRE-fold SHAs — 21099(p1)=`164c85f09c` mergeable=True, 21100(p2)=`e769f84aca` **mergeable=False (conflict)**, 21101(p3)=`5088838b5b` mergeable=True, 21159(3760)=`9366f23d3d` **mergeable=False**, 21287(3771)=`ba6c893516` **mergeable=False**. So p2+3760+3771 show Merge-Conflict; the rebase-onto-master clears them once pushed. **Storage NEEDS re-push** (gated): p2 has the conflict (stacked → all of p1'/p2'/p3' go together) AND 3760'/3771' are stacked on rebased `p3'=02c79a4be0`, so Gerrit needs p3' present or an upload of 3760/3771 spawns storage patchsets by Change-Id anyway.
- **Storage p1/p2/p3 PUSHED (2026-07-31):** `git push gerrit 02c79a4be0:refs/for/master` → new patchsets on 21099/21100/21101 at rebased SHAs (p1=`38e1ca2d84`, p2=`c641b2641d`, p3=`02c79a4be0`), based on latest master. Post-push mergeable: p1=True, p3=True, **p2=False = stacked-middle artifact** (p2's diff modifies files p1 also changes — AbstractLSMIndex/IndexBuilder/IQuantizedResource — so Gerrit's isolated merge-onto-master reports non-mergeable; NOT a real conflict, merges on p1, goes green when p1 lands). Votes reset (new PS).
- **Squash toy-build patch resynced + r05 snapshot (2026-07-31):** ran `.vtree/regen-squash.sh` (source-of-truth vtree-dev; replays BASE..3771 onto couchbase/master as one Le0shy commit; excludes spann/docs; rerere auto-resolves the SqlppCompilationProvider hash-distinct-vs-vector conflict). New `vtree-toybuild-squash`=`e5d495a776` (parent `c40857b78f`, Change-Id Ic7cc05ed82 = ASF 21470), old preserved as `_squash-prev`=`3741c6e988`. Verified: squash-vs-master diff == 3771'-vs-master diff (288 files +37743/-80), 0 spann files. Push DONE: `git push gerrit vtree-toybuild-squash:refs/for/master%wip` → 21470 **PS 6 = `e5d495a7760a`** (WIP) [first attempt hit the known transient broken-pipe; verified nothing landed, retry succeeded]. REMAINING (couchbase side, NOT done): bump manifest change **249676** to `refs/changes/70/21470/6` so the columnar toy build fetches the new squash. Snapshot tag `vtree/r05-review-fold` (annotated) at vtree-dev tip `19abb130bd`, msg "round r05: review-fold (vtree-dev 19abb130bd, 2026-07-31)" — continues the r01-baseline/r02-serialuid-jars/r03-live-master-rebase/r04-cursor-enforced series.
- **3760-3771 bundle handed off:** `~/vtree-3760-3771-2026-07-31.bundle` (667K), ref `vtree-3760-3771-tip`=`75c8422c7c` (3771'), contains 3760'=`867f17dd6c` + 3771'=`75c8422c7c`, **prereq `02c79a4be0` (storage p3')**. Admin must have p3' (i.e. storage re-pushed) for it to land as ONLY 21159/21287. NOT spann this round (user asked 3760-3771 only).

## STATUS 2026-07-29 — ROUND 3 (metric-in-factory refactor) pushed
Round-3 folded the metric-in-factory refactor (Ali's `IVTreeDistanceFunctionFactory:47` + fail-loud) plus the
`getFrameTuple` fix and the `IJsonSerializable` javadoc. Folded on branch **`vtree-fold3b`** (identical-tree
invariant held; **each patch compiled** individually). Folded SHAs: p1=`9a57f8e119` (21099), p2=`923e03d1f7`
(21100), p3=`69530cd5f9` (21101), 3760=`e3161af90b`, 3771=`2f2e1e758f`, spann=`70a5929e28`. Change-Ids + authors
preserved (Calvin on 3760/spann).
- **p1/p2/p3 PUSHED** (`git push gerrit 69530cd5f9:refs/for/master`) → new PS on 21099/21100/21101.
- **3771 commit message retitled** "ANN query optimizer…" → "Vector index DDL grammar and ANN top-k query
  support" (covers SQLPP.jj/CreateIndexStatement/VectorIndexDeclUtil, not just the optimizer); **Change-Id
  Iaa502b… preserved**.
- **BUNDLE (3760+3771+spann)**: `~/vtree-3760-3771-spann-2026-07-29.bundle` (961K, ref
  `vtree-3760-3771-spann-upload`, tip `70a5929e28`, prereq `3d6992d0e7`) for admin upload.
- **Fold gotchas hit this round** (for next time): (a) the metric refactor made 3760's `VectorDistanceFunctionFactory`
  + `VTreeResourceFactoryProvider` use `VectorSimilarityMetric`, which was introduced in 3771 → had to **move
  `VectorSimilarityMetric.java` into 3760** so 3760 compiles (per-patch compile check caught it; identical-tree
  alone does NOT). (b) In the fold script's conflict path, `git add -A` swept the **untracked** hyracks-examples
  systolic tests into a commit; use explicit `git add -- <files>`, never `-A`. (c) `git cherry-pick` has **no
  `-q`** flag (use `-x`); a silent CP failure let `--amend` rewrite the wrong commit. (d) `git diff --stat && echo`
  always prints — verify empty with `git diff --quiet`.
- Branches left: `vtree-fold3b` = canonical amended chain; `vtree-fold3`/`vtree-fold3-bak` = pre-amend;
  `fixstate-r3` + a TEMP commit on `vtree-fold2` = the byte-identical reference (safe to delete once adopted).

## STATUS 2026-07-28 — ROUND 2 pushed
Round-2 fixes (Bucket A replies-only; Bucket B #38 IAP_KEY→VD_FUN_FACTORY + #53 createMetadataTuple
moved to VTreeMetadataTupleAccessor; Bucket C float[6]→`VTreeQuantizationParams` record across
p1+p2+asterix-common; VTree:286/:601 `<=0`→`==NO_NEXT_PAGE`; #34 reply corrected). Folded on branch
**`vtree-fold2`**: p1''=`8c311dcf74` (21099), p2''=`667865451b` (21100), p3''=`0f200b06c0` (21101),
3760''=`3a06e4882e`, 3771''=`d6d5834ea6`, spann=`db8f298404`. Invariant held (tree == `vtree-fold`).
- **p1/p2/p3 PUSHED** as PS22 (`git push gerrit 0f200b06c0:refs/for/master`).
- **BUNDLE (3760+3771+spann)**: `~/vtree-3760-3771-spann-2026-07-28.bundle` (965K, tip spann
  `db8f298404`, prereq merged base `3d6992d0e7`) for admin upload.
- **46 draft replies** on 21099 cover all threads (created on PS21; still UNPUBLISHED — publish via web UI).
- **Tests green**: storage unit (26 lsm-vtree-test), 3760 operators (18 in asterix-runtime), and the
  **vector integration suite** (8 `vector` runtimets end-to-end via SqlppExecutionTest, incl. plan goldens).
  Running asterix tests needs a full hyracks-fullstack→asterix `mvn install` first (make install leaves
  algebricks stale: CompilerProperties QUERY_PLAN_CACHE_DEFAULT). Reworded 2 change-log-style javadocs
  before folding (record + createMetadataTuple).

## STATUS 2026-07-27 — DONE this round
Folded chain on branch **`vtree-fold`** (tip `6902f38bf7`): p1'=`df787da994`, p2'=`f5726536e6`,
p3'=`f4a691ba5a`, 3760'=`ff3ff616f0`, 3771'=`fe1fc3ee46`.
- **p1/p2/p3 PUSHED** → new patchsets on **21099/21100/21101** (`git push gerrit f4a691ba5a:refs/for/master`;
  needed one retry after a transient broken pipe).
- **3760+3771 BUNDLED** (not pushed; 3760 Calvin-authored → admin upload):
  `~/vtree-3760-3771-2026-07-27.bundle` (934K, tip `fe1fc3ee46`, prereq base
  `3d6992d0e7` = merged LSM sampling).
- **Docs**: `gerrit-comments-verbatim-2026-07.md` (all 73 reviewer comments verbatim + disposition) and
  `gerrit-review-replies-r2.md` (summary + push/bundle log). Committed on branch **`vtree-fold`** only
  (commit `6377d6b628`) under `asterixdb/doc-vtree/60-quality/`, so they DISAPPEAR from the working tree
  on any non-vtree-fold branch (doc-vtree is per-branch). Durable branch-independent copies:
  `~/vtree-gerrit-review-replies-r2.md` and `~/vtree-gerrit-comments-verbatim-2026-07.md`.

**Coverage:** 38 FIXED (pushed), 1 partial, 16 reply-only, **1 deferred (#33 VTreeSearchCursor →
EnforcedIndexCursor)**, **17 NOT-yet-addressed**. Root cause of the 17: original analysis ran off an
incomplete fetch; live re-fetch (2026-07-27) surfaced 73 vs the ~48 batched. The 17 = (A) 6 comments on
**21100/p2** (out of the p1-batch scope: LSMVTreeDiskComponent:159, LSMVTreeLocalResource:67/226/383,
QuantizedIndexCreateOperatorDescriptor:91, VectorSearchOperatorNodePushable:107); (B) 7 API-shape
design changes on p1 (float[]→params object at IVTreeDataTupleCreatorFactory:46 / :34 /
VTreeDataTupleCreatorFactory:46 / IVTreeQuantizerFactory:53; IVTreeMetadataFrame:53 method placement;
IVTreeDistanceFunctionFactory:38 name / :47 metrics); (C) 4 reply-only questions (IVTreeQuantizer:46
no-op?, VTreeNSMFrame:68 dup frameTuple, VTreeFlushLoader:175 getOverflowFlagBit). Next VTree session:
post C replies, scope B as a patchset, handle A on 21100.
