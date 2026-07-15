---
name: project-gerrit-stack-handoff-crosspollination
description: "Gerrit stack state after amending cross-pollination work; waiting on Calvin's 3760 rebase, then restack"
metadata: 
  node_type: memory
  type: project
  originSessionId: f918823a-9e3b-4f5f-a2b3-6f30a6442e6e
---

**Handoff state as of 2026-06-30**, branch `gerrit/storage-wrap-up-integrate`. Local stack (bottom→top): `3754 patch1 → patch2 → patch3 → 3760 Training(==was ps19) → [PENDING] amendment → 3771`.

**Done this session:**
- Amended cross-pollination edits into their owning commits (see [[project-crosspollination-dml-multicluster]]): patch1 = VTree.java + CrossPollinationConfig.java(new) + RngAcceptanceFilter.java(moved here from the amendment); patch2 = 5 lsm-vtree plumbing files; 3771 = VectorIndexAccessMethod.java (index-only Distinct). 3760 Training left untouched (patch-id still == gerrit ps19).
- Put our VTreeResourceFactoryProvider edit into the `[ASTERIXDB-3760][PENDING] Bottom-up amendment for Calvin to cherry-pick` commit (NOT into Training) so Calvin could cherry-pick it.
- Pushed new patchsets for patch1/2/3 to gerrit: #21099 / #21100 / #21101 (refs/for/master). Force-pushed branch to origin. Local safety tag `amend-safety-point` (=864fc1a02c, pre-session) kept as full undo anchor.

**Key finding — the [PENDING] amendment bundle (7 files: SecondaryVectorOperationsHelper, VTreeResourceFactoryProvider, AOrderedListVectorBinaryAccessor, HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor, VTreeBulkLoaderAndGroupingOperatorDescriptor, VTreeStaticStructureCreatorOperatorDescriptor, VectorDistanceFunctionFactory) is now FULLY absorbed into Calvin's 3760 ps20** — Calvin used the bundle to amend his patchset. Confirmed: restacking drops the whole amendment as "patch contents already upstream" (all 7 files' hunks present in ps20). Only VTreeResourceFactoryProvider was new-this-session; the other 6 pre-existed in the bundle.

**WAITING ON CALVIN:** his 3760 ps20 is currently based on the OLD patch3 (`9573fbd0`), not our new patch3 (`8ef06437fe`). He needs to rebase 3760 onto the new patch3 → ps21 (gerrit Rebase button works; SIMULATED CLEAN, no conflict).

**THEN we restack (both steps SIMULATED CLEAN):** `git fetch gerrit refs/changes/59/21159/21; git rebase --onto FETCH_HEAD <old-Training-sha> gerrit/storage-wrap-up-integrate`. The redundant amendment auto-drops. Final stack simplifies to `patch3 → Training(ps21) → 3771`. Then force-push origin + (optionally) new patchset for the 3771 change. Do NOT push Training ourselves — it's Calvin's change.

Also still uncommitted/untracked: `integration/` test changes (Part-3 cross-pollination wiring, config at eps=0.8/k=1/lim=10000). See [[todo-crosspollination-regression-test]].
