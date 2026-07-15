# Gerrit: upload instructions + review replies (new-base stack)

Stack rebased onto new base `16a49f62ea` (LSM Sampling change 20959 ps19, includes merged
ASTERIXDB-3676). As of 2026-07-13: p1/p2/p3 (Le0shy) were pushed directly to Gerrit; the three
patches above p3 (3760/3771/SPANN) go via the admin bundle in §A. Review replies below cover
change 21099 (p1, §B) and 21100 (p2, §C), reviewer Ian Maxon. Jenkins CI status is in §D.

---

## A. Admin bundle upload (needs Forge Author + Forge Committer)

**Status (2026-07-13):** p1/p2/p3 are Le0shy-authored and were already pushed directly to Gerrit
by Le0shy — **21099 ps16**, **21100 ps19**, **21101 ps18**. This bundle is needed for the three
patches above p3, which have a non-Le0shy commit in their ancestry (3760 + SPANN are
Calvin-authored; 3771 is Le0shy's but sits on top of 3760), so they require forge to upload.
Note 3771 **cannot** be pushed by Le0shy alone now — its parent is the rewritten 3760 (Calvin's,
not yet on Gerrit) — so the admin push below must land 3760 + 3771 + SPANN together.

**Includes the 3760 CI fix (2026-07-13):** 3760 previously failed Verified-1 because it declared
error codes 1256–1264 but their messages were placed in 3771; `ErrorCode`'s static-init validator
threw `error message missing for … (1256)`, failing `DataverseNameTest`. The 9 messages were moved
from 3771 into 3760 (final tree byte-identical; only which patch introduces them changed).
`DataverseNameTest` now passes at the 3760 level. Re-uploading via this bundle is what clears the
Verified-1 on 21159.

Bundle: `vtree-stack-newbase-2026-07-13.bundle` (953K, repo root), tip `738e06ebdb` =
`refs/heads/vtree-spann-integrate` (3760=`4470c14c77`, 3771=`cf24b5e101`). It carries the whole
6-commit chain for ancestry continuity; prerequisite is `16a49f62ea` = LSM Sampling 20959 ps19
(already on Gerrit).

```bash
# admin, from an up-to-date asterixdb checkout with the gerrit remote:
git fetch gerrit refs/changes/59/20959/19        # ensure the prereq commit is present
git bundle verify /path/to/vtree-stack-newbase-2026-07-13.bundle
# NOTE the leading '+': the bundle rewrote history (3760 ErrorCode fix), so this is NOT a
# fast-forward over any stale vtree-upload ref from the prior upload — without '+' git rejects it.
# (Or run `git update-ref -d refs/heads/vtree-upload` first, then drop the '+'.)
git fetch /path/to/vtree-stack-newbase-2026-07-13.bundle +refs/heads/vtree-spann-integrate:vtree-upload
git log --oneline 16a49f62ea..vtree-upload        # sanity: 6 commits, tip 738e06ebdb
git push gerrit vtree-upload:refs/for/master      # one push; Change-Ids route to changes
```
Because p1/p2/p3 are already uploaded as the identical commits, Gerrit dedupes them (no new
patchset) and this push only **creates new patchsets on 3760, 3771, and SPANN**. Authors/
Change-Ids preserved; admin shows only as uploader.

**For Calvin:** after upload, base any further work on the NEW patchsets — the stack moved to
the new upstream base (dropped the dummy-VD base; picked up merged 3676 + LSM sampling ps19).
`git fetch gerrit refs/changes/99/21099/<new> && git checkout FETCH_HEAD` before amending.

---

## B. Review replies for change 21099 (Ian Maxon)

All three replies are on change 21099 (p1), now at **ps16** (re-pushed 2026-07-13).

### Reply 1 — VTree.java, ~line 509 (antimatter terminology)

Comments change applied: Here VTree never reads or sets deletion polarity and the injected
frame factory's tuple writer decides how a delete-marker tuple is encoded, and matter/marker
reconciliation lives in the LSM layer. Comments in this module were using LSM vocabulary,
which made the base read as LSM-aware. Every "antimatter" mention in hyracks-storage-am-vtree
is reworded to "logical-delete marker tuple", with a boundary note added right here at
deleteVector.

### Reply 2 — VTreeBulkLoader.java:159 (eager copy) + the ps2 confiscation comments

Fix applied: this was over-eager. Peak is now O(1) buffer-cache pages instead of
O(#static-pages). Same fix applied to VTreeFlushLoader.copyStaticStructure.

### Reply 3 — VTreeBulkLoader.java:222 (oversized vector)

Fix applied: there was no guard: a tuple larger than a page's usable space would allocate a
fresh page and call insertSorted without re-checking and overrun the buffer. Fixed by
mirroring BTreeNSMBulkLoader: both write sites (VTreeBulkLoader.add and
VTreeStaticStructureBuilder.add) now throw ErrorCode.RECORD_IS_TOO_LARGE when a tuple exceeds
the usable empty-page space (pageSize − pageHeaderSize − slotSize). The realistic overflow
victim is a full-precision centroid in the static structure (8 bytes/dim), so the guard
matters most there. Regression test LSMVTreeOversizedTupleTest covers both the static-build
and bulk-load paths. Not sure if we need to deal with it in the DDL layer.

---

## C. Review replies for change 21100 (Ian Maxon)

Ian left 6 inline comments on **ps5**; 5 were answered "Done" on later patchsets. The one below
(LSMVTreeLocalResource.java:226) was resolved in code but never got a reply — this closes it.

### Reply — LSMVTreeLocalResource.java:226 (put quantization params in the JSON)

Done — did exactly this. At ps5 the SQ8 quantization constants (minQuantile, maxQuantile,
alpha, confidenceInterval, bits, sampleCount) were read from a binary sidecar file
`.quantization_<indexName>` via a hand-rolled DataInputStream (tryReadQuantizationSidecarFile).
That mechanism is gone: the sidecar read was removed in ps7, and ps9 moved the constants into
the JSON local resource — written in appendToJson and read back in fromJson through
IPersistedResourceRegistry, same as the other resource fields.

They round-trip as optional keys (putIfNotNull on write; readOptionalFloat/readOptionalInt on
read), so a non-quantized index simply carries none and a resource missing them reads back as
non-quantized rather than failing — which is the backwards-compat/extensibility win you were
after. No perf reason to keep the binary side file; it was just an early shortcut.

---

## D. Jenkins CI status (2026-07-13)

After the admin's first bundle upload, one real defect and three flaky failures showed up. Only
the first needed a code change — it is already in `vtree-stack-newbase-2026-07-13.bundle`.

- **3760 `notopic-jdk17-redux` — REAL, fixed.** `ErrorCode` static-init threw
  `error message missing for … (1256)` — codes 1256–1264 were declared in 3760 but their messages
  were in 3771 — failing `DataverseNameTest`. Messages moved into 3760 (see §A); verified passing
  at the 3760 level (`mvn -pl asterix-common -Dtest=DataverseNameTest`, 5/5).
- **3760 `cloud-nons3-test` — flaky.** Forked-VM crash in the cloud-storage "Unstable" tests
  (`GCSCloudStorageUnstableTest`), no VTree/ErrorCode signature. Job is ~40% fail across unrelated
  changes; the identical crash appears on unrelated build #4479.
- **3771 `verify-storage` + `verify-txnlog` — flaky.** Both built clean, then the post-build
  storage/txnlog upgrade test timed out starting the parent (3760) cluster (`state: UNUSABLE`).
  Not real: the same 3760 parent booted fine one build earlier (`verify-storage #19416` started
  both the p3-parent and 3760 clusters → SUCCESS); both jobs are ~35% flaky; 3771 is a
  compiler-only change (`storage format changes: no`); the failures sit between passing neighbors.

**Known-flaky jobs — re-trigger, don't chase:** `cloud-nons3-test`, `verify-storage`,
`verify-txnlog` (all cluster-startup / heavy-test timeouts). Read Jenkins logs with the creds in
`~/.jenkins-auth`: `curl -u "$(cat ~/.jenkins-auth)" <build-url>/consoleText`. Diagnosis method:
a real bug is deterministic + reproducible + change-specific (like the ErrorCode one); a flake is
an environmental failure mode with a high job base-rate and passing neighbors.
