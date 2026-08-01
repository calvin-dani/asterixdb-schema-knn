---
name: feedback-vtree-storage-test-scope
description: "When fixing VTree review comments, run ONLY the vtree storage tests (fast offline path), not the full -am/BTree suite"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

When making changes to address review comments on the VTree storage layer, run **only the vtree-related
storage tests** (`hyracks-storage-am-lsm-vtree-test`, ~30 tests, ~8 s) — not the whole storage suite.

**Why:** `mvn -pl …vtree-test -am test` takes ~2–3.5 min, but that is almost all overhead: `-am` rebuilds
the entire 27-module hyracks-fullstack chain, and the reactor's `test` phase also runs the unrelated
`hyracks-storage-am-lsm-btree-test` (196 tests, incl. slow multithread/examples/cluster tests). The vtree
tests themselves are only ~8 s and uniformly 0.26–0.66 s each (no outlier; the LocalWriteOnly/IOManager
refactor did NOT slow them). So the full run wastes minutes on rebuild + unrelated tests.

**How to apply (fast per-fix loop):**
1. Build+install ONLY the changed hyracks main module(s): `mvn -o -pl hyracks/hyracks-storage-am-lsm-vtree
   install -DskipTests -Dformatter.skip=true -Dcheckstyle.skip=true` (single module, fast). This step is
   essential for correctness — skipping it and running purely offline resolves the STALE installed jar and
   the fix-dependent red-green tests fail falsely (seen 2026-07-29).
2. Run only vtree tests: `mvn -o -pl hyracks/hyracks-tests/hyracks-storage-am-lsm-vtree-test test`
   (offline, NO `-am`, NO test filter needed — the module only has vtree tests). Use `surefire:test` instead
   of `test` if only re-running without a source change.

Reserve the full `-am` build + the whole storage suite + `SqlppExecutionTest` goldens + integration for the
**pre-push gate** (see [[vtree-ci-verification-workflow]]), not the per-comment iteration loop.
