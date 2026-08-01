---
name: feedback-no-cherry-pick-annotation
description: "Don't leave \"(cherry picked from commit ...)\" lines in VTree/asterix commit messages"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

Never leave a `(cherry picked from commit <sha>)` trailer in commit messages for the VTree/asterix work.

**Why:** the user keeps these commits' messages clean (they go to ASF Gerrit / bundles); the cherry-pick
provenance line is noise.

**How to apply:** do NOT use `git cherry-pick -x` (it appends that line). Use plain `git cherry-pick`, or
strip the trailer before amending. The regen path uses `git merge --squash` + `git commit` (no such line), so
it's already clean — the risk is in manual fold/cherry-pick steps. Relates to [[clusterby-commit-message-style]]
(no `Co-Authored-By: Claude` on Gerrit commits) and [[vtree-review-fix-no-provenance]] (no @AiProvenance on
review-fix commits).
