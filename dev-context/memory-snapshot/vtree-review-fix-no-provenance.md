---
name: vtree-review-fix-no-provenance
description: "Don't add @AiProvenance annotations when fixing Gerrit review comments (they fold into existing attributed commits)"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

When making **review-comment fixes** on the VTree Gerrit patches (small tweaks to existing methods that
later fold into the already-attributed patch commit, e.g. storage p1 `9fa222a5c9`/21099), do **NOT** add
`@AiProvenance` annotations.

**Why:** these are small modifications to existing (already-authored/attributed) methods, not new AI-authored
classes/methods; the changes squash into the existing commit which is already attributed; and these patches go
to **upstream Apache Gerrit** (asterix-gerrit.ics.uci.edu), where extra annotation noise on tiny fixes is
unwanted and bloats the diff. The general CLAUDE.md "annotate AI-authored Java" rule is for new/substantial
authorship, not for folding review fixes into an existing commit.

**How to apply:** implement the fix as plain code (comments explaining the change are fine and encouraged);
skip `@AiProvenance` on the changed method/field. Only annotate when authoring genuinely new
classes/methods from scratch. Confirmed 2026-07-24 after I wrongly added one on a `VTreeAccessor` ctor fix and
the user flagged it. Related: [[vtree-review-fixes-patchset-mapping]] · [[analytics-project-switching]].
