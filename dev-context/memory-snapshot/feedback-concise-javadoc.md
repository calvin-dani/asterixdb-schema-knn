---
name: feedback-concise-javadoc
description: "Don't write verbose class-level javadoc paragraphs that duplicate information available elsewhere"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: e8da6b5b-eb59-469f-9d56-05ade6986d72
---

I tend to write multi-paragraph class-level javadoc that adds context I think will help reviewers. The user repeatedly trims it. Pattern: a one-line summary is the right size for a class header; longer "context" paragraphs either duplicate what's on related classes (e.g., schema notes belong on the frame factories, not on the helper that wires them) or restate the obvious (e.g., call-site enumerations that a reverse-lookup answers in seconds). Examples trimmed:

- `VectorIndexFilterTypeEnvironment.java` — 5-line `IMPORTANT:` block about function expression type computation (PS11 cleanup pass).
- `VectorDistanceFunctionFactory.java` — 4-line "Defensive:" block explaining the null-default fallback.
- `LSMVTreeUtils.java` — two `<p>` paragraphs on call sites and frame-schema ownership (commit `4820594b21`).

**Default to a one-line class javadoc.** Add extra paragraphs only if (a) they capture a behavior the reader can't infer from the code itself, and (b) the information isn't already on a class the reader would normally look at first. For per-method content, `@param`/`@throws` exist for a reason — use them, don't reinvent them in prose.

If unsure, **leave it short and let the user ask for more**. Easier to add a sentence than to argue one out.
