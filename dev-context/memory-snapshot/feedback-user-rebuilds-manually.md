---
name: feedback-user-rebuilds-manually
description: "User prefers to rebuild Maven modules and restart the AsterixDB sample cluster manually — don't run `mvn install` or restart scripts unprompted."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 07048a41-07f2-4790-bba6-0c9478538421
---

User has repeatedly interrupted `mvn -pl … install` and cluster-restart commands, then said things like "i've already rebuilt manually" or "i've restarted the cluster". They have their own build/deploy workflow on the asterixdb-schema-knn project that is faster or otherwise preferable.

**Why:** They handle JAR refresh and NC/CC restart in their own way (probably an incremental build script or hot-reload). Running mvn/restart from the agent duplicates work and slows the session.

**How to apply:**
- After editing source files, do NOT auto-run `mvn ... install` or `mvn ... package` to deploy. Just edit the files and report what's changed.
- Don't auto-run `start-sample-cluster.sh` / `stop-sample-cluster.sh` either.
- If a probe shows the running cluster doesn't have the new code, say so explicitly and wait for the user to refresh, rather than rebuilding.
- A single `mvn` invocation to surface compile errors before they trigger a rebuild is OK — but only when verifying correctness, not when deploying.
- Same applies to remote clusters (e.g. 10.16.229.107): don't curl / probe a remote AsterixDB endpoint until the user confirms it's up. If asked to point a script at a new host, just apply the config change; defer connectivity testing until the user says go.
- Related: [[integration-test]] notes — same project's build chain.
