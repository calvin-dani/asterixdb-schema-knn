---
name: vtree-asf-ci-retrigger
description: How to re-run a failed ASF asterix Gerrit CV job and get Gerrit the result — Rebuild is BROKEN; use gerrit_manual_trigger or a committer TRIGGER comment
metadata: 
  node_type: memory
  type: reference
  originSessionId: ffa9de01-7bb7-4447-b672-a4d7a6aff7e2
---

Re-running a `-1 Verified` ASF asterix CV build (`asterix-jenkins.ics.uci.edu`, jobs like
`asterix-gerrit-asterix-app-openjdk17`) and getting the vote back onto the Gerrit change. See
[[vtree-infra-access-cheatsheet]] (auth: `-u hshi@scu.edu:$(cat ~/.jenkins_auth_asf)`).

## The `TRIGGER: <job>` Gerrit comment is permission-gated on the GERRIT account, NOT the Jenkins login
Posting `TRIGGER: asterix-gerrit-asterix-app-openjdk17` on the change re-runs just that one job and reports
back — but the Gerrit Trigger plugin only honors it from commenters with trigger rights (committers, e.g. Ali).
Our `Le0shy` contributor account is ignored. It is NOT about which email logs into Jenkins (`hshi@scu.edu`) —
that's a separate system. Vote-back is always posted by the plugin's own `Jenkins` service account regardless.

## Jenkins "Rebuild" button is BROKEN for these jobs — DO NOT USE (confirmed 2026-07-31, builds #5755/56/58)
These CV jobs have **no parameter definitions**; the Gerrit Trigger plugin injects `GERRIT_REFSPEC` etc. as
**env vars at trigger time**, captured into the build's ParametersAction only as a record. On Rebuild the Gerrit
event is not re-fired, and this Jenkins runs default safe-parameters (`keepUndefinedParameters=false`), so the
undefined `GERRIT_*` values are stripped → the git step fetches literal `$GERRIT_REFSPEC` →
`fatal: couldn't find remote ref $GERRIT_REFSPEC` → build dies at ~92 lines → posts a fresh **Verified -1**.
- Both the plain **Rebuild** and the **parameterized** rebuild form (`.../<n>/rebuild/parameterized`,
  POST json to `.../<n>/rebuild/configSubmit`) fail identically — the form shows `GERRIT_REFSPEC` pre-filled but
  the value is still stripped on submit. Rebuild preserves the `GerritCause` (so it DOES vote back) but can only
  ever post `-1` because it can't check out the patchset. It's worse than doing nothing.
- Can't fix it ourselves: job `config.xml` is **403** for us (no Configure rights), and it's a shared ASF job.

## What actually works
1. **BEST — self-service SINGLE job: the "Retrigger" link in the build's UI sidebar.** On the failed build page
   (e.g. `.../asterix-gerrit-asterix-app-openjdk17/5748/`) click **Retrigger** (NOT Rebuild). Produces a
   `GerritUserCause` ("Retriggered by user hshi@scu.edu for Gerrit: <change-url>") that **re-fires the full Gerrit
   event**, so `GERRIT_REFSPEC` resolves → checkout of `refs/changes/NN/NNNNN/PS` works → runs the real tests →
   votes Verified back. Confirmed 2026-07-31: build #5759 checked out `refs/changes/01/21101/32` and ran (Rebuild
   #5758 for the same change died at 92 lines). Re-runs ONLY that one job, no committer rights needed. (The
   `GET .../gerrit-trigger-retrigger-this/` URL 404s — it's POST-only / JS-rendered; use the UI link.)
2. **Self-service (whole verify set):** Jenkins **Gerrit manual-trigger page**
   `https://asterix-jenkins.ics.uci.edu/gerrit_manual_trigger/` — our `hshi@scu.edu` HAS this permission
   (page 200, crumb issues). Server dropdown value = `asterix-gerrit`. Flow (session/cookie-jar):
   `gerritSearch` POST (`queryString=change:NNNNN`, `selectedServer=asterix-gerrit`) → then the page lists
   `refs/changes/..`; select patchset → trigger. Fires a proper `GerritManualCause`; checkout works, reports
   back. Downside: re-runs ALL ~14 jobs. Use only if the per-build Retrigger link is unavailable.
3. **Also single job:** ask a committer (Ali) to post `TRIGGER: <jobname>` — but option 1 makes this unnecessary.
4. **Durable:** get `Le0shy` added to the ASF trigger group so our own `TRIGGER:` comments fire too.

## `GlobalVirtualBufferCacheTest` is a known FLAKY test on asterix master (not VTree)
`testFlushes` / `testFlushPtrBoundsAfterRemovingLastIndex` fail with
`AssertionError: Partition 0 Filtered disk component NN length <x> exceeds limit 16384` — a data-size-dependent
assertion. Seen failing then passing across UNRELATED changes (Ian Maxon 21477, Rithwik Koul 21471) and across
patchsets of our own 21101/21287. A lone `-1 Verified` citing only `asterix-gerrit-asterix-app-openjdk17`
UNSTABLE on this test is flaky — just re-run. (Sibling flaky: `CBOOptimizerTest 199 secondary-equi-join-multiindex`.)
