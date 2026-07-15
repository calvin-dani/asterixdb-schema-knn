---
name: feedback-surefire-needs-install
description: "When testing main-source edits, surefire:test uses stale JARs unless you mvn install the dependency first."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: e8da6b5b-eb59-469f-9d56-05ade6986d72
---

`mvn -pl <test-module> surefire:test` after editing a main-source file in a dependency module reads from the JAR in `~/.m2/repository/`, not from the dependency's `target/classes/`. So my code changes can appear to compile-and-test-fine while actually running against the previous version of the dependency.

Path I burned in the bottom-up conversion: edited `VTreeStaticStructureBuilder.java`, ran `mvn -pl <vtree-test> -am test-compile && mvn surefire:test`. Tests showed 9 failures. Spent ~30 min debugging spurious errors before realizing `surefire:test` was running against the previous-day JAR.

**Why:** `test-compile` only compiles test sources. `-am` rebuilds dependencies' `target/classes/` but does not re-install JARs. Surefire's classpath for the test module pulls from `~/.m2/` for transitive deps.

**How to apply:** When I edit a main-source file in `<dep-module>` and need to run tests in `<test-module>`, do:
```
mvn -pl <dep-module> install -DskipTests -Drat.skip=true
mvn -pl <test-module> surefire:test
```
If editing multiple modules, install them all (comma-separated `-pl`). Do NOT trust a green `surefire:test` after just `-am test-compile` for main-source edits.

Related: [[feedback-mvn-test-scope]] — `-am test` is the heavyweight (don't do it); but for `surefire:test` after main edits, `install` of the changed module IS required.
