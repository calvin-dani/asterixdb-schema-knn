---
name: feedback-mvn-test-scope
description: "Don't use `mvn -am test` to verify a single module's tests — `-am` runs tests on every transitive dependency too"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: e8da6b5b-eb59-469f-9d56-05ade6986d72
---

When verifying a single target module's tests, **never** use `mvn -pl <target> -am test`. The `-am` (also-make) flag runs the `test` lifecycle on every transitive dependency, not just the target. On this codebase that pulls in `hyracks-util`, `hyracks-api`, `hyracks-net`, `hyracks-control-common`, etc. — minutes of unrelated tests when you only wanted a 10-second focused run. The user flagged this on the LSM-vtree branch.

**Right pattern**: build deps first, then run only the target's tests.

```bash
# Step 1: build dep classes + test classes, no test execution
mvn -pl <target> -am test-compile -DskipTests

# Step 2: run ONLY the target module's tests
mvn -pl <target> surefire:test
```

The `surefire:test` invocation (without -am) runs Surefire on exactly the target module. Skip step 1 if deps are already built from a recent compile.

**Why not `mvn -pl <target> test` (no -am)?** It can fail if dep classes aren't on the classpath. The `surefire:test` goal trusts that `test-classes/` is already built and just runs the test runner.

**Why not `-am test -Dtest=...`?** `-am test` still runs every dep module's tests; `-Dtest=...` only filters within each module. Same waste.

This pattern applies to any `-pl` invocation: `-am` for compile-time deps is fine; for `test` it explodes scope.
