"""Test result reporting for integration test suite."""

import json
import time
from dataclasses import dataclass, field, asdict
from typing import List, Optional


@dataclass
class TestResult:
    part: str
    test_name: str
    status: str  # PASS, WARN, FAIL
    elapsed_seconds: float = 0.0
    error: Optional[str] = None
    details: dict = field(default_factory=dict)


class TestReporter:
    """Collects test results and generates summary reports."""

    def __init__(self):
        self.results: List[TestResult] = []
        self.start_time = time.time()

    def add_result(self, result: TestResult):
        self.results.append(result)
        symbol = {"PASS": "+", "WARN": "~", "FAIL": "!"}[result.status]
        print(f"  [{symbol}] {result.test_name}: {result.status}"
              f" ({result.elapsed_seconds:.1f}s)"
              f"{' — ' + result.error if result.error else ''}")

    def print_summary(self):
        total = time.time() - self.start_time
        passed = sum(1 for r in self.results if r.status == "PASS")
        warned = sum(1 for r in self.results if r.status == "WARN")
        failed = sum(1 for r in self.results if r.status == "FAIL")

        print()
        print("=" * 70)
        print("  INTEGRATION TEST SUMMARY")
        print("=" * 70)
        print(f"  Total: {len(self.results)}  |  "
              f"PASS: {passed}  |  WARN: {warned}  |  FAIL: {failed}  |  "
              f"Time: {total:.1f}s")
        print("-" * 70)

        for r in self.results:
            symbol = {"PASS": "PASS", "WARN": "WARN", "FAIL": "FAIL"}[r.status]
            err = f"  ({r.error})" if r.error else ""
            print(f"  [{symbol}] {r.part:>8} | {r.test_name:<40} "
                  f"{r.elapsed_seconds:>7.1f}s{err}")

        print("=" * 70)

        if failed > 0:
            print(f"\n  RESULT: FAILED ({failed} failure(s))")
        elif warned > 0:
            print(f"\n  RESULT: PASSED with warnings ({warned} warning(s))")
        else:
            print(f"\n  RESULT: ALL PASSED")
        print()

    def save_json(self, path: str):
        output = {
            "total_elapsed_seconds": time.time() - self.start_time,
            "summary": {
                "total": len(self.results),
                "passed": sum(1 for r in self.results if r.status == "PASS"),
                "warned": sum(1 for r in self.results if r.status == "WARN"),
                "failed": sum(1 for r in self.results if r.status == "FAIL"),
            },
            "results": [asdict(r) for r in self.results],
        }
        with open(path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  Results saved to: {path}")

    def exit_code(self) -> int:
        return 1 if any(r.status == "FAIL" for r in self.results) else 0
