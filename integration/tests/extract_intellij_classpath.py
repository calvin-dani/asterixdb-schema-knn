#!/usr/bin/env python3
"""Extract classpath from an IntelliJ run command and save to cache file.

Usage:
    # Paste the full IntelliJ java command into a file, then:
    python extract_intellij_classpath.py < intellij_command.txt

    # Or pipe directly:
    pbpaste | python extract_intellij_classpath.py

The extracted classpath is saved to:
    asterixdb/asterix-app/target/test-classpath.txt
"""

import os
import re
import sys


def main():
    text = sys.stdin.read()

    # Extract the -classpath argument value
    m = re.search(r'-classpath\s+(\S+)', text)
    if not m:
        print("Error: No -classpath argument found in input")
        sys.exit(1)

    classpath = m.group(1)
    entries = classpath.split(":")
    print(f"Found {len(entries)} classpath entries")

    # Determine project root (look for asterixdb/asterix-app in paths)
    project_root = None
    for entry in entries:
        idx = entry.find("/asterixdb/asterix-app/")
        if idx > 0:
            project_root = entry[:idx]
            break

    if not project_root:
        # Try hyracks path
        for entry in entries:
            idx = entry.find("/hyracks-fullstack/")
            if idx > 0:
                project_root = entry[:idx]
                break

    if project_root:
        print(f"Detected project root: {project_root}")

    # Save to cache file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Walk up to find project root
    if project_root:
        cache_file = os.path.join(
            project_root, "asterixdb", "asterix-app", "target",
            "test-classpath.txt"
        )
    else:
        cache_file = os.path.join(
            script_dir, "..", "..", "asterixdb", "asterix-app", "target",
            "test-classpath.txt"
        )

    cache_file = os.path.normpath(cache_file)
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)

    with open(cache_file, "w") as f:
        f.write(classpath)

    print(f"Saved classpath to: {cache_file}")
    print(f"  {len(entries)} entries ({sum(1 for e in entries if e.endswith('.jar'))} jars, "
          f"{sum(1 for e in entries if '/target/classes' in e or '/target/test-classes' in e)} module dirs)")


if __name__ == "__main__":
    main()
