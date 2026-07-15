---
name: Use python not python3
description: User has faiss and numpy installed under "python" command, not "python3"
type: feedback
originSessionId: a6c00089-526e-4d76-a025-5219da303cc6
---
Use `python` instead of `python3` for running scripts. The user's faiss-cpu and numpy are installed in the `python` environment.

**Why:** User has a specific Python environment with faiss-cpu 1.11.0 and numpy 1.26.4 installed under `python`.
**How to apply:** Always use `python` when running scripts, installing packages, or checking Python imports.
