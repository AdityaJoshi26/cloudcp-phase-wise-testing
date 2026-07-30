#!/usr/bin/env python3
"""Validate every generated datagen spec: YAML syntax + minimal semantic checks.
Not part of the deliverable pipeline — a one-shot sanity checker."""
import glob
import os
import sys

import yaml

os.chdir(os.path.dirname(os.path.abspath(__file__)))
files = sorted(set(glob.glob("DS-*/*.yaml") + glob.glob("DS-*/**/*.yaml", recursive=True)))

errors = []
modes = {}
for f in files:
    try:
        doc = yaml.safe_load(open(f, encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        errors.append((f, f"YAML parse error: {e}"))
        continue
    if not isinstance(doc, dict):
        errors.append((f, "top-level not a mapping"))
        continue
    m = doc.get("mode")
    modes[m] = modes.get(m, 0) + 1
    if m not in ("tree", "flat", "list", "csv-list"):
        errors.append((f, f"bad mode: {m!r}"))
    if m in ("tree", "flat") and not doc.get("root"):
        errors.append((f, "missing root"))
    if m == "tree":
        t = doc.get("tree", {})
        if t.get("fanout", 0) < 1 or t.get("depth", 0) < 1:
            errors.append((f, f"bad tree topology: {t}"))
    if m == "flat":
        if doc.get("flat", {}).get("num_files", -1) < 0:
            errors.append((f, "bad flat.num_files"))
    sz = doc.get("size", {})
    if sz.get("type") == "range" and sz.get("max", 0) < sz.get("min", 0):
        errors.append((f, "size.max < size.min"))

print(f"Validated {len(files)} spec files.")
print("Modes:", modes)
if errors:
    print(f"\n{len(errors)} PROBLEM(S):")
    for f, e in errors[:40]:
        print(f"  {f}: {e}")
    sys.exit(1)
print("All specs valid.")
