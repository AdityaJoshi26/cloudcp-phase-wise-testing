#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
specfile_engine.py — a datagen spec-file GENERATION ENGINE.

Takes a compact requirement (category + size + files + total_capacity, plus
overridable defaults) and emits ONE datagen YAML spec file per requirement.

Input channels
==============
  * command line          python specfile_engine.py --category ... --size ... --files ...
  * a single JSON file     python specfile_engine.py --input requirement.json   (object OR array)
  * a JSONL file           python specfile_engine.py --input requirements.jsonl (one record/line)

Core idea
=========
The user states INTENT in a few fields; the engine reconciles size/files/capacity
(deriving or SUGGESTING the missing one), fills defaults from the chosen CATEGORY,
picks the tier/bucket from the size, chooses a tree-or-flat topology that yields the
exact file count, and renders a single, self-documenting spec file.

Runtime dependencies
====================
  * Python 3 standard library only (argparse, json, os, zlib, math, dataclasses).
  * OPTIONAL: pyyaml — only needed to READ a .yaml/.yml *input* requirement file.
    JSON / JSONL / CLI input need nothing extra.
  * It does NOT read DatagenSpecFileGuide.md / dataset_map.json / manifest.json /
    generate_specs.py at run time — that domain knowledge is baked into this file.

See plan_specfile_engine.md for the full design.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import zlib
from dataclasses import dataclass, field
from typing import Optional

# ── Size units (binary, base-1024 — matches datagen's parser) ─────────────────
KB = 1024
MB = 1024 ** 2
GB = 1024 ** 3
TB = 1024 ** 4
PB = 1024 ** 5

_UNIT_FACTORS = {
    "": 1, "b": 1,
    "k": KB, "kb": KB, "ki": KB, "kib": KB,
    "m": MB, "mb": MB, "mi": MB, "mib": MB,
    "g": GB, "gb": GB, "gi": GB, "gib": GB,
    "t": TB, "tb": TB, "ti": TB, "tib": TB,
    "p": PB, "pb": PB, "pi": PB, "pib": PB,
}


def parse_size(value) -> int:
    """Parse a size (bytes) from an int or a string like '5MB', '12KB', '0', '1024'."""
    if isinstance(value, bool):
        raise ValueError(f"invalid size: {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"negative size: {value}")
        return value
    if isinstance(value, float):
        return int(value)
    s = str(value).strip().lower().replace("_", "")
    if s == "":
        raise ValueError("empty size string")
    # split leading numeric part from unit suffix
    i = 0
    while i < len(s) and (s[i].isdigit() or s[i] == "."):
        i += 1
    num_part, unit = s[:i], s[i:]
    if num_part == "":
        raise ValueError(f"invalid size string: {value!r}")
    unit = unit.strip()
    if unit not in _UNIT_FACTORS:
        raise ValueError(f"unknown size unit {unit!r} in {value!r}")
    num = float(num_part) if "." in num_part else int(num_part)
    return int(num * _UNIT_FACTORS[unit])


def human(n: int) -> str:
    """Short binary size label, e.g. 0->'0b', 10240->'10kb', 1048576->'1mb'."""
    if n == 0:
        return "0b"
    for unit, factor in (("pb", PB), ("tb", TB), ("gb", GB), ("mb", MB), ("kb", KB)):
        if n >= factor:
            if n % factor == 0:
                return f"{n // factor}{unit}"
            return f"{n / factor:.1f}{unit}".replace(".0", "")
    return f"{n}b"


def size_desc(policy) -> str:
    """Human description of a size policy dict for spec-file comments."""
    if policy is None:
        return "(derived from files+capacity)"
    if not isinstance(policy, dict):
        return str(policy)
    if policy.get("kind") == "fixed":
        return f"fixed {human(policy['bytes'])}"
    if policy.get("kind") == "range":
        return (f"range {human(policy['min'])}\u2013{human(policy['max'])} "
                f"({policy.get('dist', 'uniform')})")
    return str(policy)


# ── Size policy (fixed | range) ───────────────────────────────────────────────
def fixed(n: int) -> dict:
    return {"kind": "fixed", "bytes": n}


def rng(lo: int, hi: int, dist: str = "uniform") -> dict:
    return {"kind": "range", "min": lo, "max": hi, "dist": dist}


def parse_size_field(value, distribution: str = "uniform") -> dict:
    """
    Accept several forms and return a size policy dict:
      - int / '5MB' / '0'            -> fixed
      - '12KB-5MB'                   -> range
      - {'type':'fixed','bytes':..}  -> fixed
      - {'type':'range','min':..,'max':..,'distribution':..} -> range
    """
    if value is None:
        return None
    if isinstance(value, dict):
        t = str(value.get("type", "fixed")).lower()
        if t == "fixed":
            return fixed(parse_size(value["bytes"]))
        if t == "range":
            return rng(parse_size(value["min"]), parse_size(value["max"]),
                       str(value.get("distribution", distribution)))
        raise ValueError(f"unknown size.type: {t!r}")
    if isinstance(value, (int, float)):
        return fixed(parse_size(value))
    s = str(value).strip()
    if "-" in s and not s.lstrip().startswith("-"):
        lo, hi = s.split("-", 1)
        return rng(parse_size(lo), parse_size(hi), distribution)
    return fixed(parse_size(s))


def size_label(sz: dict) -> str:
    if sz["kind"] == "fixed":
        return human(sz["bytes"])
    return f'{human(sz["min"])}_{human(sz["max"])}'


def size_max(sz: dict) -> int:
    return sz["bytes"] if sz["kind"] == "fixed" else sz["max"]


def avg_size(sz: dict) -> float:
    """Estimate the mean file size for a size policy."""
    if sz["kind"] == "fixed":
        return float(sz["bytes"])
    lo, hi, dist = sz["min"], sz["max"], sz.get("dist", "uniform")
    if dist == "log-uniform":
        lo_eff = max(lo, 1)
        return math.sqrt(lo_eff * hi)
    # uniform and normal both centre on the midpoint
    return (lo + hi) / 2.0


# ── Bucket / tier model ───────────────────────────────────────────────────────
# "" is the no-extension slot.
TYPES_ALL = ["csv", "json", "txt", "log", "sql", "xml", "yaml",
             "bin", "gz", "tar", "zip", "zst", "bz2", "7z",
             "parquet", "avro", "orc", "arrow", "hdf5",
             "jpg", "png", "mp4", "mkv", "wav", "so", ""]

BUCKET_SEAL = {
    "zero":   None,
    "tiny":   (2000, "256MB", 8),
    "small":  (512,  "2GB",   8),
    "medium": (64,   "10GB",  8),
    "large":  (8,    "50GB",  8),
}


def bucket_for_size(n: int) -> str:
    """Map a size in bytes to a tier bucket (per the plan's tier table)."""
    if n <= 0:
        return "zero"
    if n < 1 * MB:
        return "tiny"
    if n < 100 * MB:
        return "small"
    if n < 1 * GB:
        return "medium"
    return "large"


def bucket_for_policy(sz: dict) -> str:
    """Bucket derived from a size policy (range uses the midpoint)."""
    if sz["kind"] == "fixed":
        return bucket_for_size(sz["bytes"])
    return bucket_for_size(int(round((sz["min"] + sz["max"]) / 2)))


# ── Category catalogue (name -> description + defaults) ───────────────────────
# defaults keys: depth, size_kind (fixed|range|None), distribution, content,
#                bucket_bias (advisory), files (forced count, e.g. single-file).
CATEGORIES = {
    "Single-Tier Isolation": {
        "desc": "All files land in one tier (zero/tiny/small/medium/large). "
                "Stress a single bucket in isolation.",
        "defaults": {"depth": 3, "size_kind": "range", "distribution": "uniform"},
    },
    "Batch Builder Mechanics": {
        "desc": "Exact-count probes that trigger count-seal / byte-seal thresholds.",
        "defaults": {"depth": 1, "size_kind": "fixed", "distribution": "uniform"},
    },
    "Batch Exhaustion / Weight Shift": {
        "desc": "Multi-tier corpora where one tier drains first and worker slots "
                "redistribute. (One tier per spec here.)",
        "defaults": {"depth": 3, "size_kind": "range", "distribution": "uniform"},
    },
    "Filename & Encoding Stress": {
        "desc": "Exercises tricky filenames (spaces, Unicode, control bytes, long names).",
        "defaults": {"depth": 1, "size_kind": "fixed", "distribution": "uniform"},
    },
    "File Type Coverage": {
        "desc": "Ensures all file types appear across the active tier.",
        "defaults": {"depth": 3, "size_kind": "range", "distribution": "uniform"},
    },
    "Network Profile Comparison": {
        "desc": "Same on-disk data run under different scheduler profiles (fixed seed).",
        "defaults": {"depth": 3, "size_kind": "range", "distribution": "uniform"},
    },
    "Mixed Full-Pipeline": {
        "desc": "End-to-end scan->batch->upload->verify (one tier per spec here).",
        "defaults": {"depth": 3, "size_kind": "range", "distribution": "uniform"},
    },
    "Configuration Edge Cases": {
        "desc": "Empty dirs, single huge file, deep trees, unreadable subdirs.",
        "defaults": {"depth": 1, "size_kind": None, "distribution": "uniform"},
    },
    "Single-File Transfer": {
        "desc": "Exactly one file at a chosen size/boundary.",
        "defaults": {"depth": 1, "size_kind": "fixed", "distribution": "uniform", "files": 1},
    },
    "Sub-Range Isolation": {
        "desc": "A narrow size sub-band (e.g. 10 KB-1 MB) at high count.",
        "defaults": {"depth": 3, "size_kind": "range", "distribution": "uniform"},
    },
    "Alternative Weight Ratios": {
        "desc": "Datasets proportioned to non-default scheduler weights "
                "(one tier per spec here).",
        "defaults": {"depth": 3, "size_kind": "range", "distribution": "uniform"},
    },
    "Tiny/Small-Heavy Mixed": {
        "desc": "Skewed mixes dominated by tiny/small files (one tier per spec here).",
        "defaults": {"depth": 3, "size_kind": "range", "distribution": "uniform"},
    },
}

GENERIC_DEFAULTS = {"depth": 3, "size_kind": None, "distribution": "uniform"}


def category_defaults(name: Optional[str]) -> dict:
    if name and name in CATEGORIES:
        return dict(CATEGORIES[name]["defaults"])
    return dict(GENERIC_DEFAULTS)


# ── Filename-variant -> naming policy (for optional `naming` overrides) ───────
@dataclass
class NamePolicy:
    charset: str = "ascii"
    alphabet: list = field(default_factory=lambda: ["lower", "upper", "digit", "dash"])
    unicode_blocks: list = field(default_factory=list)
    special_chars: str = ""
    prefix: str = ""
    suffix: str = ""
    length: int = 16
    collision: str = "append-index"
    ext_override: Optional[list] = None
    note: str = ""


def variant_policy(fn: str) -> NamePolicy:
    """Return the naming policy realising filename variant `fn` (FN-01..FN-20)."""
    m = {
        "FN-01": NamePolicy(charset="ascii", alphabet=["lower", "upper", "digit", "dash"], length=16),
        "FN-02": NamePolicy(charset="ascii", alphabet=["lower", "digit"], special_chars=" ", length=16),
        "FN-03": NamePolicy(charset="ascii", alphabet=["lower", "digit"], suffix=" ", length=14),
        "FN-04": NamePolicy(charset="ascii", alphabet=["lower", "digit"], special_chars="\n", length=16),
        "FN-05": NamePolicy(charset="ascii", alphabet=["lower", "digit"], suffix="\r", length=14),
        "FN-06": NamePolicy(charset="mixed", alphabet=["lower", "digit"],
                            unicode_blocks=["latin-supplement"], special_chars="_", length=16),
        "FN-07": NamePolicy(charset="ascii", alphabet=["lower", "digit", "dash"], length=200),
        "FN-08": NamePolicy(charset="unicode",
                            unicode_blocks=["cjk-unified", "arabic", "emoji"], length=12),
        "FN-09": NamePolicy(charset="ascii", alphabet=["lower", "digit"], length=14,
                            ext_override=[".tar.gz", ".tar.bz2", ""]),
        "FN-10": NamePolicy(charset="ascii", alphabet=["lower", "digit"],
                            special_chars=" ", suffix="\r", length=14),
        "FN-11": NamePolicy(charset="ascii", alphabet=["lower", "digit"], prefix=".", length=15),
        "FN-12": NamePolicy(charset="ascii", alphabet=["lower", "digit"], special_chars="$!&;|", length=14,
                            note="APPROX: shell metacharacters via special_chars pool."),
        "FN-13": NamePolicy(charset="ascii", alphabet=["lower", "digit"], special_chars=":<>", length=14,
                            note="APPROX: Windows-reserved chars via special_chars (valid bytes on Linux)."),
        "FN-14": NamePolicy(charset="ascii", alphabet=["lower", "digit"], prefix="CON_", length=12,
                            note="APPROX: only the CON_ device-name prefix."),
        "FN-15": NamePolicy(charset="ascii", alphabet=["lower", "digit"], special_chars="\t", length=15),
        "FN-16": NamePolicy(charset="ascii", alphabet=["lower", "digit"], prefix="-", length=15),
        "FN-17": NamePolicy(charset="mixed", alphabet=["lower", "digit"],
                            unicode_blocks=["latin-supplement"], length=14,
                            note="UNSUPPORTED: datagen cannot emit NFD-decomposed byte sequences."),
        "FN-18": NamePolicy(charset="mixed", alphabet=["lower", "digit"], special_chars="\u200b", length=14,
                            note="APPROX: zero-width space U+200B via special_chars pool."),
        "FN-19": NamePolicy(charset="ascii", alphabet=["lower", "digit"], prefix="doc   ", length=12,
                            note="APPROX: consecutive spaces via a fixed prefix."),
        "FN-20": NamePolicy(charset="ascii", alphabet=["lower", "digit"], length=16,
                            note="UNSUPPORTED: datagen cannot generate >1000-byte paths."),
    }
    key = fn.upper()
    if key not in m:
        raise ValueError(f"unknown filename variant {fn!r} (expected FN-01..FN-20)")
    return m[key]


def naming_from_spec(naming) -> NamePolicy:
    """Build a NamePolicy from a record's `naming` field (FN-id string or dict)."""
    if naming is None:
        return variant_policy("FN-01")
    if isinstance(naming, str):
        return variant_policy(naming)
    if isinstance(naming, dict):
        base = NamePolicy()
        for k in ("charset", "special_chars", "prefix", "suffix", "collision", "note"):
            if k in naming:
                setattr(base, k, naming[k])
        if "alphabet" in naming:
            base.alphabet = list(naming["alphabet"])
        if "unicode_blocks" in naming:
            base.unicode_blocks = list(naming["unicode_blocks"])
        if "length" in naming:
            base.length = int(naming["length"])
        if "ext_override" in naming:
            base.ext_override = list(naming["ext_override"])
        return base
    raise ValueError(f"invalid naming spec: {naming!r}")


# ── Reconciler (decide / suggest missing size|files|capacity) ─────────────────
@dataclass
class Plan:
    size: dict                # final size policy
    files: int                # final file count
    capacity: int             # exact or estimated total bytes
    capacity_exact: bool      # True when derived from a fixed size
    notes: list = field(default_factory=list)


def reconcile(size: Optional[dict], files: Optional[int],
              capacity: Optional[int], distribution: str = "uniform") -> Plan:
    """
    Given ANY TWO of {size, files, capacity} derive the third.
    Relationship: capacity ~= files * avg_size.
    """
    notes = []

    if size is not None and size["kind"] == "range" and distribution:
        size["dist"] = distribution

    if size is not None:
        a = avg_size(size)
        if files is not None and capacity is not None:
            # over-specified: keep size+files, validate against capacity
            exp = int(round(files * a))
            if capacity > 0 and abs(exp - capacity) / capacity > 0.05:
                notes.append(f"WARNING: given capacity {human(capacity)} disagrees with "
                             f"files x avg_size (~{human(exp)}); using files+size, capacity recomputed.")
            cap = files * a if size["kind"] == "range" else files * size["bytes"]
            return Plan(size, files, int(round(cap)), size["kind"] == "fixed", notes)
        if files is not None:
            cap = files * a if size["kind"] == "range" else files * size["bytes"]
            if size["kind"] == "range":
                notes.append(f"estimated capacity ~= {human(int(round(cap)))} "
                             f"(files x avg of range).")
            return Plan(size, files, int(round(cap)), size["kind"] == "fixed", notes)
        if capacity is not None:
            if a <= 0:
                raise ValueError("cannot derive file count: average size is 0 "
                                 "(provide `files` for a zero-byte dataset).")
            f = max(1, int(round(capacity / a)))
            cap = f * a if size["kind"] == "range" else f * size["bytes"]
            notes.append(f"derived files={f:,} from capacity/avg_size.")
            return Plan(size, f, int(round(cap)), size["kind"] == "fixed", notes)
        raise ValueError("`size` given but need also `files` or `total_capacity`.")

    # no size supplied
    if files is not None and capacity is not None:
        per = max(1, int(round(capacity / files)))
        notes.append(f"SUGGESTED fixed size={human(per)} (= capacity/files). "
                     f"Pass an explicit `size` range to override.")
        return Plan(fixed(per), files, per * files, True, notes)

    raise ValueError("provide at least two of {size, files, total_capacity} "
                     "(or files+total_capacity to let the engine suggest a size).")


# ── Topology planner ──────────────────────────────────────────────────────────
def plan_topology(n: int, depth: int, target_leaf: int = 500, max_fanout: int = 120):
    """Return ('flat', n) or ('tree', fanout, depth, per_dir) producing EXACTLY n files."""
    if depth <= 1 or n <= 1:
        return ("flat", n)
    best = None
    f = 2
    while f <= max_fanout:
        leaves = f ** depth
        if leaves > n:
            break
        if n % leaves == 0:
            per_dir = n // leaves
            score = abs(per_dir - target_leaf)
            if best is None or score < best[0]:
                best = (score, f, per_dir)
        f += 1
    if best:
        return ("tree", best[1], depth, best[2])
    return ("flat", n)


# ── YAML emission ─────────────────────────────────────────────────────────────
def yaml_dq(s: str) -> str:
    """Emit a YAML double-quoted scalar so control bytes become real bytes."""
    out = []
    for ch in s:
        if ch == "\\":
            out.append("\\\\")
        elif ch == '"':
            out.append('\\"')
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif ch == "\u200b":
            out.append("\\u200B")
        elif ord(ch) < 0x20:
            out.append(f"\\x{ord(ch):02X}")
        else:
            out.append(ch)
    return '"' + "".join(out) + '"'


def ext_token(t: str) -> str:
    if t == "":
        return '""'
    return t if t.startswith(".") else "." + t


def emit_content(content: str, sz: dict) -> list:
    lines = ["content:"]
    if content == "sparse":
        lines.append("  type: sparse")
        return lines
    if content == "fill":
        lines += ["  type: fill", "  fill_byte: 0x00"]
    else:
        lines.append("  type: random")
    lines.append("  buffer_size: 8MB")
    if size_max(sz) >= 256 * MB:
        lines += ["  direct_io:", "    enabled: true", "    min_size: 256MB"]
    else:
        lines.append("  direct_io: { enabled: false }")
    lines.append("  fsync: false")
    return lines


def emit_naming(pol: NamePolicy) -> list:
    lines = ["naming:", f"  charset: {pol.charset}"]
    if pol.charset in ("ascii", "mixed"):
        lines.append(f"  alphabet: [{', '.join(pol.alphabet)}]")
    if pol.charset in ("unicode", "mixed") and pol.unicode_blocks:
        lines.append("  unicode_blocks:")
        for b in pol.unicode_blocks:
            lines.append(f"    - {b}")
    if pol.special_chars:
        lines.append(f"  special_chars: {yaml_dq(pol.special_chars)}")
    if pol.prefix:
        lines.append(f"  prefix: {yaml_dq(pol.prefix)}")
    if pol.suffix:
        lines.append(f"  suffix: {yaml_dq(pol.suffix)}")
    lines.append(f"  length: {pol.length}")
    lines.append(f"  collision_strategy: {pol.collision}")
    return lines


def emit_size(sz: dict) -> list:
    if sz["kind"] == "fixed":
        return ["size:", "  type: fixed", f"  bytes: {sz['bytes']}"]
    return ["size:", "  type: range",
            f"  min: {sz['min']}", f"  max: {sz['max']}",
            f"  distribution: {sz['dist']}"]


def emit_extensions(types: list) -> list:
    lines = ["extensions:"]
    for t in types:
        lines.append(f"  - {{ ext: {ext_token(t)}, weight: 1 }}")
    return lines


def emit_topology(topo: tuple) -> list:
    if topo[0] == "flat":
        return ["flat:", f"  num_files: {topo[1]}"]
    _, fanout, depth, per_dir = topo
    return ["tree:", f"  fanout: {fanout}", f"  depth: {depth}",
            f"  files_per_dir: {per_dir}", "  files_in_each_dir: false"]


# ── Normalized requirement record ─────────────────────────────────────────────
@dataclass
class Record:
    id: Optional[str] = None
    name: Optional[str] = None
    category: Optional[str] = None
    size: Optional[dict] = None            # size policy (may be None -> suggest)
    files: Optional[int] = None
    capacity: Optional[int] = None
    depth: Optional[int] = None
    types: Optional[list] = None
    distribution: Optional[str] = None
    content: Optional[str] = None
    naming: object = None                  # FN-id str | dict | None
    profile: str = "dt2_100gbe"
    root_base: str = "/bryck/cloudcp"
    seed: Optional[int] = None
    tiers: Optional[list] = None           # multi-tier: list of normalized tier dicts


def _parse_tier(td: dict) -> dict:
    """Normalize one tier sub-dict (size/files/capacity/depth/content/types/...)."""
    dist = td.get("distribution")
    size = (parse_size_field(td.get("size"), dist or "uniform")
            if td.get("size") is not None else None)
    capkey = td.get("total_capacity", td.get("capacity"))
    cap = parse_size(capkey) if capkey is not None else None
    types = [str(t) for t in td["types"]] if td.get("types") is not None else None
    return {
        "size": size,
        "files": int(td["files"]) if td.get("files") is not None else None,
        "capacity": cap,
        "depth": int(td["depth"]) if td.get("depth") is not None else None,
        "content": td.get("content"),
        "types": types,
        "distribution": dist,
        "naming": td.get("naming"),
    }


def record_from_dict(d: dict) -> Record:
    dist = d.get("distribution")
    size = parse_size_field(d.get("size"), dist or "uniform") if d.get("size") is not None else None
    cap = parse_size(d["total_capacity"]) if d.get("total_capacity") is not None else None
    files = int(d["files"]) if d.get("files") is not None else None
    types = None
    if d.get("types") is not None:
        types = [str(t) for t in d["types"]]
    tiers = [_parse_tier(t) for t in d["tiers"]] if d.get("tiers") else None
    return Record(
        id=d.get("id"), name=d.get("name"), category=d.get("category"),
        size=size, files=files, capacity=cap,
        depth=int(d["depth"]) if d.get("depth") is not None else None,
        types=types, distribution=dist, content=d.get("content"),
        naming=d.get("naming"),
        profile=d.get("profile", "dt2_100gbe"),
        root_base=d.get("root_base", "/bryck/cloudcp"),
        seed=int(d["seed"]) if d.get("seed") is not None else None,
        tiers=tiers,
    )


# ── Validation ────────────────────────────────────────────────────────────────
def validate(rec: Record) -> tuple:
    errors, warnings = [], []
    if rec.category and rec.category not in CATEGORIES:
        warnings.append(f"unknown category {rec.category!r}; using generic defaults.")
    if rec.types:
        unknown = [t.lstrip(".") for t in rec.types
                   if t.lstrip(".") not in TYPES_ALL and t not in ("", ".")]
        if unknown:
            warnings.append(f"types not in the standard catalogue: {', '.join(unknown)} "
                            f"(emitted anyway).")
    if rec.content and rec.content not in ("random", "sparse", "fill"):
        errors.append(f"invalid content {rec.content!r} (random|sparse|fill).")
    if rec.distribution and rec.distribution not in ("uniform", "log-uniform", "normal"):
        errors.append(f"invalid distribution {rec.distribution!r}.")
    return errors, warnings


# ── Build spec(s) ─────────────────────────────────────────────────────────────
def build_one(rec: Record, ds_id: str, tier: dict, multi: bool, used: dict) -> tuple:
    """Build ONE spec from a normalized tier dict. Returns (filename, text, meta)."""
    defaults = category_defaults(rec.category)
    dist = tier.get("distribution") or rec.distribution or defaults.get("distribution", "uniform")

    size = tier.get("size") if tier.get("size") is not None else rec.size
    files_in = tier.get("files") if tier.get("files") is not None else rec.files
    cap_in = tier.get("capacity") if tier.get("capacity") is not None else rec.capacity

    # forced file count for some categories (e.g. Single-File Transfer)
    if files_in is None and cap_in is None and defaults.get("files") is not None:
        files_in = defaults["files"]

    plan = reconcile(size, files_in, cap_in, dist)
    bucket = bucket_for_policy(plan.size)

    # content: explicit(tier > record) > (sparse if zero) > category > random
    content = (tier.get("content") or rec.content
               or ("sparse" if bucket == "zero" else (defaults.get("content") or "random")))

    naming = tier.get("naming") if tier.get("naming") is not None else rec.naming
    pol = naming_from_spec(naming)

    types = tier.get("types") or rec.types or (pol.ext_override or TYPES_ALL)

    slabel = size_label(plan.size)
    depth = (tier.get("depth") if tier.get("depth") is not None
             else (rec.depth if rec.depth is not None else defaults.get("depth", 3)))
    topo = plan_topology(plan.files, depth)

    if multi:
        base = f"{ds_id}__{bucket}__{slabel}"
        if base in used:
            used[base] += 1
            fname = f"{base}__{used[base]}.yaml"
        else:
            used[base] = 1
            fname = f"{base}.yaml"
        root = f"{rec.root_base}/{ds_id}/{bucket}/{slabel}"
        if used[base] > 1:
            root = f"{root}/{used[base]}"
    else:
        fname = f"{ds_id}.yaml"
        root = f"{rec.root_base}/{ds_id}/{bucket}/{slabel}"

    seed = rec.seed if rec.seed is not None else (zlib.crc32(fname.encode()) & 0x7FFFFFFF)

    seal = BUCKET_SEAL.get(bucket)
    seal_txt = ("no batching (zero bucket)" if seal is None
                else f"max_files={seal[0]}, target_bytes={seal[1]}, open_batches={seal[2]}")
    cap_word = "exact" if plan.capacity_exact else "estimated"

    # ── describe what the input actually requested (before reconcile) ──
    given_size = tier.get("size") if tier.get("size") is not None else rec.size
    given_files = tier.get("files") if tier.get("files") is not None else rec.files
    given_cap = tier.get("capacity") if tier.get("capacity") is not None else rec.capacity
    given_types = tier.get("types") or rec.types
    types_label = (", ".join(given_types) if given_types else "all types (distributed by default)")
    naming_label = naming if isinstance(naming, str) else "FN-01 (default)"
    req = [
        f"#   size         : {size_desc(given_size)}",
        f"#   files        : {format(given_files, ',') if given_files is not None else '(derived from size+capacity)'}",
        f"#   capacity     : {human(given_cap) if given_cap is not None else '(derived from size+files)'}",
        f"#   depth        : {depth}",
        f"#   content      : {content}",
        f"#   distribution : {dist}",
        f"#   naming       : {naming_label}",
        f"#   types        : {types_label}",
    ]

    hdr = [
        "# " + "=" * 76,
        f"# Dataset   : {ds_id}",
        f"# Name      : {rec.name or ds_id}",
        f"# Category  : {rec.category or '(generic)'}",
        f"# Tier      : {bucket}   |   Size band: {slabel}",
        f"# Files     : {plan.files:,}   |   Capacity: {human(plan.capacity)} ({cap_word})",
        f"# Seal cfg  : {seal_txt}",
        f"# Profile   : {rec.profile}",
        "# " + "-" * 76,
        "# REQUESTED (this spec's input requirement):",
        *req,
        "# " + "-" * 76,
        "# Generated by specfile_engine.py from a compact requirement.",
        "# File TYPES are sampled from the weighted `extensions` catalogue below",
        "# (per-type share is therefore APPROXIMATE).",
    ]
    for n in plan.notes:
        hdr.append(f"# reconcile: {n}")
    if topo[0] == "flat" and depth > 1:
        hdr.append(f"# NOTE: count {plan.files:,} not divisible for a depth-{depth} tree; "
                   f"using flat mode to keep the count EXACT.")
    if pol.note:
        hdr.append(f"# naming: {pol.note}")
    hdr.append("# " + "=" * 76)

    body = ["", "version: 1"]
    body.append("mode: " + ("tree" if topo[0] == "tree" else "flat"))
    body.append(f"root: {root}")
    body.append("threads: 16")
    body.append(f"seed: {seed}")
    body.append("")
    body += emit_content(content, plan.size)
    body.append("")
    body += emit_naming(pol)
    body.append("")
    body += emit_topology(topo)
    body.append("")
    body += emit_size(plan.size)
    body.append("")
    body += emit_extensions(types)
    body.append("")

    text = "\n".join(hdr + body)
    meta = {"file": fname, "count": plan.files, "bucket": bucket,
            "variant": (naming if isinstance(naming, str) else "FN-01"),
            "size": slabel, "mode": topo[0], "root": root,
            "capacity": plan.capacity, "capacity_exact": plan.capacity_exact}
    return fname, text, meta


def build_specs(rec: Record, index: int) -> tuple:
    """Return (ds_id, multi, [(filename, text, meta), ...]) for one requirement."""
    ds_id = rec.id or f"DS-GEN-{index + 1:03d}"
    tiers = rec.tiers
    if not tiers:
        tiers = [{"size": rec.size, "files": rec.files, "capacity": rec.capacity,
                  "depth": rec.depth, "content": rec.content, "types": rec.types,
                  "distribution": rec.distribution, "naming": rec.naming}]
    multi = len(tiers) > 1
    used, out = {}, []
    for t in tiers:
        out.append(build_one(rec, ds_id, t, multi, used))
    return ds_id, multi, out


# ── Input adapters ────────────────────────────────────────────────────────────
def from_json(path: str) -> list:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        raise ValueError("JSON input must be an object or an array of objects.")
    return [record_from_dict(d) for d in data]


def from_jsonl(path: str) -> list:
    records = []
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                records.append(record_from_dict(json.loads(line)))
            except json.JSONDecodeError as e:
                raise ValueError(f"{path}:{lineno}: invalid JSON: {e}")
    return records


def from_yaml(path: str) -> list:
    try:
        import yaml  # optional
    except ImportError:
        raise SystemExit("Reading YAML input requires PyYAML (pip install pyyaml). "
                         "Use JSON/JSONL or CLI flags instead.")
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if isinstance(data, dict):
        data = [data]
    return [record_from_dict(d) for d in data]


def load(path: str) -> list:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".jsonl":
        return from_jsonl(path)
    if ext in (".yaml", ".yml"):
        return from_yaml(path)
    if ext == ".json":
        return from_json(path)
    # sniff: try JSONL first if multiple lines, else JSON
    with open(path, "r", encoding="utf-8") as fh:
        head = fh.read()
    stripped = head.lstrip()
    if stripped.startswith("{") and "\n{" in head:
        return from_jsonl(path)
    return from_json(path)


def from_cli(args) -> Record:
    dist = args.distribution
    size = parse_size_field(args.size, dist or "uniform") if args.size is not None else None
    cap = parse_size(args.capacity) if args.capacity is not None else None
    types = None
    if args.types:
        types = [t.strip() for t in args.types.split(",") if t.strip()]
    tiers = None
    if getattr(args, "tiers", None):
        try:
            raw = json.loads(args.tiers)
        except json.JSONDecodeError as e:
            raise SystemExit(f"--tiers is not valid JSON: {e}")
        if not isinstance(raw, list):
            raise SystemExit("--tiers must be a JSON array of tier objects.")
        tiers = [_parse_tier(t) for t in raw]
    return Record(
        id=args.id, name=args.name, category=args.category,
        size=size, files=args.files, capacity=cap,
        depth=args.depth, types=types, distribution=dist,
        content=args.content, naming=args.naming,
        profile=args.profile, root_base=args.root_base, seed=args.seed,
        tiers=tiers,
    )


# ── Driver ────────────────────────────────────────────────────────────────────
def generate(records: list, out_dir: str, root_base: Optional[str], dry_run: bool) -> dict:
    manifest = {"root_base": root_base or (records[0].root_base if records else "/bryck/cloudcp"),
                "datasets": []}
    to_write = []          # (abs_path, text)
    total_specs = 0
    grand_files = 0
    grand_cap = 0

    for i, rec in enumerate(records):
        if root_base:
            rec.root_base = root_base
        errors, warnings = validate(rec)
        for w in warnings:
            print(f"  [warn] {rec.id or f'#{i+1}'}: {w}")
        if errors:
            for e in errors:
                print(f"  [ERROR] {rec.id or f'#{i+1}'}: {e}")
            raise SystemExit(1)
        try:
            ds_id, multi, built = build_specs(rec, i)
        except ValueError as e:
            print(f"  [ERROR] {rec.id or f'#{i+1}'}: {e}")
            raise SystemExit(1)

        ds_files = sum(m["count"] for _, _, m in built)
        ds_cap = sum(m["capacity"] for _, _, m in built)
        grand_files += ds_files
        grand_cap += ds_cap
        total_specs += len(built)

        # per-dataset output directory when a dataset expands into >1 spec
        ds_dir = os.path.join(out_dir, ds_id) if multi else out_dir
        for fname, text, meta in built:
            to_write.append((os.path.join(ds_dir, fname), text))

        manifest["datasets"].append({
            "id": ds_id, "name": rec.name or ds_id,
            "category": rec.category, "profile": rec.profile,
            "emitted_files": ds_files, "spec_count": len(built),
            "capacity": ds_cap,
            "specs": [{k: m[k] for k in ("file", "count", "bucket", "variant",
                                         "size", "mode", "root")} for _, _, m in built],
        })

        tiers_txt = f"{len(built)} tier(s)" if multi else built[0][2]["bucket"]
        print(f"{ds_id:<16} {tiers_txt:<12} files={ds_files:>12,}  ~{human(ds_cap)}")

    if not dry_run:
        os.makedirs(out_dir, exist_ok=True)
        for path, text in to_write:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8", newline="\n") as fh:
                fh.write(text)

    print("-" * 78)
    print(f"TOTAL: {len(records)} dataset(s), {total_specs:,} spec file(s), "
          f"{grand_files:,} files, ~{human(grand_cap)} planned")

    write_manifest = (len(records) > 1 or total_specs > 1)
    if not dry_run and write_manifest:
        with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2)
        print(f"Wrote {total_specs} spec file(s) + manifest.json to {out_dir}/")
    elif not dry_run:
        print(f"Wrote {total_specs} spec file(s) to {out_dir}/")
    return manifest


# ── CLI ───────────────────────────────────────────────────────────────────────
def print_categories():
    print("Categories (use with --category):\n")
    for i, (name, info) in enumerate(CATEGORIES.items(), 1):
        print(f"  {i:>2}. {name}")
        print(f"      {info['desc']}")
    print()


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    here = os.path.dirname(os.path.abspath(__file__))
    epilog = (
        "INPUT FORMS\n"
        "  CLI    : provide any TWO of {--size,--files,--capacity} (+ optional flags),\n"
        "           or a full multi-tier dataset via --tiers '<json array>'.\n"
        "  --input: a .json (one object or an array), .jsonl (one object per line),\n"
        "           or .yaml file. One object = one dataset.\n"
        "\n"
        "SINGLE-TIER object (one size band):\n"
        '  {"id":"DS-1","name":"Small pure","size":"1MB-5MB","files":1000,"depth":3}\n'
        "\n"
        "MULTI-TIER object (one dataset, several bands -> one spec file per tier):\n"
        '  {"id":"DS-1","name":"Mixed","tiers":[\n'
        '     {"size":"0","files":500,"depth":3},\n'
        '     {"size":"200KB-600KB","files":60000,"depth":3},\n'
        '     {"size":"8GB-12GB","files":16,"depth":3}\n'
        "  ]}\n"
        "\n"
        "TIER FIELDS (all optional): size, files, total_capacity, depth, content,\n"
        "  types, distribution, naming. Missing fields fall back to the record-level\n"
        "  value, then to the category default. Provide any TWO of size/files/\n"
        "  total_capacity per tier; the engine derives the third.\n"
        "\n"
        "CLI multi-tier example:\n"
        '  --id DS-1 --tiers \'[{"size":"0","files":500},{"size":"1MB-5MB","files":1000}]\'\n'
        "\n"
        "Run with --list-categories to see all categories and descriptions.")
    ap = argparse.ArgumentParser(
        description="Generate a datagen spec file from a compact requirement.",
        epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)

    ap.add_argument("--input", help="JSON, JSONL (or YAML) requirement file.")
    ap.add_argument("--out", default=os.path.join(here, "generated_specs"),
                    help="Output directory (default: ./generated_specs).")
    ap.add_argument("--root-base", default=None,
                    help="Override the base path for generated data roots.")
    ap.add_argument("--dry-run", action="store_true", help="Plan + validate, write nothing.")
    ap.add_argument("--list-categories", action="store_true",
                    help="Print the 12 categories with descriptions and exit.")

    # single-requirement CLI fields
    ap.add_argument("--category", help="One of the 12 categories (see --list-categories).")
    ap.add_argument("--size", help="Fixed ('5MB','0') or range ('12KB-5MB').")
    ap.add_argument("--files", type=int, help="Total number of files.")
    ap.add_argument("--capacity", help="Total capacity budget, e.g. '50GB'.")
    ap.add_argument("--tiers", help="JSON array of tier objects for a multi-tier dataset, "
                    "e.g. '[{\"size\":\"0\",\"files\":500},{\"size\":\"1MB-5MB\",\"files\":1000}]'. "
                    "Each tier accepts size/files/total_capacity/depth/content/types/"
                    "distribution/naming; one spec file is written per tier.")
    ap.add_argument("--depth", type=int, help="Directory tree depth.")
    ap.add_argument("--types", help="Comma-separated extensions (else all distributed).")
    ap.add_argument("--distribution", choices=["uniform", "log-uniform", "normal"],
                    help="Size distribution for ranges (default uniform).")
    ap.add_argument("--content", choices=["random", "sparse", "fill"], help="Content type.")
    ap.add_argument("--naming", help="Filename variant FN-01..FN-20 (default FN-01).")
    ap.add_argument("--profile", default="dt2_100gbe", help="Scheduler profile label.")
    ap.add_argument("--seed", type=int, help="Fixed seed for reproducibility.")
    ap.add_argument("--id", help="Dataset id (default auto).")
    ap.add_argument("--name", help="Dataset name/label.")

    args = ap.parse_args()

    if args.list_categories:
        print_categories()
        return

    if args.input:
        records = load(args.input)
        if not records:
            print("No requirements found in input.")
            return
    else:
        if args.size is None and args.files is None and args.capacity is None and not args.tiers:
            ap.error("provide --input, --tiers, or at least two of {--size, --files, --capacity}. "
                     "See --list-categories.")
        records = [from_cli(args)]

    generate(records, args.out, args.root_base, args.dry_run)


if __name__ == "__main__":
    main()
