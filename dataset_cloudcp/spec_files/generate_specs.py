#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
generate_specs.py — emit datagen spec files for the BryckCloud cloudcp
dataset generation plan (dataset_generation_plan.md, 54 datasets, P1-P12).

WHY THIS SCRIPT EXISTS / DESIGN NOTES
=====================================
The `datagen` tool (see DatagenSpecFileGuide.md) has hard limits that make it
IMPOSSIBLE to satisfy the plan with one spec per dataset:

  1. ONE naming policy per spec (single charset / length / prefix / suffix).
     => To get an EXACT count per filename variant we MUST emit one spec per
        variant. This script therefore "explodes" each dataset into
        (size-sub-range x filename-variant) specs.
  2. FN-07 needs length:200 (long-name stress; 240 overflows the 255-byte
     filesystem NAME_MAX once prefix + extension are added) -> isolated spec.
  3. Extension weights are sampled -> per-file-type % is APPROXIMATE.
  4. Some plan requirements are simply not expressible by datagen:
        FN-17 NFD normalization, FN-20 >1000-byte path, size-encoded filenames,
        real file content (valid JPEG/gzip), chmod, "files at every tree level".
     These are emitted BEST-EFFORT and flagged with an APPROX/UNSUPPORTED note
     in each affected spec's header comment.

OUTPUT LAYOUT
=============
    spec_files/
      <DATASET_ID>/
        <DATASET_ID>__<bucket>__<sizelabel>__<variant>.yaml
        ...
      manifest.json          (machine-readable index of everything emitted)

Each dataset gets its OWN directory. Filenames encode bucket + size band +
filename variant so they are self-describing.

USAGE
=====
    python generate_specs.py                       # write everything
    python generate_specs.py --list                # list datasets, write nothing
    python generate_specs.py --dry-run             # plan + validate, write nothing
    python generate_specs.py --datasets DS-P1-01,DS-P2-03
    python generate_specs.py --out /tmp/specs --root-base /bryck/cloudcp
"""

from __future__ import annotations

import argparse
import json
import os
import zlib
from dataclasses import dataclass, field
from typing import Optional

# ── Size unit helpers (binary, base-1024 — matches datagen's parser) ──────────
KB = 1024
MB = 1024 ** 2
GB = 1024 ** 3
TB = 1024 ** 4


def human(n: int) -> str:
    """Short binary size label, e.g. 0->'0b', 10240->'10kb', 1048576->'1mb'."""
    if n == 0:
        return "0b"
    for unit, factor in (("pb", 1024 ** 5), ("tb", TB), ("gb", GB), ("mb", MB), ("kb", KB)):
        if n >= factor and n % factor == 0:
            return f"{n // factor}{unit}"
    for unit, factor in (("pb", 1024 ** 5), ("tb", TB), ("gb", GB), ("mb", MB), ("kb", KB)):
        if n >= factor:
            return f"{n / factor:.0f}{unit}".replace(".0", "")
    return f"{n}b"


# ── File-type catalogues ──────────────────────────────────────────────────────
# "" means the no-extension slot (FN-09 / plan's "no-ext" type).
TYPES_12 = ["csv", "json", "txt", "log", "sql", "bin",
            "gz", "tar", "parquet", "jpg", "mp4", ""]

TYPES_26 = ["csv", "json", "txt", "log", "sql", "xml", "yaml",
            "bin", "gz", "tar", "zip", "zst", "bz2", "7z",
            "parquet", "avro", "orc", "arrow", "hdf5",
            "jpg", "png", "mp4", "mkv", "wav", "so", ""]

# ── Bucket sealing config (informational, printed into spec header comments) ──
BUCKET_SEAL = {
    "zero":   None,
    "tiny":   (2000, "256MB", 8),
    "small":  (512,  "2GB",   8),
    "medium": (64,   "10GB",  8),
    "large":  (8,    "50GB",  8),
}

FN_1_10 = [f"FN-{i:02d}" for i in range(1, 11)]
FN_ALL_20 = [f"FN-{i:02d}" for i in range(1, 21)]


# ── Filename-variant -> datagen naming policy mapping ─────────────────────────
# Each entry describes how to configure datagen's `naming:` block to realise a
# filename variant. `note` (when present) is surfaced in the spec header so the
# operator knows where datagen can only APPROXIMATE the plan requirement.
@dataclass
class NamePolicy:
    charset: str = "ascii"
    alphabet: list = field(default_factory=lambda: ["lower", "digit"])
    unicode_blocks: list = field(default_factory=list)
    special_chars: str = ""
    prefix: str = ""
    suffix: str = ""
    length: int = 16
    collision: str = "append-index"
    ext_override: Optional[list] = None   # FN-09: force these extensions
    note: str = ""                        # APPROX/UNSUPPORTED explanation


def variant_policy(fn: str) -> NamePolicy:
    """Return the naming policy realising filename variant `fn`."""
    m = {
        # ── Fully supported (FN-01 .. FN-10, plus a few of 11-20) ────────────
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
        # ── FN-11 .. FN-20 (mostly approximations) ───────────────────────────
        "FN-11": NamePolicy(charset="ascii", alphabet=["lower", "digit"], prefix=".", length=15),
        "FN-12": NamePolicy(charset="ascii", alphabet=["lower", "digit"], special_chars="$!&;|", length=14,
                            note="APPROX: shell metacharacters injected via special_chars pool; "
                                 "exact placement not guaranteed."),
        "FN-13": NamePolicy(charset="ascii", alphabet=["lower", "digit"], special_chars=":<>", length=14,
                            note="APPROX: Windows-reserved chars via special_chars (valid bytes on Linux)."),
        "FN-14": NamePolicy(charset="ascii", alphabet=["lower", "digit"], prefix="CON_", length=12,
                            note="APPROX: only the CON_ device-name prefix. PRN_/NUL_ need separate runs "
                                 "(one prefix per spec)."),
        "FN-15": NamePolicy(charset="ascii", alphabet=["lower", "digit"], special_chars="\t", length=15),
        "FN-16": NamePolicy(charset="ascii", alphabet=["lower", "digit"], prefix="-", length=15),
        "FN-17": NamePolicy(charset="mixed", alphabet=["lower", "digit"],
                            unicode_blocks=["latin-supplement"], length=14,
                            note="UNSUPPORTED: datagen cannot emit NFD-decomposed byte sequences; "
                                 "it only samples precomposed code points. Best-effort Latin accents only."),
        "FN-18": NamePolicy(charset="mixed", alphabet=["lower", "digit"], special_chars="\u200b", length=14,
                            note="APPROX: zero-width space U+200B injected via special_chars pool."),
        "FN-19": NamePolicy(charset="ascii", alphabet=["lower", "digit"], prefix="doc   ", length=12,
                            note="APPROX: consecutive spaces forced via a fixed prefix ('doc   ')."),
        "FN-20": NamePolicy(charset="ascii", alphabet=["lower", "digit"], length=16,
                            note="UNSUPPORTED: datagen cannot generate >1000-byte paths (dir names are fixed "
                                 "short). Increase tree depth at generation time to approximate."),
    }
    return m[fn]


# ── Size policy representation ────────────────────────────────────────────────
def fixed(n: int) -> dict:
    return {"kind": "fixed", "bytes": n}


def rng(lo: int, hi: int, dist: str = "uniform") -> dict:
    return {"kind": "range", "min": lo, "max": hi, "dist": dist}


def size_label(sz: dict) -> str:
    if sz["kind"] == "fixed":
        return human(sz["bytes"])
    return f'{human(sz["min"])}_{human(sz["max"])}'


def size_max(sz: dict) -> int:
    return sz["bytes"] if sz["kind"] == "fixed" else sz["max"]


# ── Tier / dataset registry structures ────────────────────────────────────────
@dataclass
class SubRange:
    count: int
    size: dict


@dataclass
class Tier:
    bucket: str                       # zero|tiny|small|medium|large
    depth: int
    subranges: list                   # list[SubRange]
    variants: list                    # list[str] filename-variant ids
    types: list                       # file-type catalogue
    content: str = "random"           # random|sparse|fill
    files_in_each_dir: bool = False   # deep-tree edge cases
    files_per_leaf_target: int = 500


@dataclass
class Dataset:
    id: str
    phase: int
    phase_name: str
    name: str
    tiers: list
    profile: str = "dt2_100gbe"
    summary_ref: Optional[int] = None      # plan Master Summary "Total Files" for cross-check
    empty_dirs: Optional[dict] = None      # special case: DS-P8-01
    notes: list = field(default_factory=list)


def seeds_tier(count: int, depth: int, bucket_variant: str = "FN-01") -> Tier:
    """Zero-byte seed files (sparse, single variant, single fixed size)."""
    return Tier(bucket="zero", depth=depth,
                subranges=[SubRange(count, fixed(0))],
                variants=[bucket_variant], types=TYPES_12, content="sparse")


# ══════════════════════════════════════════════════════════════════════════════
# THE REGISTRY — all 53 datasets, faithfully encoded from dataset_generation_plan.md
# (Where the plan is internally inconsistent, the Master Summary table wins.)
# ══════════════════════════════════════════════════════════════════════════════
def build_registry() -> list:
    ds: list = []

    # ── Phase 1 — Single-Tier Isolation ──────────────────────────────────────
    ds.append(Dataset(
        "DS-P1-01", 1, "Single-Tier Isolation", "Zero-Tier Pure (Large Scale)",
        summary_ref=5_000_000,
        tiers=[Tier("zero", 3, [SubRange(5_000_000, fixed(0))], FN_1_10, TYPES_12, content="sparse")],
        notes=["All files 0 bytes -> zero bucket. 12 types round-robin via extensions."]))

    ds.append(Dataset(
        "DS-P1-02", 1, "Single-Tier Isolation", "Tiny-Tier Pure (~500 GB)",
        summary_ref=1_010_000,
        tiers=[
            Tier("tiny", 4, [
                SubRange(200_000, rng(1, 10 * KB)),
                SubRange(300_000, rng(10 * KB, 100 * KB)),
                SubRange(500_000, rng(100 * KB, 1 * MB)),
            ], FN_1_10, TYPES_12),
            seeds_tier(10_000, 4),
        ],
        notes=["Uniform ranges yield ~294 GB; raise large-band max or use log-uniform to reach 500 GB."]))

    ds.append(Dataset(
        "DS-P1-03", 1, "Single-Tier Isolation", "Small-Tier Pure (~5 TB)",
        summary_ref=101_000,
        tiers=[
            Tier("small", 4, [
                SubRange(30_000, rng(1 * MB, 5 * MB)),
                SubRange(40_000, rng(5 * MB, 25 * MB)),
                SubRange(30_000, rng(25 * MB, 100 * MB)),
            ], FN_1_10, TYPES_12),
            seeds_tier(1_000, 4),
        ]))

    ds.append(Dataset(
        "DS-P1-04", 1, "Single-Tier Isolation", "Medium-Tier Pure (~5 TB)",
        summary_ref=10_100,
        tiers=[
            Tier("medium", 3, [
                SubRange(3_000, rng(100 * MB, 250 * MB)),
                SubRange(4_000, rng(250 * MB, 700 * MB)),
                SubRange(3_000, rng(700 * MB, 1 * GB)),
            ], FN_1_10, TYPES_12),
            seeds_tier(100, 3),
        ]))

    ds.append(Dataset(
        "DS-P1-05", 1, "Single-Tier Isolation", "Large-Tier Pure (Smoke, ~500 GB)",
        summary_ref=20,
        tiers=[Tier("large", 1, [
            SubRange(5, rng(5 * GB, 10 * GB)),
            SubRange(10, rng(10 * GB, 30 * GB)),
            SubRange(5, rng(30 * GB, 50 * GB)),
        ], FN_1_10, TYPES_12)]))

    ds.append(Dataset(
        "DS-P1-06", 1, "Single-Tier Isolation", "Large-Tier Pure (Perf baseline, ~10 TB)",
        summary_ref=200,
        tiers=[Tier("large", 2, [
            SubRange(40, rng(5 * GB, 15 * GB)),
            SubRange(100, rng(15 * GB, 60 * GB)),
            SubRange(60, rng(60 * GB, 100 * GB)),
        ], FN_1_10, TYPES_12)]))

    # ── Phase 2 — Batch Builder Mechanics (flat, exact counts) ───────────────
    # DS-P2-01 boundary probe handled specially (per-size prefix labels).
    boundary_sizes = [
        (0, "zero"), (1, "tiny"), (10 * KB, "tiny"), (999_999, "tiny"),
        (1 * MB, "small"), (63 * MB, "small"), (64 * MB, "small"), (99 * MB, "small"),
        (100 * MB, "medium"), (999 * MB, "medium"), (1 * GB, "large"),
    ]
    p201_tiers = [
        Tier(bucket, 1, [SubRange(10, fixed(sz))], FN_1_10, TYPES_12,
             content="sparse" if sz == 0 else "random")
        for sz, bucket in boundary_sizes
    ]
    ds.append(Dataset("DS-P2-01", 2, "Batch Builder Mechanics",
                      "Boundary Values Probe", summary_ref=110, tiers=p201_tiers,
                      notes=["Each size band's prefix encodes the size (e.g. 'boundary_64mb_').",
                             "10 files per boundary, one per FN-01..FN-10."]))

    ds.append(Dataset("DS-P2-02", 2, "Batch Builder Mechanics", "Tiny Count-Seal (2001)",
                      summary_ref=2001,
                      tiers=[Tier("tiny", 1, [SubRange(2001, fixed(100))], FN_1_10, TYPES_12)]))
    ds.append(Dataset("DS-P2-03", 2, "Batch Builder Mechanics", "Tiny Byte-Seal (260)",
                      summary_ref=260,
                      tiers=[Tier("tiny", 1, [SubRange(260, fixed(1 * MB))], FN_1_10, TYPES_12)]))
    ds.append(Dataset("DS-P2-04", 2, "Batch Builder Mechanics", "Small Count-Seal (513)",
                      summary_ref=513,
                      tiers=[Tier("small", 1, [SubRange(513, fixed(1 * MB))], FN_1_10, TYPES_12)]))
    ds.append(Dataset("DS-P2-05", 2, "Batch Builder Mechanics", "Medium Count-Seal (65)",
                      summary_ref=65,
                      tiers=[Tier("medium", 1, [SubRange(65, fixed(100 * MB))], FN_1_10, TYPES_12)]))
    ds.append(Dataset("DS-P2-06", 2, "Batch Builder Mechanics", "Large Count-Seal (9)",
                      summary_ref=9,
                      tiers=[Tier("large", 1, [SubRange(9, fixed(2 * GB))],
                                  FN_1_10[:9], TYPES_12)],
                      notes=["9 files -> FN-01..FN-09 (FN-10 skipped, insufficient files)."]))
    ds.append(Dataset("DS-P2-07", 2, "Batch Builder Mechanics", "Round-Robin Distribution (800)",
                      summary_ref=800,
                      tiers=[Tier("tiny", 1, [SubRange(800, fixed(100 * KB))], FN_1_10, TYPES_12)]))

    # ── Phase 3 — Exhaustion / Weight Shift (multi-tier + zero seeds) ────────
    # NOTE: the plan's Master Summary "Total Files" for Phase 3 counts TIERS ONLY
    # (zero-byte seeds are listed separately). summary_ref below is the TRUE total
    # (tiers + seeds); the plan's tiers-only figure is recorded in the notes.
    def p3(dsid, name, tiers_only, large, medium, small, tiny, seeds, exhaust):
        tiers = [
            Tier("large", 3, [SubRange(large, rng(8 * GB, 12 * GB))], FN_1_10, TYPES_12),
            Tier("medium", 3, [SubRange(medium, rng(400 * MB, 600 * MB))], FN_1_10, TYPES_12),
            Tier("small", 3, [SubRange(small, rng(1 * MB, 4 * MB))], FN_1_10, TYPES_12),
            Tier("tiny", 3, [SubRange(tiny, rng(200 * KB, 600 * KB))], FN_1_10, TYPES_12),
            seeds_tier(seeds, 3),
        ]
        return Dataset(dsid, 3, "Batch Exhaustion / Weight Shift", name,
                       summary_ref=tiers_only + seeds, tiers=tiers,
                       notes=[f"Exhaust-first tier: {exhaust}. Profile dt2_100gbe (6:4:3:3).",
                              f"Plan Master Summary lists {tiers_only:,} (tiers only); "
                              f"+{seeds:,} zero-byte seeds = {tiers_only + seeds:,} total."])

    ds.append(p3("DS-P3-01", "Large Exhausts First", 76_656, 16, 1280, 15360, 60000, 750, "large"))
    ds.append(p3("DS-P3-02", "Medium Exhausts First", 75_916, 300, 256, 15360, 60000, 600, "medium"))
    ds.append(p3("DS-P3-03", "Small Exhausts First", 62_604, 300, 1280, 1024, 60000, 600, "small"))
    ds.append(p3("DS-P3-04", "Tiny Exhausts First", 22_940, 300, 1280, 15360, 6000, 60, "tiny"))
    ds.append(p3("DS-P3-05", "Large+Medium Drain Together", 150_792, 8, 64, 30720, 120000, 1200,
                 "large+medium"))
    ds.append(p3("DS-P3-06", "Only Tiny Remains", 400_584, 8, 64, 512, 400000, 4000, "tiny (last standing)"))

    # ── Phase 4 — Filename & Encoding Stress (ALL 20 variants, 12 types) ─────
    ds.append(Dataset("DS-P4-01", 4, "Filename & Encoding Stress", "Filename Stress: Tiny",
                      summary_ref=20_000,
                      tiers=[Tier("tiny", 1, [SubRange(20_000, fixed(512 * KB))], FN_ALL_20, TYPES_12)]))
    ds.append(Dataset("DS-P4-02", 4, "Filename & Encoding Stress", "Filename Stress: Small",
                      summary_ref=4_800,
                      tiers=[Tier("small", 1, [SubRange(4_800, fixed(10 * MB))], FN_ALL_20, TYPES_12)]))
    ds.append(Dataset("DS-P4-03", 4, "Filename & Encoding Stress", "Filename Stress: Medium",
                      summary_ref=400,
                      tiers=[Tier("medium", 1, [SubRange(400, fixed(200 * MB))], FN_ALL_20, TYPES_12)]))
    ds.append(Dataset("DS-P4-04", 4, "Filename & Encoding Stress", "Filename Stress: Large",
                      summary_ref=100,
                      tiers=[Tier("large", 1, [SubRange(100, fixed(2 * GB))], FN_ALL_20, TYPES_12)]))
    ds.append(Dataset("DS-P4-05", 4, "Filename & Encoding Stress", "Filename Stress: Cross-Tier",
                      summary_ref=12_550,
                      tiers=[
                          Tier("tiny", 2, [SubRange(10_000, fixed(512 * KB))], FN_ALL_20, TYPES_12),
                          Tier("small", 2, [SubRange(2_000, fixed(10 * MB))], FN_ALL_20, TYPES_12),
                          Tier("medium", 2, [SubRange(400, fixed(200 * MB))], FN_ALL_20, TYPES_12),
                          Tier("large", 1, [SubRange(100, fixed(2 * GB))], FN_ALL_20, TYPES_12),
                          seeds_tier(50, 2),
                      ]))

    # ── Phase 5 — File Type Coverage (26 types, 20 variants) ─────────────────
    ds.append(Dataset("DS-P5-01", 5, "File Type Coverage", "All File Types, All Tiers",
                      summary_ref=32_110,
                      tiers=[
                          Tier("zero", 2, [SubRange(260, fixed(0))], FN_ALL_20, TYPES_26, content="sparse"),
                          Tier("tiny", 3, [SubRange(26_000, rng(100 * KB, 1 * MB))], FN_ALL_20, TYPES_26),
                          Tier("small", 3, [SubRange(5_200, rng(1 * MB, 50 * MB))], FN_ALL_20, TYPES_26),
                          Tier("medium", 2, [SubRange(520, rng(100 * MB, 800 * MB))], FN_ALL_20, TYPES_26),
                          Tier("large", 2, [SubRange(130, rng(2 * GB, 20 * GB))], FN_ALL_20, TYPES_26),
                      ],
                      notes=["Real file content (valid JPEG/gzip/parquet) is NOT produced by datagen; "
                             "content is random/sparse bytes only.",
                             "Per-type exact counts are APPROX (weighted sampling)."]))

    # ── Phase 6 — Network Profile Comparison (10 variants, 12 types) ─────────
    ds.append(Dataset("DS-P6-01", 6, "Network Profile Comparison", "Profile Comparison (All Tiers)",
                      summary_ref=71_140,
                      tiers=[
                          Tier("tiny", 3, [SubRange(60_000, rng(200 * KB, 600 * KB))], FN_1_10, TYPES_12),
                          Tier("small", 3, [SubRange(10_000, rng(1 * MB, 10 * MB))], FN_1_10, TYPES_12),
                          Tier("medium", 3, [SubRange(500, rng(200 * MB, 400 * MB))], FN_1_10, TYPES_12),
                          Tier("large", 3, [SubRange(40, rng(3 * GB, 8 * GB))], FN_1_10, TYPES_12),
                          seeds_tier(600, 3),
                      ],
                      notes=["Same on-disk dataset, run under dt2_100gbe (Run A) and wan_lowbw (Run B).",
                             "Batch-file SHA-256 must be identical across runs (use a fixed seed)."]))

    # ── Phase 7 — Mixed Full-Pipeline (10 variants, 12 types) ────────────────
    ds.append(Dataset("DS-P7-01", 7, "Mixed Full-Pipeline", "Mixed Pipeline: Small (~300 GB)",
                      summary_ref=91_320,
                      tiers=[
                          Tier("zero", 3, [SubRange(5_000, fixed(0))], FN_1_10, TYPES_12, content="sparse"),
                          Tier("tiny", 3, [SubRange(80_000, rng(200 * KB, 800 * KB))], FN_1_10, TYPES_12),
                          Tier("small", 3, [SubRange(6_000, rng(1 * MB, 10 * MB))], FN_1_10, TYPES_12),
                          Tier("medium", 3, [SubRange(300, rng(200 * MB, 400 * MB))], FN_1_10, TYPES_12),
                          Tier("large", 3, [SubRange(20, rng(5 * GB, 12 * GB))], FN_1_10, TYPES_12),
                      ]))
    ds.append(Dataset("DS-P7-02", 7, "Mixed Full-Pipeline", "Mixed Pipeline: Medium (~3 TB)",
                      summary_ref=582_150,
                      tiers=[
                          Tier("zero", 4, [SubRange(50_000, fixed(0))], FN_1_10, TYPES_12, content="sparse"),
                          Tier("tiny", 4, [SubRange(500_000, rng(200 * KB, 800 * KB))], FN_1_10, TYPES_12),
                          Tier("small", 4, [SubRange(30_000, rng(1 * MB, 20 * MB))], FN_1_10, TYPES_12),
                          Tier("medium", 4, [SubRange(2_000, rng(300 * MB, 700 * MB))], FN_1_10, TYPES_12),
                          Tier("large", 4, [SubRange(150, rng(5 * GB, 20 * GB))], FN_1_10, TYPES_12),
                      ]))
    ds.append(Dataset("DS-P7-03", 7, "Mixed Full-Pipeline", "Mixed Pipeline: Full (~10 TB)",
                      summary_ref=1_166_200,
                      tiers=[
                          Tier("zero", 4, [SubRange(100_000, fixed(0))], FN_1_10, TYPES_12, content="sparse"),
                          Tier("tiny", 4, [SubRange(1_000_000, rng(200 * KB, 800 * KB))], FN_1_10, TYPES_12),
                          Tier("small", 4, [SubRange(60_000, rng(5 * MB, 50 * MB))], FN_1_10, TYPES_12),
                          Tier("medium", 4, [SubRange(6_000, rng(400 * MB, 1 * GB))], FN_1_10, TYPES_12),
                          Tier("large", 4, [SubRange(200, rng(10 * GB, 30 * GB))], FN_1_10, TYPES_12),
                      ]))

    # ── Phase 8 — Configuration Edge Cases ───────────────────────────────────
    ds.append(Dataset("DS-P8-01", 8, "Configuration Edge Cases", "Empty Source Directory",
                      summary_ref=0, tiers=[], empty_dirs={"fanout": 3, "depth": 1},
                      notes=["Creates 3 empty subdirectories, zero files (files_per_dir: 0)."]))
    ds.append(Dataset("DS-P8-02", 8, "Configuration Edge Cases", "Single Zero-Byte File",
                      summary_ref=1,
                      tiers=[Tier("zero", 1, [SubRange(1, fixed(0))], ["FN-08"], TYPES_12, content="sparse")]))
    ds.append(Dataset("DS-P8-03", 8, "Configuration Edge Cases", "Single Huge File (100 GB)",
                      summary_ref=1,
                      tiers=[Tier("large", 1, [SubRange(1, fixed(100 * GB))], ["FN-07"], TYPES_12)]))
    ds.append(Dataset("DS-P8-04", 8, "Configuration Edge Cases", "Deep Directory Tree (14 levels)",
                      summary_ref=750,
                      tiers=[Tier("tiny", 14, [SubRange(700, rng(100 * KB, 5 * MB))],
                                  FN_1_10, TYPES_12, files_in_each_dir=True)],
                      notes=["APPROX: datagen cannot place exactly 5 files x 10 dirs per level. "
                             "Uses files_in_each_dir over a 14-deep chain; count/shape approximate."]))
    ds.append(Dataset("DS-P8-05", 8, "Configuration Edge Cases", "Unreadable Subdirectory",
                      summary_ref=500,
                      tiers=[Tier("tiny", 3, [SubRange(500, rng(1 * KB, 1 * MB))], FN_1_10, TYPES_12)],
                      notes=["Only the 500 readable files are generated. The 200-file unreadable subdir "
                             "and 'chmod 000' are a MANUAL runtime step (not expressible in a spec)."]))

    # ── Phase 9 — Single-File Transfer (flat, 1 file each) ───────────────────
    def p9(dsid, name, bucket, size, fn, ftype):
        return Dataset(dsid, 9, "Single-File Transfer", name, summary_ref=1,
                       tiers=[Tier(bucket, 1, [SubRange(1, fixed(size))], [fn], [ftype],
                                   content="sparse" if size == 0 else "random")])

    ds.append(p9("DS-P9-01", "Single 1 B (tiny)", "tiny", 1, "FN-04", "bin"))
    ds.append(p9("DS-P9-02", "Single 1 MB (tiny->small)", "small", 1 * MB, "FN-12", "bin"))
    ds.append(p9("DS-P9-03", "Single 63 MB (below multipart)", "small", 63 * MB, "FN-16", "gz"))
    ds.append(p9("DS-P9-04", "Single 64 MB (first multipart)", "small", 64 * MB, "FN-13", "bin"))
    ds.append(p9("DS-P9-05", "Single 100 MB (small->medium)", "medium", 100 * MB, "FN-18", "parquet"))
    ds.append(p9("DS-P9-06", "Single 1 GB (medium->large)", "large", 1 * GB, "FN-08", "mp4"))
    ds.append(p9("DS-P9-07", "Single 100 GB (large)", "large", 100 * GB, "FN-07", "bin"))

    # ── Phase 10 — Sub-Range Isolation (20 variants, 26 types) ───────────────
    ds.append(Dataset("DS-P10-01", 10, "Sub-Range Isolation", "0B-1MB (zero+tiny) 1M",
                      summary_ref=1_000_000,
                      tiers=[
                          Tier("zero", 3, [SubRange(50_000, fixed(0))], FN_ALL_20, TYPES_26, content="sparse"),
                          Tier("tiny", 3, [SubRange(950_000, rng(1, 1 * MB))], FN_ALL_20, TYPES_26),
                      ]))
    ds.append(Dataset("DS-P10-02", 10, "Sub-Range Isolation", "1B-10KB sub-tiny 1M",
                      summary_ref=1_000_000,
                      tiers=[Tier("tiny", 3, [SubRange(1_000_000, rng(1, 10 * KB))], FN_ALL_20, TYPES_26)]))
    ds.append(Dataset("DS-P10-03", 10, "Sub-Range Isolation", "10KB-1MB tiny 1M",
                      summary_ref=1_000_000,
                      tiers=[Tier("tiny", 4, [SubRange(1_000_000, rng(10 * KB, 1 * MB))], FN_ALL_20, TYPES_26)]))
    ds.append(Dataset("DS-P10-04", 10, "Sub-Range Isolation", "1MB-4MB small 500K",
                      summary_ref=500_000,
                      tiers=[Tier("small", 3, [SubRange(500_000, rng(1 * MB, 4 * MB))], FN_ALL_20, TYPES_26)]))
    ds.append(Dataset("DS-P10-05", 10, "Sub-Range Isolation", "4MB-16MB small 500K",
                      summary_ref=500_000,
                      tiers=[Tier("small", 3, [SubRange(500_000, rng(4 * MB, 16 * MB))], FN_ALL_20, TYPES_26)]))
    ds.append(Dataset("DS-P10-06", 10, "Sub-Range Isolation", "Fixed 16MB small 500K",
                      summary_ref=500_000,
                      tiers=[Tier("small", 3, [SubRange(500_000, fixed(16 * MB))], FN_ALL_20, TYPES_26)]))
    ds.append(Dataset("DS-P10-07", 10, "Sub-Range Isolation", "10GB-120GB large 30",
                      summary_ref=30,
                      tiers=[Tier("large", 1, [SubRange(30, rng(10 * GB, 120 * GB))], FN_ALL_20, TYPES_26)]))
    ds.append(Dataset("DS-P10-08", 10, "Sub-Range Isolation", "200GB-500GB large 10",
                      summary_ref=10,
                      tiers=[Tier("large", 1, [SubRange(10, rng(200 * GB, 500 * GB))], FN_1_10, TYPES_26)],
                      notes=["10 files -> FN-01..FN-10 one each (per plan)."]))

    # ── Phase 11 — Alternative Weight Ratios (20 variants, 26 types) ─────────
    ds.append(Dataset("DS-P11-01", 11, "Alternative Weight Ratios", "Weights 7:5:3:1 (~5 TB)",
                      summary_ref=979_140,
                      tiers=[
                          Tier("large", 4, [SubRange(140, rng(10 * GB, 20 * GB))], FN_ALL_20, TYPES_26),
                          Tier("medium", 4, [SubRange(3_200, rng(300 * MB, 700 * MB))], FN_ALL_20, TYPES_26),
                          Tier("small", 4, [SubRange(188_000, rng(1 * MB, 10 * MB))], FN_ALL_20, TYPES_26),
                          Tier("tiny", 4, [SubRange(780_000, rng(200 * KB, 600 * KB))], FN_ALL_20, TYPES_26),
                          seeds_tier(7_800, 4),
                      ],
                      notes=["Scheduler weights large:7 medium:5 small:3 tiny:1."]))
    ds.append(Dataset("DS-P11-02", 11, "Alternative Weight Ratios", "Weights 9:5:2:0 (~5 TB)",
                      summary_ref=129_637,
                      tiers=[
                          Tier("large", 3, [SubRange(187, rng(10 * GB, 20 * GB))], FN_ALL_20, TYPES_26),
                          Tier("medium", 3, [SubRange(3_200, rng(300 * MB, 700 * MB))], FN_ALL_20, TYPES_26),
                          Tier("small", 3, [SubRange(125_000, rng(1 * MB, 10 * MB))], FN_ALL_20, TYPES_26),
                          seeds_tier(1_250, 3),
                      ],
                      notes=["Scheduler weights large:9 medium:5 small:2 tiny:0 (tiny intentionally empty)."]))
    ds.append(Dataset("DS-P11-03", 11, "Alternative Weight Ratios", "Weights 10:6:0:0 (~5 TB)",
                      summary_ref=4_047,
                      tiers=[
                          Tier("large", 3, [SubRange(207, rng(10 * GB, 20 * GB))], FN_ALL_20, TYPES_26),
                          Tier("medium", 3, [SubRange(3_800, rng(300 * MB, 700 * MB))], FN_ALL_20, TYPES_26),
                          seeds_tier(40, 3),
                      ],
                      notes=["Scheduler weights large:10 medium:6 small:0 tiny:0 (small+tiny empty)."]))

    # ── Phase 12 — Tiny/Small-Heavy Mixed (20 variants, 26 types) ───────────
    # High tiny count, second-highest small, plus 1,000 each of zero/medium/large
    # for tier coverage. tiny:small ≈ 70:30 of the non-"other" files.
    ds.append(Dataset("DS-P12-01", 12, "Tiny/Small-Heavy Mixed", "Tiny/Small Heavy 1M",
                      summary_ref=1_000_000,
                      tiers=[
                          Tier("zero", 3, [SubRange(1_000, fixed(0))], FN_ALL_20, TYPES_26, content="sparse"),
                          Tier("tiny", 4, [SubRange(697_000, rng(200 * KB, 1 * MB))], FN_ALL_20, TYPES_26),
                          Tier("small", 4, [SubRange(300_000, rng(1 * MB, 10 * MB))], FN_ALL_20, TYPES_26),
                          Tier("medium", 3, [SubRange(1_000, rng(300 * MB, 700 * MB))], FN_ALL_20, TYPES_26),
                          Tier("large", 2, [SubRange(1_000, rng(1 * GB, 2 * GB))], FN_ALL_20, TYPES_26),
                      ],
                      notes=["tiny:small ≈ 70:30 of the 997k non-other files; "
                             "zero/medium/large = 1,000 each for tier coverage."]))
    ds.append(Dataset("DS-P12-02", 12, "Tiny/Small-Heavy Mixed", "Tiny/Small Heavy 2M",
                      summary_ref=2_000_000,
                      tiers=[
                          Tier("zero", 3, [SubRange(1_000, fixed(0))], FN_ALL_20, TYPES_26, content="sparse"),
                          Tier("tiny", 4, [SubRange(1_397_000, rng(200 * KB, 1 * MB))], FN_ALL_20, TYPES_26),
                          Tier("small", 4, [SubRange(600_000, rng(1 * MB, 10 * MB))], FN_ALL_20, TYPES_26),
                          Tier("medium", 3, [SubRange(1_000, rng(300 * MB, 700 * MB))], FN_ALL_20, TYPES_26),
                          Tier("large", 2, [SubRange(1_000, rng(1 * GB, 2 * GB))], FN_ALL_20, TYPES_26),
                      ],
                      notes=["tiny:small ≈ 70:30 of the 1,997k non-other files; "
                             "zero/medium/large = 1,000 each for tier coverage."]))

    return ds


# ── Topology planner ──────────────────────────────────────────────────────────
def plan_topology(n: int, depth: int, target_leaf: int = 500, max_fanout: int = 120):
    """
    Return a topology that produces EXACTLY `n` files.
      ('flat', n)                        -> single directory, n files
      ('tree', fanout, depth, per_dir)   -> fanout^depth leaf dirs x per_dir files
    Tree is used only when n is exactly divisible for the requested depth,
    otherwise we fall back to flat (which is always exact).
    """
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


def emit_naming(pol: NamePolicy, size_prefix: str = "") -> list:
    lines = ["naming:", f"  charset: {pol.charset}"]
    if pol.charset in ("ascii", "mixed"):
        lines.append(f"  alphabet: [{', '.join(pol.alphabet)}]")
    if pol.charset in ("unicode", "mixed") and pol.unicode_blocks:
        lines.append("  unicode_blocks:")
        for b in pol.unicode_blocks:
            lines.append(f"    - {b}")
    if pol.special_chars:
        lines.append(f"  special_chars: {yaml_dq(pol.special_chars)}")
    prefix = size_prefix + pol.prefix
    if prefix:
        lines.append(f"  prefix: {yaml_dq(prefix)}")
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


def emit_topology(topo: tuple, files_in_each_dir: bool) -> list:
    if topo[0] == "flat":
        return ["flat:", f"  num_files: {topo[1]}"]
    _, fanout, depth, per_dir = topo
    lines = ["tree:", f"  fanout: {fanout}", f"  depth: {depth}",
             f"  files_per_dir: {per_dir}",
             f"  files_in_each_dir: {'true' if files_in_each_dir else 'false'}"]
    return lines


def build_spec_text(ds: Dataset, tier: Tier, sub: SubRange, fn: str, count: int,
                    root_base: str) -> tuple:
    """Return (filename, yaml_text, meta) for one (subrange x variant) spec."""
    pol = variant_policy(fn)
    types = pol.ext_override if pol.ext_override else tier.types
    slug = fn.lower().replace("-", "")
    slabel = size_label(sub.size)
    fname = f"{ds.id}__{tier.bucket}__{slabel}__{slug}.yaml"

    # Boundary probe (DS-P2-01): encode the size in the filename prefix.
    size_prefix = ""
    if ds.id == "DS-P2-01":
        size_prefix = f"boundary_{slabel}_"

    # Topology: deep-tree edge case forces its own shape.
    if tier.files_in_each_dir:
        depth = tier.depth
        per_dir = max(1, round(count / (depth + 1)))
        topo = ("tree", 1, depth, per_dir)
        # files_in_each_dir places `per_dir` files in every directory of the
        # chain, and a depth-N chain has (N + 1) directories (root + N levels).
        # Reconcile the advertised count with what datagen actually emits.
        count = per_dir * (depth + 1)
    else:
        topo = plan_topology(count, tier.depth, tier.files_per_leaf_target)

    root = f"{root_base}/{ds.id}/{tier.bucket}/{slabel}/{slug}"
    seed = zlib.crc32(fname.encode()) & 0x7FFFFFFF

    seal = BUCKET_SEAL.get(tier.bucket)
    seal_txt = ("no batching (zero bucket)" if seal is None
                else f"max_files={seal[0]}, target_bytes={seal[1]}, open_batches={seal[2]}")

    # ── Header comment ──
    hdr = [
        "# " + "═" * 76,
        f"# Dataset   : {ds.id}  (Phase {ds.phase} — {ds.phase_name})",
        f"# Name      : {ds.name}",
        f"# Tier      : {tier.bucket}   |   Filename variant: {fn}",
        f"# Size band : {slabel}   |   Files in THIS spec: {count:,}",
        f"# Seal cfg  : {seal_txt}",
        f"# Profile   : {ds.profile}",
        "# " + "─" * 76,
        "# Generated by generate_specs.py from dataset_generation_plan.md.",
        "# One spec per (size-band x filename-variant): datagen supports only",
        "# ONE naming policy per spec, so this is how exact per-variant counts",
        "# are achieved. File TYPES are sampled from the weighted `extensions`",
        "# catalogue below (per-type share is therefore APPROXIMATE).",
    ]
    if pol.note:
        hdr += ["# " + "─" * 76, f"# {fn} {pol.note}"]
    if topo[0] == "flat" and tier.depth > 1:
        hdr += ["# NOTE: exact count not divisible for the requested tree depth "
                f"({tier.depth}); using flat mode (single dir) to keep the count exact."]
    for n in ds.notes:
        hdr.append(f"# {n}")
    hdr.append("# " + "═" * 76)

    body = ["", "version: 1"]
    body.append("mode: " + ("tree" if topo[0] == "tree" else "flat"))
    body.append(f"root: {root}")
    body.append("threads: 16")
    body.append(f"seed: {seed}")
    body.append("")
    body += emit_content(tier.content, sub.size)
    body.append("")
    body += emit_naming(pol, size_prefix)
    body.append("")
    body += emit_topology(topo, tier.files_in_each_dir)
    body.append("")
    body += emit_size(sub.size)
    body.append("")
    body += emit_extensions(types)
    body.append("")

    text = "\n".join(hdr + body)
    meta = {"file": fname, "count": count, "bucket": tier.bucket,
            "variant": fn, "size": slabel, "mode": topo[0], "root": root}
    return fname, text, meta


def emit_empty_dirs_spec(ds: Dataset, root_base: str) -> tuple:
    fanout = ds.empty_dirs["fanout"]
    depth = ds.empty_dirs["depth"]
    root = f"{root_base}/{ds.id}/empty"
    fname = f"{ds.id}__empty_source.yaml"
    hdr = [
        "# " + "═" * 76,
        f"# Dataset   : {ds.id}  (Phase {ds.phase} — {ds.phase_name})",
        f"# Name      : {ds.name}",
        "# " + "─" * 76,
        f"# Creates {fanout} empty subdirectories and ZERO files (files_per_dir: 0).",
        "# Verifies scan_state=complete, no batch files, empty source.index.",
        "# " + "═" * 76,
    ]
    body = [
        "", "version: 1", "mode: tree", f"root: {root}", "threads: 4", "",
        "content:", "  type: sparse", "",
        "tree:", f"  fanout: {fanout}", f"  depth: {depth}",
        "  files_per_dir: 0", "  files_in_each_dir: false", "",
        "size:", "  type: fixed", "  bytes: 0", "",
    ]
    return fname, "\n".join(hdr + body), {"file": fname, "count": 0, "bucket": "zero",
                                          "variant": "-", "size": "0b", "mode": "tree", "root": root}


# ── Even split helper ─────────────────────────────────────────────────────────
def even_split(total: int, n: int) -> list:
    base, r = divmod(total, n)
    return [base + (1 if i < r else 0) for i in range(n)]


# ── Driver ────────────────────────────────────────────────────────────────────
def generate(datasets, out_dir, root_base, dry_run):
    manifest = {"root_base": root_base, "datasets": []}
    total_specs = 0
    total_files = 0

    for ds in datasets:
        ds_dir = os.path.join(out_dir, ds.id)
        specs = []
        ds_files = 0

        if ds.empty_dirs is not None:
            fname, text, meta = emit_empty_dirs_spec(ds, root_base)
            specs.append((fname, text, meta))
        else:
            for tier in ds.tiers:
                for sub in tier.subranges:
                    counts = even_split(sub.count, len(tier.variants))
                    for fn, c in zip(tier.variants, counts):
                        if c <= 0:
                            continue
                        fname, text, meta = build_spec_text(ds, tier, sub, fn, c, root_base)
                        specs.append((fname, text, meta))
                        ds_files += meta["count"]

        # cross-check vs plan Master Summary
        status = "n/a"
        if ds.summary_ref is not None:
            status = "MATCH" if ds_files == ds.summary_ref else f"DIFF (plan={ds.summary_ref:,})"

        print(f"{ds.id:<12} {ds.name:<42} specs={len(specs):<4} "
              f"files={ds_files:>12,}  [{status}]")

        if not dry_run:
            os.makedirs(ds_dir, exist_ok=True)
            for fname, text, _ in specs:
                with open(os.path.join(ds_dir, fname), "w", encoding="utf-8", newline="\n") as fh:
                    fh.write(text)

        manifest["datasets"].append({
            "id": ds.id, "phase": ds.phase, "name": ds.name,
            "profile": ds.profile, "summary_ref": ds.summary_ref,
            "emitted_files": ds_files, "spec_count": len(specs),
            "status": status,
            "specs": [m for _, _, m in specs],
        })
        total_specs += len(specs)
        total_files += ds_files

    print("-" * 96)
    print(f"TOTAL: {len(datasets)} datasets, {total_specs:,} spec files, {total_files:,} files planned")

    if not dry_run:
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2)
        print(f"Wrote specs to {out_dir}/ and manifest.json")
    return manifest


def main():
    # Windows consoles default to cp1252; force UTF-8 so summary output never crashes.
    try:
        import sys
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="Generate datagen spec files from the dataset plan.")
    ap.add_argument("--out", default=here, help="Output directory (default: this script's dir).")
    ap.add_argument("--root-base", default="/bryck/cloudcp", help="Base path for generated data roots.")
    ap.add_argument("--datasets", default="", help="Comma-separated dataset IDs to emit (default: all).")
    ap.add_argument("--dry-run", action="store_true", help="Plan + validate, write nothing.")
    ap.add_argument("--list", action="store_true", help="List datasets and exit.")
    args = ap.parse_args()

    registry = build_registry()

    if args.list:
        for ds in registry:
            n = sum(s.count for t in ds.tiers for s in t.subranges) if ds.tiers else 0
            print(f"{ds.id:<12} P{ds.phase:<2} {ds.name:<44} files={n:,}")
        return

    if args.datasets:
        wanted = {d.strip() for d in args.datasets.split(",") if d.strip()}
        registry = [d for d in registry if d.id in wanted]
        missing = wanted - {d.id for d in registry}
        if missing:
            print("Unknown dataset ids: " + ", ".join(sorted(missing)))

    generate(registry, args.out, args.root_base, args.dry_run)


if __name__ == "__main__":
    main()
