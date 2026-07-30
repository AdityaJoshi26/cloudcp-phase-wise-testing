#!/usr/bin/env python3
"""
generate_dataset.py — materialise a deterministic-enumeration scheduler dataset.

What it does
------------
1. Looks up a dataset in the embedded catalog (mirrors
   `deterministic_enumeration_datasets.md` §8 — 12 ORDER + 3 DEEP datasets).
2. Writes the five per-level `datagen` spec files under
   `spec_files/<DATASET_ID>/L<k>_<TIER>.yaml` (one flat-mode spec per chain level),
   with each spec's `root:` pointing under the /bryck data mount.
3. Invokes the `datagen` binary on each level, in BFS chain order (L0 → L4), so the
   data tree is created under /bryck exactly as the enumeration oracle predicts.

The spec files live inside CloudCpSchedulerTesting/ (this repo); the DATA bytes are
written to the /bryck mount only.

Usage
-----
    python3 generate_dataset.py <dataset> [options]

<dataset>:
    number 1..15            1..12 -> SCH-ORD-01..12 ,  13..15 -> SCH-DEEP-01..03
    id     SCH-ORD-07 | ORD-07 | ORD07 | SCH-DEEP-02 | DEEP-02 | DEEP2

Options:
    --datagen PATH      datagen binary            (default: /home/bryck/rperiyas/datagen)
    --data-root PATH    base dir for the DATA     (default: /bryck/cloudcp_sched_data)
    --spec-dir PATH     where spec files are written (default: <this dir>/spec_files)
    --content MODE      default | sparse | random (default: per-tier from catalog)
    --threads N         pass --threads N to datagen
    --write-specs-only  (re)write spec files, do NOT run datagen
    --all-specs         write spec files for ALL 15 datasets, then exit (no datagen)
    --dry-run           print the datagen commands without executing them
    --allow-non-bryck   permit a --data-root outside /bryck (guard is on by default)

Examples:
    # Write specs for every dataset (no data created):
    python3 generate_dataset.py --all-specs

    # Build dataset 1 (SCH-ORD-01) under /bryck with the default datagen path:
    python3 generate_dataset.py 1

    # Build SCH-DEEP-02 with a custom datagen binary and 32 writer threads:
    python3 generate_dataset.py DEEP-02 --datagen /opt/datagen --threads 32
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

# --------------------------------------------------------------------------------------
# Catalog (authoritative; mirrors deterministic_enumeration_datasets.md §2, §6, §8)
# --------------------------------------------------------------------------------------

# tier -> (size_string, files_per_block, name_prefix, default_content_type)
#   files_per_block = BATCH_SIZE * OPEN_BATCHES  (the deterministic "block", §2.2)
TIER = {
    "ZERO":   ("0",     8000, "z", "sparse"),
    "TINY":   ("16KB",  4088, "t", "random"),
    "SMALL":  ("2MB",   2536, "s", "random"),
    "MEDIUM": ("100MB", 400,  "m", "sparse"),
    "LARGE":  ("1GB",   40,   "l", "sparse"),
}

# tier -> BATCH_SIZE (M) and OPEN_BATCHES (K) — used only for the printed oracle summary
BATCH_M = {"ZERO": 2000, "TINY": 511, "SMALL": 317, "MEDIUM": 50, "LARGE": 5}
OPEN_K  = {"ZERO": 4,    "TINY": 8,   "SMALL": 8,   "MEDIUM": 8,  "LARGE": 8}

# profile -> per-tier depth R (number of blocks), §6.1 / §6.2
PROFILE_R = {
    "ORDER": {"ZERO": 2,   "TINY": 2,  "SMALL": 2,  "MEDIUM": 1, "LARGE": 1},
    "DEEP":  {"ZERO": 100, "TINY": 50, "SMALL": 25, "MEDIUM": 2, "LARGE": 2},
}

_TOK = {"Z": "ZERO", "T": "TINY", "S": "SMALL", "M": "MEDIUM", "L": "LARGE"}

# Curated orderings (§8.1 / §8.2), one 5-letter code per dataset.
_ORD_CODES = [
    "ZTSML",  # SCH-ORD-01  ascending
    "TSMLZ",  # SCH-ORD-02
    "SMLZT",  # SCH-ORD-03
    "MLZTS",  # SCH-ORD-04
    "LZTSM",  # SCH-ORD-05  large-first
    "LMSTZ",  # SCH-ORD-06  descending
    "MSTZL",  # SCH-ORD-07
    "STZLM",  # SCH-ORD-08
    "TZLMS",  # SCH-ORD-09
    "ZLMST",  # SCH-ORD-10
    "LSZTM",  # SCH-ORD-11  scenario
    "MZLTS",  # SCH-ORD-12  scenario
]
_DEEP_CODES = [
    "ZTSML",  # SCH-DEEP-01  balanced ascending backlog
    "LMSTZ",  # SCH-DEEP-02  large-first enumeration
    "TZSML",  # SCH-DEEP-03  tiny-first (WAN-like)
]


def build_catalog() -> dict[int, tuple[str, str, list[str]]]:
    """number -> (dataset_id, profile, ordering[list of tier names])."""
    cat: dict[int, tuple[str, str, list[str]]] = {}
    for i, code in enumerate(_ORD_CODES, start=1):
        cat[i] = (f"SCH-ORD-{i:02d}", "ORDER", [_TOK[c] for c in code])
    for j, code in enumerate(_DEEP_CODES, start=1):
        cat[12 + j] = (f"SCH-DEEP-{j:02d}", "DEEP", [_TOK[c] for c in code])
    return cat


CATALOG = build_catalog()


# --------------------------------------------------------------------------------------
# Resolution & spec generation
# --------------------------------------------------------------------------------------

def resolve(arg: str) -> int:
    """Map a user dataset selector (number or id-ish string) to a catalog number."""
    s = arg.strip().upper()
    if s.isdigit():
        n = int(s)
        if n in CATALOG:
            return n
        raise SystemExit(f"error: dataset number {n} out of range (1..{len(CATALOG)})")

    def norm(x: str) -> set[str]:
        base = x.upper()
        variants = {
            base,
            base.replace("SCH-", ""),
            base.replace("SCH-", "").replace("-", ""),
            base.replace("-", ""),
        }
        return variants

    want = norm(s)
    for n, (id_, _profile, _order) in CATALOG.items():
        if want & norm(id_):
            return n
    raise SystemExit(
        f"error: could not resolve dataset '{arg}'. "
        f"Use 1..{len(CATALOG)} or an id like SCH-ORD-07 / DEEP-02."
    )


def level_dirs(data_root: str, dataset_id: str, n_levels: int) -> list[str]:
    """Nested chain roots: base, base/L1, base/L1/L2, ...  (POSIX paths for /bryck)."""
    base = data_root.rstrip("/") + "/" + dataset_id
    dirs = [base]
    for k in range(1, n_levels):
        dirs.append(dirs[-1] + f"/L{k}")
    return dirs


def spec_text(root: str, tier: str, R: int, seed: int, content_mode: str) -> str:
    size, per_block, prefix, default_ctype = TIER[tier]
    ctype = default_ctype if content_mode == "default" else content_mode
    nfiles = per_block * R
    return (
        "version: 1\n"
        "mode: flat\n"
        f"root: {root}\n"
        f"seed: {seed}\n"
        "content:\n"
        f"  type: {ctype}\n"
        "size:\n"
        "  type: fixed\n"
        f"  bytes: {size}\n"
        "naming:\n"
        f'  prefix: "{prefix}-"\n'
        "  length: 12\n"
        "flat:\n"
        f"  num_files: {nfiles}\n"
    )


def write_specs(number: int, spec_dir: Path, data_root: str, content_mode: str):
    """Write the per-level spec files for one dataset. Returns (id, profile, [(spec,tier,root,R)])."""
    dataset_id, profile, order = CATALOG[number]
    Rmap = PROFILE_R[profile]
    dirs = level_dirs(data_root, dataset_id, len(order))
    out_dir = spec_dir / dataset_id
    out_dir.mkdir(parents=True, exist_ok=True)

    written = []
    for k, tier in enumerate(order):
        seed = 1000 * number + k
        txt = spec_text(dirs[k], tier, Rmap[tier], seed, content_mode)
        sp = out_dir / f"L{k}_{tier}.yaml"
        sp.write_text(txt)
        written.append((sp, tier, dirs[k], Rmap[tier]))
    return dataset_id, profile, written


# --------------------------------------------------------------------------------------
# Reporting & execution
# --------------------------------------------------------------------------------------

def print_oracle(dataset_id: str, profile: str, written) -> None:
    print(f"\n=== {dataset_id}  (profile: {profile}) ===")
    print("  enumeration order (BFS chain, top -> bottom):")
    print("  {:<6} {:<8} {:>10} {:>8} {:>8} {:>14}".format(
        "level", "tier", "files", "batches", "M/batch", "root"))
    tot_files = tot_batches = 0
    for k, (sp, tier, root, R) in enumerate(written):
        files = TIER[tier][1] * R
        batches = OPEN_K[tier] * R
        tot_files += files
        tot_batches += batches
        print("  L{:<5} {:<8} {:>10,} {:>8} {:>8} {:>14}".format(
            k, tier, files, batches, BATCH_M[tier], root))
    print("  {:<6} {:<8} {:>10,} {:>8}".format("TOTAL", "", tot_files, tot_batches))


def run_datagen(datagen: str, written, threads: int | None, dry_run: bool) -> None:
    for sp, tier, root, _R in written:
        cmd = [datagen, "--spec", str(sp)]
        if threads:
            cmd += ["--threads", str(threads)]
        print(f"\n[datagen] {tier:<7} -> {root}")
        print("          " + " ".join(cmd))
        if dry_run:
            continue
        subprocess.run(cmd, check=True)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        prog="generate_dataset.py",
        description="Generate a deterministic-enumeration scheduler dataset via datagen.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("dataset", nargs="?",
                    help="dataset number (1..15) or id (SCH-ORD-07 / DEEP-02)")
    ap.add_argument("--datagen", default="/home/bryck/rperiyas/datagen",
                    help="path to the datagen binary (default: %(default)s)")
    ap.add_argument("--data-root", default="/bryck/cloudcp_sched_data",
                    help="base dir for generated DATA, under /bryck (default: %(default)s)")
    ap.add_argument("--spec-dir", default=None,
                    help="dir for spec files (default: <script dir>/spec_files)")
    ap.add_argument("--content", choices=["default", "sparse", "random"], default="default",
                    help="override file content type (default: per-tier from catalog)")
    ap.add_argument("--threads", type=int, default=None,
                    help="pass --threads N to datagen")
    ap.add_argument("--write-specs-only", action="store_true",
                    help="write spec files only; do not run datagen")
    ap.add_argument("--all-specs", action="store_true",
                    help="write spec files for ALL datasets, then exit")
    ap.add_argument("--dry-run", action="store_true",
                    help="print datagen commands without executing them")
    ap.add_argument("--allow-non-bryck", action="store_true",
                    help="permit --data-root outside /bryck")
    args = ap.parse_args(argv)

    spec_dir = Path(args.spec_dir) if args.spec_dir else Path(__file__).resolve().parent / "spec_files"

    # /bryck mount guard — data must live on /bryck unless explicitly overridden.
    if not args.data_root.rstrip("/").startswith("/bryck") and not args.allow_non_bryck:
        raise SystemExit(
            f"error: --data-root '{args.data_root}' is not under /bryck. "
            f"Pass --allow-non-bryck to override."
        )

    # --all-specs: write every dataset's specs and exit (no datagen needed).
    if args.all_specs:
        for number in sorted(CATALOG):
            dataset_id, profile, written = write_specs(number, spec_dir, args.data_root, args.content)
            print_oracle(dataset_id, profile, written)
        print(f"\nWrote spec files for {len(CATALOG)} datasets under: {spec_dir}")
        return 0

    if not args.dataset:
        ap.error("a <dataset> selector is required (or use --all-specs)")

    number = resolve(args.dataset)
    dataset_id, profile, written = write_specs(number, spec_dir, args.data_root, args.content)
    print_oracle(dataset_id, profile, written)
    print(f"\nSpec files written under: {spec_dir / dataset_id}")

    if args.write_specs_only:
        return 0

    # Validate datagen binary before creating data.
    if not args.dry_run:
        dg = Path(args.datagen)
        if not dg.is_file():
            raise SystemExit(f"error: datagen binary not found: {args.datagen}")
        if not os.access(dg, os.X_OK):
            raise SystemExit(f"error: datagen binary is not executable: {args.datagen}")

    run_datagen(args.datagen, written, args.threads, args.dry_run)
    print(f"\nDone. Data root: {args.data_root.rstrip('/')}/{dataset_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
