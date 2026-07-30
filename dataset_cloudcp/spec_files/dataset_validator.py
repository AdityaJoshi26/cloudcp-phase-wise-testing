#!/usr/bin/env python3
"""
dataset_validator.py  –  Run datagen spec files phase-by-phase, validate
output file counts, clean up generated data, and produce a JSON run report.

BEHAVIOUR
---------
  For each dataset in the requested phase range the script will:
    1. Read every .yaml spec file from the dataset's spec directory.
    2. Rewrite each spec's `root` path so generated files land under
       --output-base (or a temporary directory) instead of the original
       hard-coded /bryck/cloudcp/... path.
    3. Run the datagen binary for all spec files in parallel (--parallel).
    4. Validate each spec individually as soon as it finishes: count the
       files datagen created and compare the count to the expected value in
       manifest.json.  A spec passes when
         |actual - expected| / expected * 100 <= --tolerance
    5. After all specs of a dataset complete, delete the dataset output
       directory (unless --keep-on-fail is set and a spec failed).
    6. After all datasets, print a summary table and write a JSON report
       if --report was requested.

QUICK EXAMPLES
--------------
  # Show what would happen for phases 1-3 (nothing is created or deleted)
  python dataset_validator.py --phase-from 1 --phase-to 3 --dry-run

  # Run a single phase (all its datasets)
  python dataset_validator.py --one-phase 1 \\
      --datagen ./datagen --output-base /tmp/gen

  # Run exactly one dataset: phase 1, dataset 2  (DS-P1-02)
  python dataset_validator.py --one-phase 1 --dataset-num 2 \\
      --datagen ./datagen --output-base /tmp/gen

  # Run phase 1, datagen at ./datagen, output to /tmp/gen, write report
  python dataset_validator.py --phase-from 1 --phase-to 1 \\
      --datagen ./datagen --output-base /tmp/gen --report results.json

  # Run two specific datasets, keep dirs on failure, log to file
  python dataset_validator.py --datasets DS-P1-01,DS-P2-03 \\
      --keep-on-fail --log run.log

  # Interactive mode — fill in settings and confirm each dataset
  python dataset_validator.py --ask --phase-from 2 --phase-to 4

  # 5% tolerance, stop on first failure, 8 parallel specs per dataset
  python dataset_validator.py --tolerance 5 --stop-on-fail --parallel 8

  # Keep generated data after validation (skip auto-delete)
  python dataset_validator.py --phase-from 1 --output-base /tmp/gen --skip-delete

  # Delete previously generated data for specific datasets
  python dataset_validator.py --delete --output-base /tmp/gen \
      --datasets DS-P1-01,DS-P1-02

  # Delete all DS-P1-* directories under /tmp/gen (dry-run first)
  python dataset_validator.py --delete --output-base /tmp/gen \
      --phase-from 1 --phase-to 1 --dry-run

ENVIRONMENT
-----------
  DATAGEN_BIN   Path to the datagen binary (overridden by --datagen)
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import json
import logging
import os
import pathlib
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Tuple

import yaml  # PyYAML  —  pip install pyyaml

# ─────────────────────────────────────────────────────────────────────────────
# Version / constants
# ─────────────────────────────────────────────────────────────────────────────

VERSION        = "1.0.0"
MANIFEST_FNAME = "manifest.json"
DS_ID_PATTERN  = re.compile(r"^DS-P(\d+)-\d+$")


# ─────────────────────────────────────────────────────────────────────────────
# ANSI colour helpers  (disabled on Windows / non-TTY)
# ─────────────────────────────────────────────────────────────────────────────

_USE_COLOUR = sys.stdout.isatty() and platform.system() != "Windows"


class C:
    RST = "\033[0m"  if _USE_COLOUR else ""
    BLD = "\033[1m"  if _USE_COLOUR else ""
    DIM = "\033[2m"  if _USE_COLOUR else ""
    RED = "\033[91m" if _USE_COLOUR else ""
    GRN = "\033[92m" if _USE_COLOUR else ""
    YLW = "\033[93m" if _USE_COLOUR else ""
    CYN = "\033[96m" if _USE_COLOUR else ""


# ─────────────────────────────────────────────────────────────────────────────
# Result data-classes  (serialise cleanly via dataclasses.asdict)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SpecResult:
    spec_file:      str
    dataset_id:     str
    expected_count: int
    actual_count:   int   = -1
    status:         str   = "PENDING"   # PASS | FAIL | ERROR | SKIPPED
    duration_s:     float = 0.0
    error:          str   = ""
    spec_root:      str   = ""


@dataclass
class DatasetResult:
    dataset_id:     str
    name:           str
    phase:          int
    spec_results:   List[SpecResult] = field(default_factory=list)
    status:         str   = "PENDING"   # PASS | FAIL | ERROR | SKIPPED
    total_expected: int   = 0
    total_actual:   int   = 0
    duration_s:     float = 0.0
    error:          str   = ""


@dataclass
class RunReport:
    timestamp:      str
    command_line:   str
    datagen_bin:    str
    output_base:    str
    spec_dir:       str
    phase_from:     int
    phase_to:       int
    tolerance_pct:  float
    dry_run:        bool
    datasets:       List[DatasetResult] = field(default_factory=list)
    summary:        dict = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(log_file: Optional[str], verbose: bool = False) -> logging.Logger:
    """Console at INFO (or DEBUG if verbose); optional file at DEBUG with timestamps."""
    log = logging.getLogger("dsv")
    log.setLevel(logging.DEBUG)
    log.propagate = False

    # Wrap stdout in a UTF-8 TextIOWrapper so Unicode symbols render correctly
    # on Windows terminals that default to a narrow encoding (e.g. CP1252).
    import io as _io
    _out = sys.stdout
    if hasattr(_out, "buffer"):
        try:
            _out = _io.TextIOWrapper(
                _out.buffer, encoding="utf-8", errors="replace",
                line_buffering=True,
            )
        except Exception:
            _out = sys.stdout  # fall back to original

    ch = logging.StreamHandler(_out)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    # In verbose mode, prefix console lines with level so DEBUG detail is clear
    ch.setFormatter(logging.Formatter(
        "%(levelname)-7s %(message)s" if verbose else "%(message)s"
    ))
    log.addHandler(ch)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-7s  [%(threadName)-28s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        log.addHandler(fh)

    return log


# ─────────────────────────────────────────────────────────────────────────────
# Argument parser
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dataset_validator.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    sel = p.add_argument_group("dataset selection")
    sel.add_argument(
        "--spec-dir", "-s", metavar="DIR", default=os.getcwd(),
        help="Directory containing DS-P*/ spec folders + manifest.json  "
             "(default: current working directory)",
    )
    sel.add_argument(
        "--phase-from", type=int, default=1, metavar="N",
        help="First phase to run, inclusive  [1-12]  (default: 1)",
    )
    sel.add_argument(
        "--phase-to", type=int, default=11, metavar="N",
        help="Last phase to run, inclusive   [1-12]  (default: 11)",
    )
    sel.add_argument(
        "--one-phase", type=int, default=None, metavar="N",
        help="Run a SINGLE phase [1-12]. Shortcut that sets both "
             "--phase-from and --phase-to to N (overrides them). "
             "Without --dataset-num, runs ALL datasets in that phase; "
             "combined with --dataset-num, runs only those dataset numbers.",
    )
    sel.add_argument(
        "--datasets", metavar="IDs",
        help="Comma-separated explicit dataset IDs to run; overrides phase range  "
             "e.g. DS-P1-01,DS-P2-03  (mutually exclusive with --dataset-num)",
    )
    sel.add_argument(
        "--dataset-num", "-d", metavar="N[,N...]",
        help="Dataset number(s) within each phase, comma-separated. "
             "Combined with --phase-from/--phase-to to select exact datasets. "
             "e.g. '--phase-from 1 --phase-to 1 --dataset-num 2' runs DS-P1-02; "
             "'--phase-from 1 --phase-to 3 --dataset-num 1' runs DS-P1-01, "
             "DS-P2-01, DS-P3-01.  Mutually exclusive with --datasets.",
    )

    ex = p.add_argument_group("execution")
    ex.add_argument(
        "--datagen", metavar="PATH",
        default=os.environ.get("DATAGEN_BIN", "./datagen"),
        help="Path to the datagen binary  "
             "(default: ./datagen or $DATAGEN_BIN)",
    )
    ex.add_argument(
        "--output-base", metavar="DIR", default=None,
        help="Base directory for generated files. Each spec's `root` is "
             "rewritten to land under this path. If omitted, a temporary "
             "directory is created automatically and removed at the end.",
    )
    ex.add_argument(
        "--parallel", type=int, default=4, metavar="N",
        help="Number of spec files to run in parallel per dataset  (default: 4)",
    )

    val = p.add_argument_group("validation")
    val.add_argument(
        "--tolerance", type=float, default=0.0, metavar="PCT",
        help="Allowed %% deviation from expected file count  "
             "(default: 0 = exact match)",
    )

    beh = p.add_argument_group("behaviour")
    beh.add_argument(
        "--keep-on-fail", action="store_true",
        help="Do not delete the output directory when any spec fails",
    )
    beh.add_argument(
        "--stop-on-fail", action="store_true",
        help="Stop processing after the first dataset that fails or errors",
    )
    beh.add_argument(
        "--skip-delete", action="store_true",
        help="Keep output directories after validation — skip the automatic "
             "post-run deletion regardless of pass/fail status",
    )
    beh.add_argument(
        "--delete", action="store_true",
        help="Delete mode: remove previously generated dataset directories "
             "under --output-base without running validation. "
             "Requires --output-base. Targets are determined by --datasets "
             "or --phase-from/--phase-to (all matching DS-P* dirs are found "
             "by scanning --output-base directly, no datagen binary needed).",
    )
    beh.add_argument(
        "--dry-run", action="store_true",
        help="Show plan without running datagen or creating/deleting files",
    )
    beh.add_argument(
        "--ask", action="store_true",
        help="Prompt for missing settings at startup and confirm before "
             "each dataset",
    )

    out = p.add_argument_group("output")
    out.add_argument(
        "--verbose", "-v", action="store_true",
        help="Verbose console output: show all DEBUG detail (datagen "
             "stdout/stderr, file-count timing, per-spec progress, commands) "
             "on the console. Same detail that --log writes to a file.",
    )
    out.add_argument(
        "--report", metavar="FILE",
        help="Write a full JSON run report to this path",
    )
    out.add_argument(
        "--log", metavar="FILE",
        help="Write detailed debug log (all levels) to this path",
    )
    out.add_argument(
        "--version", action="version", version=f"%(prog)s {VERSION}",
    )

    return p


def _check_args(args: argparse.Namespace) -> Optional[str]:
    # --one-phase is a shortcut that overrides the phase range
    if args.one_phase is not None:
        if not 1 <= args.one_phase <= 12:
            return f"--one-phase must be 1-12, got {args.one_phase}"
        args.phase_from = args.one_phase
        args.phase_to   = args.one_phase
    if not 1 <= args.phase_from <= 12:
        return f"--phase-from must be 1-12, got {args.phase_from}"
    if not 1 <= args.phase_to <= 12:
        return f"--phase-to must be 1-12, got {args.phase_to}"
    if args.phase_from > args.phase_to:
        return (f"--phase-from ({args.phase_from}) must be <= "
                f"--phase-to ({args.phase_to})")
    if args.parallel < 1:
        return f"--parallel must be >= 1, got {args.parallel}"
    if not 0.0 <= args.tolerance <= 100.0:
        return f"--tolerance must be 0-100, got {args.tolerance}"
    if args.delete and not args.output_base:
        return "--delete requires --output-base <dir> (the root where datasets were generated)"
    if args.skip_delete and args.keep_on_fail:
        return "--skip-delete and --keep-on-fail are mutually exclusive"
    if args.dataset_num:
        if args.datasets:
            return "--dataset-num and --datasets are mutually exclusive"
        try:
            nums = [int(x.strip()) for x in args.dataset_num.split(",") if x.strip()]
            if not nums:
                return "--dataset-num requires at least one number"
            bad = [n for n in nums if n < 1]
            if bad:
                return f"--dataset-num values must be >= 1, got: {bad}"
        except ValueError:
            return (f"--dataset-num must be comma-separated integers, "
                    f"got: {args.dataset_num!r}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Manifest
# ─────────────────────────────────────────────────────────────────────────────

def load_manifest(spec_dir: str) -> Tuple[Dict[str, dict], str]:
    """
    Load manifest.json and return (index_by_dataset_id, root_base).
    Raises FileNotFoundError if the manifest is missing.
    """
    path = os.path.join(spec_dir, MANIFEST_FNAME)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"manifest.json not found: {path}")
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    index = {ds["id"]: ds for ds in raw.get("datasets", [])}
    root_base = raw.get("root_base", "/bryck/cloudcp")
    return index, root_base


def discover_datasets(
    spec_dir: str,
    phase_from: int,
    phase_to: int,
    ds_filter: Optional[List[str]],
    manifest: Dict[str, dict],
) -> List[Tuple[str, pathlib.Path, dict]]:
    """
    Scan spec_dir for DS-P* directories that match the phase range / filter.
    Returns a sorted list of (dataset_id, dir_path, manifest_entry).
    """
    base    = pathlib.Path(spec_dir)
    results = []
    for entry in sorted(base.iterdir()):
        if not entry.is_dir():
            continue
        m = DS_ID_PATTERN.match(entry.name)
        if not m:
            continue
        ds_id = entry.name
        phase = int(m.group(1))
        if ds_filter is not None and ds_id not in ds_filter:
            continue
        if ds_filter is None and not (phase_from <= phase <= phase_to):
            continue
        if ds_id not in manifest:
            continue
        results.append((ds_id, entry, manifest[ds_id]))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Spec rewriting
# ─────────────────────────────────────────────────────────────────────────────

def rewrite_spec_root(
    spec_path:   pathlib.Path,
    root_base:   str,
    output_base: str,
) -> Tuple[pathlib.Path, pathlib.Path]:
    """
    Read a spec YAML file, replace its `root` value so that generated files
    land under output_base instead of the original root_base path.

    Replacement is done at the text level so YAML comments are preserved.
    The rewritten spec is written to the system temp directory (never
    touches the source spec tree).

    Returns
    -------
    tmp_spec_path : pathlib.Path
        Path to the temporary spec file.  The caller must delete it.
    new_root_fs   : pathlib.Path
        The resolved output root directory for filesystem operations.
    """
    text     = spec_path.read_text(encoding="utf-8")
    doc      = yaml.safe_load(text)
    orig_root: str = doc.get("root", "")

    if not orig_root:
        raise ValueError(
            f"spec '{spec_path.name}' has no 'root' field "
            f"(mode={doc.get('mode')!r})"
        )

    # Compute the relative sub-path from root_base
    if orig_root.startswith(root_base):
        rel = orig_root[len(root_base):].lstrip("/")
    else:
        rel = orig_root.lstrip("/")

    rel_parts = rel.replace("\\", "/").split("/")

    # Path that goes into the YAML: always forward slashes (datagen is POSIX)
    new_root_yaml = (
        output_base.replace("\\", "/").rstrip("/")
        + "/"
        + "/".join(rel_parts)
    )

    # Path used for host filesystem operations: native separators
    new_root_fs = pathlib.Path(output_base, *rel_parts)

    # Text-level replacement preserves field order, comments, and blank lines
    new_text = text.replace(
        f"root: {orig_root}",
        f"root: {new_root_yaml}",
        1,
    )

    # Write temp file to system temp dir — never contaminates source tree
    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".yaml",
        prefix=f"dsv_{spec_path.stem}_",
        dir=tempfile.gettempdir(),
        delete=False,
        encoding="utf-8",
    )
    tmp.write(new_text)
    tmp.close()

    return pathlib.Path(tmp.name), new_root_fs


# ─────────────────────────────────────────────────────────────────────────────
# datagen execution
# ─────────────────────────────────────────────────────────────────────────────

def run_datagen(
    spec_path:   pathlib.Path,
    bin_path:    str,
    logger:      logging.Logger,
    dry_run:     bool,
) -> Tuple[bool, str]:
    """
    Execute:  <bin_path> --spec <spec_path>

    Returns (success, error_message).
    Logs datagen stdout/stderr to DEBUG; logs elapsed time.
    """
    cmd = [bin_path, "--spec", str(spec_path)]
    logger.debug("CMD: %s", " ".join(cmd))

    if dry_run:
        return True, ""

    t_start = time.perf_counter()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        return False, f"binary not found: {bin_path!r}"
    except OSError as exc:
        return False, str(exc)

    elapsed = round(time.perf_counter() - t_start, 2)

    # Log all datagen output so it appears in the --log file
    if proc.stdout.strip():
        for line in proc.stdout.strip().splitlines():
            logger.debug("    [datagen stdout] %s", line)
    if proc.stderr.strip():
        # Log stderr at WARNING when the process failed, DEBUG otherwise
        lvl = logging.WARNING if proc.returncode != 0 else logging.DEBUG
        for line in proc.stderr.strip().splitlines():
            logger.log(lvl, "    [datagen stderr] %s", line)

    if proc.returncode != 0:
        raw_err = (proc.stderr or proc.stdout or "non-zero exit").strip()[:400]
        logger.debug("  datagen FAILED  elapsed=%.2fs  spec=%s", elapsed, spec_path.name)
        return False, f"exit {proc.returncode}: {raw_err}"

    logger.debug("  datagen OK  elapsed=%.2fs  spec=%s", elapsed, spec_path.name)
    return True, ""


# ─────────────────────────────────────────────────────────────────────────────
# File-count validation helpers
# ─────────────────────────────────────────────────────────────────────────────

def count_files_recursive(root: pathlib.Path) -> int:
    """Count all regular files anywhere under root."""
    if not root.is_dir():
        return 0
    total = 0
    for _dirpath, _dirnames, filenames in os.walk(root):
        total += len(filenames)
    return total


def within_tolerance(actual: int, expected: int, tol_pct: float) -> bool:
    if expected == 0:
        return actual == 0
    return abs(actual - expected) / expected * 100.0 <= tol_pct


# ─────────────────────────────────────────────────────────────────────────────
# Per-spec worker   (called from ThreadPoolExecutor)
# ─────────────────────────────────────────────────────────────────────────────

def _run_spec(
    spec_file:   str,
    ds_dir:      pathlib.Path,
    spec_meta:   dict,           # manifest "specs" entry for this file
    ds_id:       str,
    root_base:   str,
    output_base: str,
    datagen_bin: str,
    tolerance:   float,
    dry_run:     bool,
    logger:      logging.Logger,
) -> SpecResult:
    """
    End-to-end lifecycle for a single spec file:
      rewrite root  →  run datagen  →  count files  →  validate
    """
    spec_path = ds_dir / spec_file
    expected  = spec_meta.get("count", 0)
    result    = SpecResult(
        spec_file=spec_file,
        dataset_id=ds_id,
        expected_count=expected,
    )
    t0       = time.perf_counter()
    tmp_path: Optional[pathlib.Path] = None

    try:
        # ── 1. Rewrite spec root ──────────────────────────────────────────
        tmp_path, new_root = rewrite_spec_root(spec_path, root_base, output_base)
        result.spec_root   = str(new_root)

        if not dry_run:
            new_root.mkdir(parents=True, exist_ok=True)

        logger.debug("[%s] tmp_spec=%s", ds_id, tmp_path.name)
        logger.info(
            "  %s⟳ START%s  %-54s  expected=%s  →  %s",
            C.DIM, C.RST, spec_file, f"{expected:,}", new_root,
        )

        # ── 2. Run datagen ────────────────────────────────────────────────
        t_gen = time.perf_counter()
        ok, err = run_datagen(tmp_path, datagen_bin, logger, dry_run)
        gen_elapsed = round(time.perf_counter() - t_gen, 1)

        if not ok:
            result.status       = "ERROR"
            result.error        = err
            result.actual_count = 0
            logger.warning(
                "  %s✗ ERR %s  %-54s  expected=%-8d  %s",
                C.RED, C.RST, spec_file, expected, err,
            )
            return result

        # ── 3. Validate file count ────────────────────────────────────────
        if dry_run:
            result.actual_count = expected
            result.status       = "PASS"
            logger.info(
                "  %s~ DRY%s  %-54s  expected=%d",
                C.YLW, C.RST, spec_file, expected,
            )
            return result

        actual = count_files_recursive(new_root)
        passed = within_tolerance(actual, expected, tolerance)
        result.actual_count = actual
        result.status       = "PASS" if passed else "FAIL"

        if passed:
            logger.info(
                "  %s✓ PASS%s  %-54s  expected=%-8d  actual=%d",
                C.GRN, C.RST, spec_file, expected, actual,
            )
        else:
            diff = actual - expected
            logger.warning(
                "  %s✗ FAIL%s  %-54s  expected=%-8d  actual=%-8d  diff=%+d",
                C.RED, C.RST, spec_file, expected, actual, diff,
            )

    except Exception as exc:  # noqa: BLE001
        result.status = "ERROR"
        result.error  = str(exc)
        logger.error(
            "  %s✗ ERR %s  %-54s  exception: %s",
            C.RED, C.RST, spec_file, exc,
        )

    finally:
        # Always remove the temporary spec file
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        result.duration_s = round(time.perf_counter() - t0, 3)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Dataset orchestration
# ─────────────────────────────────────────────────────────────────────────────

def process_dataset(
    ds_id:          str,
    ds_dir:         pathlib.Path,
    manifest_entry: dict,
    root_base:      str,
    output_base:    str,
    args:           argparse.Namespace,
    logger:         logging.Logger,
) -> DatasetResult:
    """
    Run all spec files of one dataset in parallel, validate each,
    roll up the status, and clean up the output directory.
    """
    result = DatasetResult(
        dataset_id=ds_id,
        name=manifest_entry.get("name", ds_id),
        phase=manifest_entry.get("phase", 0),
        total_expected=manifest_entry.get("emitted_files", 0),
    )
    t0 = time.perf_counter()

    # Manifest lookup: filename -> spec entry
    spec_lookup: Dict[str, dict] = {
        s["file"]: s for s in manifest_entry.get("specs", [])
    }

    # Discover spec YAML files; exclude any leftover dsv_ temp files
    spec_files = sorted(
        f.name
        for f in ds_dir.glob("*.yaml")
        if not f.name.startswith("dsv_")
    )
    if not spec_files:
        result.status = "SKIPPED"
        result.error  = "No .yaml spec files found in directory"
        logger.warning("[%s] No spec files — skipping.", ds_id)
        return result

    # ── Header ────────────────────────────────────────────────────────────
    ds_out_dir = pathlib.Path(output_base) / ds_id
    logger.info(
        "\n%s%s── [%s]  %s%s"
        "\n%s   phase=%-2d  specs=%-4d  expected_files=%s  parallel=%d"
        "\n   output: %s%s",
        C.BLD, C.CYN, ds_id, result.name, C.RST,
        C.DIM, result.phase, len(spec_files),
        f"{result.total_expected:,}", args.parallel,
        ds_out_dir, C.RST,
    )
    if args.dry_run:
        logger.info(
            "  %s[dry-run]%s  %d spec(s) would run  (parallel=%d)",
            C.YLW, C.RST, len(spec_files), args.parallel,
        )

    # ── Parallel execution ────────────────────────────────────────────────
    workers      = min(args.parallel, len(spec_files))
    spec_results: List[SpecResult] = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix=ds_id[:12],
    ) as pool:
        futures: Dict[concurrent.futures.Future, str] = {
            pool.submit(
                _run_spec,
                sf,
                ds_dir,
                spec_lookup.get(sf, {"count": 0}),
                ds_id,
                root_base,
                output_base,
                args.datagen,
                args.tolerance,
                args.dry_run,
                logger,
            ): sf
            for sf in spec_files
        }
        n_done = 0
        n_total_specs = len(spec_files)
        for fut in concurrent.futures.as_completed(futures):
            sf = futures[fut]
            try:
                sr = fut.result()
                spec_results.append(sr)
                n_done += 1
                logger.debug(
                    "  [%d/%d complete]  %s  status=%s  duration=%.1fs",
                    n_done, n_total_specs, sr.spec_file,
                    sr.status, sr.duration_s,
                )
            except Exception as exc:  # noqa: BLE001
                n_done += 1
                logger.error(
                    "[%s] Unhandled thread error for %s: %s", ds_id, sf, exc,
                )
                spec_results.append(SpecResult(
                    spec_file=sf, dataset_id=ds_id, expected_count=0,
                    status="ERROR", error=str(exc),
                ))

    # ── Dataset-level rollup ──────────────────────────────────────────────
    result.spec_results  = spec_results
    result.total_actual  = sum(
        r.actual_count for r in spec_results if r.actual_count >= 0
    )
    result.duration_s    = round(time.perf_counter() - t0, 2)

    statuses     = {r.status for r in spec_results}
    result.status = (
        "ERROR" if "ERROR" in statuses else
        "FAIL"  if "FAIL"  in statuses else
        "PASS"
    )

    # ── Summary line ──────────────────────────────────────────────────────
    n_passed = sum(1 for r in spec_results if r.status == "PASS")
    n_total  = len(spec_results)
    col      = C.GRN if result.status == "PASS" else C.RED
    logger.info(
        "\n  %s%s%s  [%s]  specs %d/%d  "
        "|  files actual=%s expected=%s  |  %.1fs",
        col, result.status, C.RST, ds_id,
        n_passed, n_total,
        f"{result.total_actual:,}", f"{result.total_expected:,}",
        result.duration_s,
    )

    # ── Cleanup ───────────────────────────────────────────────────────────
    _cleanup_output(ds_id, result, output_base, args, logger)

    return result


def _cleanup_output(
    ds_id:       str,
    ds_result:   DatasetResult,
    output_base: str,
    args:        argparse.Namespace,
    logger:      logging.Logger,
) -> None:
    """Delete the dataset output directory after validation."""
    ds_out = pathlib.Path(output_base) / ds_id

    if args.dry_run:
        logger.info(
            "  %s[dry-run]%s  would delete: %s", C.YLW, C.RST, ds_out,
        )
        return

    if args.skip_delete:
        logger.info(
            "  %s⊘ kept (--skip-delete):%s  %s", C.YLW, C.RST, ds_out,
        )
        return

    has_failure = any(
        r.status in ("FAIL", "ERROR") for r in ds_result.spec_results
    )
    if has_failure and args.keep_on_fail:
        logger.info(
            "  %s⚠ kept (--keep-on-fail):%s  %s", C.YLW, C.RST, ds_out,
        )
        return

    if ds_out.is_dir():
        # Reuse the file count already computed during validation
        n_files = ds_result.total_actual
        logger.info(
            "  %s\U0001f5d1 deleting%s  [%s]  files=%s  path=%s",
            C.YLW, C.RST, ds_id, f"{n_files:,}", ds_out,
        )
        t_del = time.perf_counter()
        try:
            shutil.rmtree(ds_out)
            del_elapsed = round(time.perf_counter() - t_del, 1)
            logger.info(
                "  %s\u2713 deleted%s   [%s]  in %.1fs",
                C.GRN, C.RST, ds_id, del_elapsed,
            )
        except OSError as exc:
            logger.warning(
                "  %s\u2717 delete failed%s  [%s]  %s", C.RED, C.RST, ds_id, exc,
            )
    else:
        # Fallback: delete individual spec root dirs
        logger.debug("[%s] dataset dir not found at %s, deleting spec roots", ds_id, ds_out)
        for sr in ds_result.spec_results:
            p = pathlib.Path(sr.spec_root)
            if p.is_dir():
                logger.debug("  deleting spec root: %s", p)
                shutil.rmtree(p, ignore_errors=True)


# ─────────────────────────────────────────────────────────────────────────────
# Run report
# ─────────────────────────────────────────────────────────────────────────────

def emit_report(
    run:         RunReport,
    report_path: Optional[str],
    logger:      logging.Logger,
) -> None:
    """Print the final summary table and optionally write a JSON report."""
    ds_list = run.datasets

    n_ds    = len(ds_list)
    n_pass  = sum(1 for d in ds_list if d.status == "PASS")
    n_fail  = sum(1 for d in ds_list if d.status == "FAIL")
    n_err   = sum(1 for d in ds_list if d.status == "ERROR")
    n_skip  = sum(1 for d in ds_list if d.status == "SKIPPED")

    all_specs = [s for d in ds_list for s in d.spec_results]
    s_pass    = sum(1 for s in all_specs if s.status == "PASS")
    s_fail    = sum(1 for s in all_specs if s.status == "FAIL")
    s_err     = sum(1 for s in all_specs if s.status == "ERROR")

    run.summary = {
        "datasets_total":   n_ds,
        "datasets_passed":  n_pass,
        "datasets_failed":  n_fail,
        "datasets_errored": n_err,
        "datasets_skipped": n_skip,
        "specs_total":      len(all_specs),
        "specs_passed":     s_pass,
        "specs_failed":     s_fail,
        "specs_errored":    s_err,
    }

    col_ds = C.GRN if (n_fail + n_err) == 0 else C.RED
    col_sp = C.GRN if (s_fail + s_err) == 0 else C.RED

    logger.info(
        "\n%s%s══════════════════════  RUN SUMMARY  ══════════════════════%s",
        C.BLD, C.CYN, C.RST,
    )
    logger.info(
        "  Datasets  total=%-4d  %spassed=%-4d  failed=%-4d  "
        "errored=%-4d  skipped=%d%s",
        n_ds, col_ds, n_pass, n_fail, n_err, n_skip, C.RST,
    )
    logger.info(
        "  Specs     total=%-4d  %spassed=%-4d  failed=%-4d  errored=%d%s",
        len(all_specs), col_sp, s_pass, s_fail, s_err, C.RST,
    )

    # Per-dataset table
    if ds_list:
        hdr = f"\n  {'Dataset':<14}  {'Name':<36}  {'Expected':>10}  " \
              f"{'Actual':>10}  {'Status':<8}  {'Time(s)':>7}"
        logger.info(hdr)
        logger.info("  " + "─" * 95)
        for d in ds_list:
            col = (
                C.GRN if d.status == "PASS"    else
                C.DIM if d.status == "SKIPPED" else
                C.RED
            )
            logger.info(
                "  %-14s  %-36s  %10s  %10s  %s%-8s%s  %7.1f",
                d.dataset_id,
                d.name[:36],
                f"{d.total_expected:,}",
                f"{d.total_actual:,}",
                col, d.status, C.RST,
                d.duration_s,
            )

    # Spec-level failures / errors detail
    failures = [
        s for d in ds_list
          for s in d.spec_results
          if s.status in ("FAIL", "ERROR")
    ]
    if failures:
        logger.info("\n  %sFailed / errored specs:%s", C.RED, C.RST)
        for s in failures:
            detail = s.error if s.error else (
                f"actual={s.actual_count:,}  expected={s.expected_count:,}"
            )
            logger.info(
                "    %s%-54s%s  [%s]  %s",
                C.RED, s.spec_file, C.RST, s.dataset_id, detail,
            )

    # JSON report
    if report_path:
        try:
            with open(report_path, "w", encoding="utf-8") as fh:
                json.dump(asdict(run), fh, indent=2, default=str)
            logger.info(
                "\n  JSON report written to: %s%s%s", C.CYN, report_path, C.RST,
            )
        except OSError as exc:
            logger.error("Could not write report to %s: %s", report_path, exc)


# ─────────────────────────────────────────────────────────────────────────────
# On-demand delete mode
# ─────────────────────────────────────────────────────────────────────────────

def _scan_delete_targets(
    output_base: str,
    phase_from:  int,
    phase_to:    int,
    ds_filter:   Optional[List[str]],
) -> List[str]:
    """
    Discover dataset IDs to delete by scanning output_base for DS-P* dirs.
    No manifest is required — purely filesystem-based.

    Priority:
      1. If ds_filter is set, use exactly those IDs (existence checked later).
      2. Otherwise, return all DS-P* dirs under output_base whose phase
         number falls within [phase_from, phase_to].
    """
    if ds_filter:
        return ds_filter

    base = pathlib.Path(output_base)
    targets = []
    for entry in sorted(base.iterdir()):
        if not entry.is_dir():
            continue
        m = DS_ID_PATTERN.match(entry.name)
        if not m:
            continue
        phase = int(m.group(1))
        if phase_from <= phase <= phase_to:
            targets.append(entry.name)
    return targets


def delete_mode(
    output_base: str,
    phase_from:  int,
    phase_to:    int,
    ds_filter:   Optional[List[str]],
    ask:         bool,
    dry_run:     bool,
    logger:      logging.Logger,
) -> int:
    """
    Stand-alone delete operation.  Scans output_base for dataset directories,
    checks existence, optionally asks for confirmation, then removes them.

    Returns 0 (success) or 1 (one or more deletions failed).
    """
    output_base = os.path.abspath(output_base)

    if not os.path.isdir(output_base):
        logger.error(
            "--output-base directory does not exist: %s", output_base,
        )
        return 1

    targets = _scan_delete_targets(
        output_base, phase_from, phase_to, ds_filter,
    )

    if not targets:
        logger.warning(
            "No matching dataset directories found under: %s", output_base,
        )
        return 0

    logger.info(
        "%s%s── Delete mode  ──  root: %s%s",
        C.BLD, C.CYN, output_base, C.RST,
    )
    logger.info(
        "%s   %d target(s)  |  phase %d–%d%s%s",
        C.DIM, len(targets), phase_from, phase_to,
        f"  filter=[{','.join(ds_filter)}]" if ds_filter else "",
        C.RST,
    )

    deleted:   List[str] = []
    not_found: List[str] = []
    failed:    List[str] = []

    for ds_id in targets:
        ds_dir = pathlib.Path(output_base) / ds_id

        # ── Existence check ───────────────────────────────────────────────
        if not ds_dir.is_dir():
            not_found.append(ds_id)
            logger.warning(
                "  %s✗ not found%s  %s",
                C.YLW, C.RST, ds_dir,
            )
            continue

        # Count what's inside so the user can see what they're deleting
        n_files = sum(1 for _ in ds_dir.rglob("*") if _.is_file())
        n_dirs  = sum(1 for _ in ds_dir.rglob("*") if _.is_dir())

        if dry_run:
            logger.info(
                "  %s~ DRY%s   %-14s  would delete  "
                "(files=%s  subdirs=%s)  %s",
                C.YLW, C.RST, ds_id,
                f"{n_files:,}", f"{n_dirs:,}", ds_dir,
            )
            deleted.append(ds_id)
            continue

        # ── Optional confirmation ─────────────────────────────────────────
        if ask:
            confirmed = _yesno(
                f"  Delete [{ds_id}]  "
                f"(files={n_files:,}  subdirs={n_dirs:,})  {ds_dir}?",
            )
            if not confirmed:
                logger.info("  %s⊘ skipped%s  %s", C.YLW, C.RST, ds_id)
                not_found.append(ds_id)   # count as "not processed"
                continue

        # ── Delete ────────────────────────────────────────────────────────
        try:
            shutil.rmtree(ds_dir)
            deleted.append(ds_id)
            logger.info(
                "  %s✓ deleted%s  %-14s  (files=%s  subdirs=%s)  %s",
                C.GRN, C.RST, ds_id,
                f"{n_files:,}", f"{n_dirs:,}", ds_dir,
            )
        except OSError as exc:
            failed.append(ds_id)
            logger.error(
                "  %s✗ failed%s   %-14s  %s",
                C.RED, C.RST, ds_id, exc,
            )

    # ── Summary ───────────────────────────────────────────────────────────
    col = C.GRN if not failed else C.RED
    action = "would delete" if dry_run else "deleted"
    logger.info(
        "\n  %s%s=%d  not-found/skipped=%d  failed=%d%s",
        col, action, len(deleted), len(not_found), len(failed), C.RST,
    )
    if not_found:
        logger.info("  Not found / skipped: %s", ", ".join(not_found))
    if failed:
        logger.info("  %sFailed:%s %s", C.RED, C.RST, ", ".join(failed))

    return 1 if failed else 0


# ─────────────────────────────────────────────────────────────────────────────
# Interactive helpers
# ─────────────────────────────────────────────────────────────────────────────

def _yesno(prompt: str, default: bool = True) -> bool:
    yn = "Y/n" if default else "y/N"
    try:
        ans = input(f"{C.YLW}{prompt} [{yn}]: {C.RST}").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    return default if not ans else ans.startswith("y")


def interactive_setup(args: argparse.Namespace) -> bool:
    """
    Prompt the user to fill in any missing required settings.
    Returns False if the user wants to abort.
    """
    print(f"\n{C.BLD}{C.CYN}── Interactive Setup ──{C.RST}")

    if not os.path.isfile(args.datagen):
        val = input(f"  datagen binary path [{args.datagen}]: ").strip()
        if val:
            args.datagen = val

    if not args.output_base:
        val = input("  output base directory [auto-temp]: ").strip()
        if val:
            args.output_base = val

    print(f"\n  spec-dir    : {args.spec_dir}")
    print(f"  phases      : {args.phase_from} – {args.phase_to}")
    if getattr(args, "dataset_num", None):
        print(f"  dataset-num : {args.dataset_num}")
    print(f"  datagen     : {args.datagen}")
    print(f"  output-base : {args.output_base or '(auto-temp)'}")
    print(f"  parallel    : {args.parallel} spec(s) at a time")
    print(f"  tolerance   : {args.tolerance}%")
    print(f"  dry-run     : {args.dry_run}")
    print()

    return _yesno("Proceed?", default=True)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser  = _build_parser()
    args    = parser.parse_args()

    err_msg = _check_args(args)
    if err_msg:
        print(f"Error: {err_msg}", file=sys.stderr)
        parser.print_usage(sys.stderr)
        return 2

    logger = setup_logging(args.log, verbose=args.verbose)

    logger.info(
        "%s%sdataset_validator  v%s%s",
        C.BLD, C.CYN, VERSION, C.RST,
    )
    logger.info(
        "%s%s%s\n",
        C.DIM, datetime.datetime.now().isoformat(timespec="seconds"), C.RST,
    )

    # ── Resolve dataset filter (shared by delete mode and validation pipeline) ──
    ds_filter: Optional[List[str]] = None
    if args.datasets:
        ds_filter = [x.strip() for x in args.datasets.split(",") if x.strip()]
    elif getattr(args, "dataset_num", None):
        nums = sorted(int(x.strip()) for x in args.dataset_num.split(",") if x.strip())
        ds_filter = [
            f"DS-P{phase}-{num:02d}"
            for phase in range(args.phase_from, args.phase_to + 1)
            for num in nums
        ]
        logger.debug("--dataset-num %s resolved to: %s", args.dataset_num, ds_filter)

    # ── Delete mode (early branch — no validation pipeline needed) ─────────
    if args.delete:
        return delete_mode(
            output_base  = args.output_base,
            phase_from   = args.phase_from,
            phase_to     = args.phase_to,
            ds_filter    = ds_filter,
            ask          = args.ask,
            dry_run      = args.dry_run,
            logger       = logger,
        )

    # ── Interactive setup ──────────────────────────────────────────────────
    if args.ask:
        if not interactive_setup(args):
            logger.info("Aborted by user.")
            return 0

    # ── Validate spec directory ────────────────────────────────────────────
    spec_dir = os.path.abspath(args.spec_dir)
    if not os.path.isdir(spec_dir):
        logger.error("spec-dir not found: %s", spec_dir)
        return 1

    # ── Load manifest ──────────────────────────────────────────────────────
    try:
        manifest_idx, root_base = load_manifest(spec_dir)
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        return 1

    logger.info(
        "Manifest loaded: %d datasets  |  root_base=%s",
        len(manifest_idx), root_base,
    )

    # ── Discover datasets ──────────────────────────────────────────────────
    # (ds_filter was resolved earlier, shared with delete mode)
    datasets = discover_datasets(
        spec_dir, args.phase_from, args.phase_to, ds_filter, manifest_idx,
    )

    # Warn about any explicitly requested IDs that were not found
    if ds_filter:
        found_ids = {ds[0] for ds in datasets}
        for req_id in ds_filter:
            if req_id not in found_ids:
                logger.warning("Requested dataset not found / not in manifest: %s", req_id)

    if not datasets:
        logger.warning("No matching datasets found for the given selection.")
        return 0

    # Build a human-readable filter description for the log
    _filter_desc = ""
    if args.datasets:
        _filter_desc = f"  filter=[{args.datasets}]"
    elif getattr(args, "dataset_num", None):
        _filter_desc = f"  dataset-num=[{args.dataset_num}]  ({', '.join(ds_filter)})"

    logger.info(
        "Selected: %d dataset(s)  |  phases %d–%d%s",
        len(datasets), args.phase_from, args.phase_to, _filter_desc,
    )

    # ── Check datagen binary ───────────────────────────────────────────────
    if not args.dry_run and not os.path.isfile(args.datagen):
        logger.error(
            "datagen binary not found: %s\n"
            "  Set --datagen <path>  or  export DATAGEN_BIN=<path>",
            args.datagen,
        )
        return 1

    # ── Output directory ───────────────────────────────────────────────────
    _auto_tmp: Optional[str] = None
    if args.output_base:
        output_base = os.path.abspath(args.output_base)
        os.makedirs(output_base, exist_ok=True)
    else:
        _auto_tmp   = tempfile.mkdtemp(prefix="cloudcp_val_")
        output_base = _auto_tmp
        logger.info(
            "Auto temp dir: %s%s%s", C.DIM, output_base, C.RST,
        )

    # ── Build run record ───────────────────────────────────────────────────
    run = RunReport(
        timestamp     = datetime.datetime.now().isoformat(timespec="seconds"),
        command_line  = " ".join(sys.argv),
        datagen_bin   = args.datagen,
        output_base   = output_base,
        spec_dir      = spec_dir,
        phase_from    = args.phase_from,
        phase_to      = args.phase_to,
        tolerance_pct = args.tolerance,
        dry_run       = args.dry_run,
    )

    # ── Process each dataset ───────────────────────────────────────────────
    early_stop = False
    for ds_id, ds_dir, manifest_entry in datasets:

        # Propagate stop from a previous failure
        if early_stop:
            run.datasets.append(DatasetResult(
                dataset_id=ds_id,
                name=manifest_entry.get("name", ds_id),
                phase=manifest_entry.get("phase", 0),
                total_expected=manifest_entry.get("emitted_files", 0),
                status="SKIPPED",
                error="--stop-on-fail triggered by an earlier failure",
            ))
            continue

        # Per-dataset confirmation (--ask mode)
        if args.ask:
            if not _yesno(
                f"\n  Run [{ds_id}]  {manifest_entry.get('name', '')}?",
            ):
                run.datasets.append(DatasetResult(
                    dataset_id=ds_id,
                    name=manifest_entry.get("name", ds_id),
                    phase=manifest_entry.get("phase", 0),
                    total_expected=manifest_entry.get("emitted_files", 0),
                    status="SKIPPED",
                    error="skipped by user",
                ))
                continue

        ds_result = process_dataset(
            ds_id, ds_dir, manifest_entry,
            root_base, output_base, args, logger,
        )
        run.datasets.append(ds_result)

        if args.stop_on_fail and ds_result.status in ("FAIL", "ERROR"):
            logger.warning(
                "\n%s--stop-on-fail: halting after %s%s",
                C.YLW, ds_id, C.RST,
            )
            early_stop = True

    # ── Cleanup auto temp dir (if still around after all datasets) ─────────
    if _auto_tmp and os.path.isdir(_auto_tmp):
        try:
            remaining = list(pathlib.Path(_auto_tmp).iterdir())
            if remaining:
                shutil.rmtree(_auto_tmp, ignore_errors=True)
            else:
                pathlib.Path(_auto_tmp).rmdir()
        except OSError:
            pass

    # ── Emit report ────────────────────────────────────────────────────────
    emit_report(run, args.report, logger)

    all_ok = all(d.status in ("PASS", "SKIPPED") for d in run.datasets)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
