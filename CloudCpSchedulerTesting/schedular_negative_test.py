#!/usr/bin/env python3
"""
schedular_negative_test.py — scheduler-level NEGATIVE test harness.

Companion to schedular_test.py. It exercises only the faults the *scheduler /
batch-builder* uniquely owns — enumeration robustness, per-batch building,
batchmeta / transfer-dir lifecycle, worker/poll accounting and process
lifecycle. Transport / auth / config-loading faults (bad endpoint, bad creds,
network drop, missing/malformed config, xattr) intentionally live in the
CLI / binary test suites, not here.

Design rules
------------
* NOTHING on the machine is mutated. Every case runs inside a private throwaway
  sandbox (<out>/neg_<id>/{data,batchmeta,logs}) built and destroyed per case.
  The real /opt batchmeta and /etc config are never read or written.
* Faults are injected only via: the sandbox filesystem we own, CLI overrides the
  scheduler already accepts, and signals sent to the scheduler subprocess we
  spawn. No sudo, no iptables, no config edits.
* Each case declares an EXPECTATION; PASS means "the fault was correctly produced
  and the scheduler handled it as specified" (a controlled failure is a pass).

Datasets are synthesised in Python (tiny, mostly sparse / zero-byte), so no
`datagen` and no large disk footprint are required.

Usage
-----
    python3 schedular_negative_test.py --list
    python3 schedular_negative_test.py --all
    python3 schedular_negative_test.py --case NEG-ENUM-03
    python3 schedular_negative_test.py --case NEG-ENUM-01,NEG-META-01

Or via the main harness:
    python3 schedular_test.py --negative
    python3 schedular_test.py --negative-case NEG-ENUM-03

Most faults are POSIX-only (chmod/symlink/signals); on a non-POSIX host or with
--dry-run the case is reported as SKIP with its plan.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

import schedular_test as st

LOG = logging.getLogger("sch_neg")
POSIX = os.name == "posix"

DEF_TIMEOUT = 300          # per-case wall-clock bound; a breach counts as a hang
DEF_KILL_AFTER = 3.0       # seconds before injecting a signal / mid-run delete


# =====================================================================================
# Shared config
# =====================================================================================
@dataclass
class NegConfig:
    scheduler_python: str = st.DEF_SCHED_PY
    scheduler_script: str = st.DEF_SCHED_SCRIPT
    dir_path: str = st.DEF_DIR_PATH
    endpoint_url: str = st.DEF_ENDPOINT
    journal_tag: str = st.DEF_JOURNAL_TAG
    transfer_logs_dir: str = st.DEF_TRANSFER_LOGS
    s3_base: str = st.DEF_S3_BASE
    config: str = st.DEF_CONFIG
    capture_lead: float = st.DEF_CAPTURE_LEAD
    capture_drain: float = st.DEF_CAPTURE_DRAIN
    timeout: int = DEF_TIMEOUT
    dry_run: bool = False

    @classmethod
    def from_args(cls, args) -> "NegConfig":
        return cls(
            scheduler_python=getattr(args, "scheduler_python", st.DEF_SCHED_PY),
            scheduler_script=getattr(args, "scheduler_script", st.DEF_SCHED_SCRIPT),
            dir_path=getattr(args, "dir_path", st.DEF_DIR_PATH),
            endpoint_url=getattr(args, "endpoint_url", st.DEF_ENDPOINT),
            journal_tag=getattr(args, "journal_tag", st.DEF_JOURNAL_TAG),
            transfer_logs_dir=getattr(args, "transfer_logs_dir", st.DEF_TRANSFER_LOGS),
            s3_base=getattr(args, "s3_base", st.DEF_S3_BASE),
            config=getattr(args, "config", st.DEF_CONFIG),
            capture_lead=getattr(args, "capture_lead", st.DEF_CAPTURE_LEAD),
            capture_drain=getattr(args, "capture_drain", st.DEF_CAPTURE_DRAIN),
            timeout=getattr(args, "neg_timeout", None) or DEF_TIMEOUT,
            dry_run=getattr(args, "dry_run", False),
        )


# =====================================================================================
# Sandbox
# =====================================================================================
class Sandbox:
    """Private per-case work area; restores perms and self-destructs on exit."""

    def __init__(self, out_root: Path, case_id: str):
        self.root = out_root / f"neg_{case_id}"
        self.data = self.root / "data"
        self.batchmeta = self.root / "batchmeta"
        self.logs = self.root / "logs"
        self._restore: list[tuple[Path, int]] = []

    def __enter__(self) -> "Sandbox":
        if self.root.exists():
            _force_rmtree(self.root)
        for d in (self.data, self.batchmeta, self.logs):
            d.mkdir(parents=True, exist_ok=True)
        return self

    def guard_chmod(self, path: Path, mode: int) -> None:
        """chmod a path but remember its old mode so teardown can undo it."""
        try:
            old = stat.S_IMODE(path.stat().st_mode)
            self._restore.append((path, old))
            path.chmod(mode)
        except OSError as exc:  # noqa: BLE001
            LOG.warning("chmod %s failed: %s", path, exc)

    def __exit__(self, *exc) -> None:
        for path, mode in reversed(self._restore):
            try:
                path.chmod(mode)
            except OSError:
                pass
        _force_rmtree(self.root)


def _force_rmtree(path: Path) -> None:
    def _onerror(func, p, _exc):
        try:
            os.chmod(p, 0o700)
            func(p)
        except OSError:
            pass
    shutil.rmtree(path, onerror=_onerror)


# =====================================================================================
# Tiny dataset synthesis
# =====================================================================================
def write_file(path: Path, size: int, sparse: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as fh:
        if size <= 0:
            return
        if sparse:
            fh.truncate(size)
        else:
            fh.write(os.urandom(min(size, 1 << 16)))


def make_tier(dir_path: Path, tier: str, count: int, size: int, sparse: bool = True,
              prefix: str | None = None) -> dict:
    dir_path.mkdir(parents=True, exist_ok=True)
    pre = prefix or {"zero": "z-", "tiny": "t-", "small": "s-",
                     "medium": "m-", "large": "l-"}.get(tier, "f-")
    for i in range(count):
        write_file(dir_path / f"{pre}{i:06d}.dat", size, sparse=sparse)
    return {"dir": str(dir_path), "tier": tier, "count": count,
            "file_size_bytes": size, "total_bytes": count * size, "sparse": sparse}


# =====================================================================================
# Setup / plan produced by each case builder
# =====================================================================================
@dataclass
class Setup:
    src: str
    dst: str
    forced_id: int | None = None
    pre_transfer: bool = False              # pre-create transfer_<id> before the run
    pre_transfer_files: dict = field(default_factory=dict)  # name -> bytes (stale/corrupt)
    readonly_transfer: bool = False         # chmod 0o500 the transfer dir
    extra_args: list = field(default_factory=list)
    env: dict = field(default_factory=dict)
    signal: tuple | None = None             # (signal.SIG*, after_sec) on the single run
    pre_kill: tuple | None = None           # (signal.SIG*, after_sec): kill a first run, then resume
    delete_after: tuple | None = None       # (after_sec, [Path,...]) deleted mid-run
    expect: dict = field(default_factory=dict)
    structure: list = field(default_factory=list)
    note: str = ""
    skip: str | None = None                 # non-None => skip with reason


@dataclass
class NegCase:
    id: str
    group: str
    title: str
    desc: str
    build: "callable"
    posix_only: bool = False


# =====================================================================================
# Case builders (each returns a Setup)
# =====================================================================================
def _dst(cfg: NegConfig, case_id: str) -> str:
    return f"{cfg.s3_base.rstrip('/')}/neg/{case_id}"


# ---- A. enumeration robustness ------------------------------------------------------
def _b_missing_src(sb, cfg, cid):
    src = sb.data / "DOES_NOT_EXIST"      # deliberately never created
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 expect={"exit": "nonzero", "no_timeout": True, "no_traceback": True},
                 note="src root absent; scheduler must fail cleanly")


def _b_empty_src(sb, cfg, cid):
    src = sb.data / cid
    (src / "L1_empty").mkdir(parents=True, exist_ok=True)   # empty root + empty level
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 expect={"exit": "zero", "csv_total": 0, "no_timeout": True, "no_traceback": True},
                 structure=[{"dir": str(src), "tier": "-", "count": 0,
                             "file_size_bytes": 0, "total_bytes": 0}],
                 note="empty tree; 0 batches, clean completion")


def _b_unreadable_file(sb, cfg, cid):
    src = sb.data / cid
    s = [make_tier(src, "tiny", 6, 16 * 1024)]
    victim = src / "t-000003.dat"
    sb.guard_chmod(victim, 0o000)
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 expect={"csv_failed_min": 1, "csv_success_min": 1,
                         "no_timeout": True, "no_traceback": True},
                 structure=s, note="one file chmod 000; that file FAILED, rest SUCCESS")


def _b_unreadable_dir(sb, cfg, cid):
    src = sb.data / cid
    s = [make_tier(src, "tiny", 4, 16 * 1024)]
    locked = src / "L1"
    s.append(make_tier(locked, "small", 3, 64 * 1024))
    sb.guard_chmod(locked, 0o000)
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 expect={"no_timeout": True, "no_traceback": True},
                 structure=s, note="a level dir chmod 000; enumeration skips it, no crash")


def _b_delete_mid(sb, cfg, cid):
    src = sb.data / cid
    make_tier(src, "tiny", 20, 16 * 1024)
    deep = src / "L1" / "L2"
    s = [make_tier(deep, "zero", 2000, 0)]          # deep, reached last by BFS
    victims = [deep / f"z-{i:06d}.dat" for i in range(1500, 1700)]
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 delete_after=(DEF_KILL_AFTER, victims),
                 expect={"no_timeout": True, "no_traceback": True},
                 structure=s,
                 note="TIMING: files unlinked mid-walk; vanished entries tolerated")


def _b_symlink_loop(sb, cfg, cid):
    src = sb.data / cid
    a = src / "A"
    b = src / "B"
    a.mkdir(parents=True, exist_ok=True)
    b.mkdir(parents=True, exist_ok=True)
    make_tier(a, "tiny", 2, 16 * 1024)
    os.symlink(b, a / "to_B")
    os.symlink(a, b / "to_A")                        # A/to_B -> B, B/to_A -> A (cycle)
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 expect={"no_timeout": True, "no_traceback": True},
                 structure=[{"dir": str(src), "tier": "symlink-loop", "count": 2,
                             "file_size_bytes": 16384, "total_bytes": 32768}],
                 note="cyclic symlinks; BFS must terminate")


def _b_symlink_broken(sb, cfg, cid):
    src = sb.data / cid
    s = [make_tier(src, "tiny", 3, 16 * 1024)]
    os.symlink(src / "nowhere-target", src / "broken_link")
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 expect={"no_timeout": True, "no_traceback": True},
                 structure=s, note="dangling symlink; skipped, order preserved")


# ---- B. batch-building boundaries ---------------------------------------------------
def _b_oversized_file(sb, cfg, cid):
    src = sb.data / cid
    make_tier(src, "tiny", 4, 16 * 1024)             # normal tiny backlog
    # one sparse file larger than the TINY tier TARGET_SIZE_MB (256 MB) => own batch
    write_file(src / "t-oversized.bin", 300 * 1024 * 1024, sparse=True)
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 expect={"csv_success_min": 1, "no_timeout": True, "no_traceback": True},
                 structure=[{"dir": str(src), "tier": "tiny+oversized", "count": 5,
                             "file_size_bytes": 0, "total_bytes": 300 * 1024 * 1024}],
                 note="one file > tier TARGET_SIZE_MB closes a single-file batch")


def _b_partial_block(sb, cfg, cid):
    src = sb.data / cid
    s = [make_tier(src, "tiny", 5, 16 * 1024)]       # far fewer than one block
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 expect={"csv_total": 5, "csv_success_min": 5,
                         "no_timeout": True, "no_traceback": True},
                 structure=s, note="fewer files than a block; partial batch flushed at finish()")


# ---- C. batchmeta / transfer-dir lifecycle ------------------------------------------
def _b_readonly_transfer(sb, cfg, cid):
    src = sb.data / cid
    s = [make_tier(src, "tiny", 4, 16 * 1024)]
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 readonly_transfer=True,
                 expect={"exit": "nonzero", "no_timeout": True, "no_traceback": True},
                 structure=s, note="transfer-dir chmod 0500; metadata write must fail cleanly")


def _b_corrupt_meta(sb, cfg, cid):
    src = sb.data / cid
    s = [make_tier(src, "tiny", 4, 16 * 1024)]
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 pre_transfer=True,
                 pre_transfer_files={"batch_0.json": b"{ this is : not json ,,,",
                                     "state.json": b"\x00\x01\x02truncated"},
                 expect={"no_timeout": True, "no_traceback": True},
                 structure=s, note="malformed batchmeta pre-seeded; reject/repair, no crash")


def _b_id_collision(sb, cfg, cid):
    src = sb.data / cid
    s = [make_tier(src, "tiny", 4, 16 * 1024)]
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 pre_transfer=True,
                 pre_transfer_files={"stale.txt": b"leftover from a previous run"},
                 expect={"no_timeout": True, "no_traceback": True},
                 structure=s, note="stale transfer_<id> already present; safe handling")


# ---- D. scheduling / worker accounting ----------------------------------------------
def _b_bad_poll(sb, cfg, cid):
    src = sb.data / cid
    s = [make_tier(src, "tiny", 6, 16 * 1024)]
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 extra_args=["--poll-interval", "0"],
                 expect={"no_timeout": True, "no_traceback": True},
                 structure=s, note="--poll-interval 0; no tight-loop / div-by-zero")


def _b_stall(sb, cfg, cid):
    src = sb.data / cid
    make_tier(src, "tiny", 4, 16 * 1024)
    write_file(src / "l-stall.bin", 2 * 1024 * 1024 * 1024, sparse=True)   # 2 GiB sparse
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 expect={"no_timeout": True, "no_traceback": True},
                 structure=[{"dir": str(src), "tier": "tiny+huge", "count": 5,
                             "file_size_bytes": 0, "total_bytes": 2 * 1024 * 1024 * 1024}],
                 note="BEST-EFFORT: one huge slow batch; worker accounting must hold")


# ---- E. lifecycle -------------------------------------------------------------------
def _b_sigint(sb, cfg, cid):
    src = sb.data / cid
    make_tier(src, "zero", 4000, 0)                  # enough backlog to be mid-run at t=3s
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 signal=(signal.SIGINT if POSIX else None, DEF_KILL_AFTER),
                 expect={"exit": "handled", "no_timeout": True, "no_traceback": True},
                 structure=[{"dir": str(src), "tier": "zero", "count": 4000,
                             "file_size_bytes": 0, "total_bytes": 0}],
                 note="SIGINT mid-run; clean shutdown, capture drained, partial CSV parses")


def _b_resume(sb, cfg, cid):
    src = sb.data / cid
    make_tier(src, "zero", 4000, 0)
    return Setup(src=str(src), dst=_dst(cfg, cid),
                 pre_kill=(signal.SIGKILL if POSIX else None, DEF_KILL_AFTER),
                 expect={"exit": "zero", "no_timeout": True, "no_traceback": True},
                 structure=[{"dir": str(src), "tier": "zero", "count": 4000,
                             "file_size_bytes": 0, "total_bytes": 0}],
                 note="kill mid-run, re-run same id+transfer-dir; idempotent resume "
                      "(requires scheduler resume support)")


CASES: list[NegCase] = [
    NegCase("NEG-ENUM-01", "enumeration", "Missing source root", "source dir does not exist", _b_missing_src),
    NegCase("NEG-ENUM-02", "enumeration", "Empty source root", "empty root + empty level dir", _b_empty_src),
    NegCase("NEG-ENUM-03", "enumeration", "Unreadable file", "one file chmod 000", _b_unreadable_file, True),
    NegCase("NEG-ENUM-04", "enumeration", "Unreadable subdir", "a level dir chmod 000", _b_unreadable_dir, True),
    NegCase("NEG-ENUM-05", "enumeration", "Delete mid-enumeration", "unlink files during walk", _b_delete_mid),
    NegCase("NEG-ENUM-06", "enumeration", "Symlink loop", "cyclic symlinks", _b_symlink_loop, True),
    NegCase("NEG-ENUM-07", "enumeration", "Broken symlink", "dangling symlink", _b_symlink_broken, True),
    NegCase("NEG-BATCH-01", "batch", "Oversized file", "file > tier TARGET_SIZE_MB", _b_oversized_file),
    NegCase("NEG-BATCH-03", "batch", "Partial block", "fewer files than one block", _b_partial_block),
    NegCase("NEG-META-01", "batchmeta", "Read-only transfer-dir", "chmod 0500 transfer dir", _b_readonly_transfer, True),
    NegCase("NEG-META-02", "batchmeta", "Corrupt batchmeta", "malformed metadata pre-seeded", _b_corrupt_meta),
    NegCase("NEG-META-03", "batchmeta", "Transfer-id collision", "stale transfer_<id> present", _b_id_collision),
    NegCase("NEG-SCHED-01", "scheduling", "Bad poll-interval", "--poll-interval 0", _b_bad_poll),
    NegCase("NEG-SCHED-02", "scheduling", "Stalled batch", "one huge slow batch", _b_stall),
    NegCase("NEG-LIFE-01", "lifecycle", "SIGINT mid-run", "signal the scheduler", _b_sigint, True),
    NegCase("NEG-LIFE-02", "lifecycle", "Resume after kill", "kill then re-run same id", _b_resume, True),
]
CASE_BY_ID = {c.id: c for c in CASES}


# =====================================================================================
# Execution
# =====================================================================================
def _build_cmd(cfg: NegConfig, tr_id: int, src: str, dst: str,
               transfer_dir: Path, extra_args: list) -> list[str]:
    cmd = [
        cfg.scheduler_python, cfg.scheduler_script,
        str(tr_id), "upload", src, dst, src,
        "--transfer-dir", str(transfer_dir),
        "--dir-path", cfg.dir_path,
        "--endpoint-url", cfg.endpoint_url,
    ]
    return cmd + list(extra_args)


def _run_proc(cmd: list[str], env: dict, timeout: int, stderr_path: Path,
              signal_spec=None, delete_spec=None) -> dict:
    """Spawn the scheduler; optionally signal it / delete files mid-run; bound by timeout."""
    LOG.info("$ %s", " ".join(cmd))
    popen_kw = {}
    if POSIX:
        popen_kw["preexec_fn"] = os.setsid
    with open(stderr_path, "wb") as errfh:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=errfh, env=env, **popen_kw)

        def _fire_signal():
            sig, after = signal_spec
            time.sleep(after)
            if sig is None or proc.poll() is not None:
                return
            LOG.info("injecting %s after %.1fs", sig, after)
            try:
                if POSIX:
                    os.killpg(os.getpgid(proc.pid), sig)
                else:
                    proc.send_signal(sig)
            except (ProcessLookupError, OSError):
                pass

        def _fire_delete():
            after, paths = delete_spec
            time.sleep(after)
            LOG.info("deleting %d files mid-run", len(paths))
            for p in paths:
                try:
                    Path(p).unlink()
                except OSError:
                    pass

        threads = []
        if signal_spec and signal_spec[0] is not None:
            threads.append(threading.Thread(target=_fire_signal, daemon=True))
        if delete_spec:
            threads.append(threading.Thread(target=_fire_delete, daemon=True))
        for t in threads:
            t.start()

        timed_out = False
        try:
            rc = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            LOG.warning("case timed out after %ss; killing", timeout)
            try:
                if POSIX:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                else:
                    proc.kill()
            except (ProcessLookupError, OSError):
                pass
            rc = proc.wait()
    return {"rc": rc, "timed_out": timed_out}


def _evaluate(expect: dict, rc: int | None, timed_out: bool, csv_summary: dict,
              stderr_path: Path) -> tuple[bool, list[str]]:
    checks: list[str] = []
    ok = True

    if expect.get("no_timeout"):
        good = not timed_out
        ok &= good
        checks.append(f"[{'PASS' if good else 'FAIL'}] no_timeout (timed_out={timed_out})")

    if "exit" in expect:
        want = expect["exit"]
        if want == "zero":
            good = rc == 0
        elif want == "nonzero":
            good = rc not in (0, None)
        else:  # "handled": any real exit code, not a hang
            good = rc is not None and not timed_out
        ok &= good
        checks.append(f"[{'PASS' if good else 'FAIL'}] exit={want} (rc={rc})")

    if "csv_total" in expect:
        good = csv_summary.get("total", 0) == expect["csv_total"]
        ok &= good
        checks.append(f"[{'PASS' if good else 'FAIL'}] csv_total=={expect['csv_total']} "
                      f"(got {csv_summary.get('total', 0)})")

    if "csv_failed_min" in expect:
        good = csv_summary.get("failed", 0) >= expect["csv_failed_min"]
        ok &= good
        checks.append(f"[{'PASS' if good else 'FAIL'}] csv_failed>={expect['csv_failed_min']} "
                      f"(got {csv_summary.get('failed', 0)})")

    if "csv_success_min" in expect:
        good = csv_summary.get("success", 0) >= expect["csv_success_min"]
        ok &= good
        checks.append(f"[{'PASS' if good else 'FAIL'}] csv_success>={expect['csv_success_min']} "
                      f"(got {csv_summary.get('success', 0)})")

    if expect.get("no_traceback"):
        tb = False
        try:
            tb = b"Traceback (most recent call" in stderr_path.read_bytes()
        except OSError:
            pass
        good = not tb
        ok &= good
        checks.append(f"[{'PASS' if good else 'FAIL'}] no_traceback (traceback={tb})")

    return ok, checks


# ids already handed out this run, so we never reuse one even after cleanup
_USED_IDS: set[int] = set()


def _scan_ids(logs_dir: str, batchmeta_dir: str) -> set[int]:
    """Every transfer id already present in the host logs dir or batchmeta."""
    ids: set[int] = set()
    for d, pat in ((logs_dir, r"cloud_transfer_(\d+)"),
                   (batchmeta_dir, r"transfer_(\d+)")):
        p = Path(d)
        if not p.is_dir():
            continue
        for child in p.iterdir():
            m = re.fullmatch(pat, child.name)
            if m:
                ids.add(int(m.group(1)))
    return ids


def _alloc_transfer_id(cfg: NegConfig) -> int:
    """Pick a transfer id with no existing host log / batchmeta so each case reads
    its OWN fresh results CSV. The sandbox batchmeta is empty, so the shared host
    paths are what we must avoid colliding with."""
    existing = _scan_ids(cfg.transfer_logs_dir, st.DEF_BATCHMETA) | _USED_IDS
    nid = (max(existing) + 1) if existing else 501
    _USED_IDS.add(nid)
    return nid


def _cleanup_transfer_log(logs_dir: str, tr_id: int) -> None:
    """Remove the host-side cloud_transfer_<id> log this case created."""
    d = Path(logs_dir) / f"cloud_transfer_{tr_id}"
    if d.is_dir():
        shutil.rmtree(d, ignore_errors=True)


def run_case(case: NegCase, cfg: NegConfig, out_root: Path) -> dict:
    LOG.info("=" * 70)
    LOG.info("CASE %s — %s", case.id, case.title)
    result = {
        "id": case.id, "group": case.group, "title": case.title, "desc": case.desc,
        "status": "SKIP", "note": "", "checks": [], "rc": None,
        "csv_summary": {}, "structure": [], "expect": {},
    }

    if case.posix_only and not POSIX:
        result["note"] = "POSIX-only fault (chmod/symlink/signal); run on the Linux host"
        return result

    with Sandbox(out_root, case.id) as sb:
        setup = case.build(sb, cfg, case.id)
        result["note"] = setup.note
        result["structure"] = setup.structure
        result["expect"] = setup.expect

        if setup.skip:
            result["note"] = setup.skip
            return result

        tr_id = setup.forced_id or _alloc_transfer_id(cfg)
        transfer_dir = sb.batchmeta / f"transfer_{tr_id}"
        transfer_dir.mkdir(parents=True, exist_ok=True)
        for name, data in setup.pre_transfer_files.items():
            (transfer_dir / name).write_bytes(data)

        env = os.environ.copy()
        env.update(setup.env)

        stderr_path = sb.logs / f"stderr_{tr_id}.log"

        if cfg.dry_run:
            cmd = _build_cmd(cfg, tr_id, setup.src, setup.dst, transfer_dir, setup.extra_args)
            LOG.info("[dry-run] %s", " ".join(cmd))
            result["status"] = "SKIP"
            result["note"] = "dry-run: " + setup.note
            result["rc"] = None
            return result

        # optional first (killed) run for resume cases
        if setup.pre_kill:
            _run_proc(_build_cmd(cfg, tr_id, setup.src, setup.dst, transfer_dir, setup.extra_args),
                      env, cfg.timeout, sb.logs / f"stderr_prekill_{tr_id}.log",
                      signal_spec=setup.pre_kill)

        if setup.readonly_transfer:
            sb.guard_chmod(transfer_dir, 0o500)

        start_dt = _dt.datetime.now()
        cap = st.JournalCapture(cfg.journal_tag, tr_id, sb.logs, start_dt, cfg.dry_run,
                                lead_sec=cfg.capture_lead, drain_sec=cfg.capture_drain)
        cap.start()

        cmd = _build_cmd(cfg, tr_id, setup.src, setup.dst, transfer_dir, setup.extra_args)
        run_info = _run_proc(cmd, env, cfg.timeout, stderr_path,
                             signal_spec=setup.signal, delete_spec=setup.delete_after)
        cap.stop()

        # restore transfer-dir perms so CSV/report and rmtree work
        if setup.readonly_transfer:
            try:
                transfer_dir.chmod(0o755)
            except OSError:
                pass

        csv_summary = {"total": 0, "status_counts": {}, "success": 0, "failed": 0,
                       "total_bytes": 0, "completions_rel": [], "completion_span_sec": 0}
        csv_src = st.find_results_csv(cfg.transfer_logs_dir, tr_id)
        LOG.info("transfer_id=%s results_csv=%s", tr_id, csv_src or "<none>")
        if csv_src:
            try:
                csv_summary = st.parse_results_csv(csv_src, start_dt.year)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("could not parse results CSV: %s", exc)

        # keep the machine clean: drop the host-side log this case just created
        _cleanup_transfer_log(cfg.transfer_logs_dir, tr_id)

        ok, checks = _evaluate(setup.expect, run_info["rc"], run_info["timed_out"],
                               csv_summary, stderr_path)
        result.update({
            "status": "PASS" if ok else "FAIL",
            "checks": checks,
            "rc": run_info["rc"],
            "timed_out": run_info["timed_out"],
            "csv_summary": csv_summary,
            "transfer_id": tr_id,
        })
    return result


# =====================================================================================
# Report
# =====================================================================================
_STATUS_COLOR = {"PASS": "#3fb950", "FAIL": "#f85149", "SKIP": "#8b949e"}


def render_report(results: list[dict], out_dir: Path) -> Path:
    def _bytes(n):
        x = float(n or 0)
        for u in ("B", "KB", "MB", "GB", "TB"):
            if x < 1024 or u == "TB":
                return f"{x:.0f} {u}"
            x /= 1024
        return f"{n} B"

    npass = sum(1 for r in results if r["status"] == "PASS")
    nfail = sum(1 for r in results if r["status"] == "FAIL")
    nskip = sum(1 for r in results if r["status"] == "SKIP")

    rows = []
    for r in results:
        color = _STATUS_COLOR.get(r["status"], "#8b949e")
        checks = "<br>".join(r.get("checks", [])) or "—"
        struct = "<br>".join(
            f"{s.get('tier','-')}: {s.get('count',0)} files · {_bytes(s.get('total_bytes',0))}"
            for s in r.get("structure", [])) or "—"
        cs = r.get("csv_summary", {})
        rows.append(
            "<tr>"
            f"<td><b>{r['id']}</b><div class='mut'>{r['group']}</div></td>"
            f"<td>{r['title']}<div class='mut'>{r['note']}</div></td>"
            f"<td>{struct}</td>"
            f"<td><span class='pill' style='background:{color}'>{r['status']}</span>"
            f"<div class='mut'>rc={r.get('rc')}</div></td>"
            f"<td class='mut'>files={cs.get('total',0)} ok={cs.get('success',0)} "
            f"fail={cs.get('failed',0)}</td>"
            f"<td class='mut'>{checks}</td>"
            "</tr>"
        )

    html = _NEG_HTML.replace("__PASS__", str(npass)).replace("__FAIL__", str(nfail)) \
        .replace("__SKIP__", str(nskip)).replace("__ROWS__", "".join(rows)) \
        .replace("__TS__", _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    out = out_dir / "negative_report.html"
    out.write_text(html, encoding="utf-8")
    (out_dir / "negative_results.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    return out


_NEG_HTML = r"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Scheduler Negative Test Report</title>
<style>
body{margin:0;font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#0e1116;color:#e6edf3}
header{padding:18px 24px;border-bottom:1px solid #30363d;background:#161b22}
h1{margin:0;font-size:20px}.wrap{max-width:1200px;margin:0 auto;padding:24px}
.sub{color:#8b949e;margin-top:6px}
.tot{display:flex;gap:14px;margin:14px 0}
.tot div{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:10px 16px;font-weight:700}
table{width:100%;border-collapse:collapse;font-size:13px;background:#161b22;border:1px solid #30363d;border-radius:8px;overflow:hidden}
th,td{padding:9px 12px;border-bottom:1px solid #30363d;text-align:left;vertical-align:top}
th{color:#8b949e}.mut{color:#8b949e;font-size:12px;margin-top:3px}
.pill{color:#06131f;border-radius:6px;padding:2px 10px;font-weight:700;font-size:12px}
</style></head><body>
<header><h1>Scheduler Negative Test Report</h1>
<div class="sub">scheduler-level fault injection · __TS__</div></header>
<div class="wrap">
<div class="tot">
  <div style="color:#3fb950">PASS __PASS__</div>
  <div style="color:#f85149">FAIL __FAIL__</div>
  <div style="color:#8b949e">SKIP __SKIP__</div>
</div>
<table><thead><tr><th>Case</th><th>Fault</th><th>Synthesised data</th>
<th>Result</th><th>Transfer CSV</th><th>Checks</th></tr></thead>
<tbody>__ROWS__</tbody></table>
</div></body></html>
"""


def _zip_dir(src_dir: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(src_dir.rglob("*")):
            if p.is_file():
                zf.write(p, p.relative_to(src_dir.parent))


# =====================================================================================
# Orchestration entry points
# =====================================================================================
def select_cases(spec: str | None, run_all: bool) -> list[NegCase]:
    if run_all or not spec:
        return list(CASES)
    picked = []
    for tok in spec.split(","):
        tok = tok.strip().upper()
        if tok in CASE_BY_ID:
            picked.append(CASE_BY_ID[tok])
        else:
            raise SystemExit(f"error: unknown negative case '{tok}' "
                             f"(have {', '.join(CASE_BY_ID)})")
    return picked


def run_suite(cfg: NegConfig, out_dir: Path, cases: list[NegCase]) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    if os.name != "posix" and not cfg.dry_run:
        LOG.warning("this harness targets the Linux bryck host; use --dry-run on this OS")

    results = [run_case(c, cfg, out_dir) for c in cases]
    report = render_report(results, out_dir)
    _zip_dir(out_dir, out_dir.parent / f"{out_dir.name}.zip")

    LOG.info("=" * 70)
    for r in results:
        LOG.info("  %-13s %-4s  %s", r["id"], r["status"], r["title"])
    npass = sum(1 for r in results if r["status"] == "PASS")
    nfail = sum(1 for r in results if r["status"] == "FAIL")
    nskip = sum(1 for r in results if r["status"] == "SKIP")
    LOG.info("negative suite: %d PASS · %d FAIL · %d SKIP -> %s", npass, nfail, nskip, report)
    return 1 if nfail else 0


def run_from_args(args) -> int:
    """Entry point used by schedular_test.py --negative / --negative-case."""
    cfg = NegConfig.from_args(args)
    here = Path(__file__).resolve().parent
    out_root = Path(getattr(args, "out_dir", None) or here / "sch_test_runs")
    ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = out_root / f"negative_{ts}"
    cases = select_cases(getattr(args, "negative_case", None), getattr(args, "negative", False))
    return run_suite(cfg, out_dir, cases)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="schedular_negative_test.py",
        description="Scheduler-level negative test harness (fully sandboxed).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--all", action="store_true", help="run every negative case")
    ap.add_argument("--case", default=None, help="case id(s), comma-separated (e.g. NEG-ENUM-03)")
    ap.add_argument("--list", action="store_true", help="list cases and exit")

    ap.add_argument("--scheduler-python", default=st.DEF_SCHED_PY)
    ap.add_argument("--scheduler-script", default=st.DEF_SCHED_SCRIPT)
    ap.add_argument("--dir-path", default=st.DEF_DIR_PATH)
    ap.add_argument("--endpoint-url", default=st.DEF_ENDPOINT)
    ap.add_argument("--journal-tag", default=st.DEF_JOURNAL_TAG)
    ap.add_argument("--transfer-logs-dir", default=st.DEF_TRANSFER_LOGS)
    ap.add_argument("--s3-base", default=st.DEF_S3_BASE)
    ap.add_argument("--config", default=st.DEF_CONFIG)
    ap.add_argument("--capture-lead", type=float, default=st.DEF_CAPTURE_LEAD)
    ap.add_argument("--capture-drain", type=float, default=st.DEF_CAPTURE_DRAIN)
    ap.add_argument("--neg-timeout", type=int, default=DEF_TIMEOUT, help="per-case wall-clock bound (s)")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    return ap.parse_args(argv)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S",
    )
    if args.list:
        for c in CASES:
            tag = " (POSIX-only)" if c.posix_only else ""
            print(f"{c.id:<13} {c.group:<12} {c.title}{tag}")
        return 0

    # map standalone flags onto the shared entry point
    args.negative = args.all
    args.negative_case = args.case
    return run_from_args(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
