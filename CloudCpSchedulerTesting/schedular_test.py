#!/usr/bin/env python3
"""
schedular_test.py — end-to-end scheduler/broker test harness (standalone).

Runs on the Linux bryck host. For each selected deterministic-enumeration dataset
(SCH-ORD-01..12 / SCH-DEEP-01..03) it:

  1. materialises the data by invoking `datagen --spec-file <L*.yaml>` on every
     per-level spec under spec_files/<ID>/ in BFS chain order (L0 -> L4);
  2. records the ground-truth enumeration order to enumeration_order.json;
  3. allocates the next transfer id (max existing transfer_* in the batchmeta dir + 1)
     and creates transfer_<id>;
  4. starts a journalctl follower and fans matching lines into three log files
     (Pending-<id>, "free workers", "Running with workers") plus a raw log,
     stamping the test start;
  5. runs batch_scheduler.py and waits for it to exit (exit 0 == transfer complete),
     stamping the test completion;
  6. fetches the per-file results CSV
     (/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloud_transfer_<id>/transfer_report_<id>.csv);
  7. parses logs + CSV and renders a fully self-contained HTML report with an animated
     "replay" of pending / running workers / free workers over time, plus histograms;
  8. zips everything into sch_test_<id>.zip.

For a range or --all it produces a per-dataset zip for EACH dataset and one COMBINED
report + zip.

Assumes the spec files already exist (this script does NOT call generate_dataset.py).

Usage
-----
    python3 schedular_test.py 7                 # single dataset (number or id)
    python3 schedular_test.py SCH-DEEP-02
    python3 schedular_test.py --from 1 --to 5   # inclusive range
    python3 schedular_test.py --all             # all datasets, each + combined

Common options (all have host-sensible defaults):
    --spec-dir PATH        default: <this dir>/spec_files
    --data-root PATH       default: /bryck/cloudcp_sched_data
    --s3-base URI          default: s3://aditya/sch_test
    --datagen PATH         default: /home/bryck/rperiyas/datagen
    --datagen-flag FLAG    default: --spec-file
    --batchmeta-dir PATH   default: /opt/bryck/bryckapi/downloads/bcloud_batchmeta
    --transfer-logs-dir P  default: /opt/bryck/bryckapi/downloads/cloud_transfer_logs
    --scheduler-python P   default: /opt/bryck/.venv/bryck/bin/python3
    --scheduler-script P   default: .../site-packages/bryckcloud/lib/cloud/batch_scheduler.py
    --dir-path PATH        default: .../site-packages/bryckcloud/lib/cloud
    --endpoint-url URL     default: https://10.10.10.103:9000
    --config PATH          default: /etc/bryck/bryckcloud/config.json (for BATCH tiers)
    --out-dir PATH         default: <this dir>/sch_test_runs
    --poll-interval SEC    forwarded to batch_scheduler.py if set
    --skip-datagen         reuse already-materialised data
    --dry-run              print external commands without executing
"""

from __future__ import annotations

import argparse
import ast
import csv
import datetime as _dt
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import zipfile
from pathlib import Path

LOG = logging.getLogger("schedular_test")

# ---- host defaults -------------------------------------------------------------------
DEF_DATAGEN = "/home/bryck/rperiyas/datagen"
DEF_DATAGEN_FLAG = "--spec-file"
DEF_DATA_ROOT = "/bryck/cloudcp_sched_data"
DEF_S3_BASE = "s3://aditya/sch_test"
DEF_BATCHMETA = "/opt/bryck/bryckapi/downloads/bcloud_batchmeta"
DEF_TRANSFER_LOGS = "/opt/bryck/bryckapi/downloads/cloud_transfer_logs"
DEF_SCHED_PY = "/opt/bryck/.venv/bryck/bin/python3"
DEF_SCHED_SCRIPT = (
    "/opt/bryck/.venv/bryck/lib/python3.10/site-packages/"
    "bryckcloud/lib/cloud/batch_scheduler.py"
)
DEF_DIR_PATH = "/opt/bryck/.venv/bryck/lib/python3.10/site-packages/bryckcloud/lib/cloud"
DEF_ENDPOINT = "https://10.10.10.103:9000"
DEF_CONFIG = "/etc/bryck/bryckcloud/config.json"
DEF_JOURNAL_TAG = "bryckcloud"

# Embedded fallback if /etc config is unreadable (mirrors the BATCH block).
FALLBACK_BATCH = {
    "ZERO": {"BATCH_SIZE": 2000, "TARGET_SIZE_MB": 0, "OPEN_BATCHES": 4},
    "TINY": {"BATCH_SIZE": 511, "TARGET_SIZE_MB": 256, "OPEN_BATCHES": 8},
    "SMALL": {"BATCH_SIZE": 317, "TARGET_SIZE_MB": 2048, "OPEN_BATCHES": 8},
    "MEDIUM": {"BATCH_SIZE": 50, "TARGET_SIZE_MB": 10240, "OPEN_BATCHES": 8},
    "LARGE": {"BATCH_SIZE": 5, "TARGET_SIZE_MB": 51200, "OPEN_BATCHES": 8},
}

TIER_COLORS = {
    "zero": "#7f8c8d",
    "tiny": "#3498db",
    "small": "#2ecc71",
    "medium": "#f39c12",
    "large": "#e74c3c",
}
DEFAULT_TIER_ORDER = ["zero", "tiny", "small", "medium", "large"]

# journalctl default timestamp prefix: "Jul 31 05:15:33"
_TS_RE = re.compile(r"^([A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\b")
_PENDING_RE = re.compile(r"Pending-(\d+)\s*:\s*(\{.*\})")
_RUNNING_RE = re.compile(r"Running with workers\s*:\s*(\{.*\})")
_FREE_RE = re.compile(r"free workers\s+(\d+)")


# =====================================================================================
# Catalog / dataset resolution
# =====================================================================================
def load_manifest(spec_dir: Path) -> dict:
    mf = spec_dir / "manifest.json"
    if not mf.is_file():
        raise SystemExit(f"error: manifest.json not found at {mf}")
    with mf.open(encoding="utf-8") as fh:
        return json.load(fh)


def manifest_index(manifest: dict) -> dict[int, dict]:
    return {int(d["number"]): d for d in manifest.get("datasets", [])}


def _norm(x: str) -> set[str]:
    b = x.upper()
    return {b, b.replace("SCH-", ""), b.replace("SCH-", "").replace("-", ""), b.replace("-", "")}


def resolve_one(sel: str, by_num: dict[int, dict]) -> dict:
    s = sel.strip().upper()
    if s.isdigit():
        n = int(s)
        if n in by_num:
            return by_num[n]
        raise SystemExit(f"error: dataset number {n} out of range (have {sorted(by_num)})")
    want = _norm(s)
    for d in by_num.values():
        if want & _norm(d["id"]):
            return d
    raise SystemExit(f"error: could not resolve dataset '{sel}'")


def select_datasets(args, by_num: dict[int, dict]) -> list[dict]:
    if args.all:
        return [by_num[n] for n in sorted(by_num)]
    if args.from_ is not None or args.to is not None:
        lo = args.from_ if args.from_ is not None else min(by_num)
        hi = args.to if args.to is not None else max(by_num)
        if lo > hi:
            raise SystemExit("error: --from must be <= --to")
        picked = [by_num[n] for n in sorted(by_num) if lo <= n <= hi]
        if not picked:
            raise SystemExit(f"error: no datasets in range {lo}..{hi}")
        return picked
    if not args.dataset:
        raise SystemExit("error: provide a dataset selector, --from/--to, or --all")
    return [resolve_one(args.dataset, by_num)]


# =====================================================================================
# Config (BATCH tiers)
# =====================================================================================
def load_batch_config(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            cfg = json.load(fh)
        batch = cfg.get("BATCH", {})
        tiers = {k: v for k, v in batch.items() if isinstance(v, dict) and "BATCH_SIZE" in v}
        if tiers:
            return tiers
    except Exception as exc:  # noqa: BLE001 - best effort, fall back
        LOG.debug("could not read BATCH config from %s: %s", path, exc)
    LOG.info("using embedded fallback BATCH config")
    return dict(FALLBACK_BATCH)


def tier_order_from_config(batch_cfg: dict) -> list[str]:
    order = [t.lower() for t in ("ZERO", "TINY", "SMALL", "MEDIUM", "LARGE") if t in batch_cfg]
    for k in batch_cfg:
        if k.lower() not in order:
            order.append(k.lower())
    return order or list(DEFAULT_TIER_ORDER)


# =====================================================================================
# External command helper
# =====================================================================================
def run_cmd(cmd: list[str], dry_run: bool, check: bool = True) -> int:
    LOG.info("$ %s", " ".join(cmd))
    if dry_run:
        return 0
    proc = subprocess.run(cmd, check=False)
    if check and proc.returncode != 0:
        raise SystemExit(f"error: command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc.returncode


# =====================================================================================
# Step 1 — datagen
# =====================================================================================
def spec_files_in_order(spec_dir: Path, dataset_id: str) -> list[Path]:
    d = spec_dir / dataset_id
    if not d.is_dir():
        raise SystemExit(f"error: spec dir not found: {d}")
    specs = sorted(
        d.glob("L*.y*ml"),
        key=lambda p: int(re.match(r"L(\d+)_", p.name).group(1)) if re.match(r"L(\d+)_", p.name) else 999,
    )
    if not specs:
        raise SystemExit(f"error: no L*_*.yaml spec files under {d}")
    return specs


def run_datagen(specs: list[Path], datagen: str, flag: str, dry_run: bool) -> None:
    for sp in specs:
        run_cmd([datagen, flag, str(sp)], dry_run=dry_run, check=True)


# =====================================================================================
# Step 1.5 — enumeration order json
# =====================================================================================
def write_enumeration_order(dataset: dict, specs: list[Path], out_dir: Path) -> dict:
    levels = []
    for sp in specs:
        m = re.match(r"L(\d+)_([A-Za-z]+)\.", sp.name)
        if m:
            levels.append({"level": int(m.group(1)), "tier": m.group(2).lower(), "spec": sp.name})
    payload = {
        "dataset_id": dataset["id"],
        "number": dataset["number"],
        "profile": dataset.get("profile"),
        "enumeration_order": [t.lower() for t in dataset.get("enumeration_order", [])],
        "spec_levels": levels,
        "manifest_levels": dataset.get("levels", []),
        "totals": dataset.get("totals", {}),
        "note": "BFS chain order L0->L4; this is the deterministic enumeration oracle.",
    }
    out = out_dir / "enumeration_order.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOG.info("enumeration order -> %s : %s", out.name, " -> ".join(payload["enumeration_order"]))
    return payload


# =====================================================================================
# Step 3 — transfer id allocation
# =====================================================================================
def next_transfer_id(batchmeta_dir: str) -> int:
    d = Path(batchmeta_dir)
    ids = []
    if d.is_dir():
        for child in d.iterdir():
            m = re.fullmatch(r"transfer_(\d+)", child.name)
            if m:
                ids.append(int(m.group(1)))
    nxt = (max(ids) + 1) if ids else 501
    return nxt


def create_transfer_dir(batchmeta_dir: str, tr_id: int, dry_run: bool) -> Path:
    p = Path(batchmeta_dir) / f"transfer_{tr_id}"
    LOG.info("transfer-dir: %s", p)
    if not dry_run:
        p.mkdir(parents=True, exist_ok=True)
    return p


# =====================================================================================
# Step 4 — journalctl capture
# =====================================================================================
class JournalCapture:
    """Single `sudo journalctl -f` follower fanned into 3 filtered files + a raw log."""

    def __init__(self, tag: str, tr_id: int, log_dir: Path, since: _dt.datetime, dry_run: bool):
        self.tag = tag
        self.tr_id = tr_id
        self.dry_run = dry_run
        self.pending_path = log_dir / f"pending_{tr_id}.log"
        self.free_path = log_dir / f"free_workers_{tr_id}.log"
        self.running_path = log_dir / f"running_workers_{tr_id}.log"
        self.raw_path = log_dir / f"raw_{tr_id}.log"
        self._since = since
        self._proc: subprocess.Popen | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        for p in (self.pending_path, self.free_path, self.running_path, self.raw_path):
            p.write_text("", encoding="utf-8")
        if self.dry_run:
            LOG.info("[dry-run] would start journalctl follower for tag %s", self.tag)
            return
        since = self._since.strftime("%Y-%m-%d %H:%M:%S")
        cmd = ["sudo", "journalctl", "-f", "-t", self.tag, "--since", since, "-o", "short"]
        LOG.info("$ %s", " ".join(cmd))
        self._proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, bufsize=1, preexec_fn=os.setsid,
        )
        self._thread = threading.Thread(target=self._reader, daemon=True)
        self._thread.start()

    def _reader(self) -> None:
        pend = self.pending_path.open("a", encoding="utf-8")
        free = self.free_path.open("a", encoding="utf-8")
        run = self.running_path.open("a", encoding="utf-8")
        raw = self.raw_path.open("a", encoding="utf-8")
        try:
            assert self._proc and self._proc.stdout
            for line in self._proc.stdout:
                if self._stop.is_set():
                    break
                raw.write(line)
                raw.flush()
                if f"Pending-{self.tr_id}" in line:
                    pend.write(line)
                    pend.flush()
                elif "free workers" in line:
                    free.write(line)
                    free.flush()
                elif "Running with workers" in line:
                    run.write(line)
                    run.flush()
        except Exception as exc:  # noqa: BLE001
            LOG.debug("journal reader stopped: %s", exc)
        finally:
            for fh in (pend, free, run, raw):
                fh.close()

    def stop(self) -> None:
        self._stop.set()
        if self.dry_run or self._proc is None:
            return
        try:
            os.killpg(os.getpgid(self._proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            self._proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(self._proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
        if self._thread:
            self._thread.join(timeout=5)


# =====================================================================================
# Step 5 — scheduler
# =====================================================================================
def build_scheduler_cmd(args, tr_id: int, src: str, dst: str, base_src: str, transfer_dir: Path) -> list[str]:
    cmd = [
        args.scheduler_python, args.scheduler_script,
        str(tr_id), "upload", src, dst, base_src,
        "--transfer-dir", str(transfer_dir),
        "--dir-path", args.dir_path,
        "--endpoint-url", args.endpoint_url,
    ]
    if args.poll_interval is not None:
        cmd += ["--poll-interval", str(args.poll_interval)]
    return cmd


def run_scheduler(cmd: list[str], dry_run: bool) -> int:
    LOG.info("$ %s", " ".join(cmd))
    if dry_run:
        return 0
    proc = subprocess.run(cmd, check=False)
    LOG.info("scheduler exited with code %s", proc.returncode)
    return proc.returncode


# =====================================================================================
# Step 6/8 — parse logs & CSV
# =====================================================================================
def parse_ts(line: str, year: int) -> _dt.datetime | None:
    m = _TS_RE.match(line)
    if not m:
        return None
    try:
        return _dt.datetime.strptime(f"{year} {m.group(1)}", "%Y %b %d %H:%M:%S")
    except ValueError:
        return None


def _safe_dict(text: str) -> dict:
    try:
        d = ast.literal_eval(text)
        if isinstance(d, dict):
            return {str(k).lower(): int(v) for k, v in d.items()}
    except Exception:  # noqa: BLE001
        pass
    return {}


def parse_capture(cap: JournalCapture, year: int) -> dict:
    """Return merged, forward-filled timeline + raw series."""
    pending_events, running_events, free_events = [], [], []

    for line in cap.pending_path.read_text(encoding="utf-8").splitlines():
        ts = parse_ts(line, year)
        m = _PENDING_RE.search(line)
        if ts and m:
            pending_events.append((ts, _safe_dict(m.group(2))))
    for line in cap.running_path.read_text(encoding="utf-8").splitlines():
        ts = parse_ts(line, year)
        m = _RUNNING_RE.search(line)
        if ts and m:
            running_events.append((ts, _safe_dict(m.group(1))))
    for line in cap.free_path.read_text(encoding="utf-8").splitlines():
        ts = parse_ts(line, year)
        m = _FREE_RE.search(line)
        if ts and m:
            free_events.append((ts, int(m.group(1))))

    all_ts = sorted({ts for ts, _ in pending_events} | {ts for ts, _ in running_events} | {ts for ts, _ in free_events})
    timeline = []
    if all_ts:
        t0 = all_ts[0]
        pi = ri = fi = 0
        cur_pending, cur_running, cur_free = {}, {}, 0
        for ts in all_ts:
            while pi < len(pending_events) and pending_events[pi][0] <= ts:
                cur_pending = pending_events[pi][1]
                pi += 1
            while ri < len(running_events) and running_events[ri][0] <= ts:
                cur_running = running_events[ri][1]
                ri += 1
            while fi < len(free_events) and free_events[fi][0] <= ts:
                cur_free = free_events[fi][1]
                fi += 1
            timeline.append({
                "t": (ts - t0).total_seconds(),
                "iso": ts.isoformat(),
                "pending": dict(cur_pending),
                "running": dict(cur_running),
                "free": cur_free,
            })
    tiers_seen = set()
    for _, d in pending_events:
        tiers_seen |= set(d)
    for _, d in running_events:
        tiers_seen |= set(d)
    return {
        "timeline": timeline,
        "tiers_seen": sorted(tiers_seen),
        "counts": {
            "pending_events": len(pending_events),
            "running_events": len(running_events),
            "free_events": len(free_events),
        },
    }


def find_results_csv(transfer_logs_dir: str, tr_id: int) -> Path | None:
    p = Path(transfer_logs_dir) / f"cloud_transfer_{tr_id}" / f"transfer_report_{tr_id}.csv"
    return p if p.is_file() else None


def parse_results_csv(csv_path: Path, year: int) -> dict:
    rows, completions = [], []
    status_counts: dict[str, int] = {}
    total_bytes = 0
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
            status = (row.get("status") or "").strip().upper()
            status_counts[status] = status_counts.get(status, 0) + 1
            try:
                total_bytes += int(row.get("size") or 0)
            except ValueError:
                pass
            fin = (row.get("finished_at") or "").strip()
            if fin:
                try:
                    completions.append(_dt.datetime.fromisoformat(fin))
                except ValueError:
                    pass
    completions.sort()
    comp_rel = []
    if completions:
        t0 = completions[0]
        comp_rel = [(c - t0).total_seconds() for c in completions]
    return {
        "total": len(rows),
        "status_counts": status_counts,
        "success": status_counts.get("SUCCESS", 0),
        "failed": sum(v for k, v in status_counts.items() if k not in ("SUCCESS",)),
        "total_bytes": total_bytes,
        "completions_rel": comp_rel,
        "completion_span_sec": (comp_rel[-1] - comp_rel[0]) if comp_rel else 0,
    }


# =====================================================================================
# Step 7 — HTML report (self-contained)
# =====================================================================================
def _histogram(values: list[float], nbins: int = 40) -> dict:
    if not values:
        return {"bins": [], "counts": [], "width": 0}
    lo, hi = min(values), max(values)
    if hi <= lo:
        hi = lo + 1.0
    width = (hi - lo) / nbins
    counts = [0] * nbins
    for v in values:
        idx = min(int((v - lo) / width), nbins - 1)
        counts[idx] += 1
    bins = [lo + i * width for i in range(nbins)]
    return {"bins": bins, "counts": counts, "width": width, "lo": lo, "hi": hi}


def build_report_payload(dataset, tr_id, enum_payload, cap_data, csv_summary,
                         meta, batch_cfg, tier_order) -> dict:
    tiers = list(dict.fromkeys(tier_order + cap_data.get("tiers_seen", [])))
    return {
        "meta": meta,
        "dataset": {
            "id": dataset["id"], "number": dataset["number"],
            "profile": dataset.get("profile"), "totals": dataset.get("totals", {}),
        },
        "transfer_id": tr_id,
        "enumeration_order": enum_payload["enumeration_order"],
        "tiers": tiers,
        "tier_colors": {t: TIER_COLORS.get(t, "#9b59b6") for t in tiers},
        "batch_config": {k.lower(): v for k, v in batch_cfg.items()},
        "timeline": cap_data["timeline"],
        "log_counts": cap_data["counts"],
        "csv_summary": csv_summary,
        "completion_hist": _histogram(csv_summary.get("completions_rel", [])),
    }


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Scheduler Test Report — __TITLE__</title>
<style>
  :root{--bg:#0e1116;--panel:#161b22;--fg:#e6edf3;--mut:#8b949e;--line:#30363d;--acc:#58a6ff;}
  *{box-sizing:border-box}
  body{margin:0;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;background:var(--bg);color:var(--fg);}
  header{padding:18px 24px;border-bottom:1px solid var(--line);background:var(--panel);}
  h1{margin:0;font-size:20px}
  h2{font-size:15px;color:var(--acc);margin:0 0 10px;text-transform:uppercase;letter-spacing:.05em}
  .sub{color:var(--mut);font-size:13px;margin-top:4px}
  .wrap{max-width:1180px;margin:0 auto;padding:24px}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px}
  .kv{display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px dashed var(--line);font-size:13px}
  .kv:last-child{border-bottom:none}
  .kv .k{color:var(--mut)} .kv .v{font-weight:600}
  .big{font-size:26px;font-weight:700}
  .ok{color:#3fb950}.bad{color:#f85149}
  .controls{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin:14px 0}
  button{background:var(--acc);color:#06131f;border:0;border-radius:6px;padding:8px 14px;font-weight:700;cursor:pointer}
  button.sec{background:#21262d;color:var(--fg);border:1px solid var(--line)}
  input[type=range]{flex:1;min-width:200px}
  select{background:#21262d;color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:6px}
  canvas{width:100%;background:#0b0f14;border:1px solid var(--line);border-radius:8px}
  .row{display:grid;grid-template-columns:1fr 1fr;gap:14px}
  .clock{font-variant-numeric:tabular-nums;color:var(--acc);font-weight:700}
  .legend{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:var(--mut);margin-top:8px}
  .legend span{display:inline-flex;align-items:center;gap:6px}
  .dot{width:11px;height:11px;border-radius:2px;display:inline-block}
  .muted{color:var(--mut);font-size:12px}
  section{margin-top:26px}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line)}
  th{color:var(--mut);font-weight:600}
  @media(max-width:760px){.row{grid-template-columns:1fr}}
</style>
</head>
<body>
<header>
  <h1>Scheduler Test Report — <span id="ttl"></span></h1>
  <div class="sub" id="subttl"></div>
</header>
<div class="wrap">

  <section>
    <h2>Summary</h2>
    <div class="grid" id="summaryGrid"></div>
  </section>

  <section>
    <h2>Replay — pending / running / free workers over time</h2>
    <div class="controls">
      <button id="playBtn">▶ Play</button>
      <button id="resetBtn" class="sec">⟲ Reset</button>
      <span class="clock" id="clock">t = 0.0s</span>
      <label class="muted">speed
        <select id="speed">
          <option value="1">1×</option>
          <option value="2" selected>2×</option>
          <option value="4">4×</option>
          <option value="8">8×</option>
          <option value="16">16×</option>
        </select>
      </label>
      <input type="range" id="scrub" min="0" max="0" value="0"/>
    </div>
    <div class="row">
      <div>
        <canvas id="pendingCv" height="260"></canvas>
        <div class="muted" style="text-align:center">Pending batches per tier</div>
      </div>
      <div>
        <canvas id="runningCv" height="260"></canvas>
        <div class="muted" style="text-align:center">Running workers per tier</div>
      </div>
    </div>
    <div class="row" style="margin-top:14px">
      <div>
        <canvas id="freeCv" height="120"></canvas>
        <div class="muted" style="text-align:center">Free workers</div>
      </div>
      <div>
        <canvas id="totalCv" height="120"></canvas>
        <div class="muted" style="text-align:center">Total running vs free (stacked)</div>
      </div>
    </div>
    <div class="legend" id="legend"></div>
  </section>

  <section>
    <h2>Time-series (full transfer)</h2>
    <canvas id="tsCv" height="300"></canvas>
    <div class="muted" style="text-align:center">Pending totals, running total &amp; free workers vs time (s)</div>
  </section>

  <section>
    <h2>Completion histogram (from results CSV)</h2>
    <canvas id="histCv" height="220"></canvas>
    <div class="muted" style="text-align:center">Files completed per time-bin (finished_at)</div>
  </section>

  <section>
    <h2>Per-status breakdown</h2>
    <table id="statusTbl"><thead><tr><th>Status</th><th>Count</th></tr></thead><tbody></tbody></table>
  </section>

</div>
<script id="payload" type="application/json">__DATA__</script>
<script>
const DATA = JSON.parse(document.getElementById('payload').textContent);
const TIERS = DATA.tiers, COL = DATA.tier_colors;
const TL = DATA.timeline;
const fmt = n => (n>=1e6?(n/1e6).toFixed(1)+'M':n>=1e3?(n/1e3).toFixed(1)+'k':(''+n));
const bytes = n => {const u=['B','KB','MB','GB','TB'];let i=0,x=n;while(x>=1024&&i<u.length-1){x/=1024;i++;}return x.toFixed(x<10?2:0)+' '+u[i];};

document.getElementById('ttl').textContent = DATA.dataset.id + '  ·  transfer ' + DATA.transfer_id;
document.getElementById('subttl').textContent =
  'profile ' + (DATA.dataset.profile||'-') + '  ·  enumeration: ' + DATA.enumeration_order.join(' → ')
  + '  ·  ' + (DATA.meta.start||'') + '  →  ' + (DATA.meta.end||'');

// ---- summary cards ----
const cs = DATA.csv_summary || {};
const dur = DATA.meta.duration_sec || 0;
const cards = [
  ['Transfer', [['dataset',DATA.dataset.id],['transfer id',DATA.transfer_id],['profile',DATA.dataset.profile||'-'],
                ['scheduler exit',DATA.meta.scheduler_exit],['duration', dur.toFixed(1)+' s']]],
  ['Results (CSV)', [['files',fmt(cs.total||0)],['success',(cs.success||0)],['failed',(cs.failed||0)],
                     ['bytes',bytes(cs.total_bytes||0)],['completion span',(cs.completion_span_sec||0).toFixed(1)+' s']]],
  ['Log capture', [['pending events',DATA.log_counts.pending_events],['running events',DATA.log_counts.running_events],
                   ['free events',DATA.log_counts.free_events],['timeline frames',TL.length]]],
];
const sg = document.getElementById('summaryGrid');
for(const [title,kvs] of cards){
  const d=document.createElement('div'); d.className='card';
  d.innerHTML='<div class="big">'+title+'</div>'+kvs.map(([k,v])=>
    '<div class="kv"><span class="k">'+k+'</span><span class="v">'+(v==null?'-':v)+'</span></div>').join('');
  sg.appendChild(d);
}
// status table
const stb=document.querySelector('#statusTbl tbody');
Object.entries(cs.status_counts||{}).forEach(([s,c])=>{
  const cls = s==='SUCCESS'?'ok':(s?'bad':'');
  stb.innerHTML+='<tr><td class="'+cls+'">'+(s||'(blank)')+'</td><td>'+c+'</td></tr>';
});
// legend
const lg=document.getElementById('legend');
TIERS.forEach(t=>{lg.innerHTML+='<span><i class="dot" style="background:'+(COL[t]||'#888')+'"></i>'+t+'</span>';});

// ---- canvas helpers ----
function prep(cv){const r=window.devicePixelRatio||1;const w=cv.clientWidth;const h=cv.height;
  cv.width=w*r;cv.height=h*r;const c=cv.getContext('2d');c.setTransform(r,0,0,r,0,0);return {c,w,h};}
function clear(c,w,h){c.clearRect(0,0,w,h);}
function axis(c,w,h,pad){c.strokeStyle='#30363d';c.lineWidth=1;c.beginPath();
  c.moveTo(pad,h-pad);c.lineTo(w-6,h-pad);c.moveTo(pad,h-pad);c.lineTo(pad,8);c.stroke();}

function drawBars(cv, map, maxV){
  const {c,w,h}=prep(cv); clear(c,w,h); const pad=34; axis(c,w,h,pad);
  const n=TIERS.length; const bw=(w-pad-10)/n; maxV=Math.max(maxV,1);
  c.font='11px sans-serif';
  TIERS.forEach((t,i)=>{
    const v=map[t]||0; const bh=(h-pad-14)*(v/maxV);
    const x=pad+i*bw+bw*0.18; const bwidth=bw*0.64;
    c.fillStyle=COL[t]||'#888'; c.fillRect(x,h-pad-bh,bwidth,bh);
    c.fillStyle='#e6edf3'; c.textAlign='center';
    c.fillText(fmt(v), x+bwidth/2, h-pad-bh-4);
    c.fillStyle='#8b949e'; c.fillText(t, x+bwidth/2, h-pad+12);
  });
  c.fillStyle='#8b949e'; c.textAlign='left'; c.fillText('max '+fmt(maxV), pad+2, 12);
}
function drawFree(cv, val, maxV){
  const {c,w,h}=prep(cv); clear(c,w,h); const pad=30;
  maxV=Math.max(maxV,1); const bw=(w-pad-10)*(val/maxV);
  c.fillStyle='#238636'; c.fillRect(pad,h/2-16,bw,32);
  c.fillStyle='#e6edf3'; c.font='20px sans-serif'; c.textAlign='left';
  c.fillText(val+' free', pad+6, h/2+7);
}
function drawStack(cv, running, free, maxV){
  const {c,w,h}=prep(cv); clear(c,w,h); const pad=30; maxV=Math.max(maxV,1);
  let x=pad; const totalRun=TIERS.reduce((a,t)=>a+(running[t]||0),0);
  const scale=(w-pad-10)/maxV;
  TIERS.forEach(t=>{const seg=(running[t]||0)*scale; c.fillStyle=COL[t]||'#888';
    c.fillRect(x,h/2-16,seg,32); x+=seg;});
  c.fillStyle='#30363d'; c.fillRect(x,h/2-16,(free)*scale,32);
  c.fillStyle='#e6edf3'; c.font='13px sans-serif'; c.textAlign='left';
  c.fillText('running '+totalRun+' · free '+free, pad+4, h/2-22);
}

// precompute maxima
let maxPend=1,maxRun=1,maxFree=1,maxSlots=1;
TL.forEach(f=>{
  TIERS.forEach(t=>{maxPend=Math.max(maxPend,f.pending[t]||0);maxRun=Math.max(maxRun,f.running[t]||0);});
  maxFree=Math.max(maxFree,f.free||0);
  const run=TIERS.reduce((a,t)=>a+(f.running[t]||0),0);
  maxSlots=Math.max(maxSlots,run+(f.free||0));
});

const pendCv=document.getElementById('pendingCv'), runCv=document.getElementById('runningCv'),
      freeCv=document.getElementById('freeCv'), totCv=document.getElementById('totalCv');
function renderFrame(i){
  if(!TL.length)return;
  const f=TL[Math.max(0,Math.min(i,TL.length-1))];
  drawBars(pendCv,f.pending,maxPend);
  drawBars(runCv,f.running,maxRun);
  drawFree(freeCv,f.free||0,Math.max(maxFree,maxSlots));
  drawStack(totCv,f.running,f.free||0,maxSlots);
  document.getElementById('clock').textContent='t = '+(f.t||0).toFixed(1)+'s  ('+f.iso.split('T')[1]+')';
  document.getElementById('scrub').value=i;
}

// ---- static time-series ----
function drawTS(){
  const cv=document.getElementById('tsCv'); const {c,w,h}=prep(cv); clear(c,w,h);
  if(!TL.length){c.fillStyle='#8b949e';c.fillText('no timeline data',20,30);return;}
  const pad=40; axis(c,w,h,pad);
  const tMax=TL[TL.length-1].t||1;
  const pendTot=TL.map(f=>TIERS.reduce((a,t)=>a+(f.pending[t]||0),0));
  const runTot=TL.map(f=>TIERS.reduce((a,t)=>a+(f.running[t]||0),0));
  const freeArr=TL.map(f=>f.free||0);
  const yMaxL=Math.max(1,...pendTot);
  const yMaxR=Math.max(1,...runTot,...freeArr);
  const X=t=>pad+(w-pad-10)*(t/tMax);
  function line(arr,ymax,color){c.strokeStyle=color;c.lineWidth=2;c.beginPath();
    TL.forEach((f,i)=>{const x=X(f.t);const y=(h-pad-8)-(h-pad-16)*(arr[i]/ymax);
      i?c.lineTo(x,y):c.moveTo(x,y);});c.stroke();}
  line(pendTot,yMaxL,'#58a6ff'); line(runTot,yMaxR,'#f39c12'); line(freeArr,yMaxR,'#3fb950');
  c.font='11px sans-serif';c.textAlign='left';
  c.fillStyle='#58a6ff';c.fillText('pending total (L)',pad+4,14);
  c.fillStyle='#f39c12';c.fillText('running total (R)',pad+140,14);
  c.fillStyle='#3fb950';c.fillText('free (R)',pad+280,14);
  c.fillStyle='#8b949e';c.textAlign='center';c.fillText(tMax.toFixed(0)+'s',w-24,h-pad+14);
}
function drawHist(){
  const cv=document.getElementById('histCv'); const {c,w,h}=prep(cv); clear(c,w,h);
  const H=DATA.completion_hist||{}; const pad=40; axis(c,w,h,pad);
  const counts=H.counts||[]; if(!counts.length){c.fillStyle='#8b949e';c.fillText('no completion data',20,30);return;}
  const ymax=Math.max(1,...counts); const bw=(w-pad-10)/counts.length;
  counts.forEach((v,i)=>{const bh=(h-pad-12)*(v/ymax);const x=pad+i*bw;
    c.fillStyle='#58a6ff';c.fillRect(x+1,h-pad-bh,bw-1,bh);});
  c.fillStyle='#8b949e';c.font='11px sans-serif';c.textAlign='left';
  c.fillText('peak '+ymax+' files/bin',pad+4,14);
  c.textAlign='center';c.fillText(((H.hi||0)).toFixed(0)+'s',w-24,h-pad+14);
}

// ---- animation ----
let idx=0, playing=false, timer=null;
const playBtn=document.getElementById('playBtn'), scrub=document.getElementById('scrub');
scrub.max=Math.max(0,TL.length-1);
function tick(){
  if(!playing)return;
  idx++; if(idx>=TL.length){idx=TL.length-1;stop();renderFrame(idx);return;}
  renderFrame(idx);
}
function start(){if(!TL.length)return;playing=true;playBtn.textContent='⏸ Pause';
  const spd=parseInt(document.getElementById('speed').value,10);
  clearInterval(timer);timer=setInterval(tick,Math.max(40,400/spd));}
function stop(){playing=false;playBtn.textContent='▶ Play';clearInterval(timer);}
playBtn.onclick=()=>{playing?stop():(idx>=TL.length-1?(idx=0):0,start());};
document.getElementById('resetBtn').onclick=()=>{stop();idx=0;renderFrame(0);};
document.getElementById('speed').onchange=()=>{if(playing)start();};
scrub.oninput=e=>{stop();idx=parseInt(e.target.value,10);renderFrame(idx);};

function redraw(){renderFrame(idx);drawTS();drawHist();}
window.addEventListener('resize',redraw);
redraw();
</script>
</body>
</html>
"""


def render_html(payload: dict, out_path: Path) -> None:
    data_json = json.dumps(payload, separators=(",", ":")).replace("</", "<\\/")
    html = (HTML_TEMPLATE
            .replace("__TITLE__", f"{payload['dataset']['id']} · transfer {payload['transfer_id']}")
            .replace("__DATA__", data_json))
    out_path.write_text(html, encoding="utf-8")


# =====================================================================================
# Step 9 — zip
# =====================================================================================
def zip_dir(src_dir: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(src_dir.rglob("*")):
            if p.is_file():
                zf.write(p, p.relative_to(src_dir.parent))
    LOG.info("wrote %s (%.1f KB)", zip_path, zip_path.stat().st_size / 1024)


# =====================================================================================
# Per-dataset orchestration
# =====================================================================================
def run_one(dataset: dict, args, spec_dir: Path, batch_cfg: dict, tier_order: list[str],
            out_root: Path) -> dict:
    ds_id = dataset["id"]
    LOG.info("=" * 70)
    LOG.info("DATASET %s (#%s, profile %s)", ds_id, dataset["number"], dataset.get("profile"))

    specs = spec_files_in_order(spec_dir, ds_id)

    # 1. datagen
    if args.skip_datagen:
        LOG.info("--skip-datagen: reusing existing data for %s", ds_id)
    else:
        run_datagen(specs, args.datagen, args.datagen_flag, args.dry_run)

    # 3. transfer id + dir
    tr_id = next_transfer_id(args.batchmeta_dir)
    transfer_dir = create_transfer_dir(args.batchmeta_dir, tr_id, args.dry_run)

    report_dir = out_root / f"report_{tr_id}"
    log_dir = report_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # 1.5 enumeration order json
    enum_payload = write_enumeration_order(dataset, specs, report_dir)

    src = f"{args.data_root.rstrip('/')}/{ds_id}"
    dst = f"{args.s3_base.rstrip('/')}/{ds_id}"
    base_src = src

    # 4. journal capture
    start_dt = _dt.datetime.now()
    cap = JournalCapture(args.journal_tag, tr_id, log_dir, start_dt, args.dry_run)
    cap.start()

    # 5. scheduler (blocks until transfer complete)
    cmd = build_scheduler_cmd(args, tr_id, src, dst, base_src, transfer_dir)
    sched_rc = run_scheduler(cmd, args.dry_run)
    end_dt = _dt.datetime.now()

    # stop capture
    cap.stop()

    year = start_dt.year
    cap_data = parse_capture(cap, year)

    # 6. results csv
    csv_dst = report_dir / f"transfer_report_{tr_id}.csv"
    csv_summary: dict = {"total": 0, "status_counts": {}, "success": 0, "failed": 0,
                         "total_bytes": 0, "completions_rel": [], "completion_span_sec": 0}
    if not args.dry_run:
        csv_src = find_results_csv(args.transfer_logs_dir, tr_id)
        if csv_src:
            shutil.copy2(csv_src, csv_dst)
            csv_summary = parse_results_csv(csv_dst, year)
            LOG.info("results CSV: %s (%d rows, %d success)",
                     csv_src, csv_summary["total"], csv_summary["success"])
        else:
            LOG.warning("results CSV not found for transfer %s under %s",
                        tr_id, args.transfer_logs_dir)

    meta = {
        "dataset_id": ds_id,
        "transfer_id": tr_id,
        "src": src, "dst": dst, "base_src": base_src,
        "transfer_dir": str(transfer_dir),
        "start": start_dt.isoformat(timespec="seconds"),
        "end": end_dt.isoformat(timespec="seconds"),
        "duration_sec": round((end_dt - start_dt).total_seconds(), 1),
        "scheduler_exit": sched_rc,
        "scheduler_cmd": " ".join(cmd),
        "endpoint_url": args.endpoint_url,
    }
    (report_dir / "run_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    # 7. html + summary
    payload = build_report_payload(dataset, tr_id, enum_payload, cap_data, csv_summary,
                                   meta, batch_cfg, tier_order)
    render_html(payload, report_dir / "report.html")
    write_summary_txt(report_dir / "summary.txt", meta, enum_payload, cap_data, csv_summary)

    # 9. zip
    zip_path = out_root / f"sch_test_{tr_id}.zip"
    if zip_path.exists():
        zip_path.unlink()
    zip_dir(report_dir, zip_path)

    return {
        "dataset_id": ds_id, "transfer_id": tr_id, "meta": meta,
        "csv_summary": csv_summary, "report_dir": str(report_dir),
        "zip": str(zip_path), "log_counts": cap_data["counts"],
    }


def write_summary_txt(path: Path, meta, enum_payload, cap_data, csv_summary) -> None:
    lines = [
        "Scheduler Test Summary",
        "=" * 60,
        f"dataset          : {meta['dataset_id']}",
        f"transfer id      : {meta['transfer_id']}",
        f"enumeration order: {' -> '.join(enum_payload['enumeration_order'])}",
        f"src              : {meta['src']}",
        f"dst              : {meta['dst']}",
        f"transfer-dir     : {meta['transfer_dir']}",
        f"start            : {meta['start']}",
        f"end              : {meta['end']}",
        f"duration (s)     : {meta['duration_sec']}",
        f"scheduler exit   : {meta['scheduler_exit']}",
        "",
        "Results (CSV):",
        f"  total files    : {csv_summary['total']}",
        f"  success        : {csv_summary['success']}",
        f"  failed         : {csv_summary['failed']}",
        f"  total bytes    : {csv_summary['total_bytes']}",
        f"  status counts  : {csv_summary['status_counts']}",
        "",
        "Log capture:",
        f"  pending events : {cap_data['counts']['pending_events']}",
        f"  running events : {cap_data['counts']['running_events']}",
        f"  free events    : {cap_data['counts']['free_events']}",
        f"  timeline frames: {len(cap_data['timeline'])}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# =====================================================================================
# Combined report
# =====================================================================================
def build_combined(results: list[dict], out_root: Path, dry_run: bool) -> None:
    ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    comb_dir = out_root / f"combined_{ts}"
    comb_dir.mkdir(parents=True, exist_ok=True)

    rows = ""
    for r in results:
        cs = r["csv_summary"]
        rows += (
            "<tr>"
            f"<td>{r['dataset_id']}</td>"
            f"<td>{r['transfer_id']}</td>"
            f"<td>{r['meta']['duration_sec']}</td>"
            f"<td>{cs['total']}</td>"
            f"<td class='ok'>{cs['success']}</td>"
            f"<td class='bad'>{cs['failed']}</td>"
            f"<td>{r['meta']['scheduler_exit']}</td>"
            f"<td><a href='report_{r['transfer_id']}/report.html'>open</a></td>"
            "</tr>"
        )
    agg = {
        "datasets": len(results),
        "total_files": sum(r["csv_summary"]["total"] for r in results),
        "total_success": sum(r["csv_summary"]["success"] for r in results),
        "total_failed": sum(r["csv_summary"]["failed"] for r in results),
        "results": [{"dataset_id": r["dataset_id"], "transfer_id": r["transfer_id"],
                     "duration_sec": r["meta"]["duration_sec"],
                     "csv_summary": r["csv_summary"]} for r in results],
    }
    (comb_dir / "aggregate.json").write_text(json.dumps(agg, indent=2), encoding="utf-8")

    html = COMBINED_TEMPLATE.replace("__ROWS__", rows).replace("__TS__", ts).replace(
        "__AGG__", (f"{agg['datasets']} datasets · {agg['total_files']} files · "
                    f"{agg['total_success']} ok · {agg['total_failed']} failed"))
    (comb_dir / "index.html").write_text(html, encoding="utf-8")

    # copy per-dataset report dirs into combined for a self-contained bundle
    for r in results:
        src = Path(r["report_dir"])
        if src.is_dir():
            shutil.copytree(src, comb_dir / src.name, dirs_exist_ok=True)

    zip_path = out_root / f"sch_test_combined_{ts}.zip"
    zip_dir(comb_dir, zip_path)
    LOG.info("combined report: %s", zip_path)


COMBINED_TEMPLATE = r"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Scheduler Test — Combined __TS__</title>
<style>
body{margin:0;font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#0e1116;color:#e6edf3}
header{padding:18px 24px;border-bottom:1px solid #30363d;background:#161b22}
h1{margin:0;font-size:20px}.wrap{max-width:1000px;margin:0 auto;padding:24px}
.sub{color:#8b949e;margin-top:6px}
table{width:100%;border-collapse:collapse;font-size:14px;background:#161b22;border:1px solid #30363d;border-radius:8px;overflow:hidden}
th,td{padding:9px 12px;border-bottom:1px solid #30363d;text-align:left}
th{color:#8b949e}a{color:#58a6ff}.ok{color:#3fb950}.bad{color:#f85149}
</style></head><body>
<header><h1>Scheduler Test — Combined Report</h1><div class="sub">__AGG__ · __TS__</div></header>
<div class="wrap">
<table><thead><tr><th>Dataset</th><th>Transfer</th><th>Duration (s)</th><th>Files</th>
<th>Success</th><th>Failed</th><th>Exit</th><th>Report</th></tr></thead>
<tbody>__ROWS__</tbody></table>
</div></body></html>
"""


# =====================================================================================
# CLI
# =====================================================================================
def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="schedular_test.py",
        description="End-to-end scheduler/broker test harness for the SCH-* datasets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("dataset", nargs="?", help="dataset number (1..15) or id (SCH-ORD-07 / DEEP-02)")
    ap.add_argument("--from", dest="from_", type=int, help="range start (inclusive)")
    ap.add_argument("--to", dest="to", type=int, help="range end (inclusive)")
    ap.add_argument("--all", action="store_true", help="run every dataset (each + combined)")

    ap.add_argument("--spec-dir", default=None, help="spec_files dir (default: <script>/spec_files)")
    ap.add_argument("--data-root", default=DEF_DATA_ROOT)
    ap.add_argument("--s3-base", default=DEF_S3_BASE)
    ap.add_argument("--datagen", default=DEF_DATAGEN)
    ap.add_argument("--datagen-flag", default=DEF_DATAGEN_FLAG)
    ap.add_argument("--batchmeta-dir", default=DEF_BATCHMETA)
    ap.add_argument("--transfer-logs-dir", default=DEF_TRANSFER_LOGS)
    ap.add_argument("--scheduler-python", default=DEF_SCHED_PY)
    ap.add_argument("--scheduler-script", default=DEF_SCHED_SCRIPT)
    ap.add_argument("--dir-path", default=DEF_DIR_PATH)
    ap.add_argument("--endpoint-url", default=DEF_ENDPOINT)
    ap.add_argument("--config", default=DEF_CONFIG, help="config.json for BATCH tiers")
    ap.add_argument("--journal-tag", default=DEF_JOURNAL_TAG)
    ap.add_argument("--out-dir", default=None, help="output root (default: <script>/sch_test_runs)")
    ap.add_argument("--poll-interval", type=int, default=None)
    ap.add_argument("--skip-datagen", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    return ap.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    here = Path(__file__).resolve().parent
    spec_dir = Path(args.spec_dir) if args.spec_dir else here / "spec_files"
    out_root = Path(args.out_dir) if args.out_dir else here / "sch_test_runs"
    out_root.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest(spec_dir)
    by_num = manifest_index(manifest)
    datasets = select_datasets(args, by_num)

    batch_cfg = load_batch_config(args.config)
    tier_order = tier_order_from_config(batch_cfg)

    LOG.info("selected %d dataset(s): %s", len(datasets), ", ".join(d["id"] for d in datasets))
    if os.name != "posix" and not args.dry_run:
        LOG.warning("this harness targets the Linux bryck host; on this OS use --dry-run to preview")

    results = []
    for ds in datasets:
        try:
            results.append(run_one(ds, args, spec_dir, batch_cfg, tier_order, out_root))
        except SystemExit:
            raise
        except Exception as exc:  # noqa: BLE001
            LOG.exception("dataset %s failed: %s", ds["id"], exc)

    if len(results) > 1:
        build_combined(results, out_root, args.dry_run)

    LOG.info("=" * 70)
    LOG.info("DONE. %d dataset(s) processed. Output under: %s", len(results), out_root)
    for r in results:
        cs = r["csv_summary"]
        LOG.info("  %-13s transfer %-4s  exit=%s  files=%s ok=%s fail=%s  -> %s",
                 r["dataset_id"], r["transfer_id"], r["meta"]["scheduler_exit"],
                 cs["total"], cs["success"], cs["failed"], Path(r["zip"]).name)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
