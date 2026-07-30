# Bcloud Service Redesign — Proposed Design

> Status: **PROPOSAL / DISCUSSION**. No code changes implied. Targets a refactor of the
> batching, transfer orchestration, fallback, and verification stack.

## 0. Design Goals (from requirements)

1. **Size-bucketed, priority-ordered batching** driven by `config.json`, with bucket
   thresholds chosen *before* a scan, and ordering tuned to the network profile
   (DT2 / 100GbE → big files first; low-bandwidth → small files first).
2. **Persistent batch files** — never deleted; tracked as `pending` vs `processed`.
   Resume reprocesses only `pending`.
3. **Eliminate xattr entirely.** Resume granularity = the batch file, not per-file
   xattr scans (which caused the "stuck on restart re-walking the tree" bug).
4. **Robust filename handling**: embedded newlines, trailing spaces, CR (Ctrl+M),
   and non-UTF-8 (Latin-1) bytes must round-trip exactly.
5. **Faster scanning** to reach a consistent (fully-enumerated) state sooner;
   **stop/resume** for the BatchBuilder itself.
6. **Pipelined transfer** replacing `bcloud_src_enum | parallel | aws_transfer.py`,
   with min/max process & thread pools that **autoscale** with backlog, plus a
   **global fallback channel**.
7. **No pre/post-processing** in `aws_transfer.py`: hand the batch directly to
   cloudcp; cloudcp uploads, HeadObject-verifies, and writes a CSV transaction log.
8. **Verification with no bucket listing** — diff BatchBuilder source index against
   cloudcp transaction log; emit a report.
9. **Symmetric download path** — S3 listing with many prefixes / millions of objects,
   on-disk state, same bucketing.

---

## 1. BatchBuilder (new C++ component)

### 1.1 Responsibilities
- Walk the source tree (upload) or list the bucket (download) fast and resumably.
- Emit a **complete source index** (every entry + size) — this is the verification
  source of truth, replacing live bucket listing.
- Segregate entries into **size buckets** and pack them into **batch files**.
- Track batch lifecycle on disk: `pending → processed` (or `failed`).
- Support **stop/resume** of the scan and of transfer, without any xattr.

### 1.2 On-disk layout (per transfer)
```
{batchmeta}/transfer_<id>/
  manifest.json            # source root, bucket, bucket-prefix, bucket config,
                           # network profile, scan_state, seq_high_water, schema_version
  scan.discovered          # append-only: every dir/prefix ever enqueued (scan resume)
  scan.completed           # append-only: every dir/prefix fully listed (scan resume)
  source.index             # NUL-framed records: relpath \0 size \0 mtime \0  (verification src-of-truth)
  batches/
    pending/   <bucket>_<seq>.batch
    processed/ <bucket>_<seq>.batch
    failed/    <bucket>_<seq>.batch
  txhistory/   <bucket>_<seq>.csv      # written by cloudcp (see §6)
```

### 1.3 Batch file format — **NUL-framed, absolute paths**
Filenames may contain `\n`, trailing spaces, and CR. **Line-delimited text is unsafe.**
Each record is NUL-terminated; paths are **absolute** and stored as
**raw bytes** (no decoding, no stripping):
```
<abspath_bytes> \0 <size_decimal> \0
```
- Absolute paths make `source.index` + batches a **complete self-describing file listing**
  (absolute path + size per object), so the end-of-transfer report reads straight off them.
- Two prefixes are recorded in `manifest.json` and passed to cloudcp, which composes the key:
  `key = bucket_prefix + strip(source_prefix, abspath)`. cloudcp opens the file at the absolute
  path directly and owns all key composition (single place for prefix rules).
- Raw bytes → Latin-1 / mixed-encoding names survive untouched. Key normalization to
  valid UTF-8 (or percent-encode) happens once, at upload time, in cloudcp (§6.3).
- **Never** `strip()` — trailing spaces/CR are legitimate filename bytes.

### 1.4 Size buckets & scheduling (config-driven)
```json
"BATCH_BUILDER": {
  "size_buckets": [
    { "name": "tiny",   "max": "1MB"   },
    { "name": "small",  "max": "100MB" },
    { "name": "medium", "max": "1GB"   },
    { "name": "large",  "max": null    }
  ],
  "batch_target_bytes": "2GB",
  "batch_max_files": 2000,
  "schedule_profiles": {
    "dt2_100gbe": { "order": ["large","medium","small","tiny"],
                    "weights": { "large": 6, "medium": 4, "small": 3, "tiny": 3 } },
    "wan_lowbw":  { "order": ["tiny","small","medium","large"],
                    "weights": { "tiny": 6, "small": 4, "medium": 3, "large": 3 } }
  },
  "active_profile": "dt2_100gbe"
}
```
- Buckets are resolved **once at transfer start** (before scanning). A file lands in the
  first bucket whose `max` it is `<`.
- **Per-bucket batch sizing** so a batch is "right-sized" for its class — large-file batches
  capped by `batch_target_bytes` (few files, big bytes), tiny-file batches capped by
  `batch_max_files` (many files, small bytes). Each batch stays size-homogeneous so the
  orchestrator can predict its duration.
- **Weighted concurrent scheduling (addresses challenge #2).** Earlier we framed scheduling
  as strict sequential (large-then-tiny). That's wrong for a mixed corpus: the 165M tiny
  files (request-count bound) and the 180TB of large files (bandwidth bound) must drain
  **at the same time**. The scheduler keeps **all buckets in flight concurrently** but
  allocates worker slots by the profile's `weights`: on 100GbE most slots pull large-file
  batches (saturate the pipe) while a reserved fraction continuously drains tiny-file batches
  (so the huge small-file count finishes early instead of starving at the end). `order` only
  decides tie-breaks / which bucket gets the spare slot. Weights are reread from config and
  can be tuned per network.

> Why both: tiny files bottleneck on **requests/sec**, large files on **GB/sec** — different
> resources, so running them together uses the host fully instead of leaving one resource
> idle while the other drains.

> Data point motivating this: 165M files <1MB ≈ 40TB vs 40M files >1MB ≈ 180TB.
> On 100GbE you want the 180TB of large files moving first; on a slow WAN you'd rather
> clear the 165M-file request backlog.

### 1.5 Fast, resumable scan
- **Parallel walker**: a work queue of directories consumed by a thread pool (e.g. 16–64
  threads). Each worker `getdents64`/`readdir`s a dir, uses `d_type` to classify, and
  `statx` only files (size needed for bucketing). Per-thread batch writers (sharded by
  bucket) avoid lock contention; sequence numbers are atomic.
- **Scan resume**: append-only `scan.discovered` / `scan.completed` logs (the
  `s3_list_bucket_fast.py` model, §13.6); `frontier = discovered − completed`. On restart,
  if `scan_state != complete`, reload the frontier and continue; already-emitted batches stay.
- `scan_state = complete` is the barrier that lets verification compute "missing" safely.

### 1.6 Batch lifecycle (replaces xattr resume)
- Orchestrator processes only `batches/pending/`.
- **Intra-batch resume without xattr**: cloudcp reads its own `txhistory/<batch>.csv` on
  start and skips relpaths already `SUCCESS`, so a re-run of a partially-done batch never
  re-uploads completed files. The tx log *is* the per-file skip set.
- A batch is moved `pending → processed/` once **both cloudcp and the fallback worker** have
  run it (full state machine in §11). A `processed` batch may still contain permanently
  failed files; those are appended to a transfer-level `failed_report.csv` and handled
  separately — they are not retried inline.
- Batches with residual failures land in `failed/` for audit; their failed files are in the
  report.
- **Resume = "process everything still in pending/"**. No tree re-walk, no xattr scan.

### 1.7 Download path
- Build a C++ **parallel S3 lister** (concept from `s3_list_bucket_fast.py`): shard the
  keyspace by prefix (common delimiters or first-char ranges), run paginated
  `ListObjectsV2` per shard concurrently, **stream keys+sizes straight to `source.index`**
  (never hold millions of keys in memory).
- Resume listing by journaling each shard's continuation token.
- Then bucket-by-size and batch exactly like upload. Download batches carry S3 key →
  local relpath; cloudcp downloads and writes the same CSV tx log.

---

## 2. Orchestrator (replaces `parallel` in `aws.py`)

### 2.1 Model
A long-lived Python orchestrator per transfer (pattern from `pipelined_transfer_design.md`
+ `transfer_mp.py`), feeding cloudcp instead of GNU parallel:
```
BatchBuilder(pending/) → [scheduler] → process pool (cloudcp per batch)
                                            │ tx log per batch
                                            ▼
                                   reconcile → processed/  OR  → global fallback queue
```

### 2.2 Autoscaling pools (config-driven)
```json
"ORCHESTRATOR": {
  "min_processes": 4,
  "max_processes": 32,
  "threads_per_process": 16,
  "batch_inflight_max": 96,
  "scale_up_backlog": 50,
  "scale_down_idle_sec": 30,
  "poll_interval_sec": 5
}
```
- Custom dynamic pool (not fixed `ProcessPoolExecutor`): persistent workers pull batch
  paths from a shared queue. A controller samples backlog (count of `pending/` batches +
  in-flight) every `poll_interval_sec`:
  - backlog > `scale_up_backlog` and procs < max → spawn worker(s).
  - workers idle > `scale_down_idle_sec` and procs > min → retire worker(s).
- `batch_inflight_max` bounds memory / temp usage (backpressure).

### 2.3 Global fallback channel
- A single fallback queue shared across all workers, drained by the fallback pool (§4).
- Worker reconcile step extracts FAILED rows from each batch's tx log and enqueues them
  globally → dedup, central backoff, and isolation of "one bad batch" from the pipe.

---

## 3. aws_transfer.py — no pre/post-processing

### 3.1 Before vs after
- **Before**: `batch_preprocess` (per-file xattr/precheck → cloudcp spec) + cloudcp +
  `batch_postprocess` (parse output, set xattr, DB updates, merge files).
- **After**: thin invoker. Hand cloudcp the four inputs and get out of the way:
  ```
  cloudcp \
    --source-root /mnt/bryck/<src> \
    --bucket <bucket> \
    --bucket-prefix <prefix> \
    --batch batches/pending/<bucket>_<seq>.batch \
    --txlog txhistory/<bucket>_<seq>.csv \
    [--endpoint ...] [--profile ...] [--threads N] [--chunk-size ...]
  ```
- No xattr, no DB-per-file, no output merging. cloudcp owns upload + HeadObject + tx log.

### 3.2 Failure communication design
- **Single source of truth = the CSV tx log** (also used by verification). Each file row
  carries a `status` column. cloudcp **exit code** conveys batch-level result:
  `0 = all success`, `2 = partial`, `1 = fatal`.
- Orchestrator reads the tx log on batch completion; `status=FAILED` rows → global
  fallback queue. (Optional fast path: cloudcp also writes a `failed/<batch>.csv` subset
  so fallback need not rescan the full CSV.)
- Rationale for keeping a boto3 fallback after a C++ uploader: insulates against C++ SDK
  edge cases (odd key encodings, transient SDK faults) without failing the whole transfer.

---

## 4. Fallback (config-driven, persistent pool)

Model directly on `transfer_mp.py` — **persistent boto3 clients in a process pool**, NOT
per-file `subprocess.run(aws s3 cp ...)` (that per-file fork+TLS+client-init was the
~50 MB/s bottleneck on the 200M-file run).
```json
"FALLBACK": {
  "min_processes": 2,
  "max_processes": 8,
  "threads_per_process": 16,
  "max_attempts": 3,
  "backoff_base_sec": 1.0,
  "backoff_max_sec": 30.0,
  "multipart_threshold": "64MB",
  "multipart_chunksize": "64MB",
  "retry_on": ["SlowDown", "InternalError", "RequestTimeout", "connection reset", "broken pipe"]
}
```
- Each process: one boto3 S3 client + a thread pool (connection reuse). Consumes
  `(local_path, key, size, attempt)` from the global queue, `upload_file` + HeadObject
  verify, writes result back to the tx log (`FALLBACK_OK` / `FALLBACK_FAILED`).
- Exponential backoff; after `max_attempts` → permanent-failure log; counts surface in the
  verification report.

---

## 5. Verification (no bucket listing)

- **Inputs**: `source.index` (BatchBuilder, src-of-truth) and the concatenated cloudcp
  `txhistory/*.csv`. **No `ListObjectsV2`, no HeadObject sweep.**
- **Algorithm** (memory-safe for 200M rows): external-sort both by relpath (or load tx
  status into an on-disk KV like sqlite/rocksdb keyed by relpath), then streaming
  merge-join:
  - in source ∧ tx=SUCCESS ∧ size match → OK
  - in source ∧ missing from tx (or tx=FAILED) → **MISSING/FAILED**
  - size/etag mismatch → **MISMATCH**
  - in tx ∧ not in source → **EXTRA** (warn)
- **Report**: counts (scanned, uploaded, missing, failed, mismatched, fallback-recovered)
  + a detailed discrepancy CSV. Verification only runs after `scan_state=complete` AND all
  batches are `processed` or `failed`.

---

## 6. cloudcp (C++ uploader)

### 6.1 Inputs / outputs
- In: source root, bucket, bucket-prefix, NUL-framed batch file, tx-log path, creds
  (access-key **or** assumed-role ARN), endpoint, thread/chunk config.
- Out: per-file CSV tx row; batch exit code.

### 6.2 Tx history CSV (the contract) — and the two side logs
The success record (challenge #8) carries exactly the columns you asked for, plus the few
fields verification/fallback need:
```
# txhistory/<batch>.csv   (one row per file, the SUCCESS record + verification source-of-truth)
local_path,s3path,size,etag,status,attempt,finished_at
```
- `s3path` = full `s3://bucket/key`; `local_path` = absolute source path (raw bytes,
  CSV-quoted). `etag` is the S3 ETag returned on completion.
- `status ∈ {SUCCESS, SKIPPED, FALLBACK_OK}`. Written **only after** a post-upload
  HeadObject confirms existence + size. This is what verification diffs and what intra-batch
  resume reads to skip already-done files (§11.2).
- Append-safe (`O_APPEND`), one CSV per batch → no cross-batch contention. CSV is robust to
  newline/space/CR in `local_path` because fields are quoted; verification reads it with a
  real CSV parser (NUL-safe variant), never line-splitting.

**Two separate side logs (challenge #8):**
```
error_log         # human-readable: every transient/permanent error, with context (append)
failed_uploads    # machine-readable: only files that FAILED terminally
                  #   local_path \0 s3path \0 size \0 last_error \0   (feeds fallback + report)
```
Successes → tx CSV. Errors (including retried-then-recovered) → `error_log`. Files that could
not be uploaded → `failed_uploads` (this is the `failed/<batch>.failed` set of §11.3). Clean
separation means the report and the fallback both have a precise, small input.

### 6.3 Key handling
- cloudcp receives **absolute paths** in the batch plus `source_prefix` and `bucket_prefix`
  from `manifest.json`. It computes `relpart = strip(source_prefix, abspath)` then
  `key = bucket_prefix + normalize(relpart)`. Normalize raw bytes → valid UTF-8:
  try UTF-8, else Latin-1 decode (lossless byte→codepoint), else percent-encode. Local
  open uses the **original absolute raw bytes** (cloudcp opens by byte path), so non-UTF-8
  names still open while the key stays clean (challenge #9).
- **Preserve trailing spaces, CR, and embedded `\n` exactly** (challenges #10–12): they are
  part of the on-disk filename and the object name. cloudcp must **never** trim/strip the
  path; it receives raw bytes from the NUL-framed batch and uses them verbatim for both
  the local open (absolute) and the key (after stripping `source_prefix`).
- Use the CRT S3 client for high-concurrency small-file throughput; honor ARN/assumed-role
  creds already wired in.

### 6.4 Fast-fail / quick fallback (challenge #3)
cloudcp must **not hang** on a pathological object (multipart stalls, repeated SDK errors).
Bounded effort, then hand off:
- Per-object **wall-clock deadline** (`cloudcp.object_timeout_sec`) and a small internal
  retry cap (`cloudcp.max_object_attempts`, e.g. 2) for transient errors.
- On deadline/attempt-exhaustion (esp. repeated **multipart** failures or slow-progress
  detection — bytes/sec under a floor for N sec): **abort that object immediately**, write it
  to `failed_uploads`, and move on. Do not let one object block the batch.
- Optionally a **per-batch circuit breaker**: if the failure rate in a batch exceeds a
  threshold (e.g. SDK/endpoint is unhealthy), cloudcp stops early and dumps the remaining
  files to `failed_uploads` so the **boto3 fallback** (a different client stack) takes the
  whole remainder quickly instead of grinding. Config:
  ```json
  "CLOUDCP": { "object_timeout_sec": 120, "max_object_attempts": 2,
               "slow_floor_bytes_per_sec": 1048576, "batch_failrate_breaker": 0.5,
               "multipart_threshold": "64MB", "multipart_chunksize": "64MB" }
  ```

---

## 7. Cross-cutting: how xattr goes away
- **Within a batch**: durability/skip comes from the tx-log status rows.
- **Across restarts**: durability comes from batch location (`pending` vs `processed`).
- Net: no `os.getxattr` per file on restart → no full-tree re-walk → fixes the
  "no new batches generated, transfer stuck" restart pathology.

---

## 8. Resolved decisions (2026-06-27)
1. **Scan resume = journal the BFS frontier** (NOT full rescan). Trees are millions of
   objects, 13–14+ levels deep; a rescan is too expensive. See §10.4.
2. **BatchBuilder language**: document both C++ and Python (§10); recommendation is
   **Python first** for least re-architecting, with a clean module seam for a later C++
   drop-in. Packaging decided by that choice.
3. **Failed-file handoff**: cloudcp writes successes to the CSV tx log; failed entries are
   emitted to a per-batch `failed/<batch>.failed` set and handed to the global fallback
   worker. A batch is marked **done** once *both* cloudcp and fallback have run; any files
   still failing after fallback are written to a transfer-level failure report and handled
   separately. Full state machine in §11.
4. **Verification = external sort** (avoid DB issues); DB approach documented as the
   alternative with pros/cons in §12.
5. **Prioritization is static**, chosen at transfer start from `config.json`. No live
   re-prioritization.

Still-minor open items: tx-log granularity (lean per-batch); whether the failed-set file
is CSV or NUL-framed (lean NUL-framed to match batch format).

---

## 10. BatchBuilder: C++ vs Python — pros / cons

Decision criteria, in priority order: **(a) least re-architecting**, (b) correct handling
of non-UTF-8 / newline / trailing-space / CR filenames, (c) scan throughput on
millions-of-files, 13–14-level-deep trees, (d) maintainability / iteration speed.

### 10.1 Python BatchBuilder

**Pros**
- **Minimal re-architecting** — the current enumerator (`bcloud_src_enum.py`) and the whole
  bryckcloud service are already Python. We extend existing code rather than introduce a
  new artifact, build, and IPC boundary.
- Fast iteration: config parsing, journaling, batch packing, resume logic are all trivial
  in Python; easy to unit-test and to change bucket/scheduling policy.
- **Special chars are handled losslessly today**: `os.scandir` returns names as `str` via
  surrogateescape; `os.fsencode(entry.path)` recovers the *exact original bytes* for the
  NUL-framed batch writer. Newlines/trailing-space/CR survive because we frame on NUL and
  write bytes, never `.strip()` or line-split.
- Directory walking is **syscall-bound**, and CPython **releases the GIL around the
  `readdir`/`statx` syscalls**, so a thread pool *does* overlap I/O latency (the dominant
  cost on NFS/deep trees). For more parallelism, `multiprocessing` sidesteps the GIL
  entirely (shard subtrees across processes).
- Same language as the orchestrator/verifier → shared models, no serialization seam.

**Cons**
- Higher per-entry CPU overhead (object creation per `DirEntry`); raw single-thread walk is
  ~2–5× slower than C++. Mitigated by parallelism since the bottleneck is syscall latency,
  not CPU.
- True shared-memory multithreaded CPU work is GIL-limited → for very wide parallelism you
  use processes, which adds a bit of coordination (frontier sharding, per-process batch
  writers).
- Slightly higher memory per worker (interpreter + buffers). Irrelevant if we stream to
  disk (we do).

### 10.2 C++ BatchBuilder

**Pros**
- Fastest per-entry walk; **true threads** with a shared work-stealing dir queue, no GIL.
  Best raw scan throughput for the 200M-file case.
- **Native byte semantics** — paths are `char*`/`std::string` bytes end-to-end; non-UTF-8
  names need no surrogateescape gymnastics. Conceptually the cleanest fit for the
  special-char requirement.
- Can **share code with cloudcp** (same SDK, same key-normalization, same byte handling),
  and could run as a cloudcp subcommand.
- Lowest memory; can use `getdents64` directly and batch `statx` for fewer syscalls.

**Cons**
- **Most re-architecting** — new binary, new build/CI/packaging, versioning, and a
  **Python↔C++ IPC/handoff** for the service to drive it and read its state. Directly
  against the "without much re-architecting" goal.
- Slower to iterate on policy (bucketing, scheduling, journaling format) — recompile vs
  edit-and-run.
- More surface for low-level bugs (threading, error handling, partial writes) that Python
  gives you for free.
- Duplicate logic risk: batching/journal/manifest formats must stay in lock-step with the
  Python orchestrator/verifier that read them.

### 10.3 Recommendation
**Start in Python**, structured as a standalone module (`batch_builder.py`) with a narrow,
well-defined on-disk contract (manifest + journal + NUL batch + `source.index`). This gives
the least re-architecting, correct special-char handling now, and good-enough scan speed via
a multiprocessing walker. **Keep the on-disk formats language-agnostic** so that if scan
throughput proves insufficient at 200M-file scale, a **C++ `batchbuilder` (or `cloudcp
enumerate` subcommand) is a drop-in replacement** that emits identical files — no orchestrator
or verifier change. Packaging then follows: Python = ships in the existing venv/package;
C++ = either a sibling binary next to cloudcp or a `cloudcp` subcommand reusing its SDK.

> Throughput reality check: on local/NFS, the wall-clock is dominated by `statx` latency,
> not language. A Python **multiprocessing** walker (e.g. 16–48 procs) typically saturates
> the metadata server well before CPU. Go C++ only if measured scan time is the bottleneck.

### 10.4 BatchBuilder failure handling (both languages)
- **Unreadable dir / permission / I/O error mid-walk** → log to `scan_errors.log`, record
  the path in the manifest's `unreadable[]`, and **continue** (never abort the whole scan).
  These paths surface in the final verification report as "not scanned".
- **Crash / stop mid-scan** → resume from the `scan.discovered`/`scan.completed` logs (§13.6):
  `frontier = discovered − completed`; on restart we reload the frontier and continue;
  already-flushed batches remain. Deep trees (13–14+) are fine — the frontier is on disk, not
  the call stack, so depth doesn't matter.
- **Partial batch write on crash** → each batch is written to `<name>.tmp`, `fsync`'d, then
  atomically `rename`d to its final name. A batch file is therefore always either complete
  or absent; resume never sees a half-written batch.
- **`source.index` integrity** → also append-and-fsync with periodic checkpoints; on resume,
  truncate any trailing partial record (NUL framing makes the boundary detectable).

---

## 11. Failure handling & fallback handoff (state machine)

### 11.1 Batch lifecycle
```
pending/ ──(orchestrator claims)──▶ cloudcp runs ──▶ writes txhistory/<batch>.csv
                                                       │
                              all SUCCESS ◀────────────┤────────▶ some FAILED
                                   │                                  │
                                   │                       write failed/<batch>.failed (NUL-framed:
                                   │                       relpath \0 size \0 key \0)  + enqueue to
                                   │                       GLOBAL FALLBACK QUEUE
                                   │                                  │
                                   │                        fallback pool retries (boto3, backoff)
                                   │                        appends results to txhistory CSV
                                   │                        (FALLBACK_OK / FALLBACK_FAILED)
                                   ▼                                  ▼
                          rename pending/→processed/      both cloudcp+fallback done:
                                                          - 0 residual failures → processed/
                                                          - residual failures   → failed/  AND
                                                            append rows to transfer failed_report.csv
```

### 11.2 Key rules
- **Successes** (cloudcp or fallback) are the only thing written to the CSV tx log with a
  terminal `SUCCESS`/`FALLBACK_OK` status — that log is the verification source of truth.
- **"Done" = cloudcp has run AND fallback has drained that batch's failed-set.** A done batch
  *may still* contain permanently-failed files; those are not retried inline — they are
  appended to `failed_report.csv` and handled separately (operator review / later targeted
  re-run), exactly as requested.
- **Intra-batch resume without xattr**: on a restart that re-runs a partially-done batch,
  cloudcp first **reads its own `txhistory/<batch>.csv` and skips relpaths already SUCCESS**.
  The tx log thus doubles as the per-file skip set — this is what lets us delete xattr
  entirely while still not re-uploading completed files.
- **Idempotent claim**: a batch being worked is marked (in-memory + a `.lock`/`inflight`
  marker). On crash/restart, anything not in `processed/` is simply re-queued from `pending/`;
  the tx-log skip makes re-runs cheap and safe.
- **Fallback is global**, shared across all worker processes → a single bad batch can't stall
  the main pipe, and retries/backoff are centralized and dedup'd.

### 11.3 Failed-set handoff format
`failed/<batch>.failed`, NUL-framed to stay newline/space/CR-safe and consistent with batch
files:
```
<relpath_bytes> \0 <size> \0 <intended_key_bytes> \0 <last_error> \0
```
The orchestrator enqueues the *path* to this file (not the rows) onto the fallback queue;
the fallback worker reads it, retries, and writes outcomes back to the batch tx log.

---

## 12. Verification store: external sort vs database

Both compute the same diff of `source.index` (every scanned file+size) against the union of
`txhistory/*.csv` (per-file outcomes). The question is only **how to join 100M–200M rows
without blowing up memory**.

### 12.1 External sort + streaming merge-join (recommended)
Approach: normalize both sides to `relpath \t size \t status`, sort by `relpath` with
`LC_ALL=C sort -t$'\t' -k1` (or a k-way heap-merge of pre-sorted run files), then a single
linear merge-join emits OK / MISSING / FAILED / MISMATCH / EXTRA.

**Pros**
- **No database dependency or "DB issues"** (locks, VACUUM, single-writer insert
  throughput, file bloat) — exactly what you want to avoid.
- Bounded, predictable memory (merge holds one record per input run); scales to billions.
- Uses battle-tested `sort` (spills to disk automatically) or a simple heap-merge; trivially
  parallelizable (sort runs in parallel, merge is linear).
- Inputs are already flat files we're writing anyway; re-runnable and inspectable.
- **CR/space/newline-safe** if we frame on `\t`+`\0`/NUL and `sort -z` (NUL line sep).

**Cons**
- A sort pass over 200M rows costs temp disk (~tens of GB) and a few minutes of CPU/IO.
- Ad-hoc queries ("show all failures under prefix X") mean re-scanning/grepping, not SQL.
- Join logic is hand-written (small, but must be correct on the EXTRA/MISMATCH edges).

### 12.2 Embedded DB (sqlite, or rocksdb for scale)
Approach: bulk-load `source.index` and tx outcomes into a keyed table/store; compute the
report with SQL anti-joins / range queries.

**Pros**
- Expressive reporting: anti-joins, per-prefix rollups, mismatch queries are one SQL each.
- Easy incremental updates (fallback results UPDATE rows); natural for repeated/partial runs.
- Indexed point lookups if cloudcp ever wants to query "is this key already done?".

**Cons**
- The "DB issues" you're avoiding: 200M-row sqlite file is large; insert throughput needs
  careful batching/PRAGMAs (WAL, synchronous=OFF) or it's slow; single-writer contention;
  occasional corruption/locking pain on NFS; VACUUM/space management. rocksdb scales better
  but adds a C++ dependency and ops complexity.
- Another runtime dependency and schema/migration surface.
- Overkill if the only consumer is a one-shot diff report.

### 12.3 Recommendation
**External sort + merge-join.** It removes the DB failure modes entirely, has predictable
memory at 200M-row scale, and produces an inspectable, re-runnable report. Revisit sqlite
only if reporting needs become interactive/ad-hoc, or if a fast "already-done?" point-lookup
during transfer becomes valuable (in which case a sqlite index built *from* the tx log,
not as the primary store, is the lighter compromise).

---

## 9. Suggested phased rollout
1. **BatchBuilder (upload)** + NUL batch format + `source.index`; keep legacy transfer.
2. **cloudcp** batch/tx-log contract + key normalization.
3. **Orchestrator** (fixed pool first, then autoscaling) replacing `parallel`.
4. **Fallback** persistent pool.
5. **Verification** from index+txlog (retire bucket listing).
6. **Download** lister + symmetric batching.
Each phase is independently testable; `config.json` flags select new vs legacy paths.

---

## 13. BatchBuilder multiprocessing walker — detailed design

### 13.1 Process topology
```
                          ┌──────────────────────────────────────────────┐
                          │              Coordinator (1 proc)             │
                          │  - owns manifest.json + scan.journal           │
                          │  - global atomic batch-seq counter             │
                          │  - frontier checkpointing + completion barrier │
                          └───────────────┬───────────────────────────────┘
                                          │ multiprocessing.Manager / Queues
            ┌─────────────────────────────┼─────────────────────────────┐
            ▼                             ▼                             ▼
     Walker proc 1                 Walker proc 2     ...          Walker proc N
  (dir-work consumer +          (dir-work consumer +         (dir-work consumer +
   per-bucket batch writers)     per-bucket batch writers)    per-bucket batch writers)
```
- **`dir_queue`** (`multiprocessing.Queue`, bounded): unvisited directory paths (bytes).
- **`result_queue`** (bounded): small control messages from walkers → coordinator
  (`dirs_discovered`, `batch_flushed`, `dir_done`, `error`). Keeps coordinator's frontier
  view + counters authoritative without it touching the filesystem.
- Worker count `N = BATCH_BUILDER.scan_processes` (default `min(cpu, 32)`).

### 13.2 Why processes (not threads)
- The hot loop is `scandir`+`statx` (GIL released) **plus** per-entry Python work
  (classify, size-bucket, encode bytes, pack) which is **GIL-bound**. Processes remove that
  ceiling. On NFS the metadata-server round-trip dominates, so we want many in-flight
  syscalls *and* parallel CPU for packing.
- Trade-off: processes can't share Python objects cheaply → all coordination is via small
  messages and the on-disk frontier, never shared path sets.

### 13.3 Walker inner loop (per process)
```
loop:
  dir = dir_queue.get(timeout)          # bytes path; sentinel → drain & exit
  try:
    with os.scandir(os.fsdecode(dir)) as it:
      for e in it:
        name_bytes = os.fsencode(e.path)         # EXACT on-disk bytes (Latin-1/CR/space safe)
        if e.is_dir(follow_symlinks=False):
          local_subdirs.append(name_bytes)
        elif e.is_file(follow_symlinks=False):
          st = e.stat(follow_symlinks=False)      # scandir caches; size for bucketing
          relpath = name_bytes[len(root)+1:]      # store RELATIVE bytes
          bucket = pick_bucket(st.st_size)
          writer[bucket].add(relpath, st.st_size) # per-(proc,bucket) NUL-framed writer
          index_writer.add(relpath, st.st_size, st.st_mtime)  # → source.index shard
        # symlinks/specials: policy flag (skip by default, log)
  except OSError as err:
    result_queue.put(("error", dir, str(err)))    # permission/IO → log, continue
    continue
  # push children: enqueue to dir_queue if room, else keep locally (overflow spill, §13.6)
  for d in local_subdirs: enqueue_or_spill(d)
  result_queue.put(("dir_done", dir, discovered=len(local_subdirs)))
```
Key points:
- **Bytes throughout.** `os.fsencode` recovers the original filesystem bytes from the
  surrogateescape `str`, so non-UTF-8 names, embedded `\n`, trailing spaces and CR are
  preserved verbatim. We **never** `.strip()` or split on `\n`.
- `e.stat()` uses scandir's cached stat where the OS provides it (Linux often needs one
  `statx`, unavoidable since we need size).
- Each `(process, bucket)` pair owns its own batch writer → **zero lock contention** on the
  hot path. Batch sequence numbers come from a single atomic counter in the coordinator
  (requested in bulk, e.g. 64 at a time, to avoid per-batch round-trips).

### 13.4 Per-(proc,bucket) batch writer
- Buffers `(relpath, size)` records until `batch_target_bytes` or `batch_max_files`.
- Flush: write to `batches/pending/<bucket>_<seq>.batch.tmp`, `fsync`, atomic `rename`
  to final, then `result_queue.put(("batch_flushed", bucket, seq, nfiles, nbytes))`.
- Record framing (NUL): `relpath_bytes \0 size_ascii \0`.
- On graceful stop, partially-filled buffers are flushed as short batches (still valid).

### 13.5 `source.index` sharding & merge
- Each walker writes its own `source.index.part-<proc>` (NUL-framed:
  `relpath \0 size \0 mtime \0`) to avoid a shared-file bottleneck.
- At `scan_complete`, the coordinator concatenates parts into `source.index`
  (and, for verification, kicks off the external sort in §12 — can start as soon as parts
  are final). Concatenation is cheap; sorting is the real cost and is done once.

### 13.6 Frontier journaling & resume (the core of stop/resume)
Goal: resume a multi-hour scan of millions of dirs, 13–14+ deep, without rescanning.

**Model adopted from `s3_list_bucket_fast.py`** (proven at 300M+ objects): two append-only
logs owned by the coordinator, instead of periodic frontier snapshots — this gives a precise
crash boundary with no large array to serialize.
```
scan.discovered   # append-only: every dir ever enqueued (announced)
scan.completed    # append-only: every dir whose dir_done has been durably recorded
frontier (resume) = discovered − completed
```
Per-dir ordering the coordinator enforces (the crash-consistency invariant, mirroring the
reference's writer):
1. persist the dir's emitted batches/index-parts (already atomic-renamed by the walker),
2. append the dir's **children** → `scan.discovered`, fsync,
3. append the **dir itself** → `scan.completed`, fsync,
4. *only then* treat those children as eligible work.
A child is therefore never "started-and-lost": if we crash, every not-yet-finished dir is
still in `discovered − completed`, and no descendant of an unfinished dir was recorded.

- **Resume**: `frontier = load(discovered) − load(completed)`; re-enqueue into `dir_queue`;
  restore `seq_high_water` from the manifest (bumped in bulk, never reused). Already
  atomic-renamed batches stay; `.tmp` leftovers are deleted.
- **Depth-independent**: frontier is on-disk, not the call stack → 14+ levels are free.
  Memory ~ O(open dirs), not O(total dirs).
- **Single-writer caveat vs throughput**: unlike the network-bound S3 lister, the local walk
  emits dirs very fast, so routing *every* `dir_done` through one coordinator append could
  bottleneck. Mitigation: walkers **batch** `dir_done`/children messages (e.g. flush every
  K dirs or T ms) and the coordinator appends them in groups with a single fsync — amortizing
  the log cost while preserving the ordering invariant per group. Batch *file* writes stay
  sharded per-(proc,bucket); only the small discovered/completed bookkeeping is centralized.
- **Idempotency on resume**: a dir in-flight at crash is simply re-walked; benign duplicate
  batch content is absorbed by the transfer-side tx-log skip (§11.2) and relpath dedup in
  verification.

### 13.7 Overflow / backpressure
- `dir_queue` is bounded. If full, a walker keeps newly-found subdirs in a local deque and
  drains them itself (work-stealing degrades gracefully to local DFS) — prevents deadlock
  and unbounded queue memory on fan-out-heavy trees.

### 13.8 Completion barrier
- Scan is complete when: `dir_queue` empty AND every enqueued dir has a matching `dir_done`
  AND all walker buffers flushed. Coordinator then writes `scan_state=complete`, finalizes
  `source.index`, and signals the orchestrator that the full source listing exists.

### 13.9 Stop semantics
- SIGTERM/stop → coordinator broadcasts sentinel, walkers flush buffers + final part files,
  coordinator writes a last checkpoint, exits. Restart resumes from that checkpoint.

### 13.10 Failure summary (walker)
| Failure | Handling |
|---|---|
| Unreadable dir (EACCES/EIO) | log to `scan_errors.log`, record in `manifest.unreadable[]`, continue |
| Walker process dies | coordinator detects via missing heartbeat / closed pipe; re-enqueues that walker's in-flight dir(s) from the last checkpoint |
| Coordinator dies | restart from `scan.journal` checkpoint |
| Disk full on batch flush | retry with backoff; if persistent, stop scan with clear error (transfer can't proceed) |
| Partial `.tmp` batch on crash | deleted on resume (only atomically-renamed finals count) |

---

## 14. Orchestrator ↔ Fallback queue contract — detailed design

### 14.1 Actors & channels
```
   BatchBuilder            Orchestrator (coordinator proc)             Fallback pool
   pending/*.batch ──▶  batch_queue ──▶ cloudcp worker procs        fallback worker procs
                                   │         │ writes txhistory/<b>.csv      │
                                   │         │ writes failed/<b>.failed ─────┤
                                   │         ▼                               ▼
                                   └─── reconcile ◀── fallback_done ◀── appends txhistory
                                            │
                                  move pending→processed/ or failed/
```
Two durable, on-disk-backed queues (survive restart) plus in-memory dispatch:
- **`batch_queue`** — references to `pending/` batch files awaiting cloudcp. Source of truth
  on disk = the contents of `batches/pending/`; the in-memory queue is just a work list
  rebuilt from disk on startup.
- **`fallback_queue`** — references to `failed/<batch>.failed` files awaiting retry. Source
  of truth on disk = the contents of `batches/failed_pending/`; rebuilt on startup.

> Principle: **the filesystem is the durable queue; in-memory queues are derived.** A
> restart rebuilds both queues by scanning `pending/` and `failed_pending/`. No message
> broker, no lost work.

### 14.2 Message schemas (in-memory dispatch)
cloudcp dispatch (orchestrator → cloudcp worker):
```json
{ "transfer_id": 2, "batch": "batches/pending/large_000123.batch",
  "txlog": "txhistory/large_000123.csv", "source_root": "/mnt/bryck/<src>",
  "bucket": "pt-prd-...-idrive", "bucket_prefix": "pcontent/",
  "attempt": 1, "endpoint": "...", "profile": "default" }
```
fallback dispatch (orchestrator → fallback worker):
```json
{ "transfer_id": 2, "failed_set": "batches/failed_pending/large_000123.failed",
  "txlog": "txhistory/large_000123.csv", "source_root": "/mnt/bryck/<src>",
  "bucket": "...", "bucket_prefix": "pcontent/", "attempt": 1 }
```
completion event (worker → orchestrator reconcile), via `result_queue`:
```json
{ "kind": "cloudcp_done"|"fallback_done", "batch": "large_000123",
  "rc": 0|1|2, "ok": 41987, "failed": 13, "duration_ms": 1820 }
```

### 14.3 Batch state machine (authoritative, on disk by directory)
```
pending/ ──claim──▶ inflight/ ──cloudcp_done──┬─ failed==0 ─▶ processed/        (DONE)
                                              └─ failed>0  ─▶ write failed/<b>.failed,
                                                              move batch→fallback_wait/,
                                                              enqueue fallback_queue
fallback_wait/ ──fallback_done──┬─ residual==0 ─▶ processed/                    (DONE)
                                └─ residual>0  ─▶ failed/  + append failed_report.csv (DONE,
                                                  reported & handled separately)
```
- State is encoded by **which directory the batch file lives in** → crash-safe and visible
  with `ls`. Transitions are atomic `rename()`s on one filesystem.
- `inflight/` and `fallback_wait/` make restart unambiguous: on startup, **re-enqueue
  everything in `inflight/` back to `batch_queue`** and everything in `fallback_wait/` (that
  has a `.failed` set) back to `fallback_queue`. Idempotent thanks to tx-log skip.

### 14.4 Reconcile step (single coordinator thread)
On each `cloudcp_done`:
1. Parse `txhistory/<batch>.csv`; partition into SUCCESS vs FAILED relpaths.
2. If no FAILED → `rename inflight/<b> → processed/<b>`. Update counters. Done.
3. If FAILED → write `failed/<b>.failed` (NUL-framed: `relpath\0size\0key\0last_error\0`),
   `rename inflight/<b> → fallback_wait/<b>`, `enqueue fallback_queue(<b>)`.

On each `fallback_done`:
1. Re-parse the batch's tx log; recompute residual FAILED (post-fallback).
2. residual==0 → `rename fallback_wait/<b> → processed/<b>`.
3. residual>0 and `attempt < FALLBACK.max_attempts` → re-write `.failed` with residual,
   re-enqueue `fallback_queue` with `attempt+1` (after backoff). 
4. residual>0 and attempts exhausted → `rename fallback_wait/<b> → failed/<b>`, append the
   residual rows to transfer-level `failed_report.csv`. Batch is **DONE** (failures reported
   and handled separately, per requirement).

### 14.5 Why fallback is global (not per-batch)
- A single shared `fallback_queue` drained by a dedicated pool means one pathological batch
  (e.g. many SDK-encoding failures) can't stall cloudcp throughput, and retries/backoff are
  centralized & de-duplicated. cloudcp workers never block on retries — they free their slot
  the moment the batch's tx log is written.

### 14.6 Autoscaling interaction
- Controller samples `len(batch_queue)+len(inflight)` every `poll_interval_sec`:
  scale cloudcp procs between `min_processes..max_processes`.
- Fallback pool scales independently on `len(fallback_queue)` between its own
  `min_processes..max_processes`. The two pools share the host but have separate budgets so
  fallback pressure doesn't starve primary uploads (and vice-versa).
- Backpressure: `batch_inflight_max` caps `inflight/`; the dispatcher stops claiming from
  `pending/` when the cap is hit, so disk/temp stays bounded.

### 14.7 De-dup & exactly-handled guarantees
- A file is **handled-once-successfully** because: cloudcp skips tx-log SUCCESS rows;
  fallback only ever sees relpaths that were FAILED at handoff; reconcile recomputes residual
  from the tx log (so a fallback success flips the file out of the failed set). Duplicate
  *uploads* are at worst benign re-PUTs (idempotent), never double-counted in the report
  (verification dedups by relpath, last-status-wins).
- **No xattr anywhere** in this path — durability is the directory-state of the batch + the
  append-only tx log.

### 14.8 Failure summary (orchestrator/fallback)
| Failure | Handling |
|---|---|
| cloudcp proc crashes mid-batch | batch stays in `inflight/`; on restart re-enqueued; tx-log skip avoids re-upload of done files |
| Orchestrator crashes | rebuild `batch_queue` from `pending/`+`inflight/`, `fallback_queue` from `fallback_wait/`; resume |
| Fallback worker crashes | `.failed` set still in `fallback_wait/`; re-enqueued on restart |
| Corrupt/short tx-log line | reconcile treats unparseable row as FAILED (safe: routes to fallback) |
| `failed_report.csv` write fails | retry+backoff; block marking DONE until persisted (don't lose failure record) |
| Repeated fallback failure (poison file) | after max_attempts → `failed/` + report; never infinite-loops |

---

## 15. Download BatchBuilder — based on `s3_list_bucket_fast.py` (reviewed)

The local-FS walker (§13) uses **multiprocessing** because the bottleneck is
`statx` + per-entry CPU packing. The **download lister is the opposite**: pure
network-bound `ListObjectsV2`, no local stat, no CPU packing. So we reuse the proven
**threaded, single-writer, dynamic-prefix-tree** model from `s3_list_bucket_fast.py`
almost verbatim, extended to also pack size-bucketed batches.

### 15.1 What we keep verbatim from the reference
- **Dynamic prefix tree**: each task lists one prefix level with `Delimiter="/"`,
  streams that level's `Contents`, and reports its `CommonPrefixes` as children. Hot
  subtrees fan out automatically → natural load balancing across workers.
- **Single writer thread** is the sole owner of all durable state, applying this exact
  per-prefix order (the crash-consistency invariant):
  1. flush `source.index` (P's objects durable)
  2. append P's children → `.discovered`, flush
  3. append P → `.completed`, flush
  4. *only then* dispatch P's children as new work
  A child is started only after its parent is `completed`, so a crash leaves the fringe
  exactly `discovered − completed`. Resume re-lists only that fringe.
- **Memory bound**: objects stream straight to disk as each page arrives; only the
  fringe (open prefixes) and the optional skip-set live in memory.
- **Resilience**: per-prefix retry loop with exponential backoff + jitter on top of
  botocore adaptive retries, to ride out LIST connection storms.
- **`--import-csv` leaf-skip bootstrap**: trust leaf dirs from a prior CSV, re-list only
  interior skeleton + unreached subtrees. (Keep as an optional fast-start mode.)

### 15.2 What we add for batching
- The writer thread, in addition to writing `source.index` rows
  (`s3key \0 size \0 etag \0`), feeds each object into a **per-size-bucket batch writer**
  (bucket chosen from `o["Size"]`, §1.4). Same NUL-framed batch format, same
  `pending/` lifecycle as upload. So the download path produces identical batch artifacts
  the orchestrator already knows how to drive.
- Batch records for download carry the S3 key (relative to the base prefix) → cloudcp
  composes `local_path = dest_root + relkey`; downloads; HeadObject is replaced by a local
  `stat`/size (and optional checksum) check; writes the same CSV tx log.
- The reference's CSV (`s3path,size,etag`) **is** our `source.index` for the download
  side — it becomes the verification source-of-truth (compared against the tx log of
  local writes), so no second listing pass is needed.

### 15.3 Config
```json
"DOWNLOAD_LISTER": {
  "workers": 32,
  "prefix_retries": 6,
  "connect_timeout": 15,
  "read_timeout": 60,
  "max_pool_connections": 36
}
```
`workers` = listing threads; reuse the reference's `Config(max_pool_connections=workers+4,
retries={"max_attempts":10,"mode":"adaptive"})`. ARN/assumed-role creds plug into the
`boto3.client` exactly as elsewhere.

### 15.4 Difference table — why two different engines
| Aspect | Upload walker (§13) | Download lister (§15) |
|---|---|---|
| Bottleneck | `statx` + per-entry CPU packing | network `ListObjectsV2` latency |
| Parallelism | **multiprocessing** (GIL on packing) | **threads** (GIL released on network I/O) |
| Writers | sharded per-(proc,bucket) | **single writer thread** (crash-consistency) |
| Resume state | discovered/completed logs (§13.6, harmonized) | `.discovered` / `.completed` logs (reference) |
| Unit of fan-out | directory | S3 common-prefix |
| Special chars | `os.fsencode` raw bytes | S3 keys are already bytes/UTF-8; store raw |

### 15.5 Note on the reference's known limitations (carried forward)
- `--import-csv` may treat a mid-listing dir as a complete leaf → a few objects can be
  missed; an authoritative run needs a full `--restart`. We keep this only as a fast-start
  optimization, never as the basis for the final verification report.
- Resume can re-emit rows for prefixes that were mid-flight at crash → duplicate
  `source.index` rows. Harmless: verification dedups by key (last/`INSERT OR REPLACE`
  semantics), or `sort -u` during the external-sort pass (§12) removes them for free.

---

## 16. Preflight checks & disk-space guards (challenges #1, #17, #18)

### 16.1 Preflight (run before the transfer leaves PENDING; abort early if unmet)
A transfer must not start if it cannot resume or cannot make durable progress.
- **Resumability is no longer tied to source write-permission (challenge #1).** Because
  resume state lives in the batchmeta dir + tx logs (not source xattr), the source tree can
  be read-only. The preflight therefore checks what we *actually* need:
  - `batchmeta_root` exists and is **writable** (batches, logs, source.index, reports).
  - tx-log / report / failed-log paths are creatable & writable.
  - source root is **readable** and listable; creds resolve (ARN assume-role succeeds).
- **Min free space (challenge #18): require ≥ `preflight_min_free_pct` (default 10%)** on the
  filesystem holding `batchmeta_root` before starting. Estimate metadata footprint from a
  quick `du`/inode sample or a configured `bytes_per_million_files` and fail fast with a clear
  message if the projected batch/log/index size won't fit.
- If any check fails → transfer stays in a new **`BLOCKED`** state with a specific reason
  (not silently flipping to verifying/failed), surfaced in the UI/log.

### 16.2 Runtime disk-space monitor (challenge #17)
- A monitor thread samples free space on `batchmeta_root` every `space_poll_sec`.
- Below `pause_below_free_pct` (e.g. 5%) → orchestrator **pauses cleanly** (→ `PAUSED`,
  §17), stops claiming new batches, lets in-flight drain, and logs `ENOSPC: paused, free
  space below threshold`. It auto-resumes when space recovers above a hysteresis band, or
  waits for operator action.
- Any `ENOSPC` on a batch/log/index write is caught and converted to the same clean pause
  (never a crash, never a half-written record — writes are `.tmp`+rename / `O_APPEND` with
  pre-checked space).

```json
"SPACE": { "preflight_min_free_pct": 10, "pause_below_free_pct": 5,
           "resume_above_free_pct": 8, "space_poll_sec": 15,
           "bytes_per_million_files": "350MB" }
```

---

## 17. Transfer state machine & progress (challenges #6, #7, #19)

### 17.1 Explicit states — pause never means verify (challenge #6)
```
PENDING → PREFLIGHT → SCANNING → UPLOADING ⇄ PAUSED
                                    │            │
                                    │         (resume)
                                    ▼
                              FINALIZING → VERIFYING → DONE
   any → BLOCKED (preflight/space/creds)      any → FAILED (fatal)
```
- **`PAUSED` is a first-class state, distinct from `VERIFYING`.** The previous bug —
  pausing dropped the transfer into "verifying" and needed manual fixing — came from
  conflating "no more in-flight work" with "ready to verify". Fix: **verification is gated on
  `scan_state=complete` AND all batches terminal (processed/failed) AND `pause_requested=false`.**
  A pause sets `pause_requested=true` and transitions `UPLOADING → PAUSED`; it can **never**
  satisfy the verify gate, so the auto-advance to `VERIFYING` cannot fire on pause.
- Pause is cooperative: stop claiming from `pending/`, let in-flight cloudcp/fallback finish,
  persist state. Resume re-enters `UPLOADING` and re-reads `pending/`.

### 17.2 Progress shows done **and** total (challenges #7, #19)
The old UI showed a single number because total wasn't known until the end (and the
`TotalFiles`/`CopiedFiles` split was incomplete). New design knows the total early:
- After `SCANNING` (or incrementally as `source.index` grows), we have **`total_files` and
  `total_bytes`** — the denominators.
- The orchestrator maintains **`done_files` / `done_bytes`** by counting terminal tx-log rows
  (batched updates, not per-file), plus `failed_files`.
- Progress surface (DB + log line):
  ```
  files: 12,433,901 / 200,114,562   bytes: 41.2 TB / 220.8 TB   failed: 318
  ```
- During `SCANNING`, totals are shown as "≥ X (scanning…)" so the UI always renders two
  values, never one. Counters are derived from durable sources (source.index + tx logs), so
  they survive restart without double counting (relpath-keyed, last-status-wins).

---

## 18. Batch-level tracking via discovered/completed logs (challenges #14, #15, #16)

The user asked to apply the **S3-listing resumability model to batch management** too, so the
same append-only-log pattern that tracks scan prefixes also tracks batches — giving O(1)
resume reconciliation without per-file xattr and without even scanning the batch dirs.

### 18.1 Logs (coordinator-owned, append-only, fsync'd)
```
batches.created     # every batch file the BatchBuilder emitted (name \0 bucket \0 nfiles \0 nbytes \0)
batches.processed   # every batch that reached a terminal state (name \0 outcome \0)
```
- **Resume reconciliation** = `pending_to_run = created − processed`. The orchestrator
  rebuilds its work queue from this set difference in one pass — no tree walk, no xattr scan,
  no directory enumeration of millions of files (challenges #14, #15).
- The **directory-state** of §14.3 (`pending/ inflight/ fallback_wait/ processed/ failed/`)
  remains the human-visible truth and the atomic transition mechanism; the two logs are the
  fast index over it. They're kept consistent by writing `batches.processed` in the same step
  as the terminal `rename` (log-then-rename, idempotent on replay).
- **Fallback input is the CSV/failed-set, never xattr (challenge #16):** the fallback worker
  consumes `failed_uploads` / `failed/<batch>.failed` (§6.2, §11.3) and writes results back to
  the tx CSV. At 200M-file scale this is a small, bounded input — no per-file xattr probing.

### 18.2 Why this is faster to resume (challenge #15)
- Resume cost is **O(batches)** (tens of thousands), not **O(files)** (hundreds of millions).
- No source FS access at all to decide what's left — purely batchmeta logs.
- Combined with intra-batch tx-log skip (§11.2), a resumed partial batch re-runs only its
  unfinished files.

---

## 19. Challenge → design traceability matrix

| # | Challenge | Addressed by |
|---|-----------|--------------|
| 1 | No write perm → xattr set failed → resume broke; preflight before start | §7 (xattr removed), §11.2 (tx-log resume), §16.1 (preflight: source read-only OK, check batchmeta writable + creds) |
| 2 | Batching must balance big + small (small finish faster, big concentrated) | §1.4 (size buckets + **weighted concurrent** scheduling) |
| 3 | cloudcp multipart errors/hangs; should fail fast & fall back quickly | §6.4 (per-object deadline, slow-floor, multipart fast-fail, per-batch circuit breaker) |
| 4 | Fallback broken (`aws s3 cp`); use boto3 + dynamic pool by load | §4 (persistent boto3 pool), §2.2/§14.6 (autoscaling by `fallback_queue` depth) |
| 5 | Fallback didn't set xattr → resume problem | §7, §11.2 (no xattr; fallback writes tx CSV) |
| 6 | Pause → wrongly went to verifying; manual fix each time | §17.1 (**PAUSED** first-class; verify gate excludes pause) |
| 7 | Showed only uploaded, not total | §17.2 (total_files/total_bytes from source.index shown alongside done) |
| 8 | 300M-object bucket listing too slow; build report during upload (CSV: local, s3path, size, etag); separate error & failed logs | §5 (no listing), §6.2 (tx CSV `local_path,s3path,size,etag,...` + `error_log` + `failed_uploads`), §15 (list-during-... for download) |
| 9 | Non-UTF-8 / Latin filenames failed | §1.3 (raw bytes), §13.3 (`os.fsencode`), §6.3 (key normalize UTF-8→Latin-1→percent) |
| 10 | Trailing spaces stripped → wrong path; same for Ctrl+M | §1.3 / §6.3 (**never strip**; NUL framing preserves bytes) |
| 11 | Embedded `\n` in filenames failed | §1.3 (**NUL-framed** batch, not line-delimited), §6.2 (quoted CSV / NUL-safe parse) |
| 12 | Trailing carriage return failed | §1.3 / §6.3 (preserved verbatim) |
| 13 | boto3 fallback must HeadObject-verify | §4 (HeadObject size check after upload), §13 in fallback rules |
| 14 | Avoid xattr; use upload log for resume | §7, §11.2, §18.1 |
| 15 | Faster batch-level resume (completed/in-progress/pending) | §14.3 (directory-state), §18 (`batches.created`−`processed`, O(batches) resume) |
| 16 | Fallback based on CSV, not xattr | §6.2 (`failed_uploads`), §11.3, §18.1 |
| 17 | No space → pause with clear "no space" error | §16.2 (runtime monitor, clean pause on ENOSPC) |
| 18 | Require ≥10% free before start | §16.1 (`preflight_min_free_pct`) |
| 19 | Progress: files done + total | §17.2 |
| 20 | Remove `parallel`; stream for speed | §2 (orchestrator replaces parallel), §3 (no pre/post), §13/§15 (streaming) |
| 21 | Dynamic batch launching from config.json | §2.2 (autoscaling pools), §1.4 (weights), all `config.json` keys hot-read |

**Directory-scan resumability via the S3-listing model (explicit request):** §13.6 (scan uses
`discovered/completed` logs) + §18 (batches use `created/processed` logs) + §1.6/§14.3
(processed vs pending batch dirs) — the same proven pattern end to end.
