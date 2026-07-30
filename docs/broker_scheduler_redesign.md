# Broker / Scheduler Redesign — Design Document

**Status:** DRAFT — awaiting approval. Nothing implemented from this doc yet.
**Trigger:** `docs/requirements.txt` (new requirements) — reviewed against the current
implementation (`docs/upload_pipeline_implementation.md`).
**Decisions locked (with user):** broker replaces GNU parallel; rc==1 whole-batch retry runs
**inline in `aws_transfer.py`** via a boto3 ProcessPool; network profiles = `dt2_100gbe`,
`low_bandwidth`, `default_balanced`, selected by config key **`NETWORK_PROFILE`**; batch-builder-only
mode via **both** a config key (`BATCH_BUILDER_ONLY`) and a CLI flag (`--batch-only`); **per-batch
verification** added.

**Related:** `docs/upload_pipeline_implementation.md` (current impl), `docs/config_reference.md`,
`docs/batch_builder_design.md`, `tests/transfer_mp.py` (ProcessPool boto3 reference).

---

## 1. Why redesign

The requirements make `aws.py` an **active broker/scheduler** that owns dispatch, rather than a shell
that pipes the enumerator into GNU `parallel`. The current model cannot satisfy:

| Requirement | Current model | Needed |
|-------------|---------------|--------|
| aws.py knows which batches are scheduled + reports state | GNU parallel dispatches FIFO, no feedback | Long-lived broker tracks inflight/completed |
| Weighted bucket scheduling + work-stealing | none (FIFO, no tier awareness) | Weighted selection; empty tiers never block others |
| "Fetch next batch of same bucket" on completion | none | Broker picks next by weight/tier |
| rc==1 → retry whole batch via local pool | leaves batch inprogress for resume | Inline ProcessPool boto3 retry |
| Network-profile scheduling | none | Static `NETWORK_PROFILE` now, dynamic later |
| Batch-builder-only test mode | standalone script only | Integrated config/CLI option |
| Per-batch verification | merged-report + verifier | Batch-file-vs-log diff per batch |

The batch framing, `.lst` fallback handoff, report schema, and resume model from the current
implementation are **kept**; this redesign changes the **dispatch/scheduling** layer and adds three
behaviors (rc==1 retry, profiles, per-batch verify) plus the batch-only mode.

---

## 2. Target architecture

```mermaid
flowchart TD
    subgraph BROKER[aws.py :: broker/scheduler  (long-lived)]
      SCHED[Scheduler loop<br/>weighted tier selection]
      TRACK[State tracker<br/>inflight/pending/completed per tier]
    end
    ENUM[bcloud_src_enum.py<br/>walk + BatchBuilder] -->|publish tier-tagged batches| PEND[(batches/pending/&lt;tier&gt;)]
    BROKER -->|spawn| ENUM
    BROKER -->|spawn| FW[fallback_worker.py]
    PEND -->|counts per tier| TRACK
    SCHED -->|dispatch one batch| AT[aws_transfer.py<br/>subprocess per batch]
    AT -->|claim| INP[(batches/inprogress/&lt;tier&gt;)]
    AT -->|cloudcp| CP[cloudcp]
    CP -->|rc 0| ATDONE[complete]
    CP -->|rc 2 + .lst| DEFER[defer -> fallback]
    CP -->|rc 1| RETRY[inline ProcessPool boto3 retry]
    ATDONE --> DONE[(batches/completed/&lt;tier&gt;)]
    RETRY --> DONE
    DEFER -.-> FW
    FW -->|drained| DONE
    AT -->|exit code| SCHED
    BROKER -->|enum done + all drained| VER[verification.py<br/>per-batch verify + final summary]
```

**Broker responsibilities**
1. Load the selected `NETWORK_PROFILE` → tier weights, per-tier concurrency caps, global
   `max_workers`, pool sizes, chunk sizes.
2. Spawn the **enumerator** (batch builder) — it streams tier-tagged batches to
   `batches/pending/<tier>/`. Spawn the **fallback worker** (socket-free, unchanged).
3. Run the **scheduler loop**: while there is work and free worker slots, pick the next batch by the
   weighted algorithm (§4), spawn an `aws_transfer.py` subprocess for it, and track it inflight.
4. On each `aws_transfer` exit, decrement inflight for its tier and schedule the next batch.
5. When enumeration is complete AND all batches are `completed` AND the fallback has drained → write
   `_fallback_done`, wait for the fallback, then run **verification** (per-batch + final summary).

The broker learns a batch's tier from its **location** (`batches/pending/<tier>/…`) — see §5.

---

## 3. Network profiles (`NETWORK_PROFILE`)

A `NETWORK_PROFILES` config block defines named profiles; `NETWORK_PROFILE` selects one (default
`default_balanced`). Each profile tunes batch sizing (BatchBuilder tiers), scheduling weights,
per-tier concurrency, and the retry pools. **Static now**; future work adds NIC-speed auto-detect.

| Knob | Meaning |
|------|---------|
| `max_workers` | Global concurrent `aws_transfer`/cloudcp processes. |
| `tier.<T>.weight` | Relative share when all tiers have work (higher = more slots). |
| `tier.<T>.max_concurrent` | Hard cap on inflight batches of tier T. |
| `tier.<T>.batch_size` / `target_size_mb` / `open_batches` | Passed to BatchBuilder (existing keys). |
| `rc1_retry.processes` / `threads_per_process` | Inline whole-batch ProcessPool sizing (§6). |
| `fallback.*` | Existing FALLBACK knobs (per-file boto3 retry). |
| `multipart_chunksize_mb` | cloudcp / boto3 chunk size. |

**Starter profiles (indicative — tune during implementation):**

| Profile | max_workers | large/medium/small/tiny weight | notes |
|---------|-------------|-------------------------------|-------|
| `dt2_100gbe` | 32 | 4 / 4 / 2 / 1 | big batches, high concurrency, large multipart chunks |
| `low_bandwidth` | 4 | 2 / 2 / 1 / 1 | small batches, low concurrency, small chunks, more retries |
| `default_balanced` | 16 | 3 / 3 / 2 / 1 | current defaults |

Weights and caps live in config so ops can tune without code changes. When absent, the current
BatchBuilder defaults apply.

---

## 4. Weighted scheduling with work-stealing

Goal: honor weights **when all tiers have work**, but **never let a weight block** a tier when others
are empty — consume all workers with whatever is available.

**State (from disk + in-memory):**
- `pending[T]` = count in `batches/pending/<T>/` (refreshed cheaply by listdir/length).
- `inflight[T]` = batches of tier T currently dispatched (broker-owned counter).
- `weight[T]`, `max_concurrent[T]`, `max_workers` from the profile.

**Selection (deficit weighted round-robin with candidate filtering):**
```
free = max_workers - sum(inflight)
while free > 0 and any(pending[T] > 0 for T):
    candidates = { T : pending[T] > 0 and inflight[T] < max_concurrent[T] }
    if not candidates: break            # all eligible tiers at their cap
    # pick the tier with the largest weighted deficit
    T* = argmax_{T in candidates}  ( weight[T] / (inflight[T] + 1) )
    dispatch_one(T*)                     # spawn aws_transfer for a batch of T*
    inflight[T*] += 1 ; pending[T*] -= 1 ; free -= 1
```
- **Work-stealing** is automatic: `candidates` only includes tiers with pending work, so if
  large/medium are empty, small/tiny fill every free slot.
- **Weight fairness** applies only among tiers that currently have work, satisfying "if all batches
  available use weights, else consume all workers with what's available."
- **"Prefer same bucket after completion"**: when a large batch finishes and more large are pending,
  `weight[large]/(inflight[large]+1)` becomes large again, so the freed slot is likely re-filled with
  large — exactly the requested behavior, bounded by `max_concurrent[large]`.

**Dispatch protocol (broker ↔ aws_transfer):** the broker spawns `aws_transfer.py <id> <type> <batch>
<dst> <src> …` as a subprocess for one specific batch (no GNU parallel). `aws_transfer` claims the
batch, runs cloudcp, and handles the exit code. The broker learns the outcome from the subprocess
**exit code** and/or the batch-state transition; either way it decrements `inflight[T]` and loops.

**Completion → verifier:** the broker considers the transfer drained when the enumerator has exited
with `scan_state=complete`, `pending[*]==0`, `inflight[*]==0`, and the fallback has no un-retired
`.lst`. It then drops `_fallback_done`, waits for the fallback, and runs verification.

---

## 5. Tier-tagged batches (`batch_state` v2)

The scheduler needs each batch's tier. **Decision:** tier-partition the state directories:
```
batches/{pending,inprogress,completed}/<tier>/batch_NNNNNN.txt
```
- `BatchBuilder` already tags each `Batch` with its tier (`batch.bucket`); `publish_batch` writes
  into the tier subdir.
- `batch_state` API gains an optional `tier` argument: `publish(dir,name,lines,tier=…)`,
  `claim(dir,name,tier=…)`, `complete(dir,name,tier=…)`; `counts()` returns per-tier maps;
  `to_run()` yields `(tier,name,path)`.
- Batch id remains globally unique (`seq_high_water`), so names never collide across tiers/resume.
- `aws_transfer` derives the tier from the batch path (`…/inprogress/<tier>/batch_*.txt`).
- **Backward/҂resume:** `reset_inprogress_tmp` and re-dispatch iterate all tier subdirs. A legacy
  flat layout (no tier subdir) is still readable (treated as tier `unknown`, weight = medium) so an
  in-flight transfer from the old layout can resume.

Alternative considered: encode tier in the filename (`batch_<tier>_NNNNNN.txt`). Rejected — tier
subdirs give O(1) per-tier counts via directory listing and keep names stable.

---

## 6. rc==1 whole-batch retry (inline in `aws_transfer.py`)

**Trigger:** cloudcp returns **1** (whole batch failed to upload).
**Action:** instead of leaving the batch `inprogress`, `aws_transfer` retries the **entire batch**
with a local boto3 pool modeled on `tests/transfer_mp.py`:

```mermaid
sequenceDiagram
    participant AT as aws_transfer
    participant CP as cloudcp
    participant PP as ProcessPool(boto3)
    participant RPT as report/logs
    participant BS as batch_state
    AT->>CP: run(batch)
    CP-->>AT: rc=1 (whole batch failed)
    AT->>AT: read NUL batch -> file list
    AT->>PP: ProcessPoolExecutor(init boto3 per process)
    PP->>PP: per file: upload_file + HeadObject verify (retry/backoff)
    PP-->>AT: results (ok / failed)
    AT->>RPT: ok -> SUCCESS rows; failed -> error.log + failed_uploads
    AT->>BS: complete batch (failures are terminal, recorded)
```

- **Pool sizing:** `rc1_retry.processes × threads_per_process` from the active profile. Each worker
  process owns its **own** boto3 client (clients must not cross processes — per `transfer_mp.py`).
- **Verification:** HeadObject (existence + size) per file before recording SUCCESS.
- **Logs:** successes → the report shard (status `SUCCESS` or a distinct `MP_OK`, TBD); failures →
  `error.<pid>.log` (human) + `failed_uploads.<pid>` (machine) i.e. the **global failed log**.
- **Completion:** after the retry, the batch is terminal — `batch_state.complete`. Remaining failures
  are recorded in the global failed log (surfaced by verification).
- **Oversubscription throttle:** the broker already runs up to `max_workers` `aws_transfer`
  processes; an rc==1 retry spawns an additional ProcessPool. To avoid CPU/connection blowup, the
  profile keeps `rc1_retry.processes` small and (future) the broker may cap concurrent rc==1 retries.
  Documented as a known throttle.

**Reconciled exit-code semantics (authoritative):**

| rc | Meaning | Handler |
|----|---------|---------|
| 0 | all files uploaded | `aws_transfer` completes the batch |
| 2 | partial — some failed (`.lst` written) | `.lst` → fallback (per-file boto3), fallback completes batch |
| 1 | whole batch failed | `aws_transfer` inline ProcessPool retry of the whole batch, then complete |

(Prior implementation's "rc==1 with `.lst` = all-failed → defer" is superseded: rc==1 now always
triggers the whole-batch local-pool retry.)

---

## 7. Batch-builder-only test mode

**Toggle:** `BATCH_BUILDER_ONLY=True` in config **or** `--batch-only` on `bcloud_src_enum.py`
(CLI overrides config).

**Behavior:** run the enumerator + BatchBuilder normally (walk, tier classification, publish batches,
journals, resume) but **do not transfer** — the broker skips launching `aws_transfer`/cloudcp and the
fallback. Produces:
- the published tier-tagged batch files under `batches/pending/<tier>/`,
- a `batch_summary.csv` (per-tier: batch count, file count, total size) — extend the existing summary,
- exit 0 on complete / `RC_STOPPED` on signal (resumable), as today.

Purpose: validate batching/throttles/resume on a real directory tree without moving data.

---

## 8. Per-batch verification

New verifier step **before** the final summary:

1. For each `completed` batch, read its batch file → set of local paths `B`.
2. Build the set of terminally-recorded paths for that batch from the report
   (`SUCCESS/FALLBACK_OK/SKIPPED/MP_OK`) restricted to `B`.
3. `missing = B − recorded`. If non-empty, the batch is **incomplete** → logged to a
   verification-failures list; the transfer final state becomes `Incomplete`.
4. After all batches, emit the **final summary**: `local_path, s3_path, size, etag` rows (from the
   merged report) + totals (file count, total size) — same shape as today's `final_report.csv`.

**Association batch↔rows:** by local-path membership (batch file entries ∩ report `local_path`). This
needs no schema change. (Optional future optimization: stamp a `batch_name` column into report rows
for O(1) grouping.)

**Cost:** O(files) once, streamed per batch. At 300M files this is a large pass; verification already
performs a comparable scan, and per-batch checking can short-circuit on the first missing file per
batch. Acceptable; flagged for perf review.

---

## 9. Module-by-module change summary

| Module | Change | Kept |
|--------|--------|------|
| `aws.py` | Becomes the long-lived **broker/scheduler**: load profile, spawn enumerator+fallback, weighted scheduler loop dispatching `aws_transfer` subprocesses, track state, trigger verification. Remove GNU-parallel pipeline. | fallback done-marker handshake, validation |
| `bcloud_src_enum.py` | Publish into tier subdirs; honor `BATCH_BUILDER_ONLY`/`--batch-only`; pass profile-derived tier sizes to BatchBuilder. | walk/resume/journals |
| `BatchBuilder.py` | Accept profile-derived tier params; expose tier on published batches. | tier classification, streaming, flush |
| `batch_state.py` | Tier-aware paths + counts/to_run; legacy flat layout still readable. | atomic renames, idempotent complete |
| `aws_transfer.py` | rc==1 → inline ProcessPool boto3 whole-batch retry (transfer_mp model) then complete; derive tier from path. | rc==0 complete, rc==2 defer-to-fallback |
| `fallback_worker.py` | Unchanged (per-file `.lst` drain). | all |
| `upload_report.py` | Possibly add `MP_OK` status + (optional) `batch_name` column; helpers for per-batch verify. | schema, merge |
| `verification.py` | Add per-batch batch-file-vs-log reconciliation before the final summary. | final summary shape |
| config (`config_reference.md`) | Add `NETWORK_PROFILE`, `NETWORK_PROFILES`, `BATCH_BUILDER_ONLY`, tier weights/caps, `rc1_retry.*`. | existing keys honored |

---

## 10. Protocols, inputs, outputs, exceptions (delta from current)

### 10.1 Broker ↔ enumerator
- **Out:** broker spawns `bcloud_src_enum.py -i <id> <src> [--batch-only]`.
- **In:** enumerator publishes tier-tagged batches; broker reads `pending/<tier>/` counts and the
  `manifest.json` `scan_state`. (No stdout piping into parallel anymore; broker may still read
  enumerator stdout for logging.)
- **Exceptions:** enumerator exit `RC_NO_SPACE`/`RC_STOPPED` → broker pauses/stops, leaves resumable
  state; broker surfaces the state.

### 10.2 Broker ↔ aws_transfer
- **Out:** one subprocess per batch: `aws_transfer.py <id> <type> <batch_path> <dst> <src> [endpoint]`.
- **In:** subprocess exit code + batch-state transition. Broker decrements `inflight[tier]`.
- **Exceptions:** subprocess crash → broker treats the batch as not-completed; batch stays
  `inprogress` (resume/redispatch). Broker may cap retries of a crashing batch.

### 10.3 aws_transfer ↔ cloudcp — unchanged CLI; exit-code semantics per §6.

### 10.4 aws_transfer ↔ ProcessPool (rc==1) — §6.

### 10.5 Broker ↔ fallback — unchanged (`--transfer-dir`, `_fallback_done` marker).

### 10.6 Broker ↔ verifier — broker invokes verification once drained; verifier reads batches +
report, writes `final_report.csv` + verification-failures list.

---

## 11. Resume & crash-safety (unchanged principles)

- Enumeration two-case resume (in_progress / complete) preserved; tier subdirs iterated on
  re-dispatch.
- Batch-state transitions remain atomic renames; `claim` dedups completed batches.
- Broker is **stateless-recoverable**: on restart it rebuilds `inflight`/`pending` from disk
  (`inprogress`/`pending` per tier) and resumes scheduling. Any batch left `inprogress` by a crashed
  worker is re-dispatched.
- `.lst` durability + fallback re-glob preserved.

---

## 12. Phasing (proposed implementation order)

1. **Tier-tagged `batch_state` + BatchBuilder/enumerator publish** (`req-batch-tier-meta`).
2. **Network profiles** config + loader (`req-network-profiles`).
3. **Broker/scheduler in aws.py** replacing GNU parallel (`req-broker-scheduler`).
4. **rc==1 inline ProcessPool retry** in aws_transfer (`req-rc1-batch-retry`).
5. **Batch-builder-only mode** (`req-batch-only-mode`).
6. **Per-batch verification** (`req-per-batch-verify`).
7. **Error-case audit + tests** (`req-error-cases-audit`).
8. Update `config_reference.md` + `upload_pipeline_implementation.md`.

Each phase is independently testable with the existing stub-harness approach.

---

## 13. Open questions / to confirm during implementation

1. **rc==1 success status:** record retried uploads as `SUCCESS` or a distinct `MP_OK` (for
   observability)? (Leaning `MP_OK`.)
2. **Concurrent rc==1 retries cap:** should the broker limit how many batches may be in whole-batch
   ProcessPool retry at once (to bound CPU/connections)? (Leaning yes, profile-driven.)
3. **Profile numeric defaults:** the weights/caps/chunk sizes in §3 are indicative — confirm real
   values for DT2 100GbE vs low-bandwidth links.
4. **Per-batch verify at 300M scale:** accept the full O(files) pass, or add an optional
   `batch_name` report column now for O(1) grouping?
5. **Broker language/placement:** implement the scheduler inside `aws.py`'s transfer function, or a
   new dedicated module (`batch_scheduler.py`) that `aws.py` calls? (Leaning a dedicated module for
   testability.)
