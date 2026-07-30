# Phase 5 — Fallback & Retry

[← Back to master plan](../complete_plan.md)

> **What this phase is:** cloudcp is fast but hits S3 errors. Two retry paths catch failures:
> (1) **inline** whole-batch retry in `aws_transfer.py` via a boto3 ProcessPool on rc==1, and
> (2) a persistent **fallback worker** that drains `.lst` files from rc==2 partial failures,
> retries each file, and HeadObject-verifies before marking done. This phase validates that
> **no file is ever silently lost**.

**Priority:** P0 (no data loss).
**Config:** `FALLBACK_*`, `TM_THREAD_POOL_SIZE`, `rc1_retry.*` in the broker config.

---

## 1. Retry Paths

| Trigger | Path | Owner |
|---|---|---|
| cloudcp rc==2 (partial) + `.lst` | Fallback worker drains `.lst`, per-file boto3 retry | `fallback_worker.py` |
| cloudcp rc==1 (whole batch) | Inline ProcessPool boto3 retry, immediately | `aws_transfer.py` |

Every retried file is confirmed with a **HeadObject** size check before `FALLBACK_OK`.

---

## 2. Test Cases (P0)

| ID | Case | How | Pass when |
|---|---|---|---|
| P5-01 | Partial failure `.lst` drained | Inject 1% S3 error rate → `.lst` with ~5k files | `.lst` ingested within 5 s; every file retried; batch → `completed/` |
| P5-02 | Total failure inline retry | Block S3 for one batch, unblock after 10 s | Batch → `completed/` via inline retry; other parallel batches unaffected |
| P5-03 | HeadObject confirm before FALLBACK_OK | Mock wrong size for 10 files | Those 10 not `FALLBACK_OK`; final report `FAILED` |
| P5-04 | Transient vs permanent error policy | Inject SlowDown / InternalError / RequestTimeout / AccessDenied | Transient retried with exponential backoff to `max_attempts`; AccessDenied → 1 attempt, immediate failure |
| P5-05 | Poison file does not block batch | 5 files persistently failing beyond `max_attempts` | 5 in `failed_uploads.<pid>` with `attempt_count=max_attempts`; rest `FALLBACK_OK`; batch completes |
| P5-06 | Fallback crash-restart idempotent | Kill fallback mid-drain, restart | All `.lst` entries end `FALLBACK_OK` or `failed_uploads`; `.lst.done` not re-processed; no double-processing |
| P5-07 | Verify waits for fallback done | Instrument `_fallback_done` + verify start | Verify start timestamp > `_fallback_done` write; fallback doesn't exit before all `.lst` drained |

---

## 3. Configuration (P0)

| Setting | Value | Validate |
|---|---|---|
| `FALLBACK_ENABLED` | `False` | No fallback spawned; rc==2 failures appear as `FAILED` in final report only |
| `TM_THREAD_POOL_SIZE` | `4` | Fallback uses ≤4 threads during drain |
| `TM_THREAD_POOL_SIZE` | `64` | Fallback uses up to 64 threads; drain throughput scales |
| `rc1_retry.processes` | `4` | Inline retry spawns exactly 4 processes |
| `rc1_retry.threads_per_process` | `8` | Each process uses 8 threads + its own boto3 client |

---

## 4. Datasets & Fault Injection

- Any dataset with enough files to observe drain (e.g. DS-P1-03 small, DS-P6-01 mixed).
- Failures are **injected**, not data-driven: an S3 proxy applies error rates / blocks, and
  HeadObject mocks return wrong sizes.

---

## 5. Tools

- Fallback worker + broker inline-retry path (config-driven).
- **Fault-injection proxy** — inject S3 error rates, full blocks, wrong HeadObject sizes (**TBA**).
- **`.lst` drain harness** — assert ingest latency + terminal states (**TBA**).

See [../tools_guide.md](../tools_guide.md).

---

## 6. To Be Added

- S3 fault-injection proxy (rate/blocklist/error-code injection, HeadObject size mock).
- `.lst` lifecycle harness (ingest timing, `.lst.done` idempotency, `_fallback_done` barrier).
- Crash-injection for the fallback worker.

Narrative reference: [../../docs/planv2.md](../../docs/planv2.md) Phase 3.
