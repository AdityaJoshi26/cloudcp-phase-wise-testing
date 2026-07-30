# Phase 4 — Reporting & Verification

[← Back to master plan](../complete_plan.md)

> **What this phase is:** After transfers complete, the verification engine diffs the
> **source index** (every file on disk) against the **upload report** (every file
> transferred), producing a final per-file status and per-tier summary. This phase validates
> **status correctness, de-duplication, ordering barriers, encoding-safe joins, and progress
> counters.**

**Priority:** P0 (report correctness).
**Config:** `VERIFICATION.*` in `/etc/bryck/bryckcloud/config.json`
(`REPORT_FORMAT=json`, `VERIFY_S3_WORKERS=16`, `VERIFY_STAT_THREADS=32`).

---

## 1. Final Status Model

Every source file is classified into exactly one status:

| Status | Meaning | Injected by |
|---|---|---|
| `OK` | Transferred and verified | Normal upload |
| `MISSING` | In source, never uploaded | Skip uploading N files |
| `FAILED` | cloudcp + fallback both gave up | N permanently non-retryable errors |
| `MISMATCH` | S3 size ≠ source size | Mock wrong HeadObject size |
| `EXTRA` | In S3 but not in source index | Manually PUT extra objects |

---

## 2. Test Cases (P0)

| ID | Case | How | Pass when |
|---|---|---|---|
| P4-01 | Exactly one correct status per file | Inject all 5 status types in known counts | Each status count == injected count; no file missing/duplicated; CSV quoting preserves embedded newlines |
| P4-02 | Refuse verify while scan in progress | `scan_state=in_progress` | Error "scan_state=in_progress, cannot verify"; proceeds after complete |
| P4-03 | Paused transfer does not trigger verify | `pause_requested=True` mid-transfer | Verify not triggered until resumed to natural completion barrier |
| P4-04 | Encoding-safe merge-join | Upload full variant set (DS-P4-*) | All variants `OK`; zero false MISSING/MISMATCH from encoding |
| P4-05 | De-dup last-status-wins | 50 files with `SKIPPED` + `FALLBACK_OK` rows | Each appears once as `OK`; no dup rows for SUCCESS/SKIPPED/FALLBACK_OK combos |
| P4-06 | Progress counters monotonic | Sample every 5 s during a transfer | `files_done`/`bytes_done` never decrease; `total_files` non-zero from first checkpoint |
| P4-07 | Per-tier completion summary | DS-P6-01 / mixed | Per-tier file counts sum to total; batches created vs completed; bytes; `avg_batch_duration_sec` populated |
| P4-08 | Failure summary triage rows | Inject permanent failures | One row per permanently failed file with full triage context |

---

## 3. Datasets Used

| Category | Datasets | Purpose |
|---|---|---|
| 4 — Filename & Encoding | DS-P4-05 (cross-tier variants) | Merge-join correctness |
| 6 — Network Profile | DS-P6-01 | Per-tier summary aggregation |
| 5 — File Type Coverage | DS-P5-01 | No type mis-reported |

Expected counts: [../../dataset_cloudcp/spec_files/manifest.json](../../dataset_cloudcp/spec_files/manifest.json).

---

## 4. Tools

- Verification engine (config-driven; `REPORT_FORMAT=json`).
- `dataset_validator.py` — independent file-count ground truth vs manifest.
- **Status-injection fixtures** — deliberately create MISSING/FAILED/MISMATCH/EXTRA (**TBA**).
- **Report assertion harness** — parse JSON/CSV report, assert per-status counts (**TBA**).

See [../tools_guide.md](../tools_guide.md).

---

## 5. To Be Added

- Status-injection fixtures (skip-uploads, wrong-size mock, manual EXTRA PUTs, poison files).
- JSON/CSV report assertion harness with per-status count checks and CSV-quoting validation.
- Progress-counter sampler.

Narrative reference: [../../docs/planv2.md](../../docs/planv2.md) Phase 4.
