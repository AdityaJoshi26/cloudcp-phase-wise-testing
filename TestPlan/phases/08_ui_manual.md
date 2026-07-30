# Phase 8 — UI Testing (Manual + Automation Outline)

[← Back to master plan](../complete_plan.md)

> **What this phase is:** The operator-facing UI for creating and monitoring CloudCP
> transfers. This phase is primarily **manual** (P0) with an **automation outline** for later.
> It focuses on: can an operator start a transfer, watch accurate progress, pause/resume, and
> read the final report — for representative datasets.

**Priority:** P0 (manual functional verification).
**Approach:** manual test plan now; automation (Playwright/Selenium) outline for later.

---

## 1. Which Data To Initiate With

| Scenario | Dataset | Why |
|---|---|---|
| Smoke / happy path | DS-P8-02 (single 0-byte file) | Fastest end-to-end; verifies UI plumbing |
| Small functional | DS-P2-02 (2001 tiny) / DS-P1-03 (small) | Multiple batches, visible progress |
| All tiers visible | DS-P6-01 (balanced all-tier) | Per-tier progress + summary render |
| Encoding display | DS-P4-05 (20 variants) | Special-character names render/report correctly |
| Failure display | any + injected 1% errors | Retry/failed states surface in UI |
| Scale (optional, P2) | DS-P7-01 | Progress under sustained load |

---

## 2. Manual Test Plan (P0)

| ID | Step | Expected |
|---|---|---|
| P8-01 | Create a transfer (source, bucket, prefix, profile) | Transfer appears; accepted/queued state shown |
| P8-02 | Start and watch progress | `files_done/total` + `bytes_done/total` update; counters never go backwards |
| P8-03 | Per-tier breakdown | Each active tier shows batches created/completed, files, bytes |
| P8-04 | Pause | UI reflects paused; progress halts; no verification/report yet |
| P8-05 | Resume | Progress continues; no duplicated work |
| P8-06 | Special-character filenames | Names render without corruption; report shows them `OK` |
| P8-07 | Injected failures | Retrying/failed counts surface; poison files show `FAILED` |
| P8-08 | Completion | Final state shown; per-file report + per-tier summary accessible/downloadable |
| P8-09 | Report contents | Status counts match the manifest for the dataset |
| P8-10 | Cancel / cleanup | Transfer stops cleanly; UI reflects cancelled; no orphan artifacts |

**Manual pass criteria:** every step's expected result observed; no console errors; numbers
reconcile with the API/report ([Phase 4](04_reporting.md), [Phase 7](07_api.md)).

---

## 3. Automation Outline (general idea — to be added)

| Area | Approach |
|---|---|
| Framework | Playwright or Selenium against the web UI |
| Initiate-with data | Start automation with DS-P8-02 (fast) then DS-P6-01 (all-tier render) |
| Flows to script | create → start → assert progress increments → pause → assert halt → resume → assert completion → open report → assert status counts |
| Assertions | DOM progress values reconcile with API status; report table row counts per status == manifest |
| Data hooks | Reuse broker + `dataset_validator.py` to stage known datasets before each run |

---

## 4. Tools

- The web UI (manual).
- API/report as the source of truth to reconcile UI numbers ([Phase 7](07_api.md), [Phase 4](04_reporting.md)).
- **To be added:** Playwright/Selenium harness; dataset staging hooks.

See [../tools_guide.md](../tools_guide.md).

---

## 5. To Be Added

- Full manual checklist rows in [../../docs/testcaselist.xlsx](../../docs/testcaselist.xlsx).
- UI automation harness (framework choice, page objects, CI wiring).
- Visual/render checks for special-character filenames.

Narrative reference: [../../docs/planv2.md](../../docs/planv2.md) (progress counters, pause/verify).
