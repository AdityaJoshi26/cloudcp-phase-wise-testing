# Phase 7 — API Testing

[← Back to master plan](../complete_plan.md)

> **What this phase is:** The transfer control surface — the API used to start, control,
> observe, and report on a CloudCP transfer. This phase validates the **API contract**:
> request/response shape, state transitions, idempotency, and error handling. All API tests
> drive the **new CloudCP through the broker**.

**Priority:** P0 (API functional correctness).
**Config:** `/etc/bryck/bryckcloud/config.json`.

---

## 1. Endpoint Surface (to confirm on host)

The exact endpoints/paths must be inventoried from the running service. The expected control
verbs, mapped to pipeline actions:

| Capability | Maps to | Notes |
|---|---|---|
| Start transfer | spawn broker (`transfer-id`, source path, bucket, prefix, profile) | Returns transfer id + accepted state |
| Batch-only build | broker `--batch-only` / `BATCH_BUILDER_ONLY` | Produces `batch_summary.csv`, no upload |
| Status / progress | read `files_done/total`, `bytes_done/total`, per-tier | Monotonic counters (see [Phase 4](04_reporting.md)) |
| Pause | set `pause_requested` | Must NOT trigger verification |
| Resume | clear pause | Continues to natural completion barrier |
| Report | final per-file status + per-tier summary | `REPORT_FORMAT=json` |
| Cancel / delete | stop + clean transfer meta | Reversible/idempotent semantics |

> **To be added:** authoritative endpoint inventory (method, path, request/response schema)
> captured from the service and its OpenAPI/route table.

---

## 2. Test Cases (P0)

| ID | Case | Pass when |
|---|---|---|
| P7-01 | Start transfer | Accepts source/bucket/prefix/profile; returns transfer id + accepted state; broker spawns |
| P7-02 | Start with invalid params | Rejected with clear error; no partial transfer state left |
| P7-03 | Batch-only via API | Builds batches + `batch_summary.csv`; no cloudcp spawned |
| P7-04 | Status while running | Returns accurate `files_done/total`, `bytes_done/total`, per-tier; counters never decrease |
| P7-05 | Status before scan complete | Reports `in_progress`; does not expose false MISSING |
| P7-06 | Pause | `pause_requested=True`; transfer halts; verification NOT triggered |
| P7-07 | Resume | Continues from where it paused; no re-upload of done files |
| P7-08 | Report after completion | Returns full per-file statuses + per-tier summary matching manifest |
| P7-09 | Idempotent start (same id) | Re-issuing start for an existing id does not duplicate/corrupt state |
| P7-10 | Cancel / cleanup | Stops cleanly; transfer meta removed/marked; no orphan batches |
| P7-11 | Config surfaced correctly | Active `NETWORK_PROFILE` / `BATCH.*` reflected in transfer metadata |

---

## 3. Data & Tools

- Small, fast datasets for functional API checks (e.g. DS-P8-02 single file, DS-P2-02 small
  count-seal, DS-P7-01 for a full status/report cycle).
- Tools: API client / `curl`; broker; verification engine.
- **To be added:** endpoint inventory + contract-test harness (schema assertions,
  state-transition assertions).

See [../tools_guide.md](../tools_guide.md).

---

## 4. To Be Added

- Authoritative endpoint inventory (methods, paths, schemas).
- Contract tests (request/response validation, status codes, error bodies).
- State-machine tests (start → pause → resume → complete → report; cancel at each state).
- Auth/permission checks (if applicable).

Narrative reference: [../../docs/planv2.md](../../docs/planv2.md) (pause/verify barriers).
