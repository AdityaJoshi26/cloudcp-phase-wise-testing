# Phase 2 — Scheduler / Broker

[← Back to master plan](../complete_plan.md)

> **What this phase is:** The broker (`aws.py`) is a long-lived scheduler that owns dispatch.
> It reads `NETWORK_PROFILE`, assigns worker slots to tiers by weight, dispatches batches to
> cloudcp, work-steals freed slots to active tiers, and refills. This phase validates
> **scheduling fairness, work-stealing, caps, and profile-driven behaviour** — while batch
> packaging on disk stays byte-identical.

**Priority:** P0 (scheduling correctness). Throughput/bandwidth trade-off measurement is P2.
**Scope note:** Broker path only — **parallel mode (GNU `parallel`) is out of scope.**

---

## 1. Scheduling Contract

- Broker learns a batch's tier from its **location** (`batches/pending/<tier>/…`).
- Each tier has a **weight** (share of slots when all tiers have work) and a
  **`max_concurrent`** hard cap.
- Freed slots (a tier drains) flow to remaining active tiers — **no worker sits idle** while
  work exists anywhere.
- Changing `NETWORK_PROFILE` changes slot allocation **only**; batch files on disk are
  byte-for-byte identical between profiles.

Baseline profile `dt2_100gbe` (indicative weights per redesign doc — confirm on host):
large / medium / small / tiny.

---

## 2. Test Cases

### 2.1 Weighted Allocation (P0)

| ID | Case | Dataset | Pass when |
|---|---|---|---|
| P2-01 | Steady-state weight ratio | DS-P6-01 (balanced all-tier) | In-flight slots per tier match configured ratio (±5%); no tier exceeds `max_concurrent` |
| P2-02 | Per-tier hard cap respected | DS-P6-01 | `in-flight[tier] ≤ max_concurrent[tier]` at every sample, even with a large backlog |
| P2-03 | Same-tier refill preference | DS-P6-01 | ≥80% of post-completion dispatches pick the same tier while it has pending work |
| P2-04 | Alt weight ratio 7:5:3:1 | DS-P11-01 | Slot distribution converges to 7:5:3:1 |
| P2-05 | Weight with empty tier (9:5:2:0) | DS-P11-02 | Zero tiny workers; slots split across large/medium/small |
| P2-06 | Two tiers only (10:6:0:0) | DS-P11-03 | Only large + medium receive workers |

### 2.2 Work-Stealing / Exhaustion (P0)

Six independent scenarios, one per exhaustion pattern (dataset category 3):

| ID | Scenario | Dataset | Pass when |
|---|---|---|---|
| P2-W1 | Large drains first | DS-P3-01 | Freed large slots absorbed by medium/small/tiny; `sum(in-flight)=max_workers` while work remains |
| P2-W2 | Medium drains first | DS-P3-02 | Freed medium slots redistributed |
| P2-W3 | Small drains first | DS-P3-03 | Freed small slots redistributed |
| P2-W4 | Tiny drains first | DS-P3-04 | Freed tiny slots redistributed |
| P2-W5 | Large + medium drain together | DS-P3-05 | Small + tiny split all slots |
| P2-W6 | Only tiny remains | DS-P3-06 | All workers converge on tiny; no idle slot |

**Convergence:** after a tier drains, distribution reaches the new ratio within 3 scheduling
cycles.

### 2.3 Network Profile (P0)

| ID | Case | Dataset | Pass when |
|---|---|---|---|
| P2-N1 | Profile switch changes scheduling only | DS-P6-01 run under two profiles | Slot distribution differs per profile (tiny-heavy on low-bandwidth, large-favoured on 100 GbE); **batch file hashes byte-identical** across runs |

### 2.4 Requests/sec vs Bandwidth (P2 — performance)

| ID | Case | Dataset | Pass when |
|---|---|---|---|
| P2-P1 | Tiny bound by request rate | DS-P1-02 / tiny-heavy | Bandwidth <30% of link while PUT/sec at peak |
| P2-P2 | Large bound by bandwidth | DS-P1-06 (large) | Bandwidth ≥70% while PUT/sec <100 |
| P2-P3 | Mixed uses both simultaneously | DS-P6-01 / DS-P7-* | Bandwidth ≥50% AND PUT ≥500/sec at once; neither resource idle |

### 2.5 Configuration (P0)

| Setting | Value | Validate |
|---|---|---|
| `NETWORK_PROFILE` | `dt2_100gbe` | Scheduler uses profile weights; confirmed by slot sampling |
| `PARALLEL_WORKERS` | `1` | Never more than 1 concurrent cloudcp |
| `PARALLEL_WORKERS` | `32` | Up to 32 concurrent cloudcp |
| `PARALLEL_WORKERS` | `0` | Refuses to start; clear error |
| `BATCH_BUILDER_ONLY` | `true` | Batches built to `pending/`; no cloudcp spawned |

---

## 3. Datasets Used

| Category | Datasets | Purpose |
|---|---|---|
| 3 — Batch Exhaustion / Weight Shift | DS-P3-01 … DS-P3-06 | Work-stealing scenarios |
| 6 — Network Profile Comparison | DS-P6-01 | Dual-profile run; identical batch hashes |
| 11 — Alternative Weight Ratios | DS-P11-01 … DS-P11-03 | Weight convergence, empty tiers |

Catalog: [../../dataset_cloudcp/spec_files/dataset_map.json](../../dataset_cloudcp/spec_files/dataset_map.json).

---

## 4. Tools

- Broker with `NETWORK_PROFILE` set in `/etc/bryck/bryckcloud/config.json`.
- **Slot sampler** — polls in-flight batch counts per tier every N seconds (**TBA**).
- **Batch-hash differ** — hashes `pending/` batch files across two profile runs (**TBA**).

See [../tools_guide.md](../tools_guide.md).

---

## 5. To Be Added

- Slot-distribution sampling harness (per-tier in-flight over time → ratio + convergence).
- Profile-diff automation (run same dataset under 2 profiles, assert identical batch hashes,
  different schedules).
- Performance capture (PUT/sec, bandwidth %, CPU) for P2 cases.

Narrative reference: [../../docs/planv2.md](../../docs/planv2.md) Phase 2; design:
[../../docs/broker_scheduler_redesign.md](../../docs/broker_scheduler_redesign.md).
