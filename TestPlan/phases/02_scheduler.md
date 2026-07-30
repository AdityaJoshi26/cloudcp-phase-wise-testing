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

> **Ground truth vs. under test.** This phase is driven by the **deterministic-enumeration**
> catalog (`SCH-*`, self-contained under [../../CloudCpSchedulerTesting](../../CloudCpSchedulerTesting)).
> Those datasets fix *what work exists and in what **enumeration** order* (the oracle); the broker
> decides ***dispatch** order* (under test). A passing scheduler may dispatch in a completely
> different order than files were enumerated — that is expected. See design:
> [deterministic_enumeration_datasets.md](../../CloudCpSchedulerTesting/deterministic_enumeration_datasets.md).

---

## 2. Test Cases

> Full procedures and pass criteria live in
> [../../CloudCpSchedulerTesting/test_cases.md](../../CloudCpSchedulerTesting/test_cases.md). This is
> the phase-level index. `SCH-*` IDs map 1:1 to that file.

### 2.0 Enumeration Oracle (P0 — deterministic precondition)

Proves the pending set that every scheduler test relies on. Run with `BATCH_BUILDER_ONLY=true` on
`SCH-ORD-01 … 12` (and `SCH-DEEP-*` for the backlog oracle). Independent of the scheduler and of
`scandir` order.

| ID | Case | Dataset(s) | Pass when |
|---|---|---|---|
| SCH-EN-01 | Enumeration order chain-contiguous | SCH-ORD-01 … 12 | `source.index` is `[n1×C1 … n5×C5]` in chain order, exact per-tier counts |
| SCH-EN-02 | Single-chain walk order | SCH-ORD-01 … 12 | One dir per level; child descended only after parent files complete |
| SCH-EN-03 | `scandir` invariance | SCH-ORD-01 ×3 | Batch composition byte-identical across rebuilds; only in-batch filenames differ |
| SCH-BA-01 | Batch count per tier (ORDER) | SCH-ORD-01 … 12 | Z 8, T 16, S 16, M 8, L 8 — total 56 |
| SCH-BA-02 | Full-batch shape | SCH-ORD-01 … 12 | Every full batch `nfiles==M`, `nbytes==M×file_size` |
| SCH-BA-03 | finish()-flush handling | SCH-ORD-01 | Order asserted only for streamed R≥2 tiers; terminal K batches unordered |
| SCH-BA-04 | Batch count per tier (DEEP) | SCH-DEEP-01 … 03 | Z 400, T 400, S 200, M 16, L 16 — total 1032 |

### 2.1 Weighted Allocation & Work-Stealing (P0 — under test)

Run against the known `SCH-DEEP-*` pending backlog; sample per-tier in-flight counts.

| ID | Case | Dataset | Pass when |
|---|---|---|---|
| SCH-SD-01 | Steady-state weight ratio | SCH-DEEP-01 | In-flight slots per tier match profile ratio (±5%); no tier exceeds `max_concurrent` |
| SCH-SD-02 | Per-tier hard cap | SCH-DEEP-01 | `in-flight[tier] ≤ max_concurrent[tier]` at every sample despite deep backlog |
| SCH-SD-03 | Same-tier refill preference | SCH-DEEP-01 | ≥80% of post-completion dispatches pick same tier while it has pending work |
| SCH-SD-04 | Work-stealing on drain | SCH-DEEP-01 | LARGE (16)/MEDIUM (16) drain before SMALL (200)/TINY (400)/ZERO (400); freed slots redistributed; `sum(in-flight)=max_workers` while work remains; new ratio within 3 cycles |
| SCH-SD-05 | Large-first no-starvation | SCH-DEEP-02 | Bandwidth tiers first, yet TINY/ZERO still get reserved slots |
| SCH-SD-06 | Tiny-first request-race (WAN-like) | SCH-DEEP-03 | ZERO/TINY backlog drains at request-rate while MEDIUM/LARGE trickle; no idle slot |

**Convergence:** after a tier drains, distribution reaches the new ratio within 3 scheduling
cycles.

### 2.2 Network Profile (P0)

| ID | Case | Dataset | Pass when |
|---|---|---|---|
| SCH-SD-07 | Profile switch changes scheduling only | SCH-ORD-01 under two profiles | Slot distribution differs per profile (tiny-heavy on low-bandwidth, large-favoured on 100 GbE); **batch file hashes byte-identical** across runs |

### 2.3 Requests/sec vs Bandwidth (P2 — performance)

> Regenerate the dataset with `--content random` first (sparse files carry no real bytes to move).

| ID | Case | Dataset | Pass when |
|---|---|---|---|
| P2-P1 | Tiny bound by request rate | SCH-DEEP-03 (tiny-first) | Bandwidth <30% of link while PUT/sec at peak |
| P2-P2 | Large bound by bandwidth | SCH-DEEP-02 (large-first) | Bandwidth ≥70% while PUT/sec <100 |
| P2-P3 | Mixed uses both simultaneously | SCH-DEEP-01 (balanced) | Bandwidth ≥50% AND PUT ≥500/sec at once; neither resource idle |

### 2.4 Configuration (P0)

| ID | Setting | Value | Validate |
|---|---|---|---|
| SCH-CF-01 | `NETWORK_PROFILE` | `dt2_100gbe` | Scheduler uses profile weights; confirmed by slot sampling |
| SCH-CF-02 | `PARALLEL_WORKERS` | `1` | Never more than 1 concurrent cloudcp |
| SCH-CF-03 | `PARALLEL_WORKERS` | `32` | Up to 32 concurrent cloudcp; caps still respected |
| SCH-CF-04 | `PARALLEL_WORKERS` | `0` | Refuses to start; clear error |
| SCH-CF-05 | `BATCH_BUILDER_ONLY` | `true` | Batches built to `pending/`; no cloudcp spawned; pending set matches oracle |

---

## 3. Datasets Used

Self-contained deterministic-enumeration catalog (does **not** reuse `dataset_cloudcp/`). Each
dataset is a single nested chain — one homogeneous size-tier per level — so BFS enumerates tiers in
a pre-known order.

| Group | Datasets | Profile | Purpose |
|---|---|---|---|
| ORDER — enumeration breadth | `SCH-ORD-01 … 12` | 29 688 files / 56 batches each | Enumeration-order + batch-shape oracle across 12 curated tier orderings |
| DEEP — scheduling stress | `SCH-DEEP-01 … 03` | 1 068 680 files / 1032 batches each | Deep per-tier pending backlog for weighted allocation, caps, refill, work-stealing |

Catalog & per-level detail:
[../../CloudCpSchedulerTesting/spec_files/manifest.json](../../CloudCpSchedulerTesting/spec_files/manifest.json).
Generate with
[../../CloudCpSchedulerTesting/generate_dataset.py](../../CloudCpSchedulerTesting/generate_dataset.py)
(`python3 generate_dataset.py <1..15>`). Design & oracle:
[../../CloudCpSchedulerTesting/deterministic_enumeration_datasets.md](../../CloudCpSchedulerTesting/deterministic_enumeration_datasets.md).

---

## 4. Tools

- Broker with `NETWORK_PROFILE` set in `/etc/bryck/bryckcloud/config.json`.
- **Enumeration-order validator** — asserts `source.index` is tier-contiguous in chain order with
  the manifest's per-tier counts (**TBA**).
- **Batch-shape validator** — asserts per-tier `batches.created` counts and `(nfiles, nbytes)`
  shapes match the oracle (design doc §2.1/§6) (**TBA**).
- **Slot sampler** — polls in-flight batch counts per tier every N seconds (**TBA**).
- **Batch-hash differ** — hashes `pending/` batch files across two profile runs (**TBA**).

See [../tools_guide.md](../tools_guide.md).

---

## 5. To Be Added

- Oracle-validation harness (source.index tier-contiguity + batch count/shape vs manifest;
  finish()-flush aware per design doc §3.2).
- Slot-distribution sampling harness (per-tier in-flight over time → ratio + convergence).
- Profile-diff automation (run same dataset under 2 profiles, assert identical batch hashes,
  different schedules).
- Performance capture (PUT/sec, bandwidth %, CPU) for P2 cases (requires `--content random` builds).

Narrative reference: [../../docs/planv2.md](../../docs/planv2.md) Phase 2; design:
[../../docs/broker_scheduler_redesign.md](../../docs/broker_scheduler_redesign.md).
