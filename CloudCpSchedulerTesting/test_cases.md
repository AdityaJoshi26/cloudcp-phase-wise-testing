# Phase 2 — Scheduler Test Cases (Deterministic Enumeration)

[← Phase plan](../TestPlan/phases/02_scheduler.md) ·
Design: [deterministic_enumeration_datasets.md](deterministic_enumeration_datasets.md) ·
Dataset catalog: [spec_files/manifest.json](spec_files/manifest.json)

> **Ground truth vs. under test.** The `SCH-*` datasets fix *what work exists and in what
> **enumeration** order* (deterministic — the oracle). The scheduler/broker decides ***dispatch**
> order* (under test). A passing scheduler may dispatch in a completely different order than files
> were enumerated — that is expected (design doc §9).
>
> - **Group A** (enumeration + batch shapes) is a **P0 precondition**: it proves the pending set the
>   scheduler tests stand on. Deterministic; independent of the scheduler and of `scandir`.
> - **Group B** (dispatch/weight/work-stealing) is the **scheduler under test**, run against the
>   known pending backlog the `SCH-DEEP-*` datasets guarantee exist.
> - **Group C** is configuration/robustness.

Datasets (see [manifest.json](spec_files/manifest.json) for full per-level detail):

| Group | Datasets | Profile | Per dataset |
|---|---|---|---|
| ORDER (enumeration breadth) | `SCH-ORD-01 … 12` | ORDER | 29 688 files, 56 batches |
| DEEP (scheduling stress) | `SCH-DEEP-01 … 03` | DEEP | 1 068 680 files, 1032 batches |

Per-tier constants used by the oracle (design doc §2): `M`=BATCH_SIZE, `K`=OPEN_BATCHES,
`block = M×K`, full-batch `nbytes = M × file_size`.

| Tier | file size | M | K | block | ORDER batches/level | DEEP batches/level |
|---|---:|---:|---:|---:|---:|---:|
| ZERO   | 0 B      | 2000 | 4 | 8000 | 8  | 400 |
| TINY   | 16 KiB   | 511  | 8 | 4088 | 16 | 400 |
| SMALL  | 2 MiB    | 317  | 8 | 2536 | 16 | 200 |
| MEDIUM | 100 MiB  | 50   | 8 | 400  | 8  | 16  |
| LARGE  | 1 GiB    | 5    | 8 | 40   | 8  | 16  |

---

## Group A — Enumeration & BatchBuilder Oracle (P0, deterministic precondition)

Run with `BATCH_BUILDER_ONLY=true` (build to `pending/` without dispatch) so the full batch set is
observable. Applies to every `SCH-ORD-01 … 12` unless noted.

| ID | Case | Dataset(s) | Procedure | Pass when |
|---|---|---|---|---|
| SCH-EN-01 | Enumeration order is chain-contiguous | SCH-ORD-01 … 12 | Read `source.index`; group by tier | `source.index` is exactly `[n1×C1, n2×C2, n3×C3, n4×C4, n5×C5]` in the dataset's `C1..C5` chain order, with per-tier counts from manifest (`enumeration_order` + `num_files`) |
| SCH-EN-02 | Single-chain walk order | SCH-ORD-01 … 12 | Inspect `scan.discovered` / `scan.completed` | Exactly one directory per level (frontier depth 1 throughout); child dir descended only after parent's files complete (design doc F1) |
| SCH-EN-03 | `scandir` invariance | SCH-ORD-01 (repeat ×3) | Rebuild 3× (same seed); diff batch metadata | Batch **composition** (per-tier count, size, tier, sequence) is byte-identical across runs; only filenames *inside* a batch may differ (design doc R1) |
| SCH-BA-01 | Batch count per tier | SCH-ORD-01 … 12 | Count `batches.created` per tier | Matches ORDER column above (Z 8, T 16, S 16, M 8, L 8; total **56**) |
| SCH-BA-02 | Full-batch shape | SCH-ORD-01 … 12 | For each full batch, read `(nfiles, nbytes)` | Every full batch: `nfiles == M(tier)` and `nbytes == M × file_size` (design doc §2.1) |
| SCH-BA-03 | finish()-flush handling | SCH-ORD-01 (Z,T,S = R2; M,L = R1) | Separate streamed vs finish() batches | Streamed rounds (R≥2 tiers) publish in chain order; each tier's final K batches form an **unordered** finish() set — no cross-tier publish-order assertion for R=1 tiers (design doc §3.2). Order for R=1 tiers proven only via `source.index` (SCH-EN-01) |
| SCH-BA-04 | DEEP batch-count oracle | SCH-DEEP-01 … 03 | Count `batches.created` per tier | Matches DEEP column (Z 400, T 400, S 200, M 16, L 16; total **1032**) — establishes the pending backlog for Group B |

---

## Group B — Scheduler Dispatch (P0, under test)

Precondition: Group A green for the dataset, and the full pending backlog materialised (run
`BATCH_BUILDER_ONLY` first, or let enumeration outrun dispatch). Sample per-tier in-flight batch
counts every N seconds. Assert against **known pending counts (§8.2)** — never against creation
order.

| ID | Case | Dataset | Pass when |
|---|---|---|---|
| SCH-SD-01 | Steady-state weight ratio | SCH-DEEP-01 | In-flight slots per tier match configured `NETWORK_PROFILE` weight ratio (±5%) while all tiers have pending work |
| SCH-SD-02 | Per-tier hard cap | SCH-DEEP-01 | `in-flight[tier] ≤ max_concurrent[tier]` at every sample, despite the deep backlog |
| SCH-SD-03 | Same-tier refill preference | SCH-DEEP-01 | ≥80% of post-completion dispatches pick the same tier while it still has pending work |
| SCH-SD-04 | Work-stealing on drain | SCH-DEEP-01 | LARGE (16) & MEDIUM (16) drain long before SMALL (200)/TINY (400)/ZERO (400); freed slots absorbed by remaining active tiers; `sum(in-flight) == max_workers` while any work remains; new ratio reached within 3 scheduling cycles |
| SCH-SD-05 | Large-first no-starvation | SCH-DEEP-02 | Bandwidth tiers created first, yet TINY/ZERO still receive their reserved slots (not starved) under the 100 GbE profile |
| SCH-SD-06 | Tiny-first request-race (WAN-like) | SCH-DEEP-03 | Huge ZERO/TINY request backlog drains at request-rate while MEDIUM/LARGE trickle; no idle slot while work exists anywhere |
| SCH-SD-07 | Profile switch = scheduling only | SCH-ORD-01 under 2 profiles | Slot distribution differs per profile (tiny-favoured on low-bandwidth, large-favoured on 100 GbE); **batch-file hashes byte-identical** across both runs |

**Convergence:** after any tier drains, the distribution reaches the new ratio within 3 scheduling
cycles.

---

## Group C — Configuration & Robustness (P0)

| ID | Setting | Value | Dataset | Pass when |
|---|---|---|---|---|
| SCH-CF-01 | `NETWORK_PROFILE` | `dt2_100gbe` | SCH-DEEP-01 | Scheduler uses profile weights; confirmed by slot sampling (SCH-SD-01) |
| SCH-CF-02 | `PARALLEL_WORKERS` | `1` | SCH-DEEP-01 | Never more than 1 concurrent cloudcp |
| SCH-CF-03 | `PARALLEL_WORKERS` | `32` | SCH-DEEP-01 | Up to 32 concurrent cloudcp; caps still respected |
| SCH-CF-04 | `PARALLEL_WORKERS` | `0` | any | Refuses to start; clear error |
| SCH-CF-05 | `BATCH_BUILDER_ONLY` | `true` | SCH-ORD-01 | Batches built to `pending/`; no cloudcp spawned; pending set matches the Group A oracle |

---

## Group D — Negative / Fault Injection (P1)

Scheduler-level fault injection, driven by
[schedular_negative_test.py](schedular_negative_test.py) (`schedular_test.py --negative`). Every
case runs in a **private throwaway sandbox** (`neg_<id>/{data,batchmeta,logs}`) with tiny
synthesised data — **no machine config, creds, or the real `/opt` batchmeta are touched**. A PASS
means the fault was correctly produced *and* the scheduler handled it as specified (a controlled
failure is a pass). Transport / auth / config-loading faults (bad endpoint, bad creds, network
drop, missing/malformed config, xattr) intentionally live in the CLI / binary suites, not here.

| ID | Group | Injected fault | Pass when |
|---|---|---|---|
| NEG-ENUM-01 | enumeration | Source root absent | Exits non-zero cleanly; no orphan `transfer_<id>` |
| NEG-ENUM-02 | enumeration | Empty root + empty level dir | 0 batches, clean completion (`csv_total == 0`) |
| NEG-ENUM-03 | enumeration | One file `chmod 000` | That file `FAILED`; the tier's other files `SUCCESS` |
| NEG-ENUM-04 | enumeration | A level dir `chmod 000` | Level skipped; no crash/hang |
| NEG-ENUM-05 | enumeration | Files unlinked mid-walk (timing) | Vanished entries tolerated; no crash |
| NEG-ENUM-06 | enumeration | Cyclic symlinks | BFS terminates (no infinite walk) |
| NEG-ENUM-07 | enumeration | Dangling symlink | Skipped; enumeration order preserved |
| NEG-BATCH-01 | batch | One file > tier `TARGET_SIZE_MB` (sparse) | Closes a single-file size-capped batch; that file `SUCCESS` |
| NEG-BATCH-03 | batch | Fewer files than one block | Partial batch flushed at `finish()`; all `SUCCESS` |
| NEG-META-01 | batchmeta | Transfer-dir `chmod 0500` | Metadata write fails fast with a clear non-zero error |
| NEG-META-02 | batchmeta | Malformed batchmeta pre-seeded | Reject or repair; no crash |
| NEG-META-03 | batchmeta | Stale `transfer_<id>` present | Safe bump or clean failure |
| NEG-SCHED-01 | scheduling | `--poll-interval 0` | No tight-loop / div-by-zero; run completes within bound |
| NEG-SCHED-02 | scheduling | One huge stalled batch (best-effort) | Free-worker accounting holds; other tiers progress |
| NEG-LIFE-01 | lifecycle | SIGINT mid-run | Clean shutdown; capture drained; partial CSV parses |
| NEG-LIFE-02 | lifecycle | Kill then re-run same id + transfer-dir | Idempotent resume, no duplicate uploads (needs scheduler resume support) |

> POSIX-only faults (chmod / symlink / signals) auto-**SKIP** on non-POSIX hosts and under
> `--dry-run`. `NEG-META-02` and `NEG-LIFE-02` depend on scheduler-side reject/repair and resume
> support respectively; their expectations are encoded but only pass if the scheduler implements
> them.

---

## Traceability

| Requirement (design doc §9) | Test cases |
|---|---|
| §9.1 Enumeration order deterministic | SCH-EN-01, SCH-EN-02 |
| §9.2 Batch shapes/counts deterministic | SCH-BA-01, SCH-BA-02, SCH-BA-04 |
| §9.3 finish()-flush caveat | SCH-BA-03 |
| §9.4 Scheduler dispatch under test | SCH-SD-01 … SCH-SD-07 |
| R1 homogeneous-dir invariance | SCH-EN-03 |
| Robustness / fault handling | SCH-CF-04, NEG-ENUM-*, NEG-BATCH-*, NEG-META-*, NEG-SCHED-*, NEG-LIFE-* |

## Notes

- All `SCH-BA-*` byte assertions assume the tier's file size lands **inside** its bucket and the
  **file-count** limit closes every full batch (design doc §2.1). If a build reports byte-capped
  closure instead, the config under test differs from the oracle constants above — reconcile before
  running Group B.
- Group A is throughput-agnostic: run MEDIUM/LARGE as `sparse` (manifest default) to keep disk
  footprint near zero. For real throughput measurement (P2), regenerate with
  `generate_dataset.py … --content random`.
- Group D (negative) never mutates the machine: it builds and tears down its own sandbox per case,
  restores permissions before cleanup, and injects faults only via the sandbox filesystem, CLI
  overrides, child-process env, and signals — **no config, creds, or services on the host are
  changed**. Run it with `python schedular_test.py --negative` (`--negative-list` to enumerate,
  `--negative-case <ID>` for one).
