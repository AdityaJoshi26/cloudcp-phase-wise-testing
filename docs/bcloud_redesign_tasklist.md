# Bcloud Redesign — Detailed Task List

Derived from `bcloud_redesign_proposal.md`. Task IDs are stable handles for planning.
Legend: **§** = design section · **Bug #** = challenge from the recent-upload issue list ·
**Dep** = prerequisite task IDs · **Est** = rough size (S ≤ 1d, M ≤ 3d, L ≤ 1wk, XL > 1wk).

Status column: TODO / IN-PROGRESS / DONE / BLOCKED.

---

## Phase 0 — Foundations & shared contracts

| ID | Task | §  | Dep | Est | Status |
|----|------|----|-----|-----|--------|
| F1 | Define **on-disk layout** contract for a transfer (manifest.json, scan.discovered/completed, source.index, batches/{pending,inflight,fallback_wait,processed,failed}, txhistory, batches.created/completed, error_log, failed_uploads). Document schema_version. | §1.2,§14.3,§18 | — | S | TODO |
| F2 | Define **NUL-framed batch format** (`relpath \0 size \0`) + reader/writer helpers with byte-exact round-trip (no strip/split). Unit tests for `\n`, trailing space, CR, Latin-1, zero-byte. | §1.3 | — | S | TODO |
| F3 | Define **tx-log CSV schema** `local_path,s3path,size,etag,status,attempt,finished_at` + quoted/NUL-safe parser. Fixtures with special-char paths. | §6.2 | — | S | TODO |
| F4 | Define **failed-set format** `local_path \0 s3path \0 size \0 last_error \0` + error_log line format. | §6.2,§11.3 | — | S | TODO |
| F5 | Central **config.json schema** additions (BATCH_BUILDER, ORCHESTRATOR, FALLBACK, CLOUDCP, SPACE, DOWNLOAD_LISTER) + loader with hot-reload + validation/defaults. | §1.4,§2.2,§4,§6.4,§16 | — | M | TODO |
| F6 | **Free-space utility** (`statvfs` pct) + `NoSpace` exception, shared by all writers. | §16 | — | S | TODO |
| F7 | **Key normalization** helper (raw bytes → UTF-8, else Latin-1, else percent-encode) shared by cloudcp + fallback + verify. Unit tests. | §6.3 | F2 | S | TODO |

---

## Phase 1 — BatchBuilder (upload)

| ID | Task | §  | Bug | Dep | Est | Status |
|----|------|----|-----|-----|-----|--------|
| B1 | **Standalone BatchBuilder core** (size buckets, per-bucket packing by target_bytes/max_files, open batches, NUL batches, source.index, manifest). CSV input for test. | §1.3,§1.4 | 2 | F2,F5 | M | **DONE** (batch_builder.py) |
| B2 | **Weighted-concurrent scheduling knobs** in batch layout so tiny+large drain together (open_batches per bucket + weights consumed by orchestrator). | §1.4 | 2 | B1 | S | PARTIAL (open_batches done; weights = C-series) |
| B3 | **Preflight** (batchmeta writable, source readable, ≥10% free) + **per-flush space guard** → BLOCKED exit. | §16.1,§16.2 | 1,17,18 | F6 | S | **DONE** (in batch_builder.py) |
| B4 | **batches.created log** + pending/ atomic `.tmp`→rename write. | §18.1 | 15 | B1 | S | **DONE** |
| B5 | **Resume reconciliation** `pending_to_run = created − completed`. | §18 | 14,15 | B4 | S | **DONE** (pending_to_run) |
| B6 | **Directory-scan source** (multiprocessing walker): coordinator + N walkers, dir_queue/result_queue, per-(proc,bucket) writers, `os.fsencode` raw bytes. | §13.1–13.5,13.7 | 9,10,11,12,20 | B1 | L | TODO |
| B7 | **Scan resume via discovered/completed logs** (frontier = discovered − completed), batched dir_done messages + grouped fsync, seq_high_water in manifest. | §13.6 | 15 | B6 | M | TODO |
| B8 | **Walker failure handling** (unreadable dir → scan_errors.log + manifest.unreadable[]; walker/coordinator crash recovery; .tmp cleanup on resume). | §10.4,§13.10 | 1 | B6,B7 | M | TODO |
| B9 | **Completion barrier + stop semantics** (scan_state=complete only when frontier empty & buffers flushed; SIGTERM flush + checkpoint). | §13.8,§13.9 | 6 | B7 | S | TODO |
| B10 | **source.index finalization** (concat shards, integrity/torn-record truncation on resume). | §13.5,§10.4 | 8 | B6 | S | TODO |
| B11 | **Symlink/special-file policy** (skip+log by default, configurable). | §13.3 | — | B6 | S | TODO |
| B12 | BatchBuilder **unit + integration tests** (special chars end-to-end, resume mid-scan, deep 14-level tree, read-only source). | §13 | 1,9,10,11,12 | B6,B7,B8 | M | TODO |

---

## Phase 2 — Download BatchBuilder / S3 lister

| ID | Task | §  | Bug | Dep | Est | Status |
|----|------|----|-----|-----|-----|--------|
| D1 | **Adopt `s3_list_bucket_fast.py`** into module: threaded dynamic-prefix-tree lister, single writer, `.discovered`/`.completed`, streaming source.index (s3path,size,etag). | §15.1 | 8 | F1 | M | TODO |
| D2 | **Bucket-by-size + batch packing** in the writer thread (reuse B1 packer for download batches: key→local relpath). | §15.2 | 2 | D1,B1 | M | TODO |
| D3 | **ARN/assumed-role creds** + endpoint + Config(pool,adaptive retries) wiring for lister. | §15.3 | — | D1,F5 | S | TODO |
| D4 | **--import-csv fast-start** (optional) carry-forward with documented leaf-skip limitation. | §15.1,§15.5 | — | D1 | S | TODO |
| D5 | Download-path **cloudcp contract** (download batch → local write, size/checksum check instead of HeadObject, same tx CSV). | §15.2 | 13 | F3 | M | TODO |
| D6 | Lister **tests** (many prefixes, millions simulated via mock/minio, resume mid-listing dup handling). | §15 | 8 | D1,D2 | M | TODO |

---

## Phase 3 — cloudcp (C++ uploader)

| ID | Task | §  | Bug | Dep | Est | Status |
|----|------|----|-----|-----|-----|--------|
| C1 | **Batch ingestion**: read NUL-framed batch + args (source-root, bucket, bucket-prefix, txlog). No pre/post steps. | §3.1,§6.1 | 20 | F2,F3 | M | TODO |
| C2 | **Key handling**: raw-byte local open + `normalize()` key; preserve spaces/CR/`\n` verbatim. | §6.3 | 9,10,11,12 | F7 | M | TODO |
| C3 | **Upload + post-upload HeadObject verify** (size match) before writing SUCCESS. | §6.2 | 13 | C1 | M | TODO |
| C4 | **Tx CSV writer** (local_path,s3path,size,etag,status,attempt,finished_at), O_APPEND, per-batch. | §6.2 | 8 | F3,C3 | S | TODO |
| C5 | **error_log + failed_uploads** side logs (transient errors vs terminal failures). | §6.2 | 8 | F4 | S | TODO |
| C6 | **Fast-fail / quick fallback**: per-object deadline, max_object_attempts, slow-floor detection, multipart abort. | §6.4 | 3 | C3,F5 | M | TODO |
| C7 | **Per-batch circuit breaker** (fail-rate threshold → dump remainder to failed_uploads for boto3 fallback). | §6.4 | 3 | C6 | S | TODO |
| C8 | **Intra-batch resume**: read own tx CSV on start, skip SUCCESS relpaths. | §11.2 | 14 | C4 | S | TODO |
| C9 | **Exit-code contract** (0 all-ok / 2 partial / 1 fatal). | §3.2 | 3 | C4,C5 | S | TODO |
| C10 | CRT client + thread/chunk config from config.json; ARN creds. | §6.3,§6.4 | 4 | F5 | M | TODO |
| C11 | cloudcp **tests** (multipart stall→fastfail, special-char keys, HeadObject mismatch, resume skip). | §6 | 3,9-13 | C6,C8 | M | TODO |

---

## Phase 4 — Orchestrator (replaces GNU parallel)

| ID | Task | §  | Bug | Dep | Est | Status |
|----|------|----|-----|-----|-----|--------|
| O1 | **Batch dispatch loop**: rebuild batch_queue from pending/+inflight/, claim→inflight/, stream to cloudcp workers (no `parallel`). | §2.1,§14.1 | 20 | B5,C1 | M | TODO |
| O2 | **Reconcile step** (parse txlog → processed/ or write failed-set + fallback_wait/ + enqueue fallback). | §14.4 | 8,16 | O1,C4 | M | TODO |
| O3 | **Weighted concurrent scheduler** (slots by profile weights; tiny+large in flight together). | §1.4,§2.2 | 2 | O1,F5 | M | TODO |
| O4 | **Autoscaling process pool** (min/max procs, backlog scale-up, idle scale-down, inflight cap backpressure). | §2.2,§14.6 | 21 | O1,F5 | L | TODO |
| O5 | **batches.completed log** written with terminal rename (log-then-rename idempotent). | §18.1 | 15 | O2 | S | TODO |
| O6 | **Restart recovery** (re-enqueue inflight/ and fallback_wait/; idempotent via tx-skip). | §14.3,§14.8 | 14,15 | O2,O5 | M | TODO |
| O7 | **Global fallback queue** contract + dispatch to fallback pool. | §11,§14.5 | 4,16 | O2 | M | TODO |
| O8 | Orchestrator **tests** (crash mid-batch resume, backpressure, no-parallel streaming throughput). | §2,§14 | 20,21 | O4,O6 | M | TODO |

---

## Phase 5 — Fallback (boto3, dynamic pool)

| ID | Task | §  | Bug | Dep | Est | Status |
|----|------|----|-----|-----|-----|--------|
| FB1 | **Replace `aws s3 cp`** with persistent boto3 client pool (transfer_mp.py model): process pool + threads/proc, per-process client. | §4 | 4 | F5 | M | TODO |
| FB2 | **Consume failed-set/CSV** (not xattr); upload + **HeadObject verify**; write FALLBACK_OK/FAILED to tx CSV. | §4,§11.2 | 5,13,16 | FB1,C4 | M | TODO |
| FB3 | **Dynamic pool scaling** by fallback_queue depth (min/max, backoff, retry_on list). | §4,§14.6 | 4,21 | FB1,F5 | M | TODO |
| FB4 | **Retry policy + poison-file cap** (max_attempts → failed/ + failed_report.csv; exponential backoff+jitter). | §11.2,§14.4 | 4 | FB2 | S | TODO |
| FB5 | Fallback **tests** (dynamic scale under load, HeadObject mismatch retry, poison-file termination, no-xattr resume). | §4 | 4,5,13 | FB3,FB4 | M | TODO |

---

## Phase 6 — Verification (no bucket listing)

| ID | Task | §  | Bug | Dep | Est | Status |
|----|------|----|-----|-----|-----|--------|
| V1 | **External-sort normalize** both sides (source.index & union of txhistory) to `key \t size \t status`, NUL-safe. | §12.1 | 8 | B10,C4 | M | TODO |
| V2 | **Streaming merge-join** → OK/MISSING/FAILED/MISMATCH/EXTRA; dedup last-status-wins. | §12.1 | 8 | V1 | M | TODO |
| V3 | **Report generator** (counts + discrepancy CSV; fallback-recovered surfaced). | §5,§12 | 8 | V2 | S | TODO |
| V4 | **Gate**: run only when scan_state=complete AND all batches terminal AND not paused. | §17.1 | 6 | O5 | S | TODO |
| V5 | (Optional) **sqlite alt** documented/spiked for ad-hoc queries. | §12.2 | — | V2 | S | TODO |
| V6 | Verification **tests** (200M-row scale sample, missing/mismatch/extra fixtures, special-char keys). | §12 | 8,9 | V2,V3 | M | TODO |

---

## Phase 7 — Service integration: state machine, progress, aws.py

| ID | Task | §  | Bug | Dep | Est | Status |
|----|------|----|-----|-----|-----|--------|
| S1 | **Transfer state machine** (PENDING→PREFLIGHT→SCANNING→UPLOADING⇄PAUSED→FINALIZING→VERIFYING→DONE; BLOCKED/FAILED). | §17.1 | 6 | — | M | TODO |
| S2 | **Fix pause→verify bug**: PAUSED first-class; verify gate excludes pause_requested. | §17.1 | 6 | S1,V4 | S | TODO |
| S3 | **Preflight state + creds/space checks** hooked before leaving PENDING → BLOCKED with reason. | §16.1,§17.1 | 1,18 | F6,S1 | S | TODO |
| S4 | **Runtime space monitor** → clean PAUSE on ENOSPC with hysteresis + clear message. | §16.2 | 17 | F6,S1 | M | TODO |
| S5 | **Progress: files done + total & bytes done + total** (denominators from source.index; batched counter updates from tx logs; restart-safe). | §17.2 | 7,19 | B10,O2 | M | TODO |
| S6 | **aws.py rewire**: replace bcloud_src_enum|parallel|aws_transfer with BatchBuilder→orchestrator; ARN creds preserved. | §2,§3 | 20 | O1,B6 | M | TODO |
| S7 | **Remove xattr code paths** across enum/transfer/fallback; ensure no getxattr/setxattr on hot path. | §7 | 1,5,14 | S6,FB2 | S | TODO |
| S8 | **DB schema** (progress fields total/done files+bytes) + batched writers; migration up/down. | §17.2 | 7,19 | S5 | S | TODO |
| S9 | Config **hot-reload** wiring into orchestrator/fallback/batchbuilder live knobs. | §2.2,§21 | 21 | F5,O4,FB3 | S | TODO |

---

## Phase 8 — Rollout, migration, docs

| ID | Task | §  | Dep | Est | Status |
|----|------|----|-----|-----|--------|
| R1 | **Feature flags** (config: new vs legacy path) for staged cutover. | §9 | S6 | S | TODO |
| R2 | **End-to-end staging test** on real dataset subset (special chars, resume, pause, space-exhaustion). | §9 | all | L | TODO |
| R3 | **Perf validation** at scale (2M×1MB and mixed 200M corpus; throughput vs legacy). | §2.6 | R2 | M | TODO |
| R4 | **Operator docs / runbook** (states, logs locations, report format, recovery). | — | R2 | S | TODO |
| R5 | **Cleanup**: retire fallback_worker.py aws-cli path, bcloud_src_enum print pipeline, dead xattr helpers. | §7 | S7,R2 | S | TODO |

---

## Bug fix list (recent-upload challenges → owning tasks)

| Bug # | Symptom | Owning task(s) | Design § |
|------|---------|----------------|----------|
| 1 | No write perm → xattr set failed → resume broke; need preflight | S3, B3, S7, (§7 xattr removal) | §7,§16.1 |
| 2 | Batching didn't balance big/small; small should finish faster | B1,B2,O3 | §1.4 |
| 3 | cloudcp multipart errors/hangs; must fast-fail & fall back quick | C6,C7,C9 | §6.4 |
| 4 | Fallback broken (`aws s3 cp`); use boto3 + dynamic pool | FB1,FB3,FB4 | §4 |
| 5 | Fallback didn't set xattr → resume problem | FB2, S7 (xattr gone) | §7,§11.2 |
| 6 | Pause → wrongly went to verifying; manual fix | S1,S2,V4,B9 | §17.1 |
| 7 | Showed only uploaded, not total | S5,S8 | §17.2 |
| 8 | 300M-object listing too slow; build CSV report during upload; separate error/failed logs | C4,C5,O2,V1-V3,D1 | §5,§6.2 |
| 9 | Non-UTF-8/Latin filenames failed | F2,F7,C2,B6 | §1.3,§6.3 |
| 10 | Trailing spaces stripped → wrong path; same Ctrl+M | F2,C2 (never strip) | §1.3,§6.3 |
| 11 | Embedded `\n` in filenames failed | F2 (NUL framing),C2 | §1.3 |
| 12 | Trailing carriage return failed | F2,C2 | §1.3,§6.3 |
| 13 | boto3 fallback must HeadObject-verify | C3,FB2,D5 | §4,§6.2 |
| 14 | Avoid xattr; use upload log for resume | C8,B5,O6,S7 | §7,§11.2,§18 |
| 15 | Faster batch-level resume (completed/inprogress/pending) | B4,B5,B7,O5 | §14.3,§18 |
| 16 | Fallback based on CSV not xattr | C5,O2,FB2 | §6.2,§11.3,§18.1 |
| 17 | No space → pause with clear "no space" | S4,B3 | §16.2 |
| 18 | Require ≥10% free before start | S3,B3 | §16.1 |
| 19 | Progress: files done + total | S5,S8 | §17.2 |
| 20 | Remove `parallel`; stream for speed | O1,S6,C1 | §2,§3 |
| 21 | Dynamic batch launching from config.json | O4,FB3,S9 | §2.2,§1.4 |

---

## Suggested critical path
F1–F7 → B1(done)/B3-B5(done) → **B6/B7 (scanner)** → C1-C9 (cloudcp) → O1-O6 (orchestrator)
→ FB1-FB4 (fallback) → V1-V4 (verify) → S1-S8 (service+state+progress) → R1-R5 (rollout).

Parallelizable early: Phase 2 (download lister D1-D4) can proceed alongside Phase 1 since it
shares only F-contracts and the B1 packer.
