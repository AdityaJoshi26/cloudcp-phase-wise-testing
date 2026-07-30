# BryckCloud Transfer System — Test Plan

**Version:** 2.0  
**Audience:** Engineering leadership, QA, product stakeholders  
**Purpose:** End-to-end validation of the BryckCloud file transfer pipeline — from the moment files are discovered on disk to the moment a final verified report is produced.

---

## What This Plan Covers

The BryckCloud pipeline moves hundreds of millions of files to S3-compatible storage. This test plan validates every stage of that pipeline:

| # | Stage | What it does |
|---|---|---|
| 1 | **Batch Builder** | Scans the source, groups files by size, writes transfer jobs to disk |
| 2 | **Cloud Transfer (cloudcp)** | The C++ engine that performs the actual upload to S3 |
| 3 | **Fallback & Retry** | The safety net that catches upload failures and re-attempts them |
| 4 | **Reporting & Verification** | Reconciles source vs S3 and produces a per-file final report |

Each stage is tested in isolation first, then together as a full end-to-end pipeline.

---

## Test Data: The Five File Populations

All tests run against a **single S3 bucket**. The source data is divided into five populations reflecting real-world usage patterns.

| Population | File size range | Object count | Why it matters |
|---|---|---|---|
| **Tiny files** | Under 1 MB | 4,000,000 | Each file is one S3 API call — tests request-rate limits |
| **Small files** | 1 MB – 5 MB | 500,000 | Common data files, documents |
| **Medium files** | 16 MB – 25 MB | 200,000 | Large exports, logs |
| **Large files** | 500 MB – 100 GB | 120,000 | Database dumps, archives, media |
| **Broad mix** | Above 1 MB | 2,000,000 | Exercises full tier range together |

### File Name Variants — Applied to Every Population

Every population includes files with names that historically break transfer tools. Each variant below is tested in every phase:

| Variant | Concrete example | Why we must test it |
|---|---|---|
| Plain ASCII, no spaces | `report_2024.csv` | Baseline — must always work |
| Spaces in the name | `my report.csv` | Extremely common in user data |
| Trailing space | `export ` | Must not be stripped silently |
| Embedded newline character | `file` + `\n` + `name.txt` (raw bytes) | Breaks every line-based parser |
| Trailing carriage return | `data` + `\r` (raw bytes) | Windows-originated files |
| Non-English bytes (Latin-1) | `café_data` stored as raw Latin-1 bytes | European datasets |
| Very long name (240+ chars) | `aaaa...aaa.bin` | File system path limit edge case |
| Unicode — CJK, Arabic, emoji | `数据文件_🚀.bin` | Internationalised data |
| Double or missing extension | `archive.tar.gz`, `datafile` | Extension-agnostic handling |
| Mixed worst-case | `données export` + `\r` | Combination of all the above |

### File Type Variety — Applied to Every Population

Each population contains files of every type below, ensuring no file type is accidentally excluded or mishandled:

- Text formats: `.csv`, `.json`, `.txt`, `.log`, `.sql`
- Binary formats: `.bin`, `.gz`, `.tar`, `.parquet`
- Media: `.jpg`, `.mp4`
- No extension: bare binary files

---

## Phase 1 — Batch Builder

> **What this phase is:** Before any file is uploaded, the Batch Builder scans the source directory and groups files into "batches" — lists of files handed to the upload engine one at a time. This phase verifies that grouping is correct, efficient, and crash-safe.

---

### 1.1 Functionality Tests

---

#### Test: Files are sorted into the correct size bucket before uploading starts

- **What is validated:** Every file lands in the right tier (zero / tiny / small / medium / large) based on its exact byte size. No file is placed in the wrong tier, missed, or double-counted.
- **How it is tested:**
  - Feed files of every size class to the Batch Builder
  - Include all ten boundary values: exactly 0 B, 1 B, 999 KB, 1 MB, 63 MB, 64 MB, 999 MB, 1 GB, and 100 GB
- **Pass when:**
  - Every file's assigned tier matches the expected tier for that size
  - The total file count across all tiers equals the total number of input files
  - No file appears in more than one tier

---

#### Test: A batch closes when it hits either the file-count limit or the data-size limit — whichever comes first

- **What is validated:** Each tier has two independent sealing triggers — a maximum number of files and a maximum total data size. The batch must seal the instant either limit is crossed.
- **How it is tested:**
  - For each tier, add files one-by-one until the count limit is hit → verify seal
  - For each tier, add large files until the byte limit is hit → verify seal
  - Verify that the file that triggered sealing opens a new batch, not overflows the old one
- **Pass when:**
  - No sealed batch has more files than its `max_files` limit
  - No sealed batch has more bytes than its `target_size_mb` limit
  - The triggering file is the first record in a new batch

---

#### Test: Files are spread evenly across multiple open slots so batches of every size are available early

- **What is validated:** Each tier keeps 8 batches filling at once (round-robin). The upload engine should always have batches ready across all tiers — it never waits for one tier's backlog to finish before starting another.
- **How it is tested:**
  - Assign 800 tiny files (expecting ~100 per slot across 8 open slots)
  - Measure how many files each slot received
- **Pass when:**
  - Each of the 8 open slots holds approximately the same number of files (within ±1)
  - Batch close times are staggered — no single batch accumulates all files

---

#### Test: File names with special characters survive byte-for-byte into the batch file

- **What is validated:** Batch files use NUL (`\0`) as the only record separator — the one byte that can never appear in a filename. Every name variant must emerge from the batch file exactly as it entered, with no stripping, decoding, or re-encoding.
- **How it is tested:**
  - Write one batch containing all 10 filename variants
  - Read the batch back, split on NUL, compare each path against the original
- **Pass when:**
  - Every path recovered from the batch matches the original filesystem bytes exactly
  - A filename containing an embedded newline (`\n`) produces exactly one record — not two
  - No trailing spaces, CR characters, or Latin-1 bytes are removed or modified

---

#### Test: A crash mid-write never leaves a partial batch file visible to workers

- **What is validated:** Every batch file is written to a temporary location (`*.tmp`), then atomically renamed into place. A worker must never see an incomplete batch.
- **How it is tested:**
  - Start a flush; send `SIGKILL` to the process during the write
  - After the kill, inspect the `pending/` directory
- **Pass when:**
  - No `*.tmp` files exist in `pending/` after the kill
  - Every batch file that exists in `pending/` is complete (ends with a NUL byte)
  - No corruption or partial records are found on restart

---

#### Test: The source index contains exactly one record per source file with the correct size and timestamp

- **What is validated:** Alongside the batches, the Batch Builder writes a `source.index` — the master reference used by the verification engine. It must be complete, de-duplicated, and accurate.
- **How it is tested:**
  - Run the full scan on the mixed corpus (~6.87 million files)
  - Count records in `source.index`; compare each record against the dataset manifest
- **Pass when:**
  - Record count = input file count (no files missing, no extras)
  - Each record's path, size, and mtime match the generated dataset manifest
  - No path appears twice in `source.index`

---

### 1.2 Resume Tests

> A transfer of 200 million files can take many hours. A crash, pause, or restart must lose zero progress. These tests verify every restart scenario is safe.

---

#### Test: Restarting after the scan is interrupted mid-walk loses no files and creates no duplicates

- **What is validated:** The scanner journals every directory visited. On restart it picks up from exactly where it stopped.
- **How it is tested:**
  - Run the 4-million-file scan; kill it at 25%, 50%, and 75% completion (three separate runs)
  - Restart with the same transfer ID and let it complete each time
- **Pass when:**
  - Final `source.index` record count = exactly 4,000,000 in all three kill scenarios
  - Zero duplicate paths appear in `source.index`
  - All batch files that existed before the kill are intact and untouched after restart

---

#### Test: Restarting after the scan is fully complete skips the entire directory walk

- **What is validated:** If scanning is already done (`scan_state=complete`), a restart should skip the tree walk entirely and only re-dispatch outstanding upload batches.
- **How it is tested:**
  - Complete the scan phase; kill the process before all uploads finish
  - Restart and observe whether any directory walking occurs
- **Pass when:**
  - No `stat` or `readdir` calls on the source tree after restart
  - Only batches in `pending/` and `inprogress/` are re-dispatched
  - Batches already in `completed/` are not re-run

---

#### Test: Batch IDs never collide across any number of restarts

- **What is validated:** Each batch has a unique sequential ID stored in the manifest (`seq_high_water`). New batches after a restart must continue from where the ID sequence left off — never reusing an old ID.
- **How it is tested:**
  - Run three restart cycles; collect all batch file names across `pending/`, `inprogress/`, and `completed/`
  - Check for ID collisions
- **Pass when:**
  - All batch IDs across all directories are globally unique
  - After each restart, the first new batch ID is strictly higher than any previously used ID

---

#### Test: Files already uploaded are skipped on restart without reading any per-file extended attributes

- **What is validated:** The skip-set is built from the upload report CSV — not from `xattr` (extended file attributes). The old system set an xattr per file, which broke on read-only sources.
- **How it is tested:**
  - Run a partial transfer; record which files were uploaded
  - Restart; trace all system calls using `strace`
  - Verify that previously uploaded files are not added to any new batch
- **Pass when:**
  - Zero `getxattr` or `setxattr` system calls are observed during enumeration
  - Files already in the upload report do not appear in newly-published batches

---

### 1.3 Configuration Tests

---

#### Test: Flat config keys override nested config keys for batch sizes

- **What is validated:** If both `TINYFILE_BATCH_SIZE=100` (flat) and `BATCH.TINY.BATCH_SIZE=999` (nested) are set simultaneously, the flat key wins. This is the backward-compatibility guarantee.
- **How it is tested:**
  - Write a config with both keys set to conflicting values
  - Run the Batch Builder and inspect sealed batch sizes
- **Pass when:**
  - Tiny batches seal at 100 files (flat key)
  - The nested value of 999 is never used

---

#### Test: The transfer refuses to start if less than 10% disk space is free on the batch metadata volume

- **What is validated:** A preflight free-space check runs before any directory is created. Starting a transfer when the disk is nearly full must fail cleanly with a clear message.
- **How it is tested:**
  - Fill the batch metadata volume to 92% capacity
  - Attempt to start a transfer
- **Pass when:**
  - Process exits with an "insufficient free space" error message
  - No `transfer_<id>/` directory is created
  - Exit code is non-zero

---

#### Test: A durable checkpoint is written every N files as configured, bounding the cost of a restart

- **What is validated:** Every N files (configurable via `CHECKPOINT_EVERY_FILES`), open batches are flushed and frontier journals are synced to disk. After a crash, the restart position is at most N files behind.
- **How it is tested:**
  - Set `CHECKPOINT_EVERY_FILES=10000`
  - Run the scanner; kill it at 15,000 files
  - Restart and check where the resume begins
- **Pass when:**
  - A checkpoint was written at the 10,000-file mark
  - After the kill at 15,000, restart begins from ≥ 10,000 (not from 0)
  - `scan.discovered` and `scan.completed` logs are both present and consistent

---

#### Test: Symlinked directories and files are skipped and logged — no infinite loops can occur

- **What is validated:** The default policy is to skip all symlinks. A symlink loop (directory A → B → A) must not cause the scanner to hang indefinitely.
- **How it is tested:**
  - Create a symlink loop in the source tree
  - Run the scanner to completion
- **Pass when:**
  - Scanner completes without hanging
  - Every skipped symlink has an entry in `scan_errors.log`
  - Symlink targets are absent from `source.index`

---

### 1.4 Edge Case Tests

| Scenario | What is set up | Pass condition |
|---|---|---|
| Empty source directory | Source dir exists but has no files | `scan_state=complete` recorded; no batch files; exit code 0 |
| Single zero-byte file | One 0-byte file in the source | One batch in the `zero` tier; one NUL record in `source.index` |
| Single 100 GB file | One very large file | One batch in the `large` tier; one record |
| Unreadable sub-directory | `chmod 000` on one sub-dir | Logged to `scan_errors.log`; rest of tree scanned normally; no crash |
| Batch metadata dir not writable | Remove write permission on `batchmeta/` | Preflight fails with a clear error; no partial state left behind |
| 14 levels of nested directories | Deep tree structure | Resume works correctly at all depths; no stack overflow |
| Filename exactly 255 bytes | Max POSIX filename length | Path stored exactly in batch; readable after round-trip |

---

## Phase 2 — Cloud Transfer (cloudcp) and Scheduler

> **What this phase is:** Each batch is handed to `cloudcp`, a high-performance C++ upload engine. A Python broker (the scheduler) decides which batch runs next, based on the active network profile. This phase validates upload correctness, exit-code handling, and scheduling fairness.

---

### 2.1 Upload Correctness Tests

---

#### Test: A successful batch results in all files confirmed on S3 with correct keys

- **What is validated:** `cloudcp` exit code 0 means every file is on S3. Each file's S3 key must equal `<prefix>/<relative-path-from-source-root>` and the batch must transition to `completed/`.
- **How it is tested:**
  - Run a batch with all files reachable and S3 available
  - After completion, call `HeadObject` for every file
- **Pass when:**
  - Every `HeadObject` returns HTTP 200 with matching size
  - All rows in the transfer report show `SUCCESS`
  - Batch directory state = `completed/`

---

#### Test: A partial upload failure produces a retry list — the batch stays open until all retries are drained

- **What is validated:** `cloudcp` exit code 2 means some files failed. It writes a `.lst` file listing only the failures. The batch must not be marked done until the fallback worker processes that list.
- **How it is tested:**
  - Inject a 5% S3 failure rate via a proxy
  - Run a batch and observe the outputs
- **Pass when:**
  - A `.lst` file is created with exactly the failed file paths
  - The batch stays in `inprogress/` until the fallback worker marks it done
  - Successful files have `SUCCESS` rows in the transfer report

---

#### Test: A total batch failure triggers an immediate inline retry — no batch is left permanently stuck

- **What is validated:** `cloudcp` exit code 1 means the entire batch failed. The dispatcher must retry every file immediately using a local boto3 process pool, without waiting for the fallback worker.
- **How it is tested:**
  - Block S3 access entirely for one batch
  - Unblock S3 after 10 seconds
  - Observe whether the batch self-recovers
- **Pass when:**
  - Batch moves to `completed/` after the inline retry
  - No batch is left permanently in `inprogress/`
  - Other batches running in parallel are unaffected during the retry

---

#### Test: S3 keys for all filename variants are composed correctly and accessible via HeadObject

- **What is validated:** Key composition rule: strip the source-root prefix from the absolute path, prepend the S3 prefix, join with `/`. This must work for every filename variant, including trailing spaces, CR, newlines, and non-UTF-8 bytes.
- **How it is tested:**
  - Upload one file from each of the 10 filename variant classes
  - Compute the expected S3 key using the composition formula
  - Run `HeadObject` on each expected key
- **Pass when:**
  - `HeadObject` returns HTTP 200 for all 10 variants
  - Keys recorded in the transfer report match the formula exactly
  - No character is stripped, escaped, or re-encoded by the Python layer

---

#### Test: Files already uploaded in a previous run are skipped — not re-uploaded

- **What is validated:** `cloudcp` reads its own report on startup and skips files with status `SUCCESS`. This is the intra-batch resume mechanism.
- **How it is tested:**
  - Run a batch; kill `cloudcp` at 50% completion
  - Restart the same batch
- **Pass when:**
  - The second run produces exactly 50% `SKIPPED` rows
  - No file shows a second `PUT` in the S3 access log (ETag is unchanged)
  - Final `SUCCESS + SKIPPED` count = total files in the batch

---

#### Test: Every uploaded file is confirmed with a HeadObject check before being written as SUCCESS

- **What is validated:** A successful PUT alone is not sufficient. The file's size on S3 must be confirmed before the record is committed. A size mismatch after PUT must prevent a `SUCCESS` row.
- **How it is tested:**
  - Mock `HeadObject` to return an incorrect size for a set of files
  - Run the upload
- **Pass when:**
  - Files with a HeadObject size mismatch do not appear as `SUCCESS`
  - Those files appear in `cloudcp_retry_*.lst` or `cloudcp_failed.log`
  - They surface as `MISMATCH` in the final verification report

---

#### Test: Files above the multipart threshold use multipart upload; smaller files use a single PUT

- **What is validated:** Files ≥ 64 MB (configurable via `CHUNK_SIZE_MB`) must use S3 multipart upload. Smaller files must use a single PUT. No incomplete multipart parts should be left behind.
- **How it is tested:**
  - Run DS4 (16–25 MB files, below default threshold) → expect single PUT
  - Run DS5 (500 MB–100 GB files) → expect multipart
  - Verify via S3 access logs
- **Pass when:**
  - DS4 files: single-part PUT confirmed in S3 access log
  - DS5 files: multipart initiation + multiple part ETags confirmed in S3 access log
  - Zero incomplete multipart uploads remain after the transfer

---

### 2.2 Scheduler and Worker Weight Tests

---

#### Test: With all four tiers active, worker slots are allocated in the configured 6:4:3:3 ratio

- **What is validated:** On the `dt2_100gbe` profile with 16 workers, the target allocation is large=6, medium=4, small=3, tiny=3. This must hold in steady state when all tiers have work.
- **How it is tested:**
  - Run the full mixed corpus with all four tiers stocked
  - Sample the number of in-flight batches per tier every 5 seconds for 60 seconds
- **Pass when:**
  - `large` holds 37.5% of in-flight slots (±5%)
  - `medium` holds 25.0% (±5%)
  - `small` holds 18.75% (±5%)
  - `tiny` holds 18.75% (±5%)
  - No tier ever exceeds its `max_concurrent` cap

---

#### Test: When a tier runs out of batches, its freed slots are absorbed by the remaining tiers — no worker sits idle

- **What is validated:** Work-stealing ensures an idle slot is never wasted. When one tier has no more batches, its slot allocation flows to the other active tiers automatically.
- **How it is tested:** Six scenarios are run independently, one for each exhaustion pattern:

| Scenario | Tier that runs out | Who absorbs the freed slots |
|---|---|---|
| 1 | Large runs out first | Medium, small, and tiny share the 6 freed slots |
| 2 | Medium runs out first | Large, small, and tiny share the 4 freed slots |
| 3 | Small runs out first | Large, medium, and tiny share the 3 freed slots |
| 4 | Tiny runs out first | Large, medium, and small share the 3 freed slots |
| 5 | Large and medium both drain | Small and tiny split all 16 slots (8 each) |
| 6 | Only tiny has remaining work | All 16 slots assigned to tiny |

- **Pass when (all six scenarios):**
  - `sum(in-flight slots across all tiers) = 16` at all times while any tier has pending batches
  - No worker slot sits idle while there is work available in any tier

---

#### Test: After a tier drains, slot distribution converges to the new ratio within 3 scheduling cycles

- **What is validated:** When `large` drains, the remaining tiers (medium=4, small=3, tiny=3, total weight=10) should converge to 40% / 30% / 30% of the 16 slots within a short window.
- **How it is tested:**
  - Run `dt2_100gbe` on the mixed corpus; wait for `large` to drain
  - Measure slot distribution for 30 seconds after the last large batch completes
- **Pass when:**
  - Medium holds 40% of slots (±10%)
  - Small holds 30% of slots (±10%)
  - Tiny holds 30% of slots (±10%)
  - Convergence happens within 3 scheduling cycles (not 30 seconds from now)

---

#### Test: Per-tier hard concurrency caps are respected regardless of what weights would allow

- **What is validated:** If `max_concurrent[large]=6`, then `in-flight[large]` must never exceed 6, even if 10 large batches are waiting and there are free workers.
- **How it is tested:**
  - Set `max_concurrent[large]=6` with `weight=10` (would normally grab more slots)
  - Fill the large tier with 100 pending batches
  - Run for 10 minutes and track peak `in-flight[large]`
- **Pass when:**
  - `in-flight[large] ≤ 6` at every observed moment during the 10-minute window

---

#### Test: A freed worker slot after a large-file batch tends to be refilled with another large-file batch

- **What is validated:** When more large batches are available, the scheduler should naturally prefer refilling the freed slot with the same tier. This minimises tier-switching overhead.
- **How it is tested:**
  - Track the tier of the batch dispatched immediately after each `large` batch completion
  - Collect 100 such observations
- **Pass when:**
  - ≥ 80% of immediately post-completion dispatches pick another `large` batch (while large has pending work)

---

### 2.3 Network Profile Tests

---

#### Test: Switching network profiles changes slot allocation without touching batch packaging

- **What is validated:** Changing the profile from `dt2_100gbe` to `low_bandwidth` shifts slot weight toward tiny files (clearing the request backlog first on a slow link). The batch files on disk must be identical — only the scheduling changes.
- **How it is tested:**
  - Run DS_MIXED with `dt2_100gbe`; record slot distribution and batch file hashes
  - Run DS_MIXED again with `low_bandwidth`; record slot distribution and batch file hashes
- **Pass when:**
  - `low_bandwidth`: tiny gets the most slots; large gets fewest
  - `dt2_100gbe`: large gets the most slots; tiny gets fewest
  - Batch files on disk are byte-for-byte identical between the two runs

---

### 2.4 Requests/Second vs Bandwidth Trade-off Tests

---

#### Test: Tiny-file workers are limited by S3 request rate — not by network bandwidth

- **What is validated:** With 4 million tiny files, the network pipe should be mostly idle while the S3 request rate is at its peak. This confirms the system is correctly identifying the bottleneck.
- **How it is tested:**
  - Run DS1 (4M tiny files) with `max_workers=16`
  - Capture: PUT requests/sec, network bandwidth %, CPU utilization — every 10 seconds
- **Pass when:**
  - Network bandwidth < 30% of link capacity while requests/sec is at its peak
  - Increasing the tiny tier's weight from 3 → 6 increases PUT rate by ≥ 40%
  - Network bandwidth does not increase proportionally when weight is increased

---

#### Test: Large-file workers are limited by network bandwidth — not by request rate

- **What is validated:** With 120,000 large files, the network should be saturated while request rate is low. This is the opposite bottleneck from tiny files.
- **How it is tested:**
  - Run DS5 (500 MB–100 GB files) with `max_workers=16`
  - Capture: PUT requests/sec, network bandwidth %, CPU utilization — every 10 seconds
- **Pass when:**
  - Network bandwidth ≥ 70% of link capacity during sustained upload
  - PUT requests/sec < 100 during that same window

---

#### Test: Running both file populations at the same time uses both resources simultaneously — and finishes faster than running them separately

- **What is validated:** The entire design motivation: tiny and large files run concurrently so the request pipeline and the bandwidth pipe are both utilized at the same time.
- **How it is tested:**
  - Run DS_MIXED (all tiers) with `dt2_100gbe`; measure wall time
  - Run DS1 alone (tiny only), then DS5 alone (large only); record wall times
  - Compare total time
- **Pass when:**
  - During the mixed run: network bandwidth ≥ 50% AND PUT rate ≥ 500 requests/sec simultaneously
  - Total DS_MIXED wall time ≤ 80% of the worst single-tier-first sequential run
  - Neither resource sits at 0% while the other is at 100%

---

### 2.5 Configuration Tests

| Setting | Value tested | What is validated |
|---|---|---|
| `NETWORK_PROFILE` | `dt2_100gbe` | Scheduler uses 6:4:3:3 weights; confirmed by slot-distribution measurement |
| `PARALLEL_WORKERS` | `1` | Never more than 1 concurrent cloudcp process |
| `PARALLEL_WORKERS` | `32` | Up to 32 concurrent cloudcp processes |
| `PARALLEL_WORKERS` | `0` | Process refuses to start; clear error message |
| `BATCH_BUILDER_ONLY` | `true` | Batches built and written to `pending/`; no cloudcp process ever spawned |
| `LOCAL_AWS` | `https://10.x.x.x:9000` | All S3 API calls go to the MinIO endpoint; confirmed via network capture |
| `CHUNK_SIZE_MB` | `8` | Multipart parts are 8 MB each; confirmed via S3 access log |

---

## Phase 3 — Fallback and Retry

> **What this phase is:** `cloudcp` is fast but can encounter S3 errors. When it does, a persistent Python fallback worker retries each failed file individually, verifies the upload with a HeadObject check, and marks the batch complete. This phase validates that no file is ever silently lost.

---

#### Test: When cloudcp partially fails, the retry list is picked up and every failed file is retried

- **What is validated:** The fallback worker watches for `.lst` files dropped by cloudcp (exit code 2). It ingests them, retries each file with a boto3 client, and drains the list.
- **How it is tested:**
  - Inject a 1% S3 error rate via proxy → cloudcp produces a `.lst` with ~5,000 failed files
  - Run the fallback worker alongside the transfer
- **Pass when:**
  - The `.lst` file is ingested within 5 seconds of appearing
  - All entries in the `.lst` are retried
  - Each retry is followed by a `HeadObject` size confirmation
  - `.lst` is renamed to `.lst.done` after all entries are drained
  - Batch moves to `completed/`

---

#### Test: When cloudcp completely fails a batch, the inline retry runs immediately — without involving the fallback worker

- **What is validated:** `cloudcp` exit code 1 triggers an immediate in-process boto3 process pool retry in `aws_transfer.py`. The batch does not wait for the fallback worker.
- **How it is tested:**
  - Block S3 access entirely for one specific batch; allow all others
  - Unblock S3 after 10 seconds
- **Pass when:**
  - Batch moves to `completed/` after the inline retry completes
  - No batch is left stuck in `inprogress/`
  - Other batches running in parallel are not affected during the 10-second block

---

#### Test: Every file uploaded by the fallback is confirmed with a HeadObject check before being recorded as done

- **What is validated:** A successful `PUT` alone is not enough. The fallback worker must confirm the file's size on S3 before writing `FALLBACK_OK`. A size mismatch must prevent the file from being counted as done.
- **How it is tested:**
  - Mock `HeadObject` to return a wrong size for 10 specific files
  - Run the fallback worker
- **Pass when:**
  - Those 10 files are not written as `FALLBACK_OK`
  - They appear in `failed_uploads.<pid>` after exhausting retries
  - The final report marks them as `FAILED`

---

#### Test: Transient S3 errors are retried with exponential backoff; permanent errors are written as failures immediately

- **What is validated:** The retry policy distinguishes between errors worth retrying (SlowDown, InternalError, RequestTimeout) and errors that should not be retried (AccessDenied).
- **How it is tested:** Four error types are injected, one per sub-test:

| Error type | Should retry? | Expected behaviour |
|---|---|---|
| `SlowDown` | Yes | Retried up to `max_attempts`; delay grows exponentially between attempts |
| `InternalError` | Yes | Retried up to `max_attempts`; exponential backoff |
| `RequestTimeout` | Yes | Retried up to `max_attempts`; exponential backoff |
| `AccessDenied` | No | Written to `failed_uploads` immediately; zero retries |

- **Pass when:**
  - Transient errors are retried the configured number of times with growing delays
  - `AccessDenied` triggers exactly 1 attempt and no retries

---

#### Test: A file that exhausts all retry attempts is recorded as a permanent failure — the batch still completes

- **What is validated:** A "poison file" — one that never succeeds — must not block an entire batch from completing. Once its retries are exhausted it is written to `failed_uploads` and the batch is marked done.
- **How it is tested:**
  - Inject errors for 5 specific files that persist beyond `max_attempts`
  - Run the complete fallback cycle
- **Pass when:**
  - The 5 files appear in `failed_uploads.<pid>` with `attempt_count = max_attempts`
  - The batch still reaches `completed/`
  - The other files in the same batch are recorded as `FALLBACK_OK`

---

#### Test: The fallback worker restarts cleanly after a crash — no files are processed twice or silently dropped

- **What is validated:** The fallback can crash mid-drain. On restart it re-globs un-retired `.lst` files and uses the upload report as its skip-set to avoid re-processing already-done entries.
- **How it is tested:**
  - Kill the fallback worker halfway through draining a `.lst` file
  - Restart the fallback worker
- **Pass when:**
  - All `.lst` entries end up as either `FALLBACK_OK` or in `failed_uploads`
  - No file appears twice in the upload report
  - `.lst.done` files are not re-processed

---

#### Test: Verification does not start until the fallback worker signals it is fully done

- **What is validated:** The broker writes a `_fallback_done` marker only after all batches are in `completed/` and the fallback has no un-drained `.lst` files. Verification reads from the fully-populated report.
- **How it is tested:**
  - Instrument the broker to record when `_fallback_done` is written
  - Instrument verification to record when it starts
- **Pass when:**
  - Verification start timestamp > `_fallback_done` write timestamp
  - No `.lst` files (non-`.done`) exist at the point `_fallback_done` is written
  - Fallback does not exit before all `.lst` files are drained

---

### 3.1 Configuration Tests

| Setting | Value tested | What is validated |
|---|---|---|
| `FALLBACK_ENABLED` | `False` | No fallback worker is spawned; rc=2 failures appear as `FAILED` in the final report only |
| `TM_THREAD_POOL_SIZE` | `4` | Fallback uses at most 4 threads during drain |
| `TM_THREAD_POOL_SIZE` | `64` | Fallback uses up to 64 threads; drain throughput improves proportionally |
| `rc1_retry.processes` | `4` | Inline retry spawns exactly 4 worker processes |
| `rc1_retry.threads_per_process` | `8` | Each of the 4 processes uses 8 threads and its own boto3 client |

---

## Phase 4 — Reporting and Verification

> **What this phase is:** After all transfers complete, the verification engine diffs the source index (every file that exists on disk) against the upload report (every file successfully transferred). It produces a final per-file status report and updates the transfer database.

---

#### Test: The final report assigns exactly one correct status to every file

- **What is validated:** Every file in the source is classified into exactly one of five statuses. No file is missing from the report; no file appears twice.
- **How it is tested:** Five failure types are injected in a controlled way:

| Status | Meaning | How it is injected |
|---|---|---|
| `OK` | Transferred and verified | Normal upload — 100 files |
| `MISSING` | In source, never uploaded | Skip uploading 100 files deliberately |
| `FAILED` | cloudcp and fallback both gave up | Inject 20 permanently non-retryable errors |
| `MISMATCH` | Size on S3 does not match source | Mock wrong HeadObject size for 100 files |
| `EXTRA` | In S3 but not in the source index | Manually PUT extra objects to S3 |

- **Pass when:**
  - Each status count in the final report exactly matches the injected count
  - The total row count = sum of all injected counts
  - CSV quoting is correct — embedded newlines in paths survive in the output file

---

#### Test: Verification refuses to run while the source scan is still in progress

- **What is validated:** If `scan_state=in_progress`, calling verification must be refused. Running verification before the scan is complete would produce false `MISSING` results for files not yet discovered.
- **How it is tested:**
  - Set `scan_state=in_progress` in the manifest
  - Call the verification engine directly
- **Pass when:**
  - Verification returns an error: "scan_state=in_progress, cannot verify"
  - No `final_report.csv` is written
  - After scan completes, verification proceeds normally

---

#### Test: A paused transfer does not accidentally trigger verification

- **What is validated:** In the previous system, pausing a transfer could incorrectly kick off the verification step (bug #6). This must be impossible.
- **How it is tested:**
  - Set `pause_requested=True` mid-transfer
  - Allow all batches to reach `completed/` and the scan to reach `complete`
  - Observe whether verification is triggered
- **Pass when:**
  - Verification is not triggered while `pause_requested=True`
  - Verification only runs after the transfer is explicitly resumed and reaches the natural completion barrier

---

#### Test: Files with special-character names match correctly across the source index and the upload report

- **What is validated:** The merge-join that compares `source.index` vs the union of upload report CSVs must handle all 10 filename variant classes without any false `MISSING` or `MISMATCH` caused by encoding differences between the two sides.
- **How it is tested:**
  - Upload the full 10-variant filename set
  - Run verification
- **Pass when:**
  - All 10 filename variant classes appear as `OK` in the final report
  - Zero false mismatches caused by stripping, encoding conversion, or path rewriting

---

#### Test: A file appearing in both cloudcp's report and the fallback report gets a single correct final status

- **What is validated:** De-duplication must apply last-status-wins logic. A file recorded as `SKIPPED` (from a previous run) and later `FALLBACK_OK` (from a retry) should appear exactly once as `OK`.
- **How it is tested:**
  - Create a scenario where 50 files have both a `SKIPPED` row and a `FALLBACK_OK` row
  - Run verification
- **Pass when:**
  - Each of those 50 files appears exactly once in the final report
  - Status is `OK` (last terminal success wins)
  - No row is duplicated for any combination of `SUCCESS`, `SKIPPED`, or `FALLBACK_OK`

---

#### Test: Progress counters (files done / total and bytes done / total) are accurate and never decrease

- **What is validated:**
  - The denominator (total files, total bytes) comes from `source.index` — available after the first scan checkpoint
  - The numerator (done files, done bytes) comes from the growing union of all report CSVs
- **How it is tested:**
  - Sample progress counters every 5 seconds throughout a full DS3 transfer
  - Check each sample against the previous one
- **Pass when:**
  - `files_done` and `bytes_done` never decrease between samples
  - At transfer completion: `files_done = total_files` and `bytes_done = total_bytes`
  - `total_files` is non-zero from the first checkpoint onward

---

#### Test: The per-tier completion summary correctly aggregates all counts and byte totals

- **What is validated:** The verification output must include per-tier stats: batches created vs completed, files OK / failed / missing per tier, bytes transferred, and average batch duration.
- **How it is tested:**
  - Run DS_MIXED (all tiers); compare the per-tier summary against the dataset manifest
- **Pass when:**
  - Per-tier file counts sum to the full-transfer total
  - Per-tier byte counts sum to the full-transfer total
  - No tier row is missing from the summary
  - `avg_batch_duration_sec` is populated for each tier

---

#### Test: The failure summary report has one row per permanently failed file with full triage context

- **What is validated:** The aggregated `failed_uploads` report must be actionable for operators. Every row must have enough detail to diagnose why the file failed.
- **How it is tested:**
  - Trigger 30 permanent failures across three different error types
  - Run verification and inspect the output
- **Pass when:**
  - Exactly 30 rows appear — one per failed file
  - Each row contains: local path, S3 target key, file size, last error message, attempt count, first attempt timestamp, last attempt timestamp
  - Rows are sorted by error type for easy triage

---

## End-to-End Pipeline Tests

> These tests run the entire pipeline — scan → batch → upload → retry → report — without any manual intervention. They use the real file populations at the stated scale.

---

### 5.1 Full Happy Path

---

#### Test: 4 million tiny files transfer completely with all files verified

- **Dataset:** DS1 — 4 million files, all under 1 MB
- **Profile:** `dt2_100gbe`
- **What is run:** Full pipeline from scan to final report
- **Pass when:**
  - Final report: 4,000,000 `OK`, 0 `MISSING`, 0 `FAILED`
  - `scan_state=complete` recorded before verification starts
  - No xattr calls on hot path

---

#### Test: 120,000 large files (500 MB – 100 GB) transfer completely using multipart upload

- **Dataset:** DS5 — 120,000 files, 500 MB to 100 GB each
- **Profile:** `dt2_100gbe`
- **Pass when:**
  - All files confirmed on S3 with correct ETags
  - S3 access log shows multipart upload for every file
  - Zero incomplete multipart uploads remain after completion

---

#### Test: The full mixed corpus (~6.87 million files) completes with weighted scheduling active

- **Dataset:** DS_MIXED — all tiers combined
- **Profile:** `dt2_100gbe`
- **Pass when:**
  - All files confirmed `OK` in the final report
  - Weighted slot distribution (6:4:3:3) observed during the run
  - Total wall time recorded as the performance baseline for regression tests

---

### 5.2 Crash and Resume Tests

---

#### Test: Killing the transfer at 25% and restarting reaches 100% with no duplicate uploads

- **Setup:** Kill broker when `files_done / total_files = 0.25`; restart with same transfer ID
- **Pass when:**
  - `SKIPPED` count after restart equals the `files_done` count at the time of kill
  - Final report shows 100% `OK`, 0 `MISSING`, 0 `FAILED`
  - Resume overhead (extra time vs a clean run to the same 25% point) < 10%

---

#### Test: Killing the fallback worker mid-drain and restarting completes all retries correctly

- **Setup:** Inject 5% S3 errors to force `.lst` files; kill fallback worker halfway through drain; restart
- **Pass when:**
  - All `.lst` entries end up as either `FALLBACK_OK` or in `failed_uploads`
  - No file is counted twice in any report
  - Un-retired `.lst` files (not yet renamed `.lst.done`) are re-processed on restart

---

### 5.3 Fault Injection Tests

---

#### Test: S3 endpoint unreachable for 60 seconds — transfer pauses cleanly and resumes automatically

- **Setup:** Block all S3 traffic for 60 seconds during an active transfer
- **Pass when:**
  - Transfer pauses without crashing
  - No data corruption; no half-written files
  - No batch is left permanently in `inprogress/`
  - When S3 becomes reachable, transfer resumes automatically

---

#### Test: Disk fills up on the batch metadata volume — scanner pauses with a clear error and recovers

- **Setup:** Fill the batch metadata volume to 95% capacity during active enumeration
- **Pass when:**
  - Scanner pauses with a clear `ENOSPC`-related message
  - No half-written batch files exist
  - After freeing disk space, the transfer can be resumed normally

---

#### Test: Three simultaneous transfers on different source directories do not interfere with each other

- **Setup:** Start three transfers with `MAX_CONCURRENT_TRANSFERS=3` on different source directories
- **Pass when:**
  - Each transfer's final report contains only its own files
  - Per-tier batch counts are independent across transfers
  - No batch from one transfer is dispatched by another transfer's broker

---

## Performance Goals and Regression Thresholds

Every test run records the metrics below. A test suite run is flagged as a **regression** if any threshold is breached compared to the stored baseline from the last passing run.

| Metric | Target | Flagged as regression if |
|---|---|---|
| File throughput — tiny corpus (DS1) | ≥ 12,500 files/sec | < 90% of baseline |
| Bandwidth throughput — large corpus (DS5) | ≥ 9,500 MB/sec | < 85% of baseline |
| Scan time — full mixed corpus (DS_MIXED) | Recorded as baseline on first run | > 115% of baseline |
| Verification time — full mixed corpus | Recorded as baseline on first run | > 120% of baseline |
| Average batch duration per tier | Recorded per tier on first run | > 130% of baseline for any single tier |

### What is Recorded Per Test Run

Every test run emits a structured JSON record containing:

- Start and end timestamps for each pipeline phase (scan, upload, fallback, verification)
- Total wall time for the complete transfer
- Files transferred and bytes transferred
- Throughput in files/sec and MB/sec
- S3 PUT rate (average and peak over 10-second windows)
- Per-tier counts: batches created, batches completed, files OK, files failed, average batch duration
- Fallback files processed; inline retry events triggered

---

## Test Execution Summary

| Phase | Tests | Execution environment |
|---|---|---|
| Batch Builder — Functionality | 6 | Unit — fast, < 30 s each, no disk or S3 I/O |
| Batch Builder — Resume | 4 | Integration — real disk I/O, no S3 |
| Batch Builder — Configuration | 4 | Integration — real disk I/O |
| Batch Builder — Edge Cases | 7 | Unit / Integration |
| Cloud Transfer — Upload correctness | 7 | Functional — MinIO as S3 target |
| Cloud Transfer — Scheduler & weights | 8 | Integration |
| Cloud Transfer — Configuration | 7 | Unit / Functional |
| Fallback & Retry | 8 | Functional — MinIO |
| Reporting & Verification | 8 | Functional — MinIO |
| End-to-End — Happy path | 3 | Scale — real AWS S3, full dataset |
| End-to-End — Resume | 2 | Scale |
| End-to-End — Fault injection | 3 | Functional / Scale |
| **Total** | **67** | |

---

## Sign-off Criteria

The test plan is considered **fully passed** when all of the following are true:

- [ ] All 21 unit and integration tests pass with no failures
- [ ] All functional tests pass against MinIO
- [ ] All end-to-end happy-path tests pass against real AWS S3
- [ ] No performance regression thresholds are breached vs the stored baseline
- [ ] The final report for every scale test shows **0 MISSING** and **0 FAILED**
- [ ] The full mixed corpus transfer (`DS_MIXED`) completes within the performance baseline window
- [ ] Zero xattr system calls observed on the hot path across all integration and functional tests
- [ ] No test produces a half-written batch file, a stuck `inprogress/` batch, or a silent file drop
