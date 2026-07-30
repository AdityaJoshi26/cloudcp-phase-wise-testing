# Pipelined Batch Transfer Orchestrator — Design Document

## 1. Problem Statement

When transferring 2.5M × 1MB files, upload throughput reaches 3.6 GBPS but **drops to zero** between batch waves. The root cause is sequential execution within each GNU parallel slot:

```
[preprocess: xattr checks on NFS] → [cloudcp upload] → [postprocess: xattr set, parse output, DB update]
```

Each worker holds its parallel slot for ALL 3 phases. Upload is ~0.2s for 79 files at 3.6 GBPS, but pre/post overhead dominates.

### Contributing Factors
1. **Post-processing blocks upload slot** — xattr sets, output parsing, stat file updates
2. **Pre-processing blocks upload slot** — per-file xattr skip-checks on NFS before cloudcp starts
3. **Batch too small** — 79 files = 79MB; fixed overhead dominates
4. **Process startup overhead** — 64 Python forks per wave via GNU parallel
5. **No retry loop** — failed files are logged but not retried within the transfer

---

## 2. Proposed Architecture: 4-Stage Pipeline with Retry

```
┌───────────────┐    ┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│  Enumerator   │───▶│ Pre-process   │───▶│   Uploader    │───▶│ Post-process  │
│  (1 thread)   │    │  (M threads)  │    │  (N workers)  │    │  (P threads)  │
└───────────────┘    └───────────────┘    └───────────────┘    └───────────────┘
                                                │                      │
                                                │    ┌─────────────┐   │
                                                ◀────│ Retry Queue │◀──┘
                                                     └─────────────┘
```

### Queues (all bounded for backpressure)
- `enum_queue` (maxsize=2N) — batch file paths from enumerator → preprocessor
- `upload_queue` (maxsize=2N) — preprocessed spec files → uploader
- `postprocess_queue` (maxsize=4N) — completed upload results → postprocessor
- `retry_queue` (maxsize=N) — failed files/batches → back to upload queue

### Completion Barrier
- Orchestrator tracks outstanding work (enum done + all queues drained + all futures resolved)
- Verification only starts after barrier is passed
- Graceful SIGTERM/SIGINT handling: drain in-flight, skip new batches

---

## 3. Stage Details

### 3.1 Enumerator (1 thread)
- Reuses existing `BatchBuilder` logic from `bcloud_src_enum.py`
- Walks source (local `os.scandir` or S3 paginator)
- Produces batch file paths into `enum_queue`
- Signals completion via sentinel value

### 3.2 Preprocessor (M=4 threads)
- Pulls batch file path from `enum_queue`
- Reads file list, applies skip logic:
  - Check `user.bryckxferstate` xattr per file (upload: on src, download: on dst)
  - If already transferred for this transfer_id → skip, accumulate skipped bytes
- Writes cloudcp spec file: `U<src_path>#<s3_dst>` per line (upload) or `D<dst_path>#<s3_src>` (download)
- Pushes `(spec_file, original_batch_file, metadata)` to `upload_queue`
- If spec file is empty (all skipped): pushes directly to stats update, skips upload

### 3.3 Uploader (N=8-16 workers, ThreadPoolExecutor)
- Pulls spec file from `upload_queue` or `retry_queue`
- Runs: `subprocess.run([cloudcp, spec_file, "", endpoint_url])`
- On completion: pushes `(spec_file, original_batch, rc, stdout, stderr, attempt_num)` to `postprocess_queue`
- **Upload slot freed immediately** — no post-processing here

### 3.4 Postprocessor (P=4 threads)
- Pulls result from `postprocess_queue`
- Merges output with input (identifies processed vs unprocessed lines)
- For each file in output:
  - **Success**: set xattr `user.bryckxferstate`, accumulate transferred bytes, log
  - **Failure**: log error, collect into retry batch
  - **Checksum mismatch**: log, mark as failure, add to retry
- Updates transfer stats (transferred/skipped/failed counts)
- Updates DB bytes periodically (not per-file — batched every N files or T seconds)
- **Retry logic**: if failures exist and attempts < max_retries, builds retry spec and pushes to `retry_queue`

---

## 4. Retry Design

### 4.1 Retry Policy
```python
class RetryPolicy:
    max_retries: int = 3           # per-file retry limit
    retry_batch_size: int = 100    # group retries into new batches
    backoff_base: float = 1.0      # seconds, exponential backoff
    backoff_max: float = 30.0      # cap
    retry_on: list = [             # which errors trigger retry
        "SlowDown",                # S3 throttling
        "InternalError",           # transient S3 errors
        "RequestTimeout",
        "connection reset",
        "broken pipe",
    ]
```

### 4.2 Retry Flow
1. Postprocessor identifies failed files from cloudcp output
2. Groups failures by attempt count
3. If `attempt < max_retries` and error is retryable:
   - Builds new spec file with failed files only
   - Annotates with `attempt_num + 1`
   - Pushes to `retry_queue` after backoff delay
4. If `attempt >= max_retries` or non-retryable error:
   - Marks as permanent failure
   - Logs to error log file
   - Updates failure count

### 4.3 Retry Queue Priority
- `retry_queue` feeds back into the uploader pool
- Upload workers check `retry_queue` first (priority), then `upload_queue`
  - This ensures retries don't starve behind new batches but also don't block them
  - Implementation: workers alternate or use `PriorityQueue`

### 4.4 What Triggers Retry
| Condition | Retry? | Notes |
|-----------|--------|-------|
| cloudcp rc != 0 (transient S3 error) | YES | Up to max_retries |
| cloudcp rc != 0 (permission denied) | NO | Permanent failure |
| Checksum mismatch after upload | YES | Re-upload, re-check |
| xattr set fails (NFS error) | NO | Log warning, continue |
| cloudcp process killed/timeout | YES | Rebuild batch, retry |
| Partial batch (some succeeded, some failed) | YES | Only retry failed files |
| All files in batch failed | YES | Retry whole batch |
| AWS fallback (`aws s3 cp`) succeeds | NO | Already handled |

### 4.5 Tracking Retry State
```python
@dataclass
class FileRetryState:
    path: str
    s3_path: str
    attempts: int = 0
    last_error: str = ""
    last_attempt_time: float = 0.0
```

Stored in-memory dict keyed by source path. Persisted to a JSON manifest file per transfer for crash recovery.

---

## 5. Configuration

### New Config Keys (in `/etc/bryck/bryckcloud/config.json`)
```json
{
  "AWS_PIPELINE_MODE": "pipelined",
  "UPLOAD_WORKERS": 12,
  "PREPROCESS_WORKERS": 4,
  "POSTPROCESS_WORKERS": 4,
  "PIPELINE_BATCH_SIZE": 500,
  "RETRY_MAX_ATTEMPTS": 3,
  "RETRY_BACKOFF_BASE": 1.0,
  "RETRY_BACKOFF_MAX": 30.0,
  "DB_UPDATE_INTERVAL": 5,
  "STATS_BATCH_SIZE": 100
}
```

### Backward Compatibility
- `"AWS_PIPELINE_MODE": "legacy"` — uses existing `bcloud_src_enum | parallel` shell pipeline
- `"AWS_PIPELINE_MODE": "pipelined"` — uses new orchestrator
- Default: `"legacy"` until validated in production

---

## 6. Recommended Tuning for 2.5M × 1MB Files

```json
{
  "AWS_PIPELINE_MODE": "pipelined",
  "UPLOAD_WORKERS": 12,
  "PREPROCESS_WORKERS": 4,
  "POSTPROCESS_WORKERS": 4,
  "PIPELINE_BATCH_SIZE": 500,
  "TM_THREAD_POOL_SIZE": 32,
  "CHUNK_SIZE_MB": 8,
  "TRANSFER_CLIENT_TYPE": "crt",
  "RETRY_MAX_ATTEMPTS": 3
}
```

**Rationale:**
- 12 cloudcp processes × 500 files × 1MB = 6GB in flight per wave
- Pipeline keeps all 12 slots continuously busy (no gaps)
- 32 internal threads per cloudcp = good connection reuse
- 8MB chunks for 1MB files avoids buffer waste
- CRT client is event-driven (faster than TransferManager for high concurrency)

---

## 7. Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `bryckcloud/lib/cloud/transfer_pipeline.py` | **NEW** | Main orchestrator: PipelinedTransfer class |
| `bryckcloud/lib/cloud/retry_manager.py` | **NEW** | RetryPolicy, retry queue, backoff logic |
| `bryckcloud/lib/cloud/aws.py` | MODIFY | `transfer()` uses PipelinedTransfer when mode="pipelined" |
| `bryckcloud/lib/cloud/aws_transfer.py` | MODIFY | Extract batch_pre/postprocess for reuse; reduce global state |
| `build_scripts/bryckcloud_config.template` | MODIFY | Add new config keys |

---

## 8. Error Handling & Edge Cases

### 8.1 Crash Recovery
- On startup, check for incomplete transfer state files in `BATCH_FILE_DIR/transfer_<id>/`
- If retry manifest exists, resume from last known state
- If no manifest, re-enumerate and check xattrs (existing resume behavior)

### 8.2 Cancellation
- SIGTERM → set `shutdown_event`, drain in-flight uploads, skip new batches
- DB state set to "CANCELLED" or "PAUSED" depending on signal source
- Retry manifest saved for resume

### 8.3 Backpressure
- If postprocessor falls behind → `postprocess_queue` fills → uploaders block on `put()`
- This naturally throttles uploads to match post-processing capacity
- Prevents unbounded temp file accumulation

### 8.4 DB Update Batching
- Current code updates DB per-file — too much contention with 12 parallel workers
- Batch DB updates: accumulate bytes/counts, flush every `DB_UPDATE_INTERVAL` seconds or `STATS_BATCH_SIZE` files
- Use a single stats-writer thread with a timer

---

## 9. Metrics & Observability

Track per-stage:
- Queue depths (enum, upload, postprocess, retry)
- Stage throughput (items/sec, bytes/sec)
- Latency per stage (preprocess_ms, upload_ms, postprocess_ms)
- Retry counts by error type
- Active workers per pool

Log format:
```
[pipeline] stage=upload worker=3 batch=000142 files=500 bytes=524288000 duration_ms=1420 throughput_mbps=352
[pipeline] stage=retry batch=000142 files=12 attempt=2 reason=SlowDown
```

---

## 10. Open Questions / Discussion Points

1. **Should preprocess skip-checks be optional?** For fresh transfers (no resume), skipping xattr checks saves NFS round-trips. Add config: `"SKIP_XATTR_PRECHECK": "True"` for first-time transfers.

2. **Retry queue vs inline retry?** Current design uses a separate retry queue. Alternative: each upload worker retries inline N times before reporting failure. Tradeoff: inline retry is simpler but holds the upload slot longer.

3. **Should postprocessor set xattrs at all?** If verification (bryckck) is the source of truth, xattr-based resume may be redundant. Could replace with a manifest file per transfer.

4. **How to handle partial cloudcp output?** If cloudcp crashes mid-batch, only some files have output lines. The merge logic handles this, but retry needs to identify the "gap" files.

5. **Memory footprint for retry tracking?** With 2.5M files, if 5% fail = 125K retry entries × ~200 bytes = 25MB. Acceptable in-memory. For larger datasets, may need disk-based tracking.

6. **AWS fallback (`aws s3 cp`) in retry?** Current code has `AWS_CP_FALLBACK` that falls back to `aws s3 cp` on cloudcp failure. Should this be attempt 1 = cloudcp, attempt 2 = aws cli, attempt 3 = cloudcp again? Or always cloudcp for retry?
