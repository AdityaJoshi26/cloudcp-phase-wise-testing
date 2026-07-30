# BryckCloud Configuration Reference

Configuration file: `/etc/bryck/bryckcloud/config.json`

Both flat and nested formats are supported. Flat keys always take priority over nested keys for backward compatibility.

---

## SERVICE

Service daemon settings.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PID_FILE` | string | `/run/bryck/bcloud_transfer.pid` | PID file for the transfer daemon |
| `AUTH_KEY` | string | `bryck` | Authentication key for the queue manager |
| `QUEUE_PORT` | int | `2002` | Port for the transfer queue listener |
| `ADDRESS` | string | `127.0.0.1` | Bind address for the queue listener |
| `QUEUE_NAME` | string | `get_queue` | Queue name for transfer requests |
| `MAX_CONCURRENT_TRANSFERS` | int | `5` | Number of concurrent transfer threads (alias: `THREAD_COUNT`) |
| `MAX_TRANSFER_SIZE` | int | `1000000000` | Max bytes per transfer thread (alias: `THREAD_SIZE`) |
| `USER` | string | `bryck` | System user for file operations |

## DATABASE

PostgreSQL connection settings.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `DB_USERNAME` | string | `bryck` | Database user |
| `DB_PASSWORD` | string | — | Database password |
| `DB_NAME` | string | `Bcloud` | Database name |
| `DB_PORT` | int | `5432` | Database port |

## CLOUD

Cloud provider connection settings.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `LOCAL_AWS` | string | `""` | Custom S3-compatible endpoint URL. Leave empty for AWS S3. Example: `https://10.10.10.103:9000` for MinIO |
| `PROVIDER` | string | — | Cloud provider type (e.g., `minio`, `aws`) |
| `AWS_CONFIG_FILE` | string | `/home/bryck/.aws/config` | Path to the AWS CLI/SDK config file. Contains credentials, region, and role assumption settings |

## TRANSFER

Transfer engine settings controlling cloudcp behavior and parallelism.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `PARALLEL_TRANSFER` | string | `"False"` | Enable parallel batch transfers (`"True"` to enable). When enabled, uses `parallel -j PARALLEL_WORKERS` for concurrent batch uploads (alias: `AWS_PARALLEL`) |
| `PARALLEL_WORKERS` | int | `1` | Number of parallel cloudcp worker processes. Each runs one batch at a time. Recommended: 32 for high-throughput (i7ie.18xlarge with 75Gbps) (alias: `AWS_THREAD`) |
| `TRANSFER_CMD` | string | `aws s3 cp` | Transfer binary command. For cloudcp: `export LD_LIBRARY_PATH=/opt/bryck/aws/lib/; /opt/bryck/aws/bin/cloudcp` (alias: `AWS_CMD`) |
| `FALLBACK_ENABLED` | string | `"True"` | When `"True"`, files that fail cloudcp upload are retried via **boto3** (ARN/assumed-role aware, HeadObject-verified) through the global fallback worker (alias: `AWS_CP_FALLBACK`). Tune via the `FALLBACK` sub-object (see below). |
| `TRANSFER_CLIENT_TYPE` | string | `transfermanager` | Transfer client type used by cloudcp |
| `TM_THREAD_POOL_SIZE` | int | `16` | Default concurrency ceiling for the fallback worker (used when `FALLBACK.max_processes`/`threads_per_process` are unset) and cloudcp's TransferManager. |
| `CHUNK_SIZE_MB` | int | `64` | Multipart upload chunk size in MB. Passed to cloudcp and aws CLI |
| `HI_PERF_OPT` | string | `"True"` | Enable high-performance optimizations in cloudcp |
| `PERF_STATS` | string | `"True"` | Log per-batch timing stats (preprocess, upload, postprocess). Set to `"False"` to disable for reduced log I/O with many small files |
| `TXR_BATCH_VERIFYSIZE` | string | `"True"` | Verify file sizes during batch verification via cloudcp (alias: `BATCH_VERIFY_SIZE`) |
| `AZURE_RESUME` | string | `"False"` | Enable resume for Azure transfers |

## BATCH

Batch creation and file grouping settings. Controls how files are grouped into batches for parallel upload.

### Top-level batch settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `BATCH_FILE_DIR` | string | `/bryck/bcloud_batchmeta` | Directory for batch metadata: per-transfer `transfer_<id>/` holding batch-state dirs and the `_fallback_done` marker |

### Per-tier batch tuning

Files are classified into 5 tiers based on size. Each tier has independent tuning for optimal throughput.

#### File size classification

| Tier | File Size Range | Use Case |
|------|----------------|----------|
| **ZERO** | 0 bytes (empty files) | Hidden files, markers, placeholders |
| **TINY** | 1 byte – 1 MB | Small config files, logs, metadata |
| **SMALL** | 1 MB – 64 MB | Typical data files, documents |
| **MEDIUM** | 64 MB – 1 GB | Large data files, archives |
| **LARGE** | 1 GB+ | Database dumps, disk images, media |

#### Batch size (max files per batch)

Controls the maximum number of files grouped into a single batch for one cloudcp invocation.

| Key | Flat Key | Default | Guidance |
|-----|----------|---------|----------|
| `BATCH.ZERO.BATCH_SIZE` | `ZEROFILE_BATCH_SIZE` | `2000` | High count OK — zero-sized files have no transfer overhead |
| `BATCH.TINY.BATCH_SIZE` | `TINYFILE_BATCH_SIZE` | `511` | Increase to 1000+ for 2M+ tiny file workloads to reduce batch overhead |
| `BATCH.SMALL.BATCH_SIZE` | `SMALLFILE_BATCH_SIZE` | `317` | Balance between batch overhead and memory usage |
| `BATCH.MEDIUM.BATCH_SIZE` | `MEDIUMFILE_BATCH_SIZE` | `50` | Fewer files per batch — each file takes longer to upload |
| `BATCH.LARGE.BATCH_SIZE` | `LARGEFILE_BATCH_SIZE` | `5` | Keep low — each file may take minutes to upload |

#### Target batch size (MB)

Target total size of all files in a batch. A batch closes when either `BATCH_SIZE` or `TARGET_SIZE_MB` is reached, whichever comes first.

| Key | Flat Key | Default | Description |
|-----|----------|---------|-------------|
| `BATCH.ZERO.TARGET_SIZE_MB` | `ZEROFILE_TARGET_SIZE_MB` | `0` | Zero — empty files have no size |
| `BATCH.TINY.TARGET_SIZE_MB` | `TINYFILE_TARGET_SIZE_MB` | `256` | 256 MB per batch |
| `BATCH.SMALL.TARGET_SIZE_MB` | `SMALLFILE_TARGET_SIZE_MB` | `2048` | 2 GB per batch |
| `BATCH.MEDIUM.TARGET_SIZE_MB` | `MEDIUMFILE_TARGET_SIZE_MB` | `10240` | 10 GB per batch |
| `BATCH.LARGE.TARGET_SIZE_MB` | `LARGEFILE_TARGET_SIZE_MB` | `51200` | 50 GB per batch |

#### Open batches (round-robin concurrency)

Number of open batch slots per tier. Files are distributed round-robin across open batches to improve parallelism. Higher values distribute files more evenly but delay batch readiness.

| Key | Flat Key | Default | Guidance |
|-----|----------|---------|----------|
| `BATCH.ZERO.OPEN_BATCHES` | `ZEROFILE_OPEN_BATCHES` | `4` | Lower — zero files accumulate fast |
| `BATCH.TINY.OPEN_BATCHES` | `TINYFILE_OPEN_BATCHES` | `8` | Match `AWS_THREAD` for best parallelism |
| `BATCH.SMALL.OPEN_BATCHES` | `SMALLFILE_OPEN_BATCHES` | `8` | Match `AWS_THREAD` for best parallelism |
| `BATCH.MEDIUM.OPEN_BATCHES` | `MEDIUMFILE_OPEN_BATCHES` | `8` | Match `AWS_THREAD` for best parallelism |
| `BATCH.LARGE.OPEN_BATCHES` | `LARGEFILE_OPEN_BATCHES` | `8` | Match `AWS_THREAD` for best parallelism |

### BatchBuilder (directory scanner / resume)

New settings for the standalone/integrated BatchBuilder (`batch_builder.py`). All existing
per-tier keys above are honored **unchanged** (flat and nested both work); these are additive.
Batches carry **absolute paths** and are NUL-framed, so special characters in filenames
(Latin-1 bytes, embedded newlines, trailing spaces, CR) are preserved byte-for-byte.

| Key | Flat Key | Default | Description |
|-----|----------|---------|-------------|
| `BATCH.CHECKPOINT_EVERY_FILES` | `CHECKPOINT_EVERY_FILES` | `100000` | Files between durable resume checkpoints. On checkpoint (and on SIGINT/SIGTERM) all open batches flush, `source.index` + frontier journal fsync, and the manifest is rewritten. Lower = tighter crash bound, more short batches. |
| `BATCH.SCAN_FOLLOW_SYMLINKS` | `SCAN_FOLLOW_SYMLINKS` | `"False"` | When `"True"`, follow symlinked directories/files during the directory scan. Default skips symlinks (logged, not followed) to avoid cycles and duplicate uploads. |

Resume model (no xattr): the scanner journals the directory frontier in `scan.discovered` /
`scan.completed` (resume set = discovered − completed) and truncates `source.index` back to its
last checkpoint on restart. Batch-level resume uses `batches.created` − `batches.completed`.
Unreadable directories are logged to `scan_errors.log` and skipped; the scan continues.

## LOGGING

Log file and transfer statistics settings.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `LOGS_DIR` | string | `/opt/bryck/bryckapi/downloads/cloud_transfer_logs` | Base directory for transfer logs. Per-transfer logs go to `<LOGS_DIR>/cloud_transfer_<id>/` |
| `DEBUG_LOG_FILE` | string | `<LOGS_DIR>/cloudcp.log` | Debug log file for cloudcp output |
| `AWS_XFER_STAT` | string | `/tmp/aws_xfer_stats` | Transfer statistics enable flag file |
| `AWS_STAT_PREFIX` | string | `/tmp/aws_bryck_zfer_stat` | Prefix for per-transfer stat JSON files (FileLock-based counters) |

## UPLOAD REPORT & RESUME

The upload report is the durable, **xattr-free source of truth** for resume, the boto3
fallback, and the final report (see `docs/bcloud_redesign_proposal.md` §6.2). During upload the
Python layer records every verified object; on restart, enumeration skips files already present
in the report instead of probing a per-file xattr.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `MIN_FREE_PCT` | float | `10.0` | Minimum percent free space required on the batch-metadata volume. Enumeration refuses to start below this (preflight) and pauses with a no-space error (exit code 2) if a batch-file write hits `ENOSPC` (challenges #17/#18). |

**On-disk layout** (under `<LOGS_DIR>/cloud_transfer_<id>/report/`), sharded per writer process
to avoid lock contention:

| File | Purpose |
|------|---------|
| `upload_report.<pid>.csv` | One row per verified upload: `local_path,s3path,size,etag,status,attempt,finished_at`. `status ∈ {SUCCESS, FALLBACK_OK, SKIPPED}`. CSV-quoted + surrogateescape so embedded newlines / trailing spaces / CR / non-UTF-8 bytes survive byte-for-byte. Source of truth for resume and the final report. |
| `error.<pid>.log` | Human-readable log of every transient/permanent error, with context. |
| `failed_uploads.<pid>` | Machine-readable, NUL-framed terminal failures (`local_path \0 s3path \0 size \0 last_error \0`). Written by the boto3 fallback for files that exhausted their retries. Feeds the report's failure section. |

**cloudcp-owned outputs** (redesign): cloudcp consumes the raw NUL-framed batch directly and writes
its own three files under `<LOGS_DIR>/cloud_transfer_<id>/` — Python never pre- or post-processes the
batch text:

| File | Purpose |
|------|---------|
| `transfer_report_<id>.csv` | cloudcp's success report, same columns as the shards above (`SUCCESS`/`SKIPPED` rows). Merged as just another report source for resume-skip and the final report. |
| `cloudcp_failed.log` | cloudcp's human-readable diagnostic log of upload failures. |
| `cloudcp_retry_<id>_<batch_stem>.lst` | Per-batch NUL-framed retry list (`local_path \0 s3path \0 size \0 last_error \0`) for objects cloudcp could not upload. Written atomically (temp→rename); its appearance is the "ready" signal to the fallback. Renamed `.lst.done` once the fallback has drained it. |

Resume model: enumeration loads the union of all `upload_report.*.csv` shards once and skips any
source path already recorded terminal-success — O(reported files), no xattr, no writable-source
requirement (challenges #1/#14/#16). Before starting, a resume preflight verifies the report
directory is creatable and writable; if not, the transfer refuses to start (challenge #1).

### Enumeration & batch-level resume state

The per-transfer batch-meta directory (`<BATCH_FILE_DIR>/transfer_<id>/`) also holds the durable
state that makes **both** resume cases fast and correct (challenges #14/#15):

| Path | Purpose |
|------|---------|
| `manifest.json` | `scan_state` (`in_progress`/`complete`), `seq_high_water` (next batch id — never reused across resume), and cumulative `total_files`/`total_bytes`/`dirs_done`. Written atomically (tmp→fsync→rename). |
| `scan.discovered` / `scan.completed` | NUL-framed directory frontier journals. On resume the walk continues from `discovered − completed` instead of re-walking the whole tree. |
| `batches/pending/<name>` | Published batch, not yet claimed by a transfer worker. |
| `batches/inprogress/<name>` | Claimed by a worker (atomic rename from `pending/`). |
| `batches/completed/<name>` | Fully processed (atomic rename from `inprogress/`). |

A batch's **directory is its state**; transitions are atomic `os.rename`s, so a crash never leaves
a batch half-moved. Batch files hold absolute paths **NUL-framed** (`path \0`), written binary with
`os.fsencode`, so paths containing newlines, CR, TAB, trailing spaces, or non-UTF-8 (Latin-1) bytes
survive byte-for-byte — this is exactly the raw stream cloudcp consumes (redesign §4).

**Two resume cases, handled explicitly:**

1. **Enumeration interrupted** (`scan_state = in_progress`): the enumerator re-dispatches
   already-published `pending`+`inprogress` batches (O(batches), no walk), then **resumes the walk
   from the frontier journal**, publishing new batches. Checkpoints fire every
   `CHECKPOINT_EVERY_FILES` files and on SIGINT/SIGTERM (clean stop, exit 130).
2. **Enumeration finished** (`scan_state = complete`): the tree walk is **skipped entirely** — the
   enumerator only re-dispatches `pending`+`inprogress` batches. Big win at 300M-file scale.

In both cases a re-run of a partially-done batch re-uploads only its unfinished files (cloudcp/the
report skip files already recorded terminal-success). The final `final_report.csv`
(`AbsoluteFilePath,S3Path,FileSize,ETag`) is merged directly from the upload-report shards at
finalize — no full-bucket LIST (challenge #8).

**Batch completion & the fallback (design §11):** a batch is marked `completed` only when it is
truly drained — *cloudcp ran AND the fallback drained that batch's failed-set*. The handoff is
**socket-free and file-driven**: on partial failure cloudcp writes a durable per-batch retry list
`cloudcp_retry_<id>_<batch_stem>.lst` (atomic temp→rename) into the transfer log dir. The transfer
worker reads only cloudcp's exit code:

- **0** (all objects ok) → the worker completes the batch inline.
- **2** (partial) / **1 with a `.lst`** (all failed) → the worker **defers**: it leaves the batch in
  `inprogress` and lets the fallback own completion. The fallback globs the log dir for pending
  `.lst` files, retries each record via boto3, and once a batch's entire failed-set is drained it
  performs the `inprogress → completed` rename itself and renames the list `.lst.done`.
- **1 fatal, no `.lst`** (cloudcp could not run the batch) → the worker leaves it in `inprogress` so
  a resume re-dispatches it.

If the fallback is disabled or no `.lst` exists, the worker completes the batch inline (the failures
are terminal and already recorded by cloudcp). Because the `.lst` is durable, a crash of either
process is recovered on resume: the fallback simply re-globs the un-retired `.lst` files.

There is **no socket** and no worker↔fallback wire protocol anymore. aws.py launches the fallback
worker with `--transfer-dir <BATCH_FILE_DIR>/transfer_<id>`; after the enumerate|cloudcp pipeline
finishes it drops a `_fallback_done` marker file in that directory, and the worker exits once the
marker is present and every `.lst` has been drained. The worker polls for new `.lst` files every
`poll_interval_sec` (see `FALLBACK` below).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `CHECKPOINT_EVERY_FILES` | int | `100000` | Files between durable scan checkpoints (manifest + frontier fsync). Lower = faster resume granularity, more fsync overhead. |

### FALLBACK (boto3 retry worker)

Optional `FALLBACK` config sub-object tuning the global boto3 fallback worker. All keys are optional;
when absent, the worker falls back to `TM_THREAD_POOL_SIZE` for its concurrency ceiling and uses the
defaults below. The fallback uses the **boto3** client stack (ARN/assumed-role aware) rather than
cloudcp, verifies each object with HeadObject (size match), and records successes to the upload
report with status `FALLBACK_OK`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `min_processes` | int | `1` | Floor for dynamic concurrency (multiplied by `threads_per_process`). Concurrency scales with the live backlog between this floor and the ceiling. |
| `max_processes` | int | `TM_THREAD_POOL_SIZE`-derived | Ceiling for dynamic concurrency (`max_processes × threads_per_process`). If unset, the ceiling is `TM_THREAD_POOL_SIZE`. |
| `threads_per_process` | int | — | Multiplier applied to `min/max_processes` to derive concurrency. |
| `max_attempts` | int | `3` | Per-file boto3 retry attempts before a terminal failure. |
| `backoff_base_sec` | float | `1.0` | Base delay for exponential backoff between retries. |
| `backoff_max_sec` | float | `30.0` | Cap on the backoff delay. |
| `poll_interval_sec` | float | `1.0` | How often the worker re-globs the log dir for new `cloudcp_retry_*.lst` files (socket-free handoff). |

Example:

```json
"FALLBACK": {
    "min_processes": 1,
    "max_processes": 4,
    "threads_per_process": 8,
    "max_attempts": 5,
    "backoff_base_sec": 1.0,
    "backoff_max_sec": 30.0
}
```

## VERIFICATION

Post-transfer verification and report settings. Controls the bryckck parallel S3 enumerator and report generation.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `REPORT_FORMAT` | string | `"csv"` | Final report format: `"csv"` or `"json"`. Report includes AbsoluteFilePath, S3Path, FileSize, ETag |
| `TRANSFER_SUMMARY_FILES` | string | `/etc/bryck/bryckcloud/transfer_summary_files.json` | JSON file listing which files to include in the transfer summary zip |
| `VERIFY_S3_WORKERS` | int | `16` | Parallel S3 listing workers for verification. Each worker uses its own boto3 client and lists a different prefix subtree via BFS. Higher values improve listing speed for buckets with many prefixes (e.g. 25M objects). |
| `VERIFY_STAT_THREADS` | int | `32` | Parallel stat workers for local filesystem enumeration during verification. Tunable for NFS sources where stat latency dominates. |

## RCLONE_PARAMS

Rclone transfer parameters (used when rclone is the transfer backend).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `S3_UPLOAD_CONCURRENCY` | int | `16` | Concurrent S3 upload streams |
| `TRANSFERS` | int | `100` | Number of concurrent file transfers |
| `MULTI_THREAD_STREAMS` | int | `100` | Multi-thread download streams |
| `upload_cutoff` | string | `64Mi` | File size threshold for multipart upload |
| `chunk_size` | string | `64Mi` | Multipart upload chunk size |

---

## Example: Nested config format

```json
{
  "SERVICE": {
    "PID_FILE": "/run/bryck/bcloud_transfer.pid",
    "AUTH_KEY": "bryck",
    "QUEUE_PORT": 2002,
    "ADDRESS": "127.0.0.1",
    "QUEUE_NAME": "get_queue",
    "MAX_CONCURRENT_TRANSFERS": 5,
    "MAX_TRANSFER_SIZE": 1000000000,
    "USER": "bryck"
  },
  "DATABASE": {
    "DB_USERNAME": "bryck",
    "DB_PASSWORD": "password",
    "DB_NAME": "Bcloud",
    "DB_PORT": 5432
  },
  "CLOUD": {
    "LOCAL_AWS": "",
    "PROVIDER": "aws",
    "AWS_CONFIG_FILE": "/home/bryck/.aws/config"
  },
  "TRANSFER": {
    "PARALLEL_TRANSFER": "True",
    "PARALLEL_WORKERS": 32,
    "TRANSFER_CMD": "export LD_LIBRARY_PATH=/opt/bryck/aws/lib/; /opt/bryck/aws/bin/cloudcp",
    "FALLBACK_ENABLED": "True",
    "TRANSFER_CLIENT_TYPE": "transfermanager",
    "TM_THREAD_POOL_SIZE": 32,
    "CHUNK_SIZE_MB": 64,
    "HI_PERF_OPT": "True",
    "PERF_STATS": "True",
    "TXR_BATCH_VERIFYSIZE": "True"
  },
  "BATCH": {
    "BATCH_FILE_DIR": "/opt/bryck/bryckapi/downloads/bcloud_batchmeta",
    "ZERO":   { "BATCH_SIZE": 2000, "TARGET_SIZE_MB": 0,     "OPEN_BATCHES": 4 },
    "TINY":   { "BATCH_SIZE": 511,  "TARGET_SIZE_MB": 256,   "OPEN_BATCHES": 8 },
    "SMALL":  { "BATCH_SIZE": 317,  "TARGET_SIZE_MB": 2048,  "OPEN_BATCHES": 8 },
    "MEDIUM": { "BATCH_SIZE": 50,   "TARGET_SIZE_MB": 10240, "OPEN_BATCHES": 8 },
    "LARGE":  { "BATCH_SIZE": 5,    "TARGET_SIZE_MB": 51200, "OPEN_BATCHES": 8 }
  },
  "LOGGING": {
    "LOGS_DIR": "/opt/bryck/bryckapi/downloads/cloud_transfer_logs",
    "DEBUG_LOG_FILE": "/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloudcp.log",
    "AWS_XFER_STAT": "/tmp/aws_xfer_stats",
    "AWS_STAT_PREFIX": "/tmp/aws_bryck_zfer_stat"
  },
  "VERIFICATION": {
    "REPORT_FORMAT": "json",
    "TRANSFER_SUMMARY_FILES": "/etc/bryck/bryckcloud/transfer_summary_files.json",
    "VERIFY_S3_WORKERS": 16,
    "VERIFY_STAT_THREADS": 32
  },
  "RCLONE_PARAMS": {
    "S3_UPLOAD_CONCURRENCY": 16,
    "TRANSFERS": 100,
    "MULTI_THREAD_STREAMS": 100,
    "upload_cutoff": "64Mi",
    "chunk_size": "64Mi"
  }
}
```

## Example: Flat config format (backward compatible)

```json
{
  "PID_FILE": "/run/bryck/bcloud_transfer.pid",
  "AUTH_KEY": "bryck",
  "QUEUE_PORT": 2002,
  "ADDRESS": "127.0.0.1",
  "LOCAL_AWS": "https://10.10.10.103:9000",
  "PROVIDER": "minio",
  "THREAD_COUNT": 5,
  "THREAD_SIZE": 1000000000,
  "QUEUE_NAME": "get_queue",
  "AWS_PARALLEL": "True",
  "AWS_THREAD": 32,
  "HI_PERF_OPT": "True",
  "BATCH_VERIFY_SIZE": "True",
  "BATCH_SIZE": 79,
  "CHUNK_SIZE_MB": 64,
  "TM_THREAD_POOL_SIZE": 32,
  "TRANSFER_CLIENT_TYPE": "transfermanager",
  "BATCH_FILE_DIR": "/opt/bryck/bryckapi/downloads/bcloud_batchmeta",
  "AWS_CONFIG_FILE": "/home/bryck/.aws/config",
  "AWS_CMD": "export LD_LIBRARY_PATH=/opt/bryck/aws/lib/; /opt/bryck/aws/bin/cloudcp",
  "AWS_CP_FALLBACK": "True",
  "AWS_XFER_STAT": "/tmp/aws_xfer_stats",
  "AWS_STAT_PREFIX": "/tmp/aws_bryck_zfer_stat",
  "AZURE_RESUME": "False",
  "LOGS_DIR": "/opt/bryck/bryckapi/downloads/cloud_transfer_logs",
  "USER": "bryck",
  "DB_USERNAME": "bryck",
  "DB_PASSWORD": "password",
  "DB_NAME": "Bcloud",
  "DB_PORT": 5432,
  "TRANSFER_SUMMARY_FILES": "/etc/bryck/bryckcloud/transfer_summary_files.json",
  "DEBUG_LOG_FILE": "/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloudcp.log",
  "REPORT_FORMAT": "json",
  "PERF_STATS": "True",
  "RCLONE_PARAMS": {
    "S3_UPLOAD_CONCURRENCY": 16,
    "TRANSFERS": 100,
    "MULTI_THREAD_STREAMS": 100,
    "upload_cutoff": "64Mi",
    "chunk_size": "64Mi"
  }
}
```

## Tuning guide: High-throughput 2M+ small files

For workloads with millions of 1MB files on i7ie.18xlarge (75Gbps network):

```json
{
  "TRANSFER": {
    "PARALLEL_WORKERS": 32,
    "TM_THREAD_POOL_SIZE": 32,
    "PERF_STATS": "False"
  },
  "BATCH": {
    "TINY": { "BATCH_SIZE": 1000, "TARGET_SIZE_MB": 512, "OPEN_BATCHES": 16 },
    "SMALL": { "BATCH_SIZE": 500, "TARGET_SIZE_MB": 4096, "OPEN_BATCHES": 16 }
  }
}
```

Key rationale:
- **TINYFILE_BATCH_SIZE=1000**: Packs more files per cloudcp invocation, reducing process startup overhead
- **OPEN_BATCHES=16**: Better distribution when PARALLEL_WORKERS=32, ensures batches fill faster
- **PERF_STATS=False**: Eliminates per-batch timing log I/O (4000+ batches × 32 workers)

---

## Deprecated Key Aliases

The following keys are deprecated but still fully supported. Both old and new names work interchangeably — the normalizer resolves them bidirectionally.

| Deprecated Key | Preferred Key | Notes |
|---------------|---------------|-------|
| `AWS_PARALLEL` | `PARALLEL_TRANSFER` | Enable/disable parallel batch uploads |
| `AWS_THREAD` | `PARALLEL_WORKERS` | Number of parallel cloudcp processes |
| `AWS_CMD` | `TRANSFER_CMD` | Transfer binary command |
| `AWS_CP_FALLBACK` | `FALLBACK_ENABLED` | Enable fallback retry on failure |
| `THREAD_COUNT` | `MAX_CONCURRENT_TRANSFERS` | Concurrent transfer slots |
| `THREAD_SIZE` | `MAX_TRANSFER_SIZE` | Max bytes per transfer slot |
| `BATCH_VERIFY_SIZE` | `TXR_BATCH_VERIFYSIZE` | Verify file sizes during batch verification (moved from BATCH to TRANSFER) |
| `BATCH_SIZE` | *(removed)* | Use per-tier `BATCH.TINY.BATCH_SIZE` etc. instead |
