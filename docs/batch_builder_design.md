# BatchBuilder — Algorithm, Design, Throttles & Behaviour

Status: design spec (v1). Scope: the BatchBuilder component only.
Companion docs: `bcloud_redesign_proposal.md` (whole service), `bcloud_redesign_tasklist.md`
(tasks B1–B12, D1–D6). Reference implementation: `batch_builder.py` (CSV-input mode, working).
Reference patterns: `s3_list_bucket_fast.py` (download lister), `transfer_mp.py` (fallback pool).

---

## 1. Purpose & scope

BatchBuilder is the front of the transfer pipeline. It turns a **source** (a local directory
tree for upload, or an S3 bucket for download) into:

1. a **complete source index** — every entry + size — which is the *verification source of
   truth* (replaces live bucket listing), and
2. a set of **size-bucketed, right-sized batch files** the orchestrator hands to cloudcp.

It also owns the **resume contract**: stop/resume of both the scan and the transfer must work
at 200M+ objects and 13–14+ directory levels, **without any xattr**, and must preserve
filenames byte-for-byte (Latin-1, embedded `\n`, trailing spaces, CR).

**Non-goals:** uploading/downloading bytes (cloudcp), retrying failures (fallback worker),
producing the final report (verification). BatchBuilder only *produces* the inputs those
stages consume.

---

## 2. Design principles

| # | Principle | Why |
|---|---|---|
| P1 | **Bytes, never strings.** Paths are raw `os.fsencode` bytes end-to-end; never `.strip()`, never split on `\n`. | Filenames legitimately contain Latin-1 bytes (0xE9), `\n`, trailing space, CR. Any decode/strip corrupts the key and breaks resume. (bugs #9–12) |
| P2 | **On-disk state is authoritative; memory is a cache.** Resume is reconstructed from append-only logs + atomic renames, not from in-RAM structures. | A multi-hour scan can crash/pause at any point; the frontier is far too large to snapshot. |
| P3 | **No xattr.** Resume durability = batch directory-state + append-only batch logs + the tx-log skip set. | xattr failed on read-only files and forced a full tree re-walk on restart. (bugs #1, #14, #15) |
| P4 | **Right-size per bucket.** A batch is homogeneous in size-class so its duration is predictable and the scheduler can balance the pipe. | Mixed batches make scheduling and fallback accounting unpredictable. (bug #2) |
| P5 | **Atomic publish.** Every durable artifact is written `*.tmp` → `fsync` → `rename`. Readers only ever see complete files. | Crash during a flush must never expose a half-written batch/index. |
| P6 | **Fail fast on space.** Preflight requires ≥10% free; runtime pauses cleanly below threshold. Never crash, never half-write. | (bugs #17, #18) |
| P7 | **Scan completeness is a barrier.** `scan_state=complete` is the single fact that lets verification compute "missing" safely. | Prevents false "missing files" reports mid-scan. |

---

## 3. On-disk layout (per transfer)

```
{batchmeta}/transfer_<id>/
  manifest.json          # source_prefix, bucket_prefix, path_mode=absolute, bucket, bucket config,
                         # network profile, scan_state, seq_high_water, schema_version, totals
  scan.discovered        # append-only: every dir/prefix ever enqueued        (scan resume)
  scan.completed         # append-only: every dir/prefix fully listed         (scan resume)
  source.index           # NUL-framed: abspath \0 size \0 mtime \0  (verification src-of-truth)
  source.index.part-<p>  # per-walker shards, concatenated into source.index at completion
  batches/
    pending/    <bucket>_<seq>.batch     # ready to be claimed by the orchestrator
    inflight/   <bucket>_<seq>.batch     # claimed, cloudcp running (orchestrator-owned)
    fallback_wait/ ...                   # cloudcp done with residual failures → fallback
    processed/  <bucket>_<seq>.batch     # terminal: fully handled
    failed/     <bucket>_<seq>.batch     # terminal: residual permanent failures (audited)
  batches.created        # append-only: name \0 bucket \0 nfiles \0 nbytes \0
  batches.completed      # append-only: name \0 outcome \0        (orchestrator-owned)
  scan_errors.log        # unreadable dirs / stat errors (human-readable)
```

BatchBuilder writes: `manifest.json`, `scan.*`, `source.index*`, `batches/pending/*`,
`batches.created`, `scan_errors.log`. The orchestrator owns the batch directory transitions
after `pending/` and `batches.completed`. (Contract boundary — §14 of the proposal.)

---

## 4. Batch file format — NUL-framed, absolute paths

```
<abspath_bytes> \0 <size_decimal_ascii> \0     (repeated, one record per file)
```

- **Absolute paths, not relative.** Each record stores the full absolute path exactly as it
  exists on the source filesystem. Two prefixes are recorded **once** in `manifest.json` and
  passed to cloudcp, which composes the key itself:
  - `source_prefix` — the source directory prefix cloudcp **strips** from each absolute path to
    get the key suffix.
  - `bucket_prefix` — the S3 key prefix cloudcp **prepends**.
  - `key = bucket_prefix + strip(source_prefix, abspath)`. cloudcp opens the file at the
    absolute path directly (no path reconstruction needed) and derives the key from the prefixes.
- **Why absolute (this is the design change).** The `source.index` and batch files then double
  as a **complete, self-describing file listing** — absolute path + size for every object — with
  no dependency on a separately-remembered root. The end-of-transfer report reads directly off
  these (absolute path, size, and later etag) with nothing to re-join. cloudcp is the single
  owner of key composition, so prefix rules live in exactly one place.
- **Raw bytes** → non-UTF-8 / mixed-encoding names survive untouched. Key normalization to
  valid UTF-8 (or percent-encode) happens **once, at upload time in cloudcp** (§6.3 proposal),
  not here.
- **NUL is the only safe delimiter** — it is the one byte that cannot appear in a POSIX
  filename, so `\n`, space, CR are all just payload.
- `source.index` uses the same framing plus an `mtime` field for verification.

Download batches carry `s3key \0 size \0 [\0 local_abspath]` and are otherwise identical:
cloudcp writes to `local_dir_prefix + strip(bucket_prefix, s3key)`.

---

## 5. Size buckets & scheduling contract (config-driven)

Buckets are resolved **once at transfer start**, before scanning. A file lands in the **first
bucket whose `max` it is `<`** (half-open `[prev_max, max)`; last bucket = ∞).

```json
"BATCH_BUILDER": {
  "size_buckets": [
    { "name": "zero",   "max": "1B"    },
    { "name": "tiny",   "max": "1MB"   },
    { "name": "small",  "max": "100MB" },
    { "name": "medium", "max": "1GB"   },
    { "name": "large",  "max": null    }
  ],
  "per_bucket": {
    "tiny":   { "batch_target_bytes": "256MB", "batch_max_files": 2000, "open_batches": 8 },
    "small":  { "batch_target_bytes": "2GB",   "batch_max_files": 512,  "open_batches": 8 },
    "medium": { "batch_target_bytes": "10GB",  "batch_max_files": 64,   "open_batches": 8 },
    "large":  { "batch_target_bytes": "50GB",  "batch_max_files": 8,    "open_batches": 8 }
  },
  "schedule_profiles": {
    "dt2_100gbe": { "order": ["large","medium","small","tiny"],
                    "weights": { "large": 6, "medium": 4, "small": 3, "tiny": 3 } },
    "wan_lowbw":  { "order": ["tiny","small","medium","large"],
                    "weights": { "tiny": 6, "small": 4, "medium": 3, "large": 3 } }
  },
  "active_profile": "dt2_100gbe"
}
```

**Right-sizing (P4).** A batch closes when **either** `batch_target_bytes` **or**
`batch_max_files` is reached — whichever first. Large-file batches close on *bytes* (few files,
big); tiny-file batches close on *file count* (many files, small). This keeps each batch's
duration predictable regardless of class.

**Weighted concurrent scheduling (bug #2).** BatchBuilder only *labels* batches by bucket; the
orchestrator schedules them. The contract: buckets drain **concurrently**, worker slots
allocated by `weights`. Rationale — tiny files bottleneck on **requests/sec**, large files on
**GB/sec**; running them together saturates both resources instead of leaving one idle.
`order` is only tie-break / spare-slot assignment. (Data point: 165M files <1MB ≈ 40TB vs 40M
files >1MB ≈ 180TB — on 100GbE move large first, on slow WAN clear the request backlog first.)
Prioritization is **static per transfer** (decided at start).

---

## 5A. Batch creation logic — explained from scratch

> This section exists so that someone who has never seen this system can understand **why**
> every knob is the way it is. It builds up from the real-world problem to each decision. No
> prior context required.

### 5A.1 The problem we are actually solving

A single transfer can be **~200 million files totalling ~220 TB**. Crucially, the data is
**bimodal** (two very different populations mixed together):

| Population | Count | Bytes | What dominates the cost |
|---|---:|---:|---|
| Small files (< 1 MB) | ~165 M (82% of files) | ~40 TB (18% of data) | **Number of requests** — one PUT per file, network round-trips, S3 request-rate limits, per-object overhead |
| Large files (≥ 1 MB) | ~40 M (18% of files) | ~180 TB (82% of data) | **Raw bandwidth** — multipart parts pushed at line rate; the file count barely matters |

These two populations do **not** compete for the same resource:

- Moving the 165 M small files is a **requests/second** race. The network pipe is nearly idle;
  the limit is how many HTTP round-trips and metadata operations you can do per second.
- Moving the 180 TB of large files is a **gigabytes/second** race. The request count is tiny;
  the limit is how fast you can push bytes onto the wire.

**The core insight of the whole design:** if you treat all files the same, one population
starves the other. Upload everything in arrival order and you either (a) spend hours doing tiny
files while the 100 GbE pipe sits 95% empty, or (b) fill the pipe with big files while 165 M
small files pile up and finish dead last. We must run **both races at the same time**, each tuned
to its own bottleneck. Everything below — buckets, dual limits, open batches, weights — is a
mechanism to make that happen.

### 5A.2 Step 1 — Size buckets: sort files by which race they belong to

Before any file is read, we define **size buckets** (`zero / tiny / small / medium / large`).
As each file is discovered we drop it into the first bucket whose `max` it is below. That's the
only thing classification does: it labels a file with *which bottleneck it will hit*.

Why buckets at all? Because the right batch shape for a 500-byte file is the opposite of the
right shape for a 40 GB file. You cannot pick one batch size that suits both. Buckets let each
size-class get its own packing rule (next step) and its own share of workers (§5A.5).

Buckets are frozen **once, at transfer start**, so the same file always lands in the same bucket
across restarts — a hard requirement for resume to be consistent.

### 5A.3 Step 2 — Two independent limits per batch, and why we need both

A "batch" is just a list of files handed to one cloudcp invocation. We close the current batch
and start a new one when **either** of two limits is hit — **whichever comes first**:

- **`batch_target_bytes`** — a *byte* ceiling. "Stop once this batch holds ~N bytes of data."
- **`batch_max_files`** — a *count* ceiling. "Stop once this batch holds ~N files."

Why two limits instead of one? Because the two populations blow through different limits:

- **Large-file batches hit the byte limit first.** With `batch_target_bytes = 50 GB` and
  `batch_max_files = 8`, a batch of 40 GB files closes after **1–2 files** (byte limit wins).
  The count limit is just a safety cap. This gives us *small batches of huge files*, so a
  worker is never stuck for hours on one gigantic batch and failures are cheap to retry.
- **Tiny-file batches hit the count limit first.** With `batch_max_files = 2000` and
  `batch_target_bytes = 256 MB`, a batch of 10 KB files closes after **2000 files** (count
  limit wins, ~20 MB). This gives us *batches with lots of files but little data*, which is
  exactly what a requests/second workload wants: one worker chews through thousands of PUTs
  without waiting on bandwidth.

If we used only a byte limit, a tiny-file batch would need **~25 million** 10 KB files to reach
256 MB — an enormous, un-retryable, slow-to-resume unit. If we used only a file-count limit, a
large-file batch of 2000 × 40 GB = **80 TB** would never finish. **Two limits, whichever-first,
is the only rule that keeps every batch a sane, predictable, retryable unit regardless of file
size.** That predictability is what lets the orchestrator estimate durations and balance work.

Recommended defaults and the intent behind each:

| Bucket | `batch_target_bytes` | `batch_max_files` | Which limit usually wins | Resulting batch shape |
|---|---|---|---|---|
| tiny (<1 MB) | 256 MB | 2000 | **files** | many files, ~tens of MB — a request-rate unit |
| small (1–100 MB) | 2 GB | 512 | mixed | balanced |
| medium (0.1–1 GB) | 10 GB | 64 | mixed→bytes | few files, several GB |
| large (≥1 GB) | 50 GB | 8 | **bytes** | 1–8 files, tens of GB — a bandwidth unit |

These are per-bucket in config (`per_bucket.<bucket>.batch_target_bytes / batch_max_files`) so
each class is tuned independently.

### 5A.4 Step 3 — Open batches: keep several batches of every class filling at once

We do **not** fill one batch to completion before starting the next. Instead each bucket keeps
**`open_batches`** (default 8) partially-filled batches open simultaneously, and each incoming
file is round-robined across them (`slot = files_seen % open_batches`).

Two reasons:

1. **The orchestrator needs ready work of every class from the very beginning.** The scan
   discovers files in directory order — you might walk a folder of 100,000 tiny files before you
   ever see a large file. If each bucket had only one open batch, the orchestrator would have
   *tiny* work available for a long time and *no large* work, so the 100 GbE pipe would sit idle
   waiting. With 8 open large-file batches, the moment 8–16 large files have been seen anywhere
   in the tree, complete large batches start getting published and the pipe fills. Open batches
   **decouple discovery order from what's available to schedule.**
2. **Staggered close times → smooth publish rate.** Round-robin spreads files across the open
   batches so they reach their limits at different moments, producing a steady drip of finished
   batches rather than a burst. This keeps worker slots continuously fed.

Cost is bounded: at most `open_batches × num_buckets` batches are held in RAM (≈ 8 × 4 = 32),
each just a list of `(abspath, size)` tuples capped by `batch_max_files`. Memory is O(open
batches), **not** O(total files) — it does not grow with the 200 M-file corpus.

On graceful stop/finish, every open batch is flushed as-is (a short batch is still valid).

### 5A.5 Step 4 — Throttles: matching worker slots to the network (DT2 vs low-bandwidth)

Buckets and batches decide *how work is packaged*. **Throttles decide how many workers each
bucket gets** — and this is where the network profile matters. BatchBuilder itself only *labels*
batches; the orchestrator reads a **schedule profile** and allocates its worker slots by
**weights**. All buckets always drain concurrently (that's the whole point — run both races
together); weights just decide the *share* of workers each gets.

**DT2 network — 100 GbE (`dt2_100gbe`).** Here bandwidth is abundant, so the scarce, valuable
thing to finish is the **180 TB of large files** — they can only move fast when the fat pipe is
saturated. We give large/medium the majority of slots so the pipe stays full, while reserving a
steady minority for tiny/small so the 165 M small files drain *alongside* rather than waiting
until the end.

```json
"dt2_100gbe": {
  "order":   ["large", "medium", "small", "tiny"],   // tie-break / spare-slot preference
  "weights": { "large": 6, "medium": 4, "small": 3, "tiny": 3 }
}
```
Read the weights as shares: of every 16 worker slots, ~6 pull large batches, ~4 medium, ~3
small, ~3 tiny. Most of the machine pushes bytes (saturate 100 GbE), a guaranteed slice keeps
grinding through the small-file request backlog so it isn't left stranded at the finish.

**Low-bandwidth network — WAN (`wan_lowbw`).** Here the pipe is the bottleneck for large files
and there's little bandwidth to spare, so pushing big files first would just clog a thin link
while millions of quick small-file requests — which barely use bandwidth — pile up. We flip the
weights: prioritise clearing the **165 M small-file request backlog** (cheap on a slow link) and
trickle large files through the limited bandwidth.

```json
"wan_lowbw": {
  "order":   ["tiny", "small", "medium", "large"],
  "weights": { "tiny": 6, "small": 4, "medium": 3, "large": 3 }
}
```

**Why weights and not a strict order?** An earlier idea was "do all large, then all tiny"
(strict sequential). That's wrong: it recreates the exact starvation we're trying to avoid —
one resource runs flat out while the other sits idle. Because small and large bottleneck on
*different* resources (requests/sec vs GB/sec), running them **at the same time** uses the host
fully. `weights` is the dial for that simultaneous split; `order` only decides who gets a spare
slot or breaks a tie.

Prioritisation is **static per transfer** — you pick the profile (`active_profile`) at start
based on the link you're on, and it doesn't change mid-run. (Dynamic re-weighting was considered
and deferred to keep behaviour predictable and easy to reason about.)

### 5A.6 Putting it together — a worked example

Config: `dt2_100gbe`, tiny `{256 MB / 2000 files / 8 open}`, large `{50 GB / 8 files / 8 open}`.
The scan discovers, in this order: 500,000 tiny files (10 KB each), then 40 large files (40 GB
each).

1. **Tiny files arrive.** Classified `tiny`, round-robined across 8 open tiny batches. Each
   closes at 2000 files (~20 MB — the *count* limit wins). 500,000 / 2000 = **250 tiny batches**
   published, each a compact request-rate unit.
2. **Large files arrive.** Classified `large`. Each 40 GB file alone exceeds `batch_target_bytes`
   region quickly: batch closes after **1 file** (the 50 GB byte limit means a single 40 GB file
   fills it). 40 files → **40 large batches**, each 40 GB.
3. **Orchestrator schedules.** With weights 6:4:3:3, most slots grab the 40 large batches and
   saturate the 100 GbE pipe (bandwidth race), while ~3/16 of slots simultaneously churn through
   the 250 tiny batches (requests race). Neither waits for the other; both finish far sooner than
   if they'd run one-after-another.
4. **On a slow WAN** you'd instead select `wan_lowbw`: the same 250 tiny batches get the majority
   of slots (they barely use bandwidth, so clear them fast), and the 40 large batches trickle
   through the limited pipe.

Same batches on disk either way — **only the worker-slot weights change with the network.** That
separation (packaging is fixed; scheduling is profile-driven) is what makes the system easy to
tune for a new link: you add a profile, you don't repackage data.

---

## 6. Core algorithm

### 6.1 Lifecycle (top level)

```
start()   → preflight; open index+created logs; create N open batches per bucket
add(rec)  → index-write; classify; route to an open batch; flush-and-rotate if full
finish()  → flush all open batches; finalize index; write manifest(scan_state=complete)
```

### 6.2 `classify(size)` — O(buckets)

```
for (name, max) in size_buckets:      # buckets are pre-resolved to bytes, ascending
    if size < max:  return name
return last_bucket_name                # max == ∞
```

### 6.3 `add(abspath_bytes, size)` — the hot path

```
total_files += 1;  total_bytes += size
index_writer.write(abspath \0 size \0 mtime \0)        # verification source of truth (absolute)

bucket = classify(size)
tun    = tuning[bucket]
slot   = open_batches[bucket][ total_files % len(open_batches[bucket]) ]   # spread load

# rotate BEFORE adding if this record would overflow a non-empty batch
if slot.nfiles > 0 and (slot.nbytes + size > tun.target_bytes
                        or slot.nfiles + 1 > tun.max_files):
    flush_batch(slot)                  # atomic publish + batches.created append
    slot = new _OpenBatch(next_seq(), bucket)
    replace slot in open_batches[bucket]

slot.add(abspath_bytes, size)
```

Notes:
- **Multiple open batches per bucket** (`open_batches`, default 8) so many big-file *and*
  small-file batches are being filled simultaneously → the orchestrator always has work of
  every class available early (avoids "all tiny finish last" starvation).
- Round-robin (`% len`) spreads incoming files across the open batches of a bucket so they
  close at staggered times (smoother publish rate).
- Sequence numbers come from a single monotonic counter (`seq_high_water` in manifest);
  in the multiprocess walker they're leased in blocks of 64 to avoid per-batch round-trips.

### 6.4 `flush_batch(b)` — atomic publish (P5)

```
if b.nfiles == 0: return
check_space()                                          # throttle gate (§8)
tmp   = pending/<bucket>_<seq>.batch.tmp
write all records (abspath \0 size \0) to tmp
fh.flush(); os.fsync(fh)                                # durable bytes
os.rename(tmp, pending/<bucket>_<seq>.batch)            # atomic publish
append batches.created:  name \0 bucket \0 nfiles \0 nbytes \0 ;  flush
batches_made += 1
```

The `fsync`+`rename` pair is the crash boundary: a batch is either absent or complete. The
`batches.created` append happens *after* the rename, so a crash between them merely means one
finished batch isn't yet logged — the orchestrator finds it by directory scan on the rare cold
start, and the walker re-derives it on resume (idempotent).

### 6.5 `finish()` — completion barrier (P7)

```
flush every remaining open batch (short batches are valid)
index_writer.flush + fsync + close
(walker mode) concatenate source.index.part-* → source.index (atomic rename)
write manifest.json { totals, scan_state: "complete", seq_high_water, ts }  (tmp+rename)
```

Only after `scan_state=complete` may verification treat "in source.index but not in tx log" as
truly missing.

---

## 7. Scan engines (source production)

BatchBuilder is source-agnostic behind `add()`. Two engines feed it:

### 7.1 Upload — directory walker (bugs #10–12, resume)

**Implemented today: a single-process BFS walker** (`scan_tree()` in `batch_builder.py`) using a
`collections.deque` frontier, bytes throughout, symlink-skip, and per-dir error logging. It is the
tested baseline and shares the exact same `add()`/index/journal contract described below. The
multiprocessing topology in this section is the optional performance upgrade (task B7) that drops
in behind the same contract without changing any on-disk format.

Single-process walker inner loop (bytes throughout, the implemented path):
```
dir = frontier.popleft()
with os.scandir(os.fsdecode(dir)) as it:               # errors → scan_errors.log, dir skipped
    for e in it:
        b = os.fsencode(e.path)                        # EXACT on-disk bytes (Latin-1/CR/space safe)
        if e.is_symlink() and not follow_symlinks: continue
        if e.is_dir(follow_symlinks): frontier.append(b); newly_discovered.append(b)
        elif e.is_file(follow_symlinks):
            st = e.stat(follow_symlinks)
            add(b, st.st_size)                         # absolute path + size → classify → batch
newly_completed.append(dir)
if total_files - last_ckpt >= checkpoint_every: checkpoint(newly_discovered, newly_completed)
```

Multiprocessing upgrade (planned, task B7) — **processes, not threads**: the hot loop is
`scandir`+`statx` (GIL released) **plus** per-entry Python packing/classify/encode (GIL-bound).
Processes remove the CPU ceiling.

Topology: 1 **coordinator** + N **walkers** (`scan_processes`, default `min(cpu,32)`), linked by
a bounded `dir_queue` (byte paths) and a bounded `result_queue` (small control messages).

Walker inner loop (bytes throughout):
```
dir = dir_queue.get()                              # sentinel → drain & exit
with os.scandir(os.fsdecode(dir)) as it:
    for e in it:
        b = os.fsencode(e.path)                    # EXACT on-disk bytes (Latin-1/CR/space safe)
        if e.is_dir(follow_symlinks=False):   children.append(b)
        elif e.is_file(follow_symlinks=False):
            st = e.stat(follow_symlinks=False)     # size for bucketing
            abspath = b                            # store ABSOLUTE path (e.path is absolute)
            writer[classify(st.st_size)].add(abspath, st.st_size)   # per-(proc,bucket) writer
            index_part.add(abspath, st.st_size, st.st_mtime)
        # symlinks/specials: skip by default (config flag), log
enqueue_or_spill(children)                         # §8 backpressure
result_queue.put(batched dir_done + discovered children)
```
- Each `(process, bucket)` owns its own batch writer → **zero lock contention** on the hot path.
- **`os.fsencode` recovers original filesystem bytes** from the surrogateescape str — the whole
  reason odd names survive. Never `.strip()`, never split on `\n`.

### 7.2 Download — threaded S3 lister (from `s3_list_bucket_fast.py`)

Chosen **threads, not processes**: the bottleneck is network `ListObjectsV2` (GIL released on
I/O), and results funnel to a single writer. Dynamic prefix-tree splitting shards the keyspace;
keys+sizes stream **straight to `source.index`** (never hold millions of keys in RAM). Then
identical size-bucketing/batching as upload. Resume via per-shard continuation-token journaling
+ `.discovered`/`.completed` logs.

### 7.3 CSV — test/bootstrap mode (implemented in `batch_builder.py`)

Reads an existing `path,size` CSV (auto-detects which column is the integer size; skips comment
`#`/header rows), byte-safe via `surrogateescape`, feeds `add()`. Lets the
bucketing/lifecycle/resume/special-char logic be validated before the live scanner exists.

- If the CSV holds **relative** paths (e.g. a verifier listing with `src=/bryck/nas05` in its
  header), pass **`--abs-root /bryck/nas05`** to prepend that root so batches carry absolute
  paths (byte-joined, so odd names survive). `--source-prefix` then defaults to `--abs-root`, so
  cloudcp strips the root and the key becomes the relative path (+ `--bucket-prefix`).
- Zero-size files (`,0`) land in the `zero` bucket by design — a batch full of size-0 entries is
  correct, not a parse error.
- Resume: `--config '{"CHECKPOINT_EVERY_FILES": N}'` controls checkpoint frequency; SIGINT/SIGTERM
  checkpoint and exit 130, and re-running the same command resumes from the durable prefix (§9.6).

---

## 8. Throttles & backpressure

BatchBuilder has four independent throttle mechanisms. Each protects a different resource.

### 8.1 Disk-space throttle (bugs #17, #18)

| Gate | Trigger | Action |
|---|---|---|
| **Preflight** | free % on `batchmeta_root` `< preflight_min_free_pct` (10%), or dir not writable | Refuse to start → transfer enters **`BLOCKED`** with reason. Never silently proceed. |
| **Per-flush guard** | `check_space()` before every `flush_batch` and index finalize | Below `pause_below_free_pct` (5%) → raise `NoSpace`; orchestrator **pauses cleanly** (→ `PAUSED`), drains in-flight, logs `ENOSPC`. |
| **ENOSPC on write** | any write returns ENOSPC | Caught, converted to the same clean pause. `.tmp`+rename means no half-written record survives. |
| **Auto-resume** | free % recovers above `resume_above_free_pct` (8%, hysteresis) | Orchestrator resumes; BatchBuilder continues from open batches / frontier. |

```json
"SPACE": { "preflight_min_free_pct": 10, "pause_below_free_pct": 5,
           "resume_above_free_pct": 8, "space_poll_sec": 15,
           "bytes_per_million_files": "350MB" }
```
Preflight also *projects* metadata footprint (`bytes_per_million_files` × est. files) and fails
early if it won't fit — not just an instantaneous free check.

### 8.2 Directory-queue backpressure (walker)

`dir_queue` is **bounded**. When full, a walker does **not** block/deadlock: it keeps newly-found
subdirs in a local deque and drains them itself (work-stealing degrades to local DFS). This caps
queue memory on fan-out-heavy trees while keeping all workers busy.

### 8.3 Result-log write throttle (walker)

Routing every `dir_done` through the single coordinator append could bottleneck on a fast local
FS. Walkers **batch** `dir_done`/children messages (flush every K dirs or T ms); the coordinator
appends each group with **one fsync**, amortizing the log cost while preserving the per-group
crash-ordering invariant (§9.2). Batch *file* writes stay sharded per-(proc,bucket); only the
small discovered/completed bookkeeping is centralized.

### 8.4 Open-batch cap (memory)

At most `open_batches` × #buckets partially-filled batches are held in RAM. With defaults
(8 × 4 ≈ 32 open batches, each capped at `batch_max_files` records of small tuples) the resident
footprint is bounded regardless of corpus size — memory is O(open batches), not O(total files).

---

## 9. Behaviour: stop, resume, crash

### 9.1 Batch-level resume (the fast path — bugs #14, #15)

```
pending_to_run = batches.created − batches.completed
```
The orchestrator rebuilds its work queue as this **set difference in one pass**. Cost is
**O(batches)** (tens of thousands), not **O(files)** (hundreds of millions). No tree re-walk, no
xattr probe, no enumeration of millions of files. Already-published batches in `pending/` stay;
`.tmp` leftovers are deleted.

### 9.2 Scan-level resume (frontier journaling — the core)

Two coordinator-owned append-only logs (the proven `s3_list_bucket_fast.py` model):
```
scan.discovered   # every dir ever enqueued
scan.completed    # every dir fully listed
frontier (resume) = discovered − completed
```
**Crash-consistency invariant** — per dir, the coordinator strictly orders:
1. walker's emitted batches/index-parts are already atomic-renamed (durable),
2. append the dir's **children** → `scan.discovered`, fsync,
3. append the **dir itself** → `scan.completed`, fsync,
4. *only then* treat children as eligible work.

Therefore a child is never "started-and-lost": after a crash, every unfinished dir is still in
`discovered − completed`, and no descendant of an unfinished dir was recorded. Resume reloads the
frontier, re-enqueues it, restores `seq_high_water` (bumped in bulk, never reused).
**Depth-independent** (frontier on disk, not the call stack → 14+ levels are free).

### 9.3 Intra-batch resume (no re-upload)

On a re-run of a partially-done batch, cloudcp reads its own `txhistory/<batch>.csv` and skips
paths already `SUCCESS`. The tx log *is* the per-file skip set — so a resumed partial batch
re-runs only its unfinished files. (BatchBuilder guarantees this works by never mutating a batch
file once published.)

### 9.4 Idempotency

A dir in-flight at crash is simply re-walked on resume; duplicate batch content is benign —
absorbed by the tx-log skip (§9.3) and path dedup in verification. All logs are replay-safe
(set semantics on name/path, last-status-wins).

### 9.5 Stop semantics

SIGTERM/stop → coordinator broadcasts sentinel → walkers flush buffers + final part files →
coordinator writes last checkpoint (fsync discovered/completed) → exit. Restart resumes from that
checkpoint. **Pause ≠ verify** (bug #6): pausing only sets `pause_requested`; the verify gate
requires `scan_state=complete AND all batches terminal AND pause_requested=false`, so a pause can
never auto-advance to VERIFYING.

### 9.6 CSV-mode resume (reference implementation)

The CSV-input prototype has no directory frontier, so it resumes on an **input-position
checkpoint** instead — the CSV analog of §9.2's frontier:

- **Checkpoint** (`checkpoint()`): flush every open batch, fsync `batches.created` and
  `source.index`, record `index_len = source.index length`, then atomically write
  `manifest.json` with `scan_state=in_progress`, `records_done`, `seq_high_water`, `index_len`,
  and totals. After it returns, **every record counted in `records_done` is durable**.
- Checkpoints fire (a) every `CHECKPOINT_EVERY_FILES` records (config, default 100 000) and
  (b) on **SIGINT/SIGTERM** — a cooperative handler sets a stop flag; at the next record
  boundary the builder checkpoints and exits `130`. (A `kill -9` between periodic checkpoints
  falls back to the last periodic checkpoint.)
- **Resume** (`start()` → `_load_or_reset()`): if `manifest.scan_state != complete`, restore
  counters + `seq_high_water`, **truncate `source.index` back to `index_len`** (drop any tail
  written past the last checkpoint), delete `.tmp` leftovers, and **skip the first
  `records_done` CSV rows**. Because checkpoints flush *all* open batches, `records_done` is a
  clean contiguous prefix of the input → resume re-emits nothing and loses nothing. A fresh run
  (no manifest, or previous run `complete`) wipes stale `source.index`/`batches.created` and
  `.tmp` batches for a clean slate.
- **Verified:** killing a 500 000-record run mid-stream and resuming produced exactly 500 000
  unique records in both `source.index` and across all batch files — zero loss, zero duplication.

> The live multiprocessing walker (§7.1/§9.2) uses the richer frontier-journal model; the CSV
> prototype's simpler position-checkpoint achieves the same guarantee for a single ordered input
> stream.

---

## 10. Failure handling

| Failure | Handling |
|---|---|
| SIGINT/SIGTERM (CSV mode) | cooperative handler → checkpoint at next record boundary → exit 130; resume skips durable prefix |
| Unreadable dir (EACCES/EIO) | log to `scan_errors.log`, record in `manifest.unreadable[]`, **continue** (don't abort the whole scan) |
| Stat error on a single file | log, skip that file (it will show as missing in verification — correct) |
| Walker process dies | coordinator detects via missing heartbeat / closed pipe; re-enqueues that walker's in-flight dir(s) from last checkpoint |
| Coordinator dies | restart from `scan.discovered`/`scan.completed` checkpoint |
| Disk full on flush | retry w/ backoff; if persistent → clean pause (§8.1), never crash |
| Partial `.tmp` on crash | deleted on resume; only atomically-renamed finals count |
| Source root vanishes mid-scan | fatal → `FAILED` with reason (can't produce a complete index) |
| batchmeta becomes read-only | clean pause → `BLOCKED`; resume state is intact, no data lost |

---

## 11. Special-character handling — worked examples (bugs #9–12)

All handled by P1 (bytes end-to-end). Verified byte-exact round-trip in `batch_builder.py`.

| Input filename (bytes) | Wrong (old) behaviour | BatchBuilder behaviour |
|---|---|---|
| `café.txt` (é = 0xE9 Latin-1) | decode error / crash on enumerate | stored as raw bytes `...\xe9...`; cloudcp normalizes key to UTF-8 (`é`) or percent-encodes (`%E9`) at upload |
| `report\nfinal.csv` (embedded LF) | line-split → two bogus entries | NUL-framed → single record, LF preserved |
| `data.txt ` (trailing space) | `.strip()` → wrong key, upload to wrong path | trailing space preserved verbatim |
| `notes.txt\r` (trailing CR) | stripped or split → wrong key | CR preserved verbatim |
| `a\r\nb` (CRLF in middle) | split into two | single record, both bytes preserved |

Rule enforced everywhere: **the only delimiter is NUL; everything else is payload.**

---

## 12. Interfaces (contract for surrounding components)

**Produces (consumed downstream):**
- `batches/pending/<bucket>_<seq>.batch` → orchestrator claims (§14 proposal).
- `source.index` → verification (external-sort merge-join, §12 proposal).
- `batches.created` → orchestrator resume reconciliation.
- `manifest.json:scan_state=complete` → verification gate.

**Consumes:**
- `config.json:BATCH_BUILDER`, `config.json:SPACE`.
- source root (read-only OK) / S3 bucket (ARN assume-role creds).
- On resume: `scan.discovered`/`scan.completed`, `batches.created`, `manifest.seq_high_water`.

**Does NOT touch:** source xattr (P3), `txhistory/*` (cloudcp writes), `batches/{inflight,
processed,failed}` (orchestrator owns), `failed_uploads` (fallback owns).

---

## 13. Reference implementation status (`batch_builder.py`)

| Capability | Status |
|---|---|
| Size-bucket resolution from config before input | ✅ |
| Per-bucket target_bytes / max_files / open_batches | ✅ |
| NUL-framed batches, byte-exact special chars | ✅ (tested) |
| `source.index` write | ✅ |
| `batches.created` + `pending_to_run()` resume diff | ✅ |
| Preflight + per-flush space guard (exit code 2) | ✅ |
| Atomic `.tmp`+fsync+rename publish | ✅ |
| CSV input source | ✅ |
| **CSV-mode stop/resume** (checkpoint + skip) | ✅ (tested: kill mid-run, resume, 0 loss / 0 dup) |
| **Directory-scan walker (single-process BFS)** | ✅ (tested: 60k files/200 dirs, symlink skip, error log) |
| **Download / S3 lister source** | ⏳ tasks D1–D6 |
| `inflight/`/`fallback_wait/` dirs + full state machine | ⏳ orchestrator (§14) |
| Frontier journaling (`scan.discovered`/`scan.completed`) | ✅ (tested: kill mid-scan, resume, 0 loss / 0 dup) |
| **Directory-scan walker (multiprocessing)** | ⏳ optional perf upgrade (task B7) |

The implemented core is the stable contract; the two scan engines drop in behind `add()`
without changing the batch/index/resume formats.
