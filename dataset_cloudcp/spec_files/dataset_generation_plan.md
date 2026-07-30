# BryckCloud cloudcp — Dataset Generation Plan

**Version:** 1.0  
**Purpose:** Define every dataset needed to fully exhaust the BryckCloud transfer pipeline through all test phases — batch builder, scheduler weight shift, fallback/retry, and end-to-end verification.  
**Output:** One JSON file (`dataset_registry.json`) where each element is a fully self-describing dataset spec.  
**Scale cap:** ≤ 10 TB per dataset, ≤ 100 TB total across all datasets.

---

## Ground Rules Applied to Every Dataset

| Rule | Value |
|---|---|
| Filename variants | All 20 (see §2) must be represented in every dataset |
| File types | All 26 types (see §3) distributed across every dataset |
| Zero-byte files | Dedicated datasets AND seeded into every mixed set at ~1–2% of file count |
| Total size cap | ≤ 10 TB per individual dataset |
| JSON spec format | One JSON object per dataset in `dataset_registry.json` |
| Scheduler profile | `dt2_100gbe` is default unless the dataset is explicitly for `wan_lowbw` |

---

## Bucket Quick Reference

| Bucket | Size range | batch_max_files | batch_target_bytes | open_batches |
|---|---|---|---|---|
| `zero`   | 0 B exactly          | —    | —     | — (no batching config) |
| `tiny`   | 1 B → 1 MB           | 2000 | 256 MB | 8 |
| `small`  | 1 MB → 100 MB        | 512  | 2 GB   | 8 |
| `medium` | 100 MB → 1 GB        | 64   | 10 GB  | 8 |
| `large`  | > 1 GB               | 8    | 50 GB  | 8 |

**Batch sealing**: a batch closes the instant either `batch_max_files` OR `batch_target_bytes` is reached — whichever comes first.  
**Round-robin**: each tier keeps 8 batches open simultaneously (round-robin assignment as files arrive).

---

## Filename Variants Reference (20 variants — all must appear in every dataset)

| ID | Variant | Concrete example | Risk category |
|---|---|---|---|
| FN-01 | Plain ASCII, no spaces | `report_2024.csv` | Baseline |
| FN-02 | Spaces in name | `my report.csv` | Common |
| FN-03 | Trailing space | `export ` (raw 0x20 at end) | Whitespace stripping |
| FN-04 | Embedded newline | `file` + `\n` + `name.txt` (raw 0x0A in name) | Line-based parser break |
| FN-05 | Trailing carriage return | `data` + `\r` (raw 0x0D at end) | Windows-origin |
| FN-06 | Latin-1 bytes | `café_data` stored as raw Latin-1 bytes (0x80–0xFF) | Encoding assumption |
| FN-07 | Very long filename (240+ chars) | `aaaa…aaa.bin` (240 'a' + `.bin`) | Path length limit |
| FN-08 | Unicode — CJK, Arabic, emoji | `数据文件_🚀.bin` | Internationalisation |
| FN-09 | Double or missing extension | `archive.tar.gz`, `datafile` | Extension handling |
| FN-10 | Mixed worst-case | `données export` + `\r` | Combined |
| FN-11 | Leading dot (hidden file) | `.hidden_data.bin` | Hidden-file handling |
| FN-12 | Shell metacharacters | `file$name!.csv` (contains `$`, `!`, `&`, `;`, `\|`) | Command injection surface |
| FN-13 | Windows-reserved chars (valid on Linux) | `file:name.txt`, `file<>pipe` | Cross-platform S3 key safety |
| FN-14 | Windows reserved device names as prefix | `CON_data.csv`, `PRN_log.txt`, `NUL_file.bin` | Win compat / key composition |
| FN-15 | Tab character in name | `file` + `\t` + `name.csv` (raw 0x09) | Tab as parser break |
| FN-16 | Name starts with dash | `-filename.bin` | Looks like a CLI flag |
| FN-17 | Unicode NFD normalization | `café.bin` encoded as NFD byte sequence | Normalization mismatch vs NFC |
| FN-18 | Zero-width Unicode characters | name contains U+200B (zero-width space) | Invisible chars in S3 keys |
| FN-19 | Multiple consecutive spaces | `file   name.csv` (3 spaces) | Space collapsing |
| FN-20 | Very long total PATH (path > 1000 bytes, filename < 255 bytes) | `deep/nested/path/.../file.bin` | Path buffer overflow |

**Coverage rule**: within any dataset, the minimum quota per variant is:
- tiny tier: 1,000 files per variant (minimum 20,000 tiny files total for full coverage)
- small tier: 200 files per variant (minimum 4,000 small files)
- medium tier: 20 files per variant (minimum 400 medium files)
- large tier: 5 files per variant (minimum 100 large files, or all large files if count < 100)

FN-11 through FN-20 require the generator to produce the EXACT byte sequence — no URL-encoding or escaping by the generator. The transfer tool must handle these raw bytes.

---

## File Types Reference (26 types — distributed across every dataset)

| Type | Extension | Category | Why it matters for transfer testing |
|---|---|---|---|
| CSV | `.csv` | Text | Text encoding; verify no line-ending conversion |
| JSON | `.json` | Text | UTF-8 structured; test multi-GB JSON in medium/large |
| Plain text | `.txt` | Text | Baseline text transfer |
| Log | `.log` | Text | Append-only pattern common in real datasets |
| SQL | `.sql` | Text | Large dump files common in backup workloads |
| XML | `.xml` | Text | Large XML must not be parsed or rewritten |
| YAML | `.yaml` | Text | Config files; zero content-type inference |
| Binary blob | `.bin` | Binary | Raw bytes; must transfer without any byte mutation |
| Gzip | `.gz` | Binary/Archive | Already-compressed; verify exact size preserved |
| Tar archive | `.tar` | Binary/Archive | Packed archive; byte-for-byte size must match |
| ZIP archive | `.zip` | Binary/Archive | Central directory at end — must not be truncated |
| Zstandard | `.zst` | Binary/Archive | Modern compression; non-`.gz` binary handling |
| bzip2 | `.bz2` | Binary/Archive | Legacy compression |
| 7-zip | `.7z` | Binary/Archive | Multi-stream format; large archives common |
| Parquet | `.parquet` | Data/Analytics | Row-group format; footer integrity after transfer |
| Avro | `.avro` | Data/Analytics | Schema embedded at start; verify start bytes intact |
| ORC | `.orc` | Data/Analytics | Stripe + postscript at end; verify end bytes intact |
| Arrow IPC | `.arrow` | Data/Analytics | Memory-mapped; alignment-sensitive |
| HDF5 | `.hdf5` | Data/Analytics | Scientific data; large hierarchical files |
| JPEG | `.jpg` | Media | Common; verify binary content not mangled |
| PNG | `.png` | Media | Lossless; byte-for-byte integrity critical |
| MP4 video | `.mp4` | Media | Moov atom — multipart must not corrupt container |
| MKV video | `.mkv` | Media | Streaming format; large binary blobs |
| WAV audio | `.wav` | Media | PCM audio; exact byte count critical |
| ELF shared lib | `.so` | Binary/System | Executable binary; must not be modified |
| No extension | (bare name) | Binary/System | Ensures no extension-based filtering |

**Coverage rule**: within any tier that has ≥ 260 files, each type must represent between 2–10% of the files (1/26 ≈ 3.8%). Tiers with fewer files get types assigned round-robin.

---

## Phase Overview

| Phase | Datasets | Total datasets | Primary test coverage |
|---|---|---|---|
| 1 — Single-Tier Isolation | DS-P1-01 … DS-P1-06 | 6 | Each tier in pure form; request-rate vs bandwidth bottleneck |
| 2 — Batch Builder Mechanics | DS-P2-01 … DS-P2-07 | 7 | Seal triggers (count and bytes), boundary values, round-robin |
| 3 — Batch Exhaustion / Weight Shift | DS-P3-01 … DS-P3-06 | 6 | All 6 dynamic-weight scenarios from planv2 §2.2 |
| 4 — Filename & Encoding Stress | DS-P4-01 … DS-P4-05 | 5 | All 10 filename variants isolated per tier and cross-tier |
| 5 — File Type Coverage | DS-P5-01 | 1 | Every file type in every tier |
| 6 — Network Profile Comparison | DS-P6-01 … DS-P6-02 | 2 | `dt2_100gbe` vs `wan_lowbw` scheduling divergence |
| 7 — Mixed Full-Pipeline | DS-P7-01 … DS-P7-03 | 3 | End-to-end scan → batch → upload → verify at three scales |
| 8 — Configuration Edge Cases | DS-P8-01 … DS-P8-05 | 5 | Empty dir, single file, huge single file, deep tree, unreadable subdir |
| 9 — Single-File Transfer | DS-P9-01 … DS-P9-07 | 7 | One-file batches at every bucket boundary; multipart threshold probe |
| 10 — Sub-Range Isolation | DS-P10-01 … DS-P10-08 | 8 | Narrow size bands aligned with existing spec naming convention |
| 11 — Alternative Weight Ratios | DS-P11-01 … DS-P11-03 | 3 | Non-default scheduler weights: 7:5:3:1, 9:5:2:0, 10:6:0:0 |
| **Total** | | **53** | |

---

---

## Phase 1 — Single-Tier Isolation

**Goal:** Run each tier completely in isolation so that any failure points directly to that tier's logic. No cross-tier scheduling noise.

**What to observe per dataset:**  
- Correct bucket assignment  
- Correct batch sealing  
- Throughput bottleneck type (S3 request rate for tiny; network bandwidth for large)

---

### DS-P1-01 — Zero-Tier Pure (Large Scale)

**Purpose:** Stress-test the zero-bucket path: mass enumeration of 0-byte files, correct assignment to `zero` tier, correct handling in `source.index` (path + size=0 + mtime).

| Field | Value |
|---|---|
| Active tiers | `zero` only |
| Total files | 5,000,000 |
| File size | 0 bytes (all files) |
| Total data size | 0 bytes |
| Directory depth | 3 levels max |
| Files per directory | ~500 |

**Filename variant distribution:** 500,000 per variant (10 variants × 500K = 5M).  
**File type distribution:** extension-only (no content), types distributed round-robin.  
**Seeded zero-bytes in mixed sets:** N/A — this IS the zero dataset.

**Batch expectations:**
- All 5M files fall into `zero` bucket
- Batch count: governed by `batch_max_files` for zero tier (if configured) or passed through as a single list

**Test cases covered:** Phase 1 §1.1 (all files assigned correct tier), §1.4 (single zero-byte file edge case scaled up), Phase 4 §4.1 (final report: all 5M as OK).

---

### DS-P1-02 — Tiny-Tier Pure (Medium Scale, ~500 GB)

**Purpose:** Isolate tiny-tier behaviour. Verify S3 request rate is the bottleneck (not bandwidth). Feed enough files to trigger multiple count-seals and byte-seals across 8 round-robin slots.

| Field | Value |
|---|---|
| Active tiers | `tiny` only |
| Total files | 1,000,000 |
| Size range | 1 B – 1 MB |
| Sub-range breakdown | 200K files at 1B–10KB, 300K at 10KB–100KB, 500K at 100KB–1MB |
| Average file size | ~500 KB |
| Total data size | ~500 GB |
| Zero-byte seeds | 10,000 files (1% of total, route to `zero` bucket) |
| Directory depth | 4 levels |

**Filename variant distribution:** 100,000 files per variant across tiny tier.  
**File type distribution:** each of 12 types gets ~8.3% share.

**Batch expectations (tiny tier):**
- `batch_max_files` = 2000, `batch_target_bytes` = 256 MB, `open_batches` = 8
- With ~500KB average: ~512 files trigger byte-seal (512 × 500KB ≈ 256MB)
- Expected total batches: ~1,953 batches (500GB / 256MB per batch)
- Both count-seal and byte-seal paths exercised across 8 open slots

**Test cases covered:** §2.1 (upload correctness), §2.4 (tiny = request-rate limited, bandwidth < 30%), §1.1 (tier assignment), §1.2 (round-robin across 8 slots).

---

### DS-P1-03 — Small-Tier Pure (~5 TB)

**Purpose:** Isolate small-tier behaviour. Files span 1 MB–100 MB so both count-seal (512 files) and byte-seal (2 GB) paths will be hit depending on average size.

| Field | Value |
|---|---|
| Active tiers | `small` only |
| Total files | 100,000 |
| Size range | 1 MB – 100 MB |
| Sub-range breakdown | 30K files at 1–5MB, 40K at 5–25MB, 30K at 25–100MB |
| Average file size | ~50 MB |
| Total data size | ~5 TB |
| Zero-byte seeds | 1,000 files |
| Directory depth | 4 levels |

**Filename variant distribution:** 10,000 files per variant within small tier.  
**File type distribution:** each of 12 types ~8.3%.

**Batch expectations (small tier):**
- `batch_max_files` = 512, `batch_target_bytes` = 2 GB, `open_batches` = 8
- At 50 MB avg: ~40 files hit byte-seal (40 × 50MB = 2GB) — byte-seal dominates
- At 1–5 MB avg: count-seal (512 files) dominates
- Expected total batches: ~2,500 batches

**Note:** 30K files at 25–100MB will test the 64 MB multipart threshold (files > 64 MB use multipart PUT).

**Test cases covered:** §2.1 (multipart threshold boundary), §2.1 (key composition), §1.1 (tier assignment).

---

### DS-P1-04 — Medium-Tier Pure (~5 TB)

**Purpose:** Isolate medium-tier behaviour. Files 100 MB–1 GB. Count-seal dominates (64 files × avg 700 MB ≈ 45 GB < 10 GB... wait — 64 × 700MB = 44.8 GB > 10 GB, so byte-seal dominates here).

| Field | Value |
|---|---|
| Active tiers | `medium` only |
| Total files | 10,000 |
| Size range | 100 MB – 1 GB |
| Sub-range breakdown | 3K files at 100–250MB, 4K at 250MB–700MB, 3K at 700MB–1GB |
| Average file size | ~500 MB |
| Total data size | ~5 TB |
| Zero-byte seeds | 100 files |
| Directory depth | 3 levels |

**Filename variant distribution:** 1,000 files per variant within medium tier.  
**File type distribution:** each of 12 types ~8.3%.

**Batch expectations (medium tier):**
- `batch_max_files` = 64, `batch_target_bytes` = 10 GB, `open_batches` = 8
- At 500 MB avg: byte-seal at 20 files (20 × 500MB = 10GB) — byte-seal always dominates
- At 100 MB min: count-seal at 100 files (100 × 100MB = 10GB) — also byte-seal
- Expected total batches: ~500 batches (5TB / 10GB)

**Test cases covered:** §2.1 (upload correctness), §1.1 (tier assignment), §2.5 (CHUNK_SIZE_MB: files > 64MB use multipart — all files in this tier do).

---

### DS-P1-05 — Large-Tier Pure (Small Scale, ~500 GB)

**Purpose:** Small large-file dataset (cheap to run, quick smoke test). Verify multipart upload is used, large-file key composition is correct, batch sealing at 8 files.

| Field | Value |
|---|---|
| Active tiers | `large` only |
| Total files | 20 |
| Size range | 5 GB – 50 GB |
| Sub-range breakdown | 5 files at 5–10GB, 10 files at 10–30GB, 5 files at 30–50GB |
| Average file size | ~25 GB |
| Total data size | ~500 GB |
| Zero-byte seeds | 0 |
| Directory depth | 1 level |

**Filename variant distribution:** 2 files per variant (20 total / 10 variants).  
**File type distribution:** round-robin across 12 types.

**Batch expectations (large tier):**
- `batch_max_files` = 8, `batch_target_bytes` = 50 GB, `open_batches` = 8
- 20 files → 2–3 batches (first 2 batches fill by count at 8 files, last batch = 4 files)
- Multipart upload expected for all files (all > 64 MB threshold)

**Test cases covered:** §2.1 (multipart: all files use multipart), §5.1 (DS5 happy path partial), §2.4 (large = bandwidth limited, bandwidth ≥ 70%).

---

### DS-P1-06 — Large-Tier Pure (Full Scale, ~10 TB)

**Purpose:** Full-scale large file run. Saturate bandwidth. Stress multipart assembly. Regression baseline for bandwidth throughput (≥ 9,500 MB/sec target from planv2 performance goals).

| Field | Value |
|---|---|
| Active tiers | `large` only |
| Total files | 200 |
| Size range | 5 GB – 100 GB |
| Sub-range breakdown | 40 files at 5–15GB, 100 files at 15–60GB, 60 files at 60–100GB |
| Average file size | ~50 GB |
| Total data size | ~10 TB |
| Zero-byte seeds | 0 |
| Directory depth | 2 levels |

**Filename variant distribution:** 20 files per variant.  
**File type distribution:** round-robin across 12 types.

**Batch expectations (large tier):**
- `batch_target_bytes` = 50 GB → at ~50 GB avg, each batch holds ~1 file (byte-seal = 1 file/batch)
- Count-seal at 8 files would need 8 × 5GB = 40 GB minimum — byte-seal still dominates
- Expected total batches: ~200 batches

**Test cases covered:** §5.1 (DS5 full happy path), §2.4 (bandwidth-limited large files), §2.1 (zero incomplete multipart uploads), performance regression baseline.

---

---

## Phase 2 — Batch Builder Mechanics

**Goal:** Precisely trigger every sealing condition (count-seal, byte-seal, both simultaneously), verify round-robin slot distribution, and probe all size-bucket boundary values.

---

### DS-P2-01 — Boundary Values (Dedicated Probe Dataset)

**Purpose:** Put exactly one file at each critical size boundary. Verify every file lands in the correct bucket. No ambiguity — one file = one assertion.

| Field | Value |
|---|---|
| Total files | 110 |
| Zero-byte seeds | 10 |

**File set (11 boundary values × 10 files each = 110 files total):**

| Boundary | Byte size | Expected bucket | Files |
|---|---|---|---|
| 0 B | 0 | `zero` | 10 |
| 1 B | 1 | `tiny` | 10 |
| 10 KB | 10,240 | `tiny` | 10 |
| 999,999 B | ~1 MB – 1 B | `tiny` | 10 |
| 1 MB | 1,048,576 | `small` (first byte above tiny max) | 10 |
| 63 MB | 66,060,288 | `small` (below multipart threshold) | 10 |
| 64 MB | 67,108,864 | `small` (first multipart file) | 10 |
| 99 MB | 103,809,024 | `small` (top of small range) | 10 |
| 100 MB | 104,857,600 | `medium` (first byte above small max) | 10 |
| 999 MB | 1,047,527,424 | `medium` (top of medium range) | 10 |
| 1 GB | 1,073,741,824 | `large` (first byte above medium max) | 10 |

**Each file in this dataset has a unique, descriptive name** encoding its exact size (e.g., `boundary_1b_fn01_plain.bin`).  
**All 10 filename variants** are spread across the 11 boundaries.

**Test cases covered:** §1.1 (all files sorted into correct tier), §2.1 (multipart at exactly 64 MB), §1.4 edge cases.

---

### DS-P2-02 — Tiny Count-Seal Trigger

**Purpose:** Add exactly `batch_max_files + 1 = 2001` tiny files to a single open slot. Verify the 2001st file causes slot 0 to seal and the 2001st file opens a new batch.

| Field | Value |
|---|---|
| Active tiers | `tiny` only |
| Total files | 2,001 |
| File size | all exactly 100 B (tiny, byte-seal will NOT trigger: 2001 × 100B = 196 KB ≪ 256 MB) |
| Total data size | ~196 KB |

**Why size = 100 B:** keeps total data far below `batch_target_bytes` = 256 MB so only the count trigger fires.

**Expected outcome:**  
- Slot 0 seals after exactly 2,000 files  
- File #2001 opens batch in slot 1 (round-robin continues)  
- No sealed batch contains more than 2,000 files

**Test cases covered:** §1.1 (batch closes at count limit), §1.2 (triggering file opens new batch).

---

### DS-P2-03 — Tiny Byte-Seal Trigger

**Purpose:** Send tiny files that together exceed `batch_target_bytes` = 256 MB before hitting the file count limit. Only the byte threshold should fire.

| Field | Value |
|---|---|
| Active tiers | `tiny` only |
| Total files | 260 |
| File size | all exactly 1 MB (1 MB each × 260 = 260 MB > 256 MB target) |
| Total data size | 260 MB |

**Why 260 files of 1 MB:** keeps count at 260 ≪ 2000 so only the byte trigger fires after file #256 (256 × 1MB = 256MB).

**Expected outcome:**  
- Slot 0 seals after exactly 256 files (256 MB reached)  
- Files 257–260 continue into a new batch  
- No sealed batch exceeds 256 MB

**Test cases covered:** §1.1 (batch closes at byte limit), §1.2 (triggering file opens new batch).

---

### DS-P2-04 — Small Count-Seal Trigger

| Field | Value |
|---|---|
| Active tiers | `small` only |
| Total files | 513 |
| File size | all exactly 1 MB (513 × 1MB = 513 MB ≪ 2 GB byte limit) |
| Total data size | ~513 MB |

**Expected outcome:** batch seals after 512 files; file #513 is first in the next batch.

---

### DS-P2-05 — Medium Count-Seal Trigger

| Field | Value |
|---|---|
| Active tiers | `medium` only |
| Total files | 65 |
| File size | all exactly 100 MB (65 × 100MB = 6.5 GB ≪ 10 GB byte limit) |
| Total data size | ~6.5 GB |

**Expected outcome:** batch seals after 64 files; file #65 is first in the next batch.

---

### DS-P2-06 — Large Count-Seal Trigger

| Field | Value |
|---|---|
| Active tiers | `large` only |
| Total files | 9 |
| File size | all exactly 2 GB (9 × 2GB = 18 GB ≪ 50 GB byte limit) |
| Total data size | ~18 GB |

**Expected outcome:** batch seals after 8 files; file #9 is first in the next batch.

---

### DS-P2-07 — Round-Robin Slot Distribution (800 files)

**Purpose:** Verify the 8 open tiny slots each receive exactly 100 files (± 1) when 800 evenly-sized tiny files are fed to the batch builder.

| Field | Value |
|---|---|
| Active tiers | `tiny` only |
| Total files | 800 |
| File size | all exactly 100 KB (800 × 100KB = 80 MB ≪ 256 MB byte limit) |
| Total data size | ~80 MB |

**Expected outcome per slot:** 100 files (± 1). Batch close times are staggered — no single slot receives all files.

**Test cases covered:** §1.1 (files spread evenly across 8 open slots).

---

---

## Phase 3 — Batch Exhaustion / Dynamic Weight Shift

**Goal:** Verify that when one or more tiers run out of batches, the freed worker slots are absorbed by the remaining active tiers within 3 scheduling cycles. Test all 6 exhaustion scenarios from planv2 §2.2.

**Design principle for each dataset:**  
- The tier(s) that must exhaust early get exactly enough files to fill **`N_exhaust` batches** (chosen so their tier drains after ~1–2 scheduling cycles at their allocated worker count).  
- The remaining tiers get **`N_long` batches** each — enough to still be running when the exhausted tier drains, so that the weight shift is observable.

**Scheduling cycle math (dt2_100gbe, 16 workers, weights 6:4:3:3):**  
- Large processes 6 batches/cycle → to exhaust in cycle 2: give it 6–12 batches  
- Medium processes 4 batches/cycle → to exhaust in cycle 2: give it 4–8 batches  
- Small processes 3 batches/cycle → to exhaust in cycle 2: give it 3–6 batches  
- Tiny processes 3 batches/cycle → to exhaust in cycle 2: give it 3–6 batches  
- "Long" tiers: each gets 30–50 batches → outlast the exhausted tier by ≥ 15 cycles

All exhaustion datasets use **`dt2_100gbe`** profile.

---

### DS-P3-01 — Large Exhausts First

| Tier | File count | Avg size | Data total | Expected batches | Cycles to drain |
|---|---|---|---|---|---|
| `large` | 16 | 10 GB | ~160 GB | **2 batches** (count-seal at 8) | ~1 cycle (6 workers) |
| `medium` | 1,280 | 500 MB | ~640 GB | ~64 batches | ~16 cycles (4 workers) |
| `small` | 15,360 | 2 MB | ~30 GB | ~30 batches | ~10 cycles (3 workers) |
| `tiny` | 60,000 | 400 KB | ~24 GB | ~24 batches | ~8 cycles (3 workers) |
| **Total** | **76,656** | | **~854 GB** | | |

Zero-byte seeds: 750 files (1% of tiny file count).  
**Exhaustion order:** large → small/tiny → medium.  
**Weight-shift observation window:** after large drains, 6 freed slots redistribute to medium (24%), small (18%), tiny (18%) → converge to 40% medium : 30% small : 30% tiny of 16 workers.

---

### DS-P3-02 — Medium Exhausts First

| Tier | File count | Avg size | Data total | Expected batches | Cycles to drain |
|---|---|---|---|---|---|
| `large` | 300 | 10 GB | ~3 TB | ~38 batches | ~6 cycles |
| `medium` | 256 | 500 MB | ~128 GB | **4 batches** (count-seal) | ~1 cycle (4 workers) |
| `small` | 15,360 | 2 MB | ~30 GB | ~30 batches | ~10 cycles |
| `tiny` | 60,000 | 400 KB | ~24 GB | ~24 batches | ~8 cycles |
| **Total** | **75,916** | | **~3.2 TB** | | |

Zero-byte seeds: 600 files.  
**Exhaustion order:** medium first.  
**Weight-shift:** medium's 4 freed slots absorb into large, small, tiny.

---

### DS-P3-03 — Small Exhausts First

| Tier | File count | Avg size | Data total | Expected batches | Cycles to drain |
|---|---|---|---|---|---|
| `large` | 300 | 10 GB | ~3 TB | ~38 batches | ~6 cycles |
| `medium` | 1,280 | 500 MB | ~640 GB | ~64 batches | ~16 cycles |
| `small` | 1,024 | 2 MB | ~2 GB | **2 batches** (count-seal) | ~1 cycle (3 workers) |
| `tiny` | 60,000 | 400 KB | ~24 GB | ~24 batches | ~8 cycles |
| **Total** | **62,604** | | **~3.7 TB** | | |

Zero-byte seeds: 600 files.  
**Exhaustion order:** small first.  
**Weight-shift:** small's 3 freed slots absorb into large, medium, tiny.

---

### DS-P3-04 — Tiny Exhausts First

| Tier | File count | Avg size | Data total | Expected batches | Cycles to drain |
|---|---|---|---|---|---|
| `large` | 300 | 10 GB | ~3 TB | ~38 batches | ~6 cycles |
| `medium` | 1,280 | 500 MB | ~640 GB | ~64 batches | ~16 cycles |
| `small` | 15,360 | 2 MB | ~30 GB | ~30 batches | ~10 cycles |
| `tiny` | 6,000 | 400 KB | ~2.4 GB | **3 batches** (count-seal) | ~1 cycle (3 workers) |
| **Total** | **22,940** | | **~3.7 TB** | | |

Zero-byte seeds: 60 files.  
**Exhaustion order:** tiny first.  
**Weight-shift:** tiny's 3 freed slots absorb into large, medium, small.

---

### DS-P3-05 — Large and Medium Both Drain Together

Both large and medium get only 1–2 batches worth. After they drain, all 16 workers split between small and tiny (should converge to 8 each, since weights 3:3 are equal for small and tiny).

| Tier | File count | Avg size | Data total | Expected batches | Cycles to drain |
|---|---|---|---|---|---|
| `large` | 8 | 10 GB | ~80 GB | **1 batch** | ~1 cycle |
| `medium` | 64 | 500 MB | ~32 GB | **1 batch** | ~1 cycle |
| `small` | 30,720 | 2 MB | ~60 GB | ~60 batches | ~20 cycles after weight shift |
| `tiny` | 120,000 | 400 KB | ~48 GB | ~48 batches | ~16 cycles after weight shift |
| **Total** | **150,792** | | **~220 GB** | | |

Zero-byte seeds: 1,200 files.  
**Post-drain state:** all 16 workers split 50/50 between small and tiny (8 each).

---

### DS-P3-06 — Only Tiny Has Remaining Work

Large, medium, and small all drain early. Only tiny keeps going. At the end, all 16 workers must be assigned to tiny.

| Tier | File count | Avg size | Data total | Expected batches | Cycles to drain |
|---|---|---|---|---|---|
| `large` | 8 | 10 GB | ~80 GB | **1 batch** | ~1 cycle |
| `medium` | 64 | 500 MB | ~32 GB | **1 batch** | ~1 cycle |
| `small` | 512 | 2 MB | ~1 GB | **1 batch** | ~1 cycle |
| `tiny` | 400,000 | 400 KB | ~160 GB | ~160 batches | ~54 cycles at rate-3, or ~10 cycles at rate-16 after full steal |
| **Total** | **400,584** | | **~273 GB** | | |

Zero-byte seeds: 4,000 files.  
**Final state:** 16 workers all on tiny. This is the most extreme work-steal scenario — tests §2.2 Scenario 6.

---

---

## Phase 4 — Filename & Encoding Stress

**Goal:** Prove that every filename variant survives byte-for-byte through the full pipeline: scan → NUL-delimited batch file → cloudcp key composition → S3 key → HeadObject round-trip → source.index matching → final report.

These datasets are intentionally **small in data size** — the focus is encoding correctness, not scale.

---

### DS-P4-01 — Filename Stress: Tiny Tier Only

| Field | Value |
|---|---|
| Active tiers | `tiny` only |
| Total files | 20,000 |
| Files per variant | 1,000 each (20 variants × 1,000 = 20,000) |
| File size | fixed 512 KB each |
| Total data size | ~10 GB |

Each variant group has files of all 26 file types (cycle through types: 1000 ÷ 26 ≈ 38 each).

---

### DS-P4-02 — Filename Stress: Small Tier Only

| Field | Value |
|---|---|
| Active tiers | `small` only |
| Total files | 4,800 |
| Files per variant | 240 each (20 variants × 240 = 4,800) |
| File size | 10 MB each |
| Total data size | ~48 GB |

---

### DS-P4-03 — Filename Stress: Medium Tier Only

| Field | Value |
|---|---|
| Active tiers | `medium` only |
| Total files | 400 |
| Files per variant | 20 each (20 variants × 20 = 400) |
| File size | 200 MB each |
| Total data size | ~80 GB |

---

### DS-P4-04 — Filename Stress: Large Tier Only

| Field | Value |
|---|---|
| Active tiers | `large` only |
| Total files | 100 |
| Files per variant | 5 each (20 variants × 5 = 100) |
| File size | 2 GB each |
| Total data size | ~200 GB |

---

### DS-P4-05 — Filename Stress: Cross-Tier (All Tiers)

All 10 filename variants present in all 4 active tiers simultaneously. Tests that the merge-join in verification handles variant paths correctly when they appear across all tier batch files.

| Tier | Files per variant | Total files | Avg size | Total data |
|---|---|---|---|---|
| `tiny` | 500 | 10,000 | 500 KB | ~5 GB |
| `small` | 100 | 2,000 | 10 MB | ~20 GB |
| `medium` | 20 | 400 | 200 MB | ~80 GB |
| `large` | 5 | 100 | 2 GB | ~200 GB |
| Zero seeds | 50 | 50 | 0 B | 0 |
| **Total** | | **12,550** | | **~305 GB** |

---

---

## Phase 5 — File Type Coverage

**Goal:** Confirm that no file type is accidentally excluded, mis-routed, or corrupted during the transfer. One dataset, all tiers, all types.

---

### DS-P5-01 — All File Types, All Tiers

| Tier | Files per type | Total files | Avg size | Total data |
|---|---|---|---|---|
| `zero` | 10 | 260 | 0 B | 0 |
| `tiny` | 1,000 | 26,000 | 500 KB | ~13 GB |
| `small` | 200 | 5,200 | 10 MB | ~52 GB |
| `medium` | 20 | 520 | 300 MB | ~156 GB |
| `large` | 5 | 130 | 5 GB | ~650 GB |
| **Total** | | **32,110** | | **~871 GB** |

**All 20 filename variants** are applied to each type group within each tier (cycling: if 1000 files per type, 50 per variant).

**File content requirements:**
- `.csv`, `.json`, `.txt`, `.log`, `.sql`: valid text content (so tools that inspect content don't error)
- `.bin`, `.gz`, `.tar`, `.parquet`, `.jpg`, `.mp4`: binary content (random bytes is fine for transfer testing)
- No extension: random binary bytes

---

---

## Phase 6 — Network Profile Comparison

**Goal:** Run the same logical workload under two profiles to observe scheduling divergence. Batch files on disk must be byte-for-byte identical between runs — only the scheduling weights change.

The datasets themselves are profile-neutral. The profile is applied at the broker level. Both DS-P6-01 and DS-P6-02 use the **same underlying files** — they are two runs of the same dataset, not two different datasets.

---

### DS-P6-01 — Profile Comparison Dataset (All Tiers, Balanced)

This dataset is designed so both profiles have meaningful work in every tier, making the weight-shift clearly observable in the slot-distribution logs.

| Tier | Files | Avg size | Total data | Batches (approx) |
|---|---|---|---|---|
| `tiny` | 60,000 | 400 KB | ~24 GB | ~24 batches |
| `small` | 10,000 | 5 MB | ~50 GB | ~25 batches |
| `medium` | 500 | 300 MB | ~150 GB | ~15 batches |
| `large` | 40 | 5 GB | ~200 GB | ~5 batches |
| Zero seeds | 600 | 0 B | 0 | — |
| **Total** | **71,140** | | **~424 GB** | |

**Run A** — profile `dt2_100gbe` (weights large:6, medium:4, small:3, tiny:3):  
→ large gets most slots → bandwidth utilisation should peak first.

**Run B** — profile `wan_lowbw` (weights tiny:6, small:4, medium:3, large:3):  
→ tiny gets most slots → S3 request rate should peak first; bandwidth stays low.

**Batch file hash:** SHA-256 of every batch file must be identical between Run A and Run B (files are unchanged — only scheduling differs).

---

---

## Phase 7 — Mixed Full-Pipeline

**Goal:** Run the complete pipeline scan → batch → upload → fallback → verify on a mixed corpus at three scales. These are the end-to-end regression baselines.

All 10 filename variants and all 12 file types are present. Profile is `dt2_100gbe`. Ratio of files per tier mirrors the planv2 §2.2 weight ratio (6:4:3:3 large:medium:small:tiny in terms of data volume, not file count).

---

### DS-P7-01 — Mixed Pipeline: Small Scale (~500 GB, fast CI baseline)

Used for pipeline sanity checks and CI gating — completes quickly.

| Tier | Files | Avg size | Total data |
|---|---|---|---|
| `zero` | 5,000 | 0 B | 0 |
| `tiny` | 80,000 | 400 KB | ~32 GB |
| `small` | 6,000 | 5 MB | ~30 GB |
| `medium` | 300 | 300 MB | ~90 GB |
| `large` | 20 | 8 GB | ~160 GB |
| **Total** | **91,320** | | **~312 GB** |

**Pass criteria:** all files `OK`, 0 `MISSING`, 0 `FAILED`. Weighted slot distribution (6:4:3:3) observed during run.

---

### DS-P7-02 — Mixed Pipeline: Medium Scale (~3 TB)

Used for regular regression runs. Exercises fallback worker, verifies per-tier completion summary.

| Tier | Files | Avg size | Total data |
|---|---|---|---|
| `zero` | 50,000 | 0 B | 0 |
| `tiny` | 500,000 | 400 KB | ~200 GB |
| `small` | 30,000 | 10 MB | ~300 GB |
| `medium` | 2,000 | 500 MB | ~1 TB |
| `large` | 150 | 10 GB | ~1.5 TB |
| **Total** | **582,150** | | **~3 TB** |

---

### DS-P7-03 — Mixed Pipeline: Full Scale (~10 TB, performance baseline)

The primary regression baseline. Total wall time is recorded on first run as the performance baseline for all future regression comparisons.

| Tier | Files | Avg size | Total data |
|---|---|---|---|
| `zero` | 100,000 | 0 B | 0 |
| `tiny` | 1,000,000 | 500 KB | ~500 GB |
| `small` | 60,000 | 20 MB | ~1.2 TB |
| `medium` | 6,000 | 700 MB | ~4.2 TB |
| `large` | 200 | 20 GB | ~4 TB |
| **Total** | **1,166,200** | | **~9.9 TB** |

**Performance targets:** ≥ 12,500 files/sec (tiny), ≥ 9,500 MB/sec (large), 6:4:3:3 slot distribution observed throughout.

---

---

## Phase 8 — Configuration Edge Cases

**Goal:** Cover the §1.4 edge case table from planv2. These are small, cheap datasets — the goal is correctness, not scale.

---

### DS-P8-01 — Empty Source Directory

| Field | Value |
|---|---|
| Total files | 0 |
| Directory structure | source dir exists, 3 empty subdirectories inside |
| Total data size | 0 |

**Expected outcome:** `scan_state=complete`, no batch files created, exit code 0, `source.index` exists but is empty (0 bytes).

---

### DS-P8-02 — Single Zero-Byte File

| Field | Value |
|---|---|
| Total files | 1 |
| File size | 0 bytes |
| Filename | Uses FN-08 variant (Unicode emoji name) |
| Total data size | 0 |

**Expected outcome:** one batch in `zero` tier, one NUL record in `source.index`.

---

### DS-P8-03 — Single Huge File

| Field | Value |
|---|---|
| Total files | 1 |
| File size | Exactly 100 GB |
| Filename | Uses FN-07 variant (240-char name) |
| Total data size | 100 GB |

**Expected outcome:** one batch in `large` tier (byte-seal at 50 GB → but only 1 file, so batch sealed at end of input). S3 access log shows multipart upload.

---

### DS-P8-04 — Deep Directory Tree (14 Levels)

| Field | Value |
|---|---|
| Total files | 700 (5 per level × 14 depths × 10 leaf dirs) |
| File size range | 100 KB – 5 MB (tiny + small) |
| Max depth | 14 nested directory levels |
| Total data size | ~2 GB |

Files are distributed at every depth level (not just leaves). Tests that:
- Scanner does not overflow the directory stack
- Resume (mid-walk kill at each depth level) works correctly
- Paths longer than typical (deep absolute paths) survive batch round-trip

---

### DS-P8-05 — Unreadable Subdirectory

| Field | Value |
|---|---|
| Total files | 500 readable + 200 in unreadable subdir |
| File size range | 1 KB – 1 MB (tiny) |
| Unreadable dirs | 1 directory with `chmod 000` applied |
| Total data size | ~350 MB readable |

**Expected outcome:**  
- 500 files scanned and batched normally  
- 1 entry in `scan_errors.log` for the unreadable directory  
- No crash  
- `source.index` contains exactly 500 records (not 700)

---

---

## Dataset Registry — Master Summary Table

| Dataset ID | Phase | Tiers | Total Files | Total Data | Primary Test Coverage |
|---|---|---|---|---|---|
| DS-P1-01 | Single-Tier | zero | 5,000,000 | 0 B | Zero-bucket handling, mass enumeration |
| DS-P1-02 | Single-Tier | tiny | 1,010,000 | ~500 GB | Request-rate bottleneck, tiny batch sealing |
| DS-P1-03 | Single-Tier | small | 101,000 | ~5 TB | Multipart threshold boundary, small batching |
| DS-P1-04 | Single-Tier | medium | 10,100 | ~5 TB | Medium batching, all files use multipart |
| DS-P1-05 | Single-Tier | large | 20 | ~500 GB | Large batching smoke test, multipart |
| DS-P1-06 | Single-Tier | large | 200 | ~10 TB | Bandwidth regression baseline |
| DS-P2-01 | Batch Mechanics | all | 110 | ~150 GB | Boundary value bucket assignment |
| DS-P2-02 | Batch Mechanics | tiny | 2,001 | ~196 KB | Count-seal trigger (tiny) |
| DS-P2-03 | Batch Mechanics | tiny | 260 | ~260 MB | Byte-seal trigger (tiny) |
| DS-P2-04 | Batch Mechanics | small | 513 | ~513 MB | Count-seal trigger (small) |
| DS-P2-05 | Batch Mechanics | medium | 65 | ~6.5 GB | Count-seal trigger (medium) |
| DS-P2-06 | Batch Mechanics | large | 9 | ~18 GB | Count-seal trigger (large) |
| DS-P2-07 | Batch Mechanics | tiny | 800 | ~80 MB | Round-robin slot distribution |
| DS-P3-01 | Exhaustion | all | 76,656 | ~854 GB | Large drains first; 6 slots redistribute |
| DS-P3-02 | Exhaustion | all | 75,916 | ~3.2 TB | Medium drains first; 4 slots redistribute |
| DS-P3-03 | Exhaustion | all | 62,604 | ~3.7 TB | Small drains first; 3 slots redistribute |
| DS-P3-04 | Exhaustion | all | 22,940 | ~3.7 TB | Tiny drains first; 3 slots redistribute |
| DS-P3-05 | Exhaustion | all | 150,792 | ~220 GB | Large+Medium drain; small+tiny split 16 slots |
| DS-P3-06 | Exhaustion | all | 400,584 | ~273 GB | Only Tiny remains; all 16 slots to tiny |
| DS-P4-01 | Filename Stress | tiny | 20,000 | ~10 GB | All 20 variants in tiny tier |
| DS-P4-02 | Filename Stress | small | 4,800 | ~48 GB | All 20 variants in small tier |
| DS-P4-03 | Filename Stress | medium | 400 | ~80 GB | All 20 variants in medium tier |
| DS-P4-04 | Filename Stress | large | 100 | ~200 GB | All 20 variants in large tier |
| DS-P4-05 | Filename Stress | all | 12,550 | ~305 GB | All 20 variants across all tiers cross-checked |
| DS-P5-01 | File Types | all | 32,110 | ~871 GB | All 26 types in all tiers |
| DS-P6-01 | Net Profile | all | 71,140 | ~424 GB | dt2_100gbe vs wan_lowbw (same files, 2 runs) |
| DS-P7-01 | Full Pipeline | all | 91,320 | ~312 GB | E2E CI sanity baseline |
| DS-P7-02 | Full Pipeline | all | 582,150 | ~3 TB | E2E regression run |
| DS-P7-03 | Full Pipeline | all | 1,166,200 | ~10 TB | E2E performance baseline |
| DS-P8-01 | Edge Case | — | 0 | 0 | Empty source directory |
| DS-P8-02 | Edge Case | zero | 1 | 0 | Single 0-byte file |
| DS-P8-03 | Edge Case | large | 1 | 100 GB | Single 100 GB file |
| DS-P8-04 | Edge Case | tiny+small | 700 | ~2 GB | 14-level deep tree |
| DS-P8-05 | Edge Case | tiny | 500 | ~350 MB | Unreadable subdirectory |
| DS-P9-01 | Single-File | tiny | 1 | 1 B | Single 1B file; tiny bucket; single-part PUT |
| DS-P9-02 | Single-File | small | 1 | 1 MB | Single 1MB; tiny→small boundary |
| DS-P9-03 | Single-File | small | 1 | 63 MB | Single 63MB; just below multipart threshold |
| DS-P9-04 | Single-File | small | 1 | 64 MB | Single 64MB; first file requiring multipart |
| DS-P9-05 | Single-File | medium | 1 | 100 MB | Single 100MB; small→medium boundary |
| DS-P9-06 | Single-File | large | 1 | 1 GB | Single 1GB; medium→large boundary |
| DS-P9-07 | Single-File | large | 1 | 100 GB | Single 100GB; multipart stress |
| DS-P10-01 | Sub-Range | zero+tiny | 1,000,000 | ~493 GB | 0B–1MB; aligns with spec_0bytes_1mb_* |
| DS-P10-02 | Sub-Range | tiny | 1,000,000 | ~5 GB | 1B–10KB sub-tiny; aligns with spec_1bytes_10kb_* |
| DS-P10-03 | Sub-Range | tiny | 1,000,000 | ~500 GB | 10KB–1MB; aligns with spec_10kb_1mb_* |
| DS-P10-04 | Sub-Range | small | 500,000 | ~1.25 TB | 1MB–4MB small lower; aligns with spec_1mb_4mb_* |
| DS-P10-05 | Sub-Range | small | 500,000 | ~5 TB | 4MB–16MB small mid; aligns with spec_4mb_16mb_* |
| DS-P10-06 | Sub-Range | small | 500,000 | ~7.6 TB | Fixed 16MB; aligns with spec_16mb_* |
| DS-P10-07 | Sub-Range | large | 30 | ~2 TB | 10GB–120GB; aligns with spec_10gb_120gb_* |
| DS-P10-08 | Sub-Range | large | 10 | ~3.5 TB | 200GB–500GB; aligns with spec_200gb_500gb_* |
| DS-P11-01 | Alt Weights | all | ~979,000 | ~5 TB | Weights 7:5:3:1; aligns with spec_7_5_3_1_* |
| DS-P11-02 | Alt Weights | large+med+small | ~129,000 | ~5 TB | Weights 9:5:2:0; aligns with spec_9_5_2_0_* |
| DS-P11-03 | Alt Weights | large+medium | ~4,047 | ~5 TB | Weights 10:6:0:0; aligns with spec_10_6_0_0_* |

**Grand total:** 53 datasets. Approximate unique data: ~75 TB. (DS-P6-01 = 2 runs of same ~444 GB dataset.)

---

---

## Generation Prompts

Each prompt below fully describes one dataset for any generation tool or script. Paste each prompt individually. Every prompt assumes the generator:
- Supports all 26 file types (see §3 reference table)
- Supports all 20 filename variants via explicit count parameters (see §2 reference table)
- Can create nested directory structures
- Outputs a YAML or JSON spec file

---

### Phase 1 Prompts

---

**PROMPT [DS-P1-01] — Zero-Tier Pure, Large Scale**

```
Generate dataset DS-P1-01.
Tiers active: zero only.
File count: 5,000,000 files.
File size: ALL files must be exactly 0 bytes.
Directory layout: 3 levels deep, approximately 500 files per leaf directory.
Filename variants (distribute evenly, 500,000 files per variant):
  FN-01: plain ASCII no spaces (e.g. report_2024.csv)
  FN-02: spaces in name (e.g. my report.csv)
  FN-03: trailing space byte 0x20 (e.g. "export ")
  FN-04: embedded newline byte 0x0A in name
  FN-05: trailing carriage return byte 0x0D
  FN-06: Latin-1 byte in name (e.g. café_data, stored as raw Latin-1)
  FN-07: name >= 240 characters long
  FN-08: Unicode CJK/Arabic/emoji in name (e.g. 数据文件_🚀.bin)
  FN-09: double extension or no extension (e.g. archive.tar.gz or datafile)
  FN-10: combination of spaces + CR byte (worst-case mix)
File type distribution: cycle through 12 types (csv, json, txt, log, sql, bin, gz, tar, parquet, jpg, mp4, no-ext) round-robin. Extensions only — 0-byte files have no content.
Expected bucket: ALL files → zero bucket.
Output spec name: DS-P1-01_zero_pure_5M.yaml
```

---

**PROMPT [DS-P1-02] — Tiny-Tier Pure, ~500 GB**

```
Generate dataset DS-P1-02.
Tiers active: tiny only (size range 1 B – 1 MB).
File count: 1,000,000 files + 10,000 zero-byte seed files.
Size distribution across tiny files:
  - 200,000 files: uniform random size 1 B – 10 KB
  - 300,000 files: uniform random size 10 KB – 100 KB
  - 500,000 files: uniform random size 100 KB – 1 MB
Target total data: approximately 500 GB.
Directory layout: 4 levels deep.
Filename variants (100,000 tiny files per variant, 10,000 total across 10 variants):
  Apply FN-01 through FN-10 as defined in master variant table.
  All 10 zero-byte seeds use FN-01 (plain ASCII).
File type distribution: each of 12 types gets approximately 8.3% of files.
Batch expectations:
  bucket = tiny, batch_max_files = 2000, batch_target_bytes = 256MB, open_batches = 8
  Both count-seal and byte-seal paths will be exercised (byte-seal dominates above ~512KB avg).
Output spec name: DS-P1-02_tiny_pure_1M_500GB.yaml
```

---

**PROMPT [DS-P1-03] — Small-Tier Pure, ~5 TB**

```
Generate dataset DS-P1-03.
Tiers active: small only (size range 1 MB – 100 MB) + 1,000 zero-byte seeds.
File count: 100,000 small files + 1,000 zero-byte seeds.
Size distribution:
  - 30,000 files: uniform random 1 MB – 5 MB
  - 40,000 files: uniform random 5 MB – 25 MB
  - 30,000 files: uniform random 25 MB – 100 MB  ← these will use multipart upload (>64MB)
Target total data: approximately 5 TB.
Directory layout: 4 levels deep.
Filename variants: 10,000 small files per variant (FN-01 through FN-10).
File type distribution: each of 12 types ~8.3%.
Batch expectations:
  bucket = small, batch_max_files = 512, batch_target_bytes = 2GB, open_batches = 8
  At 5 MB avg: count-seal dominates. At 50 MB avg: byte-seal dominates (~40 files/batch).
  Files > 64 MB MUST trigger multipart PUT in cloudcp.
Output spec name: DS-P1-03_small_pure_100K_5TB.yaml
```

---

**PROMPT [DS-P1-04] — Medium-Tier Pure, ~5 TB**

```
Generate dataset DS-P1-04.
Tiers active: medium only (size range 100 MB – 1 GB) + 100 zero-byte seeds.
File count: 10,000 medium files + 100 zero-byte seeds.
Size distribution:
  - 3,000 files: uniform random 100 MB – 250 MB
  - 4,000 files: uniform random 250 MB – 700 MB
  - 3,000 files: uniform random 700 MB – 1 GB
Target total data: approximately 5 TB.
Directory layout: 3 levels deep.
Filename variants: 1,000 medium files per variant (FN-01 through FN-10).
File type distribution: each of 12 types ~8.3%.
Batch expectations:
  bucket = medium, batch_max_files = 64, batch_target_bytes = 10GB, open_batches = 8
  At 500 MB avg: byte-seal at ~20 files per batch. All files are >64MB → multipart upload for ALL.
Output spec name: DS-P1-04_medium_pure_10K_5TB.yaml
```

---

**PROMPT [DS-P1-05] — Large-Tier Pure, Smoke Test ~500 GB**

```
Generate dataset DS-P1-05.
Tiers active: large only (size > 1 GB). No zero-byte seeds.
File count: exactly 20 files.
Size distribution:
  - 5 files: uniform random 5 GB – 10 GB
  - 10 files: uniform random 10 GB – 30 GB
  - 5 files: uniform random 30 GB – 50 GB
Target total data: approximately 500 GB.
Directory layout: 1 level (all files in root of source dir).
Filename variants: 2 files per variant (FN-01 through FN-10 = 20 files total, one-to-one).
File type distribution: round-robin through 12 types (20 files / 12 types, some types appear twice).
Batch expectations:
  bucket = large, batch_max_files = 8, open_batches = 8
  20 files → batch 0 seals at 8 files (count-seal), batch 1 seals at 8 files, batch 2 = 4 files (incomplete at end).
  ALL files must use multipart PUT (all > 64 MB).
Output spec name: DS-P1-05_large_pure_20files_500GB.yaml
```

---

**PROMPT [DS-P1-06] — Large-Tier Pure, Performance Baseline ~10 TB**

```
Generate dataset DS-P1-06.
Tiers active: large only (size > 1 GB). No zero-byte seeds.
File count: exactly 200 files.
Size distribution:
  - 40 files: uniform random 5 GB – 15 GB
  - 100 files: uniform random 15 GB – 60 GB
  - 60 files: uniform random 60 GB – 100 GB
Target total data: approximately 10 TB.
Directory layout: 2 levels deep (10 subdirectories, 20 files each).
Filename variants: 20 files per variant (FN-01 through FN-10).
File type distribution: round-robin through 12 types.
Batch expectations:
  batch_target_bytes = 50 GB → at ~50 GB avg, 1 file per batch (byte-seal).
  Expected batch count: ~200 batches.
  ALL files multipart. Performance target: bandwidth >= 9,500 MB/sec.
Output spec name: DS-P1-06_large_pure_200files_10TB.yaml
```

---

### Phase 2 Prompts

---

**PROMPT [DS-P2-01] — Boundary Values Probe**

```
Generate dataset DS-P2-01.
Purpose: one file at each exact boundary size for bucket assignment verification.
File set: create EXACTLY 10 files at each of the following byte sizes:
  0 B (zero bucket), 1 B (tiny), 10240 B = 10 KB (tiny),
  999999 B (tiny), 1048576 B = 1 MB (small, first above tiny max),
  66060288 B = 63 MB (small, below multipart threshold),
  67108864 B = 64 MB (small, first file requiring multipart),
  103809024 B = 99 MB (small, top of small range),
  104857600 B = 100 MB (medium, first above small max),
  1047527424 B = 999 MB (medium, top of medium range),
  1073741824 B = 1 GB (large, first above medium max).
Total files: 110 (11 sizes × 10 files each).
File naming: each file name MUST encode its exact size, e.g. "boundary_64mb_fn01_plain.bin".
Filename variants: distribute FN-01 through FN-10 across each size group (1 per variant per size).
File content: fill with size bytes of random binary data (so size is truly as specified).
Directory layout: flat (all in source root).
Output spec name: DS-P2-01_boundary_probe.yaml
```

---

**PROMPT [DS-P2-02] — Tiny Count-Seal Trigger**

```
Generate dataset DS-P2-02.
Tiers active: tiny only. No zero-byte seeds.
File count: exactly 2,001 files.
File size: ALL files exactly 100 bytes (ensures total = 195 KB << 256 MB batch_target_bytes → ONLY count-seal fires).
Directory layout: flat.
Filename variants: distribute FN-01 through FN-10 evenly (200–201 files per variant).
File type distribution: round-robin through 12 types.
CRITICAL: generator must ensure that with batch_max_files=2000 and open_batches=8,
  exactly one batch seals after its 2000th file and file #2001 begins a new batch.
Output spec name: DS-P2-02_tiny_count_seal_2001.yaml
```

---

**PROMPT [DS-P2-03] — Tiny Byte-Seal Trigger**

```
Generate dataset DS-P2-03.
Tiers active: tiny only. No zero-byte seeds.
File count: exactly 260 files.
File size: ALL files exactly 1,048,576 bytes = 1 MB (260 files × 1MB = 260 MB > 256 MB target).
  Count = 260 << 2000, so ONLY byte-seal fires (at file #256, when 256 MB is reached).
Directory layout: flat.
Filename variants: 26 files per variant (FN-01 through FN-10).
File type distribution: round-robin through 12 types.
CRITICAL: seal must fire at exactly 256 files (256 × 1MB = 256MB = batch_target_bytes).
  File #257 must start a new batch, not overflow the sealed batch.
Output spec name: DS-P2-03_tiny_byte_seal_260.yaml
```

---

**PROMPT [DS-P2-04] — Small Count-Seal Trigger**

```
Generate dataset DS-P2-04.
Tiers active: small only. No zero-byte seeds.
File count: exactly 513 files.
File size: ALL files exactly 1,048,576 bytes = 1 MB (513 × 1MB = 513 MB << 2 GB target → only count-seal fires).
Directory layout: flat.
Filename variants: ~51 files per variant (FN-01 through FN-10).
File type distribution: round-robin through 12 types.
CRITICAL: batch seals after exactly 512 files. File #513 begins a new batch.
Output spec name: DS-P2-04_small_count_seal_513.yaml
```

---

**PROMPT [DS-P2-05] — Medium Count-Seal Trigger**

```
Generate dataset DS-P2-05.
Tiers active: medium only. No zero-byte seeds.
File count: exactly 65 files.
File size: ALL files exactly 104,857,600 bytes = 100 MB (65 × 100MB = 6.5 GB << 10 GB target → only count-seal fires).
Directory layout: flat.
Filename variants: FN-01 through FN-10 applied round-robin to 65 files (6–7 per variant).
File type distribution: round-robin through 12 types.
CRITICAL: batch seals after exactly 64 files. File #65 begins a new batch.
Output spec name: DS-P2-05_medium_count_seal_65.yaml
```

---

**PROMPT [DS-P2-06] — Large Count-Seal Trigger**

```
Generate dataset DS-P2-06.
Tiers active: large only. No zero-byte seeds.
File count: exactly 9 files.
File size: ALL files exactly 2,147,483,648 bytes = 2 GB (9 × 2GB = 18 GB << 50 GB target → only count-seal fires).
Directory layout: flat.
Filename variants: 9 files, assign FN-01 through FN-09 (one variant each; FN-10 skipped — insufficient files).
File type distribution: round-robin through first 9 of 12 types.
CRITICAL: batch seals after exactly 8 files. File #9 begins a new batch.
Output spec name: DS-P2-06_large_count_seal_9.yaml
```

---

**PROMPT [DS-P2-07] — Round-Robin Slot Distribution**

```
Generate dataset DS-P2-07.
Tiers active: tiny only. No zero-byte seeds.
File count: exactly 800 files.
File size: ALL files exactly 102,400 bytes = 100 KB (800 × 100KB = 80MB << 256MB → no byte-seal, no count-seal → all 8 slots stay open and receive files round-robin).
Directory layout: flat, single directory.
Filename variants: 80 files per variant (FN-01 through FN-10).
File type distribution: round-robin through 12 types.
CRITICAL: with open_batches=8 and no sealing occurring, each slot should receive exactly 100 files (±1).
  Generator must NOT pre-group files — they must arrive in filesystem scan order so round-robin is exercised.
Output spec name: DS-P2-07_round_robin_800.yaml
```

---

### Phase 3 Prompts

---

**PROMPT [DS-P3-01] — Large Exhausts First**

```
Generate dataset DS-P3-01.
Profile: dt2_100gbe (weights large=6, medium=4, small=3, tiny=3; total workers=16).
Design goal: large tier drains after approximately 1 scheduling cycle; all other tiers have 8+ cycles of work remaining.

Tier specs:
  large:  16 files, size range 8 GB – 12 GB, avg ~10 GB → ~160 GB total
          → fills exactly 2 count-sealed batches (8 files each)
          → with 6 large workers, drains in ~1 cycle
  medium: 1,280 files, size range 400 MB – 600 MB, avg ~500 MB → ~640 GB total
          → byte-seal at ~20 files/batch (10GB) → ~64 batches
          → with 4 workers, drains in ~16 cycles (long-running)
  small:  15,360 files, size range 1 MB – 4 MB, avg ~2 MB → ~30 GB total
          → count-seal at 512 files → ~30 batches
          → with 3 workers, drains in ~10 cycles
  tiny:   60,000 files, size range 200 KB – 600 KB, avg ~400 KB → ~24 GB total
          → byte-seal at ~640 files (640×400KB=256MB) → ~94 batches
          → with 3 workers, drains in ~31 cycles
  zero seeds: 750 files, 0 bytes each

Total files: ~77,406. Total data: ~854 GB.

Directory layout: 3 levels. All 10 filename variants applied per tier (see master table).
All 12 file types distributed across all tiers.

CRITICAL timing annotation: mark in spec which tier is "exhaust_first: true" so the test harness knows which drain event to monitor.
Output spec name: DS-P3-01_exhaust_large_first.yaml
```

---

**PROMPT [DS-P3-02] — Medium Exhausts First**

```
Generate dataset DS-P3-02.
Profile: dt2_100gbe.
Design goal: medium tier drains after ~1 cycle; all others have 6+ cycles remaining.

Tier specs:
  large:  300 files, size range 8 GB – 12 GB → ~3 TB total → ~38 batches (count-seal at 8) → 6 cycles at weight=6
  medium: 256 files, size range 400 MB – 600 MB → ~128 GB total → 4 count-sealed batches (64 files each)
          → with 4 workers, drains in exactly 1 cycle
  small:  15,360 files, size range 1 MB – 4 MB → ~30 GB → ~30 batches → 10 cycles at weight=3
  tiny:   60,000 files, size range 200 KB – 600 KB → ~24 GB → ~94 batches → 31 cycles at weight=3
  zero seeds: 600 files

Total files: ~76,516. Total data: ~3.2 TB.
Directory layout: 3 levels. All 10 variants + 12 types.
CRITICAL: mark medium as exhaust_first: true.
Output spec name: DS-P3-02_exhaust_medium_first.yaml
```

---

**PROMPT [DS-P3-03] — Small Exhausts First**

```
Generate dataset DS-P3-03.
Profile: dt2_100gbe.
Design goal: small tier drains after ~1 cycle; all others keep running.

Tier specs:
  large:  300 files, 8–12 GB each → ~3 TB → ~38 batches → 6 cycles
  medium: 1,280 files, 400–600 MB → ~640 GB → ~64 batches → 16 cycles
  small:  1,024 files, 1–4 MB → ~2 GB → 2 count-sealed batches (512 files each)
          → with 3 workers, drains in ~1 cycle
  tiny:   60,000 files, 200–600 KB → ~24 GB → ~94 batches → 31 cycles
  zero seeds: 600 files

Total files: ~63,204. Total data: ~3.7 TB.
All 10 variants + 12 types. Mark small as exhaust_first: true.
Output spec name: DS-P3-03_exhaust_small_first.yaml
```

---

**PROMPT [DS-P3-04] — Tiny Exhausts First**

```
Generate dataset DS-P3-04.
Profile: dt2_100gbe.
Design goal: tiny tier drains after ~1 cycle; all others keep running.

Tier specs:
  large:  300 files, 8–12 GB → ~3 TB → ~38 batches → 6 cycles
  medium: 1,280 files, 400–600 MB → ~640 GB → ~64 batches → 16 cycles
  small:  15,360 files, 1–4 MB → ~30 GB → ~30 batches → 10 cycles
  tiny:   6,000 files, 200–600 KB → ~2.4 GB → 3 count-sealed batches (2000 each)
          → with 3 workers, drains in exactly 1 cycle
  zero seeds: 60 files

Total files: ~23,000. Total data: ~3.7 TB.
All 10 variants + 12 types. Mark tiny as exhaust_first: true.
Output spec name: DS-P3-04_exhaust_tiny_first.yaml
```

---

**PROMPT [DS-P3-05] — Large and Medium Both Drain Together**

```
Generate dataset DS-P3-05.
Profile: dt2_100gbe.
Design goal: large AND medium each have only 1 batch; both drain in cycle 1.
  After drain: all 16 workers split between small and tiny (equal weights 3:3 → 8 each).

Tier specs:
  large:  8 files, 8–12 GB → ~80 GB → exactly 1 count-sealed batch
  medium: 64 files, 400–600 MB → ~32 GB → exactly 1 count-sealed batch
  small:  30,720 files, 1–4 MB → ~60 GB → ~60 batches → ~20 cycles at post-steal weight=8
  tiny:   120,000 files, 200–600 KB → ~48 GB → ~188 batches → ~24 cycles at post-steal weight=8
  zero seeds: 1,200 files

Total files: ~151,992. Total data: ~220 GB.
All 10 variants + 12 types.
CRITICAL annotation: exhaust_together: [large, medium]. Post-drain target: small=50%, tiny=50% of 16 slots.
Output spec name: DS-P3-05_exhaust_large_medium_together.yaml
```

---

**PROMPT [DS-P3-06] — Only Tiny Has Remaining Work**

```
Generate dataset DS-P3-06.
Profile: dt2_100gbe.
Design goal: large, medium, and small each have exactly 1 batch (drain in first cycle);
  tiny has ~160 batches. Final state: all 16 workers assigned to tiny.

Tier specs:
  large:  8 files, 8–12 GB → ~80 GB → 1 batch (drains in cycle 1 with 6 workers)
  medium: 64 files, 400–600 MB → ~32 GB → 1 batch (drains in cycle 1 with 4 workers)
  small:  512 files, 1–4 MB → ~1 GB → 1 batch (drains in cycle 1 with 3 workers)
  tiny:   400,000 files, 200–600 KB → ~160 GB → ~160 batches
          → at weight=3 (initially) → 53 cycles to drain; at weight=16 (after steal) → 10 cycles
  zero seeds: 4,000 files

Total files: ~404,584. Total data: ~273 GB.
All 10 variants + 12 types.
CRITICAL annotation: last_tier_standing: tiny. Test harness must measure that 16 workers all go to tiny after the 3rd scheduling cycle.
Output spec name: DS-P3-06_only_tiny_remaining.yaml
```

---

### Phase 4 Prompts

---

**PROMPT [DS-P4-01] — Filename Stress: Tiny Tier**

```
Generate dataset DS-P4-01.
Tiers active: tiny only.
Total files: exactly 10,000.
Files per variant: exactly 1,000 per variant (FN-01 through FN-10).
File size: ALL files exactly 512 KB = 524,288 bytes.
Total data: ~5 GB.
File type distribution: within each variant group (1000 files), cycle through all 12 types
  (83–84 files per type per variant group).
Directory layout: 1 subdirectory per variant, named for the variant (e.g. fn01_plain/, fn04_newline/).
No zero-byte seeds (this dataset is purely about name encoding — adds noise to count).
CRITICAL: generator must write variant files using the EXACT byte sequences specified:
  FN-03: filename ends with byte 0x20 (space), not URL-encoded
  FN-04: filename contains byte 0x0A (newline), not \n escape — raw byte
  FN-05: filename ends with byte 0x0D (CR), raw byte
  FN-06: filename contains bytes in Latin-1 range 0x80–0xFF (e.g. 0xE9 for 'é')
  FN-08: filename contains UTF-8 encoded CJK, Arabic, or emoji characters
Output spec name: DS-P4-01_fname_stress_tiny.yaml
```

---

**PROMPT [DS-P4-02] — Filename Stress: Small Tier**

```
Generate dataset DS-P4-02.
Tiers active: small only.
Total files: 2,400. Files per variant: 240. File size: exactly 10 MB each. Total data: ~24 GB.
Subdirectory per variant. All EXACT byte sequences as specified in DS-P4-01.
No zero-byte seeds.
File type distribution: cycle 12 types within each variant group (20 per type per variant).
Output spec name: DS-P4-02_fname_stress_small.yaml
```

---

**PROMPT [DS-P4-03] — Filename Stress: Medium Tier**

```
Generate dataset DS-P4-03.
Tiers active: medium only.
Total files: 200. Files per variant: 20. File size: exactly 200 MB each. Total data: ~40 GB.
Subdirectory per variant. Exact byte sequences as specified.
No zero-byte seeds.
File type distribution: cycle 12 types within each variant group (1–2 per type per variant).
Output spec name: DS-P4-03_fname_stress_medium.yaml
```

---

**PROMPT [DS-P4-04] — Filename Stress: Large Tier**

```
Generate dataset DS-P4-04.
Tiers active: large only.
Total files: 50. Files per variant: 5. File size: exactly 2 GB each. Total data: ~100 GB.
Flat directory (single level). Exact byte sequences as specified.
No zero-byte seeds.
File type distribution: cycle 12 types across 50 files round-robin.
Output spec name: DS-P4-04_fname_stress_large.yaml
```

---

**PROMPT [DS-P4-05] — Filename Stress: Cross-Tier**

```
Generate dataset DS-P4-05.
Tiers active: all (tiny, small, medium, large) + zero seeds.
Purpose: all 10 filename variants present simultaneously across all tiers.
  The verification engine must join source.index vs upload reports
  for files whose names contain raw 0x0A, 0x0D, 0xE9, emoji, etc.

Tier breakdown:
  tiny:   5,000 files (500 per variant), size 512 KB each → ~2.5 GB
  small:  1,000 files (100 per variant), size 10 MB each → ~10 GB
  medium: 200 files (20 per variant), size 200 MB each → ~40 GB
  large:  50 files (5 per variant), size 2 GB each → ~100 GB
  zero seeds: 50 files, 0 bytes, FN-01 only

Total: 6,300 files, ~152.5 GB.
All EXACT byte sequences as specified. One subdirectory per tier+variant combination.
Output spec name: DS-P4-05_fname_stress_cross_tier.yaml
```

---

### Phase 5 Prompt

---

**PROMPT [DS-P5-01] — All File Types, All Tiers**

```
Generate dataset DS-P5-01.
Purpose: ensure every one of the 12 file types is present in every active tier.
Tiers and counts:
  zero:   120 files (10 per type), 0 bytes each
  tiny:   12,000 files (1,000 per type), uniform random 100 KB – 1 MB
  small:  2,400 files (200 per type), uniform random 1 MB – 50 MB
  medium: 240 files (20 per type), uniform random 100 MB – 800 MB
  large:  60 files (5 per type), uniform random 2 GB – 20 GB

Total: 14,820 files, ~402 GB.

File types (one section per type, files within a type get realistic-looking content where possible):
  .csv     → UTF-8 text rows (comma-delimited, header row, 10 data rows, rest repeated)
  .json    → valid JSON object {"id": N, "data": "..."}
  .txt     → plain UTF-8 text
  .log     → timestamped log lines
  .sql     → valid SQL INSERT statements
  .bin     → random binary bytes
  .gz      → valid gzip stream wrapping random bytes
  .tar     → valid tar archive containing 1 small file
  .parquet → valid Parquet file (single column, N rows of random int64)
  .jpg     → valid minimal JPEG (1×1 pixel JFIF, padded to target size)
  .mp4     → valid minimal MP4 container padded to target size
  no-ext  → random binary bytes, filename has no extension

Filename variants: within each (tier × type) group, distribute FN-01 through FN-10 round-robin.
Directory layout: top-level directory per type (e.g. csv/, json/, …), tier files mixed within.
Output spec name: DS-P5-01_all_types_all_tiers.yaml
```

---

### Phase 6 Prompt

---

**PROMPT [DS-P6-01] — Network Profile Comparison**

```
Generate dataset DS-P6-01 (used for TWO runs: dt2_100gbe and wan_lowbw).
Purpose: identical on-disk dataset, run once per profile. Compare slot distributions.

Tier breakdown:
  zero seeds: 600 files, 0 bytes
  tiny:  60,000 files, uniform random 200 KB – 600 KB → ~24 GB
  small: 10,000 files, uniform random 1 MB – 10 MB → ~50 GB
  medium: 500 files, uniform random 200 MB – 400 MB → ~150 GB
  large:  40 files, uniform random 3 GB – 8 GB → ~220 GB

Total: 71,140 files, ~444 GB.
All 10 filename variants + all 12 file types.
Directory layout: 3 levels deep.

Run instructions (embed in spec):
  Run A: active_profile = dt2_100gbe (weights large=6, medium=4, small=3, tiny=3)
    → expect large to dominate slot allocation; bandwidth saturates first
  Run B: active_profile = wan_lowbw (weights tiny=6, small=4, medium=3, large=3)
    → expect tiny to dominate slot allocation; S3 request rate peaks; bandwidth stays low
  Verification: SHA-256 of every batch file must be IDENTICAL between Run A and Run B.

Output spec name: DS-P6-01_profile_comparison.yaml
```

---

### Phase 7 Prompts

---

**PROMPT [DS-P7-01] — Mixed Pipeline: Small Scale CI Baseline ~300 GB**

```
Generate dataset DS-P7-01.
Purpose: fast end-to-end CI sanity check. All phases run: scan → batch → upload → fallback → verify.
Profile: dt2_100gbe.

Tier breakdown:
  zero seeds: 5,000 files, 0 bytes
  tiny:   80,000 files, uniform random 200 KB – 800 KB → ~32 GB
  small:  6,000 files, uniform random 1 MB – 10 MB → ~30 GB
  medium: 300 files, uniform random 200 MB – 400 MB → ~90 GB
  large:  20 files, uniform random 5 GB – 12 GB → ~160 GB

Total: 91,320 files, ~312 GB.
All 10 filename variants + all 12 file types across all tiers.
Directory layout: 3 levels.

Pass criteria to embed in spec:
  - final_report: 100% OK, 0 MISSING, 0 FAILED
  - slot distribution 6:4:3:3 observed (sample every 5s for 60s during upload phase)
  - scan_state=complete written before verification starts
  - zero xattr calls on hot path

Output spec name: DS-P7-01_mixed_pipeline_small_300GB.yaml
```

---

**PROMPT [DS-P7-02] — Mixed Pipeline: Medium Scale Regression ~3 TB**

```
Generate dataset DS-P7-02.
Purpose: regular regression run. Large enough to exercise fallback worker and per-tier completion summary.
Profile: dt2_100gbe.

Tier breakdown:
  zero seeds: 50,000 files, 0 bytes
  tiny:   500,000 files, uniform random 200 KB – 800 KB → ~200 GB
  small:  30,000 files, uniform random 1 MB – 20 MB → ~300 GB
  medium: 2,000 files, uniform random 300 MB – 700 MB → ~1 TB
  large:  150 files, uniform random 5 GB – 20 GB → ~1.5 TB

Total: 582,150 files, ~3 TB.
All 10 variants + 12 types. Directory layout: 4 levels.

Inject for fallback testing (annotate in spec — do NOT generate corrupt files, just mark which paths should receive injected failures at test runtime):
  - Mark 1,500 tiny files as "inject_s3_error_rate: 1%" (used by proxy fault injector at runtime)

Pass criteria:
  - All files OK after fallback drains retries
  - Per-tier counts in final verification summary match this spec

Output spec name: DS-P7-02_mixed_pipeline_medium_3TB.yaml
```

---

**PROMPT [DS-P7-03] — Mixed Pipeline: Full Scale Performance Baseline ~10 TB**

```
Generate dataset DS-P7-03.
Purpose: primary regression baseline. Wall time recorded on first run; future runs flagged as regression if >115% of baseline.
Profile: dt2_100gbe.

Tier breakdown:
  zero seeds: 100,000 files, 0 bytes
  tiny:   1,000,000 files, uniform random 200 KB – 800 KB → ~500 GB
  small:  60,000 files, uniform random 5 MB – 50 MB → ~1.2 TB
  medium: 6,000 files, uniform random 400 MB – 1 GB → ~4.2 TB
  large:  200 files, uniform random 10 GB – 30 GB → ~4 TB

Total: 1,166,200 files, ~9.9 TB.
All 10 variants + 12 types. Directory layout: 4 levels, up to 500 files per leaf.

Performance targets to record (annotate in spec):
  - tiny throughput: >= 12,500 files/sec (regression threshold: < 90% of baseline)
  - large bandwidth: >= 9,500 MB/sec (regression threshold: < 85% of baseline)
  - slot distribution: 6:4:3:3 (±5%) for first 60s of sustained transfer
  - scan time: record as first-run baseline (regression if > 115% of baseline)
  - verification time: record as first-run baseline (regression if > 120%)

Output spec name: DS-P7-03_mixed_pipeline_full_10TB.yaml
```

---

### Phase 8 Prompts

---

**PROMPT [DS-P8-01] — Empty Source Directory**

```
Generate dataset DS-P8-01.
Source directory: exists on disk. Contains 3 empty subdirectories (no files anywhere).
Total files: 0. Total data: 0.
Pass criteria: scan_state=complete, 0 batch files created, source.index exists and is 0 bytes, exit code 0.
Output spec name: DS-P8-01_empty_source.yaml
```

---

**PROMPT [DS-P8-02] — Single Zero-Byte File**

```
Generate dataset DS-P8-02.
Source directory: contains exactly 1 file.
File: 0 bytes. Filename uses FN-08 (Unicode emoji: e.g. 数据文件_🚀.bin).
Total files: 1. Total data: 0 bytes.
Pass criteria: 1 batch in zero tier, 1 NUL record in source.index, exit code 0, final report shows 1 OK.
Output spec name: DS-P8-02_single_zero_byte.yaml
```

---

**PROMPT [DS-P8-03] — Single Huge File, 100 GB**

```
Generate dataset DS-P8-03.
Source directory: contains exactly 1 file.
File: exactly 107,374,182,400 bytes = 100 GB. Filename uses FN-07 (240-char name).
Total files: 1. Total data: 100 GB.
Pass criteria:
  - 1 batch in large tier
  - S3 access log confirms multipart upload (parts of configured CHUNK_SIZE_MB)
  - Zero incomplete multipart parts after transfer
  - source.index: 1 record with correct size and mtime
Output spec name: DS-P8-03_single_100gb.yaml
```

---

**PROMPT [DS-P8-04] — Deep Directory Tree (14 Levels)**

```
Generate dataset DS-P8-04.
Directory structure: exactly 14 levels of nested directories.
  At each depth level (1–14): create 5 files per leaf directory at that depth.
  Number of leaf directories per level: 10 (so 5 files × 10 dirs × 14 levels = 700 files total).
  NOT just leaf files — files at every depth level.
File sizes: uniform random 100 KB – 5 MB (mix of tiny and small bucket).
Total files: 700. Total data: ~2 GB.
All 10 filename variants distributed round-robin across files.
All 12 file types distributed round-robin.
Pass criteria:
  - Scanner completes without stack overflow
  - Resume after kill at any depth level produces correct file count on restart
  - All paths (including deepest) survive batch round-trip byte-for-byte
Output spec name: DS-P8-04_deep_tree_14levels.yaml
```

---

**PROMPT [DS-P8-05] — Unreadable Subdirectory**

```
Generate dataset DS-P8-05.
Source directory structure:
  - 500 readable files, size range 1 KB – 1 MB (tiny bucket), in readable subdirectories
  - 1 subdirectory that should be marked chmod 000 (unreadable) AFTER generation
    containing 200 additional files (which should NOT appear in source.index)
Total generated files: 700. Files visible to scanner: 500.
All 10 filename variants across the 500 readable files.
All 12 file types.
Pass criteria:
  - source.index contains exactly 500 records
  - scan_errors.log contains exactly 1 entry (for the unreadable directory)
  - No crash or hang
  - Exit code 0 (partial-scan-with-errors is not a fatal failure)
Runtime instruction (not generated, done at test setup):
  After dataset generation, run: chmod 000 <source_dir>/unreadable_subdir/
Output spec name: DS-P8-05_unreadable_subdir.yaml
```

---

## JSON Schema Snapshot

The following is the canonical JSON field structure for each entry in `dataset_registry.json`. The full populated registry will be generated in a separate step using these prompts as input.

```json
{
  "dataset_id": "DS-P1-02",
  "phase": 1,
  "phase_name": "Single-Tier Isolation",
  "name": "Tiny-Tier Pure, ~500 GB",
  "spec_file": "DS-P1-02_tiny_pure_1M_500GB.yaml",
  "active_tiers": ["zero", "tiny"],
  "scheduler_profile": "dt2_100gbe",
  "total_files": 1010000,
  "total_data_bytes": 536870912000,
  "total_data_human": "~500 GB",
  "zero_seed_files": 10000,
  "tier_specs": {
    "zero": {
      "file_count": 10000,
      "size_range_bytes": [0, 0],
      "avg_size_bytes": 0,
      "total_bytes": 0,
      "expected_batches": null
    },
    "tiny": {
      "file_count": 1000000,
      "size_range_bytes": [1, 1048576],
      "sub_ranges": [
        {"range": "1B–10KB",    "count": 200000},
        {"range": "10KB–100KB", "count": 300000},
        {"range": "100KB–1MB",  "count": 500000}
      ],
      "avg_size_bytes": 500000,
      "total_bytes": 500000000000,
      "expected_batches": 1953,
      "primary_seal_trigger": "byte_and_count_mixed",
      "batch_max_files": 2000,
      "batch_target_bytes": 268435456,
      "open_batches": 8
    },
    "small":  null,
    "medium": null,
    "large":  null
  },
  "filename_variants": {
    "FN-01_plain_ascii":          {"count": 100000, "example": "report_2024.csv"},
    "FN-02_spaces":               {"count": 100000, "example": "my report.csv"},
    "FN-03_trailing_space":       {"count": 100000, "byte_sequence": "0x20 at end"},
    "FN-04_embedded_newline":     {"count": 100000, "byte_sequence": "0x0A in name"},
    "FN-05_trailing_cr":          {"count": 100000, "byte_sequence": "0x0D at end"},
    "FN-06_latin1_bytes":         {"count": 100000, "byte_sequence": "0x80–0xFF range"},
    "FN-07_long_name_240plus":    {"count": 100000, "min_length_chars": 240},
    "FN-08_unicode_cjk_emoji":    {"count": 100000, "example": "数据文件_🚀.bin"},
    "FN-09_double_or_no_ext":     {"count": 100000, "example": "archive.tar.gz"},
    "FN-10_mixed_worst_case":     {"count": 100000, "example": "données export\\r"}
  },
  "file_types": {
    "csv": 0.0833, "json": 0.0833, "txt": 0.0833, "log": 0.0833,
    "sql": 0.0833, "bin": 0.0833, "gz": 0.0833, "tar": 0.0833,
    "parquet": 0.0833, "jpg": 0.0833, "mp4": 0.0833, "no_ext": 0.0837
  },
  "directory_layout": {"depth": 4, "approx_files_per_leaf": 500},
  "batch_exhaustion": {
    "exhaust_first_tier": null,
    "exhaust_together_tiers": null,
    "last_tier_standing": null
  },
  "test_cases_covered": ["planv2-1.1", "planv2-2.1", "planv2-2.4-tiny", "planv2-1.2-round-robin"],
  "performance_expectations": {
    "bottleneck": "s3_request_rate",
    "expected_throughput_files_per_sec": 12500,
    "expected_network_bandwidth_pct_of_link": "<30%",
    "regression_threshold_pct": 90
  },
  "fault_injection": null,
  "pass_criteria": [
    "final_report: all files OK, 0 MISSING, 0 FAILED",
    "no getxattr/setxattr calls during enumeration",
    "scan_state=complete before verification"
  ]
}
```

---

---

---

## Phase 9 — Single-File Transfer Tests

**Goal:** Prove that a batch containing exactly ONE file is handled correctly at every bucket boundary. These are the atomic unit tests of the transfer pipeline — each dataset exercises a different bucket assignment and a different S3 upload path (single PUT vs multipart).

---

### DS-P9-01 — Single 1 B File (Tiny, absolute minimum)

| Field | Value |
|---|---|
| Active tiers | `tiny` only |
| Total files | 1 |
| File size | Exactly 1 byte |
| Filename | FN-04 (embedded newline 0x0A) |

Expected: 1 batch in `tiny` tier, S3 PUT = single-part (1B ≪ 64MB threshold), HeadObject confirms size=1.

---

### DS-P9-02 — Single File at Exactly 1 MB (tiny→small boundary)

| Field | Value |
|---|---|
| Active tiers | `small` (1MB = first byte of small range) |
| Total files | 1 |
| File size | Exactly 1,048,576 bytes = 1 MB |
| Filename | FN-12 (shell metacharacters: $, !, \|, ;) |

Expected: file routes to `small` bucket (NOT tiny). Single-part PUT.

---

### DS-P9-03 — Single 63 MB File (just below multipart threshold)

| Field | Value |
|---|---|
| Active tiers | `small` only |
| Total files | 1 |
| File size | Exactly 66,060,288 bytes = 63 MB |
| Filename | FN-16 (name starts with dash: `-filename.bin`) |

Expected: **single-part PUT** (63MB < 64MB threshold). S3 access log shows 1 PUT, zero multipart initiations.

---

### DS-P9-04 — Single 64 MB File (first file requiring multipart)

| Field | Value |
|---|---|
| Active tiers | `small` only |
| Total files | 1 |
| File size | Exactly 67,108,864 bytes = 64 MB |
| Filename | FN-13 (Windows-reserved chars: `file:name.bin`) |

Expected: **multipart upload** (64MB == threshold). S3 access log: `CreateMultipartUpload` + `UploadPart` + `CompleteMultipartUpload`.

---

### DS-P9-05 — Single 100 MB File (small→medium boundary)

| Field | Value |
|---|---|
| Active tiers | `medium` (100MB = first byte of medium range) |
| Total files | 1 |
| File size | Exactly 104,857,600 bytes = 100 MB |
| Filename | FN-18 (zero-width Unicode: U+200B in name) |

Expected: file routes to `medium` bucket (NOT small). Multipart upload.

---

### DS-P9-06 — Single 1 GB File (medium→large boundary)

| Field | Value |
|---|---|
| Active tiers | `large` (1GB = first byte of large range) |
| Total files | 1 |
| File size | Exactly 1,073,741,824 bytes = 1 GB |
| Filename | FN-08 (Unicode emoji: `数据文件_🚀.bin`) |

Expected: file routes to `large` bucket (NOT medium). Multipart upload.

---

### DS-P9-07 — Single 100 GB File (large, maximum single-file stress)

| Field | Value |
|---|---|
| Active tiers | `large` only |
| Total files | 1 |
| File size | Exactly 107,374,182,400 bytes = 100 GB |
| Filename | FN-07 (240-char name) |
| Total data | 100 GB |

Expected: multipart with 100GB / CHUNK_SIZE_MB parts. Zero incomplete multipart parts after transfer. HeadObject confirms exactly 100GB.

---

---

## Phase 10 — Sub-Range Isolation (Aligned with Existing Spec Files)

**Goal:** Test narrow size bands that correspond directly to the existing `spec_*.yaml` files. Every dataset here has a named counterpart in the existing spec library.

**Alignment note:** Phase 10 datasets intentionally mirror the existing spec naming pattern (`spec_<lo>_<hi>_<N>files_<size>.yaml`). Before re-generating, verify whether the matching existing spec can be reused.

---

### DS-P10-01 — 0 B to 1 MB combined (Zero + Tiny), 1 M files, ~493 GB

Matches: `spec_0bytes_1mb_1mill_files_488gb.yaml`

| Tier | Files | Size range | Total data |
|---|---|---|---|
| `zero` | 50,000 | 0 B | 0 |
| `tiny` | 950,000 | 1 B – 1 MB | ~493 GB |
| **Total** | **1,000,000** | | **~493 GB** |

Batch math (tiny): byte-seal dominates (avg ~518 KB → ~494 files/batch). ~1,953 batches.

---

### DS-P10-02 — 1 B to 10 KB (Sub-tiny), 1 M files, ~5 GB

Matches: `spec_1bytes_10kb_1mill_files_4gb.yaml`

| Tier | Files | Size range | Total data |
|---|---|---|---|
| `tiny` | 1,000,000 | 1 B – 10 KB | ~5 GB |
| **Total** | **1,000,000** | | **~5 GB** |

Batch math: **count-seal dominates** (avg ~5 KB → 2000 files before 256 MB). Exactly 500 count-sealed batches.

---

### DS-P10-03 — 10 KB to 1 MB (Main tiny band), 1 M files, ~500 GB

Matches: `spec_10kb_1mb_1mill_files_493gb.yaml`

| Tier | Files | Size range | Total data |
|---|---|---|---|
| `tiny` | 1,000,000 | 10 KB – 1 MB | ~500 GB |
| **Total** | **1,000,000** | | **~500 GB** |

Batch math: mixed count-seal (small files) and byte-seal (files near 1 MB). ~1,953 batches.

---

### DS-P10-04 — 1 MB to 4 MB (Small lower band), 500 K files, ~1.25 TB

Matches: `spec_1mb_4mb_halfmill_files_1tb.yaml`

| Tier | Files | Size range | Total data |
|---|---|---|---|
| `small` | 500,000 | 1 MB – 4 MB | ~1.25 TB |
| **Total** | **500,000** | | **~1.25 TB** |

Batch math: **count-seal dominates** (avg ~2.5 MB → 512 files before 2 GB). All files below 64 MB → single-part PUT only.

---

### DS-P10-05 — 4 MB to 16 MB (Small mid band), 500 K files, ~5 TB

Matches: `spec_4mb_16mb_halfmill_files_5tb.yaml`

| Tier | Files | Size range | Total data |
|---|---|---|---|
| `small` | 500,000 | 4 MB – 16 MB | ~5 TB |
| **Total** | **500,000** | | **~5 TB** |

Batch math: **byte-seal dominates** (avg ~10 MB → ~200 files before 2 GB). All files below 64 MB → single-part PUT only.

---

### DS-P10-06 — Fixed 16 MB (Small uniform), 500 K files, ~7.6 TB

Matches: `spec_16mb_1mill_files_15tb.yaml` (capped at 500K files / 7.6TB per scale budget)

| Tier | Files | Size range | Total data |
|---|---|---|---|
| `small` | 500,000 | Exactly 16 MB | ~7.6 TB |
| **Total** | **500,000** | | **~7.6 TB** |

Batch math: byte-seal at exactly 128 files (128 × 16 MB = 2,048 MB). **Every batch is exactly 128 files** — highly predictable for assertion purposes.

---

### DS-P10-07 — 10 GB to 120 GB (Large varied), 30 files, ~2 TB

Matches: `spec_10gb_120gb_30files_2tb.yaml`

| Tier | Files | Size range | Total data |
|---|---|---|---|
| `large` | 30 | 10 GB – 120 GB | ~2 TB |
| **Total** | **30** | | **~2 TB** |

Batch math: byte-seal at 50 GB target; with large size variance, some batches = 1 file. 4–5 batches expected.

---

### DS-P10-08 — 200 GB to 500 GB (Very large files), 10 files, ~3.5 TB

Matches: `spec_200gb_500gb_10files_4tb.yaml`

| Tier | Files | Size range | Total data |
|---|---|---|---|
| `large` | 10 | 200 GB – 500 GB | ~3.5 TB |
| **Total** | **10** | | **~3.5 TB** |

Batch math: 1 file per batch (each > 50 GB target → byte-seal immediately). Exactly 10 batches, each containing exactly 1 file.

---

---

## Phase 11 — Alternative Weight Ratios

**Goal:** Test scheduler slot allocation under non-default weight profiles. Files are created in exact proportion to each weight ratio so the expected vs actual slot distribution can be directly compared.

**Alignment note:** These correspond to the `spec_7_5_3_1_*`, `spec_9_5_2_0_*`, and `spec_10_6_0_0_*` files in the existing spec library.

---

### DS-P11-01 — Weight Ratio 7:5:3:1 (large-heavy, tiny-minimal)

Configure: `weights: { large: 7, medium: 5, small: 3, tiny: 1 }`

| Tier | Files | Avg size | Total data | Byte-weight share |
|---|---|---|---|---|
| `large` | 140 | 15 GB | ~2.2 TB | 7/16 = 43.75% |
| `medium` | 3,200 | 500 MB | ~1.6 TB | 5/16 = 31.25% |
| `small` | 188,000 | 5 MB | ~940 GB | 3/16 = 18.75% |
| `tiny` | 780,000 | 400 KB | ~312 GB | 1/16 = 6.25% |
| Zero seeds | ~7,800 | 0 B | 0 | — |
| **Total** | **~979,140** | | **~5 TB** | |

**Pass criteria:** observed slot distribution converges to 7:5:3:1 (±5%) within 3 scheduling cycles.

---

### DS-P11-02 — Weight Ratio 9:5:2:0 (large-dominant, no tiny workers)

Configure: `weights: { large: 9, medium: 5, small: 2, tiny: 0 }`

Tiny weight = 0 → no tiny workers. 16 workers split across large, medium, small only.

| Tier | Files | Avg size | Total data | Byte-weight share |
|---|---|---|---|---|
| `large` | 187 | 15 GB | ~2.8 TB | 9/16 = 56.25% |
| `medium` | 3,200 | 500 MB | ~1.6 TB | 5/16 = 31.25% |
| `small` | 125,000 | 5 MB | ~625 GB | 2/16 = 12.5% |
| `tiny` | 0 | — | 0 | 0% (weight=0) |
| Zero seeds | ~1,250 | 0 B | 0 | — |
| **Total** | **~129,637** | | **~5 TB** | |

**Pass criteria:** tiny tier has 0 workers at all times. 9:5:2 split across large/medium/small.

---

### DS-P11-03 — Weight Ratio 10:6:0:0 (large + medium only)

Configure: `weights: { large: 10, medium: 6, small: 0, tiny: 0 }`

All 16 workers split between large and medium.

| Tier | Files | Avg size | Total data | Byte-weight share |
|---|---|---|---|---|
| `large` | 207 | 15 GB | ~3.1 TB | 10/16 = 62.5% |
| `medium` | 3,800 | 500 MB | ~1.9 TB | 6/16 = 37.5% |
| `small` | 0 | — | 0 | 0% (weight=0) |
| `tiny` | 0 | — | 0 | 0% (weight=0) |
| Zero seeds | ~40 | 0 B | 0 | — |
| **Total** | **~4,047** | | **~5 TB** | |

**Pass criteria:** small and tiny have 0 workers at all times. 10:6 split between large:medium.

---

---

## Review: Alignment with Existing Spec Files

The existing `spec_*.yaml` files use two naming conventions:

| Convention | Examples | Covered by |
|---|---|---|
| `spec_<lo>_<hi>_<N>files_<size>` — single size band | `spec_0bytes_1mb_*`, `spec_10kb_1mb_*`, `spec_4mb_16mb_*` | Phase 10 (DS-P10-01 through DS-P10-08) |
| `spec_<W1>_<W2>_<W3>_<W4>_<N>files_<size>` — weight-ratio mixed | `spec_6_4_3_3_*`, `spec_7_5_3_1_*`, `spec_9_5_2_0_*`, `spec_10_6_0_0_*` | Phase 7 (6:4:3:3) and Phase 11 (other ratios) |

**What existing specs do NOT cover (our additions):**

| Gap | Covered by |
|---|---|
| 20 filename special-character variants | Phases 4 and 9 |
| 26 file types explicitly | Phase 5 |
| Batch seal mechanics (count-seal, byte-seal triggers) | Phase 2 |
| Dynamic weight shift / tier exhaustion (all 6 scenarios) | Phase 3 |
| Single-file edge cases at every bucket boundary | Phase 9 |
| Empty source, deep tree, unreadable directory | Phase 8 |
| Network profile comparison (same data, two profiles) | Phase 6 |
| Multipart threshold boundary (63 MB vs 64 MB) | DS-P9-03, DS-P9-04 |

**Direct mapping of existing specs to this plan:**

| Existing spec | Our equivalent |
|---|---|
| `spec_0bytes_1mb_*` | DS-P10-01 |
| `spec_1bytes_10kb_*` | DS-P10-02 |
| `spec_10kb_1mb_*` | DS-P10-03 |
| `spec_1mb_4mb_*` | DS-P10-04 |
| `spec_4mb_16mb_*` | DS-P10-05 |
| `spec_16mb_*` | DS-P10-06 |
| `spec_10gb_120gb_*` | DS-P10-07 |
| `spec_200gb_500gb_*` | DS-P10-08 |
| `spec_6_4_3_3_*` | DS-P7-03 (full scale) |
| `spec_7_5_3_1_*` | DS-P11-01 |
| `spec_9_5_2_0_*` | DS-P11-02 |
| `spec_10_6_0_0_*` | DS-P11-03 |

**Scale note:** Existing specs go up to 1,119 TB. Our plan is capped at 10 TB per dataset. Phases 10 and 11 can be scaled up by increasing file counts using the same size ratios and weight distributions.

---

---

## Generation Prompts for Phases 9–11

---

### Phase 9 Prompts

---

**PROMPT [DS-P9-01] — Single 1 B File**

```
Generate dataset DS-P9-01.
Total files: 1. File size: exactly 1 byte. Bucket: tiny.
Filename: FN-04 variant (embedded newline byte 0x0A in name — raw byte, not escaped).
File type: .bin (1 byte of random data).
Directory: flat (source root only).
Pass criteria: 1 batch in tiny tier, single-part PUT, HeadObject size = 1 byte.
Output spec name: DS-P9-01_single_1B.yaml
```

---

**PROMPT [DS-P9-02] — Single 1 MB File (tiny→small boundary)**

```
Generate dataset DS-P9-02.
Total files: 1. File size: exactly 1,048,576 bytes = 1 MB.
Bucket: small (1MB is the first byte size assigned to small bucket, not tiny).
Filename: FN-12 variant (shell metacharacters in name: file$name!pipe.bin).
File type: .bin.
Pass criteria: file routes to small bucket (NOT tiny). Single-part PUT.
Output spec name: DS-P9-02_single_1MB_tiny_small_boundary.yaml
```

---

**PROMPT [DS-P9-03] — Single 63 MB File (below multipart threshold)**

```
Generate dataset DS-P9-03.
Total files: 1. File size: exactly 66,060,288 bytes = 63 MB. Bucket: small.
Filename: FN-16 variant (name starts with dash: -filename.bin).
File type: .gz.
CRITICAL: 63 MB is BELOW the 64 MB multipart threshold.
Pass criteria: SINGLE-PART PUT only. S3 access log must show zero CreateMultipartUpload calls.
Output spec name: DS-P9-03_single_63MB_single_part.yaml
```

---

**PROMPT [DS-P9-04] — Single 64 MB File (first multipart file)**

```
Generate dataset DS-P9-04.
Total files: 1. File size: exactly 67,108,864 bytes = 64 MB. Bucket: small.
Filename: FN-13 variant (Windows-reserved chars: file:name.bin).
File type: .bin.
CRITICAL: 64 MB is AT the multipart threshold — this is the FIRST file size that MUST use multipart.
Pass criteria: S3 access log shows CreateMultipartUpload + UploadPart(s) + CompleteMultipartUpload.
  A single-part PUT for this size is a test FAILURE.
Output spec name: DS-P9-04_single_64MB_first_multipart.yaml
```

---

**PROMPT [DS-P9-05] — Single 100 MB File (small→medium boundary)**

```
Generate dataset DS-P9-05.
Total files: 1. File size: exactly 104,857,600 bytes = 100 MB.
Bucket: medium (100MB is the first byte size assigned to medium bucket, not small).
Filename: FN-18 variant (zero-width Unicode character U+200B embedded in name).
File type: .parquet.
Pass criteria: file routes to medium bucket (NOT small). Multipart upload.
Output spec name: DS-P9-05_single_100MB_small_medium_boundary.yaml
```

---

**PROMPT [DS-P9-06] — Single 1 GB File (medium→large boundary)**

```
Generate dataset DS-P9-06.
Total files: 1. File size: exactly 1,073,741,824 bytes = 1 GB.
Bucket: large (1GB is the first byte size assigned to large bucket, not medium).
Filename: FN-08 variant (Unicode emoji in name: 数据文件_🚀.bin).
File type: .mp4.
Pass criteria: file routes to large bucket (NOT medium). Multipart upload.
Output spec name: DS-P9-06_single_1GB_medium_large_boundary.yaml
```

---

**PROMPT [DS-P9-07] — Single 100 GB File**

```
Generate dataset DS-P9-07.
Total files: 1. File size: exactly 107,374,182,400 bytes = 100 GB. Bucket: large.
Filename: FN-07 variant (name >= 240 characters: aaaa...aaa.bin).
File type: .bin (100 GB of random bytes).
Pass criteria:
  - Multipart upload with parts of exactly CHUNK_SIZE_MB bytes each
  - S3 access log: CompleteMultipartUpload with correct final ETag
  - Zero incomplete multipart uploads remaining after transfer completes
  - HeadObject confirms Content-Length = 107,374,182,400 bytes exactly
Output spec name: DS-P9-07_single_100GB.yaml
```

---

### Phase 10 Prompts

---

**PROMPT [DS-P10-01] — 0 B to 1 MB, 1 M files**

```
Generate dataset DS-P10-01.
Aligns with existing: spec_0bytes_1mb_1mill_files_488gb.yaml
Total files: 1,000,000.
File breakdown:
  - 50,000 files: exactly 0 bytes (zero bucket)
  - 950,000 files: uniform random 1 B – 1,048,576 B (tiny bucket)
Target total data: ~493 GB.
All 20 filename variants: 50,000 files per variant across all files.
All 26 file types: distributed round-robin.
Directory layout: 3 levels deep, ~330 files per leaf.
Batch expectations: byte-seal dominates in tiny (avg ~518KB).
Output spec name: DS-P10-01_0bytes_1mb_1mill_493GB.yaml
```

---

**PROMPT [DS-P10-02] — 1 B to 10 KB, 1 M files**

```
Generate dataset DS-P10-02.
Aligns with existing: spec_1bytes_10kb_1mill_files_4gb.yaml
Total files: 1,000,000.
File sizes: uniform random 1 B – 10,240 B (all in tiny bucket, all well below 256MB batch target).
Target total data: ~5 GB.
All 20 filename variants: 50,000 per variant.
All 26 file types: round-robin.
Batch expectations: COUNT-SEAL dominates (avg ~5KB × 2000 files = 10MB per batch << 256MB).
  Expected batches: ~500 count-sealed batches of exactly 2000 files each.
Directory layout: 3 levels.
Output spec name: DS-P10-02_1bytes_10kb_1mill_5GB.yaml
```

---

**PROMPT [DS-P10-03] — 10 KB to 1 MB, 1 M files**

```
Generate dataset DS-P10-03.
Aligns with existing: spec_10kb_1mb_1mill_files_493gb.yaml
Total files: 1,000,000.
File sizes: uniform random 10,240 B – 1,048,576 B (all in tiny bucket).
Target total data: ~500 GB.
All 20 filename variants: 50,000 per variant.
All 26 file types: round-robin.
Batch expectations: mix — count-seal for files near 10KB, byte-seal for files near 1MB.
Directory layout: 4 levels.
Output spec name: DS-P10-03_10kb_1mb_1mill_500GB.yaml
```

---

**PROMPT [DS-P10-04] — 1 MB to 4 MB, 500 K files**

```
Generate dataset DS-P10-04.
Aligns with existing: spec_1mb_4mb_halfmill_files_1tb.yaml
Total files: 500,000.
File sizes: uniform random 1,048,576 B – 4,194,304 B (all in small bucket; all < 64MB → single-part PUT).
Target total data: ~1.25 TB.
All 20 filename variants: 25,000 per variant.
All 26 file types: round-robin.
Batch expectations: COUNT-SEAL dominates (avg ~2.5MB × 512 = 1.28GB per batch < 2GB).
  Expected batches: ~977 batches.
Directory layout: 3 levels.
Output spec name: DS-P10-04_1mb_4mb_500k_1TB.yaml
```

---

**PROMPT [DS-P10-05] — 4 MB to 16 MB, 500 K files**

```
Generate dataset DS-P10-05.
Aligns with existing: spec_4mb_16mb_halfmill_files_5tb.yaml
Total files: 500,000.
File sizes: uniform random 4,194,304 B – 16,777,216 B (all in small bucket; all < 64MB → single-part PUT).
Target total data: ~5 TB.
All 20 filename variants: 25,000 per variant.
All 26 file types: round-robin.
Batch expectations: BYTE-SEAL dominates (avg ~10MB × 200 files = 2GB per batch).
  Expected batches: ~2,500 batches.
Directory layout: 3 levels.
Output spec name: DS-P10-05_4mb_16mb_500k_5TB.yaml
```

---

**PROMPT [DS-P10-06] — Fixed 16 MB, 500 K files**

```
Generate dataset DS-P10-06.
Aligns with existing: spec_16mb_1mill_files_15tb.yaml (capped at 500K files)
Total files: 500,000.
File size: ALL files EXACTLY 16,777,216 bytes = 16 MB (uniform, not random — every file identical size).
Target total data: ~7.6 TB.
All 20 filename variants: 25,000 per variant.
All 26 file types: round-robin.
Batch expectations: BYTE-SEAL at exactly 128 files (128 × 16MB = 2048MB ≈ 2GB).
  Every batch contains EXACTLY 128 files — no variance. Total: 3,906 batches.
Directory layout: 3 levels.
Output spec name: DS-P10-06_16mb_fixed_500k_7TB.yaml
```

---

**PROMPT [DS-P10-07] — 10 GB to 120 GB, 30 files**

```
Generate dataset DS-P10-07.
Aligns with existing: spec_10gb_120gb_30files_2tb.yaml
Total files: 30.
File sizes: uniform random 10,737,418,240 B – 128,849,018,880 B (10 GB – 120 GB, all in large bucket).
Target total data: ~2 TB.
Filename variants: applied round-robin to 30 files (1–2 per variant; all 20 variants cycle twice).
File types: round-robin through 26 types (some types appear twice).
Batch expectations: byte-seal (50 GB target) with high size variance; 4–5 batches expected.
Directory layout: flat (all in source root).
Output spec name: DS-P10-07_10gb_120gb_30files_2TB.yaml
```

---

**PROMPT [DS-P10-08] — 200 GB to 500 GB, 10 files**

```
Generate dataset DS-P10-08.
Aligns with existing: spec_200gb_500gb_10files_4tb.yaml
Total files: 10.
File sizes: uniform random 214,748,364,800 B – 536,870,912,000 B (200 GB – 500 GB, all in large bucket).
Target total data: ~3.5 TB.
Filename variants: FN-01 through FN-10 assigned one per file (10 files, 10 variants, 1:1).
File types: assign one type per file round-robin.
Batch expectations: 1 file per batch (each > 50 GB → byte-seal fires immediately after first file).
  Exactly 10 batches, each containing exactly 1 file.
Directory layout: flat (all in source root).
Output spec name: DS-P10-08_200gb_500gb_10files_3TB.yaml
```

---

### Phase 11 Prompts

---

**PROMPT [DS-P11-01] — Weight Ratio 7:5:3:1**

```
Generate dataset DS-P11-01.
Aligns with existing: spec_7_5_3_1_halfmill_files_226tb.yaml (scaled down to ~5 TB)
Scheduler weight config for test: { large: 7, medium: 5, small: 3, tiny: 1 }; total_workers: 16

File counts proportional to byte-weight (target 5 TB total):
  large:   140 files,   uniform random 10 GB – 20 GB  → ~2.2 TB   (7/16 of data)
  medium:  3,200 files, uniform random 300 MB – 700 MB → ~1.6 TB   (5/16)
  small:   188,000 files, uniform random 1 MB – 10 MB  → ~940 GB   (3/16)
  tiny:    780,000 files, uniform random 200 KB – 600 KB → ~312 GB  (1/16)
  zero seeds: 7,800 files, 0 bytes

Total: ~979,140 files, ~5 TB.
All 20 filename variants + all 26 file types distributed across tiers. Directory: 4 levels.
Test annotation in spec: scheduler_profile = "custom_7531"
Pass criteria: slot distribution converges to 7:5:3:1 ratio (±5%) within 3 scheduling cycles.
Output spec name: DS-P11-01_weights_7_5_3_1_1M_5TB.yaml
```

---

**PROMPT [DS-P11-02] — Weight Ratio 9:5:2:0 (no tiny tier)**

```
Generate dataset DS-P11-02.
Aligns with existing: spec_9_5_2_0_halfmill_files_41tb.yaml (scaled to ~5 TB)
Scheduler weight config: { large: 9, medium: 5, small: 2, tiny: 0 }; total_workers: 16
NOTE: tiny weight = 0 → zero tiny workers. Tiny bucket intentionally empty.

File counts (target 5 TB, tiny excluded from data):
  large:   187 files,   uniform random 10 GB – 20 GB  → ~2.8 TB   (9/16)
  medium:  3,200 files, uniform random 300 MB – 700 MB → ~1.6 TB   (5/16)
  small:   125,000 files, uniform random 1 MB – 10 MB  → ~625 GB   (2/16)
  tiny:    0 files (intentionally empty — tests scheduler with weight=0 tier)
  zero seeds: 1,250 files, 0 bytes

Total: ~129,637 files, ~5 TB.
All 20 variants + all 26 types on large/medium/small only. Directory: 3 levels.
Pass criteria: tiny bucket has 0 workers at every observed scheduling cycle. 9:5:2 split.
Output spec name: DS-P11-02_weights_9_5_2_0_130k_5TB.yaml
```

---

**PROMPT [DS-P11-03] — Weight Ratio 10:6:0:0 (large + medium only)**

```
Generate dataset DS-P11-03.
Aligns with existing: spec_10_6_0_0_halfmill_files_9tb.yaml (scaled to ~5 TB)
Scheduler weight config: { large: 10, medium: 6, small: 0, tiny: 0 }; total_workers: 16
NOTE: small and tiny weights = 0 → only large and medium are active tiers.

File counts (target 5 TB):
  large:   207 files,   uniform random 10 GB – 20 GB  → ~3.1 TB   (10/16)
  medium:  3,800 files, uniform random 300 MB – 700 MB → ~1.9 TB   (6/16)
  small:   0 files (intentionally empty)
  tiny:    0 files (intentionally empty)
  zero seeds: 40 files, 0 bytes

Total: ~4,047 files, ~5 TB.
All 20 filename variants + all 26 file types distributed across large and medium only.
Pass criteria: small and tiny have 0 workers at all times. Slot split is exactly 10:6.
Output spec name: DS-P11-03_weights_10_6_0_0_4k_5TB.yaml
```

---

## Next Steps

1. **Review additions** — verify Phase 9 (single-file), Phase 10 (sub-range isolation), and Phase 11 (alt weight ratios) align with your test harness input format.
2. **Cross-reference existing specs** — before re-generating Phase 10 datasets, check whether the matching `spec_*.yaml` files can be reused directly. Phase 10 prompts list the exact existing filename to check.
3. **Generate `dataset_registry.json`** — one JSON object per dataset, all 53 entries total, using the schema above.
4. **Run generation** — Phase 8/9 (edge cases + single-file) and Phase 2 (seal mechanics) generate in seconds. Phase 7 and Phase 10 take the longest.
5. **Phase execution order (cheapest → most expensive):** P8 → P9 → P2 → P4 → P5 → P10 (small-count specs first) → P1 (single tier) → P6 (profile comparison) → P3 (exhaustion) → P11 (alt weights) → P7 (full pipeline).
