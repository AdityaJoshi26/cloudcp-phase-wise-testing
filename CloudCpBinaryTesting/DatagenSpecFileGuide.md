# Datagen Spec File Guide

Complete reference for writing spec files for the `datagen` tool.  
Spec files can be **YAML** (`.yaml` / `.yml`) or **JSON** (`.json`) — the tool auto-detects by extension.

---

## Table of Contents

1. [Quick Concept](#1-quick-concept)
2. [The Four Modes](#2-the-four-modes)
3. [Top-Level Fields](#3-top-level-fields)
4. [Section: `content`](#4-section-content)
5. [Section: `naming`](#5-section-naming)
6. [Section: `size`](#6-section-size)
7. [Section: `extensions`](#7-section-extensions)
8. [Section: `tree`](#8-section-tree)
9. [Section: `flat`](#9-section-flat)
10. [Section: `list`](#10-section-list)
11. [Size String Formats](#11-size-string-formats)
12. [Which Sections Are Required per Mode](#12-which-sections-are-required-per-mode)
13. [Valid Parameter Combinations](#13-valid-parameter-combinations)
14. [Complete Annotated Examples](#14-complete-annotated-examples)

---

## 1. Quick Concept

A spec file tells `datagen`:
- **What structure** to create (a tree, a flat folder, or files from a list)
- **How many files** and where to put them
- **How to name** the files (charset, prefix, length, etc.)
- **How large** each file should be (fixed, range, or per-extension)
- **What content** to write (random bytes, sparse/empty, or a repeating fill byte)

Run it with:
```bash
./build/datagen --spec path/to/your-spec.yaml
```

---

## 2. The Four Modes

| Mode | What It Does |
|------|-------------|
| `tree` | Creates a balanced directory tree: `fanout` subdirs per level × `depth` levels. Files go at leaf directories (or all directories if `files_in_each_dir: true`). |
| `flat` | Creates `num_files` files all inside a single directory (`root`). No subdirectories. |
| `list` | Reads one or more text files. Each line is a path; size comes from extension rules. Lines can optionally include an explicit size (`bytes,path` format). |
| `csv-list` | Like `list`, but **every** line must be `bytes,path`. Lines without a size prefix are rejected. |

---

## 3. Top-Level Fields

These fields sit at the root level of the spec file.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `version` | integer | No | `1` | Spec version. Always `1` for now. |
| `mode` | string | **Yes** | — | One of: `tree`, `flat`, `list`, `csv-list` |
| `root` | string | Yes for `tree`/`flat` | — | Output directory path. Created if it doesn't exist. Ignored in `list`/`csv-list` modes. |
| `threads` | integer | No | hardware concurrency | Number of parallel writer threads. Can be overridden by `--threads` CLI flag. |
| `seed` | integer | No | random | Fixed seed makes filenames and sizes reproducible across runs. Can be overridden by `--seed` CLI flag. |

```yaml
version: 1
mode: tree
root: /tmp/output
threads: 8
seed: 42
```

---

## 4. Section: `content`

Controls **how file data is written**.

```yaml
content:
  type: random
  fill_byte: 0x00
  buffer_size: 8MB
  direct_io:
    enabled: false
    min_size: 256MB
  fsync: false
```

### `content.type`

| Value | Description |
|-------|-------------|
| `random` | **(default)** Fill files with pseudo-random bytes from a PRNG. |
| `sparse` | Call `ftruncate` to the target size. No real data is written; the file is a "hole" on supported filesystems. Very fast; useful for testing metadata at scale. |
| `fill` | Every byte in the file is set to `fill_byte`. |

### All `content` Parameters

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `type` | string | No | `random` | `random` \| `sparse` \| `fill` |
| `fill_byte` | string/int | Only when `type: fill` | `0x00` | Accepts hex (`0xFF`) or decimal (`255`). |
| `buffer_size` | size string | No | `8MB` | Internal write buffer and Direct-I/O stripe size. |
| `direct_io.enabled` | bool | No | `false` | Use O_DIRECT / unbuffered writes (bypasses OS page cache). |
| `direct_io.min_size` | size string | No | `256MB` | Files **smaller** than this use normal buffered I/O even if `direct_io.enabled: true`. |
| `fsync` | bool | No | `false` | Call `fsync` after each file is written (guarantees durability). |

**Rules:**
- `fill_byte` is only meaningful when `type: fill`. It is silently ignored for `random` and `sparse`.
- `direct_io.min_size` is only meaningful when `direct_io.enabled: true`.
- `buffer_size` affects memory usage per thread. Larger = fewer write syscalls but more RAM.

---

## 5. Section: `naming`

Controls **how filenames are generated**. Ignored in `list` and `csv-list` modes (paths come from input files).

```yaml
naming:
  charset: ascii
  alphabet: [lower, digit]
  unicode_blocks:
    - cjk-unified
  special_chars: "_-"
  prefix: "obj-"
  suffix: ""
  length: 12
  collision_strategy: append-index
```

The generated filename looks like:

```
<prefix> + <random middle of length `length`> + <suffix> + <extension>
```

### `naming.charset`

| Value | Description |
|-------|-------------|
| `ascii` | **(default)** Random middle section uses only ASCII characters (from `alphabet` pool + `special_chars`). |
| `unicode` | Random middle section uses only Unicode code points (from `unicode_blocks` + `special_chars`). |
| `mixed` | Random middle section uses both ASCII and Unicode characters. |

### `naming.alphabet` (ASCII character classes)

Used when `charset` is `ascii` or `mixed`. List any combination of the following tokens:

| Token | Characters Included |
|-------|---------------------|
| `lower` | `a-z` |
| `upper` | `A-Z` |
| `digit` | `0-9` |
| `dash` | `-` |

Default: `[lower, digit]`

```yaml
alphabet: [lower, upper, digit, dash]
```

### `naming.unicode_blocks`

Used when `charset` is `unicode` or `mixed`. List any combination:

| Token | Block / Characters |
|-------|--------------------|
| `latin-supplement` | `àáâãäåæçèéêëì…` (Latin Extended) |
| `cyrillic` | `АБВГДЕЁЖ…` (Russian/Slavic scripts) |
| `arabic` | `ابتثجحخد…` (Arabic script) |
| `cjk-unified` | `一丁丂七万…` (Chinese/Japanese/Korean ideographs) |
| `emoji` | `😀🎉🚀🔥💡…` (Emoji) |

```yaml
unicode_blocks:
  - latin-supplement
  - cjk-unified
  - emoji
```

**Rule:** `unicode_blocks` is only meaningful when `charset` is `unicode` or `mixed`. It is ignored when `charset: ascii`.

### All `naming` Parameters

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `charset` | string | No | `ascii` | `ascii` \| `unicode` \| `mixed` |
| `alphabet` | list of strings | No | `[lower, digit]` | Only used for ASCII portion. Any subset of `lower`, `upper`, `digit`, `dash`. |
| `unicode_blocks` | list of strings | No | `[]` | Only used when `charset` is `unicode` or `mixed`. |
| `special_chars` | string | No | `""` | Extra characters always included in the sampling pool (e.g. `" ()[]&_-"`). Applied on top of `alphabet`/`unicode_blocks`. |
| `prefix` | string | No | `""` | Fixed string prepended to every generated name. |
| `suffix` | string | No | `""` | Fixed string appended to the random part (before the extension). |
| `length` | integer | No | `12` | Number of randomly generated characters in the middle section. |
| `collision_strategy` | string | No | `append-index` | `append-index` \| `retry` |

### `naming.collision_strategy`

| Value | Behavior |
|-------|----------|
| `append-index` | **(default)** On collision: `file.txt` → `file-2.txt` → `file-3.txt` → … |
| `retry` | On collision: discard and re-roll a completely new random name. |

---

## 6. Section: `size`

The **default size policy** — applies to all files unless overridden by a per-extension `size` block.

```yaml
size:
  type: fixed
  bytes: 64KB
```

or

```yaml
size:
  type: range
  min: 1MB
  max: 16MB
  distribution: uniform
```

### `size.type`

| Value | Required fields | Description |
|-------|----------------|-------------|
| `fixed` | `bytes` | Every file is exactly `bytes` large. |
| `range` | `min`, `max` | File size is randomly sampled between `min` and `max` (inclusive). |

### `size.distribution` (only for `type: range`)

| Value | Description |
|-------|-------------|
| `uniform` | **(default)** Each size in `[min, max]` is equally likely. |
| `log-uniform` | Sizes are uniform in log-space — biases toward smaller files while still allowing large ones. |
| `normal` | Sizes are normally distributed around the midpoint `(min+max)/2`. Clamped to `[min, max]`. |

### All `size` Parameters

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `type` | string | **Yes** | — | `fixed` \| `range` |
| `bytes` | size string | Yes if `type: fixed` | — | Exact file size. |
| `min` | size string | Yes if `type: range` | — | Lower bound (inclusive). |
| `max` | size string | Yes if `type: range` | — | Upper bound (inclusive). Must be ≥ `min`. |
| `distribution` | string | No | `uniform` | `uniform` \| `log-uniform` \| `normal`. Only applies when `type: range`. |

---

## 7. Section: `extensions`

A **weighted catalogue** of file extensions. `datagen` picks an extension by sampling this list according to weights, then appends it to the generated name.

```yaml
extensions:
  - ext: .txt
    weight: 70
  - ext: .json
    weight: 30
    size: { type: range, min: 1MB, max: 10MB }
  - ext: .mp4
    weight: 15
    size: { type: range, min: 100MB, max: 2GB }
```

### Per-Extension Fields

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `ext` | string | **Yes** | — | The extension, with or without a leading dot. `.mp4` and `mp4` are both accepted. |
| `weight` | integer | No | `1` | Relative sampling weight. Higher = picked more often. All weights are relative to each other. |
| `size` | size block | No | falls back to global `size:` | Per-extension size policy. Uses the same `fixed`/`range` format as the top-level `size` section. |

**Rules:**
- `weight` values do not have to sum to 100; they are relative weights. `{30, 20, 50}` is the same proportion as `{3, 2, 5}`.
- If no `extensions` list is provided, files have no extension (empty suffix after the name).
- In `list` / `csv-list` mode, the `weight` field is irrelevant (extensions come from the input paths). The `size` field still applies for bare-path lines.

---

## 8. Section: `tree`

**Only used when `mode: tree`.**

```yaml
tree:
  fanout: 5
  depth: 3
  files_per_dir: 10
  files_in_each_dir: false
```

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `fanout` | integer | No | `1` | Number of subdirectories created under each parent directory. |
| `depth` | integer | No | `1` | Number of directory levels. |
| `files_per_dir` | integer | No | `1` | Number of files created in each directory that receives files. |
| `files_in_each_dir` | bool | No | `false` | If `false`: files only at leaf directories. If `true`: files at every directory (including intermediate nodes). |

**Total file count formula:**

- `files_in_each_dir: false` (default): `fanout^depth × files_per_dir`
  - Example: fanout=5, depth=3, files_per_dir=10 → 5³ × 10 = **1,250 files**
- `files_in_each_dir: true`: files at all nodes including root and intermediate dirs.
  - Total directories = `(fanout^(depth+1) - 1) / (fanout - 1)`
  - Each gets `files_per_dir` files.

---

## 9. Section: `flat`

**Only used when `mode: flat`.**

```yaml
flat:
  num_files: 500
```

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `num_files` | integer | No | `1` | Total number of files created directly under `root`. No subdirectories are created. |

---

## 10. Section: `list`

**Used when `mode: list` or `mode: csv-list`.**

```yaml
list:
  paths:
    - /tmp/filelist.txt
    - /tmp/extra_files.csv
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `paths` | list of strings | **Yes** | One or more paths to input text files. All input files are concatenated (order preserved). |

### Input File Line Formats

Each line in the input files can be one of:

| Format | Example | Size Determined By |
|--------|---------|--------------------|
| Bare path | `/tmp/out/report.json` | Extension rules in the spec (`extensions` → per-ext `size`, or global `size`) |
| CSV (bytes prefix) | `1MB,/tmp/out/report.json` | The explicit byte count in the line |

**Rules:**
- Blank lines are ignored.
- Lines starting with `#` are ignored (comments).
- In `mode: list` — both bare and CSV lines are accepted.
- In `mode: csv-list` — every line **must** be in `bytes,path` format. Bare paths cause an error.
- Bare-path lines use the extension of the filename to look up the `extensions` catalogue for a size. If no matching extension is found, the global `size` policy applies.

---

## 11. Size String Formats

All size fields (`bytes`, `min`, `max`, `buffer_size`, `direct_io.min_size`, and inline CSV sizes) accept the following formats:

| Format | Value | Example |
|--------|-------|---------|
| Bare integer | Exact bytes | `1024` |
| `K` / `KB` / `Ki` / `KiB` | 1,024 bytes | `4K`, `64KB`, `1Ki` |
| `M` / `MB` / `Mi` / `MiB` | 1,048,576 bytes | `8MB`, `256Mi` |
| `G` / `GB` / `Gi` / `GiB` | 1,073,741,824 bytes | `2GB`, `1Gi` |
| `T` / `TB` / `Ti` / `TiB` | ~1.1 TB | `5TB`, `2Ti` |
| `P` / `PB` / `Pi` / `PiB` | ~1.1 PB | `1PB` |

All suffixes are **case-insensitive**. All units are **binary (base-1024)**.

---

## 12. Which Sections Are Required per Mode

| Section | `tree` | `flat` | `list` | `csv-list` |
|---------|--------|--------|--------|------------|
| `mode` | Required | Required | Required | Required |
| `root` | **Required** | **Required** | Ignored | Ignored |
| `threads` | Optional | Optional | Optional | Optional |
| `seed` | Optional | Optional | Optional | Optional |
| `content` | Optional | Optional | Optional | Optional |
| `naming` | Optional | Optional | **Ignored** (paths come from input) | **Ignored** |
| `size` | Optional (fallback) | Optional (fallback) | Optional (for bare-path lines) | **Ignored** (sizes from CSV) |
| `extensions` | Optional | Optional | Optional (for bare-path lines) | **Ignored** |
| `tree` | **Required** (holds topology) | Ignored | Ignored | Ignored |
| `flat` | Ignored | **Required** (holds `num_files`) | Ignored | Ignored |
| `list` | Ignored | Ignored | **Required** (holds `paths`) | **Required** (holds `paths`) |

---

## 13. Valid Parameter Combinations

### Content Type combinations

| `content.type` | `fill_byte` | `direct_io` | Notes |
|----------------|-------------|-------------|-------|
| `random` | Ignored | Allowed | Default and most common. |
| `sparse` | Ignored | Ignored (no data written) | Fastest; good for metadata-only tests. |
| `fill` | **Used** | Allowed | Must specify `fill_byte`. |

### Size Type combinations

| `size.type` | `bytes` | `min` | `max` | `distribution` |
|-------------|---------|-------|-------|----------------|
| `fixed` | **Required** | Ignored | Ignored | Ignored |
| `range` | Ignored | **Required** | **Required** | Optional (`uniform` default) |

### Naming charset combinations

| `charset` | `alphabet` | `unicode_blocks` | `special_chars` |
|-----------|------------|-----------------|-----------------|
| `ascii` | Used | **Ignored** | Appended to ASCII pool |
| `unicode` | **Ignored** | Used | Appended to Unicode pool |
| `mixed` | Used (ASCII portion) | Used (Unicode portion) | Appended to both pools |

### Mode + required topology section

| `mode` | Required topology section | Root needed |
|--------|--------------------------|-------------|
| `tree` | `tree:` | Yes |
| `flat` | `flat:` | Yes |
| `list` | `list:` with `paths` | No |
| `csv-list` | `list:` with `paths` | No |

### Extensions catalogue — when it applies

| Mode | Extension `weight` used | Extension `size` used |
|------|------------------------|----------------------|
| `tree` | Yes | Yes (overrides global `size`) |
| `flat` | Yes | Yes (overrides global `size`) |
| `list` (bare-path lines) | No (ext is in the path) | Yes (looked up by path's extension) |
| `list` (CSV lines) | No | No (size is explicit) |
| `csv-list` | No | No |

---

## 14. Complete Annotated Examples

### Example 1 — Directory Tree

Creates `5^3 = 125` leaf dirs × `10` files = **1,250 files** in a balanced tree. Large media files and small JSON/text.

```yaml
version: 1
mode: tree
root: /tmp/datagen_tree
threads: 8
seed: 42

content:
  type: random
  buffer_size: 8MB
  direct_io:
    enabled: true
    min_size: 256MB    # only files >= 256MB use O_DIRECT
  fsync: false

naming:
  charset: ascii
  alphabet: [lower, digit]
  prefix: "obj-"       # every filename starts with "obj-"
  length: 12
  collision_strategy: append-index

tree:
  fanout: 5
  depth: 3
  files_per_dir: 10
  files_in_each_dir: false    # files only at leaf dirs

size:
  type: range
  min: 1MB
  max: 16MB
  distribution: uniform       # default fallback size

extensions:
  - ext: .json
    weight: 30
    size: { type: range, min: 1MB, max: 10MB }
  - ext: .xml
    weight: 20                # uses global size (1MB–16MB)
  - ext: .txt
    weight: 30                # uses global size (1MB–16MB)
  - ext: .mp4
    weight: 15
    size: { type: range, min: 100MB, max: 2GB }
  - ext: .mxf
    weight: 5
    size: { type: range, min: 500MB, max: 5GB }
```

---

### Example 2 — Flat Directory with Unicode Filenames

Creates **500 files** all in one folder with Unicode/emoji/CJK names.

```yaml
version: 1
mode: flat
root: /tmp/datagen_flat
threads: 4

content:
  type: random
  buffer_size: 4MB
  direct_io: { enabled: false }

naming:
  charset: mixed                  # both ASCII and Unicode characters
  alphabet: [lower, upper, digit]
  unicode_blocks:
    - latin-supplement
    - cjk-unified
    - emoji
  special_chars: " ()[]&_-"       # extra characters added to the pool
  length: 16

flat:
  num_files: 500

size:
  type: fixed
  bytes: 64KB                     # every file is exactly 64 KB

extensions:
  - ext: .txt
    weight: 70
  - ext: .json
    weight: 30
```

---

### Example 3 — List Mode (mixed bare + CSV lines)

Reads path lists from files. Bare paths get sizes from extension rules; CSV lines use exact sizes.

```yaml
version: 1
mode: list
threads: 8

content:
  type: random
  buffer_size: 8MB
  direct_io:
    enabled: true
    min_size: 256MB

size:                             # fallback for bare-path lines with no extension match
  type: range
  min: 1KB
  max: 1MB

extensions:                       # sizes for bare-path lines, looked up by extension
  - ext: .json
    size: { type: range, min: 1MB, max: 10MB }
  - ext: .mp4
    size: { type: range, min: 1GB, max: 5GB }
  - ext: .mxf
    size: { type: range, min: 500MB, max: 2GB }
  - ext: .txt
    size: { type: range, min: 1KB, max: 4KB }

list:
  paths:
    - /tmp/inputs/filelist.txt    # can contain both bare paths and CSV lines
    - /tmp/inputs/exact.csv       # can also contain both forms
```

Sample content of `/tmp/inputs/filelist.txt`:
```
# Bare paths — size determined by extension rules above
/tmp/out/reports/q1.json
/tmp/out/media/clip-001.mp4
/tmp/out/notes/readme.txt

# CSV lines — exact size specified
1024,/tmp/out/exact/tiny.bin
2GB,/tmp/out/exact/large.bin
512KB,/tmp/out/exact/medium.bin
```

---

### Example 4 — CSV-List Mode (all exact sizes)

Every line **must** have a size prefix. No extension rules needed.

```yaml
version: 1
mode: csv-list
threads: 4

content:
  type: sparse    # just ftruncate to size; no real data written (fastest)

list:
  paths:
    - /tmp/inputs/exact_sizes.csv
```

Sample `/tmp/inputs/exact_sizes.csv`:
```
1MB,/data/file1.bin
256KB,/data/file2.bin
10GB,/data/largefile.img
```

---

### Example 5 — Fill Content (all zeros / specific byte)

Creates files filled with a repeating byte value.

```yaml
version: 1
mode: flat
root: /tmp/zero_files
threads: 4

content:
  type: fill
  fill_byte: 0x00    # fill with null bytes (all zeros)
  buffer_size: 16MB
  fsync: true        # flush to disk after each file

naming:
  charset: ascii
  alphabet: [lower, digit]
  prefix: "zeros-"
  length: 8

flat:
  num_files: 100

size:
  type: fixed
  bytes: 1MB

extensions:
  - ext: .bin
    weight: 1
```

---

### Example 6 — Reproducible Tree (fixed seed)

Using `seed` makes all filenames and sizes identical across runs — useful for regression testing.

```yaml
version: 1
mode: tree
root: /tmp/repro_tree
seed: 12345          # fixed seed = same output every run

content:
  type: random

naming:
  charset: ascii
  alphabet: [lower, digit]
  length: 10
  collision_strategy: retry    # re-roll on collision instead of appending index

tree:
  fanout: 3
  depth: 2
  files_per_dir: 5

size:
  type: range
  min: 4KB
  max: 128KB
  distribution: log-uniform    # biased toward smaller files

extensions:
  - ext: .dat
    weight: 1
```

---

## CLI Override Flags

These CLI flags override values set in the spec file:

| Flag | Overrides | Example |
|------|-----------|---------|
| `--threads N` | `threads:` | `--threads 16` |
| `--seed N` | `seed:` | `--seed 99` |
| `--dry-run` | — | Print plan, write nothing |
| `--verbose` / `-v` | — | Print every file path as written |

**Shortcuts that bypass the spec file entirely:**

| Flag | Equivalent to |
|------|--------------|
| `--list PATH` | `mode: list` with the given file as the sole path |
| `--csv-list PATH` | `mode: csv-list` with the given file as the sole path |
