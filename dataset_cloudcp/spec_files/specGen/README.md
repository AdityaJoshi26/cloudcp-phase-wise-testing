# specfile_engine.py — Datagen Spec-File Generation Engine

Generate [datagen](https://en.wikipedia.org/wiki/Synthetic_data) spec files (`.yaml`) from a
**compact requirement**. You describe *what* you want — a size, a file count, a capacity budget,
optionally split into size **tiers** — and the engine emits ready-to-run spec files with an exact
topology, a size policy, a filename policy, and a weighted file-type catalogue.

The engine is fully self-contained: it does **not** read any sibling files at run time
(`plan_specfile_engine.md`, `dataset_map.json`, etc.). All domain knowledge is baked into the script.

---

## 1. Requirements

- **Python 3.8+** (standard library only).
- **Optional:** `pyyaml` — needed **only** if you pass a `.yaml` / `.yml` *input* file
  (`pip install pyyaml`). JSON / JSONL / CLI need nothing extra.

---

## 2. Quick start

```powershell
# See the 12 categories
python specfile_engine.py --list-categories

# One dataset from the CLI: 1000 files sized 1MB–5MB, 3 levels deep
python specfile_engine.py --size "1MB-5MB" --files 1000 --depth 3 --id DS-DEMO

# Build the whole corpus from the bundled input file
python specfile_engine.py --input input.jsonl --out generated_specs

# Plan only — validate and print, write nothing
python specfile_engine.py --input input.jsonl --dry-run
```

---

## 3. Core concepts

### 3.1 The "any two of three" rule (reconciler)
A dataset is defined by three related quantities:

| Field            | Meaning                          |
|------------------|----------------------------------|
| `size`           | per-file size (fixed or a range) |
| `files`          | total number of files            |
| `total_capacity` | total bytes on disk              |

Relationship: **`capacity ≈ files × avg_size`**. Provide **any two** and the engine derives the third:

- `size` + `files` → computes `capacity` (exact for fixed sizes, estimated for ranges).
- `size` + `capacity` → derives `files = capacity / avg_size`.
- `files` + `capacity` → **suggests** a fixed `size = capacity / files` (pass an explicit `size` to override).
- All three → keeps `size` + `files`; warns if the given `capacity` disagrees by >5 %.

### 3.2 Size units (binary, base-1024)
Matches datagen's parser: `KB=1024`, `MB=1024²`, `GB=1024³`, `TB`, `PB`.
Accepted forms: `0`, `1024`, `999999`, `12KB`, `5MB`, `2GB`, `100gb` (case-insensitive),
and ranges like `"12KB-5MB"`, `"10GB-120GB"`.

### 3.3 Distributions (for ranges)
`uniform` (default), `log-uniform` (geometric mean → biases toward smaller files),
`normal` (centres on the midpoint).

### 3.4 Tiers → buckets
Each spec belongs to a **bucket**, derived from its size (range uses the midpoint):

| Bucket   | Size range        | Seal config (max_files, target_bytes, open_batches) |
|----------|-------------------|-----------------------------------------------------|
| `zero`   | `size ≤ 0`        | no batching                                         |
| `tiny`   | `< 1 MB`          | 2000, 256MB, 8                                      |
| `small`  | `1 MB – < 100 MB` | 512, 2GB, 8                                         |
| `medium` | `100 MB – < 1 GB` | 64, 10GB, 8                                         |
| `large`  | `≥ 1 GB`          | 8, 50GB, 8                                          |

> Note: the bucket is computed from the size, so boundary values shift labels — a fixed `1MB`
> file lands in `small`, `100MB` in `medium`, `1GB` in `large`.

### 3.5 Topology
The engine picks `flat` or `tree` mode that produces **exactly** the requested file count.
If a count can't be evenly divided for the requested `depth`, it falls back to `flat` (and notes it
in the header) rather than change your count.

### 3.6 Content
`random` (default), `sparse` (auto-selected for the `zero` bucket), `fill`.

### 3.7 File types
If you don't specify `types`, the full catalogue is emitted and files are distributed across it
(per-type share is therefore approximate):
`csv, json, txt, log, sql, xml, yaml, bin, gz, tar, zip, zst, bz2, 7z, parquet, avro, orc, arrow,
hdf5, jpg, png, mp4, mkv, wav, so, ""` (`""` = no-extension slot).

---

## 4. Input forms

### 4.1 CLI — single-tier
Provide any two of `--size` / `--files` / `--capacity` plus optional flags.

### 4.2 CLI — multi-tier (`--tiers`)
Pass a JSON array of tier objects:

```powershell
python specfile_engine.py --id DS-1 --name "Mixed" `
  --tiers '[{"size":"0","files":500},{"size":"1MB-5MB","files":1000,"naming":"FN-12"}]'
```

### 4.3 `--input` file (JSON / JSONL / YAML)
One object = one dataset.

- **`.json`** — a single object **or** an array of objects.
- **`.jsonl`** — one JSON object per line (see [input.jsonl](input.jsonl)).
- **`.yaml` / `.yml`** — requires `pyyaml`.

**Single-tier object:**
```json
{"id":"DS-1","name":"Small pure","size":"1MB-5MB","files":1000,"depth":3}
```

**Multi-tier object** (one dataset → one spec file per tier):
```json
{"id":"DS-1","name":"Mixed","tiers":[
  {"size":"0","files":500,"depth":3},
  {"size":"200KB-600KB","files":60000,"depth":3},
  {"size":"8GB-12GB","files":16,"depth":3}
]}
```

**Tier / record fields** (all optional except the "two of three" rule):
`size`, `files`, `total_capacity`, `depth`, `content`, `types`, `distribution`, `naming`.
Missing tier fields fall back to the **record-level** value, then to the **category default**.

Record-only metadata: `id`, `name`, `category`, `profile`, `root_base`, `seed`.

---

## 5. CLI options

| Flag                | Description                                                              |
|---------------------|--------------------------------------------------------------------------|
| `--input PATH`      | JSON / JSONL / YAML requirement file.                                    |
| `--out DIR`         | Output directory (default: `./generated_specs` next to the script).      |
| `--root-base PATH`  | Override the base path for generated data roots (default `/bryck/cloudcp`). |
| `--dry-run`         | Plan + validate, write nothing.                                          |
| `--list-categories` | Print the 12 categories and exit.                                        |
| `--category NAME`   | One of the 12 categories (supplies defaults).                            |
| `--size VAL`        | Fixed (`5MB`, `0`) or range (`12KB-5MB`).                                |
| `--files N`         | Total number of files.                                                   |
| `--capacity VAL`    | Total capacity budget, e.g. `50GB`.                                      |
| `--tiers JSON`      | JSON array of tier objects (multi-tier dataset).                         |
| `--depth N`         | Directory tree depth.                                                    |
| `--types LIST`      | Comma-separated extensions (else all distributed).                       |
| `--distribution`    | `uniform` \| `log-uniform` \| `normal`.                                  |
| `--content`         | `random` \| `sparse` \| `fill`.                                          |
| `--naming FN`       | Filename variant `FN-01`..`FN-20` (default `FN-01`).                     |
| `--profile NAME`    | Scheduler profile label (default `dt2_100gbe`).                          |
| `--seed N`          | Fixed seed for reproducibility.                                          |
| `--id ID`           | Dataset id (default auto `DS-GEN-001`).                                  |
| `--name NAME`       | Dataset name/label.                                                      |

---

## 6. Examples for every case

### 6.1 size + files → capacity derived
```powershell
python specfile_engine.py --size "1MB-5MB" --files 1000 --id DS-A
```

### 6.2 files + capacity → engine suggests a fixed size
```powershell
python specfile_engine.py --files 200 --capacity 50GB --id DS-B
# header notes: SUGGESTED fixed size=256mb (= capacity/files)
```

### 6.3 size + capacity → files derived
```powershell
python specfile_engine.py --size 10MB --capacity 48GB --id DS-C
# header notes: derived files=4,915 from capacity/avg_size
```

### 6.4 Range size with an explicit distribution
```powershell
python specfile_engine.py --size "10KB-1MB" --files 100000 --distribution log-uniform --id DS-D
```

### 6.5 Fixed zero-byte dataset (must give files — avg size is 0)
```powershell
python specfile_engine.py --size 0 --files 5000000 --id DS-ZERO
```

### 6.6 Using a category for defaults
```powershell
python specfile_engine.py --category "Single-File Transfer" --size 100GB --id DS-HUGE
# category forces files=1
```

### 6.7 Filename-stress variant
```powershell
python specfile_engine.py --size 512KB --files 20000 --naming FN-08 --id DS-UNICODE
```

### 6.8 Restrict file types
```powershell
python specfile_engine.py --size "1MB-4MB" --files 5000 --types "parquet,orc,avro" --id DS-COL
```

### 6.9 Multi-tier via CLI
```powershell
python specfile_engine.py --id DS-MIX --name "Mixed pipeline" `
  --tiers '[{"size":"0","files":5000,"depth":3},
            {"size":"200KB-800KB","files":80000,"depth":3},
            {"size":"5GB-12GB","files":20,"depth":3}]'
```

### 6.10 Single JSON object
```powershell
python specfile_engine.py --input one.json
```
`one.json`:
```json
{"id":"DS-1","size":"1MB-5MB","files":1000,"depth":3}
```

### 6.11 JSON array (many datasets)
```json
[
  {"id":"DS-1","size":"512KB","files":20000,"depth":1},
  {"id":"DS-2","size":"10MB","files":4800,"depth":1}
]
```

### 6.12 JSONL (one dataset per line, multi-tier)
See [input.jsonl](input.jsonl) — the full 54-dataset corpus, one line each:
```powershell
python specfile_engine.py --input input.jsonl --out generated_specs
```

### 6.13 YAML input (needs pyyaml)
```yaml
- id: DS-1
  size: "1MB-5MB"
  files: 1000
  depth: 3
```
```powershell
python specfile_engine.py --input req.yaml
```

### 6.14 Reproducibility, output location, custom root
```powershell
python specfile_engine.py --size 1GB --files 100 --id DS-R `
  --seed 42 --out ./out --root-base /data/testcorpus
```

### 6.15 Dry run
```powershell
python specfile_engine.py --input input.jsonl --dry-run
```

---

## 7. Output layout

- **Single-tier dataset** → one file written directly in `--out`:
  ```
  generated_specs/DS-A.yaml
  ```
- **Multi-tier dataset** → a per-dataset subfolder, one spec per tier:
  ```
  generated_specs/DS-MIX/
    DS-MIX__zero__0b.yaml
    DS-MIX__tiny__200kb_800kb.yaml
    DS-MIX__large__5gb_12gb.yaml
  ```
- **`manifest.json`** is written to `--out` whenever more than one spec (or more than one dataset)
  is produced. It records, per dataset: `id`, `name`, `category`, `profile`, `emitted_files`,
  `spec_count`, `capacity`, and the list of `specs` (file, count, bucket, variant, size, mode, root).

Data roots inside each spec follow: `<root_base>/<ID>/<bucket>/<size_label>`.

---

## 8. Anatomy of a generated spec

Each file starts with a comment header that **echoes the input requirement** (so the spec is
self-documenting), followed by the datagen YAML body:

```yaml
# ============================================================================
# Dataset   : DS-A
# Name      : Small pure
# Category  : (generic)
# Tier      : small   |   Size band: 1mb_5mb
# Files     : 1,000   |   Capacity: 2.9gb (estimated)
# Seal cfg  : max_files=512, target_bytes=2GB, open_batches=8
# Profile   : dt2_100gbe
# ----------------------------------------------------------------------------
# REQUESTED (this spec's input requirement):
#   size         : range 1mb–5mb (uniform)
#   files        : 1,000
#   capacity     : (derived from size+files)
#   depth        : 3
#   content      : random
#   distribution : uniform
#   naming       : FN-01 (default)
#   types        : all types (distributed by default)
# ----------------------------------------------------------------------------
# Generated by specfile_engine.py from a compact requirement.
# ============================================================================

version: 1
mode: tree
root: /bryck/cloudcp/DS-A/small/1mb_5mb
threads: 16
seed: 123456789
content: { ... }
naming:  { ... }
topology:{ ... }
size:    { ... }
extensions: [ ... ]
```
Fields you didn't supply are shown as `(derived …)` so you can tell input from computed values.

---

## 9. Filename variants (`--naming` / `naming`)

`FN-01` (default) is plain ASCII. `FN-02`..`FN-20` exercise tricky filenames. Some are approximate or
unsupported by datagen and are flagged in the spec header:

| Variant | Focus                              | Note                                         |
|---------|------------------------------------|----------------------------------------------|
| FN-01   | plain ASCII, len 16                | —                                            |
| FN-02   | embedded space                     | —                                            |
| FN-03   | trailing space                     | —                                            |
| FN-04   | embedded newline                   | —                                            |
| FN-05   | trailing carriage return           | —                                            |
| FN-06   | Latin-supplement + `_`             | —                                            |
| FN-07   | 200-char long names                | —                                            |
| FN-08   | CJK / Arabic / emoji               | —                                            |
| FN-09   | double extensions (`.tar.gz`)      | —                                            |
| FN-10   | space + CR                         | —                                            |
| FN-11   | leading dot (hidden)               | —                                            |
| FN-12   | shell metacharacters `$!&;\|`      | APPROX via special-char pool                 |
| FN-13   | Windows-reserved `:<>`             | APPROX (valid bytes on Linux)                |
| FN-14   | device-name prefix `CON_`          | APPROX (prefix only)                         |
| FN-15   | embedded tab                       | —                                            |
| FN-16   | leading dash                       | —                                            |
| FN-17   | NFD-decomposed Unicode             | **UNSUPPORTED** by datagen                   |
| FN-18   | zero-width space (U+200B)          | APPROX via special-char pool                 |
| FN-19   | consecutive spaces                 | APPROX via fixed prefix                      |
| FN-20   | >1000-byte paths                   | **UNSUPPORTED** by datagen                   |

You can also pass a `naming` **object** (in JSON/JSONL) to override individual fields:
`charset`, `alphabet`, `unicode_blocks`, `special_chars`, `prefix`, `suffix`, `length`,
`collision`, `ext_override`.

---

## 10. Categories (`--list-categories`)

1. **Single-Tier Isolation** — all files in one tier; stress a single bucket.
2. **Batch Builder Mechanics** — exact-count probes for count/byte-seal thresholds.
3. **Batch Exhaustion / Weight Shift** — multi-tier corpora where one tier drains first.
4. **Filename & Encoding Stress** — tricky filenames.
5. **File Type Coverage** — all file types across the active tier.
6. **Network Profile Comparison** — same data under different profiles (fixed seed).
7. **Mixed Full-Pipeline** — end-to-end scan→batch→upload→verify.
8. **Configuration Edge Cases** — empty dirs, single huge file, deep trees, unreadable subdirs.
9. **Single-File Transfer** — exactly one file (forces `files=1`).
10. **Sub-Range Isolation** — a narrow size sub-band at high count.
11. **Alternative Weight Ratios** — proportioned to non-default scheduler weights.
12. **Tiny/Small-Heavy Mixed** — skewed mixes dominated by tiny/small files.

Categories only supply **defaults** (depth, size kind, distribution, and sometimes a forced file
count). Any field you set explicitly wins.

---

## 11. Known limitations / approximations

- **Empty directories** (e.g. an empty source) are approximated as a zero-file spec.
- **Very deep trees**: `depth` is honoured, but the format cannot pin an exact files-per-directory count.
- **Permissions** (`chmod` / unreadable subdirs) can't be expressed — set them manually after generation.
- **Bucket label shift**: because the bucket is derived from size, boundary sizes may land in a
  neighbouring tier (`1MB`→`small`, `100MB`→`medium`, `1GB`→`large`).
- **Per-type share is approximate** when `types` isn't restricted (files are distributed across the catalogue).
- `FN-17` and `FN-20` are **unsupported** by datagen and flagged in the header.

---

## 12. Reproducing the full corpus

[input.jsonl](input.jsonl) contains all 54 datasets (one per line, using the `tiers[]` form).
To regenerate every spec:

```powershell
python specfile_engine.py --input input.jsonl --out generated_specs
```

This writes 143 spec files across 54 dataset folders plus a `manifest.json`.
Add `--dry-run` first to preview the plan without writing anything.
