# Plan — Datagen **Spec-File Generation Engine**

**Version:** 2.0
**Goal:** A Python engine that takes a *compact* description of a data requirement —
**category**, **size** (a single/fixed value or a range), **file count**, and
**total capacity**, plus optional overridable defaults — and emits **one `datagen`
spec file** per requirement.

**Input channels:** command line, a single **JSON** file, or a **JSONL** file (one
requirement per line → many spec files in one run).

**Final code deliverable (built from this plan):** `specfile_engine.py`.

---

## 1. Core Idea (one requirement → one spec file)

The user does **not** hand-write datagen YAML. They state *intent* in a few fields;
the engine reconciles them, fills defaults from the chosen **category**, and writes a
single valid spec file. For a **JSONL** input, each line is an independent
requirement and produces its own spec file.

```
requirement (CLI | JSON | JSONL line)
        │
        ▼
  normalize → reconcile size/files/capacity → apply category defaults
        │
        ▼
   one datagen spec file  (+ optional manifest when many)
```

This is deliberately simpler than a multi-spec "explosion" model: **one spec = one
naming policy = one file**. Filename-variant explosion is *not* used here; file
**types** are distributed via the weighted `extensions` catalogue.

---

## 2. Inputs

### 2.1 The five input concepts

| Input | Meaning | Example | Required? |
|-------|---------|---------|-----------|
| `category` | One of the 12 categories (§3). Drives defaults. | `"Single-Tier Isolation"` | Recommended (defaults to a generic profile if omitted) |
| `size` | **Fixed** single value **or** a **range**. | `5MB` · `0` · `1KB` · `12KB-5MB` | Provide `size` and/or `capacity` |
| `files` | Total number of files to create. | `100000` | Provide any 2 of {size, files, capacity} |
| `total_capacity` | Total byte budget for the dataset. | `50GB` | Provide any 2 of {size, files, capacity} |
| overridable **defaults** | `depth`, `types`, `distribution`, `content`, `naming`, `profile`, `root_base`, `seed`, `id`, `name` | see §4 | Optional |

### 2.2 The reconciliation rule (the engine's "decide / suggest" logic)

Sizing is governed by the identity **`capacity ≈ files × avg_size`**. The user gives
**any two** of `{size, files, capacity}`; the engine derives (and *reports*) the
third. For a **range**, `avg_size` is estimated from the distribution
(uniform → midpoint; log-uniform → geometric-ish mean; normal → midpoint).

| You provide | Engine derives / suggests |
|-------------|---------------------------|
| fixed `size` + `files` | `capacity = files × size` |
| fixed `size` + `capacity` | `files = round(capacity / size)` |
| `range` + `files` | estimates `capacity` (prints the estimate; ranges are approximate) |
| `range` + `capacity` | `files = round(capacity / avg_size)` |
| `files` + `capacity` (no size) | **suggests** a size: fixed `= capacity/files`, or a sensible range around it |
| `range` + `files` + `capacity` | validates; if inconsistent, **adjusts distribution** or warns |
| fixed `size` only | needs `files` **or** `capacity`; else error |

Whatever the engine decides is printed and written into the spec's header comment so
the choice is transparent and reproducible.

### 2.3 Minimal valid inputs

```jsonc
// fixed size + count
{ "category": "Single-Tier Isolation", "size": "5MB", "files": 1000 }

// range + capacity (engine derives count)
{ "category": "Sub-Range Isolation", "size": "12KB-5MB", "total_capacity": "50GB" }

// files + capacity, no size (engine suggests a size)
{ "category": "Mixed Full-Pipeline", "files": 100000, "total_capacity": "300GB" }
```

---

## 3. Categories (shown in `--help`, each with a description)

The engine's `--help` prints this table. Each category sets **defaults** (bucket
bias, content type, topology depth, distribution, and whether sizes are fixed or
ranged). Source: [dataset_map.json](dataset_map.json).

| # | Category | Description | Default bias |
|---|----------|-------------|--------------|
| 1 | **Single-Tier Isolation** | All files land in one tier (zero/tiny/small/medium/large). Stress a single bucket in isolation. | tree, depth 3, range sizes |
| 2 | **Batch Builder Mechanics** | Exact-count probes that trigger count-seal / byte-seal thresholds. | flat, **fixed** sizes, exact counts |
| 3 | **Batch Exhaustion / Weight Shift** | Multi-tier corpora where one tier drains first and worker slots redistribute. | tree, depth 3, mixed tiers |
| 4 | **Filename & Encoding Stress** | Exercises tricky filenames (spaces, Unicode, control bytes, long names). | flat/tree, fixed size, naming override |
| 5 | **File Type Coverage** | Ensures all file types appear across active tiers. | tree, ranges, `types=all` |
| 6 | **Network Profile Comparison** | Same on-disk data run under different scheduler profiles. | tree, ranges, fixed seed |
| 7 | **Mixed Full-Pipeline** | End-to-end scan→batch→upload→verify across all tiers. | tree, ranges, large counts |
| 8 | **Configuration Edge Cases** | Empty dirs, single huge file, deep trees, unreadable subdirs. | special topologies |
| 9 | **Single-File Transfer** | Exactly one file at a chosen size/boundary. | flat, `files=1` |
| 10 | **Sub-Range Isolation** | A narrow size sub-band (e.g. 10 KB–1 MB) at high count. | tree, range, high count |
| 11 | **Alternative Weight Ratios** | Datasets proportioned to non-default scheduler weights. | tree, ranges, multi-tier |
| 12 | **Tiny/Small-Heavy Mixed** | Skewed mixes dominated by tiny/small files. | tree, ranges, tiny-heavy |

If `category` is omitted, the engine uses generic defaults and infers the bucket
purely from the size.

---

## 4. Overridable Defaults

Any of these override the category defaults. Omit them to accept the category's
choice.

| Field | What it controls | Default (if omitted) |
|-------|------------------|----------------------|
| `depth` | directory tree depth | from category (e.g. 3); `flat` if count not divisible |
| `types` | list of extensions (e.g. `["csv","json"]`) | **all types distributed** (equal weight) |
| `distribution` | for a range: `uniform` \| `log-uniform` \| `normal` | `uniform` |
| `content` | `random` \| `sparse` \| `fill` | `sparse` if size 0, else `random` |
| `naming` | filename variant / charset (FN-01…FN-20 or explicit) | baseline ASCII (FN-01) |
| `profile` | scheduler profile label (header only) | `dt2_100gbe` |
| `root_base` | base output path in the spec's `root` | `/bryck/cloudcp` |
| `seed` | reproducibility | derived from spec filename (crc32) |
| `id` / `name` | dataset identity / label | auto-generated from category + size |

**`types` rule:** if a list is given, only those extensions are emitted (equal
weight → even distribution). If omitted, the full catalogue is emitted with equal
weights so every type appears.

---

## 5. Bucket Assignment

The engine derives the tier from the size (range → use the midpoint), matching the
plan's tier table:

| Bucket | Size range | Sealing (informational, in header) |
|--------|-----------|-------------------------------------|
| zero   | 0 B exactly   | no batching |
| tiny   | 1 B – 1 MB    | max 2000 files / 256 MB / 8 open |
| small  | 1 MB – 100 MB | max 512 / 2 GB / 8 |
| medium | 100 MB – 1 GB | max 64 / 10 GB / 8 |
| large  | > 1 GB        | max 8 / 50 GB / 8 |

A warning is printed if an explicit `category`/`bucket` disagrees with the size band.

---

## 6. Output — a single spec file

For each requirement, the engine emits **one** `.yaml` spec:

```
<out_dir>/<ID>.yaml                       # single JSON input or CLI
<out_dir>/<ID>.yaml  (one per JSONL line) # JSONL input
<out_dir>/manifest.json                   # written when >1 spec is produced
```

Each spec contains: a rich **header comment** (category, derived bucket, the
reconciled size/files/capacity decision, seal config, profile, provenance, any
APPROX notes), then `version`, `mode` (`tree`/`flat`), `root`, `threads`, `seed`,
`content`, `naming`, topology (`tree:`/`flat:`), `size`, `extensions`.

Spec correctness follows [DatagenSpecFileGuide.md](DatagenSpecFileGuide.md):
- exact count via `tree` when `fanout^depth × files_per_dir == files`, else `flat`;
- `direct_io` auto-enabled for sizes ≥ 256 MB;
- `content: sparse` for zero-byte;
- raw control bytes in names emitted via double-quoted YAML escaping.

The `manifest.json` (when multiple specs) matches the existing schema in
[manifest.json](manifest.json) so [dataset_validator.py](dataset_validator.py) keeps
working.

---

## 7. Input Channels

### 7.1 Command line
```
# category + range + count, with overrides
python specfile_engine.py \
    --category "Single-Tier Isolation" \
    --size 12KB-5MB --files 100000 \
    --depth 3 --types csv,json --distribution log-uniform

# fixed size + capacity (engine derives count)
python specfile_engine.py --category "Batch Builder Mechanics" --size 100KB --capacity 200MB

# files + capacity, no size (engine suggests a size)
python specfile_engine.py --files 100000 --capacity 50GB

# zero-byte tier
python specfile_engine.py --category "Single-Tier Isolation" --size 0 --files 500000
```
`--size` accepts: `5MB` (fixed), `0` (zero), `12KB-5MB` (range).

### 7.2 Single JSON file
One object (one spec) **or** an array of objects (many specs):
```
python specfile_engine.py --input requirement.json
```

### 7.3 JSONL file (one requirement per line → one spec per line)
```
python specfile_engine.py --input requirements.jsonl
```
```jsonl
{"category":"Single-Tier Isolation","size":"5MB","files":1000,"id":"DS-A"}
{"category":"Sub-Range Isolation","size":"12KB-5MB","total_capacity":"50GB","id":"DS-B"}
{"category":"Single-File Transfer","size":"100GB","files":1,"id":"DS-C"}
```

Common flags: `--out`, `--root-base`, `--dry-run`, `--list-categories`, `--help`.

---

## 8. Input Schema (JSON / JSONL record)

```jsonc
{
  "id":            "DS-CUSTOM-01",              // optional; auto if omitted
  "name":          "My tiny stress",           // optional
  "category":      "Single-Tier Isolation",    // one of the 12 (§3)
  "size":          "12KB-5MB",                  // "5MB" | "0" | "12KB-5MB"
  "files":         100000,                      // optional if 2 others given
  "total_capacity":"50GB",                      // optional if 2 others given
  "depth":         3,                           // override
  "types":         ["csv","json"],              // override; else all distributed
  "distribution":  "log-uniform",              // override (ranges only)
  "content":       "random",                    // override
  "naming":        "FN-01",                     // override (variant id or object)
  "profile":       "dt2_100gbe",               // override
  "root_base":     "/bryck/cloudcp",           // override
  "seed":          1234567                       // override
}
```

`size` may also be given as an object: `{"type":"fixed","bytes":"5MB"}` or
`{"type":"range","min":"12KB","max":"5MB"}`.

---

## 9. Compilation Pipeline

```
record (CLI/JSON/JSONL)
   │  normalize fields, parse sizes (parse_size)
   ▼
reconcile {size, files, capacity}         # §2.2  (decide/suggest; print result)
   ▼
apply category defaults + overrides       # §3, §4
   ▼
derive bucket from size                   # §5
   ▼
choose topology (tree if divisible else flat)   # plan_topology
   ▼
build naming policy (default FN-01 or override)
   ▼
render ONE spec YAML (header + body)
   ▼
write <ID>.yaml   (+ manifest.json if many)
```

Reused from [generate_specs.py](generate_specs.py): `human`, `size_label`,
`plan_topology`, `yaml_dq`, the `emit_content/naming/size/extensions/topology`
functions, and `variant_policy` (for optional naming overrides). New logic:
`parse_size` (string→bytes, incl. ranges), the **reconciler**, the **category
default table**, and the input adapters.

---

## 10. Module Structure of `specfile_engine.py`

1. **Units & parsing** — `KB/MB/GB/TB`, `parse_size(str|int)`, `parse_size_field`
   (fixed vs `A-B` range), `human`, `size_label`.
2. **Catalogues** — `TYPES_ALL`, `BUCKET_SEAL`, `bucket_for_size`,
   `CATEGORIES` (name → description + defaults).
3. **Reconciler** — `reconcile(size, files, capacity, distribution) -> Plan`
   (returns final size policy, file count, est. capacity, and human notes).
4. **Defaults** — `category_defaults(name)` and `merge_overrides()`.
5. **Naming** — `NamePolicy`, `variant_policy` (for `naming` overrides).
6. **Emitters** — `yaml_dq`, `emit_*`, `build_spec_text` (single spec).
7. **Adapters** — `from_cli(args)`, `from_json(path)`, `from_jsonl(path)`,
   `load(path)` dispatcher (by extension / content).
8. **Validation** — `validate(record) -> (errors, warnings)`.
9. **Driver** — `generate(records, out_dir, dry_run)` → writes specs + manifest,
   prints a table (id, category, bucket, files, est-capacity, mode).
10. **CLI** — `main()` (`argparse`), incl. `--list-categories` and a `--help` that
    prints the §3 category table.

---

## 11. Validation & Testing

1. **Reconciler unit tests:** every combination in §2.2 (fixed/range × which two of
   size/files/capacity), including the "suggest a size" path and the
   over-specified/inconsistent path.
2. **`parse_size` tests:** all suffixes (K/KB/Ki…P), bare ints, ranges (`12KB-5MB`),
   zero.
3. **Bucket boundaries:** 0, 1, 1 MB, 100 MB, 1 GB, >1 GB.
4. **Topology:** tree-divisible vs flat fallback (with header NOTE).
5. **Schema:** every emitted spec parses with `pyyaml` and satisfies the guide's
   per-mode required-section matrix.
6. **JSONL:** N lines → N spec files + one manifest.
7. **Category defaults:** each of the 12 produces a valid spec from a minimal record.

---

## 12. Milestones

| # | Milestone | Output |
|---|-----------|--------|
| M1 | Units, `parse_size` (incl. ranges), catalogues, `bucket_for_size` | unit tests green |
| M2 | Reconciler (`size`/`files`/`capacity`) + tests | decide/suggest works |
| M3 | Category defaults + override merge | 12 categories yield defaults |
| M4 | Emitters + single-spec `build_spec_text` | one valid `.yaml` from a record |
| M5 | Adapters: CLI, JSON, JSONL + driver + manifest | all 3 input channels work |
| M6 | Validation, `--help`/`--list-categories`, tests | end-to-end + docs |

---

## 13. Generation Prompt (hand to an LLM to produce `specfile_engine.py`)

> **Task:** Write a single, self-contained Python 3 file `specfile_engine.py` — a
> *datagen spec-file generation engine*. It takes a compact requirement (via CLI, a
> JSON file, or a JSONL file) and emits **one** `datagen` YAML spec per requirement.
>
> **Context files to honour:**
> - `DatagenSpecFileGuide.md` — authoritative spec grammar (modes, sections, size
>   strings, per-mode required-section matrix).
> - `dataset_map.json` — the 12 categories and their descriptions (use for the
>   `--help` table and category defaults).
> - `manifest.json` — the manifest schema to emit when producing multiple specs.
> - `generate_specs.py` — reuse `human`, `size_label`, `plan_topology`, `yaml_dq`,
>   the `emit_content/naming/size/extensions/topology` functions, and `variant_policy`.
>
> **Inputs (per requirement):** `category` (one of 12), `size` (fixed like `5MB`/`0`
> or a range like `12KB-5MB`), `files` (count), `total_capacity`, plus overridable
> defaults: `depth`, `types` (list; else all types distributed equally),
> `distribution`, `content`, `naming`, `profile`, `root_base`, `seed`, `id`, `name`.
>
> **Reconciler (key logic):** using `capacity ≈ files × avg_size`, the user gives any
> two of `{size, files, capacity}`; derive the third. For a range, estimate
> `avg_size` from `distribution` (uniform→midpoint, log-uniform→geometric-ish,
> normal→midpoint). If only `files`+`capacity` are given, **suggest** a size
> (fixed `= capacity/files`, or a range around it). If over-specified and
> inconsistent, adjust distribution or warn. Print and record every decision in the
> spec header.
>
> **Build:**
> 1. `parse_size(str|int)->int` and `parse_size_field(str|dict)->SizePolicy`
>    (accept `"A-B"` ranges); keep `human`, `size_label`.
> 2. Catalogues: `TYPES_ALL`, `BUCKET_SEAL`, `bucket_for_size`, and a `CATEGORIES`
>    map of name → {description, defaults(bucket bias, depth, fixed-vs-range,
>    distribution, content)}.
> 3. `reconcile(...)`, `category_defaults(name)`, `merge_overrides(...)`.
> 4. Emitters + `build_spec_text(record, plan)` → ONE spec (header comment with the
>    reconciled decision, seal config, profile, notes; then version/mode/root/
>    threads/seed/content/naming/topology/size/extensions). Topology: `tree` when
>    `fanout^depth × per_dir == files`, else `flat` (+ NOTE). `direct_io` on for
>    sizes ≥ 256 MB; `sparse` for size 0. `types` list → equal-weight extensions;
>    omitted → full catalogue equal-weight. Naming defaults to baseline ASCII
>    (FN-01) unless `naming` overrides.
> 5. Adapters: `from_cli(args)`, `from_json(path)` (object or array),
>    `from_jsonl(path)` (one record/line), `load(path)` dispatcher.
> 6. `validate(record)->(errors,warnings)`; `generate(records,out_dir,dry_run)`
>    writes `<ID>.yaml` per record and `manifest.json` when >1, printing a summary
>    table.
> 7. `main()` argparse: `--category --size --files --capacity --depth --types
>    --distribution --content --naming --profile --root-base --seed --id --name`,
>    plus `--input`, `--out`, `--dry-run`, `--list-categories`. `--help` prints the
>    12-category description table. Force UTF-8 stdout on Windows.
>
> **Hard constraints:** stdlib only (optional `pyyaml` to read YAML input; fall back
> to JSON). One spec per requirement. Exact counts via tree-or-flat. Raw control
> bytes via `yaml_dq`. Every reconciliation decision surfaced in the header.
>
> **Acceptance:** `--size 5MB --files 1000` writes one valid tiny/small spec;
> `--size 12KB-5MB --capacity 50GB` derives and prints a file count and writes a
> range spec; a 3-line JSONL writes 3 specs + a manifest; `--list-categories` prints
> all 12 with descriptions.

---

## 14. Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Ambiguous sizing (which 2 of 3 given) | Explicit reconciler table (§2.2); print every decision. |
| Range capacity is only an estimate | Label it "estimated"; allow `distribution` override; validate if all three given. |
| Category vs size mismatch | Warn when derived bucket disagrees with category bias. |
| Non-divisible counts for a tree depth | Fall back to `flat`; emit header NOTE. |
| Datagen-inexpressible naming (FN-17/FN-20) | Best-effort + UNSUPPORTED note when such a `naming` override is requested. |
