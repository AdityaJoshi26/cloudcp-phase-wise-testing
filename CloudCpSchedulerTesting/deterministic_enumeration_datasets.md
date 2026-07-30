# CloudCp Scheduler — Deterministic Enumeration Datasets

> **Goal.** Design source directory trees whose **BFS enumeration order is 100% predictable
> *before* the transfer runs**, so the scheduler/broker can be validated against a pre-computed
> ground-truth ("oracle") of *which tier's files are seen in what order* and *which batches the
> BatchBuilder will create in what order*.
>
> This catalog is **self-contained and atomic** — it does **not** reuse anything under
> `dataset_cloudcp/`. Everything needed to generate and validate lives in this document.

---

## 0. TL;DR

1. The upload scanner is a **single-process BFS walker** (`deque` frontier, `popleft`). It drains a
   directory's **own files first**, then descends to child directories **level by level**
   (`docs/batch_builder_design.md` §7.1).
2. Inside one directory, `os.scandir` order is **not** guaranteed. We neutralise that by making
   **every directory hold files of exactly one tier, all identical in size** → scandir order can no
   longer change batch *composition* (every batch in a tier is byte-identical).
3. We lay each dataset out as a **single nested chain**: tier `C1` at the root, `C2` one level
   down, `C3` two levels down, … So BFS enumerates **all of `C1`, then all of `C2`, …** — a strict,
   pre-known order.
4. Because each tier is homogeneous and its file count is a multiple of its
   `BATCH_SIZE × OPEN_BATCHES` **block**, the **number, size and creation order of batches per tier
   is fully determined** by arithmetic — the oracle.
5. We ship **12 order-verification datasets** (`SCH-ORD-01..12`, curated permutations) + **3 deep
   stress datasets** (`SCH-DEEP-01..03`, hundreds of batches on the cheap tiers). Total on-disk
   footprint ≈ **1.88 TB** (budget: target < 2 TB, hard max 5 TB). The **full 120-permutation
   catalog** is defined by a deterministic ID scheme (§11) so any additional ordering can be
   materialised on demand.

---

## 1. Why this is deterministic — the mechanism

### 1.1 What the walker actually does

From `docs/batch_builder_design.md` §7.1 (single-process BFS walker, the tested baseline):

```
frontier = deque([root])
while frontier:
    dir = frontier.popleft()                 # BFS: FIFO, level by level
    for e in os.scandir(dir):                # order NOT guaranteed
        if e.is_dir():   frontier.append(e)  # descended LATER
        elif e.is_file(): add(e, size)       # classified + batched NOW
```

Two facts fall out:

- **F1 — a directory's own files are fully enumerated before any of its subdirectories are
  descended.** The subdir is only *appended* to the frontier during the parent scan; it is not
  visited until every file of the parent (and every earlier sibling in the frontier) is done.
- **F2 — the order of files *within* a single directory is unspecified** (filesystem/`scandir`
  dependent). So we must never let *which file* lands in *which batch* matter.

### 1.2 The two design rules that make order predictable

| Rule | Statement | Kills which non-determinism |
|------|-----------|-----------------------------|
| **R1 — Homogeneous directories** | Every directory contains files of exactly **one tier**, all the **same size**. | F2. If all files in a dir are identical, any `scandir` permutation yields byte-identical batches (same count, same bytes, same tier). Only the *filenames inside* a batch may differ — never the batch's tier/size/count/sequence. |
| **R2 — Single nested chain** | Put tier `C1` in the root, `C2` in one child dir, `C3` in a grandchild, … one tier per level, **exactly one directory per level**. | Sibling ordering. With one dir per level, BFS has no sibling choice to make — it walks a straight line, so tier order = chain order, exactly. |

With R1 + R2, **file-enumeration order is a total, pre-known order**: all of `C1`, then all of `C2`,
… then all of `C5`. This is exactly the "0-byte first, then medium, then large …" behaviour asked
for.

### 1.3 Structure of one dataset

For an ordering `C1 → C2 → C3 → C4 → C5` (each `Ci` one of ZERO/TINY/SMALL/MEDIUM/LARGE):

```
<root>/                     level 0  → C1 files  (n1 identical files)
<root>/L1/                  level 1  → C2 files  (n2 identical files)
<root>/L1/L2/               level 2  → C3 files  (n3 identical files)
<root>/L1/L2/L3/            level 3  → C4 files  (n4 identical files)
<root>/L1/L2/L3/L4/         level 4  → C5 files  (n5 identical files)
```

`L1..L4` are **pure link directories** — they hold the next level's files and nothing else. The BFS
frontier evolves as: `[root] → [L1] → [L2] → [L3] → [L4] → []`, so files come out strictly
`C1, C2, C3, C4, C5`.

---

## 2. Tier definitions & batch-close arithmetic

Size-class boundaries (confirmed = design-doc default, half-open `[prev, max)`):

| Tier | Bucket range | Canonical file size | In-bucket? |
|------|--------------|---------------------|-----------|
| **ZERO**   | `size < 1 B` (i.e. == 0) | **0 B**        | yes (only 0 qualifies) |
| **TINY**   | `[1 B, 1 MB)`           | **16 KiB** (16 384 B)      | clearly inside |
| **SMALL**  | `[1 MB, 100 MB)`        | **2 MiB** (2 097 152 B)    | clearly inside |
| **MEDIUM** | `[100 MB, 1 GB)`        | **100 MiB** (104 857 600 B)| clearly inside |
| **LARGE**  | `[1 GB, ∞)`             | **1 GiB** (1 073 741 824 B)| clearly inside (≥ 1 GiB) |

> **Boundary safety.** Sizes are picked to sit *clearly inside* their bucket (design principle:
> never sit on the `max`). LARGE is the tightest — 1 GiB is the smallest sane "≥ 1 GB" value and is
> the **irreducible** floor for the LARGE byte budget.

Scheduler config under test (your values):

| Tier | `BATCH_SIZE` (M, max files) | `TARGET_SIZE_MB` (byte cap) | `OPEN_BATCHES` (K) |
|------|---------------------------:|----------------------------:|-------------------:|
| ZERO   | 2000 | 0     | 4 |
| TINY   | 511  | 256   | 8 |
| SMALL  | 317  | 2048  | 8 |
| MEDIUM | 50   | 10240 | 8 |
| LARGE  | 5    | 51200 | 8 |

### 2.1 Which limit closes each batch (we force the **file-count** limit)

A batch closes on `TARGET_SIZE_MB` **or** `BATCH_SIZE`, **whichever first**. We deliberately choose
file sizes so the **file-count limit always wins** → every full batch has **exactly `M` files** (a
clean integer, the whole point of determinism):

| Tier | M × file_size | vs `TARGET_SIZE_MB` | Winner | Files / full batch |
|------|--------------:|--------------------:|:------:|-------------------:|
| ZERO   | 2000 × 0 B     = 0 MB      | 0 MB (never trips: `0+0 ≯ 0`) | **count** | **2000** |
| TINY   | 511  × 16 KiB  = 7.98 MiB  | 256 MB   | **count** | **511** |
| SMALL  | 317  × 2 MiB   = 634 MiB   | 2048 MB  | **count** | **317** |
| MEDIUM | 50   × 100 MiB = 4883 MiB  | 10240 MB | **count** | **50** |
| LARGE  | 5    × 1 GiB   = 5120 MiB  | 51200 MB | **count** | **5** |

Every full batch is therefore a fixed shape. Good.

### 2.2 The deterministic **block** (the atomic sizing unit)

The BatchBuilder keeps `K = OPEN_BATCHES` batches open per tier and round-robins each incoming file
across them (`slot = global_file_index % K`, `docs/batch_builder_design.md` §6.3). For **all K open
batches to close as full `M`-file batches with no ragged remainder**, a tier's file count must be a
multiple of:

```
block(tier) = BATCH_SIZE × OPEN_BATCHES = M × K
```

| Tier | block = M × K | batches per block |
|------|--------------:|------------------:|
| ZERO   | 2000 × 4 = **8000** | 4 |
| TINY   | 511 × 8  = **4088** | 8 |
| SMALL  | 317 × 8  = **2536** | 8 |
| MEDIUM | 50 × 8   = **400**  | 8 |
| LARGE  | 5 × 8    = **40**   | 8 |

**Always size a tier as `R × block` files** (`R` = whole number of "rounds"). Then that tier yields
exactly `R × (batches per block)` full batches, no partial batches. `R` is the per-tier depth knob.

---

## 3. The oracle — predicting batch **creation order**

Two orders must be kept distinct:

- **Enumeration / creation order — DETERMINISTIC (this is the oracle).** Fixed by BFS + the chain.
  This is what BatchBuilder *produces*.
- **Dispatch order — UNDER TEST (not fixed).** The broker picks batches from `pending/<tier>/` by
  **weight + work-stealing**. It is *not* required to equal creation order; validating it *is the
  point of Phase 2*.

The oracle validates the **creation** side; the scheduler test compares its **dispatch** decisions
against the pending set the oracle guarantees exists.

### 3.1 File-level oracle (always exact, even at R = 1)

`source.index` is appended in walk order, so it is **strictly**:

```
[ n1 × C1 records ] then [ n2 × C2 records ] then … then [ n5 × C5 records ]
```

Validate enumeration order by checking `source.index` (and `scan.discovered` / `scan.completed` for
directory order) is exactly tier-contiguous in the chain order. This never depends on `scandir`.

### 3.2 Batch-level oracle — mind the **finish() flush**

The rotate rule flushes a batch only when the **next** file would overflow it
(`docs/batch_builder_design.md` §6.3), and any still-open batch is flushed at `finish()` (§6.5).
Consequently, for a tier sized at `R` blocks:

- **Rounds 1 … R−1** overflow *during the scan* → published **in enumeration order**, tier by tier.
- **Round R** (the last `K` batches of each tier) never overflows (no further same-tier file
  arrives — the next thing walked is a link dir, then a *different* tier) → it is flushed at
  **`finish()`**, in the builder's fixed **bucket-iteration order**, at the very end.

**Design consequences:**

- To observe an in-order **streamed** batch signal for a tier, size it at **`R ≥ 2`**.
- At **`R = 1`**, that tier's batches all appear at `finish()` (correct and still fully predictable,
  just not streamed mid-scan). We use `R = 1` only for the expensive tiers (MEDIUM/LARGE), whose
  *relative enumeration order* is still proven by `source.index` (§3.1).

> ⚠️ **Do not treat the finish() flush as a bug.** A test that asserts "every LARGE batch is
> published before the first MEDIUM batch" will *fail* even on a correct build if both are `R = 1`,
> because both terminal rounds flush together at finish() in bucket order. Assert **enumeration
> order via `source.index`**, and **batch counts/shapes via `batches.created`** — not wall-clock
> publish order of terminal rounds.

### 3.3 Worked example — ordering `ZERO → TINY → SMALL → MEDIUM → LARGE`, ORDER profile

Per-tier depth (ORDER profile, §6): `Z:R=2, T:R=2, S:R=2, M:R=1, L:R=1`.

| Step | Level | Tier | Files | Full batches | Streamed (rounds 1..R-1) | At finish() (round R) |
|------|:-----:|------|------:|:------------:|:------------------------:|:---------------------:|
| 1 | 0 | ZERO   | 16 000 | 8  | 4 (round 1) | 4 |
| 2 | 1 | TINY   | 8 176  | 16 | 8 (round 1) | 8 |
| 3 | 2 | SMALL  | 5 072  | 16 | 8 (round 1) | 8 |
| 4 | 3 | MEDIUM | 400    | 8  | 0           | 8 |
| 5 | 4 | LARGE  | 40     | 8  | 0           | 8 |

- **Streamed publish order** (during scan): `4×ZERO, 8×TINY, 8×SMALL` — strictly in chain order.
- **finish() publish order** (bucket-iteration): the remaining `4×ZERO, 8×TINY, 8×SMALL, 8×MEDIUM,
  8×LARGE`.
- **`source.index` order**: `16000×ZERO, 8176×TINY, 5072×SMALL, 400×MEDIUM, 40×LARGE` — the exact
  enumeration oracle.
- **Total batches** = 8+16+16+8+8 = **56**. **Total files** = 29 688.

---

## 4. Canonical footprint accounting

Per **block** (§2.2):

| Tier | files/block | bytes/block |
|------|------------:|------------:|
| ZERO   | 8000 | **0** |
| TINY   | 4088 | 4088 × 16 KiB = **63.875 MiB** |
| SMALL  | 2536 | 2536 × 2 MiB  = **4.9531 GiB** |
| MEDIUM | 400  | 400 × 100 MiB = **39.0625 GiB** |
| LARGE  | 40   | 40 × 1 GiB    = **40 GiB** |

> **The budget is dominated by MEDIUM + LARGE** (~79 GiB per block-pair). ZERO is *free* (0 bytes)
> and TINY is nearly free, so **file-count can be inflated almost arbitrarily on ZERO/TINY** to
> stress the enumerator without spending byte budget — this is how we "add as many files as
> possible under 2 TB".

---

## 5. Budget reconciliation (why the profiles look the way they do)

Three of your asks are in tension under a 2 TB byte ceiling:

- **"Full catalog of the 120 permutations."**
- **"Hundreds of batches per tier."**
- **"Keep it under 2 TB (max 5 TB)."**

A single dataset with **hundreds** of *LARGE* batches is impossible: 200 LARGE batches = 25 blocks =
1000 × 1 GiB ≈ **1 TB for LARGE alone**, per dataset. Likewise 120 datasets each carrying even *one*
LARGE + MEDIUM block already costs `120 × ~79 GiB ≈ 9.3 TB` — over the hard max.

**Resolution (honours intent within physics):**

1. **Hundreds of batches only where it's cheap** — ZERO/TINY/SMALL (request-rate tiers; also exactly
   where the scheduler's request-race / work-stealing dynamics live). MEDIUM/LARGE are capped at a
   small number of blocks (bandwidth tiers; a handful of batches already exercises their scheduling).
2. **Materialise a curated 12 of the 120 permutations** (systematic coverage — each tier occupies
   each chain position ≥ twice, §8). The remaining permutations are **defined** (§11) and
   generatable on demand at ≈ 89 GiB each.
3. **Two scale profiles** (§6): a cheap **ORDER** profile for breadth of orderings, and a **DEEP**
   profile for batch-count stress.

Materialised total ≈ **1.88 TB** (§8.3) — under the 2 TB target, with head-room to 5 TB if you
enable more permutations or deeper MEDIUM/LARGE.

---

## 6. Scale profiles

### 6.1 `ORDER` profile — breadth of orderings (per-tier depth R)

| Tier | R (blocks) | files | full batches | bytes |
|------|-----------:|------:|-------------:|------:|
| ZERO   | 2 | 16 000 | 8  | 0 |
| TINY   | 2 | 8 176  | 16 | 127.75 MiB |
| SMALL  | 2 | 5 072  | 16 | 9.9063 GiB |
| MEDIUM | 1 | 400    | 8  | 39.0625 GiB |
| LARGE  | 1 | 40     | 8  | 40 GiB |
| **Total** | | **29 688** | **56** | **≈ 89.09 GiB** |

`R = 2` on the cheap tiers gives a **streamed in-order** batch signal (§3.2); MEDIUM/LARGE stay at
`R = 1` (order still proven via `source.index`). Purpose: validate **enumeration order** across many
orderings cheaply.

### 6.2 `DEEP` profile — batch-count stress (hundreds of batches on cheap tiers)

| Tier | R (blocks) | files | full batches | bytes |
|------|-----------:|------:|-------------:|------:|
| ZERO   | 100 | 800 000 | **400** | 0 |
| TINY   | 50  | 204 400 | **400** | 3.1189 GiB |
| SMALL  | 25  | 63 400  | **200** | 123.83 GiB |
| MEDIUM | 2   | 800     | 16      | 78.125 GiB |
| LARGE  | 2   | 80      | 16      | 80 GiB |
| **Total** | | **1 068 680** | **1032** | **≈ 285.07 GiB** |

Purpose: exercise **weighted allocation, per-tier caps, refill preference, and work-stealing** with
a deep pending backlog. Hundreds of batches on ZERO/TINY/SMALL; MEDIUM/LARGE kept at 2 blocks to
respect the byte budget (raise them only if you spend toward the 5 TB ceiling).

---

## 7. How to generate the data (`datagen`)

Each level of the chain is one **`flat`-mode** spec (all files in one directory, no subdirs). The
nested link dirs are created implicitly because a child spec's `root` is created if absent.

### 7.1 Per-tier spec templates

`content.type: random` writes real bytes (needed if you also measure throughput). Swap to
`content.type: sparse` to keep the *size metadata* (all classification/batching is identical) while
using near-zero disk — recommended if you only validate enumeration/scheduling.

```yaml
# --- ZERO level (0-byte files) ---
version: 1
mode: flat
root: <LEVEL_DIR>
seed: 101
content: { type: sparse }   # 0-byte: sparse or random are equivalent
size:    { type: fixed, bytes: 0 }
naming:  { prefix: "z-", length: 12 }
flat:    { num_files: 16000 }   # R=2 (ORDER) ; 800000 for DEEP
```
```yaml
# --- TINY level (16 KiB files) ---
version: 1
mode: flat
root: <LEVEL_DIR>
seed: 102
content: { type: random }
size:    { type: fixed, bytes: 16KB }   # 16 KiB, clearly < 1 MB
naming:  { prefix: "t-", length: 12 }
flat:    { num_files: 8176 }    # ORDER ; 204400 for DEEP
```
```yaml
# --- SMALL level (2 MiB files) ---
version: 1
mode: flat
root: <LEVEL_DIR>
seed: 103
content: { type: random }
size:    { type: fixed, bytes: 2MB }    # 2 MiB, in [1MB,100MB)
naming:  { prefix: "s-", length: 12 }
flat:    { num_files: 5072 }    # ORDER ; 63400 for DEEP
```
```yaml
# --- MEDIUM level (100 MiB files) ---
version: 1
mode: flat
root: <LEVEL_DIR>
seed: 104
content: { type: sparse }   # sparse strongly advised for MEDIUM/LARGE
size:    { type: fixed, bytes: 100MB }  # 100 MiB, in [100MB,1GB)
naming:  { prefix: "m-", length: 12 }
flat:    { num_files: 400 }     # ORDER ; 800 for DEEP
```
```yaml
# --- LARGE level (1 GiB files) ---
version: 1
mode: flat
root: <LEVEL_DIR>
seed: 105
content: { type: sparse }
size:    { type: fixed, bytes: 1GB }    # 1 GiB, ≥ 1 GB
naming:  { prefix: "l-", length: 12 }
flat:    { num_files: 40 }      # ORDER ; 80 for DEEP
```

> **Size-string note.** `datagen` size suffixes are **binary, base-1024** (per the Datagen Spec File
> Guide §11): `16KB`=16 KiB, `2MB`=2 MiB, `100MB`=100 MiB, `1GB`=1 GiB. So the sizes above are exact
> and land clearly inside their buckets. (`flat` mode requires the file count under a `flat:` section,
> `flat: { num_files: N }` — not a top-level key.)

### 7.2 Chain generator (drives the 5 specs per dataset)

```python
#!/usr/bin/env python3
"""Materialise one deterministic-enumeration dataset for a given tier ordering."""
import itertools, subprocess, sys, textwrap
from pathlib import Path

TIERS = ["ZERO", "TINY", "SMALL", "MEDIUM", "LARGE"]     # canonical order for the ID scheme

# (file_size, files_per_block, prefix, content_type)
SPEC = {
    "ZERO":   ("0",     8000, "z", "sparse"),
    "TINY":   ("16KB",  4088, "t", "random"),
    "SMALL":  ("2MB",   2536, "s", "random"),
    "MEDIUM": ("100MB", 400,  "m", "sparse"),
    "LARGE":  ("1GB",   40,   "l", "sparse"),
}
ORDER_R = {"ZERO": 2, "TINY": 2, "SMALL": 2, "MEDIUM": 1, "LARGE": 1}          # §6.1
DEEP_R  = {"ZERO": 100, "TINY": 50, "SMALL": 25, "MEDIUM": 2, "LARGE": 2}      # §6.2

def emit(base: Path, ordering, blocks, seed0=100):
    """ordering: list like ['ZERO','TINY',...]; blocks: {tier: R}."""
    level_dir = base
    for depth, tier in enumerate(ordering):
        size, per_block, prefix, ctype = SPEC[tier]
        nfiles = per_block * blocks[tier]
        spec = textwrap.dedent(f"""\
            version: 1
            mode: flat
            root: {level_dir}
            seed: {seed0 + depth}
            content: {{ type: {ctype} }}
            size:    {{ type: fixed, bytes: {size} }}
            naming:  {{ prefix: "{prefix}-", length: 12 }}
            flat:    {{ num_files: {nfiles} }}
        """)
        sp = base / f"_spec_L{depth}_{tier}.yaml"
        sp.write_text(spec)
        subprocess.run(["./build/datagen", "--spec", str(sp)], check=True)
        level_dir = level_dir / f"L{depth+1}"          # next level nests one deeper

if __name__ == "__main__":
    # e.g.  python gen.py /bryck/sched ORDER 1   ->  perm #1 in ORDER profile
    root, profile, perm_id = sys.argv[1], sys.argv[2].upper(), int(sys.argv[3])
    ordering = list(itertools.permutations(TIERS))[perm_id - 1]   # §11 ID scheme
    blocks = ORDER_R if profile == "ORDER" else DEEP_R
    ds = Path(root) / f"SCH-{profile[:3]}-{perm_id:03d}"
    emit(ds, list(ordering), blocks)
```

The `perm_id → ordering` map is **exactly** `itertools.permutations(["ZERO","TINY","SMALL",
"MEDIUM","LARGE"])[perm_id-1]` (1-based) — the canonical catalog of all 120 orderings (§11).

---

## 8. Materialised catalog

### 8.1 `SCH-ORD-01 … 12` — order-verification (ORDER profile, ≈ 89.09 GiB each)

Curated so every tier occupies every chain position at least twice (two Latin squares + 2 scenario
rows). `Z=ZERO, T=TINY, S=SMALL, M=MEDIUM, L=LARGE`.

| ID | C1 | C2 | C3 | C4 | C5 | Notes |
|----|----|----|----|----|----|-------|
| SCH-ORD-01 | Z | T | S | M | L | ascending (0-byte → large) — the canonical example |
| SCH-ORD-02 | T | S | M | L | Z | cyclic +1 |
| SCH-ORD-03 | S | M | L | Z | T | cyclic +2 |
| SCH-ORD-04 | M | L | Z | T | S | cyclic +3 |
| SCH-ORD-05 | L | Z | T | S | M | cyclic +4 (large-first) |
| SCH-ORD-06 | L | M | S | T | Z | descending (large → 0-byte) |
| SCH-ORD-07 | M | S | T | Z | L | reverse-cyclic +1 |
| SCH-ORD-08 | S | T | Z | L | M | reverse-cyclic +2 |
| SCH-ORD-09 | T | Z | L | M | S | reverse-cyclic +3 |
| SCH-ORD-10 | Z | L | M | S | T | reverse-cyclic +4 |
| SCH-ORD-11 | L | S | Z | T | M | scenario: large-first, mixed |
| SCH-ORD-12 | M | Z | L | T | S | scenario: medium-first, mixed |

Each: **29 688 files, 56 batches** (per §6.1), enumerated strictly in its `C1..C5` chain order.

### 8.2 `SCH-DEEP-01 … 03` — scheduling stress (DEEP profile, ≈ 285.07 GiB each)

| ID | C1 | C2 | C3 | C4 | C5 | Scheduling scenario it drives |
|----|----|----|----|----|----|-------------------------------|
| SCH-DEEP-01 | Z | T | S | M | L | Balanced ascending backlog. All tiers pending → weighted allocation + per-tier caps; LARGE (16 batches) drains long before SMALL (200) → **work-stealing** as tiers empty. |
| SCH-DEEP-02 | L | M | S | T | Z | Large-first enumeration. Bandwidth tiers created first; verify tiny/small are **not starved** (100 GbE profile still reserves slots). |
| SCH-DEEP-03 | T | Z | S | M | L | Tiny-first (WAN-like). Huge cheap-tier request backlog created first; verify request-race tiers drain while MEDIUM/LARGE trickle. |

Each: **1 068 680 files, 1032 batches** (per §6.2). Per-tier pending backlog (the oracle for
weighted-allocation / work-stealing tests):

| Tier | pending batches | files/batch | note |
|------|----------------:|------------:|------|
| ZERO   | 400 | 2000 | drains on request rate |
| TINY   | 400 | 511  | request-race tier |
| SMALL  | 200 | 317  | request-race tier |
| MEDIUM | 16  | 50   | bandwidth tier (budget-capped) |
| LARGE  | 16  | 5    | bandwidth tier (budget-capped) |

### 8.3 Total footprint

| Group | Count | Per-dataset | Files/dataset | Subtotal bytes |
|-------|------:|------------:|--------------:|---------------:|
| SCH-ORD-01..12  | 12 | 89.09 GiB  | 29 688     | 1069.1 GiB |
| SCH-DEEP-01..03 | 3  | 285.07 GiB | 1 068 680  | 855.2 GiB |
| **Total** | **15** | | **≈ 3.56 M files** | **≈ 1924 GiB ≈ 1.88 TB** |

Under the 2 TB target. Head-room to the 5 TB max ≈ 3.1 TB — spend it by (a) materialising more of
the 120 permutations at ORDER scale (≈ 89 GiB each → ~35 more fit), or (b) raising MEDIUM/LARGE `R`
in the DEEP datasets.

---

## 9. Validation procedure (how to use the oracle)

For each dataset:

1. **Enumeration order (deterministic).** After a scan (or `BATCH_BUILDER_ONLY=true`), assert
   `source.index` is tier-contiguous in the chain order `C1..C5` with the exact per-tier file counts
   from §8. (Also check `scan.discovered`/`scan.completed` reflect the single chain: one dir per
   level.) *This is independent of `scandir` and of the scheduler.*
2. **Batch shapes/counts (deterministic).** Assert `batches.created` contains exactly the per-tier
   batch counts and `(nfiles, nbytes)` shapes from §2.1/§6. Every full batch: `nfiles == M`,
   `nbytes == M × file_size`.
3. **Terminal-round caveat (§3.2).** When asserting *publish order*, only assert order for
   **streamed** rounds (`R ≥ 2` tiers). Treat each tier's final `K` batches as a finish()-time set,
   not an ordered sequence. Never assert cross-tier publish order for `R = 1` tiers — use step 1 for
   their order.
4. **Scheduler dispatch (under test — DEEP datasets).** With the pending backlog from §8.2 fully
   built (run `BATCH_BUILDER_ONLY` first, or let enumeration outrun dispatch), sample per-tier
   in-flight counts and assert: weight ratio (±5 %), `in-flight[tier] ≤ max_concurrent[tier]`,
   same-tier refill preference, and work-stealing convergence as a tier drains — against the known
   pending counts, **not** against creation order.

> **Key invariant to remember:** the dataset fixes *what work exists and in what enumeration order*;
> the scheduler decides *dispatch order*. A passing scheduler may dispatch in a completely different
> order than files were enumerated — that's expected and is exactly what §9.4 measures.

---

## 10. Assumptions & open items

- **A1 — walker is BFS single-process** (`docs/batch_builder_design.md` §7.1). If the multiprocess
  walker (task B7) is enabled, files from *different directories* are enumerated concurrently by N
  walkers. Within our **single chain** there is only ever **one non-empty directory at a time**
  (frontier depth 1), so N-walker parallelism collapses to one active walker → **order is preserved**.
  Still worth confirming the multiprocess path honours the frontier the same way.
- **A2 — `slot = global_file_index % K`.** The starting slot for a tier depends on the running global
  file counter (sum of earlier tiers' counts). This only *rotates which slot* gets the first file; it
  never changes batch counts/shapes because every file in a tier is identical. Oracle unaffected.
- **A3 — `TARGET_SIZE_MB = 0` for ZERO** means the byte cap never trips (`0 + 0 ≯ 0`); ZERO batches
  close purely on `BATCH_SIZE = 2000`. Confirm the build treats a 0 target as "count-only", not as
  "close every file".
- **A4 — `datagen` size-string units** are **binary/base-1024** (Datagen Spec File Guide §11), so
  `16KB/2MB/100MB/1GB` = KiB/MiB/MiB/GiB exactly. No ambiguity; sizes sit clearly inside their buckets.
- **A5 — sparse vs real bytes.** Enumeration/scheduling depend only on **size metadata**, so `sparse`
  is sufficient and keeps disk near-zero — but a real *transfer* of sparse files still reads them as
  full-size zero bytes. Use `random` only where you also measure throughput.
- **A6 — budget knobs.** To push toward 5 TB: bump `SCH-DEEP` MEDIUM/LARGE `R`, or materialise more
  of the 120 permutations (§11) at ORDER scale.

---

## 11. Appendix — the full 120-permutation catalog (ID scheme)

The complete catalog is **defined, not hand-listed**, to stay exact and error-free. The canonical
ordering of a permutation ID is:

```python
import itertools
TIERS = ["ZERO", "TINY", "SMALL", "MEDIUM", "LARGE"]
CATALOG = list(itertools.permutations(TIERS))   # 120 orderings, 0-based
def ordering_for(perm_id):        # perm_id is 1-based (1..120)
    return CATALOG[perm_id - 1]
# e.g. ordering_for(1)  == ('ZERO','TINY','SMALL','MEDIUM','LARGE')
#      ordering_for(120)== ('LARGE','MEDIUM','SMALL','TINY','ZERO')
```

- Any ordering `perm_id ∈ [1,120]` is materialisable with the §7.2 generator:
  `python gen.py <root> ORDER <perm_id>` (≈ 89 GiB) or `... DEEP <perm_id>` (≈ 285 GiB).
- The **12 materialised** ORDER datasets (§8.1) correspond to specific `perm_id`s; the remaining 108
  orderings are on-demand and cost-tracked by the §4 block table.
- IDs are stable: `perm_id` ↔ ordering never changes, so tests can reference orderings by number
  across runs and machines.
