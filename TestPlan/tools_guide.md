# Tools Guide — CloudCP Test Suite

Every tool used across the phases, what it is for, and its `--help` / usage. Run each with
`--help` on the host for the authoritative option list; the summaries below are the
`--help`-level view.

> Host note: `datagen`, `cloudcp`, `aws`, and the `/bryck` & `/opt` paths exist only on the
> Linux test host (0.71 / the DT host). `--dry-run` modes work anywhere.

---

## 1. `datagen` — materialize files from a spec

Reads a datagen YAML spec and creates real files on disk under the spec's `root`.

```bash
./datagen --spec <spec>.yaml
```

- Input: a spec file (see [../CloudCpBinaryTesting/DatagenSpecFileGuide.md](../CloudCpBinaryTesting/DatagenSpecFileGuide.md)).
- Output: real files under `root:` (set `root` under your `--fs-prefix` / mount path first).
- Cannot produce hostile/corrupt objects — those come from `make_batches.py --negative`.

---

## 2. `generate_specs.py` — build spec files from the dataset plan

Location: [../dataset_cloudcp/spec_files/generate_specs.py](../dataset_cloudcp/spec_files/generate_specs.py)

```text
usage: generate_specs.py [--out DIR] [--root-base PATH] [--datasets IDs] [--dry-run] [--list]

  --out DIR         Output directory (default: script dir)
  --root-base PATH  Base path for generated data roots (default: /bryck/cloudcp)
  --datasets IDs    Comma-separated dataset IDs to emit (default: all)
  --dry-run         Plan + validate, write nothing
  --list            List datasets and exit
```

Example:

```bash
python generate_specs.py --list
python generate_specs.py --datasets DS-P2-02,DS-P2-03 --root-base /bryck/mount --out ./specs
```

---

## 3. `dataset_validator.py` — run specs, validate counts, clean up, report

Location: [../dataset_cloudcp/spec_files/dataset_validator.py](../dataset_cloudcp/spec_files/dataset_validator.py)

Runs datagen spec files phase-by-phase, rewrites each spec's `root` under an output base,
runs datagen in parallel, validates generated file counts against `manifest.json`, cleans
up, and writes a JSON report. Also has a stand-alone delete mode.

```text
usage: dataset_validator.py [selection] [execution] [validation] [behaviour] [output]

selection:
  --spec-dir, -s DIR        Dir with DS-P*/ folders + manifest.json (default: cwd)
  --phase-from N            First phase [1-12] (default: 1)
  --phase-to N              Last phase  [1-12] (default: 11)
  --one-phase N             Shortcut: set phase-from = phase-to = N
  --datasets IDs            Explicit IDs, e.g. DS-P1-01,DS-P2-03 (overrides phase range)
  --dataset-num, -d N[,N]   Dataset number(s) within each phase

execution:
  --datagen PATH            datagen binary (default: ./datagen or $DATAGEN_BIN)
  --output-base DIR         Base dir for generated files (default: auto temp)
  --parallel N              Spec files run in parallel per dataset (default: 4)

validation:
  --tolerance PCT           Allowed % deviation from expected count (default: 0 = exact)

behaviour:
  --keep-on-fail            Keep output dir when a spec fails
  --stop-on-fail            Stop after first failing dataset
  --skip-delete             Keep output dirs after validation
  --delete                  Delete mode: remove generated dirs under --output-base
  --dry-run                 Show plan only; create/delete nothing
  --ask                     Interactive setup + per-dataset confirmation

output:
  --verbose, -v             Show all DEBUG detail on console
  --report FILE             Write JSON run report
  --log FILE                Write detailed debug log
  --version
```

Examples:

```bash
# Dry-run a phase range (nothing created/deleted)
python dataset_validator.py --phase-from 1 --phase-to 3 --dry-run

# Run exactly one dataset with a report
python dataset_validator.py --one-phase 2 --dataset-num 2 \
    --datagen ./datagen --output-base /tmp/gen --report results.json

# Delete previously generated data for specific datasets
python dataset_validator.py --delete --output-base /tmp/gen --datasets DS-P1-01,DS-P1-02
```

---

## 4. `bcloud_src_enum.py` — batch builder / enumerator (broker entry, batch-only)

The broker's source enumerator. In `--batch-only` mode it runs the BatchBuilder to produce
batch files + `batch_summary.csv` **without** uploading. This is the tool under test in the
batch-builder phase.

```bash
/opt/bryck/.venv/bryck/bin/python3 \
  /opt/bryck/.venv/bryck/lib/python3.10/site-packages/bryckcloud/lib/cloud/bcloud_src_enum.py \
  -i <transfer-id> </bryck/mount/path/with/data> --batch-only
```

- `-i <transfer-id>` — transfer id; determines the output directory
  `.../bcloud_batchmeta/transfer_<id>/`.
- positional `<path>` — source directory (mount path) to scan.
- `--batch-only` — build batches + summary, do not dispatch/upload.
- Output summary: `/opt/bryck/bryckapi/downloads/bcloud_batchmeta/transfer_<id>/batch_summary.csv`.

> Run it once to confirm the exact generated path on your host, then wire the comparator to it.
> Config that governs tier sizes/seals is read from `/etc/bryck/bryckcloud/config.json`.

---

## 5. `make_batches.py` — build NUL-framed batch files (binary suite)

Location: [../CloudCpBinaryTesting/make_batches.py](../CloudCpBinaryTesting/make_batches.py)

Creates NUL-framed `batch_XXXXXX.txt` files from a directory, or builds the negative /
hostile suite that `datagen` cannot produce.

```text
usage: make_batches.py [src_dir] [-o OUTPUT_DIR] [--batch-size N] [--single]
                       [-n | --negative] [--corrupt-from DATASET_DIR]

  src_dir              Directory to enumerate into batch files
  -o, --output-dir     Output directory (default: batches)
  --batch-size N       Files per batch file (default: 1000)
  --single             Emit exactly one batch_000000.txt
  -n, --negative       Build hostile files + malformed batch files (B01-B12, N01-N11)
  --corrupt-from DIR   Derive corrupt batches from an existing dataset dir
```

Examples:

```bash
python make_batches.py /bryck/1mb_halfmill/cloudcp_test -o batches --batch-size 500
python make_batches.py --negative -o CloudCpBinaryTesting
```

---

## 6. `run_cloudcp_tests.py` — cloudcp binary end-to-end orchestrator

Location: [../CloudCpBinaryTesting/run_cloudcp_tests.py](../CloudCpBinaryTesting/run_cloudcp_tests.py)

Per dataset: datagen → single batch → stage into `transfer_<id>/batches/inprogress/zero/`
→ run cloudcp → validate `transfer_report_<id>.csv` → clear bucket → write per-run report.

```text
usage: run_cloudcp_tests.py [selection] [configuration] [behaviour]

selection:
  --dataset NAME|NUM   Run one dataset by name or spec number
  --from N             Start of inclusive spec-number range
  --to N               End of inclusive spec-number range
  --negative           Run the negative / malformed-batch suite (B01-B12)
  --all                All positive datasets + negative suite
  --list               List what would run

configuration:
  --specs-dir DIR      Spec directory
  --datagen-bin PATH   datagen binary
  --bucket NAME        Target bucket (default: aditya)
  --endpoint-url URL   Object-store endpoint

behaviour:
  --dry-run            Print commands only; touch nothing
  --skip-delete/--no-clear   Keep uploaded objects (skip bucket clear)
  --yes                Assume yes to prompts
```

Examples:

```bash
python run_cloudcp_tests.py --list
python run_cloudcp_tests.py --dataset tiny_2million
python run_cloudcp_tests.py --from 1 --to 4
python run_cloudcp_tests.py --negative
python run_cloudcp_tests.py --all --dry-run
```

Direct cloudcp invocation (what the orchestrator runs):

```bash
LD_LIBRARY_PATH=/opt/bryck/aws/lib/ /opt/bryck/aws/bin/cloudcp \
  "/opt/bryck/bryckapi/downloads/bcloud_batchmeta/transfer_103/batches/inprogress/small/batch_001611.txt" \
  --bucket aditya \
  --fs-prefix /bryck/1mb_halfmill \
  --transfer-id 103 \
  --prefix cloudcp_test2 \
  --endpoint-url https://10.10.10.103:9000
```

---

## 7. `batch_summary` expectation helper (to be added)

A small Python helper that reads [../dataset_cloudcp/spec_files/manifest.json](../dataset_cloudcp/spec_files/manifest.json)
plus the active `BATCH.*` config and emits the **expected** `batch_summary.csv` (per-tier
batch count and per-batch file/byte rollups) for a dataset, then diffs it against the
generated summary. See [phases/01_batch_builder.md](phases/01_batch_builder.md) §Tools.

Planned interface:

```text
usage: batch_summary_expect.py --dataset DS-Pn-nn --config /etc/bryck/bryckcloud/config.json
                               [--actual <batch_summary.csv>] [--tolerance PCT]
```

**Status: To be added.**

---

## 8. Quick Reference — which tool per phase

| Phase | Primary tools |
|---|---|
| Batch Builder | `datagen`, `generate_specs.py`, `bcloud_src_enum.py --batch-only`, `dataset_validator.py`, `batch_summary_expect.py` (TBA) |
| Scheduler | broker (config `NETWORK_PROFILE`), slot sampler (TBA) |
| CloudCP Binary | `make_batches.py`, `run_cloudcp_tests.py`, `cloudcp` |
| Reporting | verification engine, `dataset_validator.py` (counts), status-injection fixtures (TBA) |
| Fallback | fault-injection proxy (TBA), fallback worker |
| Complete Functional | broker full run, all of the above |
| API | API client / `curl` (endpoint inventory TBA) |
| UI | manual checklist; Playwright/Selenium outline (TBA) |
