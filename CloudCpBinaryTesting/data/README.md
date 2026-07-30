# Cloudcp Binary Testing

Datasets + batch files for testing the `cloudcp` binary. See
[../plan_cp_binary.md](../plan_cp_binary.md) for the full test plan.

## Layout

- `specs/` — `datagen` spec files that materialize the **positive** datasets.
- `batches/` — (generated) NUL-framed batch files from `make_batches.py`.
- `negative_data/` — (generated) hostile filesystem objects from `make_batches.py --negative`.
- `negative_batches/` — (generated) malformed batch files from `make_batches.py --negative`.

## Run order

```bash
# 1. Materialize positive data (set each spec's root under your --fs-prefix first)
for s in specs/*.yaml; do ./build/datagen --spec "$s"; done

# 2. Build positive batch files
python ../make_batches.py /bryck/1mb_halfmill/cloudcp_test -o batches --batch-size 500

# 3. Build the negative suite (hostile files + malformed batches)
python ../make_batches.py --negative -o .

# 4. Run cloudcp against a batch file
/opt/bryck/aws/bin/cloudcp "batches/batch_000000.txt" \
    --bucket aditya --fs-prefix /bryck/1mb_halfmill \
    --transfer-id 103 --prefix cloudcp_test --endpoint-url https://10.10.10.103:9000
```

## Notes

- Adjust each spec's `root:` to sit under the `--fs-prefix` root so `cloudcp`
  strips the prefix correctly.
- `05_large_files.yaml` and `06_sparse_files.yaml` use sparse content to keep
  local disk usage low; switch to `type: random` for full checksum coverage.
- Negative POSIX objects (symlinks, FIFO, `chmod 000`, newline/non-UTF-8 names)
  are created on Linux/macOS and skipped on Windows.
