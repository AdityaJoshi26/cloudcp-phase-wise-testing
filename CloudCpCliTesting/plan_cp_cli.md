# CloudCp CLI Test Plan

Test plan for exercising the `bryckcloud transfer add aws` CLI using dataset definitions
from `dataset_cloudcp/spec_files/`.

Invocation under test:

```bash
/opt/bryck/.venv/bryck/bin/bryckcloud transfer add aws \
  --src /bryck/cloudcp_cli_data/DS-P2-01 \
  --dst s3://aditya/cloudcp-cli/DS-P2-01
```

## 1. Pipeline Overview

```text
dataset_cloudcp/spec_files/<dataset>/specs  ->  datagen materializes one dataset on disk
cloudcpclitesting.py                        ->  validates local counts against manifest.json
bryckcloud transfer add aws                 ->  submits a broker-managed transfer
cloud transfer logs                         ->  transfer_report / upload_report shards / final_report
cloudcpclitesting.py                        ->  validates merged transfer results
```

## 2. What this folder tests

- Dataset generation from the authoritative spec catalog.
- Submission through the real `bryckcloud transfer add aws` CLI entry point.
- Transfer id discovery from the created broker artifacts.
- Report-level validation after the transfer completes.
- Reuse of one existing transfer id for validation-only workflows.

## 3. Recommended datasets by intent

| Intent | Dataset | Why |
|---|---|---|
| Smoke | `DS-P8-02` | Single zero-byte file, cheapest end-to-end check. |
| Boundary | `DS-P2-01` | Tier boundary coverage without huge volume. |
| Filename/path | `DS-P4-01` | Filename stress on tiny files. |
| Mixed pipeline | `DS-P7-01` | Realistic mixed workload at moderate size. |

## 4. Validation rules

- Local generated file count must equal the dataset `emitted_files` count in `manifest.json`.
- Merged terminal-success rows across `transfer_report_<id>.csv` and `report/upload_report.*.csv`
  must match the expected dataset file count.
- `final_report.csv` must exist and have the expected row count.
- `S3Path` values in `final_report.csv` must start with the requested destination prefix.
- Reported file sizes must match the local source files.
- `failed_uploads.*` must be empty or absent.
- Live `cloudcp_retry_<id>_*.lst` files must not remain at the end.

## 5. Artifacts

- Per-run local report: `CloudCpCliTesting/runs/run_<timestamp>_<dataset>/report.json`
- Broker batch metadata: `/opt/bryck/bryckapi/downloads/bcloud_batchmeta/transfer_<id>/`
- Transfer logs: `/opt/bryck/bryckapi/downloads/cloud_transfer_logs/cloud_transfer_<id>/`

## 6. Entry points

- `cloudcpclitesting.py`: main dataset-driven CLI runner.
- `scripts/run_smoke_cli_test.sh`: smoke-test wrapper.
- `scripts/run_boundary_cli_test.sh`: boundary-test wrapper.
- `scripts/validate_existing_transfer.sh`: validation-only wrapper.