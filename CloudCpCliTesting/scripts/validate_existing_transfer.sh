#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <transfer-id> [extra runner args...]" >&2
  exit 2
fi

transfer_id="$1"
shift

python3 CloudCpCliTesting/cloudcpclitesting.py \
  --dataset DS-P2-01 \
  --output-base /bryck/cloudcp_cli_data \
  --dst s3://aditya/cloudcp-cli/DS-P2-01 \
  --skip-transfer \
  --transfer-id "$transfer_id" \
  "$@"