#!/usr/bin/env bash
set -euo pipefail

python3 CloudCpCliTesting/cloudcpclitesting.py \
  --dataset DS-P2-01 \
  --output-base /bryck/cloudcp_cli_data \
  --dst s3://aditya/cloudcp-cli/DS-P2-01 \
  --yes "$@"