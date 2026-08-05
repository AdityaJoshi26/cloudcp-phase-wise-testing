#!/usr/bin/env bash
set -euo pipefail

python3 CloudCpCliTesting/cloudcpclitesting.py \
  --dataset DS-P8-02 \
  --output-base /bryck/cloudcp_cli_data \
  --dst s3://aditya/cloudcp-cli/DS-P8-02 \
  --yes "$@"