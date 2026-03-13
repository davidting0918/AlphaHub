#!/bin/bash
# Pipeline Runner Script
#
# Usage: ./scripts/run_pipeline.sh --name <portfolio> <job_type> [--start YYYYMMDD] [--end YYYYMMDD]
#
# Examples:
#   ./scripts/run_pipeline.sh --name OKX_MAIN_01 instrument
#   ./scripts/run_pipeline.sh --name OKX_MAIN_01 funding_rate
#   ./scripts/run_pipeline.sh --name OKX_MAIN_01 --start 20260301 --end 20260313 funding_rate

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

# Load secrets
if [[ -f /home/ubuntu/clawd/.env.secrets ]]; then
    source /home/ubuntu/clawd/.env.secrets
else
    echo "ERROR: /home/ubuntu/clawd/.env.secrets not found"
    exit 1
fi

export DATABASE_URL="${ALPHAHUB_DATABASE_URL:?ALPHAHUB_DATABASE_URL not set}"
export PYTHONPATH="${REPO_DIR}:${PYTHONPATH:-}"

cd "$REPO_DIR"

echo "[$(date -Iseconds)] Running pipeline: $@"
python3 -m pipeline.job_manager "$@"
echo "[$(date -Iseconds)] Pipeline completed"
