#!/bin/bash
# Pipeline Runner Script
#
# Usage: ./scripts/run_pipeline.sh <pipeline_name> [args...]
# 
# Examples:
#   ./scripts/run_pipeline.sh okx_instruments
#   ./scripts/run_pipeline.sh okx_funding_rates --name my_portfolio --funding_rate
#   ./scripts/run_pipeline.sh okx_funding_rates --name my_portfolio --funding_rate --start 2026-01-01 --end 2026-03-01
#   ./scripts/run_pipeline.sh okx_funding_rates --name my_portfolio --funding_rate --backfill
#
# Crontab examples:
#   # Sync instruments daily at 00:00 UTC
#   0 0 * * * /home/ubuntu/clawd/repos/AlphaHub/scripts/run_pipeline.sh okx_instruments
#
#   # Sync funding rates every 8 hours (incremental)
#   0 */8 * * * /home/ubuntu/clawd/repos/AlphaHub/scripts/run_pipeline.sh okx_funding_rates --name my_portfolio --funding_rate

set -euo pipefail

# Get script and repo directories
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

# Load secrets from Clawdbot environment
if [[ -f /home/ubuntu/clawd/.env.secrets ]]; then
    source /home/ubuntu/clawd/.env.secrets
else
    echo "ERROR: /home/ubuntu/clawd/.env.secrets not found"
    exit 1
fi

# Export required environment variables
export DATABASE_URL="${ALPHAHUB_DATABASE_URL:?ALPHAHUB_DATABASE_URL not set in .env.secrets}"
export TELEGRAM_BOT_TOKEN="${ALPHAHUB_TELEGRAM_BOT_TOKEN:-}"
export TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID_DAVID:-}"

# Validate arguments
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <pipeline_name> [args...]"
    echo ""
    echo "Available pipelines:"
    echo "  okx_instruments      - Sync OKX instruments (SWAP + SPOT)"
    echo "  okx_funding_rates    - Sync OKX funding rates"
    echo ""
    echo "Options for okx_funding_rates:"
    echo "  --name NAME          - Portfolio name (required)"
    echo "  --funding_rate       - Fetch funding rates (required)"
    echo "  --start DATE         - Start date (YYYY-MM-DD)"
    echo "  --end DATE           - End date (YYYY-MM-DD)"
    echo "  --backfill           - Fetch full history (paginate all data)"
    exit 1
fi

PIPELINE_NAME="$1"
shift  # Remove pipeline name, keep remaining args

# Change to repo directory
cd "$REPO_DIR"

# Add repo to PYTHONPATH for imports
export PYTHONPATH="${REPO_DIR}:${PYTHONPATH:-}"

# Run the pipeline
echo "[$(date -Iseconds)] Running pipeline: $PIPELINE_NAME"
python3 -m "pipeline.${PIPELINE_NAME}" "$@"
echo "[$(date -Iseconds)] Pipeline completed: $PIPELINE_NAME"
