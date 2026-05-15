#!/usr/bin/env bash
set -e

# Change directory to the root directory where the python scripts exist
cd "$(dirname "$0")/.."

# Prevent Python from buffering stdout/stderr outputs
export PYTHONUNBUFFERED=1

echo "==> Validating Engine Architecture Shapes..."
# Validates field names (incident_id, cause_event_id, effect_event_id) against schema.py
python self_check.py --adapter adapters.myteam:Engine --quick

echo "==> Starting Automated Evaluation Suite..."
# Runs the main benchmark scenario over the standard seeds and prints the evaluation JSON block
python run.py \
    --adapter adapters.myteam:Engine \
    --mode fast \
    --seeds 42 101 202 303 404 \
    --n-services 12 \
    --days 7 \
    --out -