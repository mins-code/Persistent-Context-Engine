#!/usr/bin/env bash
set -e

# Move into the root directory where the python files reside
cd "$(dirname "$0")/.."

# Prevent Python from buffering standard log channels
export PYTHONUNBUFFERED=1

echo "========================================================="
echo "==> VALIDATING ARCHITECTURE SHAPES (SELF-CHECK)..."
echo "========================================================="
python self_check.py --adapter adapters.myteam:Engine --quick

echo ""
echo "========================================================="
echo "==> EXECUTING LEVEL 2 SIMULATION -> report.json"
echo "========================================================="
# L2 Configuration: Saves tracking data to report.json
python run.py \
    --adapter adapters.myteam:Engine \
    --mode fast \
    --seeds 42 101 202 303 404 \
    --n-services 12 \
    --days 7 \
    --out report.json

echo "SUCCESS: Level 2 evaluation saved to report.json"

echo ""
echo "========================================================="
echo "==> EXECUTING LEVEL 3 SIMULATION -> report_l3.json"
echo "========================================================="
# L3 Configuration: Saves structural adversarial matrix to report_l3.json
python run.py \
    --adapter adapters.myteam:Engine \
    --mode fast \
    --seeds 314159 271828 161803 141421 173205 \
    --n-services 30 \
    --days 21 \
    --out report_l3.json

echo "SUCCESS: Level 3 evaluation saved to report_l3.json"