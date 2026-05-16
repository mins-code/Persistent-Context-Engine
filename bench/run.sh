#!/usr/bin/env bash
set -e

# Change directory to the root workspace where python scripts exist
cd "$(dirname "$0")/.."

# Prevent Python from buffering stdout/stderr output lines
export PYTHONUNBUFFERED=1

echo "========================================================="
echo "==> VALIDATING ARCHITECTURE SHAPES (SELF-CHECK)..."
echo "========================================================="
python self_check.py --adapter adapters.myteam:Engine --quick

echo ""
echo "========================================================="
echo "==> RUNNING CANONICAL LEVEL 2 AUTOMATED BENCHMARK SUITE..."
echo "========================================================="
# L2 configuration maps to the standard 12 services and 7 days setup
python run.py \
    --adapter adapters.myteam:Engine \
    --mode fast \
    --seeds 42 101 202 303 404 \
    --n-services 12 \
    --days 7 \
    --out -

echo ""
echo "========================================================="
echo "==> RUNNING ADVERSARIAL LEVEL 3 AUTOMATED BENCHMARK SUITE..."
echo "========================================================="
# L3 configuration extracts the exact parameters from your uploaded l3_sample.json:
# Seeds: 314159 271828 161803 141421 173205
# Services: 30, Days: 21
python run.py \
    --adapter adapters.myteam:Engine \
    --mode fast \
    --seeds 314159 271828 161803 141421 173205 \
    --n-services 30 \
    --days 21 \
    --out -