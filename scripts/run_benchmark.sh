#!/bin/bash
# VLL Benchmark Runner
# Runs the sweep benchmark and generates plots

set -e

OUTPUT_DIR="${OUTPUT_DIR:-/app/results}"
OUTPUT_PREFIX="${OUTPUT_PREFIX:-benchmark}"
NUM_THREADS="${NUM_THREADS:-5}"
DURATION="${DURATION:-10}"
WRITES_PER_TX="${WRITES_PER_TX:-10}"

mkdir -p "$OUTPUT_DIR"

echo "Running VLL benchmark sweep..."
./build/bench_microbenchmark \
    --sweep \
    --num_threads="$NUM_THREADS" \
    --duration_seconds="$DURATION" \
    --writes_per_tx="$WRITES_PER_TX" \
    --output_prefix="$OUTPUT_DIR/$OUTPUT_PREFIX"

echo ""
echo "Generating plots..."
cd "$OUTPUT_DIR"
python3 /app/scripts/plot_results.py "$OUTPUT_PREFIX"

echo ""
echo "========================================="
echo "Results saved to $OUTPUT_DIR:"
ls -la "$OUTPUT_DIR"
echo "========================================="
