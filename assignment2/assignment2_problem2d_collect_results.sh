#!/bin/bash

set -euo pipefail

SCRIPT_DIR="${ASSIGNMENT2_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
RESULT_DIR="${SCRIPT_DIR}/assignment2_problem2d_results_parallel"
SUMMARY_FILE="${RESULT_DIR}/problem2d_summary.txt"
WORKERS=(1 2 4 8 16 32 64)

mkdir -p "$RESULT_DIR"
: > "$SUMMARY_FILE"

for workers in "${WORKERS[@]}"; do
  result_file="${RESULT_DIR}/workers_${workers}.txt"
  {
    echo "===== workers=${workers} ====="
    if [[ -f "$result_file" ]]; then
      grep -E '^(workers=|cpus_per_task=|batch_size=|dataset=|Time|Total time|Checksum)' "$result_file" || true
    else
      echo "missing result file: $result_file"
    fi
    echo
  } >> "$SUMMARY_FILE"
done
