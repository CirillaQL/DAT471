#!/bin/bash

set -euo pipefail

SCRIPT_DIR="${ASSIGNMENT2_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
RESULT_DIR="${SCRIPT_DIR}/assignment2_problem2e_results_parallel"
SUMMARY_FILE="${RESULT_DIR}/problem2e_summary.txt"
CSV_FILE="${RESULT_DIR}/speedup.csv"
WORKERS=(1 2 4 8 16 32 64)

mkdir -p "$RESULT_DIR"
: > "$SUMMARY_FILE"
echo "workers,total_time,speedup" > "$CSV_FILE"

baseline_time=""
for workers in "${WORKERS[@]}"; do
  result_file="${RESULT_DIR}/workers_${workers}.txt"
  total_time=""

  {
    echo "===== workers=${workers} ====="
    if [[ -f "$result_file" ]]; then
      grep -E '^(workers=|cpus_per_task=|batch_size=|dataset=|Time|Total time|Checksum)' "$result_file" || true
      total_time="$(awk '/^Total time:/ {print $3}' "$result_file")"
    else
      echo "missing result file: $result_file"
    fi

    if [[ -n "$total_time" ]]; then
      if [[ -z "$baseline_time" && "$workers" -eq 1 ]]; then
        baseline_time="$total_time"
      fi
      if [[ -n "$baseline_time" ]]; then
        speedup="$(awk -v baseline="$baseline_time" -v total="$total_time" 'BEGIN { printf "%.6f", baseline / total }')"
        echo "Speedup compared with 1 worker: ${speedup}"
        echo "${workers},${total_time},${speedup}" >> "$CSV_FILE"
      fi
      if [[ "$workers" -eq 64 ]]; then
        echo "Total absolute running time with 64 cores: ${total_time} seconds"
      fi
    fi
    echo
  } >> "$SUMMARY_FILE"
done
