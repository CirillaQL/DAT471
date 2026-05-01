#!/bin/bash

set -euo pipefail

SCRIPT_DIR="${ASSIGNMENT3_DIR:-/data/users/luqia/DAT471/assignment3}"
RESULT_DIR="${SCRIPT_DIR}/assignment3_problem3_results_parallel"
SUMMARY_FILE="${RESULT_DIR}/problem3_summary.txt"
CSV_FILE="${RESULT_DIR}/speedup.csv"
WORKERS=(1 2 4 8 16 32)

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
      grep -E '^(workers=|cpus_per_task=|dataset=|most followed id|most followed|average followed|count follows no-one|Number of workers:|Time elapsed:)' "$result_file" || true
      total_time="$(awk '/^Time elapsed:/ {print $3}' "$result_file")"
    else
      echo "missing result file: $result_file"
    fi

    if [[ -n "$total_time" ]]; then
      if [[ -z "$baseline_time" && "$workers" -eq 1 ]]; then
        baseline_time="$total_time"
        echo "Single-core runtime: ${baseline_time} seconds"
      fi
      if [[ -n "$baseline_time" ]]; then
        speedup="$(awk -v baseline="$baseline_time" -v total="$total_time" 'BEGIN { printf "%.6f", baseline / total }')"
        echo "Speedup compared with 1 worker: ${speedup}"
        echo "${workers},${total_time},${speedup}" >> "$CSV_FILE"
      fi
    fi
    echo
  } >> "$SUMMARY_FILE"
done

echo "Wrote summary to ${SUMMARY_FILE}"
echo "Wrote speedup CSV to ${CSV_FILE}"
