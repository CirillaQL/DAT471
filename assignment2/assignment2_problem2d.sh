#!/bin/bash

#SBATCH --job-name=assignment2_p2d
#SBATCH --output=assignment2_problem2d.out
#SBATCH --error=assignment2_problem2d.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=64

set -euo pipefail

CONTAINER="/data/courses/2026_dat471_dit066/containers/assignment2.sif"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="${SCRIPT_DIR}/assignment2_problem2d.py"
DATASET_PATH="/data/courses/2026_dat471_dit066/datasets/gutenberg/huge"
RESULT_DIR="${SCRIPT_DIR}/assignment2_problem2d_results"
SUMMARY_FILE="${RESULT_DIR}/problem2d_summary.txt"
WORKERS=(1 2 4 8 16 32 64)

mkdir -p "$RESULT_DIR"
: > "$SUMMARY_FILE"

for workers in "${WORKERS[@]}"; do
  result_file="${RESULT_DIR}/workers_${workers}.txt"

  {
    echo "workers=${workers}"
    echo "started_at=$(date --iso-8601=seconds)"
    echo

    apptainer exec \
      --bind /data:/data \
      --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
      "$CONTAINER" \
      python3 "$SCRIPT_PATH" --num-workers "$workers" "$DATASET_PATH"

    echo
    echo "finished_at=$(date --iso-8601=seconds)"
  } 2>&1 | tee "$result_file"

  {
    echo "===== workers=${workers} ====="
    grep -E '^(Time|Total time|Checksum)' "$result_file" || true
    echo
  } >> "$SUMMARY_FILE"
done
