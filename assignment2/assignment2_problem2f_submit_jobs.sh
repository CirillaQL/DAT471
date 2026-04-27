#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOB_SCRIPT="${SCRIPT_DIR}/assignment2_problem2f_job.sh"
COLLECT_SCRIPT="${SCRIPT_DIR}/assignment2_problem2f_collect_results.sh"
RESULT_DIR="${SCRIPT_DIR}/assignment2_problem2f_results_parallel"
BATCH_SIZE=10
DATASET_PATH="${DATASET_PATH:-/data/courses/2026_dat471_dit066/datasets/gutenberg/huge}"
WORKERS=(1 2 4 8 16 32 64)

mkdir -p "$RESULT_DIR"

job_ids=()
for workers in "${WORKERS[@]}"; do
  cpus=$((workers + 2))
  job_id="$(
    sbatch \
      --parsable \
      --job-name="a2_p2f_w${workers}" \
      --cpus-per-task="$cpus" \
      --output="${RESULT_DIR}/workers_${workers}.slurm.out" \
      --error="${RESULT_DIR}/workers_${workers}.slurm.err" \
      --export=ALL,ASSIGNMENT2_DIR="$SCRIPT_DIR",WORKERS="$workers",BATCH_SIZE="$BATCH_SIZE",DATASET_PATH="$DATASET_PATH" \
      "$JOB_SCRIPT"
  )"
  job_ids+=("$job_id")
  echo "submitted problem2f workers=${workers}, cpus=${cpus}, job_id=${job_id}"
done

echo
echo "After all problem2f jobs finish, run this on the login node:"
echo "bash ${COLLECT_SCRIPT}"
