#!/bin/bash

set -euo pipefail

SCRIPT_DIR="${ASSIGNMENT3_DIR:-/data/users/luqia/DAT471/assignment3}"
JOB_SCRIPT="${SCRIPT_DIR}/assignment3_problem3_job.sh"
COLLECT_SCRIPT="${SCRIPT_DIR}/assignment3_problem3_collect_results.sh"
RESULT_DIR="${SCRIPT_DIR}/assignment3_problem3_results_parallel"
DATASET_PATH="${DATASET_PATH:-/data/courses/2026_dat471_dit066/datasets/twitter/twitter_10M.txt}"
WORKERS=(1 2 4 8 16 32)

mkdir -p "$RESULT_DIR"

for workers in "${WORKERS[@]}"; do
  job_id="$(
    sbatch \
      --parsable \
      --job-name="a3_p3_w${workers}" \
      --cpus-per-task="$workers" \
      --output="${RESULT_DIR}/workers_${workers}.slurm.out" \
      --error="${RESULT_DIR}/workers_${workers}.slurm.err" \
      --export=ALL,ASSIGNMENT3_DIR="$SCRIPT_DIR",WORKERS="$workers",DATASET_PATH="$DATASET_PATH" \
      "$JOB_SCRIPT"
  )"
  echo "submitted problem3 workers=${workers}, cpus=${workers}, job_id=${job_id}"
done

echo
echo "After all problem3 jobs finish, run this on the login node:"
echo "bash ${COLLECT_SCRIPT}"
