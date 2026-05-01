#!/bin/bash

#SBATCH --time=00:30:00

set -euo pipefail

CONTAINER="/data/courses/2026_dat471_dit066/containers/assignment3.sif"
SCRIPT_DIR="${ASSIGNMENT3_DIR:-/data/users/luqia/DAT471/assignment3}"
SCRIPT_PATH="${SCRIPT_DIR}/mrjob_twitter_followers_measure.py"
DATASET_PATH="${DATASET_PATH:-/data/courses/2026_dat471_dit066/datasets/twitter/twitter-2010_10M.txt}"
RESULT_DIR="${SCRIPT_DIR}/assignment3_problem4_results_parallel"
WORKERS="${WORKERS:?WORKERS is not set}"

mkdir -p "$RESULT_DIR"
result_file="${RESULT_DIR}/workers_${WORKERS}.txt"

{
  echo "workers=${WORKERS}"
  echo "cpus_per_task=${SLURM_CPUS_PER_TASK:-unknown}"
  echo "dataset=${DATASET_PATH}"
  echo "started_at=$(date --iso-8601=seconds)"
  echo

  apptainer exec \
    --bind /data:/data \
    --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
    "$CONTAINER" \
    python3 "$SCRIPT_PATH" --num-workers "$WORKERS" "$DATASET_PATH"

  echo
  echo "finished_at=$(date --iso-8601=seconds)"
} 2>&1 | tee "$result_file"
