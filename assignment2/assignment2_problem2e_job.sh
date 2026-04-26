#!/bin/bash

#SBATCH --time=00:30:00

set -euo pipefail

CONTAINER="/data/courses/2026_dat471_dit066/containers/assignment2.sif"
SCRIPT_DIR="${ASSIGNMENT2_DIR:?ASSIGNMENT2_DIR is not set}"
SCRIPT_PATH="${SCRIPT_DIR}/assignment2_problem2e.py"
DATASET_PATH="${DATASET_PATH:?DATASET_PATH is not set}"
RESULT_DIR="${SCRIPT_DIR}/assignment2_problem2e_results_parallel"
BATCH_SIZE="${BATCH_SIZE:?BATCH_SIZE is not set}"
WORKERS="${WORKERS:?WORKERS is not set}"

mkdir -p "$RESULT_DIR"
result_file="${RESULT_DIR}/workers_${WORKERS}.txt"

{
  echo "workers=${WORKERS}"
  echo "cpus_per_task=${SLURM_CPUS_PER_TASK:-unknown}"
  echo "batch_size=${BATCH_SIZE}"
  echo "dataset=${DATASET_PATH}"
  echo "started_at=$(date --iso-8601=seconds)"
  echo

  apptainer exec \
    --bind /data:/data \
    --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
    "$CONTAINER" \
    python3 "$SCRIPT_PATH" --num-workers "$WORKERS" --batch-size "$BATCH_SIZE" "$DATASET_PATH"

  echo
  echo "finished_at=$(date --iso-8601=seconds)"
} 2>&1 | tee "$result_file"
