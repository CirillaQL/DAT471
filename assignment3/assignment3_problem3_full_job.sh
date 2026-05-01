#!/bin/bash

#SBATCH --job-name=assignment3_p3_full
#SBATCH --output=assignment3_problem3_full.out
#SBATCH --error=assignment3_problem3_full.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=32

set -euo pipefail

CONTAINER="/data/courses/2026_dat471_dit066/containers/assignment3.sif"
SCRIPT_DIR="${ASSIGNMENT3_DIR:-/data/users/luqia/DAT471/assignment3}"
SCRIPT_PATH="${SCRIPT_DIR}/mrjob_twitter_measure.py"
DATASET_PATH="${DATASET_PATH:-/data/courses/2026_dat471_dit066/twitter/twitter-2010_full.txt}"
WORKERS="${SLURM_CPUS_PER_TASK:-32}"

apptainer exec \
  --bind /data:/data \
  --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
  "$CONTAINER" \
  python3 "$SCRIPT_PATH" --num-workers "$WORKERS" "$DATASET_PATH"
