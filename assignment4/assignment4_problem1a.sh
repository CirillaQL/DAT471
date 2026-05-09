#!/bin/bash

#SBATCH --job-name=assignment4_p1a
#SBATCH --output=assignment4_problem1a.out
#SBATCH --error=assignment4_problem1a`.err
#SBATCH --time=00:30:00

set -euo pipefail

CONTAINER="/data/courses/2026_dat471_dit066/containers/assignment4.sif"
SCRIPT_PATH="${SLURM_SUBMIT_DIR}/assignment4_problem1a.py"

apptainer exec \
  --bind /data:/data \
  --bind "${SLURM_SUBMIT_DIR}:${SLURM_SUBMIT_DIR}" \
  "$CONTAINER" \
  python3 "$SCRIPT_PATH" /data/courses/2026_dat471_dit066/datasets/twitter/twitter-2010_10M.txt