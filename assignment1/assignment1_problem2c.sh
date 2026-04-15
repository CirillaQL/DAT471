#!/bin/bash

#SBATCH --job-name=assignment1_p2c
#SBATCH --output=assignment1_problem2c.out
#SBATCH --error=assignment1_problem2c.err
#SBATCH --time=00:05:00

set -euo pipefail

CONTAINER="/data/courses/2026_dat471_dit066/containers/assignment1.sif"
SCRIPT_PATH="${SLURM_SUBMIT_DIR}/assignment1_problem2c.py"

apptainer exec \
  --bind /data:/data \
  --bind "${SLURM_SUBMIT_DIR}:${SLURM_SUBMIT_DIR}" \
  "$CONTAINER" \
  python3 "$SCRIPT_PATH"

