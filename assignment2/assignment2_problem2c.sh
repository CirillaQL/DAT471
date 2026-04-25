#!/bin/bash

#SBATCH --job-name=assignment2_p2c
#SBATCH --output=assignment2_problem2c.out
#SBATCH --error=assignment2_problem2c.err
#SBATCH --time=00:30:00

set -euo pipefail

CONTAINER="/data/courses/2026_dat471_dit066/containers/assignment2.sif"
SCRIPT_PATH="${SLURM_SUBMIT_DIR}/assignment2_problem2c.py"

apptainer exec \
  --bind /data:/data \
  --bind "${SLURM_SUBMIT_DIR}:${SLURM_SUBMIT_DIR}" \
  "$CONTAINER" \
  python3 "$SCRIPT_PATH" /data/courses/2026_dat471_dit066/datasets/gutenberg 