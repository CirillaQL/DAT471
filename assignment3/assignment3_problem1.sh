#!/bin/bash

#SBATCH --job-name=assignment3_p1
#SBATCH --output=assignment3_problem1.out
#SBATCH --error=assignment3_problem1.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=8

set -euo pipefail

CONTAINER="/data/courses/2026_dat471_dit066/containers/assignment3.sif"
DATASET_PATH="/data/courses/2026_dat471_dit066/datasets/sc2/planets.csv"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="${SCRIPT_DIR}/assignment3_problem1.py"
NUM_CORES="${SLURM_CPUS_PER_TASK:-1}"

apptainer exec \
  --bind /data:/data \
  --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
  "$CONTAINER" \
  python3 "$SCRIPT_PATH" -r local --num-cores "$NUM_CORES" "$DATASET_PATH"
