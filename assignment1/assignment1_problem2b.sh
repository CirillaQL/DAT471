#!/bin/bash

#SBATCH --job-name=assignment1_p2b
#SBATCH --output=assignment1_problem2b.out
#SBATCH --error=assignment1_problem2b.err
#SBATCH --time=00:05:00

set -euo pipefail

CONTAINER="/data/courses/2026_dat471_dit066/containers/assignment1.sif"
DATASET="/data/courses/2026_dat471_dit066/datasets/bike_sharing_hourly.csv"

apptainer exec \
  --bind /data:/data \
  "$CONTAINER" \
  bash -c "
    echo '=== Running mystery.py ==='
    python3 /opt/mystery.py \"$DATASET\"

    echo
    echo '=== mystery.py source ==='
    cat /opt/mystery.py
  "