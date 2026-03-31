#!/bin/bash

#SBATCH --job-name=assignment1_p2b
#SBATCH --output=assignment1_problem2b.out
#SBATCH --error=assignment1_problem2b.err
#SBATCH --time=00:05:00

set -euo pipefail

JOB_DIR="${SLURM_SUBMIT_DIR:-$(pwd)}"
cd "$JOB_DIR"

export JOB_DIR

apptainer exec --bind "$JOB_DIR:$JOB_DIR" --bind /data:/data "$JOB_DIR/assignment1.sif" bash -s <<'EOF'
set -euo pipefail

dataset="/data/courses/2026_dat471_dit066/datasets/bike_sharing_hourly.csv"

if [[ ! -f "$dataset" ]]; then
  echo "Could not find the bike sharing dataset: $dataset"
  exit 1
fi

echo "Using dataset: $dataset"
echo
echo "Output from /opt/mystery.py:"
python3 /opt/mystery.py "$dataset"
echo
echo "Contents of /opt/mystery.py:"
cat /opt/mystery.py
EOF
