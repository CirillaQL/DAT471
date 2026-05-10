#!/bin/bash

#SBATCH --job-name=assignment4_p2d_full
#SBATCH --output=assignment4_problem2d_full.out
#SBATCH --error=assignment4_problem2d_full.err
#SBATCH --time=00:30:00

set -euo pipefail

SCRIPT_DIR="${ASSIGNMENT4_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
SELF_PATH="${SCRIPT_DIR}/assignment4_problem2d_full.sh"
SCRIPT_PATH="${SCRIPT_DIR}/assignment4_problem2.py"
RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/assignment4_problem2d_full_results}"
DATASET_PATH="${DATASET_PATH:-/data/courses/2026_dat471_dit066/datasets/climate/climate_full.csv}"
CONTAINER="${CONTAINER:-/data/courses/2026_dat471_dit066/containers/assignment4.sif}"
WORKERS="${WORKERS:-64}"

if [[ "${1:-}" != "--run" ]]; then
  mkdir -p "$RESULT_DIR"
  job_id="$(
    sbatch \
      --parsable \
      --job-name="a4_p2d_full_w${WORKERS}" \
      --cpus-per-task="$WORKERS" \
      --mem=64G \
      --output="${RESULT_DIR}/full_workers_${WORKERS}.slurm.out" \
      --error="${RESULT_DIR}/full_workers_${WORKERS}.slurm.err" \
      --export=ALL,ASSIGNMENT4_DIR="$SCRIPT_DIR",RESULT_DIR="$RESULT_DIR",DATASET_PATH="$DATASET_PATH",CONTAINER="$CONTAINER",WORKERS="$WORKERS" \
      "$SELF_PATH" --run
  )"
  echo "submitted problem2d full dataset workers=${WORKERS}, cpus=${WORKERS}, job_id=${job_id}"
  echo "results directory: ${RESULT_DIR}"
  exit 0
fi

mkdir -p "$RESULT_DIR"
result_file="${RESULT_DIR}/full_workers_${WORKERS}.txt"

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

summary_file="${RESULT_DIR}/problem2d_full_summary.txt"
{
  echo "===== full dataset, workers=${WORKERS} ====="
  grep -E '^(workers=|cpus_per_task=|dataset=|Top 5 coefficients table:|Top 5 decade temperature differences table:|BETA five-number summary table:|Decade temperature difference five-number summary table:|\\||Fraction of positive coefficients:|Fraction of positive differences:|num workers:|records:|read time:|compute time:|read fraction:|compute fraction:|total time:)' "$result_file" || true
} > "$summary_file"

echo "Wrote full result to ${result_file}"
echo "Wrote summary to ${summary_file}"
