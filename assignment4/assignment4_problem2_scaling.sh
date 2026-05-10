#!/bin/bash

#SBATCH --job-name=assignment4_p2
#SBATCH --time=00:30:00

set -euo pipefail

SCRIPT_DIR="${ASSIGNMENT4_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
SELF_PATH="${SCRIPT_DIR}/assignment4_problem2_scaling.sh"
SCRIPT_PATH="${SCRIPT_DIR}/assignment4_problem2.py"
RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/assignment4_problem2_results}"
DATASET_PATH="${DATASET_PATH:-/data/courses/2026_dat471_dit066/datasets/climate/climate_large.csv}"
CONTAINER="${CONTAINER:-/data/courses/2026_dat471_dit066/containers/assignment4.sif}"

WORKERS=(1 2 4 8 16 32 64)

run_one() {
  local workers="${1:?workers is required}"
  local result_file="${RESULT_DIR}/workers_${workers}.txt"

  mkdir -p "$RESULT_DIR"

  {
    echo "workers=${workers}"
    echo "cpus_per_task=${SLURM_CPUS_PER_TASK:-unknown}"
    echo "dataset=${DATASET_PATH}"
    echo "started_at=$(date --iso-8601=seconds)"
    echo

    apptainer exec \
      --bind /data:/data \
      --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
      "$CONTAINER" \
      python3 "$SCRIPT_PATH" --num-workers "$workers" "$DATASET_PATH"

    echo
    echo "finished_at=$(date --iso-8601=seconds)"
  } 2>&1 | tee "$result_file"
}

collect_results() {
  local summary_file="${RESULT_DIR}/problem2_summary.txt"
  local csv_file="${RESULT_DIR}/speedup.csv"
  local baseline_time=""

  mkdir -p "$RESULT_DIR"
  : > "$summary_file"
  echo "workers,total_time,speedup" > "$csv_file"

  for workers in "${WORKERS[@]}"; do
    local result_file="${RESULT_DIR}/workers_${workers}.txt"
    local total_time=""

    {
      echo "===== workers=${workers} ====="
      if [[ -f "$result_file" ]]; then
        grep -E '^(workers=|cpus_per_task=|dataset=|Top 5 coefficients:|Fraction of positive coefficients:|Five-number summary of BETA values:|beta_|Top 5 differences:|Fraction of positive differences:|Five-number summary of decade average difference values:|tdiff_|num workers:|total time:)' "$result_file" || true
        total_time="$(awk '/^total time:/ {print $3}' "$result_file" | tail -n 1)"
      else
        echo "missing result file: $result_file"
      fi

      if [[ -n "$total_time" ]]; then
        if [[ -z "$baseline_time" && "$workers" -eq 1 ]]; then
          baseline_time="$total_time"
          echo "single-worker runtime: ${baseline_time} seconds"
        fi
        if [[ -n "$baseline_time" ]]; then
          local speedup
          speedup="$(awk -v baseline="$baseline_time" -v total="$total_time" 'BEGIN { printf "%.6f", baseline / total }')"
          echo "speedup compared with 1 worker: ${speedup}"
          echo "${workers},${total_time},${speedup}" >> "$csv_file"
        fi
      fi
      echo
    } >> "$summary_file"
  done

  echo "Wrote summary to ${summary_file}"
  echo "Wrote speedup CSV to ${csv_file}"
}

submit_jobs() {
  mkdir -p "$RESULT_DIR"

  local job_ids=()
  for workers in "${WORKERS[@]}"; do
    local job_id
    job_id="$(
      sbatch \
        --parsable \
        --job-name="a4_p2_w${workers}" \
        --cpus-per-task="$workers" \
        --output="${RESULT_DIR}/workers_${workers}.slurm.out" \
        --error="${RESULT_DIR}/workers_${workers}.slurm.err" \
        --export=ALL,ASSIGNMENT4_DIR="$SCRIPT_DIR",RESULT_DIR="$RESULT_DIR",DATASET_PATH="$DATASET_PATH",CONTAINER="$CONTAINER" \
        "$SELF_PATH" --run-one "$workers"
    )"
    job_ids+=("$job_id")
    echo "submitted problem2 workers=${workers}, cpus=${workers}, job_id=${job_id}"
  done

  local dependency
  dependency="$(IFS=:; echo "${job_ids[*]}")"
  local collect_job_id
  collect_job_id="$(
    sbatch \
      --parsable \
      --job-name="a4_p2_collect" \
      --dependency="afterok:${dependency}" \
      --output="${RESULT_DIR}/collect.slurm.out" \
      --error="${RESULT_DIR}/collect.slurm.err" \
      --export=ALL,ASSIGNMENT4_DIR="$SCRIPT_DIR",RESULT_DIR="$RESULT_DIR" \
      "$SELF_PATH" --collect
  )"

  echo
  echo "submitted collection job_id=${collect_job_id}"
  echo "results directory: ${RESULT_DIR}"
}

case "${1:-}" in
  --run-one)
    run_one "${2:?missing worker count}"
    ;;
  --collect)
    collect_results
    ;;
  "")
    submit_jobs
    ;;
  *)
    echo "Usage: $0 [--run-one WORKERS|--collect]" >&2
    exit 2
    ;;
esac
