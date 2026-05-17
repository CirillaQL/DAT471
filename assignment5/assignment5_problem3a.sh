#!/bin/bash

#SBATCH --job-name=assignment5_p3a
#SBATCH --output=assignment5_problem3a.out
#SBATCH --error=assignment5_problem3a.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=16

set -euo pipefail

SCRIPT_DIR="${ASSIGNMENT5_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}}"
SCRIPT_PATH="${SCRIPT_DIR}/problem3.py"
RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/assignment5_problem3a_results}"
CONTAINER="${CONTAINER:-/data/courses/2026_dat471_dit066/containers/assignment4.sif}"
GUTENBERG_DIR="${GUTENBERG_DIR:-/data/courses/2026_dat471_dit066/datasets/gutenberg}"

TINY_PATH="${TINY_PATH:-${GUTENBERG_DIR}/tiny}"
SMALL_PATH="${SMALL_PATH:-${GUTENBERG_DIR}/small}"
MEDIUM_PATH="${MEDIUM_PATH:-${GUTENBERG_DIR}/medium}"
HUGE_PATH="${HUGE_PATH:-${GUTENBERG_DIR}/huge}"

DEFAULT_WORKERS="${DEFAULT_WORKERS:-4}"
HUGE_WORKERS="${HUGE_WORKERS:-16}"

mkdir -p "$RESULT_DIR"

run_hll() {
  local dataset_path="$1"
  local seed="$2"
  local registers="$3"
  local workers="$4"
  local output_file="$5"

  apptainer exec \
    --bind /data:/data \
    --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
    "$CONTAINER" \
    python3 "$SCRIPT_PATH" "$dataset_path" -s "$seed" -m "$registers" -w "$workers" \
    2>&1 | tee "$output_file"
}

extract_estimate() {
  awk -F': ' '/Cardinality estimate/ {print $2}' "$1" | tail -n 1
}

{
  echo "Problem 3(a): table 3 checks and huge dataset estimate"
  echo "script=${SCRIPT_PATH}"
  echo "gutenberg_dir=${GUTENBERG_DIR}"
  echo "result_dir=${RESULT_DIR}"
  echo "started_at=$(date --iso-8601=seconds)"
  echo

  table3_csv="${RESULT_DIR}/problem3a_table3_estimates.csv"
  echo "dataset,seed,m,true_n,estimate,output_file" > "$table3_csv"

  while IFS=, read -r name path true_n; do
    for seed in 0x9747b28c 0xc40376f3; do
      for registers in 16 256 32768; do
        safe_name="$(printf '%s' "$name" | tr '[:upper:]' '[:lower:]')"
        output_file="${RESULT_DIR}/table3_${safe_name}_${seed}_m${registers}.txt"
        echo "running table3 dataset=${name} seed=${seed} m=${registers}"
        run_hll "$path" "$seed" "$registers" "$DEFAULT_WORKERS" "$output_file"
        estimate="$(extract_estimate "$output_file")"
        echo "${name},${seed},${registers},${true_n},${estimate},${output_file}" >> "$table3_csv"
        echo
      done
    done
  done <<EOF
Tiny,${TINY_PATH},47442
Small,${SMALL_PATH},284689
Medium,${MEDIUM_PATH},1730194
EOF

  echo "wrote table 3 estimates: ${table3_csv}"
  echo

  huge_output="${RESULT_DIR}/problem3a_huge_m1024_seed9747b28c.txt"
  echo "running huge estimate seed=0x9747b28c m=1024 workers=${HUGE_WORKERS}"
  run_hll "$HUGE_PATH" 0x9747b28c 1024 "$HUGE_WORKERS" "$huge_output"
  echo "wrote huge estimate: ${huge_output}"
  echo

  echo "finished_at=$(date --iso-8601=seconds)"
} 2>&1 | tee "${RESULT_DIR}/problem3a_run.txt"
