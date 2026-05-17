#!/bin/bash

#SBATCH --job-name=assignment5_p3c
#SBATCH --output=assignment5_problem3c.out
#SBATCH --error=assignment5_problem3c.err
#SBATCH --time=00:30:00

set -euo pipefail

SCRIPT_DIR="${ASSIGNMENT5_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}}"
SCRIPT_PATH="${SCRIPT_DIR}/problem3.py"
RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/assignment5_problem3c_results}"
CONTAINER="${CONTAINER:-/data/courses/2026_dat471_dit066/containers/assignment4.sif}"
GUTENBERG_DIR="${GUTENBERG_DIR:-/data/courses/2026_dat471_dit066/datasets/gutenberg}"
SMALL_PATH="${SMALL_PATH:-${GUTENBERG_DIR}/small}"

SEED_COUNT="${SEED_COUNT:-1000}"
SEED_M="${SEED_M:-1024}"
SEED_WORKERS="${SEED_WORKERS:-4}"
SEED_BASE="${SEED_BASE:-0x9747b28c}"
SMALL_TRUE_N="${SMALL_TRUE_N:-284689}"

mkdir -p "$RESULT_DIR"
export MPLCONFIGDIR="${RESULT_DIR}/.matplotlib"
mkdir -p "$MPLCONFIGDIR"

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
  echo "Problem 3(c): 1000 seeds on the small dataset"
  echo "script=${SCRIPT_PATH}"
  echo "small_path=${SMALL_PATH}"
  echo "result_dir=${RESULT_DIR}"
  echo "seed_count=${SEED_COUNT}"
  echo "m=${SEED_M}"
  echo "workers=${SEED_WORKERS}"
  echo "started_at=$(date --iso-8601=seconds)"
  echo

  seeds_csv="${RESULT_DIR}/problem3c_small_${SEED_COUNT}_seeds.csv"
  echo "index,seed,estimate,output_file" > "$seeds_csv"

  for index in $(seq 0 $((SEED_COUNT - 1))); do
    seed="$(printf '0x%08x' $(( (SEED_BASE + index) & 0xffffffff )))"
    output_file="${RESULT_DIR}/small_seed_${index}.txt"
    echo "running seed sample $((index + 1))/${SEED_COUNT}: seed=${seed}"
    run_hll "$SMALL_PATH" "$seed" "$SEED_M" "$SEED_WORKERS" "$output_file"
    estimate="$(extract_estimate "$output_file")"
    echo "${index},${seed},${estimate},${output_file}" >> "$seeds_csv"
    echo
  done

  apptainer exec \
    --bind /data:/data \
    --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
    "$CONTAINER" \
    python3 - "$seeds_csv" "$RESULT_DIR" "$SMALL_TRUE_N" "$SEED_M" <<'PY'
import csv
import math
import pathlib
import statistics
import sys

csv_path = pathlib.Path(sys.argv[1])
result_dir = pathlib.Path(sys.argv[2])
true_n = int(sys.argv[3])
m = int(sys.argv[4])

estimates = []
with csv_path.open("r", encoding="utf-8", newline="") as infile:
    for row in csv.DictReader(infile):
        estimates.append(float(row["estimate"]))

if not estimates:
    raise SystemExit("no seed estimates collected")

mean = statistics.fmean(estimates)
stddev = statistics.pstdev(estimates)
relative_sigma = 1.04 / math.sqrt(m)

summary_path = result_dir / "problem3c_seed_summary.txt"
with summary_path.open("w", encoding="utf-8") as outfile:
    outfile.write(f"correct distinct elements: {true_n}\n")
    outfile.write(f"number of estimates: {len(estimates)}\n")
    outfile.write(f"average estimate: {mean:.10f}\n")
    outfile.write(f"standard deviation of estimates: {stddev:.10f}\n")
    outfile.write(f"theoretical relative sigma: {relative_sigma:.10f}\n")
    for k in (1, 2, 3):
        low = true_n * (1 - k * relative_sigma)
        high = true_n * (1 + k * relative_sigma)
        fraction = sum(low <= estimate <= high for estimate in estimates) / len(estimates)
        outfile.write(f"fraction within n(1 +/- {k} sigma): {fraction:.10f}\n")

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figure_path = result_dir / "problem3c_small_seed_histogram.png"
    plt.figure(figsize=(9, 5))
    plt.hist(estimates, bins=40, edgecolor="black", linewidth=0.3)
    plt.axvline(true_n, color="red", linestyle="--", label="true n")
    plt.xlabel("Cardinality estimate")
    plt.ylabel("Frequency")
    plt.title(f"Problem 3(c) small dataset estimates for {len(estimates)} seeds, m={m}")
    plt.legend()
    plt.tight_layout()
    plt.savefig(figure_path, dpi=200)
    plt.close()
    print(f"wrote seed histogram: {figure_path}")
except Exception as exc:
    print(f"warning: could not write seed histogram with matplotlib: {exc}", file=sys.stderr)

print(f"wrote seed csv: {csv_path}")
print(f"wrote seed summary: {summary_path}")
PY

  echo
  echo "finished_at=$(date --iso-8601=seconds)"
} 2>&1 | tee "${RESULT_DIR}/problem3c_run.txt"
