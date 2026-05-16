#!/bin/bash

#SBATCH --job-name=assignment5_p3
#SBATCH --output=assignment5_problem3.out
#SBATCH --error=assignment5_problem3.err
#SBATCH --time=12:00:00

set -euo pipefail

SCRIPT_DIR="${ASSIGNMENT5_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}}"
SCRIPT_PATH="${SCRIPT_DIR}/problem3.py"
RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/assignment5_problem3_results}"
CONTAINER="${CONTAINER:-/data/courses/2026_dat471_dit066/containers/assignment4.sif}"
GUTENBERG_DIR="${GUTENBERG_DIR:-/data/courses/2026_dat471_dit066/datasets/gutenberg}"

TINY_PATH="${TINY_PATH:-${GUTENBERG_DIR}/tiny}"
SMALL_PATH="${SMALL_PATH:-${GUTENBERG_DIR}/small}"
MEDIUM_PATH="${MEDIUM_PATH:-${GUTENBERG_DIR}/medium}"
BIG_PATH="${BIG_PATH:-${GUTENBERG_DIR}/big}"
HUGE_PATH="${HUGE_PATH:-${GUTENBERG_DIR}/huge}"

RUN_TABLE3="${RUN_TABLE3:-1}"
RUN_HUGE="${RUN_HUGE:-1}"
RUN_SCALING="${RUN_SCALING:-0}"
RUN_SEEDS="${RUN_SEEDS:-0}"

DEFAULT_WORKERS="${DEFAULT_WORKERS:-4}"
HUGE_WORKERS="${HUGE_WORKERS:-16}"
SCALING_M="${SCALING_M:-1024}"
SCALING_SEED="${SCALING_SEED:-0x9747b28c}"
SCALING_WORKERS="${SCALING_WORKERS:-1 2 4 8 16 32 64}"
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
  echo "script=${SCRIPT_PATH}"
  echo "gutenberg_dir=${GUTENBERG_DIR}"
  echo "result_dir=${RESULT_DIR}"
  echo "started_at=$(date --iso-8601=seconds)"
  echo

  if [[ "$RUN_TABLE3" == "1" ]]; then
    table3_csv="${RESULT_DIR}/problem3_table3_estimates.csv"
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
  fi

  if [[ "$RUN_HUGE" == "1" ]]; then
    huge_output="${RESULT_DIR}/problem3_huge_m1024_seed9747b28c.txt"
    echo "running huge estimate seed=0x9747b28c m=1024 workers=${HUGE_WORKERS}"
    run_hll "$HUGE_PATH" 0x9747b28c 1024 "$HUGE_WORKERS" "$huge_output"
    echo "wrote huge estimate: ${huge_output}"
    echo
  fi

  if [[ "$RUN_SCALING" == "1" ]]; then
    scaling_csv="${RESULT_DIR}/problem3_scaling_big.csv"
    echo "workers,seconds,estimate,output_file" > "$scaling_csv"

    for workers in $SCALING_WORKERS; do
      output_file="${RESULT_DIR}/scaling_big_w${workers}.txt"
      echo "running scaling dataset=big workers=${workers} seed=${SCALING_SEED} m=${SCALING_M}"
      run_hll "$BIG_PATH" "$SCALING_SEED" "$SCALING_M" "$workers" "$output_file"
      estimate="$(extract_estimate "$output_file")"
      seconds="$(awk '/Took / {print $2}' "$output_file" | tail -n 1)"
      echo "${workers},${seconds},${estimate},${output_file}" >> "$scaling_csv"
      echo
    done

    apptainer exec \
      --bind /data:/data \
      --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
      "$CONTAINER" \
      python3 - "$scaling_csv" "$RESULT_DIR" <<'PY'
import csv
import pathlib
import sys

csv_path = pathlib.Path(sys.argv[1])
result_dir = pathlib.Path(sys.argv[2])

rows = []
with csv_path.open("r", encoding="utf-8", newline="") as infile:
    for row in csv.DictReader(infile):
        row["workers"] = int(row["workers"])
        row["seconds"] = float(row["seconds"])
        rows.append(row)

if not rows:
    raise SystemExit("no scaling rows collected")

baseline = next((row["seconds"] for row in rows if row["workers"] == 1), rows[0]["seconds"])
speedup_csv = result_dir / "problem3_scaling_speedup.csv"
with speedup_csv.open("w", encoding="utf-8", newline="") as outfile:
    writer = csv.writer(outfile)
    writer.writerow(["workers", "seconds", "speedup", "estimate"])
    for row in rows:
        writer.writerow([row["workers"], row["seconds"], baseline / row["seconds"], row["estimate"]])

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figure_path = result_dir / "problem3_scaling_speedup.png"
    plt.figure(figsize=(8, 5))
    plt.plot([row["workers"] for row in rows],
             [baseline / row["seconds"] for row in rows],
             marker="o")
    plt.xlabel("Workers")
    plt.ylabel("Speedup")
    plt.title("Problem 3 scalability on the big dataset")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(figure_path, dpi=200)
    plt.close()
    print(f"wrote scaling plot: {figure_path}")
except Exception as exc:
    print(f"warning: could not write scaling plot with matplotlib: {exc}", file=sys.stderr)

print(f"wrote speedup csv: {speedup_csv}")
PY

    echo
  fi

  if [[ "$RUN_SEEDS" == "1" ]]; then
    seeds_csv="${RESULT_DIR}/problem3_small_${SEED_COUNT}_seeds.csv"
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

summary_path = result_dir / "problem3_seed_summary.txt"
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

    figure_path = result_dir / "problem3_small_seed_histogram.png"
    plt.figure(figsize=(9, 5))
    plt.hist(estimates, bins=40, edgecolor="black", linewidth=0.3)
    plt.axvline(true_n, color="red", linestyle="--", label="true n")
    plt.xlabel("Cardinality estimate")
    plt.ylabel("Frequency")
    plt.title(f"Small dataset estimates for {len(estimates)} seeds, m={m}")
    plt.legend()
    plt.tight_layout()
    plt.savefig(figure_path, dpi=200)
    plt.close()
    print(f"wrote seed histogram: {figure_path}")
except Exception as exc:
    print(f"warning: could not write seed histogram with matplotlib: {exc}", file=sys.stderr)

print(f"wrote seed summary: {summary_path}")
PY

    echo
  fi

  echo "finished_at=$(date --iso-8601=seconds)"
} 2>&1 | tee "${RESULT_DIR}/problem3_run.txt"
