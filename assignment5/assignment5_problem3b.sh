#!/bin/bash

#SBATCH --job-name=assignment5_p3b
#SBATCH --output=assignment5_problem3b.out
#SBATCH --error=assignment5_problem3b.err
#SBATCH --time=00:30:00

set -euo pipefail

SCRIPT_DIR="${ASSIGNMENT5_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
SELF_PATH="${SCRIPT_DIR}/assignment5_problem3b.sh"
SCRIPT_PATH="${SCRIPT_DIR}/problem3.py"
RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/assignment5_problem3b_results}"
CONTAINER="${CONTAINER:-/data/courses/2026_dat471_dit066/containers/assignment4.sif}"
GUTENBERG_DIR="${GUTENBERG_DIR:-/data/courses/2026_dat471_dit066/datasets/gutenberg}"
BIG_PATH="${BIG_PATH:-${GUTENBERG_DIR}/big}"

SCALING_M="${SCALING_M:-1024}"
SCALING_SEED="${SCALING_SEED:-0x9747b28c}"
WORKERS=(1 2 4 8 16 32 64)

run_one() {
  local workers="${1:?workers is required}"
  local output_file="${RESULT_DIR}/workers_${workers}.txt"

  mkdir -p "$RESULT_DIR"

  {
    echo "Problem 3(b): one scalability run"
    echo "workers=${workers}"
    echo "cpus_per_task=${SLURM_CPUS_PER_TASK:-unknown}"
    echo "script=${SCRIPT_PATH}"
    echo "big_path=${BIG_PATH}"
    echo "m=${SCALING_M}"
    echo "seed=${SCALING_SEED}"
    echo "started_at=$(date --iso-8601=seconds)"
    echo

    apptainer exec \
      --bind /data:/data \
      --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
      "$CONTAINER" \
      python3 "$SCRIPT_PATH" "$BIG_PATH" -s "$SCALING_SEED" -m "$SCALING_M" -w "$workers"

    echo
    echo "finished_at=$(date --iso-8601=seconds)"
  } 2>&1 | tee "$output_file"
}

collect_results() {
  local scaling_csv="${RESULT_DIR}/problem3b_scaling_big.csv"

  mkdir -p "$RESULT_DIR"
  export MPLCONFIGDIR="${RESULT_DIR}/.matplotlib"
  mkdir -p "$MPLCONFIGDIR"

  echo "workers,seconds,estimate,output_file" > "$scaling_csv"

  for workers in "${WORKERS[@]}"; do
    local output_file="${RESULT_DIR}/workers_${workers}.txt"
    local estimate=""
    local seconds=""

    if [[ -f "$output_file" ]]; then
      estimate="$(awk -F': ' '/Cardinality estimate/ {print $2}' "$output_file" | tail -n 1)"
      seconds="$(awk '/Took / {print $2}' "$output_file" | tail -n 1)"
    fi

    if [[ -n "$estimate" && -n "$seconds" ]]; then
      echo "${workers},${seconds},${estimate},${output_file}" >> "$scaling_csv"
    else
      echo "missing complete result for workers=${workers}: ${output_file}" >&2
    fi
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
speedup_csv = result_dir / "problem3b_scaling_speedup.csv"
with speedup_csv.open("w", encoding="utf-8", newline="") as outfile:
    writer = csv.writer(outfile)
    writer.writerow(["workers", "seconds", "speedup", "estimate"])
    for row in rows:
        writer.writerow([row["workers"], row["seconds"], baseline / row["seconds"], row["estimate"]])

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figure_path = result_dir / "problem3b_scaling_speedup.png"
    plt.figure(figsize=(8, 5))
    plt.plot([row["workers"] for row in rows],
             [baseline / row["seconds"] for row in rows],
             marker="o")
    plt.xlabel("Workers")
    plt.ylabel("Speedup")
    plt.title("Problem 3(b) speedup on the big dataset")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(figure_path, dpi=200)
    plt.close()
    print(f"wrote scaling plot: {figure_path}")
except Exception as exc:
    print(f"warning: could not write scaling plot with matplotlib: {exc}", file=sys.stderr)

print(f"wrote raw scaling csv: {csv_path}")
print(f"wrote speedup csv: {speedup_csv}")
PY
}

submit_jobs() {
  mkdir -p "$RESULT_DIR"

  local job_ids=()
  for workers in "${WORKERS[@]}"; do
    local job_id
    job_id="$(
      sbatch \
        --parsable \
        --job-name="a5_p3b_w${workers}" \
        --time=00:30:00 \
        --cpus-per-task="$workers" \
        --output="${RESULT_DIR}/workers_${workers}.slurm.out" \
        --error="${RESULT_DIR}/workers_${workers}.slurm.err" \
        --export=ALL,ASSIGNMENT5_DIR="$SCRIPT_DIR",RESULT_DIR="$RESULT_DIR",CONTAINER="$CONTAINER",BIG_PATH="$BIG_PATH",SCALING_M="$SCALING_M",SCALING_SEED="$SCALING_SEED" \
        "$SELF_PATH" --run-one "$workers"
    )"
    job_ids+=("$job_id")
    echo "submitted problem3b workers=${workers}, cpus=${workers}, job_id=${job_id}"
  done

  local dependency
  dependency="$(IFS=:; echo "${job_ids[*]}")"

  local collect_job_id
  collect_job_id="$(
    sbatch \
      --parsable \
      --job-name="a5_p3b_collect" \
      --time=00:30:00 \
      --dependency="afterok:${dependency}" \
      --output="${RESULT_DIR}/collect.slurm.out" \
      --error="${RESULT_DIR}/collect.slurm.err" \
      --export=ALL,ASSIGNMENT5_DIR="$SCRIPT_DIR",RESULT_DIR="$RESULT_DIR",CONTAINER="$CONTAINER" \
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
