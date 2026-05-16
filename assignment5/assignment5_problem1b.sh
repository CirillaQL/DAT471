#!/bin/bash

#SBATCH --job-name=assignment5_p1b
#SBATCH --output=assignment5_problem1b.out
#SBATCH --error=assignment5_problem1b.err
#SBATCH --time=00:30:00

set -euo pipefail

SCRIPT_DIR="${ASSIGNMENT5_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
SCRIPT_PATH="${SCRIPT_DIR}/problem1.py"
RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/assignment5_problem1b_results}"
DATASET_PATH="${DATASET_PATH:-/data/courses/2026_dat471_dit066/datasets/words}"
CONTAINER="${CONTAINER:-/data/courses/2026_dat471_dit066/containers/assignment4.sif}"
SEED="${SEED:-0xee418b6c}"
M="${M:-128}"

mkdir -p "$RESULT_DIR"
export MPLCONFIGDIR="${RESULT_DIR}/.matplotlib"
mkdir -p "$MPLCONFIGDIR"

{
  echo "dataset=${DATASET_PATH}"
  echo "script=${SCRIPT_PATH}"
  echo "seed=${SEED}"
  echo "m=${M}"
  echo "started_at=$(date --iso-8601=seconds)"
  echo

  apptainer exec \
    --bind /data:/data \
    --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
    "$CONTAINER" \
    python3 - "$SCRIPT_PATH" "$DATASET_PATH" "$RESULT_DIR" "$SEED" "$M" <<'PY'
import csv
import importlib.util
import math
import pathlib
import sys

script_path = pathlib.Path(sys.argv[1])
dataset_path = pathlib.Path(sys.argv[2])
result_dir = pathlib.Path(sys.argv[3])
seed = int(sys.argv[4], 0)
m = int(sys.argv[5], 0)

if m <= 0 or m & (m - 1) != 0:
    raise SystemExit(f"m must be a positive power of two, got {m}")

spec = importlib.util.spec_from_file_location("problem1", script_path)
problem1 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(problem1)

counts = [0] * m
values = []

with dataset_path.open("r", encoding="utf-8") as infile:
    for line in infile:
        key = line.rstrip("\n\r")
        value = problem1.murmur3_32(key, seed) & (m - 1)
        counts[value] += 1
        values.append(value)

n = len(values)
if n == 0:
    raise SystemExit("input dataset contains no keys")

mean = sum(values) / n
variance = sum((value - mean) ** 2 for value in values) / n
stddev = math.sqrt(variance)
collisions = sum(count * (count - 1) // 2 for count in counts)
key_pairs = n * (n - 1) // 2
collision_probability = collisions / key_pairs if key_pairs else 0.0

frequency_csv = result_dir / "problem1b_frequency.csv"
with frequency_csv.open("w", encoding="utf-8", newline="") as outfile:
    writer = csv.writer(outfile)
    writer.writerow(["hash_value", "frequency"])
    for value, count in enumerate(counts):
        writer.writerow([value, count])

summary_txt = result_dir / "problem1b_summary.txt"
with summary_txt.open("w", encoding="utf-8") as outfile:
    outfile.write(f"dataset: {dataset_path}\n")
    outfile.write(f"seed: {seed:#010x}\n")
    outfile.write(f"m: {m}\n")
    outfile.write(f"number of keys: {n}\n")
    outfile.write(f"mean: {mean:.10f}\n")
    outfile.write(f"standard deviation: {stddev:.10f}\n")
    outfile.write(f"number of collisions: {collisions}\n")
    outfile.write(f"number of key pairs: {key_pairs}\n")
    outfile.write(f"collision probability: {collision_probability:.10f}\n")
    outfile.write(f"ideal uniform collision probability: {1 / m:.10f}\n")

print(f"number of keys: {n}")
print(f"mean: {mean:.10f}")
print(f"standard deviation: {stddev:.10f}")
print(f"number of collisions: {collisions}")
print(f"number of key pairs: {key_pairs}")
print(f"collision probability: {collision_probability:.10f}")
print(f"ideal uniform collision probability: {1 / m:.10f}")
print(f"wrote frequency distribution: {frequency_csv}")
print(f"wrote summary: {summary_txt}")

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figure_path = result_dir / "problem1b_histogram.png"
    plt.figure(figsize=(12, 5))
    plt.bar(range(m), counts, width=1.0, edgecolor="black", linewidth=0.2)
    plt.xlabel("Hash value using the least significant 7 bits")
    plt.ylabel("Frequency")
    plt.title(f"Murmur3_32 frequency distribution, m={m}, seed={seed:#010x}")
    plt.tight_layout()
    plt.savefig(figure_path, dpi=200)
    plt.close()
    print(f"wrote histogram: {figure_path}")
except Exception as exc:
    print(f"warning: could not write histogram with matplotlib: {exc}", file=sys.stderr)
PY

  echo
  echo "finished_at=$(date --iso-8601=seconds)"
} 2>&1 | tee "${RESULT_DIR}/problem1b_run.txt"
