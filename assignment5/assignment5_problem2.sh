#!/bin/bash

#SBATCH --job-name=assignment5_p2
#SBATCH --output=assignment5_problem2.out
#SBATCH --error=assignment5_problem2.err
#SBATCH --time=00:10:00

set -euo pipefail

SCRIPT_DIR="${ASSIGNMENT5_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}}"
SCRIPT_PATH="${SCRIPT_DIR}/problem2.py"
RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/assignment5_problem2_results}"
CONTAINER="${CONTAINER:-/data/courses/2026_dat471_dit066/containers/assignment4.sif}"
M="${M:-128}"

mkdir -p "$RESULT_DIR"

{
  echo "script=${SCRIPT_PATH}"
  echo "m=${M}"
  echo "started_at=$(date --iso-8601=seconds)"
  echo

  apptainer exec \
    --bind /data:/data \
    --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
    "$CONTAINER" \
    python3 - "$SCRIPT_PATH" "$RESULT_DIR" "$M" <<'PY'
import csv
import importlib.util
import pathlib
import sys

script_path = pathlib.Path(sys.argv[1])
result_dir = pathlib.Path(sys.argv[2])
m = int(sys.argv[3], 0)

if m <= 0 or m & (m - 1) != 0:
    raise SystemExit(f"m must be a positive power of two, got {m}")

spec = importlib.util.spec_from_file_location("problem2", script_path)
problem2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(problem2)
log2m = problem2.dlog2(m)

cases = [
    ("empty string", "", 0x00000000),
    ("empty string", "", 0x00000001),
    ("empty string", "", 0xffffffff),
    ("test", "test", 0x00000000),
    ("test", "test", 0x9747b28c),
    ("Hello, world!", "Hello, world!", 0x00000000),
    ("Hello, world!", "Hello, world!", 0x9747b28c),
    ("The quick brown fox jumps over the lazy dog",
     "The quick brown fox jumps over the lazy dog", 0x00000000),
    ("The quick brown fox jumps over the lazy dog",
     "The quick brown fox jumps over the lazy dog", 0x9747b28c),
    ("Rychla hneda liska preskocila leniveho psa",
     "Rýchla hnedá líška preskočila lenivého psa", 0x00000000),
    ("Rychla hneda liska preskocila leniveho psa",
     "Rýchla hnedá líška preskočila lenivého psa", 0x9747b28c),
    ("Bystraya korichnevaya lisa pereprygivaet cherez lenivuyu sobaku",
     "Быстрая коричневая лиса перепрыгивает через ленивую собаку", 0x00000000),
    ("Bystraya korichnevaya lisa pereprygivaet cherez lenivuyu sobaku",
     "Быстрая коричневая лиса перепрыгивает через ленивую собаку", 0x9747b28c),
    ("Chinese pangram", "敏捷的棕色狐狸跳过了懒狗", 0x00000000),
    ("Chinese pangram", "敏捷的棕色狐狸跳过了懒狗", 0x9747b28c),
]

csv_path = result_dir / "problem2_jr_pairs.csv"
with csv_path.open("w", encoding="utf-8", newline="") as outfile:
    writer = csv.writer(outfile)
    writer.writerow(["label", "seed", "j", "r"])
    for label, key, seed in cases:
        j, r = problem2.compute_jr(key, seed, log2m)
        writer.writerow([label, f"{seed:#010x}", j, r])
        print(f"{label}\t{seed:#010x}\t{j}\t{r}")

summary_path = result_dir / "problem2_summary.txt"
with summary_path.open("w", encoding="utf-8") as outfile:
    outfile.write(f"script: {script_path}\n")
    outfile.write(f"m: {m}\n")
    outfile.write(f"log2m: {log2m}\n")
    outfile.write(f"wrote pairs: {csv_path}\n")

print(f"wrote pairs: {csv_path}")
print(f"wrote summary: {summary_path}")
PY

  echo
  echo "finished_at=$(date --iso-8601=seconds)"
} 2>&1 | tee "${RESULT_DIR}/problem2_run.txt"
