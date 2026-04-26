#!/bin/bash

#SBATCH --job-name=assignment2_p2e
#SBATCH --output=assignment2_problem2e.out
#SBATCH --error=assignment2_problem2e.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=64

set -euo pipefail

CONTAINER="/data/courses/2026_dat471_dit066/containers/assignment2.sif"
SCRIPT_DIR="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
SCRIPT_PATH="${SCRIPT_DIR}/assignment2_problem2e.py"
DATASET_PATH="/data/courses/2026_dat471_dit066/datasets/gutenberg/huge"
RESULT_DIR="${SCRIPT_DIR}/assignment2_problem2e_results"
SUMMARY_FILE="${RESULT_DIR}/problem2e_summary.txt"
CSV_FILE="${RESULT_DIR}/speedup.csv"
PLOT_FILE="${RESULT_DIR}/speedup.svg"
BATCH_SIZE="${BATCH_SIZE:-1}"
WORKERS=(1 2 4 8 16 32 64)

mkdir -p "$RESULT_DIR"
: > "$SUMMARY_FILE"
echo "workers,total_time,speedup" > "$CSV_FILE"

baseline_time=""

for workers in "${WORKERS[@]}"; do
  result_file="${RESULT_DIR}/workers_${workers}.txt"

  {
    echo "workers=${workers}"
    echo "started_at=$(date --iso-8601=seconds)"
    echo

    apptainer exec \
      --bind /data:/data \
      --bind "${SCRIPT_DIR}:${SCRIPT_DIR}" \
      "$CONTAINER" \
      python3 "$SCRIPT_PATH" --num-workers "$workers" --batch-size "$BATCH_SIZE" "$DATASET_PATH"

    echo
    echo "finished_at=$(date --iso-8601=seconds)"
  } 2>&1 | tee "$result_file"

  total_time="$(awk '/^Total time:/ {print $3}' "$result_file")"
  if [[ -z "$baseline_time" ]]; then
    baseline_time="$total_time"
  fi
  speedup="$(awk -v baseline="$baseline_time" -v total="$total_time" 'BEGIN { printf "%.6f", baseline / total }')"

  echo "${workers},${total_time},${speedup}" >> "$CSV_FILE"

  {
    echo "===== workers=${workers} ====="
    grep -E '^(Time|Total time|Checksum)' "$result_file" || true
    echo "Speedup compared with 1 worker: ${speedup}"
    if [[ "$workers" -eq 64 ]]; then
      echo "Total absolute running time with 64 cores: ${total_time} seconds"
    fi
    echo
  } >> "$SUMMARY_FILE"
done

python3 - "$CSV_FILE" "$PLOT_FILE" <<'PY'
import csv
import sys

csv_path, svg_path = sys.argv[1], sys.argv[2]
rows = []
with open(csv_path, newline='') as f:
    for row in csv.DictReader(f):
        rows.append((int(row['workers']), float(row['speedup'])))

width, height = 760, 460
left, right, top, bottom = 70, 30, 30, 70
plot_w = width - left - right
plot_h = height - top - bottom
max_x = max(x for x, _ in rows)
max_y = max(max(y for _, y in rows), 1.0)

def sx(x):
    return left + (x - 1) / (max_x - 1) * plot_w if max_x > 1 else left

def sy(y):
    return top + (max_y - y) / max_y * plot_h

points = ' '.join(f'{sx(x):.2f},{sy(y):.2f}' for x, y in rows)
circles = '\n'.join(
    f'<circle cx="{sx(x):.2f}" cy="{sy(y):.2f}" r="4" fill="#1f77b4" />'
    f'<text x="{sx(x):.2f}" y="{sy(y) - 10:.2f}" text-anchor="middle" font-size="12">{y:.2f}</text>'
    for x, y in rows
)
x_labels = '\n'.join(
    f'<text x="{sx(x):.2f}" y="{height - 40}" text-anchor="middle" font-size="12">{x}</text>'
    for x, _ in rows
)

svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <rect width="100%" height="100%" fill="white" />
  <text x="{width / 2}" y="22" text-anchor="middle" font-size="18" font-family="sans-serif">Problem 2e Speedup</text>
  <line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" stroke="black" />
  <line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" stroke="black" />
  <text x="{width / 2}" y="{height - 15}" text-anchor="middle" font-size="14" font-family="sans-serif">Workers</text>
  <text x="18" y="{height / 2}" text-anchor="middle" font-size="14" font-family="sans-serif" transform="rotate(-90 18 {height / 2})">Speedup</text>
  <text x="{left - 10}" y="{sy(1):.2f}" text-anchor="end" dominant-baseline="middle" font-size="12">1.0</text>
  <line x1="{left - 4}" y1="{sy(1):.2f}" x2="{width - right}" y2="{sy(1):.2f}" stroke="#ddd" />
  <polyline points="{points}" fill="none" stroke="#1f77b4" stroke-width="2" />
  {circles}
  {x_labels}
</svg>
'''

with open(svg_path, 'w') as f:
    f.write(svg)
PY
