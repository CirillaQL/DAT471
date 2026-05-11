#!/usr/bin/env python3

from pathlib import Path

import matplotlib.pyplot as plt


WORKERS = [1, 2, 4, 8, 16, 32, 64]
TOTAL_TIMES = [
    643.083073,
    634.849398,
    321.727388,
    173.238790,
    103.823519,
    65.464242,
    57.138437,
]


def main():
    output_dir = Path(__file__).resolve().parent
    output_path = output_dir / "problem2b_speedup.png"

    single_worker_runtime = TOTAL_TIMES[0]
    speedups = [single_worker_runtime / runtime for runtime in TOTAL_TIMES]

    plt.figure(figsize=(7, 4.5))
    plt.plot(WORKERS, speedups, marker="o", linewidth=2, label="Empirical speedup")

    plt.xlabel("Number of workers")
    plt.ylabel("Speedup")
    plt.title("Problem 2(b) empirical speedup")
    plt.xticks(WORKERS)
    plt.grid(True, linestyle=":", linewidth=0.8)
    plt.legend()

    runtime_text = f"Single-worker runtime: {single_worker_runtime:.2f} s"
    plt.annotate(
        runtime_text,
        xy=(0.03, 0.92),
        xycoords="axes fraction",
        bbox={"boxstyle": "round,pad=0.3", "facecolor": "white", "alpha": 0.85},
    )

    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()

    print(f"Problem 2(b): {runtime_text}")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
