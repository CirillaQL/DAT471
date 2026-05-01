#!/usr/bin/env python3

import csv
from pathlib import Path

import matplotlib.pyplot as plt


def main():
    script_dir = Path(__file__).resolve().parent
    result_dir = script_dir / "assignment3_problem4_results_parallel"
    csv_path = result_dir / "speedup.csv"
    output_path = result_dir / "problem4_speedup.png"

    workers = []
    speedups = []
    with csv_path.open(newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            workers.append(int(row["workers"]))
            speedups.append(float(row["speedup"]))

    plt.figure(figsize=(7, 4.5))
    plt.plot(workers, speedups, marker="o", label="Empirical speedup")
    plt.plot(workers, workers, linestyle="--", label="Ideal speedup")
    plt.xlabel("Number of cores")
    plt.ylabel("Speedup")
    plt.title("Problem 4 empirical speedup on twitter-2010_10M")
    plt.xticks(workers)
    plt.grid(True, linestyle=":", linewidth=0.8)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
