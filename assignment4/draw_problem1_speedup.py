#!/usr/bin/env python3

from pathlib import Path

import matplotlib.pyplot as plt


DATA = {
    "b": {
        "title": "Problem 1(b) empirical speedup",
        "output": "problem1b_speedup.png",
        "single_core_runtime": 28.547500133514404,
        "cores": [1, 2, 4, 8, 16, 32, 64],
        "runtimes": [
            28.569560050964355,
            27.416767835617065,
            24.415091276168823,
            21.7857666015625,
            19.641891479492188,
            17.919296503067017,
            20.47074317932129,
        ],
        "speedups": [
            1.000000,
            1.042047,
            1.170160,
            1.311386,
            1.454522,
            1.594346,
            1.395629,
        ],
    },
    "d": {
        "title": "Problem 1(d) empirical speedup",
        "output": "problem1d_speedup.png",
        "single_core_runtime": 110.12968707084656,
        "cores": [1, 2, 4, 8, 16, 32, 64],
        "runtimes": [
            110.12968707084656,
            98.45034503936768,
            61.75579071044922,
            41.94463586807251,
            30.56769847869873,
            24.80927085876465,
            21.28773474693298,
        ],
        "speedups": [
            1.000000,
            1.118632,
            1.783309,
            2.625596,
            3.602813,
            4.439054,
            5.173387,
        ],
    },
}


def draw_speedup_plot(problem_key, data, output_dir):
    cores = data["cores"]
    speedups = data["speedups"]

    plt.figure(figsize=(7, 4.5))
    plt.plot(cores, speedups, marker="o", linewidth=2, label="Empirical speedup")

    plt.xlabel("Number of cores")
    plt.ylabel("Speedup")
    plt.title(data["title"])
    plt.xticks(cores)
    plt.grid(True, linestyle=":", linewidth=0.8)
    plt.legend()

    runtime_text = f"Single-core runtime: {data['single_core_runtime']:.2f} s"
    plt.annotate(
        runtime_text,
        xy=(0.03, 0.92),
        xycoords="axes fraction",
        bbox={"boxstyle": "round,pad=0.3", "facecolor": "white", "alpha": 0.85},
    )

    output_path = output_dir / data["output"]
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()
    print(f"Problem 1({problem_key}): {runtime_text}")
    print(f"Wrote {output_path}")


def main():
    output_dir = Path(__file__).resolve().parent
    for problem_key, data in DATA.items():
        draw_speedup_plot(problem_key, data, output_dir)


if __name__ == "__main__":
    main()
