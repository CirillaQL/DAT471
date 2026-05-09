#!/usr/bin/env python3

from pathlib import Path

import matplotlib.pyplot as plt


PROBLEM3 = {
    "cores": [1, 2, 4, 8, 16, 32],
    "speedup": [1.000000, 1.007783, 1.504552, 2.300083, 3.107272, 3.821241],
    "title": "Problem 3 empirical speedup on twitter-2010_10M",
    "output": "problem3_speedup.png",
}

PROBLEM4 = {
    "cores": [1, 2, 4, 8, 16, 32],
    "speedup": [1.000000, 0.954090, 1.657961, 2.713135, 3.899284, 4.859165],
    "title": "Problem 4 empirical speedup on twitter-2010_10M",
    "output": "problem4_speedup.png",
}


def draw_speedup_chart(data, output_dir):
    cores = data["cores"]
    speedup = data["speedup"]
    output_path = output_dir / data["output"]

    plt.figure(figsize=(7.2, 4.4))
    plt.plot(cores, speedup, marker="o", linewidth=2.2, label="Empirical speedup")
    plt.plot(cores, cores, linestyle="--", linewidth=1.6, label="Ideal speedup")
    plt.title(data["title"])
    plt.xlabel("Number of cores")
    plt.ylabel("Speedup")
    plt.xticks(cores)
    plt.grid(True, linestyle=":", linewidth=0.8)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()
    print(f"Wrote {output_path}")


def main():
    output_dir = Path(__file__).resolve().parent / "figures"
    output_dir.mkdir(exist_ok=True)

    draw_speedup_chart(PROBLEM3, output_dir)
    draw_speedup_chart(PROBLEM4, output_dir)


if __name__ == "__main__":
    main()
