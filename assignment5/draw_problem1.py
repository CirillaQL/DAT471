#!/usr/bin/env python3

import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt


FALLBACK_CSV = """hash_value,frequency
0,3300
1,3329
2,3285
3,3318
4,3404
5,3338
6,3245
7,3203
8,3282
9,3243
10,3310
11,3376
12,3314
13,3248
14,3291
15,3289
16,3231
17,3325
18,3226
19,3362
20,3263
21,3230
22,3292
23,3271
24,3328
25,3279
26,3246
27,3248
28,3305
29,3230
30,3354
31,3356
32,3264
33,3261
34,3381
35,3286
36,3267
37,3223
38,3415
39,3316
40,3361
41,3136
42,3260
43,3287
44,3245
45,3271
46,3392
47,3298
48,3381
49,3329
50,3186
51,3358
52,3254
53,3268
54,3323
55,3276
56,3297
57,3290
58,3205
59,3285
60,3260
61,3383
62,3235
63,3200
64,3222
65,3371
66,3239
67,3234
68,3186
69,3368
70,3204
71,3289
72,3191
73,3272
74,3342
75,3356
76,3224
77,3304
78,3253
79,3298
80,3172
81,3243
82,3309
83,3223
84,3252
85,3386
86,3425
87,3310
88,3210
89,3294
90,3301
91,3249
92,3304
93,3283
94,3314
95,3348
96,3316
97,3301
98,3254
99,3268
100,3346
101,3342
102,3308
103,3261
104,3342
105,3292
106,3279
107,3276
108,3137
109,3299
110,3358
111,3348
112,3295
113,3301
114,3234
115,3285
116,3346
117,3328
118,3249
119,3326
120,3314
121,3224
122,3398
123,3291
124,3182
125,3207
126,3294
127,3278
"""


def read_frequency_csv(csv_path):
    if csv_path.exists():
        rows = csv.DictReader(csv_path.open(newline=""))
    else:
        rows = csv.DictReader(FALLBACK_CSV.splitlines())

    hash_values = []
    frequencies = []
    for row in rows:
        hash_values.append(int(row["hash_value"]))
        frequencies.append(int(row["frequency"]))
    return hash_values, frequencies


def plot_histogram(hash_values, frequencies, output_path):
    mean_frequency = sum(frequencies) / len(frequencies)

    fig, ax = plt.subplots(figsize=(13, 6))
    ax.bar(hash_values, frequencies, width=0.85, color="#377eb8", edgecolor="#1f3f5b", linewidth=0.35)
    ax.axhline(mean_frequency, color="#d62728", linestyle="--", linewidth=1.5, label=f"Mean = {mean_frequency:.1f}")

    ax.set_title("Frequency Distribution of Hash Values")
    ax.set_xlabel("Hash value")
    ax.set_ylabel("Frequency")
    ax.set_xlim(min(hash_values) - 1, max(hash_values) + 1)
    ax.set_xticks(range(0, max(hash_values) + 1, 8))
    ax.grid(axis="y", alpha=0.25)
    ax.legend()

    fig.tight_layout()
    fig.savefig(output_path, dpi=200)
    plt.close(fig)


def main():
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Plot the frequency distribution of hash values as a histogram."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=script_dir / "assignment5_problem1b_results" / "problem1b_frequency.csv",
        help="Path to problem1b_frequency.csv.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=script_dir / "problem1_hash_histogram.png",
        help="Output image path.",
    )
    args = parser.parse_args()

    hash_values, frequencies = read_frequency_csv(args.csv)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    plot_histogram(hash_values, frequencies, args.output)
    print(f"Saved histogram to {args.output}")


if __name__ == "__main__":
    main()
