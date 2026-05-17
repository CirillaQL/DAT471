#!/usr/bin/env python3

import argparse
import sys
import os
from pyspark import SparkContext, SparkConf
import math
import time


def rol32(x, k):
    """Auxiliary function (left rotation for 32-bit words)"""
    return ((x << k) | (x >> (32 - k))) & 0xffffffff


def murmur3_32(key, seed):
    """Computes the 32-bit murmur3 hash"""
    data = key.encode('utf-8')
    length = len(data)

    c1 = 0xcc9e2d51
    c2 = 0x1b873593

    h1 = seed & 0xffffffff

    nblocks = length // 4
    for i in range(nblocks):
        off = 4 * i
        k1 = (data[off]
              | (data[off + 1] << 8)
              | (data[off + 2] << 16)
              | (data[off + 3] << 24))

        k1 = (k1 * c1) & 0xffffffff
        k1 = rol32(k1, 15)
        k1 = (k1 * c2) & 0xffffffff

        h1 ^= k1
        h1 = rol32(h1, 13)
        h1 = (h1 * 5 + 0xe6546b64) & 0xffffffff

    tail_start = 4 * nblocks
    k1 = 0
    tail_size = length - tail_start

    if tail_size >= 3:
        k1 ^= data[tail_start + 2] << 16
    if tail_size >= 2:
        k1 ^= data[tail_start + 1] << 8
    if tail_size >= 1:
        k1 ^= data[tail_start]

    if tail_size > 0:
        k1 = (k1 * c1) & 0xffffffff
        k1 = rol32(k1, 15)
        k1 = (k1 * c2) & 0xffffffff
        h1 ^= k1

    h1 ^= length

    h1 ^= h1 >> 16
    h1 = (h1 * 0x85ebca6b) & 0xffffffff
    h1 ^= h1 >> 13
    h1 = (h1 * 0xc2b2ae35) & 0xffffffff
    h1 ^= h1 >> 16

    return h1


def auto_int(x):
    """Auxiliary function to help convert e.g. hex integers"""
    return int(x, 0)


def dlog2(n):
    return n.bit_length() - 1


def rho(n, width=32):
    """Return the 1-based position of the first 1-bit from the left."""
    if n == 0:
        return 0
    return width - n.bit_length() + 1


def compute_jr(key, seed, log2m):
    """hash the string key with murmur3_32, using the given seed
    then take the **least significant** log2(m) bits as j
    then compute the rho value **from the left**

    E.g., if m = 1024 and we compute hash value 0x70ffec73
    or 0b01110000111111111110110001110011
    then j = 0b0001110011 = 115
         r = 2
         since the 2nd digit of 0111000011111111111011 is the first 1

    Return a tuple (j,r) of integers
    """
    h = murmur3_32(key, seed)
    j = h & ((1 << log2m) - 1)
    remaining = h >> log2m
    r = rho(remaining, 32 - log2m)
    return j, r


def get_files(path):
    """
    A generator function: Iterates through all .txt files in the path and
    returns the content of the files

    Parameters:
    - path : string, path to walk through

    Yields:
    The content of the files as strings
    """
    for (root, dirs, files) in os.walk(path):
        for file in files:
            if file.endswith('.txt'):
                path = f'{root}/{file}'
                with open(path, 'r') as f:
                    yield f.read()


def get_filenames(path):
    """Iterate through all .txt filenames under path."""
    for (root, dirs, files) in os.walk(path):
        for file in files:
            if file.endswith('.txt'):
                yield f'{root}/{file}'


def get_words_from_file(path):
    """Read one file on a Spark worker and return its whitespace-separated words."""
    with open(path, 'r') as f:
        return f.read().split()


def alpha(m):
    """Auxiliary function: bias correction"""
    if m == 16:
        return 0.673
    if m == 32:
        return 0.697
    if m == 64:
        return 0.709
    return 0.7213 / (1 + 1.079 / m)


def estimate_cardinality(registers):
    """Compute the HyperLogLog estimate from a complete register array."""
    m = len(registers)
    raw_estimate = alpha(m) * m * m / sum(2.0 ** -r for r in registers)

    empty_registers = registers.count(0)
    if raw_estimate <= 2.5 * m and empty_registers > 0:
        return m * math.log(m / empty_registers)

    two_to_32 = 2.0 ** 32
    if raw_estimate > two_to_32 / 30.0:
        return -two_to_32 * math.log(1.0 - raw_estimate / two_to_32)

    return raw_estimate


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Using HyperLogLog, computes the approximate number of '
            'distinct words in all .txt files under the given path.'
    )
    parser.add_argument('path', help='path to walk', type=str)
    parser.add_argument('-s', '--seed', type=auto_int, default=0,
                        help='seed value')
    parser.add_argument('-m', '--num-registers', type=int, required=True,
                        help=('number of registers (must be a power of two)'))
    parser.add_argument('-w', '--num-workers', type=int, default=1,
                        help='number of Spark workers')
    args = parser.parse_args()

    seed = args.seed
    m = args.num_registers
    if m <= 0 or (m & (m - 1)) != 0:
        sys.stderr.write(f'{sys.argv[0]}: m must be a positive power of 2\n')
        quit(1)
    if m > (1 << 32):
        sys.stderr.write(f'{sys.argv[0]}: m must be at most 2^32\n')
        quit(1)
    log2m = dlog2(m)

    num_workers = args.num_workers
    if num_workers < 1:
        sys.stderr.write(f'{sys.argv[0]}: must have a positive number of '
                         'workers\n')
        quit(1)

    path = args.path
    if not os.path.isdir(path):
        sys.stderr.write(f"{sys.argv[0]}: `{path}' is not a valid directory\n")
        quit(1)

    start = time.time()
    conf = SparkConf()
    conf.setMaster(f'local[{num_workers}]')
    conf.set('spark.driver.memory', '16g')
    sc = SparkContext(conf=conf)

    num_partitions = num_workers * 4

    try:
        filenames = list(get_filenames(path))
        if not filenames:
            E = 0.0
        else:
            data = sc.parallelize(filenames, num_partitions)

            register_pairs = (data
                              .flatMap(get_words_from_file)
                              .map(lambda word: compute_jr(word, seed, log2m))
                              .reduceByKey(max)
                              .collect())

            registers = [0] * m
            for j, r in register_pairs:
                registers[j] = r

            E = estimate_cardinality(registers)
    finally:
        sc.stop()

    end = time.time()

    print(f'Cardinality estimate: {E:.1f}')
    print(f'Number of workers: {num_workers}')
    print(f'Took {end-start} s')
