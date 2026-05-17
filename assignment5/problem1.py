#!/usr/bin/env python3

import argparse

def rol32(x,k):
    """Auxiliary function (left rotation for 32-bit words)"""
    return ((x << k) | (x >> (32-k))) & 0xffffffff

def murmur3_32(key, seed):
    """Computes the 32-bit MurmurHash3 of a string key."""
    data = key.encode('utf-8')
    length = len(data)

    c1 = 0xCC9E2D51
    c2 = 0x1B873593

    h1 = seed & 0xFFFFFFFF

    nblocks = length // 4
    for i in range(nblocks):
        off = i * 4
        k1 = (data[off]
              | (data[off + 1] << 8)
              | (data[off + 2] << 16)
              | (data[off + 3] << 24))

        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = rol32(k1, 15)
        k1 = (k1 * c2) & 0xFFFFFFFF

        h1 ^= k1
        h1 = rol32(h1, 13)
        h1 = (h1 * 5 + 0xE6546B64) & 0xFFFFFFFF

    tail_start = nblocks * 4
    k1 = 0
    remaining = length - tail_start

    if remaining >= 3:
        k1 ^= data[tail_start + 2] << 16
    if remaining >= 2:
        k1 ^= data[tail_start + 1] << 8
    if remaining >= 1:
        k1 ^= data[tail_start + 0]

    if remaining > 0:
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = rol32(k1, 15)
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1

    h1 ^= length

    h1 ^= h1 >> 16
    h1 = (h1 * 0x85EBCA6B) & 0xFFFFFFFF
    h1 ^= h1 >> 13
    h1 = (h1 * 0xC2B2AE35) & 0xFFFFFFFF
    h1 ^= h1 >> 16

    return h1

def auto_int(x):
    """Auxiliary function to help convert e.g. hex integers"""
    return int(x,0)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Computes MurMurHash3 for the keys.'
    )
    parser.add_argument('key',nargs='*',help='key(s) to be hashed',type=str)
    parser.add_argument('-s','--seed',type=auto_int,default=0,help='seed value')
    args = parser.parse_args()

    seed = args.seed
    for key in args.key:
        h = murmur3_32(key,seed)
        print(f'{h:#010x}\t{key}')
        