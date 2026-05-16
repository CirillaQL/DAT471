#!/usr/bin/env python3

import argparse
import sys

def rol32(x,k):
    """Auxiliary function (left rotation for 32-bit words)"""
    return ((x << k) | (x >> (32-k))) & 0xffffffff

def murmur3_32(key, seed):
    """Computes the 32-bit murmur3 hash"""
    # use the implementation from Problem 1
    data = key.encode('utf-8')
    length = len(data)

    c1 = 0xCC9E2D51
    c2 = 0x1B873593

    h1 = seed & 0xFFFFFFFF

    nblocks = length // 4
    for i in range(nblocks):
        off = i * 4
        # little-endian 4 bytes → uint32（纯位运算，不依赖 struct）
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

def dlog2(n):
    """Auxiliary function to compute discrete base2 logarithm"""
    return n.bit_length() - 1

def rho(n, width=32):
    """Given a 32-bit number n, return the 1-based position of the first
    1-bit"""
    if n == 0:
        return 0
    return width - n.bit_length() + 1

def compute_jr(key,seed,log2m):
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
    h = murmur3_32(key,seed)
    j = h & ((1 << log2m) - 1)
    remaining = h >> log2m
    r = rho(remaining, 32 - log2m)
    return j, r


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Computes (j,r) pairs for input integers.'
    )
    parser.add_argument('key',nargs='*',help='key(s) to be hashed',type=str)
    parser.add_argument('-s','--seed',type=auto_int,default=0,help='seed value')
    parser.add_argument('-m','--num-registers',type=int,required=True,
                            help=('Number of registers (must be a power of two)'))
    args = parser.parse_args()

    seed = args.seed
    m = args.num_registers
    if m <= 0 or (m&(m-1)) != 0:
        sys.stderr.write(f'{sys.argv[0]}: m must be a positive power of 2\n')
        quit(1)

    log2m = dlog2(m)

    for key in args.key:
        j, r = compute_jr(key,seed,log2m)

        print(f'{key}\t{j}\t{r}')
        
