#!/usr/bin/env python3

import argparse
import sys

def murmur3_32(key, seed):
    """Computes the 32-bit murmur3 hash"""
    # use the implementation from Problem 1
    raise NotImplementedError()

def auto_int(x):
    """Auxiliary function to help convert e.g. hex integers"""
    return int(x,0)

def dlog2(n):
    """Auxiliary function to compute discrete base2 logarithm"""
    return n.bit_length() - 1

def rho(n):
    """Given a 32-bit number n, return the 1-based position of the first
    1-bit"""
    raise NotImplementedError()

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
    print(f'{h:08x}')
    j = ~(0xffffffff << log2m) & h
    r = rho(h)
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
        h = murmur3_32(key,seed)

        j, r = compute_jr(key,seed,log2m)

        print(f'{key}\t{j}\t{r}')
        