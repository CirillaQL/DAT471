#!/usr/bin/env python3

import time
import argparse
import findspark
findspark.init()
from pyspark import SparkContext

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = \
                                    'Compute Twitter follows.')
    parser.add_argument('-w','--num-workers',default=1,type=int,
                            help = 'Number of workers')
    parser.add_argument('filename',type=str,help='Input filename')
    args = parser.parse_args()

    start = time.time()
    sc = SparkContext(master = f'local[{args.num_workers}]')

    lines = sc.textFile(args.filename)

    def parse_follows(line):
        user_id_text, follows_text = line.split(":", 1)
        follows = follows_text.replace(",", " ").split()
        return int(user_id_text), len(follows)

    follows_counts = lines.filter(lambda line: line.strip()) \
        .map(parse_follows) \
        .cache()

    user_count = follows_counts.count()
    max_pair = follows_counts.max(key=lambda x: (x[1], -x[0]))
    total_follows = follows_counts.map(lambda x: x[1]).sum()
    average_follows = total_follows / user_count if user_count else 0
    no_follow_count = follows_counts.filter(lambda x: x[1] == 0).count()

    end = time.time()
    
    total_time = end - start

    # the first ??? should be the twitter id
    print(f'max follows: {max_pair[0]}    follows {max_pair[1]}')
    print(f'users follow on average: {average_follows}')
    print(f'number of user who follow no-one: {no_follow_count}')
    print(f'num workers: {args.num_workers}')
    print(f'total time: {total_time}')
