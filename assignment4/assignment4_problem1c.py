#!/usr/bin/env python3

import time
import argparse
import findspark
findspark.init()
from operator import add
from pyspark import SparkContext

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = \
                                    'Compute Twitter followers.')
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
        return int(user_id_text), [int(followed_id) for followed_id in follows]

    def follower_count_records(user_and_follows):
        user_id, follows = user_and_follows
        yield user_id, 0
        for followed_id in follows:
            yield followed_id, 1

    parsed_lines = lines.filter(lambda line: line.strip()) \
        .map(parse_follows)

    follower_counts = parsed_lines.flatMap(follower_count_records) \
        .reduceByKey(add) \
        .cache()

    user_count = follower_counts.count()
    max_pair = follower_counts.max(key=lambda x: (x[1], -x[0]))
    total_followers = follower_counts.map(lambda x: x[1]).sum()
    average_followers = total_followers / user_count if user_count else 0
    no_followers_count = follower_counts.filter(lambda x: x[1] == 0).count()
    
    end = time.time()
    
    total_time = end - start

    # the first ??? should be the twitter id
    print(f'max followers: {max_pair[0]} has {max_pair[1]} followers')
    print(f'followers on average: {average_followers}')
    print(f'number of user with no followers: {no_followers_count}')
    print(f'num workers: {args.num_workers}')
    print(f'total time: {total_time}')
