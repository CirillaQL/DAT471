import os
import argparse
import sys
import time
import multiprocessing as mp

def get_filenames(path):
    """
    A generator function: Iterates through all .txt files in the path and
    returns the full names of the files

    Parameters:
    - path : string, path to walk through

    Yields:
    The full filenames of all files ending in .txt
    """
    for (root, dirs, files) in os.walk(path):
        for file in files:
            if file.endswith('.txt'):
                yield f'{root}/{file}'

def get_file(path):
    """
    Reads the content of the file and returns it as a string.

    Parameters:
    - path : string, path to a file

    Return value:
    The content of the file in a string.
    """
    with open(path,'r') as f:
        return f.read()

def count_words_in_file(file):
    """
    Counts the number of occurrences of words in the file
    Whitespace is ignored

    Parameters:
    - file, string : the content of a file

    Returns: Dictionary that maps words (strings) to counts (ints)
    """
    counts = dict()
    for word in file.split():
        if word in counts:
            counts[word] += 1
        else:
            counts[word] = 1
    return counts



def get_top10(counts):
    """
    Determines the 10 words with the most occurrences.
    Ties can be solved arbitrarily.

    Parameters:
    - counts, dictionary : a mapping from words (str) to counts (int)
    
    Return value:
    A list of (count,word) pairs (int,str)
    """
    sorted_counts = dict(sorted(counts.items(), key=lambda item: item[1], reverse=True))
    return [(v, k) for k, v in list(sorted_counts.items())[:10]]



def merge_counts(dict_to, dict_from):
    """
    Merges the word counts from dict_from into dict_to, such that
    if the word exists in dict_to, then the count is added to it,
    otherwise a new entry is created with count from dict_from

    Parameters:
    - dict_to, dictionary : dictionary to merge to
    - dict_from, dictionary : dictionary to merge from

    Return value: None
    """
    for (k,v) in dict_from.items():
        if k not in dict_to:
            dict_to[k] = v
        else:
            dict_to[k] += v



def compute_checksum(counts):
    """
    Computes the checksum for the counts as follows:
    The checksum is the sum of products of the length of the word and its count

    Parameters:
    - counts, dictionary : word to count dictionary

    Return value:
    The checksum (int)
    """
    checksum = 0
    for (k,v) in counts.items():
        checksum += len(k) * v
    return checksum


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Counts words of all the text files in the given directory')
    parser.add_argument('-w', '--num-workers', help = 'Number of workers', default=1, type=int)
    parser.add_argument('-b', '--batch-size', help = 'Batch size', default=1, type=int)
    parser.add_argument('path', help = 'Path that contains text files')
    args = parser.parse_args()

    path = args.path

    if not os.path.isdir(path):
        sys.stderr.write(f'{sys.argv[0]}: ERROR: `{path}\' is not a valid directory!\n')
        quit(1)

    num_workers = args.num_workers
    if num_workers < 1:
        sys.stderr.write(f'{sys.argv[0]}: ERROR: Number of workers must be positive (got {num_workers})!\n')
        quit(1)

    batch_size = args.batch_size
    if batch_size < 1:
        sys.stderr.write(f'{sys.argv[0]}: ERROR: Batch size must be positive (got {batch_size})!\n')
        quit(1)

    time_total_start = time.time()
    time_measurement_a_start = time.time()
    files = [get_file(fn) for fn in get_filenames(path)]
    time_measurement_a_end = time.time()
    print(f'Time for reading files: {time_measurement_a_end - time_measurement_a_start:.2f} seconds')

    time_measurement_b_start = time.time()
    file_counts = list()
    for file in files:
        file_counts.append(count_words_in_file(file))
    time_measurement_b_end = time.time()
    print(f'Time for counting words in files: {time_measurement_b_end - time_measurement_b_start:.2f} seconds')

    time_measurement_c_start = time.time()
    global_counts = dict()
    for counts in file_counts:
        merge_counts(global_counts,counts)
    time_measurement_c_end = time.time()
    print(f'Time for merging counts: {time_measurement_c_end - time_measurement_c_start:.2f} seconds')

    time_measurement_d_start = time.time()
    top10 = get_top10(global_counts)
    time_measurement_d_end = time.time()
    print(f'Time for getting top10: {time_measurement_d_end - time_measurement_d_start:.2f} seconds')   

    time_measurement_e_start = time.time()
    checksum = compute_checksum(global_counts)
    time_measurement_e_end = time.time()
    print(f'Time for computing checksum: {time_measurement_e_end - time_measurement_e_start:.2f} seconds')
    
    print(f'Checksum: {checksum}')
    print('Top 10:')
    for (count, word) in top10:
        print(f'{word}: {count}')   

    time_total_end = time.time()
    print(f'Total time: {time_total_end - time_total_start:.2f} seconds')