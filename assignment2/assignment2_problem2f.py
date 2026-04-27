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
    with open(path, 'r') as f:
        return f.read()


def merge_count_dicts(dict_to, dict_from):
    """
    Merges the word counts from dict_from into dict_to, such that
    if the word exists in dict_to, then the count is added to it,
    otherwise a new entry is created with count from dict_from

    Parameters:
    - dict_to, dictionary : dictionary to merge to
    - dict_from, dictionary : dictionary to merge from

    Return value: None
    """
    for (k, v) in dict_from.items():
        if k not in dict_to:
            dict_to[k] = v
        else:
            dict_to[k] += v


def count_words_in_file(filename_queue, wordcount_queue, batch_size):
    """
    Counts the number of occurrences of words in the file.
    Performs counting until a None is encountered in the queue.
    Counts are stored in wordcount_queue.
    Whitespace is ignored.

    Parameters:
    - filename_queue, multiprocessing queue : will contain filenames and None as a sentinel to indicate end of input
    - wordcount_queue, multiprocessing queue : word count dictionaries are put in the queue, and end of input is indicated with None
    - batch_size, int : size of batches to process

    Returns: None
    """
    counts = dict()
    files_in_batch = 0

    while True:
        filename = filename_queue.get()
        if filename is None:
            if counts:
                wordcount_queue.put(counts)
            wordcount_queue.put(None)
            return

        file = get_file(filename)
        for word in file.split():
            if word in counts:
                counts[word] += 1
            else:
                counts[word] = 1

        files_in_batch += 1
        if files_in_batch >= batch_size:
            wordcount_queue.put(counts)
            counts = dict()
            files_in_batch = 0


def get_top10(counts):
    """
    Determines the 10 words with the most occurrences.
    Ties can be solved arbitrarily.

    Parameters:
    - counts, dictionary : a mapping from words (str) to counts (int)

    Return value:
    A list of (count,word) pairs (int,str)
    """
    sorted_counts = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    return [(v, k) for (k, v) in sorted_counts[:10]]


def merge_counts(out_queue, wordcount_queue, num_workers):
    """
    Merges the counts from the queue into a global word count dictionary.
    Quits when num_workers Nones have been encountered.

    Parameters:
    - out_queue, multiprocessing queue : queue used to return checksum and top10 to the main process
    - wordcount_queue, multiprocessing queue : queue that contains count dictionaries and Nones to signal end of input from a worker
    - num_workers, int : number of workers (i.e., how many Nones to expect)

    Return value: None
    """
    global_counts = dict()
    finished_workers = 0

    while finished_workers < num_workers:
        counts = wordcount_queue.get()
        if counts is None:
            finished_workers += 1
        else:
            merge_count_dicts(global_counts, counts)

    checksum = compute_checksum(global_counts)
    top10 = get_top10(global_counts)
    out_queue.put((checksum, top10))


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
    for (k, v) in counts.items():
        checksum += len(k) * v
    return checksum


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Counts words of all the text files in the given directory')
    parser.add_argument('-w', '--num-workers', help='Number of workers', default=1, type=int)
    parser.add_argument('-b', '--batch-size', help='Batch size', default=1, type=int)
    parser.add_argument('path', help='Path that contains text files')
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

    filename_queue = mp.Queue()
    wordcount_queue = mp.Queue()
    out_queue = mp.Queue()

    merger = mp.Process(target=merge_counts, args=(out_queue, wordcount_queue, num_workers))
    workers = [
        mp.Process(target=count_words_in_file, args=(filename_queue, wordcount_queue, batch_size))
        for _ in range(num_workers)
    ]

    time_measurement_a_start = time.time()
    merger.start()
    for worker in workers:
        worker.start()

    num_files = 0
    for filename in get_filenames(path):
        filename_queue.put(filename)
        num_files += 1

    for _ in range(num_workers):
        filename_queue.put(None)
    time_measurement_a_end = time.time()
    print(f'Time for feeding filenames: {time_measurement_a_end - time_measurement_a_start:.2f} seconds')
    print(f'Number of files: {num_files}')

    time_measurement_b_start = time.time()
    checksum, top10 = out_queue.get()
    time_measurement_b_end = time.time()
    print(f'Time for counting, merging, and computing results: {time_measurement_b_end - time_measurement_b_start:.2f} seconds')

    for worker in workers:
        worker.join()
    merger.join()

    print(f'Checksum: {checksum}')
    print('Top 10:')
    for (count, word) in top10:
        print(f'{word}: {count}')

    time_total_end = time.time()
    print(f'Total time: {time_total_end - time_total_start:.2f} seconds')
