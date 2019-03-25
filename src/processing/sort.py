import heapq
import os
from contextlib import ExitStack
from glob import glob
from itertools import islice, zip_longest
from tempfile import TemporaryDirectory

from tqdm import tqdm


def chunks_of_file(fp, n_bytes):
    """
    Yields lines of a file such that size of lines is not greater than n_bytes

    Args:
        fp (file pointer): file to read
        n_bytes (int): maximum number of bytes

    Yields:
        list: a list of lines using not more than n_bytes
    """
    while True:
        d = fp.readlines(n_bytes)
        if not d:
            break
        yield d
        del d


def grouper(iterable, n):
    """
    Yields the next n items from iterable until exhausted. 

    Args:
        iterable (iterable): an iterable
        n (int): number of items to yield in each chunk

    Yields:
        list: next n items from iterable
    """
    it = iter(iterable)
    x = list(islice(it, n))
    while x:
        yield x
        x = list(islice(it, n))


def k_way_merge(files, out_file, n=2**20):
    """
    Merges sorted files into a single sorted file.

    This function loads n bytes of lines from each file in files, and merges
    them into a single file using a min heap. 

    Args:
        files (list): list of file paths to merge. 
            Note that each file must be merged. 
        out_file (str): output file path
        n (int): number of bytes to load in from each file at a time
    """
    with ExitStack() as stack, open(out_file, 'w') as out:
        fps = [stack.enter_context(open(f)) for f in files]
        chunks = [chunks_of_file(fp, n) for fp in fps]

        while True:
            q = heapq.merge(*[next(c, []) for c in chunks])
            
            item = next(q, None)
            if not item:
                # No items were added to the queue, so it's done
                return
            out.write(item)

            for item in q:
                out.write(item)


def sort_chunks(file_to_sort, temp, n_bytes=2**20):
    """
    Creates sorted chunks from file_to_sort. Each chunk will be less than or 
    equal to n_bytes in size. 

    Args:
        file_to_sort (str): path to input file
        temp (str): path to directory in which to save chunks
        n_bytes (int): maximum size of each chunk file
    """
    with open(file_to_sort, 'rb') as fp:
        i = 0
        chunk = fp.readlines(n_bytes)
        while chunk:
            chunk.sort()

            with open(os.path.join(temp, str(i)), 'wb') as out:
                out.writelines(chunk)

            del chunk
            chunk = fp.readlines(n_bytes)
            i += 1


def external_sort(in_file, out_file, n_bytes=2**25, k=10, temp_dir=None):
    """
    Performs an external merge sort on an input file. 

    The function works by first splitting in_file into sorted chunks using the
    sort_chunks function. 

    The files are then merged into larger sorted files in batches of k using the
    k_way_merge function. 

    Args:
        in_file (str): path to file to sort
        out_file (str): path to output file
        n_bytes (int): roughly the maximum number of bytes that will be loaded
            into memory at any one time. 
        k (int): number of files to merge at a time
        temp_dir (str): a folder in which to place the temporary files. 
            If None, the system default is chosen, according to the tempfile
            package. 
    """
    pass_num = 0
    with TemporaryDirectory(prefix='sort-', dir=temp_dir) as temp:
        # Sort into chunks
        chunks_dir = os.path.join(temp, str(pass_num))
        
        print('Sorting Chunks...')
        os.mkdir(chunks_dir)
        sort_chunks(in_file, chunks_dir, n_bytes=n_bytes)

        while True:
            pass_num += 1
            print('Pass number {}...'.format(pass_num))

            files_to_merge = glob(os.path.join(temp, str(pass_num - 1), '*'))
            if len(files_to_merge) <= k:
                k_way_merge(files_to_merge, out_file, n=int(n_bytes/k))
                return

            merge_to = os.path.join(temp, str(pass_num))
            os.mkdir(merge_to)

            for i, k_files in tqdm(enumerate(grouper(files_to_merge, k))):
                k_way_merge(k_files, os.path.join(merge_to, str(i)), n=int(n_bytes/k))
                [os.remove(f) for f in k_files]


if __name__ == '__main__':
    external_sort('data/raw/unsorted.txt', 'data/raw/sorted.txt', n_bytes=2**19, k=5, temp_dir='data/raw/temp')
