import heapq
import os
from itertools import islice


def chunks_of_file(fp, n_bytes):
    while True:
        d = fp.readlines(n_bytes)
        if not d:
            break
        yield d


def grouper(iterable, n):
    it = iter(iterable)
    x = list(islice(it, n))
    while x:
        yield x
        x = list(islice(it, n))


def k_way_merge(files, out_file, n=1000):
    pointers = [open(f) for f in files]
    q = heapq.merge(*pointers)

    with open(out_file, 'w') as out:
        for group in grouper(q, 100):
            out.writelines(group)

    [fp.close() for fp in pointers]
    [os.remove(f) for f in files]
    

def sort_chunks(fp, temp, n_bytes):
    count = 0
    for chunk in chunks_of_file(fp, n_bytes):
        chunk.sort()

        with open(os.path.join(temp, 'chunk_{}.txt'.format(count)), 'w') as out:
            out.writelines(chunk)
        
        count += 1


if __name__ == '__main__':
    with open('data/raw/unsorted.txt') as fp:
        sort_chunks(fp, 'data/raw/temp', 50000)

    files = ['data/raw/temp/' + i for i in os.listdir('data/raw/temp/')]
    out_file = 'data/raw/temp/sorted.txt'
    k_way_merge(files, out_file)