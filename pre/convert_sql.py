import os
import re

import pandas as pd
from tqdm import tqdm

tqdm.monitor_interval = 0

def sql_dump_to_csv(in_file, out_file):
    """
    Converts a MySQL dump from the wikimedia site to a CSV.

    Args:
        in_file (str): path to MySQL dump
        out_file (str): path to output CSV
    """
    # Statistics
    total_size = os.stat(in_file).st_size
    progress = tqdm(total=total_size)

    # Returns all groups inside round brackets
    pattern = re.compile(b'[(](.*?)[)]')

    with open(in_file, 'rb') as f, open(out_file, 'wb') as out:
        for line in f:
            progress.update(len(line))

            # Ignore lines that dont contain values
            if line[:11] != b'INSERT INTO':
                continue

            # Write values in the line to CSV
            match = pattern.findall(line)
            csv_line = b'\n'.join(match) + b'\n'
            out.write(csv_line)            


if __name__ == '__main__':
    sql_dump_to_csv('/mnt/f/wikipedia-links/extracted/enwiki-20190301-pagelinks.sql', '/mnt/f/wikipedia-links/extracted/out.txt')
