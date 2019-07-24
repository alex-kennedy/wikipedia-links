import gzip
import os
import re
import shutil
import subprocess

from tqdm import tqdm

tqdm.monitor_interval = 0


def download_table(config, table):
    """Downloads compressed SQL dump.

    Args:
        config (dict): project config dictionary
        table (str): name of wiki table
    """
    folder = os.path.join(config['data_root'], table)
    file_name = '-'.join(
        ['enwiki', str(config['data_date']), table + '.sql.gz'])
    file_path = os.path.join(folder, file_name)

    url = os.path.join(config['data_remote'], config['data_date'], file_name)

    # Make data folder
    if not os.path.isdir(folder):
        os.mkdir(folder)

    # Downloads file
    subprocess.call(['wget', '-O', file_path, url])


def unzip_table(config, table):
    """Unzips gz file to sql."""
    folder = os.path.join(config['data_root'], table)
    bits = ['enwiki', str(config['data_date']), table + '.sql.gz']
    file_name = '-'.join(bits)
    file_path = os.path.join(folder, file_name)

    with gzip.open(file_path, 'rb') as f_in:
        with open(file_path[:-3], 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def sql_dump_to_csv(in_file, out_file, n_bytes=2**26):
    """Converts a MySQL dump from the wikimedia site to a CSV.

    Args:
        in_file (str): path to MySQL dump
        out_file (str): path to output CSV
        n_bytes (int): bytes of the in_file to load at once
    """
    # Statistics
    total_size = os.stat(in_file).st_size
    progress = tqdm(total=total_size, unit_scale=True)

    # This is done line by line because each line is ~1MB
    with open(in_file, 'r', encoding='utf-8') as f:
        with open(out_file, 'w', encoding='utf-8') as out:
            chunk = f.readlines(n_bytes)
            while chunk:
                for line in chunk:
                    progress.update(len(line))

                    # Ignore lines that don't contain values
                    if line[:11] != 'INSERT INTO':
                        continue

                    line = line[line.find('(') + 1:-3]
                    bits = re.split(r'(?<!\\)\)\,\(', line)
                    out.writelines([x + '\n' for x in bits])
                chunk = f.readlines(n_bytes)