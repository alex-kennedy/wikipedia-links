import gzip
import os
import re
import shutil
import subprocess

import requests
from tqdm import tqdm

from utils import load_config

tqdm.monitor_interval = 0

def download_and_decompress(config, table, force=False):
    """
    Download and unzip compressed SQL dump

    Args:
        config (dict): project config dictionary
        table (str): name of wiki table
        force (bool): overwrite file if it already exists
    """
    folder = os.path.join(config['data_root'], table)
    file_name = '-'.join(['enwiki', str(config['data_date']), table + '.sql.gz'])
    file_path = os.path.join(folder, file_name)
    
    url = os.path.join(config['data_remote'], config['data_date'], file_name)
    
    # Make data folder
    if not os.path.isdir(folder):
        os.mkdir(folder)

    # If unzipped file exists, exits, unless force=True
    if not force and os.path.exists(file_path[:-3]):
        print('Unzipped table ({}) exists'.format(table))
        return

    # Downloads file
    if not os.path.exists(file_path):
        subprocess.call(['wget', '-O', file_path, url])

    # Unzips file
    print('Unzipping {}...'.format(table))
    with gzip.open(file_path, 'rb') as f_in:
        with open(file_path[:-3], 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print('Unzipped!')


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

    # This is done line by line because each line is ~1MB
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


def download_table_to_csv(config, table, force=False):
    """
    Convert a wiki SQL table to a CSV.

    The the decompressed SQL file does not exist, download_and_decompress is 
    called. 
    
    Args:
        config (dict): project config dictionary
        table (str): table name to convert
        force (bool): overwrite CSV if it is already present
    """
    sql_file_name = '-'.join(['enwiki', str(config['data_date']), table + '.sql'])
    path_in = os.path.join(config['data_root'], table, sql_file_name)
    path_out = os.path.join(config['data_root'], table, config['tables'][table]['out_csv'])

    if not os.path.exists(path_in):
        download_and_decompress(config, table)

    if not force and os.path.exists(path_out):
        print('Table ({}) already converted to CSV'.format(table))
        return 

    print('Unpacking table: {}'.format(table))
    sql_dump_to_csv(path_in, path_out)


if __name__ == '__main__':
    config = load_config('config/pi.yaml')
    for table in config['tables'].keys():
        download_table_to_csv(config, table)
