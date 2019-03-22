import os

import pandas as pd
from tqdm import tqdm

import utils
from sort import external_sort

# file to create the title -> id lookup table. i.e. the title then id columns of the page table, sorted by title


def extract_page_columns(config):
    root = config['data_root']
    source = os.path.join(root, 'page', config['tables']['page']['out_csv'])
    page = os.path.join(root, config['gen']['page_unsorted'])
    redirects = os.path.join(root, config['gen']['page_redirects_unsorted'])
    names = config['tables']['page']['names']

    extract_columns = ['page_title', 'page_id']
    chunksize = 10**4

    page_it = pd.read_csv(
        source, chunksize=chunksize, names=names, dtype=str, engine='c')
    page_it = tqdm(page_it, unit_scale=chunksize)

    with open(page, 'w') as fp_page:
        with open(redirects, 'w') as fp_redirects:
            for ch in page_it:
                is_redirect = ch[ch['page_is_redirect'] == '1'][extract_columns]

                is_redirect.to_csv(
                    fp_redirects, index=False, header=None, mode='a')
                ch[extract_columns].to_csv(
                    fp_page, index=False, header=False, mode='a')


def sort_on_title(config):
    root = config['data_root']

    # Sort the page file
    unsorted = os.path.join(root, config['gen']['page_unsorted'])
    out = os.path.join(root, config['gen']['page'])
    external_sort(unsorted, out, config['free_memory'], temp_dir=config['temp_dir'])

    # Sort the redirects file
    unsorted = os.path.join(root, config['gen']['page_redirects_unsorted'])
    out = os.path.join(root, config['gen']['page_redirects'])
    external_sort(unsorted, out, config['free_memory'], temp_dir=config['temp_dir'])


def resolve_redirects(config):
    # get from_title in page.csv
    # get from_id in page.csv
    # go to from_id in redirects.csv
    # get to_title from redirects.csv
    # get to_id from page.csv
    # set from_title in page.csv to correct to_id
    pass

if __name__ == '__main__':
    config = utils.load_config('config/pi.yaml')
    # extract_page_columns(config)