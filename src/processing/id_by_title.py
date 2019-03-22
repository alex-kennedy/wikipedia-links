import os

import pandas as pd
from tqdm import tqdm

import utils

# file to create the title -> id lookup table. i.e. the title then id columns of the page table, sorted by title


def extract_page_columns(config):
    page = os.path.join(config['data_root'], 'page',
                        config['tables']['page']['out_csv'])
    page_unsorted = os.path.join(config['data_root'],
                                 config['gen']['page_unsorted'])
    page_redirects_unsorted = os.path.join(
        config['data_root'], config['gen']['page_redirects_unsorted'])
    names = config['tables']['page']['names']
    chunksize = 10**4
    extract_columns = ['page_title', 'page_id']

    page_it = pd.read_csv(
        page, chunksize=chunksize, names=names, dtype=str, engine='c')
    page_it = tqdm(page_it, unit_scale=chunksize)

    with open(page_unsorted, 'w') as fp_page:
        with open(page_redirects_unsorted, 'w') as fp_page_redirects:
            for ch in page_it:
                is_redirect = ch[ch['page_is_redirect'] == '1'][extract_columns]

                is_redirect.to_csv(
                    fp_page_redirects, index=False, header=None, mode='a')

                ch[extract_columns].to_csv(
                    fp_page, index=False, header=False, mode='a')


def sort_on_title(config):
    # external merge sort that bad boy
    pass


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
    extract_page_columns(config)