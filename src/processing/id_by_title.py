import os

import pandas as pd
import yaml
from atomicwrites import atomic_write
from tqdm import tqdm

from processing.bsearch import BinarySearchFile


def load_config(config_path):
    """Loads YAML config"""
    with open(config_path) as f:
        return yaml.load(f)


def extract_page_columns(config):
    root = config['data_root']
    source = os.path.join(root, 'page', 'page.csv')
    page = os.path.join(root, config['gen']['page_direct_unsorted'])
    redirects = os.path.join(root, config['gen']['page_redirect_unsorted'])
    names = config['tables']['page']['names']

    extract_columns = ['page_title', 'page_id']
    chunksize = 10**4  # measured in lines, not bytes

    page_it = pd.read_csv(
        source, chunksize=chunksize, names=names, dtype=str, engine='c')
    page_it = tqdm(page_it, unit_scale=True)

    with atomic_write(page, overwrite=True) as fp_page:
        with atomic_write(redirects, overwrite=True) as fp_redirects:
            for ch in page_it:
                # We will use only main namespace pages (i.e. content)
                ch = ch[ch['page_namespace'] == '0']

                # Redirects filter
                r = ch['page_is_redirect'] == '1'

                ch = ch[extract_columns]

                # Writes redirects and direct pages to respective files
                ch[r].to_csv(fp_redirects, index=False, header=None, mode='a')
                ch[~r].to_csv(fp_page, index=False, header=False, mode='a')


def extract_redirect_columns(config):
    root = config['data_root']
    source = os.path.join(root, 'redirect', 'redirect.csv')
    out = os.path.join(root, config['gen']['redirect'])
    names = config['tables']['redirect']['names']

    extract_columns = ['rd_from', 'rd_title']
    chunksize = 10**4  # measured in lines, not bytes

    page_it = pd.read_csv(
        source, chunksize=chunksize, names=names, dtype=str, engine='c')
    page_it = tqdm(page_it, unit_scale=True, desc='Extracting Redirect Table: ')

    with atomic_write(out) as f:
        for ch in page_it:
            # Only uses content
            ch = ch[ch['rd_namespace'] == '0']

            ch[extract_columns].to_csv(f, index=False, header=None, mode='a')


def resolve_redirects(config):
    root = config['data_root']

    # Redirects to resolve
    page_redirect_path = os.path.join(root, config['gen']['page_redirect'])

    # `redirect` table providing ID -> Title lookup
    redirect_path = os.path.join(root, config['gen']['redirect'])

    # `page` table giving Title -> true ID lookup
    page_path = os.path.join(root, config['gen']['page_direct'])

    # Output file of resolved redirects
    resolved_path = os.path.join(root, config['gen']['page_redirect_resolved'])

    chunk_size = 2**20  # uses 1MB chunks

    with atomic_write(resolved_path, mode='wb') as out, \
         BinarySearchFile(redirect_path) as redirect, \
         BinarySearchFile(page_path) as page, \
         open(page_redirect_path, 'rb') as rpage:

        pbar = tqdm(desc='Resolving redirects: ')

        ch = rpage.readlines(chunk_size)
        while ch:
            idx = [line.rfind(b',') for line in ch]
            ch = [(ch[i][:v], ch[i][v + 1:-1]) for i, v in enumerate(idx)]
            ch = sorted(ch, key=lambda x: x[1])

            to_titles = redirect.search_many([x[1] for x in ch])
            to_ids = [page.search(t) if t else False for t in to_titles]

            for i, to_id in to_ids:
                if to_id:
                    out.write(ch[i][0] + b',' + to_id, b'\n')
        
    # get from_title and from_id from page.csv
    # e.g. 'AccessibleComputing',10

    # go to from_id and to_title from redirects (original table x.csv)
    # e.g. 10,'Computer_accessibility'

    # get to_id from page (original table x.csv)

    # set from_title in page.csv to correct to_id, so searching on that title
    # gives the id of the 'to' page.


if __name__ == '__main__':
    config = load_config('config/pi.yaml')
    # root = config['data_root']
    # source = os.path.join(root, config['gen']['page_direct'])
    # out = os.path.join(root, 'gen/page_index.csv')
    resolve_redirects(config)