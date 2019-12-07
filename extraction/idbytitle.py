import os
from tempfile import TemporaryDirectory

import pandas as pd
import yaml
from atomicwrites import atomic_write
from tqdm import tqdm

from bsearch import BinarySearchFile
from constants import Cols, Dat
from extsort import external_sort, k_way_merge


def load_config(config_path):
    """Loads YAML config"""
    with open(config_path) as f:
        return yaml.load(f)


def extract_page_columns():
    source = os.path.join(Dat.ROOT_PAGE, 'page.csv')
    page = Dat.PAGE_DIRECT_UNS
    redirects = Dat.PAGE_REDIRECT_UNS
    names = Cols.PAGE

    extract_columns = ['page_title', 'page_id']
    chunksize = 10**4  # measured in lines, not bytes

    page_it = pd.read_csv(source,
                          chunksize=chunksize,
                          names=names,
                          dtype=object,
                          engine='c',
                          encoding='utf-8')
    page_it = tqdm(page_it, unit_scale=chunksize)
    with atomic_write(page, encoding='utf-8') as fp_page:
        with atomic_write(redirects, encoding='utf-8') as fp_redirects:
            for ch in page_it:
                # We will use only main namespace pages (i.e. content)
                ch = ch[ch['page_namespace'] == '0']

                # Redirects filter
                r = ch['page_is_redirect'] == '1'

                ch = ch[extract_columns]

                # Writes redirects and direct pages to respective files
                ch[r].to_csv(fp_redirects, index=False, header=None, mode='a')
                ch[~r].to_csv(fp_page, index=False, header=False, mode='a')


def extract_redirect_columns():
    extract_columns = ['rd_from', 'rd_title']
    chunksize = 10**4  # measured in lines, not bytes

    page_it = pd.read_csv(os.path.join(Dat.ROOT_REDIRECT, 'redirect.csv'),
                          chunksize=chunksize,
                          names=Cols.REDIRECT,
                          dtype=object,
                          engine='c',
                          encoding='utf-8')
    page_it = tqdm(page_it, unit_scale=chunksize, desc='Extracting Redirect')
    with atomic_write(Dat.REDIRECT, encoding='utf-8') as f:
        for ch in page_it:
            # Only uses content
            ch = ch[ch['rd_namespace'] == '0']
            ch[extract_columns].to_csv(f, index=False, header=None, mode='a')


def resolve_redirects():
    # Redirects to resolve

    # `redirect` table providing ID -> Title lookup

    # `page` table giving Title -> true ID lookup

    # Output file of resolved redirects

    chunk_size = 2**20  # uses 1MB chunks

    with atomic_write(Dat.PAGE_REDIRECT_RESOLVED, encoding='utf-8') as out, \
         BinarySearchFile(Dat.REDIRECT, index=Dat.REDIRECT_I) as rd, \
         BinarySearchFile(Dat.PAGE_DIRECT, index=Dat.PAGE_DIRECT_I) as page, \
         open(Dat.PAGE_REDIRECT, encoding='utf-8') as rpage:

        pbar = tqdm(desc='Resolving redirects')

        ch = rpage.readlines(chunk_size)
        while ch:
            # Gets [from_title, from_id] for each line and sorts on from_id
            idx = [line.rfind(',') for line in ch]
            ch = [[ch[i][:v], ch[i][v + 1:-1]] for i, v in enumerate(idx)]
            ch.sort(key=lambda x: x[1])

            # Searches the redirect table for to_titles
            to_titles, _ = rd.search_many([x[1] for x in ch])
            [ch[i].append(to_titles[i][0]) for i in range(len(ch))]

            # Searches the direct pages table for their ID
            to_ids = [page.search(t[2]) if t[2] else (False, -1) for t in ch]
            to_write = []
            for i, to_id in enumerate(to_ids):
                if to_id[0]:
                    to_write.append(ch[i][0] + ',' + to_id[0] + '\n')
            out.writelines(to_write)
            print(len(to_write) / len(ch))
            pbar.update(len(ch))
            ch = rpage.readlines(chunk_size)


def merge_page_tables():
    with TemporaryDirectory() as temp:
        res_sorted = os.path.join(temp, 'res_sorted')
        external_sort(Dat.PAGE_REDIRECT_RESOLVED,
                      os.path.join(temp, res_sorted),
                      n_bytes=2**Dat.MEMORY)

        files = [Dat.PAGE_DIRECT, res_sorted]
        k_way_merge(files, Dat.PAGE)


if __name__ == '__main__':
    config = load_config('config/pi.yaml')
    # root = config['data_root']
    # source = os.path.join(root, config['gen']['page_direct'])
    # out = os.path.join(root, 'gen/page_index.csv')
