import os
from tempfile import TemporaryDirectory

import pandas as pd
import yaml
from atomicwrites import atomic_write
from tqdm import tqdm

from processing.bsearch import BinarySearchFile
from processing.sort import external_sort, k_way_merge


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


def extract_redirect_columns(config):
    root = config['data_root']
    source = os.path.join(root, 'redirect', 'redirect.csv')
    out = os.path.join(root, config['gen']['redirect'])
    names = config['tables']['redirect']['names']
    extract_columns = ['rd_from', 'rd_title']
    chunksize = 10**4  # measured in lines, not bytes

    page_it = pd.read_csv(source,
                          chunksize=chunksize,
                          names=names,
                          dtype=object,
                          engine='c',
                          encoding='utf-8')
    page_it = tqdm(page_it, unit_scale=chunksize, desc='Extracting Redirect')
    with atomic_write(out, encoding='utf-8') as f:
        for ch in page_it:
            # Only uses content
            ch = ch[ch['rd_namespace'] == '0']
            ch[extract_columns].to_csv(f, index=False, header=None, mode='a')


def resolve_redirects(config):
    root_to = lambda p: os.path.join(config['data_root'], p)

    # Redirects to resolve
    page_redirect_path = root_to(config['gen']['page_redirect'])

    # `redirect` table providing ID -> Title lookup
    redirect_path = root_to(config['gen']['redirect'])
    redirect_index_path = root_to(config['gen']['redirect_index'])

    # `page` table giving Title -> true ID lookup
    page_path = root_to(config['gen']['page_direct'])
    page_index_path = root_to(config['gen']['page_direct_index'])

    # Output file of resolved redirects
    resolved_path = root_to(config['gen']['page_redirect_resolved'])

    chunk_size = 2**20  # uses 1MB chunks

    with atomic_write(resolved_path, encoding='utf-8') as out, \
         BinarySearchFile(redirect_path, index=redirect_index_path) as rd, \
         BinarySearchFile(page_path, index=page_index_path) as page, \
         open(page_redirect_path, encoding='utf-8') as rpage:

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


def merge_page_tables(config):
    root = config['data_root']
    res_in = os.path.join(root, config['gen']['page_redirect_resolved'])
    page_direct = os.path.join(root, config['gen']['page_direct'])
    page_out = os.path.join(root, config['gen']['page'])

    # sort resolved
    with TemporaryDirectory() as temp:
        res_sorted = os.path.join(temp, 'res_sorted')
        external_sort(res_in,
                      os.path.join(temp, res_sorted),
                      n_bytes=2**config['free_memory'])

        files = [page_direct, res_sorted]
        k_way_merge(files, page_out)


if __name__ == '__main__':
    config = load_config('config/pi.yaml')
    # root = config['data_root']
    # source = os.path.join(root, config['gen']['page_direct'])
    # out = os.path.join(root, 'gen/page_index.csv')
    resolve_redirects(config)
