import csv
import os

import pandas as pd
from atomicwrites import atomic_write
from tqdm import tqdm

from processing.bsearch import BinarySearchFile


def extract_pagelinks_columns(config):
    root = config['data_root']
    source = os.path.join(root, 'pagelinks', 'pagelinks.csv')
    out = os.path.join(root, config['gen']['pagelinks_unresolved'])
    names = config['tables']['pagelinks']['names']

    extract_columns = ['pl_title', 'pl_from']
    chunksize = 10**6  # Measured in lines, not bytes

    page_it = pd.read_csv(source,
                          chunksize=chunksize,
                          names=names,
                          dtype=object,
                          engine='c',
                          encoding='utf-8')
    page_it = tqdm(page_it, unit_scale=True)
    with atomic_write(out, overwrite=True, encoding='utf-8') as fp:
        for ch in page_it:
            # We will use only links between main namespace pages (i.e. content)
            from_main = ch['pl_from_namespace'] == '0'
            is_main = ch['pl_namespace'] == '0'
            ch = ch[from_main & is_main]
            ch[extract_columns].to_csv(fp, index=False, header=False, mode='a')


def resolve_pagelinks(config):
    root_to = lambda x: os.path.join(config['data_root'], x)
    page_path = root_to(config['gen']['page'])
    pagelinks_path = root_to(config['gen']['pagelinks_unresolved'])
    out_path = root_to(config['gen']['pagelinks'])

    pagelinks_it = pd.read_csv(pagelinks_path,
                               header=None,
                               chunksize=10**6,
                               engine='c',
                               encoding='latin-1')
    pagelinks_it = tqdm(pagelinks_it, desc='Resolving pagelinks')

    k_old, v_old = None, None
    with atomic_write(out_path, mode='w') as out:
        with BinarySearchFile(page_path) as page:
            out_csv = csv.writer(out)
            for ch in tqdm(pagelinks_it):
                rows = []
                for i in range(ch.shape[0]):
                    k = bytes(ch.iloc[i, 0], 'utf-8')
                    if k == k_old:
                        v = v_old
                    else:
                        k_old = k
                        v = page.search(k)[0]
                        v_old = v

                    if v:
                        rows.append([int(v), int(ch.iloc[i, 1])])
                out_csv.writerows(rows)
