import os

import pandas as pd
from atomicwrites import atomic_write
from tqdm import tqdm


def extract_pagelinks_columns(config):
    root = config['data_root']
    source = os.path.join(root, 'pagelinks', 'pagelinks.csv')
    out = os.path.join(root, config['gen']['pagelinks_unresolved'])
    names = config['tables']['pagelinks']['names']

    extract_columns = ['pl_from', 'pl_title']
    chunksize = 10**6  # measured in lines, not bytes

    page_it = pd.read_csv(
        source, chunksize=chunksize, names=names, dtype=str, engine='c')
    page_it = tqdm(page_it, unit_scale=chunksize)

    with atomic_write(out, overwrite=True) as fp_pagelinks:
        for ch in page_it:
            # We will use only links between main namespace pages (i.e. content)
            ch = ch[(ch['pl_from_namespace'] == '0')
                    & (ch['pl_namespace'] == '0')]

            ch[extract_columns].to_csv(
                fp_pagelinks, index=False, header=False, mode='a')
