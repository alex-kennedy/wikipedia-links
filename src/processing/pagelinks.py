import os

import pandas as pd
from atomicwrites import atomic_write
from tqdm import tqdm


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
                          engine='c',
                          dtype=object,
                          encoding='latin-1')
    page_it = tqdm(page_it, unit_scale=True)

    with atomic_write(out, overwrite=True) as fp_pagelinks:
        for ch in page_it:
            # We will use only links between main namespace pages (i.e. content)
            from_main = ch['pl_from_namespace'] == '0'
            is_main = ch['pl_namespace'] == '0'
            ch = ch[from_main & is_main]

            ch[extract_columns].to_csv(fp_pagelinks,
                                       index=False,
                                       header=False,
                                       mode='a')
