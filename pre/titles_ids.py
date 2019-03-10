import os
import pickle

import pandas as pd
from tqdm import tqdm

from utils import load_config

def hash_page_titles_partition(page_csv, names, f_r, part, max_part):
    titles = {}

    for c in tqdm(pd.read_csv(page_csv, chunksize=2**18, names=names, dtype='object')):
        # Write out redirects
        redirects = c['page_is_redirect'] == '1'
        c[redirects].to_csv(f_r, index=False, header=False)

        # Only consider rows in the correct hash partition
        part_i = c['page_title'].apply(lambda x: hash(x) & max_part) == part

        for page in c[~redirects & part_i].itertuples(index=False):
            titles[page[2]] = page[0]

    return titles


def hash_page_titles(config):
    page_redirect_true = os.path.join(config['data_root'], 'generated_files', config['generated_files']['page_redirect_true'])
    page_csv = os.path.join(config['data_root'], 'page', config['tables']['page']['out_csv'])
    names = config['tables']['page']['names']
    max_part = config['generated_files']['titles_map']['max_part']
    
    with open(page_redirect_true, 'w') as f_r:
        for part in range(max_part + 1):
            titles = hash_page_titles_partition(page_csv, names, f_r, part, max_part)

            pickle_path = os.path.join(config['data_root'], 'generated_files', 'titles_map', 'part{}.pickle'.format(part))
            with open(pickle_path, 'wb') as jar:
                pickle.dump(titles, jar)


if __name__ == '__main__':
    config = load_config('config/pi.yaml')
    hash_page_titles(config)