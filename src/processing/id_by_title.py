import os

import pandas as pd
import yaml
from atomicwrites import atomic_write
from tqdm import tqdm


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


def resolve_redirects(config):
    root = config['data_root']
    resolved_path = os.path.join(root, config['gen']['page_redirect_resolved'])
    unresolved_path = os.path.join(root, config['gen']['page_redirect'])
    redirect_path = os.path.join(root, 'redirect', 'redirect.csv')

    with atomic_write(resolved_path) as unresolved_path, \
         open(unresolved_path) as unresolved, \
         open(redirect_path) as redirect:
        pass
        
    # get from_title and from_id from page.csv
    # e.g. 'AccessibleComputing',10

    # go to from_id and to_title from redirects.csv
    # e.g. 10,0,'Computer_accessibility','',''

    # get to_id from page.csv

    # set from_title in page.csv to correct to_id, so searching on that title
    # gives the id of the 'to' page.


if __name__ == '__main__':
    config = load_config('config/pi.yaml')
    root = config['data_root']
    source = os.path.join(root, config['gen']['page_direct'])
    out = os.path.join(root, 'gen/page_index.csv')