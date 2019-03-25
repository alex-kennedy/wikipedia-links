import os

import pandas as pd
import yaml
from atomicwrites import atomic_write
from tqdm import tqdm

from processing.sort import external_sort


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
    page_it = tqdm(page_it, unit_scale=chunksize)

    with atomic_write(page, overwrite=True) as fp_page:
        with atomic_write(redirects, overwrite=True) as fp_redirects:
            for ch in page_it:
                # We will use only main namespace pages (i.e. content)
                ch = ch[ch['page_namespace'] == '0']

                is_redirect = ch[ch['page_is_redirect'] == '1'][extract_columns]

                is_redirect.to_csv(
                    fp_redirects, index=False, header=None, mode='a')
                ch[extract_columns].to_csv(
                    fp_page, index=False, header=False, mode='a')


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
    # config = utils.load_config('config/pi.yaml')
    # extract_page_columns(config)
    # sort_on_title(config)
    pass