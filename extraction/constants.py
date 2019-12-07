class Dottable(dict):
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


Cols = Dottable(
    dict(
        # https://www.mediawiki.org/wiki/Manual:Pagelinks_table
        PAGELINKS=('pl_from', 'pl_namespace', 'pl_title', 'pl_from_namespace'),
        # https://www.mediawiki.org/wiki/Manual:Page_table
        PAGE=('page_id', 'page_namespace', 'page_title', 'page_restrictions',
              'page_is_redirect', 'page_is_new', 'page_random', 'page_touched',
              'page_links_updated', 'page_latest', 'page_len',
              'page_content_model', 'page_lang'),
        # https://www.mediawiki.org/wiki/Manual:Redirect_table
        REDIRECT=('rd_from', 'rd_namespace', 'rd_title', 'rd_interwiki',
                  'rd_fragment')))

ROOT = '/Users/alex/Documents/GitHub/wikipedia-links/'

Dat = Dottable(
    dict(
        REMOTE='https://dumps.wikimedia.org/enwiki/',
        MEMORY=28,  # 2**28 is 256MB
        DATE='20191001',
        ROOT=ROOT,
        ROOT_PAGE=ROOT + 'data/page/',
        ROOT_PAGELINKS=ROOT + 'data/pagelinks/',
        ROOT_REDIRECT=ROOT + 'data/redirect/',
        PAGE_DIRECT_UNS=ROOT + 'data/page_direct_unsorted.csv',
        PAGE_REDIRECT_UNS=ROOT + 'data/page_redirect_unsorted.csv',
        PAGE_DIRECT=ROOT + 'data/page_direct.csv',
        PAGE_DIRECT_I=ROOT + 'data/page_direct_index.bin',
        PAGE_REDIRECT=ROOT + 'data/page_redirect.csv',
        PAGE_REDIRECT_RESOLVED=ROOT + 'data/page_redirect_resolved.csv',
        REDIRECT=ROOT + 'data/redirect.csv',
        REDIRECT_I=ROOT + 'data/redirect_index.bin',
        PAGE=ROOT + 'data/page.csv',
        PAGELINKS_UNRESOLVED=ROOT + 'data/pagelinks_unresolved.csv',
        PAGELINKS=ROOT + 'data/pagelinks.csv'))
