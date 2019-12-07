import os
from os.path import join as pjoin

import luigi
import yaml

import bsearch
import download
import idbytitle
import pagelinks
from constants import Cols, Dat
from extsort import external_sort


class DownloadZippedTable(luigi.Task):
    root = luigi.Parameter()
    table = luigi.Parameter()

    def output(self):
        name = '-'.join(['enwiki', Dat.DATE, self.table + '.sql.gz'])
        return luigi.LocalTarget(pjoin(self.root, name))

    def run(self):
        download.download_table(self.root, self.table)


class UnzipTable(luigi.Task):
    table = luigi.Parameter()
    root = luigi.Parameter()

    def requires(self):
        return DownloadZippedTable(table=self.table, root=self.root)

    def output(self):
        name = '-'.join(['enwiki', Dat.DATE, self.table + '.sql'])
        return luigi.LocalTarget(pjoin(self.root, name))

    def run(self):
        download.unzip_table(self.root, self.table)


class SqlDumpToCsv(luigi.Task):
    table = luigi.Parameter()
    root = luigi.Parameter()

    def requires(self):
        return UnzipTable(table=self.table, root=self.root)

    def output(self):
        return luigi.LocalTarget(pjoin(self.root, self.table + '.csv'))

    def run(self):
        sql = '-'.join(['enwiki', Dat.DATE, self.table + '.sql'])
        path_in = os.path.join(self.root, sql)
        path_out = os.path.join(self.root, self.table + '.csv')
        download.sql_dump_to_csv(path_in, path_out)


class ExtractPageColumns(luigi.Task):
    def requires(self):
        return SqlDumpToCsv(table='page', root=Dat.ROOT_PAGE)

    def output(self):
        return [
            luigi.LocalTarget(Dat.PAGE_DIRECT_UNS),
            luigi.LocalTarget(Dat.PAGE_REDIRECT_UNS)
        ]

    def run(self):
        idbytitle.extract_page_columns()


class SortPageRedirectTable(luigi.Task):
    def requires(self):
        return ExtractPageColumns()

    def output(self):
        return luigi.LocalTarget(Dat.PAGE_REDIRECT)

    def run(self):
        external_sort(Dat.PAGE_REDIRECT_UNS,
                      Dat.PAGE_REDIRECT,
                      n_bytes=2**Dat.MEMORY)


class ExtractRedirectColumns(luigi.Task):
    def requires(self):
        return SqlDumpToCsv(root=Dat.ROOT_REDIRECT, table='redirect')

    def output(self):
        return luigi.LocalTarget(Dat.REDIRECT)

    def run(self):
        idbytitle.extract_redirect_columns()


class SortPageTable(luigi.Task):
    def requires(self):
        return ExtractPageColumns()

    def output(self):
        return luigi.LocalTarget(Dat.PAGE_DIRECT)

    def run(self):
        external_sort(Dat.PAGE_DIRECT_UNS,
                      Dat.PAGE_DIRECT,
                      n_bytes=2**Dat.MEMORY)


class IndexForResolveRedirects(luigi.Task):
    def requires(self):
        return [SortPageTable(), SortPageRedirectTable()]

    def output(self):
        return [
            luigi.LocalTarget(Dat.REDIRECT_I),
            luigi.LocalTarget(Dat.PAGE_DIRECT_I)
        ]

    def run(self):
        with bsearch.BinarySearchFile(Dat.REDIRECT, encoding='utf-8') as f:
            f.pickle_index(Dat.REDIRECT_I)

        with bsearch.BinarySearchFile(Dat.PAGE_DIRECT, encoding='utf-8') as f:
            f.pickle_index(Dat.PAGE_DIRECT_I)


class ResolveRedirects(luigi.Task):
    def requires(self):
        return [IndexForResolveRedirects(), ExtractRedirectColumns()]

    def output(self):
        return luigi.LocalTarget(Dat.PAGE_REDIRECT_RESOLVED)

    def run(self):
        idbytitle.resolve_redirects()


class MergePage(luigi.Task):
    def requires(self):
        return ResolveRedirects()

    def output(self):
        return luigi.LocalTarget(Dat.PAGE)

    def run(self):
        idbytitle.merge_page_tables()


class ExtractPagelinksColumns(luigi.Task):
    def requires(self):
        return SqlDumpToCsv(root=Dat.ROOT_PAGELINKS, table='pagelinks')

    def output(self):
        return luigi.LocalTarget(Dat.PAGELINKS_UNRESOLVED)

    def run(self):
        pagelinks.extract_pagelinks_columns()


class ResolvePagelinks(luigi.Task):
    def requires(self):
        return ExtractPagelinksColumns()

    def output(self):
        return luigi.LocalTarget(Dat.PAGELINKS)

    def run(self):
        pagelinks.resolve_pagelinks()


if __name__ == '__main__':
    luigi.build([ResolveRedirects()],
                local_scheduler=True)
