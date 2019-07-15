import os

import luigi
import yaml

import preparation.download_extract
import processing.id_by_title
import processing.pagelinks
from processing.sort import external_sort


def load_config(config_path):
    """Loads YAML config"""
    with open(config_path) as f:
        return yaml.load(f, Loader=yaml.SafeLoader)


class DownloadZippedTable(luigi.Task):
    config_path = luigi.Parameter()
    table = luigi.Parameter()

    def output(self):
        config = load_config(self.config_path)
        folder = os.path.join(config['data_root'], self.table)
        name = '-'.join(
            ['enwiki', str(config['data_date']), self.table + '.sql.gz'])
        return luigi.LocalTarget(os.path.join(folder, name))

    def run(self):
        config = load_config(self.config_path)
        folder = os.path.join(config['data_root'], self.table)
        if not os.path.exists(folder):
            os.makedirs(folder)
        preparation.download_extract.download_table(config, self.table)


class UnzipTable(luigi.Task):
    config_path = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return DownloadZippedTable(config_path=self.config_path,
                                   table=self.table)

    def output(self):
        config = load_config(self.config_path)
        folder = os.path.join(config['data_root'], self.table)
        name = '-'.join(
            ['enwiki', str(config['data_date']), self.table + '.sql'])
        return luigi.LocalTarget(os.path.join(folder, name))

    def run(self):
        config = load_config(self.config_path)
        preparation.download_extract.unzip_table(config, self.table)


class SqlDumpToCsv(luigi.Task):
    config_path = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return UnzipTable(config_path=self.config_path, table=self.table)

    def output(self):
        config = load_config(self.config_path)
        file_name = os.path.join(config['data_root'], self.table,
                                 self.table + '.csv')
        return luigi.LocalTarget(file_name)

    def run(self):
        config = load_config(self.config_path)

        folder = os.path.join(config['data_root'], self.table)
        sql = '-'.join(['enwiki', str(config['data_date']), self.table + '.sql'])
        path_in = os.path.join(folder, sql)
        path_out = os.path.join(folder, self.table + '.csv')

        preparation.download_extract.sql_dump_to_csv(path_in, path_out)


class AllTablesAsCsv(luigi.Task):
    config_path = luigi.Parameter()

    def requires(self):
        config = load_config(self.config_path)
        tables = [t for t in config['tables'].keys()]

        tasks = []
        for table in tables:
            task = SqlDumpToCsv(config_path=self.config_path, table=table)
            tasks.append(task)

        return tasks

    def run(self):
        pass

    def complete(self):
        # TODO: work out why this function is required when dependencies are met
        # This task is complete if its dependencies are met
        return True


class ExtractPageColumns(luigi.Task):
    config_path = luigi.Parameter()

    def requires(self):
        return AllTablesAsCsv(config_path=self.config_path)

    def output(self):
        config = load_config(self.config_path)
        root = config['data_root']
        page = os.path.join(root, config['gen']['page_direct_unsorted'])
        redirects = os.path.join(root, config['gen']['page_redirect_unsorted'])

        return [luigi.LocalTarget(page), luigi.LocalTarget(redirects)]

    def run(self):
        config = load_config(self.config_path)
        processing.id_by_title.extract_page_columns(config)


class SortRedirectTable(luigi.Task):
    config_path = luigi.Parameter()

    def requires(self):
        return ExtractPageColumns(config_path=self.config_path)

    def output(self):
        config = load_config(self.config_path)
        root = config['data_root']
        return luigi.LocalTarget(
            os.path.join(root, config['gen']['page_redirect']))

    def run(self):
        config = load_config(self.config_path)
        root = config['data_root']
        n_bytes = 2 ** config['free_memory']
        temp = config['temp_dir']

        unsorted = os.path.join(root, config['gen']['page_redirect_unsorted'])
        out = os.path.join(root, config['gen']['page_redirect'])
        external_sort(unsorted, out, n_bytes=n_bytes, temp_dir=temp)


class ExtractRedirectColumns(luigi.Task):
    config_path = luigi.Parameter()

    def requires(self):
        return AllTablesAsCsv(config_path=self.config_path)

    def output(self):
        config = load_config(self.config_path)
        root = config['data_root']
        return luigi.LocalTarget(os.path.join(root, config['gen']['redirect']))

    def run(self):
        config = load_config(self.config_path)
        processing.id_by_title.extract_redirect_columns(config)


class SortPageTable(luigi.Task):
    config_path = luigi.Parameter()

    def requires(self):
        return ExtractPageColumns(config_path=self.config_path)

    def output(self):
        config = load_config(self.config_path)
        root = config['data_root']
        return luigi.LocalTarget(os.path.join(root, config['gen']['page_direct']))

    def run(self):
        config = load_config(self.config_path)
        root = config['data_root']
        n_bytes = 2 ** config['free_memory']
        temp = config['temp_dir']

        unsorted = os.path.join(root, config['gen']['page_direct_unsorted'])
        out = os.path.join(root, config['gen']['page_direct'])
        external_sort(unsorted, out, n_bytes=n_bytes, temp_dir=temp)


class ResolveRedirects(luigi.Task):
    config_path = luigi.Parameter()

    def requires(self):
        tasks = [
            SortPageTable(config_path=self.config_path),
            SortRedirectTable(config_path=self.config_path),
            ExtractRedirectColumns(config_path=self.config_path),
        ]
        return tasks

    def output(self):
        config = load_config(self.config_path)
        root = config['data_root']
        return luigi.LocalTarget(
            os.path.join(root, config['gen']['page_redirect_resolved']))

    def run(self):
        config = load_config(self.config_path)
        processing.id_by_title.resolve_redirects(config)


class MergePage(luigi.Task):
    config_path = luigi.Parameter()

    def requires(self):
        return ResolveRedirects(config_path=self.config_path)

    def output(self):
        config = load_config(self.config_path)
        root = config['data_root']
        return luigi.LocalTarget(os.path.join(root, config['gen']['page']))

    def run(self):
        config = load_config(self.config_path)
        processing.id_by_title.merge_page_tables(config)


class ExtractPagelinksColumns(luigi.Task):
    config_path = luigi.Parameter()

    def requires(self):
        return AllTablesAsCsv(config_path=self.config_path)

    def output(self):
        config = load_config(self.config_path)
        root = config['data_root']
        return luigi.LocalTarget(
            os.path.join(root, config['gen']['pagelinks_unresolved']))

    def run(self):
        config = load_config(self.config_path)
        processing.pagelinks.extract_pagelinks_columns(config)


if __name__ == '__main__':
    luigi.build([SortRedirectTable(config_path='config/pi.yaml')],
                local_scheduler=True)
