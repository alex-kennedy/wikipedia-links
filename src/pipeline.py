import os

import luigi
import yaml

import preparation.download_extract


def load_config(config_path):
    """Loads YAML config"""
    with open(config_path) as f:
        return yaml.load(f)


class DownloadZippedTable(luigi.Task):
    config_path = luigi.Parameter()
    table = luigi.Parameter()

    def output(self):
        config = load_config(self.config_path)
        folder = os.path.join(config['data_root'], self.table)
        name = '-'.join(['enwiki', str(config['data_date']), self.table + '.sql.gz'])
        return luigi.LocalTarget(os.path.join(folder, name))

    def run(self):
        pass


class UnzipTable(luigi.Task):
    config_path = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return DownloadZippedTable(config_path=self.config_path, table=self.table)

    def output(self):
        config = load_config(self.config_path)
        folder = os.path.join(config['data_root'], self.table)
        name = '-'.join(['enwiki', str(config['data_date']), self.table + '.sql'])
        return luigi.LocalTarget(os.path.join(folder, name))

    def run(self):
        pass


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
        path_in = os.path.join(folder, self.table, sql)
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


if __name__ == '__main__':
    luigi.build([AllTablesAsCsv(config_path='config/pi.yaml')],
                local_scheduler=True)
