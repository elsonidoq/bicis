import zipfile

import os
from distutils.spawn import find_executable
from urllib import urlopen

import luigi
from luigi.contrib.external_program import ExternalProgramTask

from bicis.lib.data_paths import scripts_dir, trajectories_dir, stations_dir, data_dir


class FetchFileTask(luigi.Task):
    url = luigi.Parameter()
    dst_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.dst_path)

    def run(self):
        with self.output().open('w') as f:
            f.write(urlopen(self.url).read())

class DownloadRawData(luigi.Task):
    """
    Downloads the trajectories and stations data from https://data.buenosaires.gob.ar/dataset/bicicletas-publicas
    """
    def requires(self):
        # TODO: add this is necesary https://data.buenosaires.gob.ar/api/datasets/rk7pYtZQke/download
        return FetchFileTask(
                'https://data.buenosaires.gob.ar/api/datasets/HJ8rdKWmJl/download',
                os.path.join(data_dir, 'raw', 'trajectories.zip')
            )


    def output(self):
        return luigi.LocalTarget(os.path.join(data_dir, 'raw', 'bicicletas-publicas'))

    def run(self):
        zip = zipfile.ZipFile(self.input().path, 'r')
        zip.extractall(os.path.join(data_dir, 'raw'))
        zip.close()


