import os
from distutils.spawn import find_executable

import luigi
from luigi.contrib.external_program import ExternalProgramTask

from bicis.lib.data_paths import scripts_dir, trajectories_dir, stations_dir


class DownloadRawData(ExternalProgramTask):
    def program_args(self):
        return [
            find_executable('bash'),
            os.path.join(scripts_dir, 'get_data.sh')
        ]

    def output(self):
        return {
            'trajectories': luigi.LocalTarget(trajectories_dir),
            'estaciones': luigi.LocalTarget(stations_dir)
        }

    def program_environment(self):
        res = super(DownloadRawData, self).program_environment()
        res['PWD'] = scripts_dir
        return res

    def _clean_output_file(self, file_object):
        # There's a bug in luigi when the output really uses unicode
        res = super(DownloadRawData, self)._clean_output_file(file_object)
        return res.encode('utf8')



