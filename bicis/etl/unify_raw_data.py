import csv
import os
from glob import glob
import luigi
from tqdm import tqdm

from bicis.etl.get_raw_data import DownloadRawData
from bicis.lib.data_paths import data_dir, trajectories_dir
from bicis.lib.parse_raw_data import iter_fname
from bicis.lib.utils import load_csv_dataframe


class UnifyRawData(luigi.Task):
    """
    Reads the raw data from the heterogeneous format
    [detailed here](https://data.buenosaires.gob.ar/layout/H1giQXf7kx/preview) to these for columns
    id, rent_date, rent_station, return_date, return_station
    """

    def requires(self):
        return DownloadRawData()

    def output(self):
        return luigi.LocalTarget(os.path.join(data_dir, 'unified/full.csv'))

    def load_dataframe(self, spark_sql):
        return load_csv_dataframe(spark_sql, self.output().path)

    def run(self):
        fnames = glob(os.path.join(self.input().path, 'recorridos-realizados-*csv'))

        writer = None
        id = 0
        with self.output().open('w') as out_stream:
            for i, fname in enumerate(tqdm(fnames, desc='files to process')):
                for doc in tqdm(iter_fname(fname), desc='processing rows'):

                    if writer is None:
                        writer = csv.DictWriter(out_stream, doc.keys() + ['id'])
                        writer.writeheader()

                    for date_field in 'rent_date return_date'.split():
                        if date_field not in doc: break
                        doc[date_field] = doc[date_field].strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # only write docs with both date fields
                        doc['id'] = id
                        id += 1
                        writer.writerow(doc)



if __name__ == '__main__':
    luigi.run(main_task_cls=UnifyRawData)
