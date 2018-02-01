import csv
import os
from glob import glob
import luigi
from tqdm import tqdm

from bicis.lib.data_paths import data_dir
from bicis.lib.parse_raw_data import iter_fname


class UnifyRawData(luigi.Task):

    def output(self):
        return luigi.LocalTarget(os.path.join(data_dir, 'unified_data.csv'))

    def run(self):
        fnames = glob(os.path.join(data_dir, 'bicicletas-publicas/recorridos-realizados-*csv'))

        writer = None
        with self.output().open('w') as out_stream:
            for i, fname in enumerate(tqdm(fnames, desc='files to process')):
                for doc in tqdm(iter_fname(fname), desc='processing rows'):

                    if writer is None:
                        writer = csv.DictWriter(out_stream, doc.keys())
                        writer.writeheader()

                    for date_field in 'rent_date return_date'.split():
                        if date_field not in doc: break
                        doc[date_field] = doc[date_field].strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # only write docs with both date fields
                        writer.writerow(doc)


if __name__ == '__main__':
    luigi.run(main_task_cls=UnifyRawData)
