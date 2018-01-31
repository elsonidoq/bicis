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
            for fname in tqdm(fnames, desc='files to process'):
                for doc in tqdm(iter_fname(fname), desc='processing rows'):
                    if writer is None:
                        writer = csv.DictWriter(out_stream, doc.keys())
                        writer.writeheader()
                    writer.writerow(doc)


if __name__ == '__main__':
    luigi.run(main_task_cls=UnifyRawData)
