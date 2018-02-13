import pandas as pd
import os

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import Row
from pyspark.sql import SparkSession

from bicis.etl.unify_raw_data import UnifyRawData
from bicis.lib.data_paths import data_dir


class NextWindowTarget(PySparkTask):
    """
    Builds a series for each station.
    :param key: Determines the x axis of the series.

    Outputs a csv file with this columns: [id, cnt]
    """
    mode = luigi.ChoiceParameter(choices=['rent', 'return'])
    window = luigi.Parameter(default='1h')

    def output(self):
        return luigi.LocalTarget(os.path.join(data_dir, 'next_{}_{}s_by_station.csv'.format(self.window, self.mode)))

    def requires(self):
        return UnifyRawData()

    @property
    def station_field(self):
        return '{}_station'.format(self.mode)

    @property
    def timestamp_field(self):
        return '{}_date'.format(self.mode)

    @property
    def output_field(self):
        return 'n_{}s'.format(self.mode)

    def main(self, sc, *args):
        (
            SparkSession.builder.getOrCreate()
            .read.load(
                self.input().path.replace('.csv', '_10k.csv'),
                format="csv",
                sep=",",
                inferSchema="true",
                header="true")
            .rdd # TODO: check how to load and save RDDs directly
            # Group data by station
            .map(lambda x: (x[self.station_field], x))
            .groupByKey()
            # Compute target variable for each station
            .flatMap(lambda x: self._compute_targets(x[1]))
            # Dump to csv
            .toDF()
            .write.csv(self.output().path, header='true')
        )

    def _compute_targets(self, station_data):
        if not station_data: return []

        index = [x[self.timestamp_field] for x in station_data]
        df = pd.DataFrame(
            {
                'series': [1] * len(station_data),
                'id': [e['id'] for e in station_data]
            }
            ,index=index
        ).sort_index(ascending=False)

        # pandas does not support rolling opperations over non monotonic indices
        # so here we construct a monotonic index that respects the diffs between events
        # and allows us to go back and forth
        monotonic_index = [df.index[0]]
        for i, prev in enumerate(df.index[1:], 1):
            # The series is sorted in descending order
            next = df.index[i-1]

            monotonic_index.append(
                monotonic_index[-1] + (next - prev)
            )


        # consider only full windows
        max_date = df.series.index[0] - pd.Timedelta(self.window)

        mask = [timestamp <= max_date for timestamp in df.index]
        df['res'] = (
            pd.Series(df.series.values, index=monotonic_index)
                .rolling(self.window)
                .sum()
                .values
        )
        df = df[mask].set_index('id')

        res_data = df.res.to_dict()

        res = []
        for doc in station_data:
            if doc['id'] not in res_data: continue

            res.append(
                Row(**{
                    self.output_field: int(res_data[doc['id']]),
                    'id': doc['id']
                })
            )

        return res

if __name__ == '__main__':
    luigi.run(main_task_cls=NextWindowTarget)
