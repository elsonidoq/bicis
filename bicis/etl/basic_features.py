import os
from functools import partial

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import Row
from pyspark.sql import SparkSession

from bicis.etl.split_raw_data import DatasetSplitter
from bicis.lib.data_paths import data_dir
from bicis.lib.utils import load_csv_dataframe


class BasicFeaturesBuilder(PySparkTask):
    """
    Builds a series for each station that can be used as a feature vector.
    :param key: Determines the x axis of the series.

    Outputs a csv file with this columns: [station, <key>, avg_n_rents, avg_n_returns]
    Both `n_rents` and `n_returns` on the output csv are averages on the training data
    """
    key = luigi.ChoiceParameter(choices=['weekday', 'hour', 'month'])

    def output(self):
        return luigi.LocalTarget(os.path.join(data_dir, 'rents_by_{}.csv'.format(self.key)))

    def requires(self):
        return DatasetSplitter()

    def main(self, sc, *args):
        spark_sql = SparkSession.builder.getOrCreate()

        general_df = (
            load_csv_dataframe(spark_sql, self.requires().output()['training'].path)
            .rdd
            .map(partial(_add_keys, key=self.key))
            .toDF()
        )


        n_rents = (
            general_df
            .groupBy('rent_station', 'rent_group_by', 'rent_' + self.key)
            .count()
            .groupBy('rent_station', 'rent_' + self.key)
            .mean('count')
            .withColumnRenamed('avg(count)', 'n_rents')
            .withColumnRenamed('rent_station', 'station')
            .withColumnRenamed('rent_' + self.key, self.key)
        )

        n_returns = (
            general_df
            .groupBy('return_station', 'return_group_by', 'return_' + self.key)
            .count()
            .groupBy('return_station', 'return_' + self.key)
            .mean('count')
            .withColumnRenamed('avg(count)', 'n_returns')
            .withColumnRenamed('return_station', 'station')
            .withColumnRenamed('return_' + self.key, self.key)
        )

        (
            n_rents
            .join(n_returns, ['station', self.key])
            .write
            .csv(self.output().path, header='true')
        )


def _add_keys(doc, key):
    res = doc.asDict()
    for date_field in 'rent_date return_date'.split():
        prefix = date_field.split('_')[0]
        group_field = '{}_group_by'.format(prefix)
        key_field = '{}_{}'.format(prefix, key)

        if key == 'weekday':
            res[group_field] = doc[date_field].isocalendar()[:2]
            res[key_field] = doc[date_field].isoweekday()
        elif key == 'month':
            res[group_field] = doc[date_field].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            res[key_field] = doc[date_field].month
        elif key == 'hour':
            res[group_field] = doc[date_field].replace(minute=0, second=0, microsecond=0)
            res[key_field] = doc[date_field].hour

    return Row(**res)


if __name__ == '__main__':
    luigi.run(main_task_cls=BasicFeaturesBuilder)
