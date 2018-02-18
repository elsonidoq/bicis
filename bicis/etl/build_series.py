import os
from functools import partial

from pyspark.sql import Row
import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession

from bicis.etl.unify_raw_data import UnifyRawData
from bicis.lib.data_paths import data_dir


class BuildAllSeries(luigi.WrapperTask):
    """
    Builds series for all keys
    """

    def requires(self):
        res = []
        for key in SeriesBuilder.key._choices:
            res.append(SeriesBuilder(key=key))
        return res

class SeriesBuilder(PySparkTask):
    """
    Builds a series for each station.
    :param key: Determines the x axis of the series.

    Outputs a csv file with this columns: [station, <key>, n_rents, n_returns]
    """
    key = luigi.ChoiceParameter(choices=['weekday', 'hour', 'month'])

    def output(self):
        return luigi.LocalTarget(os.path.join(data_dir, 'rents_by_{}.csv'.format(self.key)))

    def requires(self):
        return UnifyRawData()

    def main(self, sc, *args):
        spark_sql = SparkSession.builder.getOrCreate()

        general_df = (
            spark_sql
                .read
                .load(
                    self.input().path,  # .replace('.csv', '_sample.csv'),
                    format="csv",
                    sep=",",
                    inferSchema="true",
                    header="true")
                .rdd
                .map(partial(_add_keys, key=self.key))
                .toDF()
        )


        n_rents = (
            general_df
            .groupBy('rent_station', 'group_by', self.key)
            .count()
            .groupBy('rent_station', self.key)
            .mean('count')
            .withColumnRenamed('avg(count)', 'n_rents')
            .withColumnRenamed('rent_station', 'station')
        )

        n_returns = (
            general_df
            .groupBy('return_station', 'group_by', self.key)
            .count()
            .groupBy('return_station', self.key)
            .mean('count')
            .withColumnRenamed('avg(count)', 'n_returns')
            .withColumnRenamed('return_station', 'station')
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
        # TODO: hacer que _translate_doc tome el key
        # y le meta al doc una key 'group_by' y 'resulting_key'
        # if key == 'day_of_month':
        #     res['group_by'] = doc[date_field].date()
        #     res['resulting_key'] = doc[date_field].
        #
        # res[date_field + '_day_of_month'] = doc[date_field].date()

        if key == 'weekday':
            res['group_by'] = doc[date_field].isocalendar()[:2]
            res[key] = doc[date_field].isoweekday()
        elif key == 'month':
            res['group_by'] = doc[date_field].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            res[key] = doc[date_field].month
        elif key == 'hour':
            res['group_by'] = doc[date_field].replace(minute=0, second=0, microsecond=0)
            res[key] = doc[date_field].hour

    return Row(**res)


if __name__ == '__main__':
    luigi.run(main_task_cls=SeriesBuilder)
