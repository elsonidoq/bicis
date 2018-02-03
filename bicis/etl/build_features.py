import os

from pyspark.sql import Row
import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession

from bicis.etl.build_series import SeriesBuilder
from bicis.etl.unify_raw_data import UnifyRawData
from bicis.lib.data_paths import data_dir


class BuildHourFeatures(PySparkTask):
    def main(self, sc, *args):
        spark_sql = SparkSession.builder.getOrCreate()

        df = (spark_sql
                .read
                .load(
                    self.input().path,
                    format="csv",
                    sep=",",
                    inferSchema="true",
                    header="true")
        )

        (
            df
            .groupBy('station')
            .map
        )

    def requires(self):
        return SeriesBuilder(key='hour')

