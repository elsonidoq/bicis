import json
import os

import luigi
import pandas as pd
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as max_

from bicis.etl.raw_data.unify import UnifyRawData
from bicis.lib.data_paths import data_dir


class DatasetSplitter(PySparkTask):
    # These are held contant to "ensure" reproducibility
    validation_period = '90D'
    test_period = '90D'


    def requires(self):
        return UnifyRawData()

    def output(self):
        return {
            'training': luigi.LocalTarget(os.path.join(data_dir, 'unified/training.csv')),
            'validation': luigi.LocalTarget(os.path.join(data_dir, 'unified/validation.csv')),
            'testing': luigi.LocalTarget(os.path.join(data_dir, 'unified/testing.csv')),
            'metadata': luigi.LocalTarget(os.path.join(data_dir, 'unified/split_metadata.json'))
        }

    def main(self, sc, *args):
        spark_sql = SparkSession.builder.getOrCreate()

        raw_data = self.requires().load_dataframe(spark_sql)

        max_dates = (
            raw_data
            .groupBy()
            .agg(max_('rent_date'), max_('return_date'))
            .first()
        )
        max_date = min(max_dates.asDict().values())

        testing_end_date = max_date
        validation_end_date = testing_start_date = testing_end_date - pd.Timedelta(self.test_period).to_pytimedelta()
        training_end_date = validation_start_date = validation_end_date - pd.Timedelta(self.validation_period).to_pytimedelta()

        if not self.output()['training'].exists():
            (
                raw_data
                .filter(raw_data.rent_date < training_end_date)
                .write
                .csv(self.output()['training'].path, header='true')
            )

        if not self.output()['validation'].exists():
            (
                raw_data
                .filter(raw_data.rent_date >= validation_start_date)
                .filter(raw_data.rent_date < validation_end_date)
                .write
                .csv(self.output()['validation'].path, header='true')
            )

        if not self.output()['testing'].exists():
            (
                raw_data
                    .filter(raw_data.rent_date >= testing_start_date)
                    .filter(raw_data.rent_date <= testing_end_date)
                    .write
                    .csv(self.output()['testing'].path, header='true')
            )

        with self.output()['metadata'].open('w') as f:
            json.dump(
                {
                    'training_end_date': training_end_date.isoformat(),
                    'validation_start_date': validation_start_date.isoformat(),
                    'validation_end_date': validation_end_date.isoformat(),
                    'testing_start_date': testing_start_date.isoformat(),
                    'testing_end_date': testing_end_date.isoformat(),
                },
                f,
                indent=2,
            )




