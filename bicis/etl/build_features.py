from bicis.lib.utils import get_logger
logger = get_logger(__name__)

import json
import os

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession

from bicis.etl.unify_raw_data import UnifyRawData
from bicis.lib.data_paths import data_dir
from bicis.lib.object_loader import ObjectLoader


class BuildFeaturesDataset(PySparkTask):
    features_config_fname = luigi.Parameter()

    @property
    def object_loader(self):
        return ObjectLoader.from_yaml(self.features_config_fname)

    @property
    def feature_builder(self):
        return self.object_loader.get('features_builder')

    def requires(self):
        return {
            # Each feature builder depends on different datasets,
            # thus this requires is delegated to the specified feature builder
            'builder_requirements': self.feature_builder.requires(),
            'raw_data': UnifyRawData()
        }

    def output(self):
        fname = os.path.basename(self.features_config_fname.replace('.yaml', ''))
        fname_prefix = os.path.join(data_dir, fname)
        return {
            'features': luigi.LocalTarget(fname_prefix + '.csv'),
            'fails': luigi.LocalTarget(fname_prefix + '.fails')
        }

    def main(self, sc, *args):
        logger.info('Starting building features')

        spark_sql = SparkSession.builder.getOrCreate()

        df = (
            spark_sql
                .read
                .load(
                self.input()['raw_data'].path,
                format="csv",
                sep=",",
                inferSchema="true",
                header="true"
            )
        )

        features_df = (
            df
            # There are some null rows
            # TODO: check whether this is a parsing error
            .filter(df.src_station.isNotNull())
            .rdd
            .map(lambda x: self.feature_builder.get_features(x['src_station'], x['rent_date']))
        )

        logger.info('Saving dataset')
        (
            features_df
            # Filter out the ones that failed
            .filter(lambda x: x is not None)
            .toDF()
            .write
            .csv(self.output()['features'].path, header='true')
        )

        logger.info('Collecting stats')
        # TODO: Is it possible to piggy back this computation on the previous one?
        output_count = (
                spark_sql
                .read
                .load(
                    self.output()['features'].path,
                    format="csv",
                    sep=",",
                    inferSchema="true",
                    header="true"
                )
        ).count()
        input_count = df.count()

        with self.output()['fails'].open('w') as f:
            json.dump(
                {
                    'input_count': input_count,
                    'output_count': output_count,
                    'number_of_errors': output_count - input_count
                },
                f, indent=2
            )

        logger.info('Done')

