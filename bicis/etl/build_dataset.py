from bson import json_util
from pyspark import StorageLevel

from bicis.etl.split_raw_data import DatasetSplitter
from bicis.lib.utils import get_logger, load_csv_dataframe

logger = get_logger(__name__)

import json
import os

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession

from bicis.etl.unify_raw_data import UnifyRawData
from bicis.lib.data_paths import data_dir
from bicis.lib.object_loader import ObjectLoader


class BuildAllDatasets(luigi.WrapperTask):
    config_fname = luigi.Parameter()

    def requires(self):
        return [
            BuildDataset(self.config_fname, 'training'),
            BuildDataset(self.config_fname, 'validation'),
            BuildDataset(self.config_fname, 'testing'),
        ]


class BuildDataset(PySparkTask):
    config_fname = luigi.Parameter()
    dataset_type = luigi.ChoiceParameter(choices=['training', 'testing', 'validation'])

    @property
    def object_loader(self):
        return ObjectLoader.from_yaml(self.config_fname)

    @property
    def feature_builder(self):
        return self.object_loader.get('features_builder')

    @property
    def target_builder(self):
        """
        Returns the builder for the target class, what you want to predict
        """
        return self.object_loader.get('target_builder')

    def requires(self):
        return {
            # Each feature builder depends on different datasets,
            # thus this requires is delegated to the specified feature builder
            'builder_requirements': self.feature_builder.requires(),
            'target_requirements': self.target_builder.requires(),
            'raw_data': DatasetSplitter()
        }

    def output(self):
        experiment_name = os.path.basename(self.config_fname.replace('.yaml', ''))
        fname = os.path.join(experiment_name, self.dataset_type)
        fname_prefix = os.path.join(data_dir, fname)
        return {
            'dataset': luigi.LocalTarget(fname_prefix + '_dataset.csv'),
            'fails': luigi.LocalTarget(fname_prefix + '.fails')
        }

    def main(self, sc, *args):
        logger.info('Starting building features')
        spark_sql = SparkSession.builder.getOrCreate()

        input_df = self.get_input_df(spark_sql)
        features_rdd = self.get_features_rdd(input_df)
        target_rdd = self.get_target_rdd(input_df)

        full_dataset_rdd = (
            features_rdd
            .join(target_rdd)
            # there are some rows without features
            # TODO: fix this
            .filter(lambda x: x[1][0] is not None)

            .map(lambda (id, (features, target)): build_doc(id, features, target))
            .toDF()
        )

        if not self.output()['dataset'].exists():
            logger.info('Saving dataset')

            full_dataset_rdd.write.csv(self.output()['dataset'].path, header='true')

        if not self.output()['fails'].exists():
            logger.info('Collecting some fails')

            output_count = full_dataset_rdd.count()
            input_count = input_df.count()
            error_ids = (
                features_rdd
                .filter(lambda x:x[1] is None)
                .map(lambda x: x[0])
                .take(100)
            )

            with self.output()['fails'].open('w') as f:
                json.dump(
                    {
                        'input_count': input_count,
                        'output_count': output_count,
                        'number_of_errors': input_count - output_count,
                        'error_ids': error_ids
                    },
                    f, indent=2,
                    # used to make datetime serializable
                    default=json_util.default
                )

        logger.info('Done')

    def get_target_rdd(self, input_df):
        self.target_builder.ensure_structure()
        target_rdd = (
            input_df
                # There are some null rows
                # TODO: check whether this is a parsing error
                .filter(input_df.rent_station.isNotNull())
                .rdd
                .map(lambda x: (x['id'], self.target_builder.get_features(x)))
                .persist(StorageLevel.DISK_ONLY)
        )
        return target_rdd

    def get_features_rdd(self, input_df):
        self.feature_builder.ensure_structure()
        features_rdd = (
            input_df
                # There are some null rows
                # TODO: check whether this is a parsing error
                .filter(input_df.rent_station.isNotNull())
                .rdd
                .map(lambda x: (x['id'], self.feature_builder.get_features(x)))
                .persist(StorageLevel.DISK_ONLY)
        )
        return features_rdd

    def get_input_df(self, spark_sql):
        return load_csv_dataframe(spark_sql, self.input()['raw_data'][self.dataset_type].path)


def build_doc(id, features_doc, target_doc):
    """

    :param id: The id generated by UnifyRawData
    :param features_doc: Features generated by the feature builder
    :param target_doc: Target doc, must output only a "target" key
    """


    res = {
        'id': id,
        'target': target_doc['target']
    }
    res.update(features_doc.asDict())
    return res


if __name__ == '__main__':
    luigi.run(main_task_cls=BuildAllDatasets)
