from bson import json_util
from pyspark import StorageLevel, Row
from pyspark.ml.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from bicis.etl.raw_data.split import DatasetSplitter
from bicis.lib.utils import get_logger, load_csv_dataframe

logger = get_logger(__name__)

import json
import os

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession

from bicis.lib.data_paths import data_dir
from bicis.lib.object_loader import object_loader


class BuildAllDatasets(luigi.WrapperTask):
    def requires(self):
        return [
            BuildDataset('training'),
            BuildDataset('validation'),
            BuildDataset('testing'),
        ]



class BuildDataset(PySparkTask):
    dataset_type = luigi.ChoiceParameter(choices=['training', 'testing', 'validation'])

    @property
    def feature_builder(self):
        return object_loader.get('features_builder')

    @property
    def target_builder(self):
        """
        Returns the builder for the target class, what you want to predict
        """
        return object_loader.get('target_builder')

    def requires(self):
        return {
            # Each feature builder depends on different datasets,
            # thus this requires is delegated to the specified feature builder
            'builder_requirements': self.feature_builder.requirements(),
            'target_requirements': self.target_builder.requirements(),
            'raw_data': DatasetSplitter()
        }

    def output(self):
        fname = os.path.join(object_loader.experiment_name, self.dataset_type)
        fname_prefix = os.path.join(data_dir, fname)
        return {
            'dataset': luigi.LocalTarget(fname_prefix + '_dataset.csv'),
            'fails': luigi.LocalTarget(fname_prefix + '.fails')
        }

    def main(self, sc, *args):
        logger.info('Starting building features')
        spark_sql = SparkSession.builder.getOrCreate()

        input_df= self.get_input_df(spark_sql)
        features_rdd = self.get_features_rdd(input_df)
        target_rdd = self.get_target_rdd(input_df)

        full_dataset_df = (
            features_rdd
            .join(target_rdd)
            # Depending on the target, there might be some ids without target
            # however, there are some rows without features TODO: fix this
            .filter(lambda (id, (features, target)): features is not None and target is not None)

            .map(lambda (id, (features, target)): build_doc(id, features, target))
            .toDF()
        )

        if not self.output()['dataset'].exists():
            logger.info('Saving dataset')

            full_dataset_df.write.csv(self.output()['dataset'].path, header='true')

        if not self.output()['fails'].exists():
            logger.info('Collecting some fails')

            output_count = full_dataset_df.count()
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


    def get_points_rdd(self, sc, parser_class=None):
        """
        Returns an rdd pointing to this task output.
        It can parse the output for it to be a LabeledPoint or a Row.
        Depending on the model you use it has to be either one or the other

        :param parser_class: Defaults to RowParser
        :return:
        """
        parser_class = parser_class or RowParser

        input_rdd = sc.textFile(self.output()['dataset'].path)

        # Build parser for dataset
        # maybe use csv module?
        fields = input_rdd.take(1)[0].split(',')
        feature_indices = [
            i
            for i, f in enumerate(fields)
            if f != 'id' and f != 'target'
        ]
        target_index = fields.index('target')
        id_index = fields.index('id')
        parser = parser_class(feature_indices, target_index, id_index)

        points = (
            input_rdd
                .map(parser)
                .filter(lambda x: x is not None)
                .persist(StorageLevel.DISK_ONLY)
        )
        return points

# Helper code to load the output generated by the task and feed it to spark mlib

class ExampleParser(object):
    def __init__(self, feature_indices, target_index, id_index):
        self.id_index = id_index
        self.target_index = target_index
        self.feature_indices = feature_indices

    def __call__(self, line):
        # maybe use csv module?
        row = line.split(',')

        try:
            target = float(row[self.target_index])
        except ValueError, e:
            # most likely a header
            # TODO: filter the header before :P
            logger.error("invalid target: '{}'".format(row[self.target_index]))
            return

        features = [parse_string(row[i]) for i in self.feature_indices]
        id = row[self.id_index]

        return self._get_point(features, target, id)

    def _get_point(self, features, target, id):
        raise NotImplementedError()


class PointParser(ExampleParser):
    def _get_point(self, features, target, id):
        return LabeledPoint(target, features)

class RowParser(ExampleParser):
    def _get_point(self, features, target, id):
        return Row(label=target, features=Vectors.dense(features), id=id)

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
    return Row(**res)

def parse_string(s):
    if s.isdigit(): return int(s)
    else:
        try:
            return float(s)
        except Exception:
            logger.error("Couldn't parse '{}' into a number".format(s))
            return s



def shift_target(point):
    logger.warn("This is testing code, shouln't be used")
    point.label -= 1
    return point

if __name__ == '__main__':
    luigi.run(main_task_cls=BuildAllDatasets)
