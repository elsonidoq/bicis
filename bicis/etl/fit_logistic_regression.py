import os

import luigi
from luigi.contrib.spark import PySparkTask
from luigi.util import inherits
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel

from bicis.etl.build_dataset import BuildDataset
from bicis.lib.data_paths import data_dir
from bicis.lib.utils import get_logger

logger = get_logger(__name__)


class FitLogisticRegression(PySparkTask):
    dataset_config = luigi.Parameter()
    iterations = luigi.IntParameter(default=1)

    def requires(self):
        return BuildDataset(self.dataset_config, 'training')

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                data_dir,
                self.requires().experiment_name,
                'logistic_regression/model'
            )
        )


    def main(self, sc, *args):
        points_rdd = self.requires().get_points_rdd(sc)

        numClasses = (
            points_rdd
            .map(lambda x:(x.label, 1))
            .groupByKey()
            .count()
        )
        model = LogisticRegressionWithLBFGS.train(
            points_rdd,
            self.iterations,
            numClasses=numClasses
        )
        model.save(sc, self.output().path)

    def load_model(self, sc):
        return LogisticRegressionModel.load(sc, self.output().path)


@inherits(FitLogisticRegression)
class PredictWithLogisticRegression(PySparkTask):
    dataset_type = luigi.ChoiceParameter(choices=['training', 'testing', 'validation'])

    @property
    def input_dataset_task(self):
        return BuildDataset(self.dataset_config, self.dataset_type)

    def requires(self):
        return [self.clone_parent(), self.input_dataset_task]

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                data_dir,
                self.input_dataset_task.experiment_name,
                'logistic_regression/predictions/{}'.format(self.dataset_type)
            )
        )

    def main(self, sc, *args):
        points_rdd = self.input_dataset_task.get_points_rdd(sc)

        model = self.clone_parent().load_model(sc)
        (
            model
            .predict(points_rdd.map(lambda x:x.features))
            .saveAsTextFile(self.output().path)
        )


