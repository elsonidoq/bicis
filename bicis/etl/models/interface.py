import json
import os

import luigi
from luigi.contrib.spark import PySparkTask
from luigi.util import inherits
from pyspark.ml.regression import GeneralizedLinearRegression, GeneralizedLinearRegressionModel
from pyspark.sql import SparkSession
from sklearn.metrics import mean_squared_error

from bicis.etl.feature_extraction.build_dataset import BuildDataset
from bicis.lib.data_paths import data_dir
from bicis.lib.object_loader import object_loader
from bicis.lib.utils import get_logger, load_csv_dataframe

logger = get_logger(__name__)

class FitModelTask(PySparkTask):
    model_name = luigi.Parameter()

    def requires(self):
        return BuildDataset('training')

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                data_dir,
                object_loader.experiment_name,
                '{}/model'.format(self.model_name)
            )
        )

# The code assumes that all subclasses of PredictTask inherits from the compatible version of FitModelTask
# see poisson_regression.py
@inherits(FitModelTask)
class PredictTask(PySparkTask):
    dataset_type = luigi.ChoiceParameter(choices=['training', 'testing', 'validation'])

    @property
    def input_dataset_task(self):
        # This property is just to reuse if on the requires method and on the main method
        return BuildDataset(self.dataset_type)

    def requires(self):
        return [self.clone_parent(), self.input_dataset_task]

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                data_dir,
                object_loader.experiment_name,
                '{}/predictions/{}'.format(self.model_name, self.dataset_type)
            )
        )

    def main(self, sc, *args):
        raise NotImplementedError()
