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
from bicis.lib.utils import get_logger, load_csv_dataframe

logger = get_logger(__name__)


class FitPoissonRegression(PySparkTask):
    dataset_config = luigi.Parameter()
    iterations = luigi.IntParameter(default=1)
    link = luigi.ChoiceParameter(default='identity', choices=["log", "identity", "sqrt"])

    def requires(self):
        return BuildDataset(self.dataset_config, 'training')

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                data_dir,
                self.requires().experiment_name,
                'poisson_regression/model'
            )
        )


    def main(self, sc, *args):
        points_rdd = self.requires().get_points_rdd(sc)

        model = GeneralizedLinearRegression(
            family='poisson',
            link=self.link,
            maxIter=self.iterations
        )

        spark_sql = SparkSession.builder.getOrCreate()
        model = model.fit(spark_sql.createDataFrame(points_rdd))

        model.save(self.output().path)
        self.load_model(sc)

    def load_model(self, sc):
        return GeneralizedLinearRegressionModel.load(self.output().path)


@inherits(FitPoissonRegression)
class PredictWithPoissonRegression(PySparkTask):
    dataset_type = luigi.ChoiceParameter(choices=['training', 'testing', 'validation'])

    @property
    def input_dataset_task(self):
        # This property is just to reuse if on the requires method and on the main method
        return BuildDataset(self.dataset_config, self.dataset_type)

    def requires(self):
        return [self.clone_parent(), self.input_dataset_task]

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                data_dir,
                self.input_dataset_task.experiment_name,
                'poisson_regression/predictions/{}'.format(self.dataset_type)
            )
        )

    def main(self, sc, *args):
        points_rdd = self.input_dataset_task.get_points_rdd(sc)
        model = self.clone_parent().load_model(sc)
        spark_sql = SparkSession.builder.getOrCreate()

        (
            model
            .transform(spark_sql.createDataFrame(points_rdd))
            .drop('features')
            .write.csv(self.output().path, header='true')
        )


@inherits(PredictWithPoissonRegression)
class SquaredLoss(PySparkTask):
    def requires(self):
        return self.clone_parent()

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                data_dir,
                self.requires().input_dataset_task.experiment_name,
                'poisson_regression/evaluation/{}.json'.format(self.dataset_type)
            )
        )

    def main(self, sc, *args):
        spark_sql = SparkSession.builder.getOrCreate()
        df = load_csv_dataframe(spark_sql, self.input().path).toPandas()

        with self.output().open('w') as f:
            json.dump(
                {
                    'mean_squared_error': mean_squared_error(df.label, df.prediction)
                }, f, indent=2
            )



