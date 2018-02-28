import luigi
from luigi.util import inherits
from pyspark.ml.regression import GeneralizedLinearRegression, GeneralizedLinearRegressionModel
from pyspark.sql import SparkSession

from bicis.etl.models.interface import PredictTask, FitModelTask
from bicis.lib.utils import get_logger

logger = get_logger(__name__)


class FitPoissonRegression(FitModelTask):
    iterations = luigi.IntParameter(default=1)
    link = luigi.ChoiceParameter(default='identity', choices=["log", "identity", "sqrt"])
    model_name = 'poisson_regression'

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

    def load_model(self, sc):
        return GeneralizedLinearRegressionModel.load(self.output().path)


@inherits(FitPoissonRegression)
class PredictWithPoissonRegression(PredictTask):
    model_name = FitPoissonRegression.model_name

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


