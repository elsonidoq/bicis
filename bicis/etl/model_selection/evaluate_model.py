import json
import os

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession

from bicis.lib.data_paths import data_dir
from bicis.lib.object_loader import object_loader, obj_from_path
from bicis.lib.utils import load_csv_dataframe


class ModelEvaluationTask(PySparkTask):
    dataset_type = luigi.Parameter()

    def requires(self):
        return object_loader.get('prediction_task', dataset_type=self.dataset_type)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                data_dir,
                object_loader.experiment_name,
                '{}/evaluation/{}.json'.format(self.requires().model_name, self.dataset_type)
            )
        )

    @property
    def metrics(self):
        return map(obj_from_path, object_loader.get('metrics'))

    def main(self, sc, *args):
        spark_sql = SparkSession.builder.getOrCreate()
        df = load_csv_dataframe(spark_sql, self.input().path).toPandas()

        res = {}
        for metric in self.metrics:
            res[metric.__name__] = metric(df.label, df.prediction)

        with self.output().open('w') as f:
            json.dump(res, f, indent=2)

