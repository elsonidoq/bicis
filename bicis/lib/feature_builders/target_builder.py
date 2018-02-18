from bicis.lib.utils import get_logger
logger = get_logger(__name__)

from bicis.etl.next_window_target import NextWindowTarget
from bicis.lib.feature_builders.base_builders import FeatureBuilder

import redis
from pyspark.sql import SparkSession

redis_client = redis.StrictRedis()


class TargetFeatureBuilder(FeatureBuilder):

    # TODO: check whether it makes sense to keep the `mode` parameter
    def __init__(self, mode='rent', window='1h'):
        """
        :param mode: whether to use the `rent` fields or the `return` fields on the raw_doc
        """
        self.window = window
        self.mode = mode
        super(TargetFeatureBuilder, self).__init__()

    def get_features(self, raw_doc):
        return {'target': redis_client.get(self._get_key_field(raw_doc.id))}

    def requires(self):
        return NextWindowTarget(mode=self.mode, window=self.window)

    def ensure_structure(self):
        if redis_client.get('TargetFeatureBuilder.done'):
            return

        spark_sql = SparkSession.builder.getOrCreate()
        input_fname = self.requires().output().path

        rows_iterator = (spark_sql
            .read
            .load(
                input_fname,
                format="csv",
                sep=",",
                inferSchema="true",
                header="true")
        ).toLocalIterator()

        for row in rows_iterator:
            redis_client.set(
                self._get_key_field(row.id),
                row[self.requires().output_field]
            )

        redis_client.set('TargetFeatureBuilder.done', 1)

    def _get_key_field(self, id):
        return '{}_id={}'.format(type(self), id)
