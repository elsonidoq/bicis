from bicis.lib.utils import get_logger
logger = get_logger(__name__)

from bicis.etl.feature_extraction.next_window_target import NextWindowTarget
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
        super(TargetFeatureBuilder, self).__init__()
        self.window = window
        self.mode = mode

    def get_features(self, raw_doc):
        res = redis_client.get(self._get_key_field(raw_doc.id))
        return {'target': res}

    def requirements(self):
        return NextWindowTarget(mode=self.mode, window=self.window)

    def ensure_structure(self):
        done_key = 'TargetFeatureBuilder(mode={}, window={}).done'.format(self.mode, self.window)
        if redis_client.get(done_key):
            return

        spark_sql = SparkSession.builder.getOrCreate()
        input_fname = self.requirements().output().path

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
                row[self.requirements().output_field]
            )

        redis_client.set(done_key, 1)

    def _get_key_field(self, id):
        return '{}_id={}'.format(type(self), id)
