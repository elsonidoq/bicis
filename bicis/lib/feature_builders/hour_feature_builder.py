from bicis.etl.basic_features import BasicFeaturesBuilder
from bicis.lib.feature_builders.base_builders import FeatureBuilder
from bicis.lib.utils import get_logger
logger = get_logger(__name__)

from itertools import chain

import redis
from pyspark import Row
from pyspark.sql import SparkSession

from bicis.lib.utils import head

redis_client = redis.StrictRedis()


class HourFeaturesBuilder(FeatureBuilder):
    # TODO: check whether it makes sense to keep the `mode` parameter
    def __init__(self, mode='rent', window_size=24):
        """
        :param mode: whether to use the `rent` fields or the `return` fields on the raw_doc
        """
        self.window_size = window_size
        self.mode = mode

    def get_features(self, raw_doc):
        station = raw_doc[self.mode + '_station']
        timestamp = raw_doc[self.mode + '_date']
        hour = timestamp.hour

        # e.g. for hour=3 generates indices 2, 1, 0, 24, 23, ...
        indices = head(
            chain(
                xrange(hour-1, -1, -1),
                xrange(24-1, hour-1, -1)
            ),
            self.window_size
        )

        res = {}
        for i, hour in enumerate(indices):
            hour_data = redis_client.hgetall(self._get_station_hour_key(hour, station))
            try:
                res['n_rents_{}_hb'.format(i)] = float(hour_data['n_rents'])
                res['n_returns_{}_hb'.format(i)] = float(hour_data['n_returns'])
            except Exception, e:
                logger.warn(u'station {} on hour {} has a weird thing'.format(station, hour))
                return

        # XXX should it return the dict or the row?
        return Row(**res)

    def _get_station_hour_key(self, hour, station):
        return u'{}_{}'.format(station, hour)

    def requires(self):
        """
        Specifies what luigi task this feature extractod depends on
        """
        return BasicFeaturesBuilder(key='hour')

    def ensure_structure(self, force=False):
        if redis_client.get('HourFeaturesBuilder.done') and not force:
            return

        spark_sql = SparkSession.builder.getOrCreate()
        input_fname = BasicFeaturesBuilder(key='hour').output().path

        df = (spark_sql
            .read
            .load(
            input_fname,
            format="csv",
            sep=",",
            inferSchema="true",
            header="true")
        ).toPandas()

        n_rents_by_hour = df.pivot(index='station', columns='hour', values='n_rents')
        n_returns_by_hour = df.pivot(index='station', columns='hour', values='n_returns')

        # TODO: check whether the `fillna(0)` actually makes sense
        n_returns_by_hour = n_returns_by_hour.fillna(0).to_dict('index')
        for station, rent_data in n_rents_by_hour.fillna(0).to_dict('index').iteritems():
            returns_data = n_returns_by_hour[station]

            for hour, n_rents in rent_data.iteritems():
                doc = {
                    'n_rents': float(n_rents),
                    'n_returns': float(returns_data[hour])
                }

                redis_client.hmset(
                    self._get_station_hour_key(hour, station),
                    doc
                )


        redis_client.set('HourFeaturesBuilder.done', 1)
