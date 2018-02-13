from abc import abstractmethod, ABCMeta

from bicis.lib.utils import get_logger
logger = get_logger(__name__)

from itertools import chain

import redis
from luigi.task import flatten
from pyspark import Row, SparkContext
from pyspark.sql import SparkSession

from bicis.etl.build_series import SeriesBuilder
from bicis.lib.utils import head

redis_client = redis.StrictRedis()

class FeatureBuilder:
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_features(self, raw_doc):
        """
        :param raw_doc: A dictionary from the output of bicis.etl.UnifyRawData
        """

    def requires(self):
        """
        Specifies requirements for this feature builder to run. This requirements are luigi tasks
        that the BuildDataset task adds to its requirements
        :return:
        """
        return []

class CompositeBuilder(FeatureBuilder):
    builder_classes = []

    def __init__(self, *builders):
        self.builders = builders

    def get_features(self, raw_doc):
        res = {}
        for builder in self.builders:
            out = builder.get_features(raw_doc)

            overlap = set(out).intersection(res)
            if overlap:
                raise RuntimeError("There's an overlap between features {}".format(', '.join(overlap)))

            res.update(out)

        return Row(**res)

    def requires(self):
        return flatten([b.requires() for b in self.builders])


class HourFeaturesBuilder(FeatureBuilder):
    def __init__(self, mode='rent', window_size=24):
        # TODO: check whether it makes sense to keep the `mode` parameter
        """
        :param mode: whether to use the `rent` fields or the `return` fields on the raw_doc
        """
        self.window_size = window_size
        self.mode = mode
        self._ensure_structure()

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
                res['n_rents_{}_hb'.format(i)] = int(hour_data['n_rents'])
                res['n_returns_{}_hb'.format(i)] = int(hour_data['n_returns'])
            except Exception, e:
                logger.warn('station {} on hour {} has a weird thing'.format(station, hour))
                return

        # XXX should it return the dict or the row?
        return Row(**res)

    def _get_station_hour_key(self, hour, station):
        return u'{}_{}'.format(station, hour)

    def requires(self):
        """
        Specifies what luigi task this feature extractod depends on
        """
        return SeriesBuilder(key='hour')

    def _ensure_structure(self):
        if redis_client.get('HourFeaturesBuilder.done'):
            return

        spark_sql = SparkSession.builder.getOrCreate()
        input_fname = SeriesBuilder(key='hour').output().path

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
                    'n_rents': int(n_rents),
                    'n_returns': int(returns_data[hour])
                }

                redis_client.hmset(
                    self._get_station_hour_key(hour, station),
                    doc
                )


        redis_client.set('HourFeaturesBuilder.done', 1)

