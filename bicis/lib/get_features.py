from itertools import chain

import numpy as np
import pandas as pd
from pyspark import Row
from pyspark.sql import SparkSession

from bicis.etl.build_series import SeriesBuilder


class CompositeBuilder(object):
    def __init__(self, hour_features_builder):
        self.hour_features_builder = hour_features_builder

    @classmethod
    def build(cls):
        return cls(HourFeaturesBuilder.build())

    def get_features(self, station, timestamp):
        return self.hour_features_builder.get_features(station, timestamp)


def shift_series(series, steps, window_size):
    # Yes, it's inefficient, yet easy to code
    return pd.Series(
        np.hstack([
            series.values[steps-1::-1],
            series.values[:steps-1:-1]
        ]),
        index=np.arange(len(series))
    )[:window_size]

def head(iterator, n):
    iterator = iter(iterator)
    for i in xrange(n):
        yield iterator.next()

class HourFeaturesBuilder(object):
    def __init__(self, stations_data):
        self.stations_data = stations_data

    def get_features(self, station, timestamp, window_size=24):
        hour = timestamp.hour

        # for hour=3 generates indices 2, 1, 0, 24, 23, ...
        indices = head(
            chain(
                xrange(hour-1, -1, -1),
                xrange(24-1, hour-1, -1)
            ),
            window_size
        )

        station_data = self.stations_data[station]
        res = {}
        for i, hour in enumerate(indices):
            hour_data = station_data[hour]
            res['n_rents_{}_hb'.format(i)] = hour_data['n_rents']
            res['n_returns_{}_hb'.format(i)] = hour_data['n_returns']

        # XXX should it return the dict or the row?
        return Row(**res)

    @classmethod
    def build(cls):
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

        return cls(cls._build_structure(n_rents_by_hour, n_returns_by_hour))

    @classmethod
    def _build_structure(cls, n_rents_by_hour, n_returns_by_hour):
        res = {}
        n_returns_by_hour = n_returns_by_hour.to_dict('index')
        for station, rent_data in n_rents_by_hour.to_dict('index').iteritems():
            returns_data = n_returns_by_hour[station]

            station_data = [
                Row(**
                    {
                        'hour': hour,
                        'n_rents': n_rents,
                        'n_returns': returns_data[hour]
                    }
                )
                for hour, n_rents
                in rent_data.iteritems()
            ]
            cls._check_structure(station_data, station)
            res[station] = station_data
        return res

    @classmethod
    def _check_structure(cls, station_data, station_name):
        for i, row in enumerate(station_data):
            if row['hour'] != i:
                raise RuntimeError('Invalid station data ({})'.format(station_name))



