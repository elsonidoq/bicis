from abc import abstractmethod, ABCMeta

from bicis.lib.data_paths import get_lock
from bicis.lib.utils import get_logger

logger = get_logger(__name__)

import redis
from luigi.task import flatten
from pyspark import Row

redis_client = redis.StrictRedis()

class FeatureBuilder:
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_features(self, raw_doc):
        """
        :param raw_doc: A dictionary from the output of bicis.etl.UnifyRawData
        """

    def ensure_structure(self):
        """
        Either creates a memory structure or ensures that a shared collection is set
        """
        pass

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
        super(CompositeBuilder, self).__init__()
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

