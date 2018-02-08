import logging
def head(iterator, n):
    iterator = iter(iterator)
    for i in xrange(n):
        yield iterator.next()


def get_logger(name, level=logging.INFO):
    # hack to overcome the fact that every application called from PySparkRunner
    # will be configurated with logging.WARNING
    logging.root.level = level
    return logging.getLogger(name)

