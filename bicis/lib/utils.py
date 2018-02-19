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


def load_csv_dataframe(spark_sql, path):
    return (
        spark_sql
            .read
            .load(
                path,
                format="csv",
                sep=",",
                inferSchema="true",
                header="true"
        )
    )

