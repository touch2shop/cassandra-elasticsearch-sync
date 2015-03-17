import logging
import yaml


DEFAULT_CASSANDRA_LOG_KEYSPACE = "logger"
DEFAULT_CASSANDRA_LOG_TABLE = "log"
DEFAULT_INTERVAL_BETWEEN_RUNS = 10
DEFAULT_CASSANDRA_ID_COLUMN_NAME = "id"
DEFAULT_CASSANDRA_TIMESTAMP_COLUMN_NAME = "timestamp"


# TODO: add an option containing the name of the "timestamp" column in Cassandra.


class Settings(object):

    def __init__(self, dictionary):
        self._interval_between_runs = dictionary.get("interval_between_runs", DEFAULT_INTERVAL_BETWEEN_RUNS)
        self._cassandra_log_keyspace = dictionary.get("cassandra_log_keyspace", DEFAULT_CASSANDRA_LOG_KEYSPACE)
        self._cassandra_log_table = dictionary.get("cassandra_log_table", DEFAULT_CASSANDRA_LOG_TABLE)
        self._cassandra_id_column_name = dictionary.get("cassandra_id_column_name", DEFAULT_CASSANDRA_ID_COLUMN_NAME)
        self._cassandra_timestamp_column_name = \
            dictionary.get("cassandra_timestamp_column_name", DEFAULT_CASSANDRA_TIMESTAMP_COLUMN_NAME)

    @property
    def interval_between_runs(self):
        return self._interval_between_runs

    @property
    def cassandra_log_keyspace(self):
        return self._cassandra_log_keyspace

    @property
    def cassandra_log_table(self):
        return self._cassandra_log_table

    @property
    def cassandra_id_column_name(self):
        return self._cassandra_id_column_name

    @property
    def cassandra_timestamp_column_name(self):
        return self._cassandra_timestamp_column_name

    @classmethod
    def load_from_file(cls, filename):
        logger = logging.getLogger()
        logger.info("Loading settings from file %s", filename)
        stream = open(filename, 'r')
        return Settings(yaml.load(stream))
