import logging
import os
import arrow
import yaml
from app.sync_state import SyncState


DATE_FORMAT = "YYYY-MM-DDTHH:mm:ss.SSSSSSZZ"


class SyncStateStore:

    def __init__(self, filename):
        if not filename:
            raise ValueError("File name can not be empty")
        self._filename = filename
        self._logger = logging.getLogger()

    def load(self):
        if os.path.isfile(self._filename):
            self._logger.info("Loading previous state from file %s", self._filename)
            stream = open(self._filename, 'r')
            data = yaml.load(stream)
            return self._from_yaml(data)
        else:
            self._logger.warn("Previous state file %s not found. Loading initial state.", self._filename)
            return self._default_instance()

    def save(self, sync_state):
        stream = open(self._filename, 'w')
        yaml.dump(self._to_yaml(sync_state), stream)

    @classmethod
    def _to_utc_time_string(cls, timestamp):
        return arrow.get(timestamp).format(DATE_FORMAT) if timestamp else ""

    @classmethod
    def _to_timestamp(cls, utc_time):
        return arrow.get(utc_time).float_timestamp if utc_time else None

    def _default_instance(self):
        return SyncState(self)

    @classmethod
    def _to_yaml(cls, sync_state):
        return {
            "last_cassandra_to_elasticsearch_sync":
                cls._to_utc_time_string(sync_state.last_cassandra_to_elasticsearch_sync),
            "last_elasticsearch_to_cassandra_sync":
                cls._to_utc_time_string(sync_state.last_elasticsearch_to_cassandra_sync)
        }

    def _from_yaml(self, data):
        last_cassandra_to_elasticsearch_sync_string = data.get("last_cassandra_to_elasticsearch_sync", None)
        if last_cassandra_to_elasticsearch_sync_string:
            last_cassandra_to_elasticsearch_sync = self._to_timestamp(last_cassandra_to_elasticsearch_sync_string)
        else:
            last_cassandra_to_elasticsearch_sync = None

        last_elasticsearch_to_cassandra_sync_string = data.get("last_elasticsearch_to_cassandra_sync", None)
        if last_elasticsearch_to_cassandra_sync_string:
            last_elasticsearch_to_cassandra_sync = self._to_timestamp(last_elasticsearch_to_cassandra_sync_string)
        else:
            last_elasticsearch_to_cassandra_sync = None

        return SyncState(self,
            last_cassandra_to_elasticsearch_sync=last_cassandra_to_elasticsearch_sync,
            last_elasticsearch_to_cassandra_sync=last_elasticsearch_to_cassandra_sync,
        )
