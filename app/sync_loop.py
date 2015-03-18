import logging
from time import sleep
import arrow

from app.cassandra_to_elasticsearch_river import CassandraToElasticsearchRiver
from app.elasticsearch_to_cassandra_river import ElasticsearchToCassandraRiver
from app.sync_state_store import SyncStateStore


_INTERVAL_BETWEEN_RIVER_SYNCS = 5
_DEFAULT_STATE_FILENAME = "state.yaml"


class SyncLoop:

    def __init__(self, cassandra_cluster, elasticsearch_client, settings, state_file_name=None):
        self._logger = logging.getLogger()

        if not state_file_name:
            state_file_name = _DEFAULT_STATE_FILENAME
        self._state_store = SyncStateStore(state_file_name)

        self._interval_between_runs = settings.interval_between_runs

        self._cassandra_to_elasticsearch_river = CassandraToElasticsearchRiver(
            cassandra_cluster, elasticsearch_client, settings)
        self._elasticsearch_to_cassandra_river = ElasticsearchToCassandraRiver(
            elasticsearch_client, cassandra_cluster, settings)

    def run(self):
        try:
            state = self._state_store.load()
            self.__initial_sync_if_necessary(state)

            while True:
                self.__incremental_sync(state)
                sleep(self._interval_between_runs)

        except Exception as e:
            self._logger.error(str(e))
            self._logger.error("Aborting...")
            return

    def __initial_sync_if_necessary(self, state):
        if not state.last_cassandra_to_elasticsearch_sync:
            self.__initial_cassandra_to_elasticsearch_sync(state)
        if not state.last_elasticsearch_to_cassandra_sync:
            self.__initial_elasticsearch_to_cassandra_sync(state)

    def __initial_cassandra_to_elasticsearch_sync(self, state):
        self._logger.info("Initial Cassandra to Elasticsearch sync. This will take a while...")
        state.last_cassandra_to_elasticsearch_sync = self._cassandra_to_elasticsearch_river.propagate_updates()
        state.save()

    def __initial_elasticsearch_to_cassandra_sync(self, state):
        self._logger.info("Initial Elasticsearch to Cassandra sync. This will take a while...")
        state.last_elasticsearch_to_cassandra_sync = self._elasticsearch_to_cassandra_river.propagate_updates()
        state.save()

    def __incremental_sync(self, state):
        self.__incremental_cassandra_to_elasticsearch_sync(state)
        sleep(_INTERVAL_BETWEEN_RIVER_SYNCS)
        self.__incremental_elasticsearch_to_cassandra_sync(state)

    def __incremental_cassandra_to_elasticsearch_sync(self, state):
        self._logger.info("Syncing Cassandra to Elasticsearch since %s...",
                          self.__format_timestamp(state.last_cassandra_to_elasticsearch_sync))

        timestamp = self._cassandra_to_elasticsearch_river.propagate_updates(state.last_cassandra_to_elasticsearch_sync)
        state.last_cassandra_to_elasticsearch_sync = timestamp
        state.save()

        self._logger.info("...synced until %s.", self.__format_timestamp(timestamp))

    def __incremental_elasticsearch_to_cassandra_sync(self, state):
        self._logger.info("Syncing Elasticsearch to Cassandra since %s...",
                          self.__format_timestamp(state.last_elasticsearch_to_cassandra_sync))

        timestamp = self._elasticsearch_to_cassandra_river.propagate_updates(state.last_elasticsearch_to_cassandra_sync)
        state.last_elasticsearch_to_cassandra_sync = timestamp
        state.save()

        self._logger.info("...synced until %s.", self.__format_timestamp(timestamp))

    @staticmethod
    def __format_timestamp(timestamp):
        return arrow.get(timestamp).to("local").format("YYYY-MM-DD HH:mm:ss.SSSSSSZZ")
