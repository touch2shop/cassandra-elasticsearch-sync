import logging
from time import sleep

import arrow
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch

from app.cassandra_to_elasticsearch_river import CassandraToElasticsearchRiver
from app.core.exception.unrecoverable_error import UnrecoverableError
from app.sync_state_store import SyncStateStore


_STATE_FILENAME = "state.yaml"


class SyncLoop:

    def __init__(self, settings):
        self._logger = logging.getLogger()
        self._state_store = SyncStateStore(_STATE_FILENAME)
        self._interval_between_runs = settings.interval_between_runs

        # TODO: For now, connecting to localhost's cassandra and elasticsearch. Load this from environment variables.
        cassandra_cluster = Cluster()
        elasticsearch_client = Elasticsearch()

        self._cassandra_to_elasticsearch_river = CassandraToElasticsearchRiver(
            cassandra_cluster, elasticsearch_client, settings)

    def run(self):
        state = self._state_store.load()
        try:
            if not state.last_sync_timestamp:
                self._logger.info("Syncing from the beginning of time...")

            state.last_sync_timestamp = self._run_cycle(state.last_sync_timestamp)

            while True:
                sleep(self._interval_between_runs)
                new_sync_timestamp = self._run_cycle(state.last_sync_timestamp)
                if new_sync_timestamp:
                    state.last_sync_timestamp = new_sync_timestamp
                    self._state_store.save(state)

        except UnrecoverableError as e:
            self._logger.error(str(e))
            self._logger.error("Aborting...")
            return

    def _run_cycle(self, last_sync_timestamp):
        try:
            if last_sync_timestamp:
                self._logger.info("Syncing since %s...", self.__format_timestamp(last_sync_timestamp))

            new_sync_timestamp = self._cassandra_to_elasticsearch_river.propagate_updates(last_sync_timestamp)
            if new_sync_timestamp:
                self._logger.info("...synced until %s", self.__format_timestamp(new_sync_timestamp))
            return new_sync_timestamp

        except UnrecoverableError:
            raise
        except Exception as e:
            self._logger.error(str(e))
            self._logger.error("Retrying...")
            return None

    @staticmethod
    def __format_timestamp(timestamp):
        arrow.get(timestamp).to("local").format("YYYY-MM-DD HH:mm:ss.SSSSSSZZ")
