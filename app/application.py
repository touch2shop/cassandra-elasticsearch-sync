import logging
import sys
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch
from app.sync_loop import SyncLoop


_DEFAULT_STATE_FILENAME = "state.yaml"
_DEFAULT_LOGGER_LEVEL = logging.INFO


def _setup_logger(logger_level):
    root_logger = logging.getLogger()
    root_logger.setLevel(logger_level)

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")

    log_stream_handler = logging.StreamHandler(sys.stdout)
    log_stream_handler.setLevel(logger_level)
    log_stream_handler.setFormatter(formatter)
    root_logger.addHandler(log_stream_handler)


def run(settings, state_file_name=_DEFAULT_STATE_FILENAME, logger_level=_DEFAULT_LOGGER_LEVEL):
    _setup_logger(logger_level)

    # TODO: For now, connecting to localhost's cassandra and elasticsearch. Load this from environment variables.
    cassandra_cluster = Cluster()
    elasticsearch_client = Elasticsearch()

    sync_loop = SyncLoop(cassandra_cluster, elasticsearch_client, settings, state_file_name)
    sync_loop.run()
