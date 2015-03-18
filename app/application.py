import logging
import sys
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch
from app.sync_loop import SyncLoop


_LOG_FILE_NAME = "cassandra-elasticsearch-sync.log"


def _setup_logger():
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")

    log_file_handler = logging.FileHandler(_LOG_FILE_NAME)
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(formatter)
    root_logger.addHandler(log_file_handler)

    log_stream_handler = logging.StreamHandler(sys.stdout)
    log_stream_handler.setLevel(logging.INFO)
    log_stream_handler.setFormatter(formatter)
    root_logger.addHandler(log_stream_handler)


def run(settings, state_file_name=None):
    _setup_logger()

    # TODO: For now, connecting to localhost's cassandra and elasticsearch. Load this from environment variables.
    cassandra_cluster = Cluster()
    elasticsearch_client = Elasticsearch()

    sync_loop = SyncLoop(cassandra_cluster, elasticsearch_client, settings, state_file_name)
    sync_loop.run()
