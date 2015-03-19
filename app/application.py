import logging
import os
import sys
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch
from app.settings import Settings
from app.sync_loop import SyncLoop


ELASTICSEARCH_URLS_ENVIRONMENT_VARIABLE = "ELASTICSEARCH_URLS"
CASSANDRA_PASSWORD_ENVIRONMENT_VARIABLE = "CASSANDRA_PASSWORD"
CASSANDRA_USERNAME_ENVIRONMENT_VARIABLE = "CASSANDRA_USERNAME"
CASSANDRA_PORT_ENVIRONMENT_VARIABLE = "CASSANDRA_PORT"
CASSANDRA_NODES_ENVIRONMENT_VARIABLE = "CASSANDRA_NODES"

_DEFAULT_STATE_FILENAME = "state.yaml"
_DEFAULT_LOGGER_LEVEL = logging.INFO
_DEFAULT_SETTINGS_FILE_NAME = "settings.yaml"


class Application:

    __ELASTICSEARCH_CLIENT = None
    __CASSANDRA_CLUSTER = None

    @classmethod
    def get_cassandra_cluster(cls):
        if not cls.__CASSANDRA_CLUSTER:
            nodes = os.environ.get(CASSANDRA_NODES_ENVIRONMENT_VARIABLE, "127.0.0.1").split()
            port = int(os.environ.get(CASSANDRA_PORT_ENVIRONMENT_VARIABLE, 9042))
            username = os.environ.get(CASSANDRA_USERNAME_ENVIRONMENT_VARIABLE, None)
            if username:
                authentication = {"username": username,
                                  "password": os.environ.get(CASSANDRA_PASSWORD_ENVIRONMENT_VARIABLE, "")}
            else:
                authentication = None

            cls.__CASSANDRA_CLUSTER = Cluster(nodes, port=port, auth_provider=authentication)
        return cls.__CASSANDRA_CLUSTER

    @classmethod
    def get_elasticsearch_client(cls):
        if not cls.__ELASTICSEARCH_CLIENT:
            hosts = os.environ.get(ELASTICSEARCH_URLS_ENVIRONMENT_VARIABLE, "http://localhost:9200").split()
            cls.__ELASTICSEARCH_CLIENT = Elasticsearch(hosts)
        return cls.__ELASTICSEARCH_CLIENT

    @staticmethod
    def _setup_logger(logger_level):
        root_logger = logging.getLogger()
        root_logger.setLevel(logger_level)

        formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")

        log_stream_handler = logging.StreamHandler(sys.stdout)
        log_stream_handler.setLevel(logger_level)
        log_stream_handler.setFormatter(formatter)
        root_logger.addHandler(log_stream_handler)

    def __init__(self, settings=None):
        if settings:
            self._settings = settings
        else:
            self._settings = Settings.load_from_file(_DEFAULT_SETTINGS_FILE_NAME)

    def run(self, state_file_name=_DEFAULT_STATE_FILENAME, logger_level=_DEFAULT_LOGGER_LEVEL):
        self._setup_logger(logger_level)

        cassandra_cluster = self.get_cassandra_cluster()
        elasticsearch_client = self.get_elasticsearch_client()

        sync_loop = SyncLoop(cassandra_cluster, elasticsearch_client, self._settings, state_file_name)
        sync_loop.run()
