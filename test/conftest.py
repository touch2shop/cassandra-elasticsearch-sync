# py.test configuration file.
import os
from cassandra.cluster import Cluster

from elasticsearch import Elasticsearch

# noinspection PyUnresolvedReferences
import pytest

from app.cassandra_domain.store.cassandra_log_entry_store import CassandraLogEntryStore
from app.cassandra_domain.store.cassandra_client import CassandraClient
from app.settings import Settings

from test.fixtures.product import *


@pytest.fixture(scope="session")
def current_directory():
    return os.path.dirname(os.path.abspath(__file__))


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def resources_directory(current_directory):
    return os.path.join(current_directory, "resources")


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def settings(current_directory):
    settings_file = os.path.join(current_directory, "..", "test_settings.yaml")
    return Settings.load_from_file(settings_file)


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def cassandra_log_keyspace(settings):
    return settings.cassandra_log_keyspace


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def cassandra_log_table(settings):
    return settings.cassandra_log_table


@pytest.fixture(scope="session")
def cassandra_fixture_keyspace():
    return "test_fixture"


@pytest.fixture(scope="session")
def cassandra_nodes():
    return ['127.0.0.1']


@pytest.fixture(scope="session")
def cassandra_log_trigger_name():
    return "com.felipead.cassandra.logger.LogTrigger"


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def cassandra_cluster(cassandra_nodes):
    return Cluster(cassandra_nodes)


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def cassandra_fixture_client(cassandra_cluster):
    return CassandraClient(cassandra_cluster)


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def cassandra_log_entry_store(cassandra_cluster, cassandra_log_keyspace, cassandra_log_table):
    return CassandraLogEntryStore(cassandra_cluster, cassandra_log_keyspace, cassandra_log_table)


# noinspection PyShadowingNames
@pytest.fixture(scope="session", autouse=True)
def create_cassandra_fixture_keyspace(cassandra_fixture_client, cassandra_fixture_keyspace):
    cassandra_fixture_client.execute("DROP KEYSPACE IF EXISTS %s" % cassandra_fixture_keyspace)
    cassandra_fixture_client.execute(
        """
        CREATE KEYSPACE %s
        WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};
        """
        % cassandra_fixture_keyspace
    )
    cassandra_fixture_client.keyspace = cassandra_fixture_keyspace


@pytest.fixture(scope="session")
def elasticsearch_nodes():
    return [{"host": "localhost", "port": 9200}]


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def elasticsearch_client(elasticsearch_nodes):
    return Elasticsearch(elasticsearch_nodes)


@pytest.fixture(scope="session")
def elasticsearch_fixture_index():
    return "test_fixture"


# noinspection PyShadowingNames
@pytest.fixture(scope="session", autouse=True)
def create_elasticsearch_fixture_index(elasticsearch_client, elasticsearch_fixture_index):
    if elasticsearch_client.indices.exists(elasticsearch_fixture_index):
        elasticsearch_client.indices.delete(index=elasticsearch_fixture_index)
    elasticsearch_client.indices.create(index=elasticsearch_fixture_index)
