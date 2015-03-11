# py.test configuration file.

# noinspection PyUnresolvedReferences
import pytest

from app.elasticsearch.store.ElasticsearchDocumentStore import ElasticsearchDocumentStore
from app.cassandra.store.CassandraLogEntryStore import CassandraLogEntryStore
from app.cassandra.store.SimpleCassandraClient import SimpleCassandraClient

from test.fixture.product import *


@pytest.fixture(scope="session")
def cassandra_log_keyspace():
    return "logger"


@pytest.fixture(scope="session")
def cassandra_log_table():
    return "log"


@pytest.fixture(scope="session")
def cassandra_fixture_keyspace():
    return "test_fixture"


@pytest.fixture(scope="session")
def cassandra_nodes():
    return ['127.0.0.1']


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def cassandra_log_entry_store(cassandra_nodes, cassandra_log_keyspace, cassandra_log_table):
    return CassandraLogEntryStore(cassandra_nodes, cassandra_log_keyspace, cassandra_log_table)


@pytest.fixture(scope="session")
def cassandra_log_trigger_name():
    return "com.felipead.cassandra.logger.LogTrigger"


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def cassandra_fixture_client(cassandra_nodes):
    return SimpleCassandraClient(cassandra_nodes)


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
def elasticsearch_document_store(elasticsearch_nodes):
    return ElasticsearchDocumentStore(elasticsearch_nodes)


@pytest.fixture(scope="session")
def elasticsearch_fixture_index():
    return "test_fixture"


# noinspection PyShadowingNames
@pytest.fixture(scope="session", autouse=True)
def create_elasticsearch_fixture_index(elasticsearch_document_store, elasticsearch_fixture_index):
    if elasticsearch_document_store.index_exists(elasticsearch_fixture_index):
        elasticsearch_document_store.delete_index(index=elasticsearch_fixture_index)
    elasticsearch_document_store.create_index(index=elasticsearch_fixture_index)
