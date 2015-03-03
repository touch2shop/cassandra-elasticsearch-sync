# py.test configuration file.

import pytest
from app.cassandra.CassandraLogEntryStore import CassandraLogEntryStore
from app.cassandra.SimpleCassandraClient import SimpleCassandraClient


# This variable tells py.test which files and folders to ignore
collect_ignore = ["env"]


@pytest.fixture(scope="session")
def cassandra_log_keyspace():
    return "logger"


@pytest.fixture(scope="session")
def cassandra_log_table():
    return "log"


@pytest.fixture(scope="session")
def fixture_cassandra_keyspace():
    return "test"


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
def fixture_cassandra_client(cassandra_nodes):
    return SimpleCassandraClient(cassandra_nodes)


# noinspection PyShadowingNames
@pytest.fixture(scope="session", autouse=True)
def create_fixture_cassandra_keyspace(fixture_cassandra_client, fixture_cassandra_keyspace):
    fixture_cassandra_client.execute("DROP KEYSPACE IF EXISTS %s" % fixture_cassandra_keyspace)
    fixture_cassandra_client.execute(
        """
        CREATE KEYSPACE %s
        WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};
        """
        % fixture_cassandra_keyspace
    )
    fixture_cassandra_client.keyspace = fixture_cassandra_keyspace
