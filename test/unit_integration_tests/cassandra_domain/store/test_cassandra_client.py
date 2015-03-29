import uuid

import pytest
from app.cassandra_domain.store.cassandra_client import CassandraClient


@pytest.fixture(scope="module")
def cassandra_client(cassandra_cluster):
    return CassandraClient(cassandra_cluster)


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
class TestCassandraClient:

    def test_keyspace_exists(self, cassandra_client, cassandra_log_keyspace, cassandra_fixture_keyspace):
        assert cassandra_client.keyspace_exists(cassandra_log_keyspace)
        assert cassandra_client.keyspace_exists(cassandra_log_keyspace.upper())
        assert cassandra_client.keyspace_exists(cassandra_fixture_keyspace)
        assert cassandra_client.keyspace_exists(cassandra_fixture_keyspace.upper())

    def test_keyspace_does_not_exist(self, cassandra_client):
        assert not cassandra_client.keyspace_exists(str(uuid.uuid4()))

    def test_table_exists(self, cassandra_client, cassandra_log_keyspace, cassandra_log_table):
        assert cassandra_client.table_exists(cassandra_log_keyspace, cassandra_log_table)
        assert cassandra_client.table_exists(cassandra_log_keyspace.upper(), cassandra_log_table.upper())

    def test_table_does_not_exist(self, cassandra_client, cassandra_fixture_keyspace):
        assert not cassandra_client.table_exists(cassandra_fixture_keyspace, "this_table_does_not_exist")
