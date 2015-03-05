# py.test configuration file.

import pytest
from test.cassandra.fixtures import ProductFixtureStore


@pytest.fixture(scope="session")
def product_fixture_table():
    return "product_fixture"


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def create_product_fixture_schema(cassandra_fixture_client, product_fixture_table, cassandra_log_trigger_name):
    cassandra_fixture_client.execute("DROP TABLE IF EXISTS product")
    cassandra_fixture_client.execute(
        """
        CREATE TABLE %s (
          id uuid PRIMARY KEY,
          name text,
          quantity int,
          description text,
          created_at timestamp,
          updated_at timestamp
        )
        """ % product_fixture_table)

    cassandra_fixture_client.execute("CREATE TRIGGER logger ON %s USING '%s'" %
                                     (product_fixture_table, cassandra_log_trigger_name))


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def product_fixture_store(cassandra_nodes, cassandra_fixture_keyspace, product_fixture_table):
    return ProductFixtureStore(cassandra_nodes, cassandra_fixture_keyspace, product_fixture_table)
