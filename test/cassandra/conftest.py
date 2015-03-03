# py.test configuration file.

import pytest


@pytest.fixture(scope="session")
def product_fixture_table():
    return "product_fixture"


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def create_product_fixture_schema(fixture_cassandra_client, cassandra_log_trigger_name, product_fixture_table):
    fixture_cassandra_client.execute("DROP TABLE IF EXISTS product")
    fixture_cassandra_client.execute(
        """
        CREATE TABLE %s (
          id uuid PRIMARY KEY,
          name text,
          quantity int,
          description text
        )
        """ % product_fixture_table)

    fixture_cassandra_client.execute("CREATE TRIGGER logger ON %s USING '%s'" %
                                     (product_fixture_table, cassandra_log_trigger_name))
