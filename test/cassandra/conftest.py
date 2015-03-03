# py.test configuration file.

import pytest


@pytest.fixture(scope="session")
def create_fixture_product_schema(fixture_cassandra_client, cassandra_log_trigger_name):
    fixture_cassandra_client.execute("DROP TABLE IF EXISTS product")
    fixture_cassandra_client.execute(
        """
        CREATE TABLE product (
          id uuid PRIMARY KEY,
          name text,
          quantity int,
          description text
        )
        """)
    fixture_cassandra_client.execute("CREATE TRIGGER logger ON product USING '%s'" % cassandra_log_trigger_name)
