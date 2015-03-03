# py.test configuration file.

import uuid
import pytest
from test.cassandra.fixtures import FixtureProduct, create_fixture_product
from app.cassandra.SimpleCassandraClient import SimpleCassandraClient


@pytest.fixture(scope="module")
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


@pytest.fixture(scope="module")
def fixture_products(fixture_cassandra_client):
    products = list()
    products.append(FixtureProduct(uuid.uuid4(), "navy polo shirt", 5, "great shirt, great price!"))
    products.append(FixtureProduct(uuid.uuid4(), "cool red shorts", 7, "perfect to go to the beach"))
    products.append(FixtureProduct(uuid.uuid4(), "black DC skater shoes", 10, "yo!"))

    for product in products:
        create_fixture_product(fixture_cassandra_client, product)

    return products


@pytest.fixture(scope="module")
def cassandra_client(cassandra_nodes):
    return SimpleCassandraClient(cassandra_nodes)
