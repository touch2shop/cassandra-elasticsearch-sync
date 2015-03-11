import uuid

import pytest

from app.cassandra.store.SimpleCassandraClient import SimpleCassandraClient
from test.cassandra.fixtures import ProductFixture


@pytest.fixture(scope="module")
def product_fixtures(product_fixture_store):
    products = list()
    products.append(ProductFixture(uuid.uuid4(), "navy polo shirt", 5, "great shirt, great price!"))
    products.append(ProductFixture(uuid.uuid4(), "cool red shorts", 7, "perfect to go to the beach"))
    products.append(ProductFixture(uuid.uuid4(), "black DC skater shoes", 10, "yo!"))

    for product in products:
        product_fixture_store.create(product)

    return products


@pytest.fixture(scope="module")
def cassandra_client(cassandra_nodes):
    return SimpleCassandraClient(cassandra_nodes)


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
@pytest.mark.slow
@pytest.mark.usefixtures("create_product_fixture_schema")
class TestSimpleCassandraClient:

    def test_select_specific_columns_by_id(self, cassandra_client, cassandra_fixture_keyspace,
                                           product_fixtures, product_fixture_table):

        for product in product_fixtures:
            rows = cassandra_client.select_by_id(product_fixture_table, product.id,
                                                 columns=("quantity", "description", "name"),
                                                 keyspace=cassandra_fixture_keyspace)

            assert len(rows) == 1
            row = rows[0]
            assert row[0] == product.quantity
            assert row[1] == product.description
            assert row[2] == product.name

    def test_select_all_columns_by_id(self, cassandra_client, cassandra_fixture_keyspace,
                                      product_fixtures, product_fixture_table):

        for product in product_fixtures:
            rows = cassandra_client.select_by_id(product_fixture_table, product.id,
                                                 keyspace=cassandra_fixture_keyspace)

            assert len(rows) == 1
            row = rows[0]
            assert row.id == product.id
            assert row.quantity == product.quantity
            assert row.description == product.description
            assert row.name == product.name

    def test_select_by_uuid_encoded_as_string(self, cassandra_client, cassandra_fixture_keyspace,
                                              product_fixtures, product_fixture_table):

        for product in product_fixtures:
            encoded_uuid = str(product.id)
            rows = cassandra_client.select_by_id(product_fixture_table, encoded_uuid,
                                                 keyspace=cassandra_fixture_keyspace)

            assert len(rows) == 1
            row = rows[0]
            assert row.id == product.id
            assert row.quantity == product.quantity
            assert row.description == product.description
            assert row.name == product.name

    def test_select_by_id_on_default_keyspace(self, cassandra_client, cassandra_fixture_keyspace,
                                              product_fixtures, product_fixture_table):

        cassandra_client.execute("USE %s" % cassandra_fixture_keyspace)
        for product in product_fixtures:
            rows = cassandra_client.select_by_id(product_fixture_table, product.id, keyspace=None)

            assert len(rows) == 1
            row = rows[0]
            assert row.id == product.id
            assert row.quantity == product.quantity
            assert row.description == product.description
            assert row.name == product.name
