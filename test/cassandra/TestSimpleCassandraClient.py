import uuid
import pytest
from app.cassandra.SimpleCassandraClient import SimpleCassandraClient
from test.cassandra.fixtures import create_fixture_product, FixtureProduct


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


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
@pytest.mark.slow
@pytest.mark.usefixtures("create_fixture_product_schema")
class TestSimpleCassandraClient:

    def test_select_specific_columns_by_id(self, cassandra_client, fixture_cassandra_keyspace, fixture_products):
        for product in fixture_products:
            rows = cassandra_client.select_by_id("product", product.id_, columns=("quantity", "description", "name"),
                                                 keyspace=fixture_cassandra_keyspace)

            assert len(rows) == 1
            row = rows[0]
            assert row[0] == product.quantity
            assert row[1] == product.description
            assert row[2] == product.name

    def test_select_all_columns_by_id(self, cassandra_client, fixture_cassandra_keyspace, fixture_products):
        for product in fixture_products:
            rows = cassandra_client.select_by_id("product", product.id_, keyspace=fixture_cassandra_keyspace)

            assert len(rows) == 1
            row = rows[0]
            assert row.id == product.id_
            assert row.quantity == product.quantity
            assert row.description == product.description
            assert row.name == product.name

    def test_select_by_id_on_default_keyspace(self, cassandra_client, fixture_cassandra_keyspace, fixture_products):
        cassandra_client.execute("USE %s" % fixture_cassandra_keyspace)
        for product in fixture_products:
            rows = cassandra_client.select_by_id("product", product.id_, keyspace=None)

            assert len(rows) == 1
            row = rows[0]
            assert row.id == product.id_
            assert row.quantity == product.quantity
            assert row.description == product.description
            assert row.name == product.name
