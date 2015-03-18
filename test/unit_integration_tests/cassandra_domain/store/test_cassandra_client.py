from decimal import Decimal
from time import time
import uuid

import pytest

from app.cassandra_domain.store.cassandra_client import CassandraClient
from test.fixtures.product import ProductFixture


@pytest.fixture(scope="module")
def product_fixtures(product_fixture_cassandra_store):
    products = list()
    products.append(ProductFixture(_id=uuid.uuid4(), name="navy polo shirt", quantity=5, price=Decimal("19.99"),
                                   enabled=True, description="great shirt, great price!", timestamp=time()))
    products.append(ProductFixture(_id=uuid.uuid4(), name="cool red shorts", quantity=7, price=Decimal("29.99"),
                                   enabled=True, description="perfect to go to the beach", timestamp=time()))
    products.append(ProductFixture(_id=uuid.uuid4(), name="black DC skater shoes", quantity=10, price=Decimal("99.99"),
                                   enabled=True, description="yo!", timestamp=time()))

    for product in products:
        product_fixture_cassandra_store.create(product)

    return products


@pytest.fixture(scope="module")
def cassandra_client(cassandra_cluster):
    return CassandraClient(cassandra_cluster)


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
class TestCassandraClient:

    def test_select_specific_columns_by_id(self, cassandra_client, cassandra_fixture_keyspace, product_fixtures):

        for product in product_fixtures:
            rows = cassandra_client.select_by_id(ProductFixture.TABLE_NAME, product.id,
                                                 columns=("quantity", "description", "name", "price", "enabled"),
                                                 keyspace=cassandra_fixture_keyspace)

            assert len(rows) == 1
            row = rows[0]
            assert row[0] == product.quantity
            assert row[1] == product.description
            assert row[2] == product.name
            assert row[3] == product.price
            assert row[4] == product.enabled

    def test_select_all_columns_by_id(self, cassandra_client, cassandra_fixture_keyspace, product_fixtures):

        for product in product_fixtures:
            rows = cassandra_client.select_by_id(ProductFixture.TABLE_NAME, product.id,
                                                 keyspace=cassandra_fixture_keyspace)

            assert len(rows) == 1
            row = rows[0]
            assert row.id == product.id
            assert row.quantity == product.quantity
            assert row.description == product.description
            assert row.name == product.name
            assert row.price == product.price
            assert row.enabled == product.enabled

    def test_select_by_uuid_encoded_as_string(self, cassandra_client, cassandra_fixture_keyspace, product_fixtures):

        for product in product_fixtures:
            encoded_uuid = str(product.id)
            rows = cassandra_client.select_by_id(ProductFixture.TABLE_NAME, encoded_uuid,
                                                 keyspace=cassandra_fixture_keyspace)

            assert len(rows) == 1
            row = rows[0]
            assert row.id == product.id
            assert row.quantity == product.quantity
            assert row.description == product.description
            assert row.name == product.name
            assert row.price == product.price
            assert row.enabled == product.enabled

    def test_select_by_id_on_default_keyspace(self, cassandra_client, cassandra_fixture_keyspace, product_fixtures):

        cassandra_client.execute("USE %s" % cassandra_fixture_keyspace)
        for product in product_fixtures:
            rows = cassandra_client.select_by_id(ProductFixture.TABLE_NAME, product.id, keyspace=None)

            assert len(rows) == 1
            row = rows[0]
            assert row.id == product.id
            assert row.quantity == product.quantity
            assert row.description == product.description
            assert row.name == product.name
            assert row.price == product.price
            assert row.enabled == product.enabled

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
