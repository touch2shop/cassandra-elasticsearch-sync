import pytest


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
