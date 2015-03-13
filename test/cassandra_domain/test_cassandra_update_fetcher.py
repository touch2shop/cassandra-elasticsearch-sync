from datetime import datetime
from operator import attrgetter
import uuid

import pytest

from test.fixture.product import ProductFixture
from app.cassandra_domain.cassandra_update_fetcher import CassandraUpdateFetcher


@pytest.fixture(scope="module")
def setup(cassandra_log_entry_store):
    cassandra_log_entry_store.delete_all()


@pytest.fixture(scope="function")
def product_fixtures_creation_time():
    return datetime.utcnow()


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.fixture(scope="function")
def product_fixtures(product_fixture_cassandra_store, product_fixtures_creation_time):
    product_fixture_cassandra_store.delete_all()
    products = [ProductFixture(uuid.uuid4(), "navy polo shirt", 5, "great shirt, great price!"),
                ProductFixture(uuid.uuid4(), "cool red shorts", 7, "perfect to go to the beach"),
                ProductFixture(uuid.uuid4(), "black DC skater shoes", 10, "yo!")]

    for product in products:
        product_fixture_cassandra_store.create(product)
    return products


@pytest.fixture("module")
def cassandra_update_fetcher(cassandra_log_entry_store):
    return CassandraUpdateFetcher(cassandra_log_entry_store)


# noinspection PyShadowingNames,PyMethodMayBeStatic,PyClassHasNoInit
@pytest.mark.slow
@pytest.mark.usefixtures("setup")
class TestCassandraUpdateFetcher:

    def test_fetch_updates_for_product_fixtures_creation_from_beginning_of_time(
            self, cassandra_update_fetcher, product_fixtures, 
            cassandra_fixture_keyspace, product_fixtures_creation_time):

        updates = cassandra_update_fetcher.fetch_updates()
        updates_by_key = self.sort_updates_by_key(updates)

        assert len(updates_by_key) == 3
        for product_fixture in product_fixtures:
            key = str(product_fixture.key)
            assert key in updates_by_key
            update = updates_by_key[key]

            self.check_update_timestamp(update, product_fixtures_creation_time)
            self.check_update_identifier(update, cassandra_fixture_keyspace, ProductFixture.TABLE_NAME)
            self.check_update_matches_product_fixture_creation(update, product_fixture)

    def test_fetch_updates_for_product_fixtures_creation_with_minimum_time_in_utc(
            self, cassandra_update_fetcher, product_fixtures, 
            cassandra_fixture_keyspace, product_fixtures_creation_time):

        updates = cassandra_update_fetcher.fetch_updates(product_fixtures_creation_time)
        updates_by_key = self.sort_updates_by_key(updates)

        assert len(updates_by_key) == 3
        for product_fixture in product_fixtures:
            assert product_fixture.key in updates_by_key
            update = updates_by_key[product_fixture.key]

            self.check_update_timestamp(update, product_fixtures_creation_time)
            self.check_update_identifier(update, cassandra_fixture_keyspace, ProductFixture.TABLE_NAME)
            self.check_update_matches_product_fixture_creation(update, product_fixture)

    def test_fetch_updates_for_product_fixtures_updates(
            self, cassandra_update_fetcher, product_fixtures, product_fixture_cassandra_store, 
            cassandra_fixture_keyspace):

        update_time = datetime.now()

        product_fixtures[0].name = "new name"
        product_fixture_cassandra_store.update(product_fixtures[0])
        product_fixture_cassandra_store.update(product_fixtures[0])
        product_fixtures[0].description = "new description"
        product_fixture_cassandra_store.update(product_fixtures[0])

        product_fixtures[1].description = "new description"
        product_fixture_cassandra_store.update(product_fixtures[1])
        product_fixture_cassandra_store.delete(product_fixtures[1])

        product_fixtures[2].quantity = 100
        product_fixture_cassandra_store.update(product_fixtures[2])

        updates = cassandra_update_fetcher.fetch_updates(update_time)

        updates_by_key = self.sort_updates_by_key(updates)
        assert len(updates_by_key) == 3

        for product_fixture in product_fixtures:
            assert product_fixture.key in updates_by_key
            update = updates_by_key[product_fixture.key]

            self.check_update_timestamp(update, update_time)
            self.check_update_identifier(update, cassandra_fixture_keyspace, ProductFixture.TABLE_NAME)

        assert updates_by_key[product_fixtures[0].key].is_delete is False
        self.check_update_fields(updates_by_key[product_fixtures[0].key],
                                 {"name": "new name", "description": "new description"})

        assert updates_by_key[product_fixtures[1].key].is_delete is True

        self.check_update_fields(updates_by_key[product_fixtures[2].key], {"quantity": 100})
        assert updates_by_key[product_fixtures[2].key].is_delete is False

    @staticmethod
    def check_update_identifier(update, keyspace, table):
        assert update.identifier.namespace == keyspace
        assert update.identifier.table == table

    @staticmethod
    def check_update_timestamp(update, operation_time):
        update_time = datetime.utcfromtimestamp(update.timestamp)
        assert datetime.utcnow() >= update_time
        assert update_time >= operation_time
        if update.fields:
            for field in update.fields:
                assert field.timestamp <= update.timestamp
                assert datetime.utcfromtimestamp(field.timestamp) >= operation_time

    @staticmethod
    def check_update_matches_product_fixture_creation(update, product_fixture):
        assert update.is_delete is False
        assert len(update.fields) == 3
        for field in update.fields:
            assert attrgetter(field.name)(product_fixture) == field.value

    @staticmethod
    def check_update_fields(update, expected_fields):
        actual_fields = {}
        for field in update.fields:
            assert field.name not in actual_fields
            actual_fields[field.name] = field.value

        for (expected_name, expected_value) in expected_fields.items():
            assert actual_fields[expected_name] == expected_value

        assert "timestamp" not in actual_fields

    @staticmethod
    def sort_updates_by_key(updates):
        updates_by_key = {}
        for update in updates:
            key = update.identifier.key
            assert key not in updates_by_key
            updates_by_key[key] = update
        return updates_by_key
