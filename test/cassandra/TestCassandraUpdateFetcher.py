from datetime import datetime
from operator import attrgetter
import uuid
import pytest
from test.cassandra.fixtures import ProductFixture
from app.cassandra.CassandraUpdateFetcher import CassandraUpdateFetcher


@pytest.fixture(scope="function")
def setup(product_fixture_store):
    product_fixture_store.delete_all()


@pytest.fixture(scope="function")
def product_fixtures_creation_time():
    return datetime.utcnow()


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.fixture(scope="function")
def product_fixtures(product_fixture_store, product_fixtures_creation_time):
    products = [ProductFixture(uuid.uuid4(), "navy polo shirt", 5, "great shirt, great price!"),
                ProductFixture(uuid.uuid4(), "cool red shorts", 7, "perfect to go to the beach"),
                ProductFixture(uuid.uuid4(), "black DC skater shoes", 10, "yo!")]

    for product in products:
        product_fixture_store.create(product)
    return products


@pytest.fixture("module")
def cassandra_update_fetcher(cassandra_log_entry_store):
    return CassandraUpdateFetcher(cassandra_log_entry_store)


# noinspection PyShadowingNames,PyMethodMayBeStatic,PyClassHasNoInit
@pytest.mark.slow
@pytest.mark.usefixtures("create_product_fixture_schema", "setup")
class TestCassandraUpdateFetcher:

    def test_fetch_updates_for_product_fixtures_creation_from_beggining_of_time(self, cassandra_update_fetcher,
                                                         product_fixtures, product_fixture_table,
                                                         cassandra_fixture_keyspace,
                                                         product_fixtures_creation_time):

        updates = cassandra_update_fetcher.fetch_updates()
        updates_by_key = self.group_updates_by_key(updates)

        assert len(updates_by_key) >= 3
        for product_fixture in product_fixtures:
            key = str(product_fixture.id_)
            assert key in updates_by_key
            updates = updates_by_key[key]
            assert len(updates) == 1
            update = updates[0]

            self.check_update_timestamp(update, product_fixtures_creation_time)
            self.check_update_identifier(update, cassandra_fixture_keyspace, product_fixture_table)
            self.check_update_matches_product_fixture_creation(update, product_fixture)

    @staticmethod
    def check_update_identifier(update, keyspace, product_fixture_table):
        assert update.identifier.namespace == keyspace
        assert update.identifier.table == product_fixture_table

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

    def group_updates_by_key(self, updates):
        updates_by_key = {}
        for update in updates:
            key = update.identifier.key
            if key not in updates_by_key:
                updates_by_key[key] = list()
            updates_by_key[key].append(update)
        return updates_by_key
