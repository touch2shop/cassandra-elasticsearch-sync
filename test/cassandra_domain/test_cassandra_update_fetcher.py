from decimal import Decimal
from operator import attrgetter
from time import time
import uuid
import arrow
import pytest

from test.fixtures.product import ProductFixture
from app.cassandra_domain.cassandra_update_fetcher import CassandraUpdateFetcher


@pytest.fixture(scope="module")
def setup(cassandra_log_entry_store):
    cassandra_log_entry_store.delete_all()


@pytest.fixture(scope="module")
def product_fixtures_creation_timestamp():
    return time()


# noinspection PyUnusedLocal,PyShadowingNames
@pytest.fixture(scope="module")
def product_fixtures(product_fixture_cassandra_store, product_fixtures_creation_timestamp):
    product_fixture_cassandra_store.delete_all()

    products = [ProductFixture(_id=uuid.uuid4(), timestamp=product_fixtures_creation_timestamp,
                               name="navy polo shirt", quantity=5, description="great shirt, great price!",
                               price=Decimal("99.99"), enabled=True),
                ProductFixture(_id=uuid.uuid4(), timestamp=product_fixtures_creation_timestamp,
                               name="cool red shorts", quantity=7, description="perfect to go to the beach",
                               price=Decimal("49.99"), enabled=False),
                ProductFixture(_id=uuid.uuid4(), timestamp=product_fixtures_creation_timestamp,
                               name="black DC skater shoes", quantity=10, description="yo!",
                               price=Decimal("149.99"), enabled=True),
                ProductFixture(_id=uuid.uuid4(), timestamp=product_fixtures_creation_timestamp,
                               name="warm gloves", quantity=15, description="elegant, warm, one-size-fits-all gloves",
                               price=Decimal("19.99"), enabled=True)]

    for product in products:
        product_fixture_cassandra_store.create(product)
    return products


@pytest.fixture("module")
def cassandra_update_fetcher(cassandra_cluster, settings):
    return CassandraUpdateFetcher(cassandra_cluster, settings)


# noinspection PyShadowingNames,PyMethodMayBeStatic,PyClassHasNoInit
@pytest.mark.usefixtures("setup")
class TestCassandraUpdateFetcher:

    def test_fetch_updates_for_product_fixtures_creation_from_beginning_of_time(
            self, cassandra_update_fetcher, product_fixtures,
            cassandra_fixture_keyspace, product_fixtures_creation_timestamp):

        updates = cassandra_update_fetcher.fetch_updates()
        updates_by_key = self.group_updates_by_unique_key(updates)

        assert len(updates_by_key) == 4
        for product_fixture in product_fixtures:
            key = str(product_fixture.key)
            assert key in updates_by_key
            update = updates_by_key[key]

            self.check_update_timestamp(update, product_fixtures_creation_timestamp)
            self.check_update_identifier(update, cassandra_fixture_keyspace, ProductFixture.TABLE_NAME)
            self.check_update_matches_product_fixture_creation(update, product_fixture)

    def test_fetch_updates_for_product_fixtures_creation_with_minimum_timestamp(
            self, cassandra_update_fetcher, product_fixtures,
            cassandra_fixture_keyspace, product_fixtures_creation_timestamp):

        updates = cassandra_update_fetcher.fetch_updates(product_fixtures_creation_timestamp)
        updates_by_key = self.group_updates_by_unique_key(updates)

        assert len(updates_by_key) == 4
        for product_fixture in product_fixtures:
            key = str(product_fixture.key)
            assert key in updates_by_key
            update = updates_by_key[key]

            self.check_update_timestamp(update, product_fixtures_creation_timestamp)
            self.check_update_identifier(update, cassandra_fixture_keyspace, ProductFixture.TABLE_NAME)
            self.check_update_matches_product_fixture_creation(update, product_fixture)

    def test_fetch_updates_for_product_fixtures_updates(
            self, cassandra_update_fetcher, product_fixtures, product_fixture_cassandra_store):

        update_time = arrow.utcnow()

        product_fixtures[0].name = "name update"
        product_fixtures[0].description = "description update"
        product_fixture_cassandra_store.update(product_fixtures[0])

        product_fixture_cassandra_store.delete(product_fixtures[1])

        product_fixtures[2].quantity = 100
        product_fixture_cassandra_store.update(product_fixtures[2])

        product_fixtures[3].price = Decimal("29.99")
        product_fixtures[3].enabled = False
        product_fixture_cassandra_store.update(product_fixtures[3])

        updates = cassandra_update_fetcher.fetch_updates(update_time.float_timestamp)

        updates_by_key = self.group_updates_by_unique_key(updates)
        assert len(updates_by_key) == 4

        assert updates_by_key[product_fixtures[0].key].is_delete is False
        self.check_update_fields(updates_by_key[product_fixtures[0].key],
                                 {"name": "name update", "description": "description update"})

        assert updates_by_key[product_fixtures[1].key].is_delete is True
        self.check_update_fields(updates_by_key[product_fixtures[1].key], {})

        assert updates_by_key[product_fixtures[2].key].is_delete is False
        self.check_update_fields(updates_by_key[product_fixtures[2].key], {"quantity": 100})

        assert updates_by_key[product_fixtures[3].key].is_delete is False
        self.check_update_fields(updates_by_key[product_fixtures[3].key], {"price": Decimal("29.99"), "enabled": False})

    def test_fetch_multiple_save_updates_for_the_same_entity(self, cassandra_update_fetcher,
                                                             product_fixture_cassandra_store):
        _id = uuid.uuid4()
        timestamp = time()

        product_fixture = ProductFixture(_id=_id, timestamp=timestamp, name="The new MacBook Pro by Apple", quantity=10,
                                         description="it's amazing", price=Decimal("1999.99"), enabled=True)
        product_fixture_cassandra_store.create(product_fixture)

        product_fixture.price = Decimal("1499.99")
        product_fixture.timestamp = time()
        product_fixture_cassandra_store.update(product_fixture)

        product_fixture.description = "it' beyond amazing"
        product_fixture.enabled = False
        product_fixture.timestamp = time()
        product_fixture_cassandra_store.update(product_fixture)

        updates_iterator = cassandra_update_fetcher.fetch_updates(minimum_timestamp=timestamp)

        updates = []
        for update in updates_iterator:
            updates.append(update)

        assert len(updates) == 3

        fields = {}
        for update in updates:
            for field in update.fields:
                fields[field.name] = field.value

        assert fields["price"] == Decimal("1499.99")
        assert fields["description"] == "it' beyond amazing"
        assert fields["enabled"] is False

    @staticmethod
    def check_update_identifier(update, keyspace, table):
        assert update.identifier.namespace == keyspace
        assert update.identifier.table == table

    @staticmethod
    def check_update_timestamp(update, operation_timestamp):
        assert time() >= update.timestamp
        assert abs(update.timestamp - operation_timestamp) <= 0.001

    @staticmethod
    def check_update_matches_product_fixture_creation(update, product_fixture):
        assert update.is_delete is False
        assert len(update.fields) == 5
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
    def group_updates_by_unique_key(updates):
        updates_by_key = {}
        for update in updates:
            key = update.identifier.key
            assert key not in updates_by_key
            updates_by_key[key] = update
        return updates_by_key
