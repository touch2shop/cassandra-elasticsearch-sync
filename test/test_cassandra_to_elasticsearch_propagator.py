from decimal import Decimal
from time import time, sleep
from uuid import uuid4
from datetime import datetime
import pytest
from app.cassandra_to_elasticsearch_propagator import CassandraToElasticsearchPropagator
from test.fixtures.product import ProductFixture


@pytest.fixture(scope="function", autouse=True)
def setup(cassandra_log_entry_store):
    cassandra_log_entry_store.delete_all()


@pytest.fixture(scope="module")
def propagator(cassandra_cluster, elasticsearch_client, settings):
    return CassandraToElasticsearchPropagator(cassandra_cluster, elasticsearch_client, settings)


@pytest.fixture(scope="function")
def product_fixtures():
    now = time()
    return [ProductFixture(_id=uuid4(), name="navy polo shirt", quantity=5, description="great shirt, great price!",
                           price=Decimal("99.99"), enabled=True, timestamp=now),
            ProductFixture(_id=uuid4(), name="cool red shorts", quantity=7, description="perfect to go to the beach",
                           price=Decimal("49.99"), enabled=False, timestamp=now),
            ProductFixture(_id=uuid4(), name="black DC skater shoes", quantity=10, description="yo!",
                           price=Decimal("149.99"), enabled=True, timestamp=now)]


# noinspection PyShadowingNames,PyClassHasNoInit,PyMethodMayBeStatic
class TestCassandraToElasticsearchPropagator:

    def test_propagate_creation_updates_from_the_beginning_of_time(self, propagator,
            product_fixture_cassandra_store, product_fixture_elasticsearch_store, product_fixtures):

        for product in product_fixtures:
            product_fixture_cassandra_store.create(product)

        propagator.propagate_updates(minimum_timestamp=None)

        for product in product_fixtures:
            read_from_cassandra = product_fixture_cassandra_store.read(product.id)
            read_from_elasticsearch = product_fixture_elasticsearch_store.read(product.id)
            assert product == read_from_cassandra == read_from_elasticsearch

    def test_propagate_creation_and_modification_updates_from_the_beginning_of_time(self, propagator,
            product_fixture_cassandra_store, product_fixture_elasticsearch_store, product_fixtures):

        for product in product_fixtures:
            product_fixture_cassandra_store.create(product)

        for product in product_fixtures:
            product.name = "new_name"
            product.description = "new_description"
            product.price = Decimal("98.99")
            product.enabled = True
            product.timestamp = time()
            product_fixture_cassandra_store.update(product)

        propagator.propagate_updates(minimum_timestamp=None)

        for product in product_fixtures:
            read_from_cassandra = product_fixture_cassandra_store.read(product.id)
            read_from_elasticsearch = product_fixture_elasticsearch_store.read(product.id)
            assert product == read_from_cassandra == read_from_elasticsearch

    def test_propagate_creation_modification_and_deletion_updates_from_the_beginning_of_time(self, propagator,
            product_fixture_cassandra_store, product_fixture_elasticsearch_store, product_fixtures):

        for product in product_fixtures:
            product_fixture_cassandra_store.create(product)

        for product in product_fixtures:
            product.name = "new_name"
            product.description = "new_description"
            product.timestamp = time()
            product_fixture_cassandra_store.update(product)

        deleted = product_fixtures.pop(0)
        product_fixture_cassandra_store.delete(deleted)

        propagator.propagate_updates(minimum_timestamp=None)

        for product in product_fixtures:
            read_from_cassandra = product_fixture_cassandra_store.read(product.id)
            read_from_elasticsearch = product_fixture_elasticsearch_store.read(product.id)
            assert product == read_from_cassandra == read_from_elasticsearch

        assert not product_fixture_elasticsearch_store.read(deleted.id)

    def test_only_propagate_updates_that_are_created_after_minimum_timestamp(self, propagator,
            product_fixture_cassandra_store, product_fixture_elasticsearch_store):

        before_timestamp = time()
        product_created_before = ProductFixture(_id=uuid4(), name="gloves", description="warm pair of gloves",
                                                quantity=50, timestamp=before_timestamp)
        product_fixture_cassandra_store.create(product_created_before)

        sleep(0.001)
        minimum_timestamp = time()

        products_created_after = [
            ProductFixture(uuid4(), "navy polo shirt", 5, "great shirt, great price!", timestamp=minimum_timestamp),
            ProductFixture(uuid4(), "cool red shorts", 7, "perfect to go to the beach", timestamp=minimum_timestamp),
            ProductFixture(uuid4(), "black DC skater shoes", 10, "yo!", timestamp=minimum_timestamp),
            ProductFixture(uuid4(), "regular jeans", 12, "blue, nice jeans", timestamp=minimum_timestamp)
        ]

        for product in products_created_after:
            product_fixture_cassandra_store.create(product)

        for product in products_created_after:
            product.name = "new_name"
            product.description = "new_description"
            product.timestamp = time()
            product_fixture_cassandra_store.update(product)

        propagator.propagate_updates(minimum_timestamp=datetime.utcfromtimestamp(minimum_timestamp))

        for product in products_created_after:
            read_from_cassandra = product_fixture_cassandra_store.read(product.id)
            read_from_elasticsearch = product_fixture_elasticsearch_store.read(product.id)
            assert product == read_from_cassandra == read_from_elasticsearch

        assert product_fixture_cassandra_store.read(product_created_before.id)
        assert not product_fixture_elasticsearch_store.read(product_created_before.id)

    def test_does_nothing_if_no_updates(self, propagator):
        assert propagator.propagate_updates() is None

    def test_returns_timestamp_of_the_most_recent_update(self, propagator, product_fixtures,
                                                         product_fixture_cassandra_store):

        for product in product_fixtures:
            product_fixture_cassandra_store.create(product)

        for product in product_fixtures:
            product.name = "new_name"
            product.description = "new_description"
            product.timestamp = time()
            product_fixture_cassandra_store.update(product)

        deleted = product_fixtures.pop(0)
        product_fixture_cassandra_store.delete(deleted)

        sleep(1)

        most_recent_timestamp = time()
        product_fixture_cassandra_store.create(
            ProductFixture(uuid4(), "navy polo shirt", 5, "great shirt, great price!", timestamp=most_recent_timestamp))

        actual_most_recent_timestamp = propagator.propagate_updates(minimum_timestamp=None)
        assert abs(actual_most_recent_timestamp - most_recent_timestamp) < 0.01
