from decimal import Decimal
from time import sleep, time
from uuid import uuid4
from datetime import datetime
import pytest
from test.fixtures.product import ProductFixture


@pytest.fixture(scope="function")
def product_fixtures():
    timestamp = time()
    return [ProductFixture(_id=uuid4(), name="navy polo shirt", quantity=5, description="great shirt, great price!",
                           price=Decimal("99.99"), enabled=True, publish_date=datetime.utcnow(),
                           external_id=uuid4(), timestamp=timestamp),
            ProductFixture(_id=uuid4(), name="cool red shorts", quantity=7, description="perfect to go to the beach",
                           price=Decimal("49.99"), enabled=False, publish_date=datetime.utcnow(),
                           external_id=uuid4(), timestamp=timestamp),
            ProductFixture(_id=uuid4(), name="black DC skater shoes", quantity=10, description="yo!",
                           price=Decimal("149.99"), enabled=True, publish_date=datetime.utcnow(),
                           external_id=uuid4(), timestamp=timestamp),
            ProductFixture(_id=uuid4(), name="black shirt", quantity=20, description="black is the new black",
                           price=Decimal("39.99"), enabled=True, publish_date=datetime.utcnow(),
                           external_id=uuid4(), timestamp=timestamp),
            ProductFixture(_id=uuid4(), name="white boxers", quantity=10, description="5 units in a package",
                           price=Decimal("10.99"), enabled=False, publish_date=datetime.utcnow(),
                           external_id=uuid4(), timestamp=timestamp),
            ProductFixture(_id=uuid4(), name="armani business shirt", quantity=2, description="dress like a boss",
                           price=Decimal("299.99"), enabled=True, publish_date=datetime.utcnow(),
                           external_id=uuid4(), timestamp=timestamp)
            ]


# noinspection PyMethodMayBeStatic,PyClassHasNoInit,PyShadowingNames
@pytest.mark.slow
class TestBidirectionalSync:

    def test_bidirectional_initial_sync(self, product_fixtures, product_fixture_cassandra_store,
                          product_fixture_elasticsearch_store, sync_interval_time):

        for product in product_fixtures:
            product_fixture_cassandra_store.create(product)
            product_fixture_elasticsearch_store.create(product)

        # Waits a long time to make sure no deadlocks or cycles are being generated.
        sleep(sync_interval_time * 10)

        for product in product_fixtures:
            from_elasticsearch = product_fixture_elasticsearch_store.read(product.id)
            from_cassandra = product_fixture_cassandra_store.read(product.id)
            assert from_elasticsearch
            assert from_cassandra
            assert from_elasticsearch == from_cassandra

    def test_bidirectional_incremental_sync(self, product_fixtures, product_fixture_cassandra_store,
                                            product_fixture_elasticsearch_store, sync_interval_time):

        for product in product_fixtures:
            product_fixture_cassandra_store.create(product)
            product_fixture_elasticsearch_store.create(product)

        sleep(sync_interval_time)

        product_fixtures[0].name = "foo"
        product_fixtures[0].timestamp = time()
        product_fixture_cassandra_store.update(product_fixtures[0])

        product_fixtures[1].price = Decimal("23.99")
        product_fixtures[1].timestamp = time()
        product_fixture_elasticsearch_store.update(product_fixtures[1])

        product_fixtures[2].quantity = 3
        product_fixtures[2].timestamp = time()
        product_fixture_cassandra_store.update(product_fixtures[2])

        sleep(sync_interval_time)

        deleted_from_cassandra = product_fixtures.pop(3)
        product_fixture_cassandra_store.delete(deleted_from_cassandra)

        sleep(sync_interval_time)

        for product in product_fixtures:
            from_elasticsearch = product_fixture_elasticsearch_store.read(product.id)
            from_cassandra = product_fixture_cassandra_store.read(product.id)
            assert from_elasticsearch
            assert from_cassandra
            assert from_elasticsearch == from_cassandra

        assert not product_fixture_cassandra_store.read(deleted_from_cassandra.id)
        assert not product_fixture_elasticsearch_store.read(deleted_from_cassandra.id)
