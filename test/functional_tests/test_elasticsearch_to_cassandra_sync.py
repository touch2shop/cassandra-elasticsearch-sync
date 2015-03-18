from decimal import Decimal
from time import time, sleep
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
                           external_id=uuid4(), timestamp=timestamp)]


# noinspection PyClassHasNoInit,PyMethodMayBeStatic,PyShadowingNames
@pytest.mark.slow
class TestElasticsearchToCassandraSync:

    def test_initial_sync(self, product_fixtures, product_fixture_cassandra_store,
                          product_fixture_elasticsearch_store, sync_interval_time):

        for product in product_fixtures:
            product_fixture_elasticsearch_store.create(product)

        sleep(sync_interval_time)

        for product in product_fixtures:
            assert product_fixture_cassandra_store.read(product.id)

    def test_initial_sync_then_incremental_saves(self,
            product_fixtures, product_fixture_cassandra_store, product_fixture_elasticsearch_store, sync_interval_time):

        for product in product_fixtures:
            product_fixture_elasticsearch_store.create(product)

        sleep(sync_interval_time)

        for product in product_fixtures:
            product.name = "new name"
            product.description = "new description"
            product.quantity = 100
            product.price = Decimal("299.99")
            product.publish_date = datetime.utcnow()
            product.external_id = uuid4()
            product.timestamp = time()
            product_fixture_elasticsearch_store.update(product)

        sleep(sync_interval_time)

        product_fixtures[0].description = "another description"
        product_fixtures[0].timestamp = time()
        product_fixture_elasticsearch_store.update(product_fixtures[0])

        product_fixtures[1].price = Decimal("1.99")
        product_fixtures[1].timestamp = time()
        product_fixture_elasticsearch_store.update(product_fixtures[1])

        sleep(sync_interval_time)

        new_product = ProductFixture(_id=uuid4(), name="black gloves", quantity=10, description="comfortable, warm",
                           price=Decimal("29.99"), enabled=False, publish_date=datetime.utcnow(),
                           external_id=uuid4(), timestamp=time())
        product_fixture_elasticsearch_store.create(new_product)
        product_fixtures.append(new_product)

        sleep(sync_interval_time)

        for product in product_fixtures:
            product_read_from_elasticsearch = product_fixture_cassandra_store.read(product.id)
            assert product_read_from_elasticsearch
            assert product == product_read_from_elasticsearch

    def test_initial_sync_then_incremental_deletes_do_not_work(self,
            product_fixtures, product_fixture_cassandra_store, product_fixture_elasticsearch_store, sync_interval_time):

        for product in product_fixtures:
            product_fixture_elasticsearch_store.create(product)

        sleep(sync_interval_time)

        for product in product_fixtures:
            product.name = "new name"
            product.description = "new description"
            product.quantity = 100
            product.price = Decimal("299.99")
            product.publish_date = datetime.utcnow()
            product.external_id = uuid4()
            product.timestamp = time()
            product_fixture_elasticsearch_store.update(product)

        sleep(sync_interval_time)

        for product in product_fixtures:
            product_fixture_elasticsearch_store.delete(product.id)

        sleep(sync_interval_time)

        new_product = ProductFixture(_id=uuid4(), name="black gloves", quantity=10, description="comfortable, warm",
                           price=Decimal("29.99"), enabled=False, publish_date=datetime.utcnow(),
                           external_id=uuid4(), timestamp=time())
        product_fixture_elasticsearch_store.create(new_product)

        sleep(sync_interval_time)

        for product in product_fixtures:
            assert product_fixture_cassandra_store.read(product.id)

        assert product_fixture_cassandra_store.read(new_product.id)
