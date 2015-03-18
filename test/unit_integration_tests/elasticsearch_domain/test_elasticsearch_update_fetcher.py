from decimal import Decimal
from time import time, sleep
from uuid import uuid4
import pytest
from app.core.util.timestamp_util import TimestampUtil
from app.elasticsearch_domain.elasticsearch_update_fetcher import ElasticsearchUpdateFetcher
from app.elasticsearch_domain.invalid_elasticsearch_schema_exception import InvalidElasticsearchSchemaException
from test.fixtures.product import ProductFixture


@pytest.fixture(scope="function", autouse=True)
def setup(product_fixture_elasticsearch_store):
    product_fixture_elasticsearch_store.delete_all()


@pytest.fixture
def elasticsearch_update_fetcher(elasticsearch_client, settings):
    return ElasticsearchUpdateFetcher(elasticsearch_client, settings)


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
class TestElasticsearchUpdateFetcher:

    def test_fetch_updates_from_the_beginning_of_time(self, elasticsearch_update_fetcher,
                                                      product_fixture_elasticsearch_store, elasticsearch_fixture_index,
                                                      product_fixture_table):

        p1 = ProductFixture(_id=uuid4(), name="navy polo shirt", description="great shirt, great price!",
                            quantity=5, price=Decimal("99.99"), enabled=True, timestamp=time())
        p2 = ProductFixture(_id=uuid4(), name="cool red shorts", description="perfect to go to the beach",
                            quantity=7, price=Decimal("49.99"), enabled=False, timestamp=time())
        p3 = ProductFixture(_id=uuid4(), name="black DC skater shoes", description="yo!",
                            quantity=10, price=Decimal("149.99"), enabled=True, timestamp=time())

        product_fixture_elasticsearch_store.create(p1)
        product_fixture_elasticsearch_store.create(p2)
        product_fixture_elasticsearch_store.create(p3)

        sleep(0.001)

        p3.description = "very cool shoes"
        p3.price = Decimal("199.99")
        p3.timestamp = time()
        product_fixture_elasticsearch_store.update(p3)

        p4 = ProductFixture(_id=uuid4(), name="black gloves", description="warm, comfortable gloves",
                            quantity=7, price=Decimal("19.99"), enabled=True, timestamp=time())
        product_fixture_elasticsearch_store.create(p4)

        p4.description = "warm, comfortable, one-size fits all gloves"
        p4.quantity = 19
        p4.timestamp = time()
        product_fixture_elasticsearch_store.update(p4)

        p1.quantity = 10
        p1.timestamp = time()
        product_fixture_elasticsearch_store.update(p1)

        products = (p1, p2, p3, p4)

        updates_iterator = elasticsearch_update_fetcher.fetch_updates()
        product_updates = self.filter_documents_by_type(
            elasticsearch_fixture_index, product_fixture_table, updates_iterator)

        assert len(product_updates) == len(products)

        updates_by_key = self.group_updates_by_key(product_updates)

        for product in products:
            update = updates_by_key[product.key]
            assert abs(product.timestamp - update.timestamp) < 0.001
            assert product.name == update.get_field_value("name")
            assert product.description == update.get_field_value("description")
            assert product.price == Decimal(update.get_field_value("price"))
            assert product.enabled == update.get_field_value("enabled")
            assert product.quantity == update.get_field_value("quantity")

    def test_fetch_updates_with_minimum_time(self, elasticsearch_update_fetcher,
                                             product_fixture_elasticsearch_store, elasticsearch_fixture_index,
                                             product_fixture_table):

        before_minimum_timestamp = time()

        p1 = ProductFixture(_id=uuid4(), name="navy polo shirt", description="great shirt, great price!",
                            quantity=5, price=Decimal("99.99"), enabled=True, timestamp=before_minimum_timestamp)
        p2 = ProductFixture(_id=uuid4(), name="cool red shorts", description="perfect to go to the beach",
                            quantity=7, price=Decimal("49.99"), enabled=False, timestamp=before_minimum_timestamp)
        p3 = ProductFixture(_id=uuid4(), name="black DC skater shoes", description="yo!",
                            quantity=10, price=Decimal("149.99"), enabled=True, timestamp=before_minimum_timestamp)

        product_fixture_elasticsearch_store.create(p1)
        product_fixture_elasticsearch_store.create(p2)
        product_fixture_elasticsearch_store.create(p3)

        sleep(0.001)

        minimum_timestamp = time()

        p3.description = "very cool shoes"
        p3.price = Decimal("199.99")
        p3.timestamp = time()
        product_fixture_elasticsearch_store.update(p3)

        p4 = ProductFixture(_id=uuid4(), name="black gloves", description="warm, comfortable gloves",
                            quantity=7, price=Decimal("19.99"), enabled=True, timestamp=time())
        product_fixture_elasticsearch_store.create(p4)

        p4.description = "warm, comfortable, one-size fits all gloves"
        p4.quantity = 19
        product_fixture_elasticsearch_store.update(p4)

        p2.quantity = 10
        p2.timestamp = time()
        product_fixture_elasticsearch_store.update(p2)

        updates_iterator = elasticsearch_update_fetcher.fetch_updates(minimum_timestamp)
        product_updates = self.filter_documents_by_type(
            elasticsearch_fixture_index, product_fixture_table, updates_iterator)

        assert len(product_updates) == 3

        updates_by_key = self.group_updates_by_key(product_updates)

        assert p2.key in updates_by_key
        assert p3.key in updates_by_key
        assert p4.key in updates_by_key
        assert p1.key not in updates_by_key

    def test_fail_if_mapping_does_not_have_timestamp_enabled(self, elasticsearch_update_fetcher,
                                                             elasticsearch_client, elasticsearch_fixture_index):
        _index = elasticsearch_fixture_index
        _type = "type_without_timestamp"
        _id = uuid4()

        bogus_mapping = {"_timestamp": {"enabled": False}}
        elasticsearch_client.indices.put_mapping(index=_index, doc_type=_type, body=bogus_mapping)

        try:
            elasticsearch_client.index(index=_index, doc_type=_type, id=_id, body={"foo": "bar"},
                                       timestamp=TimestampUtil.seconds_to_milliseconds(time()), refresh=True)

            with pytest.raises(InvalidElasticsearchSchemaException) as e:
                for _ in elasticsearch_update_fetcher.fetch_updates():
                    pass

            assert e.value.identifier.key == str(_id)
            assert "Could not retrieve '_timestamp' for Elasticsearch document" in e.value.message
        finally:
            elasticsearch_client.indices.delete_mapping(index=elasticsearch_fixture_index, doc_type=_type)

    def filter_documents_by_type(self, index, _type, documents):
        filtered_documents = []
        for document in documents:
            identifier = document.identifier
            if identifier.namespace == index and identifier.table == _type:
                filtered_documents.append(document)
        return filtered_documents

    def group_updates_by_key(self, updates):
        updates_by_key = {}
        for update in updates:
            key = update.identifier.key
            assert key not in updates_by_key
            updates_by_key[key] = update
        return updates_by_key
