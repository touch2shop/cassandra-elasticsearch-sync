from decimal import Decimal
from time import time
from uuid import uuid4, UUID
import pytest

from app.core.model.document import Document
from app.core.model.identifier import Identifier
from app.elasticsearch_domain.store.elasticsearch_document_store import ElasticsearchDocumentStore
from test.fixtures.product import ProductFixture


@pytest.fixture(scope="function")
def product_fixtures():
    now = time()
    return [ProductFixture(_id=uuid4(), name="navy polo shirt", description="great shirt, great price!",
                           quantity=5, price=Decimal("99.99"), enabled=True, timestamp=now),
            ProductFixture(_id=uuid4(), name="cool red shorts", description="perfect to go to the beach",
                           quantity=7, price=Decimal("49.99"), enabled=False, timestamp=now),
            ProductFixture(_id=uuid4(), name="black DC skater shoes", description="yo!",
                           quantity=10, price=Decimal("149.99"), enabled=True, timestamp=now)]


@pytest.fixture(scope="module")
def elasticsearch_document_store(elasticsearch_client):
    return ElasticsearchDocumentStore(elasticsearch_client)


# noinspection PyShadowingNames
@pytest.fixture(scope="function")
def elasticsearch_documents(product_fixtures, elasticsearch_fixture_index, product_fixture_table):
    documents = []
    for product_fixture in product_fixtures:
        document = build_elasticsearch_document(elasticsearch_fixture_index, product_fixture_table, product_fixture)
        documents.append(document)
    return documents


def build_elasticsearch_document(index, _type, product_fixture):
    document = Document()
    document.identifier = Identifier(index, _type, product_fixture.id)
    document.add_field("name", product_fixture.name)
    document.add_field("description", product_fixture.description)
    document.add_field("quantity", product_fixture.quantity)
    document.add_field("price", product_fixture.price)
    document.add_field("enabled", product_fixture.enabled)
    document.timestamp = product_fixture.timestamp
    return document


# noinspection PyClassHasNoInit,PyMethodMayBeStatic,PyShadowingNames
class TestGenericElasticsearchStore:

    def test_read_non_existent(self, elasticsearch_document_store, elasticsearch_fixture_index, product_fixture_table):
        identifier = Identifier(elasticsearch_fixture_index, product_fixture_table, uuid4())
        read = elasticsearch_document_store.read(identifier)
        assert read is None

    def test_create_and_read(self, product_fixtures, elasticsearch_documents, elasticsearch_document_store,
                             elasticsearch_fixture_index, product_fixture_table):

        for document in elasticsearch_documents:
            elasticsearch_document_store.create(document)

        for product_fixture in product_fixtures:
            identifier = Identifier(elasticsearch_fixture_index, product_fixture_table, product_fixture.key)
            read_document = elasticsearch_document_store.read(identifier)
            assert abs(read_document.timestamp - product_fixture.timestamp) <= 0.001
            assert read_document.identifier.key == product_fixture.key
            assert read_document.get_field_value("name") == product_fixture.name
            assert read_document.get_field_value("description") == product_fixture.description
            assert read_document.get_field_value("quantity") == product_fixture.quantity
            assert read_document.get_field_value("enabled") == product_fixture.enabled
            assert read_document.get_field_value("price") == str(product_fixture.price)

    def test_full_update(self, elasticsearch_documents, elasticsearch_document_store):

        for document in elasticsearch_documents:
            elasticsearch_document_store.create(document)

        new_name = "updated_name"
        new_description = "updated_description"
        new_quantity = 99
        new_enabled = False
        new_price = Decimal("149.89")

        for document in elasticsearch_documents:
            document.set_field_value("name", new_name)
            document.set_field_value("description", new_description)
            document.set_field_value("quantity", new_quantity)
            document.set_field_value("enabled", new_enabled)
            document.set_field_value("price", new_price)
            elasticsearch_document_store.update(document)

        for document in elasticsearch_documents:
            read_document = elasticsearch_document_store.read(document.identifier)
            assert read_document.get_field_value("name") == new_name
            assert read_document.get_field_value("description") == new_description
            assert read_document.get_field_value("quantity") == new_quantity
            assert read_document.get_field_value("enabled") == new_enabled
            assert read_document.get_field_value("price") == str(new_price)

    def test_partial_update(self, elasticsearch_documents, elasticsearch_document_store):

        for document in elasticsearch_documents:
            elasticsearch_document_store.create(document)

        new_description = "updated_description"
        new_quantity = 99

        for document in elasticsearch_documents:
            document.set_field_value("description", new_description)
            document.set_field_value("quantity", new_quantity)
            elasticsearch_document_store.update(document)

        for document in elasticsearch_documents:
            read_document = elasticsearch_document_store.read(document.identifier)
            assert read_document.get_field_value("name") is not None
            assert read_document.get_field_value("price") is not None
            assert read_document.get_field_value("enabled") is not None
            assert read_document.get_field_value("description") == new_description
            assert read_document.get_field_value("quantity") == new_quantity

    def test_delete(self, elasticsearch_documents, elasticsearch_document_store, elasticsearch_client):

        for document in elasticsearch_documents:
            elasticsearch_document_store.create(document)

        for document in elasticsearch_documents:
            elasticsearch_document_store.delete(document.identifier)

        for document in elasticsearch_documents:
            identifier = document.identifier
            assert not elasticsearch_client.exists(index=identifier.namespace,
                                                   doc_type=identifier.table, id=identifier.key)

    def filter_documents_by_type(self, index, _type, documents):
        filtered_documents = []
        for document in documents:
            identifier = document.identifier
            if identifier.namespace == index and identifier.table == _type:
                filtered_documents.append(document)
        return filtered_documents

    def group_documents_by_key(self, documents):
        grouped = {}
        for document in documents:
            grouped[document.identifier.key] = document
        return grouped

    def test_search_all(self, product_fixture_elasticsearch_store, elasticsearch_document_store,
                        elasticsearch_fixture_index, product_fixture_table):
        products = [
            ProductFixture(_id=uuid4(), name="Apple iPhone 6s",
                           description="the new iPhone", price="1999.99", timestamp=time()),
            ProductFixture(_id=uuid4(), name="Samsung Galaxy A+",
                           description="beats iPhone", price="999.99", timestamp=time()),
            ProductFixture(_id=uuid4(), name="Nokia Ultimate 89",
                           description="windows phone 10", price="899.99", timestamp=time())]

        for product in products:
            product_fixture_elasticsearch_store.create(product)

        search_result = elasticsearch_document_store.search_all()

        filtered_documents = self.filter_documents_by_type(
            elasticsearch_fixture_index, product_fixture_table, search_result)
        assert len(filtered_documents) >= len(products)

        documents_by_key = self.group_documents_by_key(filtered_documents)

        for product in products:
            document = documents_by_key[str(product.id)]
            assert document
            assert abs(document.timestamp - product.timestamp) < 0.001
            assert product.name == document.get_field_value("name")
            assert product.description == document.get_field_value("description")
            assert product.price == document.get_field_value("price")

    def test_search_by_minimum_timestamp(self, product_fixture_elasticsearch_store, elasticsearch_document_store):

        base_timestamp = time()

        p1 = ProductFixture(_id=uuid4(), name="1", timestamp=base_timestamp)
        p2 = ProductFixture(_id=uuid4(), name="2", timestamp=base_timestamp + 1)
        p3 = ProductFixture(_id=uuid4(), name="3", timestamp=base_timestamp + 2)
        p4 = ProductFixture(_id=uuid4(), name="4", timestamp=base_timestamp + 3)

        minimum_timestamp = (base_timestamp + 3) + 0.001

        p5 = ProductFixture(_id=uuid4(), name="5", timestamp=minimum_timestamp)
        p6 = ProductFixture(_id=uuid4(), name="6", timestamp=minimum_timestamp + 1)
        p7 = ProductFixture(_id=uuid4(), name="7", timestamp=minimum_timestamp + 2)
        p8 = ProductFixture(_id=uuid4(), name="8", timestamp=minimum_timestamp + 3)

        products = [p1, p2, p3, p4, p5, p6, p7, p8]

        for p in products:
            product_fixture_elasticsearch_store.create(p)

        found_documents = elasticsearch_document_store.search_by_minimum_timestamp(minimum_timestamp).to_list()

        assert len(found_documents) == 4

        found_ids = []
        for document in found_documents:
            found_ids.append(UUID(document.identifier.key))

        assert p5.id in found_ids
        assert p6.id in found_ids
        assert p7.id in found_ids
        assert p8.id in found_ids
