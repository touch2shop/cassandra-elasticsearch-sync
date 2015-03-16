from decimal import Decimal
import uuid
import time

import pytest
from app.core.document import Document

from app.core.identifier import Identifier
from app.elasticsearch_domain.store.elasticsearch_document_store import ElasticsearchDocumentStore
from test.fixtures.product import ProductFixture


@pytest.fixture(scope="function")
def product_fixtures():
    now = time.time()
    return [ProductFixture(_id=uuid.uuid4(), name="navy polo shirt", description="great shirt, great price!",
                           quantity=5, price=Decimal("99.99"), enabled=True, timestamp=now),
            ProductFixture(_id=uuid.uuid4(), name="cool red shorts", description="perfect to go to the beach",
                           quantity=7, price=Decimal("49.99"), enabled=False, timestamp=now),
            ProductFixture(_id=uuid.uuid4(), name="black DC skater shoes", description="yo!",
                           quantity=10, price=Decimal("149.99"), enabled=True, timestamp=now)]


@pytest.fixture(scope="module")
def generic_elasticsearch_store(elasticsearch_client):
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

    def test_read_non_existent(self, generic_elasticsearch_store, elasticsearch_fixture_index, product_fixture_table):
        identifier = Identifier(elasticsearch_fixture_index, product_fixture_table, uuid.uuid4())
        read = generic_elasticsearch_store.read(identifier)
        assert read is None

    def test_create_and_read(self, product_fixtures, elasticsearch_documents, generic_elasticsearch_store,
                             elasticsearch_fixture_index, product_fixture_table):

        for document in elasticsearch_documents:
            generic_elasticsearch_store.create(document)

        for product_fixture in product_fixtures:
            identifier = Identifier(elasticsearch_fixture_index, product_fixture_table, product_fixture.key)
            read_document = generic_elasticsearch_store.read(identifier)
            assert abs(read_document.timestamp - product_fixture.timestamp) <= 0.001
            assert read_document.identifier.key == product_fixture.key
            assert read_document.get_field_value("name") == product_fixture.name
            assert read_document.get_field_value("description") == product_fixture.description
            assert read_document.get_field_value("quantity") == product_fixture.quantity
            assert read_document.get_field_value("enabled") == product_fixture.enabled
            assert read_document.get_field_value("price") == str(product_fixture.price)

    def test_full_update(self, elasticsearch_documents, generic_elasticsearch_store):

        for document in elasticsearch_documents:
            generic_elasticsearch_store.create(document)

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
            generic_elasticsearch_store.update(document)

        for document in elasticsearch_documents:
            read_document = generic_elasticsearch_store.read(document.identifier)
            assert read_document.get_field_value("name") == new_name
            assert read_document.get_field_value("description") == new_description
            assert read_document.get_field_value("quantity") == new_quantity
            assert read_document.get_field_value("enabled") == new_enabled
            assert read_document.get_field_value("price") == str(new_price)

    def test_partial_update(self, elasticsearch_documents, generic_elasticsearch_store):

        for document in elasticsearch_documents:
            generic_elasticsearch_store.create(document)

        new_description = "updated_description"
        new_quantity = 99

        for document in elasticsearch_documents:
            document.set_field_value("description", new_description)
            document.set_field_value("quantity", new_quantity)
            generic_elasticsearch_store.update(document)

        for document in elasticsearch_documents:
            read_document = generic_elasticsearch_store.read(document.identifier)
            assert read_document.get_field_value("name") is not None
            assert read_document.get_field_value("price") is not None
            assert read_document.get_field_value("enabled") is not None
            assert read_document.get_field_value("description") == new_description
            assert read_document.get_field_value("quantity") == new_quantity

    def test_delete(self, elasticsearch_documents, generic_elasticsearch_store, elasticsearch_client):

        for document in elasticsearch_documents:
            generic_elasticsearch_store.create(document)

        for document in elasticsearch_documents:
            generic_elasticsearch_store.delete(document.identifier)

        for document in elasticsearch_documents:
            identifier = document.identifier
            assert not elasticsearch_client.exists(index=identifier.namespace,
                                                   doc_type=identifier.table, id=identifier.key)
