import uuid
import time

import pytest

from app.core.identifier import Identifier
from app.elasticsearch_domain.generic_elasticsearch_document import GenericElasticsearchDocument
from app.elasticsearch_domain.store.generic_elasticsearch_store import GenericElasticsearchStore
from test.fixture.product import ProductFixture


@pytest.fixture(scope="function")
def product_fixtures():
    now = time.time()
    products = [ProductFixture(uuid.uuid4(), "navy polo shirt", 5, "great shirt, great price!", timestamp=now),
                ProductFixture(uuid.uuid4(), "cool red shorts", 7, "perfect to go to the beach", timestamp=now),
                ProductFixture(uuid.uuid4(), "black DC skater shoes", 10, "yo!", timestamp=now)]
    return products


@pytest.fixture(scope="module")
def generic_elasticsearch_store(elasticsearch_client):
    return GenericElasticsearchStore(elasticsearch_client)


# noinspection PyShadowingNames
@pytest.fixture(scope="function")
def elasticsearch_documents(product_fixtures, elasticsearch_fixture_index, product_fixture_table):
    documents = []
    for product_fixture in product_fixtures:
        document = build_elasticsearch_document(elasticsearch_fixture_index, product_fixture_table, product_fixture)
        documents.append(document)
    return documents


def build_elasticsearch_document(index, _type, product_fixture):
    document = GenericElasticsearchDocument()
    document.identifier = Identifier(index, _type, product_fixture.id)
    document.add_field("name", product_fixture.name)
    document.add_field("description", product_fixture.description)
    document.add_field("quantity", product_fixture.quantity)
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

    def test_full_update(self, elasticsearch_documents, generic_elasticsearch_store):

        for document in elasticsearch_documents:
            generic_elasticsearch_store.create(document)

        new_name = "updated_name"
        new_description = "updated_description"
        new_quantity = 99

        for document in elasticsearch_documents:
            document.set_field_value("name", new_name)
            document.set_field_value("description", new_description)
            document.set_field_value("quantity", new_quantity)
            generic_elasticsearch_store.update(document)

        for document in elasticsearch_documents:
            read_document = generic_elasticsearch_store.read(document.identifier)
            assert read_document.get_field_value("name") == new_name
            assert read_document.get_field_value("description") == new_description
            assert read_document.get_field_value("quantity") == new_quantity

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
            assert read_document.get_field_value("name")
            assert read_document.get_field_value("name") != ""
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
