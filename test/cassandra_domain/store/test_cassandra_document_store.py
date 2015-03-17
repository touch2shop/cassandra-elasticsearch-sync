from decimal import Decimal
from time import time
from uuid import uuid4
import pytest
from app.cassandra_domain.store.cassandra_document_store import CassandraDocumentStore
from app.core.model.document import Document
from app.core.model.identifier import Identifier
from test.fixtures.product import ProductFixture


@pytest.fixture(scope="module")
def cassandra_document_store(cassandra_cluster):
    return CassandraDocumentStore(cassandra_cluster)


@pytest.fixture(scope="function")
def product():
    return ProductFixture(_id=uuid4(), timestamp=time(), name="jeans", description="cool jeans",
                          price=Decimal("19.99"), quantity=7, enabled=False)


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
class TestCassandraDocumentStore:

    def test_read(self, cassandra_document_store, product,
                  product_fixture_cassandra_store, cassandra_fixture_keyspace, product_fixture_table):

        product_fixture_cassandra_store.create(product)

        identifier = Identifier(cassandra_fixture_keyspace, product_fixture_table, product.key)
        document = cassandra_document_store.read(identifier)

        assert abs(product.timestamp - document.timestamp) < 0.001
        assert document.identifier == identifier
        assert document.get_field_value("name") == product.name
        assert document.get_field_value("description") == product.description
        assert document.get_field_value("price") == product.price
        assert document.get_field_value("quantity") == product.quantity
        assert document.get_field_value("enabled") == product.enabled

    def test_read_not_found(self, cassandra_document_store, cassandra_fixture_keyspace, product_fixture_table):
        identifier = Identifier(cassandra_fixture_keyspace, product_fixture_table, uuid4())
        assert not cassandra_document_store.read(identifier)

    def test_fail_if_table_has_no_timestamp(self, cassandra_fixture_client):
        # TODO
        pass

    def test_fail_if_id_is_not_unique(self):
        # TODO
        pass

    def test_create(self, cassandra_document_store, product, product_fixture_cassandra_store,
                    cassandra_fixture_keyspace, product_fixture_table):

        document = self.to_document(product, cassandra_fixture_keyspace, product_fixture_table)
        cassandra_document_store.create(document)

        read_product = product_fixture_cassandra_store.read(product.id)
        assert product == read_product

    def test_update(self, cassandra_document_store, product, product_fixture_cassandra_store,
                    cassandra_fixture_keyspace, product_fixture_table):

        product_fixture_cassandra_store.create(product)

        document = self.to_document(product, cassandra_fixture_keyspace, product_fixture_table)
        document.set_field_value("name", "useless gadget")
        document.set_field_value("quantity", 99)
        document.set_field_value("enabled", False)
        document.set_field_value("price", Decimal("2.99"))

        cassandra_document_store.update(document)

        read_product = product_fixture_cassandra_store.read(product.id)
        assert read_product.name == document.get_field_value("name")
        assert read_product.quantity == document.get_field_value("quantity")
        assert read_product.enabled == document.get_field_value("enabled")
        assert read_product.price == document.get_field_value("price")
        assert read_product.description == product.description

    def test_delete(self, cassandra_document_store, product, product_fixture_cassandra_store,
                    cassandra_fixture_keyspace, product_fixture_table):

        product_fixture_cassandra_store.create(product)
        document = self.to_document(product, cassandra_fixture_keyspace, product_fixture_table)

        cassandra_document_store.delete(document.identifier)

        assert not product_fixture_cassandra_store.read(product.id)

    @staticmethod
    def to_document(product, keyspace, table):
        document = Document()
        document.identifier = Identifier(keyspace, table, product.key)
        document.timestamp = product.timestamp
        document.add_field("name", product.name)
        document.add_field("description", product.description)
        document.add_field("price", product.price)
        document.add_field("quantity", product.quantity)
        document.add_field("enabled", product.enabled)
        return document
