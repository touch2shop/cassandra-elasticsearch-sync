from decimal import Decimal
from time import time
from uuid import uuid4
from datetime import datetime
import pytest
from app.cassandra_domain.store.cassandra_document_store import CassandraDocumentStore
from app.core.model.document import Document
from app.core.model.identifier import Identifier
from app.cassandra_domain.invalid_cassandra_schema_exception import InvalidCassandraSchemaException
from app.core.util.datetime_util import DateTimeUtil
from app.core.util.timestamp_util import TimestampUtil
from test.fixtures.product import ProductFixture


@pytest.fixture(scope="module")
def cassandra_document_store(cassandra_cluster):
    return CassandraDocumentStore(cassandra_cluster)


@pytest.fixture(scope="function")
def product():
    return ProductFixture(_id=uuid4(), timestamp=time(), name="jeans", description="cool jeans",
                          price=Decimal("19.99"), quantity=7, publish_date=datetime.utcnow(),
                          external_id=uuid4(), enabled=False)


# noinspection PyClassHasNoInit,PyShadowingNames,PyMethodMayBeStatic
class TestCassandraDocumentStore:

    def test_read(self, cassandra_document_store, product,
                  product_fixture_cassandra_store, cassandra_fixture_keyspace, product_fixture_table):

        product_fixture_cassandra_store.create(product)

        identifier = Identifier(cassandra_fixture_keyspace, product_fixture_table, product.key)
        document = cassandra_document_store.read(identifier)

        assert TimestampUtil.are_equal_by_less_than(product.timestamp, document.timestamp, 0.001)
        assert document.identifier == identifier
        assert document.get_field_value("name") == product.name
        assert document.get_field_value("description") == product.description
        assert document.get_field_value("price") == product.price
        assert document.get_field_value("quantity") == product.quantity
        assert document.get_field_value("enabled") == product.enabled
        assert document.get_field_value("external_id") == product.external_id
        assert DateTimeUtil.are_equal_by_less_than(
            document.get_field_value("publish_date"), product.publish_date, 0.001)

    def test_read_not_found(self, cassandra_document_store, cassandra_fixture_keyspace, product_fixture_table):
        identifier = Identifier(cassandra_fixture_keyspace, product_fixture_table, uuid4())
        assert not cassandra_document_store.read(identifier)

    def test_fail_if_table_has_no_timestamp(self, cassandra_document_store, cassandra_fixture_client,
                                            cassandra_fixture_keyspace):
        cassandra_fixture_client.execute(
            """
            CREATE TABLE table_without_timestamp (
              id uuid PRIMARY KEY,
              whatever text
            )
            """)

        statement = cassandra_fixture_client.prepare_statement(
            """
            INSERT INTO table_without_timestamp (id,whatever) VALUES (?,?)
            """)

        _id = uuid4()
        whatever = "whatever"
        cassandra_fixture_client.execute(statement, (_id, whatever))

        identifier = Identifier(cassandra_fixture_keyspace, "table_without_timestamp", str(_id))
        with pytest.raises(InvalidCassandraSchemaException) as e:
            cassandra_document_store.read(identifier)
        assert e.value.identifier == identifier
        assert "No timestamp column found for entity on Cassandra." in e.value.message

    def test_fail_if_id_is_not_unique(self, cassandra_document_store, cassandra_fixture_client,
                                      cassandra_fixture_keyspace):

        cassandra_fixture_client.execute(
            """
            CREATE TABLE table_with_composite_key (
              id uuid,
              timestamp timestamp,
              whatever text,
              PRIMARY KEY (id, timestamp)
            )
            """)

        statement = cassandra_fixture_client.prepare_statement(
            """
            INSERT INTO table_with_composite_key (id,timestamp,whatever) VALUES (?,?,?)
            """)

        _id = uuid4()
        timestamp1 = TimestampUtil.seconds_to_milliseconds(time())
        timestamp2 = TimestampUtil.seconds_to_milliseconds(time() + 1)
        whatever = "whatever"

        cassandra_fixture_client.execute(statement, (_id, timestamp1, whatever))
        cassandra_fixture_client.execute(statement, (_id, timestamp2, whatever))

        identifier = Identifier(cassandra_fixture_keyspace, "table_with_composite_key", str(_id))
        with pytest.raises(InvalidCassandraSchemaException) as e:
            cassandra_document_store.read(identifier)
        assert e.value.identifier == identifier
        assert "More than one row found for entity on Cassandra." in e.value.message

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
        assert read_product.external_id == document.get_field_value("external_id")
        assert read_product.description == product.description

    def test_delete(self, cassandra_document_store, product, product_fixture_cassandra_store,
                    cassandra_fixture_keyspace, product_fixture_table):

        product_fixture_cassandra_store.create(product)
        document = self.to_document(product, cassandra_fixture_keyspace, product_fixture_table)

        cassandra_document_store.delete(document.identifier)

        assert not product_fixture_cassandra_store.read(product.id)

    def test_serialize_string_as_decimal_if_column_type_is_decimal(self,
            cassandra_document_store, cassandra_fixture_keyspace, product_fixture_table):
        document = Document()
        document.identifier = Identifier(cassandra_fixture_keyspace, product_fixture_table, uuid4())
        document.timestamp = time()
        document.add_field("price", "99.99")

        cassandra_document_store.create(document)
        read_document = cassandra_document_store.read(document.identifier)
        assert read_document.get_field_value("price") == Decimal("99.99")

    def test_serialize_string_as_uuid_if_column_type_is_uuid(self,
            cassandra_document_store, cassandra_fixture_keyspace, product_fixture_table):
        external_id = uuid4()

        document = Document()
        document.identifier = Identifier(cassandra_fixture_keyspace, product_fixture_table, uuid4())
        document.timestamp = time()
        document.add_field("external_id", str(external_id))

        cassandra_document_store.create(document)
        read_document = cassandra_document_store.read(document.identifier)
        assert read_document.get_field_value("external_id") == external_id

    def test_serialize_string_as_datetime_if_column_type_is_datetime(self,
            cassandra_document_store, cassandra_fixture_keyspace, product_fixture_table):
        publish_date = datetime.utcnow()

        document = Document()
        document.identifier = Identifier(cassandra_fixture_keyspace, product_fixture_table, uuid4())
        document.timestamp = time()
        document.add_field("publish_date", str(publish_date))

        cassandra_document_store.create(document)
        read_document = cassandra_document_store.read(document.identifier)
        assert DateTimeUtil.are_equal_by_less_than(read_document.get_field_value("publish_date"), publish_date, 0.001)

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
        document.add_field("publish_date", product.publish_date)
        document.add_field("external_id", product.external_id)
        return document
