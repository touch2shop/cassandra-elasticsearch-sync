from decimal import Decimal
from uuid import UUID

import arrow
import pytest

from app.cassandra_domain.store.abstract_cassandra_store import AbstractCassandraStore
from app.core.model.abstract_data_object import AbstractDataObject
from app.core.util.datetime_util import DateTimeUtil
from app.core.util.timestamp_util import TimestampUtil
from app.elasticsearch_domain.store.abstract_elasticsearch_store import AbstractElasticsearchStore


class ProductFixture(AbstractDataObject):

    TABLE_NAME = "product"

    def __init__(self, _id=None, name=None, quantity=None, description=None, price=None, enabled=None,
                 timestamp=None, external_id=None, publish_date=None):
        self._id = _id
        self.name = name
        self.quantity = quantity
        self.description = description
        self.price = price
        self.enabled = enabled
        self.timestamp = timestamp
        self.external_id = external_id
        self.publish_date = publish_date

    @property
    def id(self):
        return self._id

    @property
    def key(self):
        return unicode(self._id)

    def _deep_hash(self):
        return hash((self._id, self.name, self.quantity, self.description, self.price,
                     self.enabled, self.external_id, self.publish_date))

    # noinspection PyProtectedMember
    def _deep_equals(self, other):
        return self._id == other._id and self.name == other.name and \
            self.quantity == other.quantity and self.description == other.description and \
            self.price == other.price and self.enabled == other.enabled and \
            self.external_id == other.external_id and \
            DateTimeUtil.are_equal_by_less_than(self.publish_date, other.publish_date, 0.001)


@pytest.fixture(scope="session")
def product_fixture_table():
    return ProductFixture.TABLE_NAME


########################################################################################################################


class ProductFixtureCassandraStore(AbstractCassandraStore):

    def __init__(self, nodes, keyspace):
        super(ProductFixtureCassandraStore, self).__init__(nodes, keyspace, ProductFixture.TABLE_NAME)

    def read(self, _id):
        statement = self.prepare_statement(
            """
            SELECT id, name, quantity, description, price, enabled, external_id, publish_date, timestamp FROM %s
            WHERE id=?
            """ % self.table)
        rows = self.execute(statement, [_id])

        assert len(rows) in (0, 1)

        if len(rows) == 1:
            row = rows[0]
            return ProductFixture(_id=row.id, name=row.name, description=row.description,
                                  quantity=row.quantity, price=row.price, enabled=row.enabled,
                                  timestamp=arrow.get(row.timestamp).float_timestamp,
                                  external_id=row.external_id, publish_date=row.publish_date)
        else:
            return None

    def create(self, product):
        statement = self.prepare_statement(
            """
            INSERT INTO %s (id, name, quantity, description, price, enabled, external_id, publish_date, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """ % self.table)
        self.execute(statement, (product.id, product.name, product.quantity, product.description,
                                 product.price, product.enabled, product.external_id, product.publish_date,
                                 TimestampUtil.seconds_to_milliseconds(product.timestamp)))

    def update(self, product):
        statement = self.prepare_statement(
            """
            UPDATE %s
            SET name=?, quantity=?, description=?, price=?, enabled=?, external_id=?, publish_date=?, timestamp=?
            WHERE id=?
            """ % self.table)
        self.execute(statement, (product.name, product.quantity, product.description,
                                 product.price, product.enabled, product.external_id, product.publish_date,
                                 TimestampUtil.seconds_to_milliseconds(product.timestamp),
                                 product.id))

    def delete(self, product):
        statement = self.prepare_statement(
            """
            DELETE FROM %s
            WHERE id=?
            """ % self.table)
        self.execute(statement, [product.id])


@pytest.fixture(scope="session")
def product_fixture_cassandra_store(cassandra_cluster, cassandra_fixture_keyspace):
    return ProductFixtureCassandraStore(cassandra_cluster, cassandra_fixture_keyspace)


@pytest.fixture(scope="session", autouse=True)
def create_product_fixture_cassandra_schema(cassandra_fixture_client, cassandra_log_trigger_name):
    cassandra_fixture_client.execute("DROP TABLE IF EXISTS %s" % ProductFixture.TABLE_NAME)
    cassandra_fixture_client.execute(
        """
        CREATE TABLE %s (
          id uuid PRIMARY KEY,
          name text,
          quantity int,
          description text,
          price decimal,
          enabled boolean,
          external_id uuid,
          publish_date timestamp,
          timestamp timestamp
        )
        """ % ProductFixture.TABLE_NAME)

    cassandra_fixture_client.execute("CREATE TRIGGER logger ON %s USING '%s'" %
                                     (ProductFixture.TABLE_NAME, cassandra_log_trigger_name))


########################################################################################################################


class ProductFixtureElasticsearchStore(AbstractElasticsearchStore):

    def __init__(self, client, index):
        super(ProductFixtureElasticsearchStore, self).__init__(client)
        self._index = index
        self._type = ProductFixture.TABLE_NAME

    def read(self, _id):
        return self._base_read(self._index, self._type, _id)

    def delete(self, _id):
        self._base_delete(self._index, self._type, _id)

    def create(self, document):
        self._base_create(self._index, self._type, document.id, document)

    def update(self, document):
        self._base_update(self._index, self._type, document.id, document)

    def delete_all(self):
        self._base_delete_all(self._index, self._type)

    def _from_response(self, identifier, timestamp, source):
        product = ProductFixture()
        product._id = UUID(identifier.key)
        product.timestamp = timestamp
        product.name = source.get("name", None)
        product.quantity = source.get("quantity", None)
        product.description = source.get("description", None)
        product.enabled = source.get("enabled", None)

        publish_date = source.get("publish_date", None)
        product.publish_date = publish_date

        price = source.get("price", None)
        product.price = Decimal(price) if price else None

        external_id = source.get("external_id", None)
        product.external_id = UUID(external_id) if external_id else None

        return product

    def _to_request_body(self, document):
        return {"name": document.name,
                "quantity": document.quantity,
                "description": document.description,
                "price": str(document.price) if document.price else None,
                "enabled": document.enabled,
                "publish_date": document.publish_date,
                "external_id": str(document.external_id) if document.external_id else None}


@pytest.fixture(scope="session")
def product_fixture_elasticsearch_store(elasticsearch_client, elasticsearch_fixture_index):
    return ProductFixtureElasticsearchStore(elasticsearch_client, elasticsearch_fixture_index)


# noinspection PyShadowingNames
@pytest.fixture(scope="session", autouse=True)
def create_product_fixture_elasticsearch_schema(elasticsearch_client,
                                                elasticsearch_fixture_index, product_fixture_table):

    if elasticsearch_client.indices.exists_type(index=elasticsearch_fixture_index, doc_type=product_fixture_table):
        elasticsearch_client.indices.delete_mapping(index=elasticsearch_fixture_index, doc_type=product_fixture_table)

    mapping = {
        "_timestamp": {"enabled": True, "store": True},
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "quantity": {"type": "integer"},
            "price": {"type": "string"},
            "enabled": {"type": "boolean"},
            "publish_date": {"type": "date"},
            "external_id": {"type": "string"}
        }
    }
    elasticsearch_client.indices.put_mapping(index=elasticsearch_fixture_index,
                                             doc_type=product_fixture_table, body=mapping)
