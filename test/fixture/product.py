from datetime import datetime

import pytest

from app.cassandra_domain.store.AbstractCassandraStore import AbstractCassandraStore
from app.elasticsearch_domain.store.AbstractEntityElasticsearchStore import AbstractEntityElasticsearchStore


class ProductFixture:

    TABLE_NAME = "product"

    def __init__(self, _id=None, name=None, quantity=None, description=None, timestamp=None):
        self._id = _id
        self.name = name
        self.quantity = quantity
        self.description = description
        self.timestamp = timestamp

    @property
    def id(self):
        return self._id

    @property
    def key(self):
        return unicode(self._id)


@pytest.fixture(scope="session")
def product_fixture_table():
    return ProductFixture.TABLE_NAME


########################################################################################################################


class ProductFixtureCassandraStore(AbstractCassandraStore):

    def __init__(self, nodes, keyspace):
        super(ProductFixtureCassandraStore, self).__init__(nodes, keyspace, ProductFixture.TABLE_NAME)

    def create(self, product):
        product.timestamp = datetime.utcnow()
        statement = self.prepare_statement(
            """
            INSERT INTO %s (id, name, quantity, description, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """ % self.table)
        self.execute(statement, (product.id, product.name, product.quantity, product.description, product.timestamp))

    def update(self, product):
        product.timestamp = datetime.utcnow()
        statement = self.prepare_statement(
            """
            UPDATE %s
            SET name=?, quantity=?, description=?, timestamp=?
            WHERE id=?
            """ % self.table)
        self.execute(statement, (product.name, product.quantity, product.description, product.timestamp, product.id))

    def delete(self, product):
        statement = self.prepare_statement(
            """
            DELETE FROM %s
            WHERE id=?
            """ % self.table)
        self.execute(statement, [product.id])

    def delete_all(self):
        self.execute("TRUNCATE %s" % self.table)


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
          timestamp timestamp
        )
        """ % ProductFixture.TABLE_NAME)

    cassandra_fixture_client.execute("CREATE TRIGGER logger ON %s USING '%s'" %
                                     (ProductFixture.TABLE_NAME, cassandra_log_trigger_name))


########################################################################################################################


class ProductFixtureElasticsearchStore(AbstractEntityElasticsearchStore):

    def __init__(self, client, index):
        super(ProductFixtureElasticsearchStore, self).__init__(client, index, ProductFixture.TABLE_NAME)

    def _from_response(self, body, timestamp, index, _type, _id):
        product = ProductFixture()
        product._id = _id
        product.timestamp = timestamp
        product.name = body.get("name", None)
        product.quantity = body.get("quantity", None)
        product.description = body.get("description", None)

    def _to_request_body(self, document):
        return {"name": document.name, "quantity": document.quantity, "description": document.description}


@pytest.fixture(scope="session")
def product_fixture_elasticsearch_store(elasticsearch_client, elasticsearch_fixture_index):
    return ProductFixtureElasticsearchStore(elasticsearch_client, elasticsearch_fixture_index)


# noinspection PyShadowingNames
@pytest.fixture(scope="session", autouse=True)
def create_product_fixture_elasticsearch_schema(elasticsearch_client,
                                                elasticsearch_fixture_index, product_fixture_table):
    mapping = {"_timestamp": {"enabled": True, "store": True}}
    elasticsearch_client.indices.put_mapping(index=elasticsearch_fixture_index,
                                             doc_type=product_fixture_table, body=mapping)
