from app.cassandra.store.AbstractCassandraStore import AbstractCassandraStore


class ProductFixture:
    def __init__(self, id_, name, quantity, description):
        self.id_ = id_
        self.name = name
        self.quantity = quantity
        self.description = description

    @property
    def key(self):
        return str(self.id_)


class ProductFixtureStore(AbstractCassandraStore):

    def __init__(self, nodes, keyspace, table):
        super(ProductFixtureStore, self).__init__(nodes, keyspace, table)

    def create(self, product):
        statement = self.prepare_statement(
            """
            INSERT INTO %s (id, name, quantity, description)
            VALUES (?, ?, ?, ?)
            """ % self.table)
        self.execute(statement, [product.id_, product.name, product.quantity, product.description])

    def update(self, product):
        statement = self.prepare_statement(
            """
            UPDATE %s
            SET name=?, quantity=?, description=?
            WHERE id=?
            """ % self.table)
        self.execute(statement, [product.name, product.quantity, product.description, product.id_])

    def delete(self, product):
        statement = self.prepare_statement(
            """
            DELETE FROM %s
            WHERE id=?
            """ % self.table)
        self.execute(statement, [product.id_])

    def delete_all(self):
        self.execute("TRUNCATE %s" % self.table)
