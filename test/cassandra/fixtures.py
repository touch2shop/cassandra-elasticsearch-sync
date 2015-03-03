
class ProductFixture:
    def __init__(self, id_, name, quantity, description):
        self.id_ = id_
        self.name = name
        self.quantity = quantity
        self.description = description


# noinspection PyShadowingNames
def create_fixture_product(cassandra_client, product, product_fixture_table):
    statement = cassandra_client.prepare_statement(
        """
        INSERT INTO %s (id, name, quantity, description)
        VALUES (?, ?, ?, ?)
        """ % product_fixture_table)
    cassandra_client.execute(statement, [product.id_, product.name, product.quantity, product.description])


def delete_all_product_fixtures(cassandra_client, product_fixture_table):
    cassandra_client.execute("TRUNCATE %s" % product_fixture_table)
