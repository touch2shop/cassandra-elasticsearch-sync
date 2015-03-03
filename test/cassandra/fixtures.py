
class FixtureProduct:
    def __init__(self, id_, name, quantity, description):
        self.id_ = id_
        self.name = name
        self.quantity = quantity
        self.description = description


# noinspection PyShadowingNames
def create_fixture_product(cassandra_client, product):
    statement = cassandra_client.prepare_statement(
        """
        INSERT INTO product (id, name, quantity, description)
        VALUES (?, ?, ?, ?)
        """)
    cassandra_client.execute(statement, [product.id_, product.name, product.quantity, product.description])
