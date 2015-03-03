from app.cassandra.SimpleCassandraClient import SimpleCassandraClient


class AbstractCassandraStore(SimpleCassandraClient):

    def __init__(self, nodes, keyspace, table):
        super(AbstractCassandraStore, self).__init__(nodes, keyspace)
        self._table = table

    @property
    def table(self):
        return self._table
