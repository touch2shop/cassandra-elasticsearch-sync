from app.cassandra_domain.store.cassandra_client import CassandraClient


class AbstractCassandraStore(object):

    def __init__(self, cluster, keyspace, table):
        self._client = CassandraClient(cluster, keyspace)
        self._table = table

    @property
    def table(self):
        return self._table

    @property
    def keyspace(self):
        return self._client.keyspace

    @property
    def client(self):
        return self._client

    def prepare_statement(self, query):
        return self._client.prepare_statement(query)

    def execute(self, query_or_statement, parameters=None, timeout=None):
        return self._client.execute(query_or_statement, parameters, timeout)

    def delete_all(self):
        self.execute("TRUNCATE %s" % self.table)
