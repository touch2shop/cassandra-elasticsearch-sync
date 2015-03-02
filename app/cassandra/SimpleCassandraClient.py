from cassandra.cluster import Cluster


class SimpleCassandraClient(object):

    def __init__(self, nodes, keyspace=None):
        cluster = Cluster(nodes)
        if keyspace is not None:
            self._session = cluster.connect(keyspace)
        else:
            self._session = cluster.connect()
        self._keyspace = keyspace

    @property
    def session(self):
        return self._session

    @property
    def keyspace(self):
        return self._keyspace

    def prepare_statement(self, query):
        return self._session.prepare(query)

    def execute(self, query_or_statement, parameters=None, timeout=None):
        if timeout is None:
            return self._session.execute(query_or_statement, parameters)
        else:
            return self._session.execute(query_or_statement, parameters, timeout)
