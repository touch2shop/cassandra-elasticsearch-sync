import string
from cassandra.cluster import Cluster

_DEFAULT_ID_COLUMN_NAME = "id"


class SimpleCassandraClient(object):

    def __init__(self, nodes, keyspace=None):
        cluster = Cluster(nodes)
        if keyspace:
            self._session = cluster.connect(keyspace)
        else:
            self._session = cluster.connect()

    @property
    def session(self):
        return self._session

    @property
    def keyspace(self):
        return self.session.keyspace

    @keyspace.setter
    def keyspace(self, value):
        self.session.set_keyspace(value)

    def prepare_statement(self, query):
        return self._session.prepare(query)

    def execute(self, query_or_statement, parameters=None, timeout=None):
        if timeout is None:
            return self._session.execute(query_or_statement, parameters)
        else:
            return self._session.execute(query_or_statement, parameters, timeout)

    def select_by_id(self, table, _id, columns=None, keyspace=None, id_column_name=_DEFAULT_ID_COLUMN_NAME):
        if columns and len(columns) > 0:
            columns_string = string.join(columns, ",")
        else:
            columns_string = "*"

        if not keyspace:
            keyspace = self.keyspace

        statement = self.prepare_statement(
            """
            SELECT %s FROM %s.%s WHERE %s=%s
            """ % (columns_string, keyspace, table, id_column_name, _id)
        )
        return self.execute(statement)