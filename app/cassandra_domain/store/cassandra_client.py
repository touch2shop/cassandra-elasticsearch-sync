import string


class CassandraClient(object):

    def __init__(self, cluster, keyspace=None):
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

    def select_by_id(self, table, _id, columns=None, keyspace=None, id_column_name="id"):
        if columns:
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

    def __select_table_names_from_keyspace(self, keyspace):
        statement = self.prepare_statement(
            """
            SELECT columnfamily_name FROM System.schema_columnfamilies WHERE keyspace_name=?
            """
        )
        return self.execute(statement, [keyspace])

    def keyspace_exists(self, keyspace):
        # this for is necessary because the result might be a paginated iterator
        for row in self.__select_table_names_from_keyspace(keyspace.lower()):
            return True
        return False

    def table_exists(self, keyspace, table):
        all_tables = self.__select_table_names_from_keyspace(keyspace.lower())
        for row in all_tables:
            if row.columnfamily_name.lower() == table.lower():
                return True
        return False
