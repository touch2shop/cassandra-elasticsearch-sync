from app.cassandra_domain.store.abstract_cassandra_store import AbstractCassandraStore
from app.cassandra_domain.cassandra_log_entry import CassandraLogEntry


class CassandraLogEntryStore(AbstractCassandraStore):

    def __init__(self, cluster, log_keyspace, log_table):
        super(CassandraLogEntryStore, self).__init__(cluster, log_keyspace, log_table)

    def _build_select_query(self, where=None, allow_filtering=False):
        query = """
          SELECT logged_keyspace, logged_table, logged_key, time_uuid, operation, updated_columns
          FROM %s
          """ % self.table

        if where is not None:
            query += " WHERE " + where
        if allow_filtering:
            query += " ALLOW FILTERING"

        return query

    def create(self, log_entry):
        statement = self.prepare_statement("""
          INSERT INTO %s (logged_keyspace, logged_table, logged_key, time_uuid, operation, updated_columns)
          VALUES (?, ?, ?, ?, ?, ?)
        """ % self.table)

        self.execute(statement, (
            log_entry.logged_keyspace,
            log_entry.logged_table,
            log_entry.logged_key,
            log_entry.time_uuid,
            log_entry.operation,
            log_entry.updated_columns))

    def find_all(self, timeout=None):
        statement = self.prepare_statement(self._build_select_query())
        rows = self.execute(statement, timeout)
        return self._to_log_entries(rows)

    def find_by_logged_row(self, logged_keyspace, logged_table, logged_key, timeout=None):
        statement = self.prepare_statement(
            self._build_select_query(where="logged_keyspace = ? AND logged_table = ? AND logged_key = ?"))

        rows = self.execute(statement, (logged_keyspace, logged_table, logged_key), timeout)
        return self._to_log_entries(rows)

    def find_by_time_greater_or_equal_than(self, minimum_time, timeout=None):
        statement = self.prepare_statement(
            self._build_select_query(where="time_uuid >= minTimeuuid(?)", allow_filtering=True))

        rows = self.execute(statement, [minimum_time], timeout)
        return self._to_log_entries(rows)

    def delete_all(self):
        """Warning: wipes out the entire log table, use with caution."""
        self.execute("TRUNCATE %s" % self.table)

    @staticmethod
    def _to_log_entries(rows):
        log_entries = []
        for logged_keyspace, logged_table, logged_key, time_uuid, operation, updated_columns in rows:
            log_entry = CassandraLogEntry()
            log_entry.time_uuid = time_uuid
            log_entry.logged_keyspace = logged_keyspace
            log_entry.logged_table = logged_table
            log_entry.logged_key = logged_key
            log_entry.operation = operation
            log_entry.updated_columns = updated_columns
            log_entries.append(log_entry)
        return log_entries
