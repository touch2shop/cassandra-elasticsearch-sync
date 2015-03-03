from app.CassandraUpdateFetcher import CassandraUpdateFetcher
from app.cassandra.CassandraLogEntryStore import CassandraLogEntryStore

class CassandraToElasticSearchPropagator:

    def __init__(self, cassandra_nodes, cassandra_log_keyspace, cassandra_log_table):
        cassandra_log_entry_store = CassandraLogEntryStore(
            cassandra_nodes, cassandra_log_keyspace, cassandra_log_table)
        self._cassandra_update_fetcher = CassandraUpdateFetcher(cassandra_log_entry_store)

    def propagate_updates(self, minimum_time=None):
        cassandra_updates = self._cassandra_update_fetcher.fetch_updates(minimum_time)
