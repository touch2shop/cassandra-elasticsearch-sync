from app.cassandra_domain.cassandra_update_fetcher import CassandraUpdateFetcher
from app.cassandra_domain.store.cassandra_log_entry_store import CassandraLogEntryStore
from app.elasticsearch_domain.elasticsearch_update_applier import ElasticsearchUpdateApplier


class CassandraToElasticsearchPropagator:

    def __init__(self, cassandra_cluster, elasticsearch_client, settings):
        log_entry_store = CassandraLogEntryStore(cassandra_cluster,
                                                 settings.cassandra_log_keyspace, settings.cassandra_log_table)
        self._cassandra_update_fetcher = CassandraUpdateFetcher(log_entry_store, settings.cassandra_id_column_name)
        self._elasticsearch_update_applier = ElasticsearchUpdateApplier(elasticsearch_client)

    def propagate_updates(self, minimum_time=None):
        updates = self._cassandra_update_fetcher.fetch_updates(minimum_time)
        self._elasticsearch_update_applier.apply_updates(updates)
