from app.cassandra_domain.cassandra_update_fetcher import CassandraUpdateFetcher
from app.cassandra_domain.store.cassandra_log_entry_store import CassandraLogEntryStore
from app.elasticsearch_domain.elasticsearch_update_applier import ElasticsearchUpdateApplier


class CassandraToElasticsearchPropagator:

    def __init__(self, cassandra_cluster, elasticsearch_client, settings):
        log_entry_store = CassandraLogEntryStore(cassandra_cluster,
                                                 settings.cassandra_log_keyspace, settings.cassandra_log_table)
        self._cassandra = CassandraUpdateFetcher(log_entry_store, settings.cassandra_id_column_name)
        self._elasticsearch = ElasticsearchUpdateApplier(elasticsearch_client)

    def propagate_updates(self, minimum_timestamp=None):
        last_update_timestamp = None

        for update in self._cassandra.fetch_updates(minimum_timestamp):
            self._elasticsearch.apply_update(update)
            if update.timestamp > last_update_timestamp:
                last_update_timestamp = update.timestamp

        return last_update_timestamp
