from app.cassandra.CassandraUpdateFetcher import CassandraUpdateFetcher
from app.elasticsearch.ElastisearchUpdateApplier import ElasticsearchUpdateApplier


class CassandraToElasticSearchPropagator:

    def __init__(self, cassandra_log_entry_store, elasticsearch_document_store):
        self._cassandra_update_fetcher = CassandraUpdateFetcher(cassandra_log_entry_store)
        self._elasticsearch_update_applier = ElasticsearchUpdateApplier(elasticsearch_document_store)

    def propagate_updates(self, minimum_time=None):
        updates = self._cassandra_update_fetcher.fetch_updates(minimum_time)
        self._elasticsearch_update_applier.apply_updates(updates)
