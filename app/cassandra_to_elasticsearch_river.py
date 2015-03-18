from app.cassandra_domain.cassandra_update_fetcher import CassandraUpdateFetcher
from app.core.abstract_data_river import AbstractDataRiver
from app.elasticsearch_domain.elasticsearch_update_applier import ElasticsearchUpdateApplier


class CassandraToElasticsearchRiver(AbstractDataRiver):

    def __init__(self, cassandra_cluster, elasticsearch_client, settings):
        update_applier = ElasticsearchUpdateApplier(elasticsearch_client)
        update_fetcher = CassandraUpdateFetcher(cassandra_cluster, settings)
        super(CassandraToElasticsearchRiver, self).__init__(update_fetcher, update_applier)
