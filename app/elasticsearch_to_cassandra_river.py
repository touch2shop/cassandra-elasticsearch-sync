from app.cassandra_domain.cassandra_update_applier import CassandraUpdateApplier
from app.core.abstract_data_river import AbstractDataRiver
from app.elasticsearch_domain.elasticsearch_update_fetcher import ElasticsearchUpdateFetcher


class ElasticsearchToCassandraRiver(AbstractDataRiver):

    def __init__(self, elasticsearch_client, cassandra_cluster, settings):
        update_fetcher = ElasticsearchUpdateFetcher(elasticsearch_client, settings)
        update_applier = CassandraUpdateApplier(cassandra_cluster, settings)
        super(ElasticsearchToCassandraRiver, self).__init__(update_fetcher, update_applier)
