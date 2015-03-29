from app.core.abstract_update_fetcher import AbstractUpdateFetcher
from app.core.model.update import Update

from app.elasticsearch_domain.store.elasticsearch_document_store import ElasticsearchDocumentStore


# TODO: add a settings option to get a list of indexes to retrieve updates from. Currently, queries all indexes.
class ElasticsearchUpdateFetcher(AbstractUpdateFetcher):

    # noinspection PyUnusedLocal
    def __init__(self, elasticsearch_client, settings):
        super(ElasticsearchUpdateFetcher, self).__init__()
        self._document_store = ElasticsearchDocumentStore(elasticsearch_client)

    def _fetch_data(self, minimum_timestamp):
        if minimum_timestamp:
            return self._document_store.search_by_minimum_timestamp(minimum_timestamp)
        else:
            return self._document_store.search_all()

    def _to_update(self, data):
        # TODO: as of now, it is impossible to retrieve delete updates from Elasticsearch. All updates are save only.
        return Update.from_document(data, is_delete=False)
