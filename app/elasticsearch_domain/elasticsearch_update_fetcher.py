from app.core.abstract_update_fetcher import AbstractUpdateFetcher
from app.core.model.update import Update
from app.elasticsearch_domain.invalid_elasticsearch_schema_exception import InvalidElasticsearchSchemaException

from app.elasticsearch_domain.store.elasticsearch_document_store import ElasticsearchDocumentStore


# TODO: add a settings option to get a list of indexes to retrieve updates from. Currently, queries all indexes.
class ElasticsearchUpdateFetcher(AbstractUpdateFetcher):

    def __init__(self, elasticsearch_client):
        super(ElasticsearchUpdateFetcher, self).__init__()
        self._document_store = ElasticsearchDocumentStore(elasticsearch_client)
        self._documents_iterator = None

    def fetch_updates(self, minimum_timestamp=None):
        if minimum_timestamp:
            self._documents_iterator = self._document_store.search_by_minimum_timestamp(minimum_timestamp)
        else:
            self._documents_iterator = self._document_store.search_all()
        return self

    def next(self):
        if not self._documents_iterator:
            raise ValueError("No updates fetched. Did you call fetch_updates first?")

        next_document = next(self._documents_iterator)
        self.__validate_document(next_document)
        return self.__to_update(next_document)

    @staticmethod
    def __validate_document(document):
        if not document.timestamp:
            raise InvalidElasticsearchSchemaException(identifier=document.identifier,
                message="Could not retrieve '_timestamp' for Elasticsearch document. Please check your mappings.")

    @staticmethod
    def __to_update(document):
        update = Update()
        update.identifier = document.identifier
        update.timestamp = document.timestamp
        update.fields = document.fields

        # TODO: as of now, it is impossible to retrieve delete updates from Elasticsearch.
        update.is_delete = False
        return update
