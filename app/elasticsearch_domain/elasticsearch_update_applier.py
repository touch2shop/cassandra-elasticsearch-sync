from app.core.abstract_update_applier import AbstractUpdateApplier
from app.core.exception.invalid_schema_exception import InvalidSchemaException

from app.elasticsearch_domain.store.elasticsearch_document_store import ElasticsearchDocumentStore


class ElasticsearchUpdateApplier(AbstractUpdateApplier):

    # noinspection PyUnusedLocal
    def __init__(self, elasticsearch_client, settings):
        document_store = ElasticsearchDocumentStore(elasticsearch_client)
        super(ElasticsearchUpdateApplier, self).__init__(document_store)
        self._elasticsearch_client = elasticsearch_client

    def _check_namespace_and_table_exists(self, identifier):
        if not self._elasticsearch_client.indices.exists(index=identifier.namespace):
            raise InvalidSchemaException(identifier=identifier,
                message="Index does not exist on Elasticsearch.")
        if not self._elasticsearch_client.indices.exists_type(index=identifier.namespace, doc_type=identifier.table):
            raise InvalidSchemaException(identifier=identifier,
                message="Type does not exist on Elasticsearch.")
