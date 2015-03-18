from app.core.abstract_update_applier import AbstractUpdateApplier

from app.elasticsearch_domain.invalid_elasticsearch_schema_exception import InvalidElasticsearchSchemaException
from app.elasticsearch_domain.store.elasticsearch_document_store import ElasticsearchDocumentStore


class ElasticsearchUpdateApplier(AbstractUpdateApplier):

    def __init__(self, elasticsearch_client, settings):
        document_store = ElasticsearchDocumentStore(elasticsearch_client)
        super(ElasticsearchUpdateApplier, self).__init__(document_store)
        self._elasticsearch_client = elasticsearch_client

    def _check_namespace_and_table_exists(self, identifier):
        if not self._elasticsearch_client.indices.exists(index=identifier.namespace):
            raise InvalidElasticsearchSchemaException(identifier=identifier,
                message="Index does not exist on Elasticsearch.")
        if not self._elasticsearch_client.indices.exists_type(index=identifier.namespace, doc_type=identifier.table):
            raise InvalidElasticsearchSchemaException(identifier=identifier,
                message="Type does not exist on Elasticsearch.")
