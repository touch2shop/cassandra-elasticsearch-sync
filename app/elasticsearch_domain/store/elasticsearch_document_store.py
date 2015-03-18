import elasticsearch.helpers
from decimal import Decimal
from uuid import UUID
from app.core.abstract_document_store import AbstractDocumentStore

from app.core.abstract_iterable_result import AbstractIterableResult
from app.core.model.document import Document
from app.core.util.timestamp_util import TimestampUtil
from app.elasticsearch_domain.invalid_elasticsearch_schema_exception import InvalidElasticsearchSchemaException
from app.elasticsearch_domain.store.abstract_elasticsearch_store import AbstractElasticsearchStore, MATCH_ALL_QUERY
from app.elasticsearch_domain.store.elasticsearch_response_util import ElasticsearchResponseUtil


def _build_document(identifier, timestamp, source):
    if not timestamp:
        raise InvalidElasticsearchSchemaException(identifier=identifier,
                message="Could not retrieve '_timestamp' for Elasticsearch document. Please check your mappings.")

    fields = ElasticsearchResponseUtil.extract_document_fields_from_source(source)
    return Document(identifier, timestamp, fields)


class ElasticsearchDocumentIterableResponse(AbstractIterableResult):

    def _to_entity(self, response):
        identifier = ElasticsearchResponseUtil.extract_identifier(response)
        timestamp = ElasticsearchResponseUtil.extract_timestamp(response)
        source = ElasticsearchResponseUtil.extract_source(response)
        return _build_document(identifier, timestamp, source)


_DEFAULT_SCROLL_TIME = "5m"


class ElasticsearchDocumentStore(AbstractElasticsearchStore, AbstractDocumentStore):

    def __init__(self, client):
        super(ElasticsearchDocumentStore, self).__init__(client)

    def read(self, identifier):
        return self._base_read(identifier.namespace, identifier.table, identifier.key)

    def delete(self, identifier):
        self._base_delete(identifier.namespace, identifier.table, identifier.key)

    def create(self, document):
        identifier = document.identifier
        self._base_create(identifier.namespace, identifier.table, identifier.key, document)

    def update(self, document):
        identifier = document.identifier
        self._base_update(identifier.namespace, identifier.table, identifier.key, document)

    def search(self, query, scroll_time=_DEFAULT_SCROLL_TIME):
        response_iterator = elasticsearch.helpers.scan(client=self._client, query=query, scroll=scroll_time,
                                                       _source=True, fields="_timestamp")
        return ElasticsearchDocumentIterableResponse(response_iterator)

    def search_by_minimum_timestamp(self, minimum_timestamp):
        return self.search(query=self.__filter_by_timestamp_greater_than_or_equal_to(minimum_timestamp))

    def search_all(self):
        return self.search(query=MATCH_ALL_QUERY)

    @staticmethod
    def __filter_by_timestamp_greater_than_or_equal_to(minimum_timestamp):
        ts = TimestampUtil.seconds_to_milliseconds(minimum_timestamp)
        return {"filter": {"range": {"_timestamp": {"gte": ts}}}}

    def _from_response(self, identifier, timestamp, source):
        return _build_document(identifier, timestamp, source)

    def _to_request_body(self, document):
        body = {}
        for field in document.fields:
            if isinstance(field.value, UUID):
                serialized_value = str(field.value)
            elif isinstance(field.value, Decimal):
                serialized_value = str(field.value)
            else:
                serialized_value = field.value
            body[field.name] = serialized_value
        return body
