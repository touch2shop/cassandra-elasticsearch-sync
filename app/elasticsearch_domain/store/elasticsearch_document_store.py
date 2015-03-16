from decimal import Decimal
from uuid import UUID

from app.core.document import Document

from app.core.identifier import Identifier
from app.core.field import Field
from app.elasticsearch_domain.store.abstract_elasticsearch_store import AbstractElasticsearchStore


class ElasticsearchDocumentStore(AbstractElasticsearchStore):

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

    def _from_response(self, body, timestamp, index, _type, _id):
        identifier = Identifier(index, _type, _id)

        fields = self._extract_fields(body)

        return Document(identifier, timestamp, fields)

    @staticmethod
    def _extract_fields(body):
        fields = []
        for (field_name, field_value) in body.items():
            fields.append(Field(field_name, field_value))
        return fields

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
