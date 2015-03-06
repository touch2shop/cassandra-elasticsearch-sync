from datetime import datetime
import logging
from uuid import UUID
from app.elasticsearch.ElasticsearchDocument import ElasticsearchDocument
from app.elasticsearch.store.SimpleElasticsearchClient import SimpleElasticsearchClient
from app.sync.Identifier import Identifier
from app.sync.ValueField import ValueField


_WRITE_CONSISTENCY = "quorum"


class ElasticsearchDocumentStore(SimpleElasticsearchClient):

    def __init__(self, elasticsearch):
        SimpleElasticsearchClient.__init__(self, elasticsearch)
        self._logger = logging.getLogger(__name__)

    def read(self, identifier):
        response = self._elasticsearch.get(
            index=identifier.namespace, doc_type=identifier.table, id=identifier.key,
            realtime=True, fields="_source,_timestamp")
        return self._extract_document(response)

    def delete(self, document):
        _id = document.identifier
        return self._elasticsearch.delete(
            index=_id.namespace, doc_type=_id.table, id=_id.key, consistency=_WRITE_CONSISTENCY)

    def create(self, document):
        _id = document.identifier
        return self._elasticsearch.create(index=_id.namespace, doc_type=_id.table, id=_id.key,
                                          body=self._to_body(document),
                                          timestamp=self._get_time(document),
                                          consistency=_WRITE_CONSISTENCY, refresh=True)

    def update(self, document):
        _id = document.identifier
        return self._elasticsearch.update(index=_id.namespace, doc_type=_id.table, id=_id.key,
                                          body=self._to_body(document),
                                          timestamp=self._get_time(document),
                                          consistency=_WRITE_CONSISTENCY, refresh=True)

    @staticmethod
    def _get_time(document):
        return datetime.utcfromtimestamp(document.timestamp) if document.timestamp else datetime.utcnow()

    def _extract_document(self, response):
        if not response["found"]:
            return None

        _id = response["_id"]
        _type = response["_type"]
        _index = response["_index"]
        identifier = Identifier(_index, _type, _id)

        _timestamp = self._extract_timestamp(response)
        fields = self._extract_fields(response)

        return ElasticsearchDocument(identifier, _timestamp, fields)

    @staticmethod
    def _extract_timestamp(response):
        if "fields" in response and "_timestamp" in response["fields"]:
            return response["fields"]["_timestamp"] / 1000.0
        else:
            return None

    @staticmethod
    def _extract_fields(response):
        fields = []
        for (field_name, field_value) in response["source"]:
            fields.append(ValueField(field_name, field_value))
        return fields

    @staticmethod
    def _to_body(document):
        body = {}
        for field in document.fields:
            if isinstance(field.value, UUID):
                serialized_value = str(field.value)
            else:
                serialized_value = field.value
            body[field.name] = serialized_value
        return body
