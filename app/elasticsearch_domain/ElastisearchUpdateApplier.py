import logging

from app.core.exception.InvalidElasticsearchSchemaException import InvalidElasticsearchSchemaException
from app.elasticsearch_domain.GenericElasticsearchDocument import GenericElasticsearchDocument
from app.elasticsearch_domain.store.GenericElasticsearchStore import GenericElasticsearchStore


class ElasticsearchUpdateApplier:

    def __init__(self, elasticsearch):
        self._elasticsearch = elasticsearch
        self._generic_store = GenericElasticsearchStore(elasticsearch)
        self._logger = logging.getLogger(__name__)

    def apply_updates(self, updates):
        if updates:
            for update in updates:
                self._check_index_and_type_exist(update.identifier)
                self._apply_update(update)

    def _apply_update(self, update):
        document = self._generic_store.read(update.identifier)
        if document:
            self._validate_document(document)
            self._apply_update_to_existing_document(update, document)
        else:
            self._apply_update_to_nonexistent_document(update)

    def _apply_update_to_existing_document(self, update, existing_document):
        if existing_document.timestamp > update.timestamp:
            self._logger.info("Elasticsearch document %s is newer than update. No action performed.", update.identifier)
        else:
            if update.is_delete:
                self._generic_store.delete(update.identifier)
            else:
                # in order to avoid deadlocks and break cycles, a document is updated only if there are differences.
                update_document = self._build_document(update)

                if self._documents_are_different(existing_document, update_document):
                    self._generic_store.update(update_document)

    @classmethod
    def _documents_are_different(cls, document1, document2):
        for field1 in document1.fields:
            for field2 in document2.fields:
                if field1.name == field2.name:
                    if cls._fields_are_different(field1, field2):
                        return True
        return False

    @staticmethod
    def _fields_are_different(field1, field2):
        return str(field1) != str(field2)

    def _apply_update_to_nonexistent_document(self, update):
        if update.is_delete:
            self._logger.info(
                "Document %s already deleted from Elasticsearch. No action performed.", update.identifier)
        else:
            self._generic_store.create(self._build_document(update))

    def _check_index_and_type_exist(self, identifier):
        if not self._elasticsearch.indices.exists(index=identifier.namespace):
            raise InvalidElasticsearchSchemaException(identifier=identifier,
                message="Index does not exist on elasticsearch. No action performed.")
        if not self._elasticsearch.indices.exists_type(index=identifier.namespace, doc_type=identifier.table):
            raise InvalidElasticsearchSchemaException(identifier=identifier,
                message="Type does not exist on elasticsearch. No action performed.")

    @staticmethod
    def _build_document(update):
        return GenericElasticsearchDocument(update.identifier, update.timestamp, list(update.fields))

    @staticmethod
    def _validate_document(document):
        if not document.timestamp:
            raise InvalidElasticsearchSchemaException(identifier=document.identifier,
                message="Could not retrieve timestamp for Elasticsearch document. Please check your mapping.")
