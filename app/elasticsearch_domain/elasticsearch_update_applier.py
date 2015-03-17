import logging

from app.elasticsearch_domain.invalid_elasticsearch_schema_exception import InvalidElasticsearchSchemaException
from app.core.model.field import Field
from app.elasticsearch_domain.store.elasticsearch_document_store import ElasticsearchDocumentStore


class ElasticsearchUpdateApplier:

    def __init__(self, elasticsearch):
        self._elasticsearch = elasticsearch
        self._document_store = ElasticsearchDocumentStore(elasticsearch)
        self._logger = logging.getLogger()

    def apply_update(self, update):
        self._check_index_and_type_exist(update.identifier)
        existing_document = self._document_store.read(update.identifier)
        if existing_document:
            self._validate_existing_document(existing_document)
            self._apply_update_to_existing_document(update, existing_document)
        else:
            self._apply_update_to_nonexistent_document(update)

    def _apply_update_to_existing_document(self, update, existing_document):
        if existing_document.timestamp > update.timestamp:
            self._logger.info("Elasticsearch document %s is newer than update. No action performed.", update.identifier)
        else:
            if update.is_delete:
                self._document_store.delete(update.identifier)
            else:
                # in order to avoid deadlocks and break cycles, a document is updated only if there are differences.
                if not Field.fields_are_identical(existing_document.fields, update.fields):
                    self._document_store.update(update)

    def _apply_update_to_nonexistent_document(self, update):
        if update.is_delete:
            self._logger.info(
                "Document %s already deleted from Elasticsearch. No action performed.", update.identifier)
        else:
            self._document_store.create(update)

    def _check_index_and_type_exist(self, identifier):
        if not self._elasticsearch.indices.exists(index=identifier.namespace):
            raise InvalidElasticsearchSchemaException(identifier=identifier,
                message="Index does not exist on elasticsearch. No action performed.")
        if not self._elasticsearch.indices.exists_type(index=identifier.namespace, doc_type=identifier.table):
            raise InvalidElasticsearchSchemaException(identifier=identifier,
                message="Type does not exist on elasticsearch. No action performed.")

    @staticmethod
    def _validate_existing_document(document):
        if not document.timestamp:
            raise InvalidElasticsearchSchemaException(identifier=document.identifier,
                message="Could not retrieve '_timestamp' for Elasticsearch document. Please check your mappings.")
