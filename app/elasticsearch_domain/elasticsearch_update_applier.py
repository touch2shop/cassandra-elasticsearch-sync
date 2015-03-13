import logging

from app.elasticsearch_domain.invalid_elasticsearch_schema_exception import InvalidElasticsearchSchemaException
from app.core.value_field import ValueField
from app.elasticsearch_domain.generic_elasticsearch_document import GenericElasticsearchDocument
from app.elasticsearch_domain.store.generic_elasticsearch_store import GenericElasticsearchStore


class ElasticsearchUpdateApplier:

    def __init__(self, elasticsearch):
        self._elasticsearch = elasticsearch
        self._generic_store = GenericElasticsearchStore(elasticsearch)
        self._logger = logging.getLogger()

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

                if not ValueField.fields_are_identical(existing_document.fields, update_document.fields):
                    self._generic_store.update(update_document)

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
