import logging
from app.elasticsearch.ElasticsearchDocument import ElasticsearchDocument


class ElasticsearchUpdateApplier:

    def __init__(self, document_store):
        self._document_store = document_store
        self._logger = logging.getLogger(__name__)

    def apply_updates(self, update_container):
        for namespace in update_container.get_namespaces():
            self._check_index_exists(namespace)
            for table in update_container.get_tables(namespace):
                self._check_type_exists(namespace, table)
                for update in update_container.get_updates(namespace, table):
                    self._apply_update(update)

    def _apply_update(self, update):
        document = self._document_store.read(update.identifier)
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
                self._document_store.delete(update)
            else:
                # in order to avoid deadlocks and break cycles, a document is updated only if there are differences.
                update_document = ElasticsearchDocument.from_update(update)

                if self._documents_are_different(existing_document, update_document):
                    self._document_store.update(update_document)

    def _documents_are_different(self, document1, document2):
        for field1 in document1.fields:
            for field2 in document2.fields:
                if field1.name == field2.name:
                    if self._fields_are_different(field1, field2):
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
            self._document_store.create(ElasticsearchDocument.from_update(update))

    def _check_index_exists(self, index):
        if not self._document_store.index_exists(index=index):
            raise Exception(("Index %s does not exist on elasticsearch. " +
                             "No action performed.") % index)

    def _check_type_exists(self, index, _type):
        if not self._document_store.type_exists(index=index, doc_type=_type):
            raise Exception(("Type %s does not exist at index %s on Elasticsearch. " +
                             "No action performed.") % (_type, index))

    @staticmethod
    def _validate_document(document):
        if not document.timestamp:
            raise Exception("Could not retrieve timestamp for Elasticsearch document %. Please check your mappings.")
