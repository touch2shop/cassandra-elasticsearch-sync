from abc import ABCMeta, abstractmethod
import logging
from app.core.model.field import Field


class AbstractUpdateApplier(object):

    __metaclass__ = ABCMeta

    def __init__(self, document_store):
        self._document_store = document_store
        self._logger = logging.getLogger()

    def apply_update(self, update):
        self._check_namespace_and_table_exists(update.identifier)
        existing_document = self._document_store.read(update.identifier)
        if existing_document:
            self.__apply_update_to_existing_document(update, existing_document)
        else:
            self.__apply_update_to_nonexistent_document(update)

    def __apply_update_to_existing_document(self, update, existing_document):
        if update.timestamp > existing_document.timestamp:
            if update.is_delete:
                self._document_store.delete(update.identifier)
            else:
                # in order to avoid deadlocks and break cycles, a document is updated only if there are differences.
                if not Field.fields_are_identical(existing_document.fields, update.fields):
                    self._document_store.update(update)

    def __apply_update_to_nonexistent_document(self, update):
        if update.is_delete:
            # Document already deleted. No action needed.
            pass
        else:
            self._document_store.create(update)

    @abstractmethod
    def _check_namespace_and_table_exists(self, identifier):
        """
        Makes sure the update namespace and table exists on the target database. Raises an exception if not.
        """
        pass
