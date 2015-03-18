from app.cassandra_domain.store.cassandra_document_store import CassandraDocumentStore
from app.cassandra_domain.store.cassandra_log_entry_store import CassandraLogEntryStore
from app.core.abstract_update_fetcher import AbstractUpdateFetcher
from app.core.model.update import Update


_DEFAULT_ID_COLUMN_NAME = "id"
_DEFAULT_TIMESTAMP_COLUMN_NAME = "timestamp"


class CassandraUpdateFetcher(AbstractUpdateFetcher):

    def __init__(self, cassandra_cluster, settings):

        super(CassandraUpdateFetcher, self).__init__()

        self._log_entry_store = CassandraLogEntryStore(
            cassandra_cluster, settings.cassandra_log_keyspace, settings.cassandra_log_table)
        self._document_store = CassandraDocumentStore(
            cassandra_cluster, settings.cassandra_id_column_name, settings.cassandra_timestamp_column_name)

    def _fetch_data(self, minimum_timestamp):
        if minimum_timestamp is None:
            return self._log_entry_store.search_all()
        else:
            return self._log_entry_store.search_by_minimum_timestamp(minimum_timestamp)

    def _to_update(self, data):
        log_entry = data
        existing_document = self._document_store.read(log_entry.logged_identifier)

        if log_entry.is_delete:
            if not existing_document:
                return self._build_delete_update(log_entry)
            else:
                return None
        else:
            if existing_document:
                return self._build_save_update(existing_document)
            else:
                return None

    @staticmethod
    def _build_delete_update(log_entry):
        update = Update()
        update.is_delete = True
        update.identifier = log_entry.logged_identifier
        update.timestamp = log_entry.timestamp
        return update

    @staticmethod
    def _build_save_update(existing_document):
        if existing_document:
            return Update.from_document(existing_document, is_delete=False)
        else:
            return None
