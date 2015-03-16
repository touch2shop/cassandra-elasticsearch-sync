import logging
from operator import attrgetter
import arrow

from app.cassandra_domain.invalid_cassandra_schema_exception import InvalidCassandraSchemaException
from app.core.abstract_update_fetcher import AbstractUpdateFetcher
from app.core.update import Update


_DEFAULT_CASSANDRA_ID_COLUMN_NAME = "id"
_DEFAULT_CASSANDRA_TIMESTAMP_COLUMN_NAME = "timestamp"


class CassandraUpdateFetcher(AbstractUpdateFetcher):

    def __init__(self, log_entry_store,
                 id_column_name=_DEFAULT_CASSANDRA_ID_COLUMN_NAME,
                 timestamp_column_name=_DEFAULT_CASSANDRA_TIMESTAMP_COLUMN_NAME):
        super(CassandraUpdateFetcher, self).__init__()
        self._logger = logging.getLogger()
        self._log_entry_store = log_entry_store
        self._id_column_name = id_column_name
        self._timestamp_column_name = timestamp_column_name
        self._cassandra_client = log_entry_store.client
        self._log_entries_iterator = None

    def fetch_updates(self, minimum_timestamp=None):
        self._log_entries_iterator = self._fetch_log_entries(minimum_timestamp)
        return self

    def _fetch_log_entries(self, minimum_timestamp):
        if minimum_timestamp is None:
            return self._log_entry_store.find_all()
        else:
            return self._log_entry_store.find_by_time_greater_or_equal_than(minimum_timestamp)

    def next(self):
        if self._log_entries_iterator is None:
            raise ValueError("No fetched log entries. Did you call fetch_updates first?")

        update = None
        while update is None:
            log_entry = next(self._log_entries_iterator)
            update = self._fetch_update(log_entry)
        return update

    def _fetch_update(self, log_entry):
        if log_entry.is_save:
            return self._fetch_save_update(log_entry)
        elif log_entry.is_delete:
            return self._fetch_delete_update(log_entry)
        else:
            raise ValueError("Unsupported Cassandra log entry operation: " + log_entry.operation)

    def _fetch_delete_update(self, log_entry):
        row = self._fetch_single_row(log_entry)
        if not row:
            return self._build_update(log_entry)
        else:
            return None

    def _fetch_save_update(self, log_entry):
        row = self._fetch_single_row(log_entry)
        if row:
            return self._build_update(log_entry, row)
        else:
            return None

    def _fetch_single_row(self, log_entry):
        rows = self._fetch_rows(log_entry)

        if len(rows) > 1:
            raise InvalidCassandraSchemaException(identifier=log_entry.logged_identifier,
                    message=("More than one row found for entity on Cassandra. " +
                             "Please make sure the schema has a single primary key column with name '%s'. " +
                             "No action performed for this log entry.") % self._id_column_name)
        elif len(rows) == 0:
            return None
        else:
            row = rows[0]
            if not hasattr(row, self._timestamp_column_name):
                raise InvalidCassandraSchemaException(identifier=log_entry.logged_identifier,
                        message=("No timestamp column found for entity on Cassandra. " +
                                 "Please make sure the schema has a timestamp column with name '%s'. " +
                                 "No action performed for this log entry.") % self._timestamp_column_name)
            return row

    def _fetch_rows(self, log_entry):
        columns = set()
        columns.add(self._timestamp_column_name)
        if log_entry.updated_columns:
            for column in log_entry.updated_columns:
                columns.add(column)

        rows = self._cassandra_client.select_by_id(keyspace=log_entry.logged_keyspace,
                                                   table=log_entry.logged_table,
                                                   _id=log_entry.logged_key,
                                                   columns=columns,
                                                   id_column_name=self._id_column_name)
        return rows

    def _build_update(self, log_entry, fetched_row=None):
        update = Update()
        update.identifier = log_entry.logged_identifier
        update.is_delete = log_entry.is_delete
        if fetched_row:
            update.timestamp = self._get_timestamp(fetched_row)
            for field_name in log_entry.updated_columns:
                field_name = field_name.lower()
                field_value = attrgetter(field_name)(fetched_row)
                update.add_field(field_name, field_value)
        else:
            update.timestamp = log_entry.timestamp

        return update

    def _get_timestamp(self, fetched_row):
        time = attrgetter(self._timestamp_column_name)(fetched_row)
        return arrow.get(time).float_timestamp
