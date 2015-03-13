import logging
from operator import attrgetter
from app.cassandra_domain.invalid_cassandra_schema_exception import InvalidCassandraSchemaException

from app.core.identifier import Identifier
from app.core.update.update import Update
from app.core.update.event.update_event import UpdateEvent
from app.core.update.event.update_event_combiner import UpdateEventCombiner


_DEFAULT_CASSANDRA_ID_COLUMN_NAME = "id"


class CassandraUpdateFetcher(object):

    def __init__(self, log_entry_store, id_column_name=_DEFAULT_CASSANDRA_ID_COLUMN_NAME):

        self._logger = logging.getLogger(__name__)
        self._log_entry_store = log_entry_store
        self._id_column_name = id_column_name
        self._cassandra_client = log_entry_store.client

    def fetch_updates(self, minimum_timestamp=None):
        log_entries = self._fetch_log_entries(minimum_timestamp)
        combined_update_events = self._combine_update_events(log_entries)
        updates = self._fetch_updates(combined_update_events)
        return updates

    def _fetch_log_entries(self, minimum_timestamp):
        if minimum_timestamp is None:
            return self._log_entry_store.find_all()
        else:
            return self._log_entry_store.find_by_time_greater_or_equal_than(minimum_timestamp)

    @classmethod
    def _combine_update_events(cls, log_entries):
        update_events = []
        for log_entry in log_entries:
            update_events.append(cls._build_update_event(log_entry))

        return UpdateEventCombiner.combine(update_events)

    @classmethod
    def _build_update_event(cls, log_entry):
        identifier = Identifier(log_entry.logged_keyspace, log_entry.logged_table, log_entry.logged_key)
        return UpdateEvent(identifier, log_entry.timestamp, log_entry.is_delete, log_entry.updated_columns)

    def _fetch_updates(self, combined_update_events):
        updates = []
        for update_event in combined_update_events:
            update = self._fetch_update(update_event)
            if update:
                updates.append(update)
        return updates

    def _fetch_update(self, update_event):
        if update_event.is_delete:
            return Update(update_event)
        else:
            return self._fetch_save_update(update_event)

    def _fetch_save_update(self, update_event):
        _id = update_event.identifier
        rows = self._cassandra_client.select_by_id(_id.table, _id.key, update_event.field_names,
                                                   _id.namespace, id_column_name=self._id_column_name)

        if len(rows) > 1:
            raise InvalidCassandraSchemaException(identifier=_id,
                    message=("More than one row found for entity on Cassandra. " +
                             "Please make sure the schema has a single primary key with name %s. " +
                             "No action performed.") % self._id_column_name)
        elif len(rows) == 0:
            # If the entity was deleted, a delete event will be will be available in the next log fetch.
            self._logger.info("No row found for entity %s on Cassandra. No action performed.", _id)
            return None

        return self._to_update(rows[0], update_event)

    @classmethod
    def _to_update(cls, row, combined_update_event):
        update = Update(combined_update_event)
        for field in combined_update_event.fields:
            field_value = attrgetter(field.name)(row)
            update.add_field(field.name, field_value, field.timestamp)
        return update
