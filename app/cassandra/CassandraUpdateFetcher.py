from operator import attrgetter

from app.sync.EntityIdentifier import EntityIdentifier
from app.sync.EntityUpdate import EntityUpdate
from app.sync.UpdateEvent import UpdateEvent
from app.sync.UpdateEventCombiner import UpdateEventCombiner


_ID_COLUMN_NAME = "id"


class CassandraUpdateFetcher(object):

    def __init__(self, log_entry_store):
        self._log_entry_store = log_entry_store
        self._cassandra_client = log_entry_store

    def fetch_updates(self, minimum_log_entry_time=None):
        log_entries = self._fetch_log_entries(minimum_log_entry_time)
        combined_update_events = self._combine_update_events(log_entries)
        return self._fetch_entity_updates(combined_update_events)

    def _fetch_log_entries(self, minimum_time):
        if minimum_time is None:
            return self._log_entry_store.find_all()
        else:
            return self._log_entry_store.find_by_time_greater_or_equal_than(minimum_time)

    @classmethod
    def _combine_update_events(cls, log_entries):
        update_events = []
        for log_entry in log_entries:
            update_events.append(cls._build_update_event(log_entry))

        return UpdateEventCombiner.combine(update_events)

    @classmethod
    def _build_update_event(cls, log_entry):
        identifier = EntityIdentifier(log_entry.logged_keyspace, log_entry.logged_table, log_entry.logged_key)
        return UpdateEvent(identifier, log_entry.timestamp, log_entry.is_delete, log_entry.updated_columns)

    def _fetch_entity_updates(self, combined_update_events):
        entity_updates = []
        for update_event in combined_update_events:
            entity_update = self._fetch_entity_update(update_event)
            if entity_update:
                entity_updates.append(entity_update)

    def _fetch_entity_update(self, update_event):
        if update_event.is_delete:
            return EntityUpdate(update_event)
        else:
            return self._fetch_save_entity_update(update_event)

    def _fetch_save_entity_update(self, update_event):
        _id = update_event.identifier
        rows = self._cassandra_client.select_by_id(_id.table, _id.key, update_event.field_names,
                                                   _id.namespace, _ID_COLUMN_NAME)

        if len(rows) > 1:
            raise Exception("More than one row found for identifier %s on Cassandra." % _id)
        elif len(rows) == 0:
            # Not found. No action will be performed.
            # If the entity was deleted, a delete event will be will be available in the next log fetch.
            return None
        return self._to_entity_update(rows[0], update_event)

    @classmethod
    def _to_entity_update(cls, row, combined_update_event):
        entity_update = EntityUpdate(combined_update_event)
        for field in combined_update_event.fields:
            field_value = attrgetter(field.name)(row)
            entity_update.add_field(field.name, field_value, field.timestamp)
        return entity_update
