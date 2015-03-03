from operator import attrgetter
from app.sync.CombinedUpdateEvent import CombinedUpdateEvent
from app.sync.EntityIdentifier import EntityIdentifier
from app.sync.EntityUpdate import EntityUpdate
from app.sync.UpdateEvent import UpdateEvent


_ID_NAME = "id"


class CassandraUpdateFetcher():

    def __init__(self, log_entry_store):
        self._log_entry_store = log_entry_store
        self._cassandra_client = log_entry_store

    def fetch_updates(self, minimum_time):
        log_entries = self._fetch_log_entries(minimum_time)
        combined_update_events = self._combine_update_events(log_entries)
        return self._fetch_entity_updates(combined_update_events)

    def _fetch_log_entries(self, minimum_time):
        if minimum_time is None:
            log_entries = self._log_entry_store.find_all()
        else:
            log_entries = self._log_entry_store.find_by_time_greater_or_equal_than(minimum_time)
        return log_entries

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
        identifier = update_event.identifier
        rows = self._cassandra_client.select_by_id(identifier.table, identifier.key, identifier.namespace, _ID_NAME)

        if len(rows) > 1:
            raise Exception("More than one row found for identifier %s on Cassandra." % identifier)
        elif len(rows) == 0:
            # Not found. No action will be performed.
            # If the entity was deleted, it will be will be available in the next log batch.
            return None

        return self._to_entity_update(rows[0], update_event)

    @staticmethod
    def _to_entity_update(row, update_event):
        entity_update = EntityUpdate(update_event)
        for field_name in update_event.field_names:
            field_value = attrgetter(field_name)(row)
            entity_update.add_field(field_name, field_value)
        return entity_update

    @classmethod
    def _combine_update_events(cls, log_entries):
        combined = set()
        for log_entry in log_entries:
            combined.add(CombinedUpdateEvent(cls._build_update_event(log_entry)))
        return combined

    @classmethod
    def _build_update_event(cls, log_entry):
        identifier = EntityIdentifier(log_entry.logged_keyspace, log_entry.logged_table, log_entry.logged_key)
        return UpdateEvent(identifier, log_entry.timestamp, log_entry.is_delete, log_entry.updated_columns)
