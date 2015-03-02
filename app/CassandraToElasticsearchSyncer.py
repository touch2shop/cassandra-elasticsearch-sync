from app.cassandra.CassandraLogEntryStore import CassandraLogEntryStore
from app.sync.EntityIdentifier import EntityIdentifier
from app.sync.CombinedUpdateEvent import CombinedUpdateEvent
from app.sync.UpdateEvent import UpdateEvent


class CassandraToElasticsearchSyncer:

    def __init__(self, cassandra_nodes, cassandra_log_keyspace, cassandra_log_table):
        self._cassandra_log_entry_store = CassandraLogEntryStore(
            cassandra_nodes, cassandra_log_keyspace, cassandra_log_table)

    def sync(self, minimum_time=None):
        if minimum_time is None:
            log_entries = self._cassandra_log_entry_store.find_all()
        else:
            log_entries = self._cassandra_log_entry_store.find_by_time_greater_or_equal_than(minimum_time)

        update_descriptors = self._get_combined_descriptors(log_entries)

    @classmethod
    def _get_combined_descriptors(cls, log_entries):
        combined_updates = set()
        for log_entry in log_entries:
            combined_updates.add(CombinedUpdateEvent(cls._to_entity_update_descriptor(log_entry)))
        return combined_updates

    def _fetch_combined_updates(self, combined_update_descriptors):
        for update_descriptor in combined_update_descriptors:
            pass

    @classmethod
    def _to_entity_update_descriptor(cls, log_entry):
        identifier = EntityIdentifier(log_entry.logged_keyspace, log_entry.logged_table, log_entry.logged_key)
        return UpdateEvent(identifier, log_entry.timestamp, log_entry.is_delete, log_entry.updated_columns)

    def _apply_updates_to_elastic_search(self):
        pass
