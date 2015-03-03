from sortedcontainers import SortedSet
from app.sync.UpdateEventField import UpdateEventField


class CombinedUpdateEvent(object):

    def __init__(self, identifier):
        self._identifier = identifier
        self._update_events = SortedSet()
        self._fields = {}

    def add_update_event(self, update):
        self._update_events.add(update)
        if update.field_names:
            for field_name in update.field_names:
                if field_name not in self._fields:
                    self._fields[field_name] = UpdateEventField(field_name, update.timestamp)
                elif update.timestamp > self._fields[field_name].timestamp:
                        self._fields[field_name] = UpdateEventField(field_name, update.timestamp)

    @property
    def fields(self):
        return self._fields.values()

    @property
    def field_names(self):
        return self._fields.keys()

    def get_field(self, name):
        return self._fields[name]

    @property
    def is_delete(self):
        return self._get_most_recent_update().is_delete

    @property
    def identifier(self):
        return self._identifier

    @property
    def timestamp(self):
        return self._get_most_recent_update().timestamp

    def _get_most_recent_update(self):
        return self._update_events[-1]
