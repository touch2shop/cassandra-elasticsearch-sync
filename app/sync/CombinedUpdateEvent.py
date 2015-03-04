from sortedcontainers import SortedSet
from app.core.AbstractDataObject import AbstractDataObject
from app.sync.UpdateEventField import UpdateEventField


class CombinedUpdateEvent(AbstractDataObject):

    def __init__(self, identifier):
        self._identifier = identifier
        self._update_events = SortedSet()
        self._fields = {}

    def add(self, update_event):
        self._update_events.add(update_event)
        if update_event.field_names:
            for field_name in update_event.field_names:
                if field_name not in self._fields:
                    self._fields[field_name] = UpdateEventField(field_name, update_event.timestamp)
                elif update_event.timestamp > self._fields[field_name].timestamp:
                        self._fields[field_name] = UpdateEventField(field_name, update_event.timestamp)

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
        if len(self._update_events) > 0:
            return self._update_events[-1]
        else:
            return None

    # noinspection PyProtectedMember
    def _deep_equals(self, other):
        return self._identifier == other._identifier and \
            self._update_events == other._update_events

    def _deep_hash(self):
        if self._update_events:
            return hash((self._identifier, frozenset(self._update_events)))
        else:
            return hash(self._identifier)

    def __repr__(self):
        return repr({
            "identifier": repr(self.identifier),
            "timestamp": self.timestamp,
            "is_delete": self.is_delete,
            "fields": repr(self.fields)
        })
