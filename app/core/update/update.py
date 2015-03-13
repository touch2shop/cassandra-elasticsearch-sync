from app.core.abstract_data_object import AbstractDataObject
from app.core.update.field_update import FieldUpdate


class Update(AbstractDataObject):

    def __init__(self, event, fields=None):
        self._event = event
        if fields:
            self._fields = set(fields)
        else:
            self._fields = set()

    @property
    def event(self):
        return self._event

    @property
    def identifier(self):
        return self._event.identifier

    @property
    def is_delete(self):
        return self._event.is_delete

    @property
    def timestamp(self):
        return self._event.timestamp

    def add_field(self, name, value, timestamp):
        self._fields.add(FieldUpdate(name, value, timestamp))

    @property
    def fields(self):
        return self._fields

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def __le__(self, other):
        return self.timestamp <= other.timestamp

    def __gt__(self, other):
        return self.timestamp > other.timestamp

    def __ge__(self, other):
        return self.timestamp >= other.timestamp

    # noinspection PyProtectedMember
    def _deep_equals(self, other):
        return self._event == other._event and self._fields == other._fields

    def _deep_hash(self):
        return hash((self._event, frozenset(self._fields)))

    def _deep_string_dictionary(self):
        return {
            "identifier": repr(self.identifier),
            "is_delete": self.is_delete,
            "timestamp": self.timestamp,
            "fields": repr(self.fields),
            "event": repr(self.event)
        }
