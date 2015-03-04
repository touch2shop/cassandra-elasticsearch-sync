from app.core.AbstractDataObject import AbstractDataObject
from app.sync.FieldUpdate import FieldUpdate


class EntityUpdate(AbstractDataObject):

    def __init__(self, event):
        self._event = event
        self._fields = []

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
        self._fields.append(FieldUpdate(name, value, timestamp))

    @property
    def get_fields(self):
        return self._fields

    def _deep_equals(self, other):
        return self._event == other._event and \
               self._fields == other._fields

    def _deep_hash(self):
        if self._fields:
            return hash((self._event, frozenset(self._fields)))
        else:
            return hash(self._event)
