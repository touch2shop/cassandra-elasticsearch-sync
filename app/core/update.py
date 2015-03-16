from app.core.abstract_data_object import AbstractDataObject
from app.core.update_field import UpdateField


class Update(AbstractDataObject):

    def __init__(self, identifier=None, is_delete=None, timestamp=None, fields=None):
        self._identifier = identifier
        self._is_delete = is_delete
        self._timestamp = timestamp
        if fields:
            self._fields = set(fields)
        else:
            self._fields = set()

    @property
    def identifier(self):
        return self._identifier

    @identifier.setter
    def identifier(self, value):
        self._identifier = value

    @property
    def is_delete(self):
        return self._is_delete

    @is_delete.setter
    def is_delete(self, value):
        self._is_delete = value

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        self._timestamp = value

    @property
    def fields(self):
        return self._fields

    def add_field(self, name, value):
        self._fields.add(UpdateField(name, value))

    # noinspection PyProtectedMember
    def _deep_equals(self, other):
        return self._identifier == other._identifier and \
            self._is_delete == other._is_delete and \
            self._timestamp == other._timestamp and \
            self._fields == other._fields

    def _deep_hash(self):
        return hash((self._identifier, self._is_delete, self._timestamp, frozenset(self._fields)))

    def _deep_string_dictionary(self):
        return {
            "identifier": self.identifier,
            "is_delete": self.is_delete,
            "timestamp": self.timestamp,
            "fields": self.fields
        }
