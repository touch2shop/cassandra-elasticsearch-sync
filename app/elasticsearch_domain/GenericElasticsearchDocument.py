from app.core.AbstractDataObject import AbstractDataObject
from app.core.ValueField import ValueField


class GenericElasticsearchDocument(AbstractDataObject):
    def __init__(self, identifier=None, timestamp=None, fields=None):
        self._identifier = identifier
        self._timestamp = timestamp
        self._fields = dict()
        if fields:
            for field in fields:
                self._fields[field.name] = field

    @property
    def identifier(self):
        return self._identifier

    @identifier.setter
    def identifier(self, value):
        self._identifier = value

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        self._timestamp = value

    @property
    def fields(self):
        return self._fields.values()

    def add_field(self, name, value):
        self._fields[name] = ValueField(name, value)

    def get_field_value(self, name):
        field = self._fields.get(name, None)
        if field:
            return field.value
        else:
            return None

    def set_field_value(self, name, value):
        self._fields[name] = ValueField(name, value)

    # noinspection PyProtectedMember
    def _deep_equals(self, other):
        return self._identifier == other._identifier and \
            self._timestamp == other._timestamp and \
            self._fields == other._fields

    def _deep_hash(self):
        if self._fields:
            return hash((self._identifier, self._timestamp, self._fields))
        else:
            return hash((self._identifier, self._timestamp))

    def _deep_string(self):
        return {
            "identifier": self.identifier,
            "fields": self.fields,
            "timestamp": self.timestamp
        }
