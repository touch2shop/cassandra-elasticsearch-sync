from app.core.abstract_data_object import AbstractDataObject
from app.core.field import Field


class Document(AbstractDataObject):

    def __init__(self, identifier=None, timestamp=None, fields=None):
        self._identifier = identifier
        self._timestamp = timestamp
        self._fields = dict()
        self.fields = fields

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

    @fields.setter
    def fields(self, value):
        self._fields.clear()
        if value:
            for field in value:
                self._fields[field.name] = field

    def add_field(self, name, value):
        if name not in self._fields:
            self._fields[name] = Field(name, value)
        else:
            raise KeyError("Field with name %s already exists." % name)

    def get_field_value(self, name):
        field = self._fields.get(name, None)
        if field:
            return field.value
        else:
            return None

    def set_field_value(self, name, value):
        if name in self._fields:
            field = self._fields[name]
            field.value = value
        else:
            raise KeyError("Field with name %s does not exists." % name)

    def _deep_equals(self, other):
        return self.identifier == other.identifier and \
            self.timestamp == other.timestamp and \
            self.fields == other.fields

    def _deep_hash(self):
        return hash((self.identifier, self.timestamp, frozenset(self.fields)))

    def _deep_string_dictionary(self):
        return {
            "identifier": self.identifier,
            "timestamp": self.timestamp,
            "fields": self.fields
        }
