from app.core.abstract_data_object import AbstractDataObject
from app.core.value_field import ValueField


class GenericEntity(AbstractDataObject):

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

    def _deep_string_dictionary(self):
        return {
            "identifier": self.identifier,
            "fields": self.fields,
            "timestamp": self.timestamp
        }
