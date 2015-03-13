from app.core.abstract_data_object import AbstractDataObject


class UpdateEvent(AbstractDataObject):

    def __init__(self, identifier, timestamp, is_delete=False, field_names=None):
        self._identifier = identifier
        self._timestamp = timestamp
        self._is_delete = is_delete
        self._field_names = field_names

    @property
    def identifier(self):
        return self._identifier

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def is_delete(self):
        return self._is_delete

    @property
    def field_names(self):
        return self._field_names

    # FIXME: migrate to python 3 rich comparisons
    def __cmp__(self, other):
        if self.timestamp != other.timestamp:
            return cmp(self.timestamp, other.timestamp)
        else:
            return cmp(self.is_delete, other.is_delete)

    def _deep_hash(self):
        current_hash = hash((self.identifier, self.timestamp, self.is_delete))
        if self.field_names is None:
            return current_hash
        else:
            return hash((current_hash, frozenset(self.field_names)))

    def _deep_equals(self, other):
        return self.identifier == other.identifier and \
            self.timestamp == other.timestamp and \
            self.is_delete == other.is_delete and \
            self.field_names == other.field_names

    def _deep_string_dictionary(self):
        return {
            "identifier": self.identifier,
            "timestamp": self.timestamp,
            "is_delete": self.is_delete,
            "field_names": repr(self.field_names)
        }
