from app.core.AbstractDataObject import AbstractDataObject


class FieldUpdate(AbstractDataObject):

    def __init__(self, name, value, timestamp):
        self._name = name
        self._value = value
        self._timestamp = timestamp

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value

    @property
    def timestamp(self):
        return self._timestamp

    def _deep_equals(self, other):
        return self.name == other.name and \
            self.value == other.value and \
            self.timestamp == other.timestamp

    def _deep_hash(self):
        return hash((self.name, self.value, self.timestamp))

    def __repr__(self):
        return repr({
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp
        })
