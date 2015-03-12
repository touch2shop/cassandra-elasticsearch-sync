from app.core.AbstractDataObject import AbstractDataObject


class ValueField(AbstractDataObject):

    def __init__(self, name, value):
        self._name = name
        self._value = value

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value

    def _deep_equals(self, other):
        return self.name == other.name and \
            self.value == other.value

    def _deep_hash(self):
        return hash((self.name, self.value))

    def _deep_string_dictionary(self):
        return {
            "name": self.name,
            "value": self.value
        }
