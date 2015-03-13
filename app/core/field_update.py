from app.core.value_field import ValueField


class FieldUpdate(ValueField):

    def __init__(self, name, value, timestamp):
        super(FieldUpdate, self).__init__(name, value)
        self._timestamp = timestamp

    @property
    def timestamp(self):
        return self._timestamp

    # FIXME: couldn't find a way to call ValueField super method without causing an infinite recursion
    def _deep_equals(self, other):
        return self.name == other.name and \
            self.value == other.value and \
            self.timestamp == other.timestamp

    # FIXME: couldn't find a way to call ValueField super method without causing an infinite recursion
    def _deep_hash(self):
        return hash((self.name, self.value, self.timestamp))

    # FIXME: couldn't find a way to call ValueField super method without causing an infinite recursion
    def _deep_string_dictionary(self):
        return {
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp
        }
