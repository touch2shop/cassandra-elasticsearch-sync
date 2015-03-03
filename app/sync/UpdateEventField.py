from app.core.AbstractDataObject import AbstractDataObject


class UpdateEventField(AbstractDataObject):

    def __init__(self, name, timestamp):
        self._name = name
        self._timestamp = timestamp

    @property
    def name(self):
        return self._name

    @property
    def timestamp(self):
        return self._timestamp

    def _deep_equals(self, other):
        return self.name == other.name and self.timestamp == other.timestamp

    def _deep_hash(self):
        return hash((self.name, self.timestamp))
