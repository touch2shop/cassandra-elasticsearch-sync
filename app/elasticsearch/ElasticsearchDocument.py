from app.core.AbstractDataObject import AbstractDataObject


class ElasticsearchDocument(AbstractDataObject):

    def __init__(self, identifier, timestamp=None, fields=None):
        self._identifier = identifier
        if fields:
            self._fields = fields
        else:
            self._fields = list()
        self._timestamp = timestamp

    @property
    def identifier(self):
        return self._identifier

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def fields(self):
        return self._fields

    @classmethod
    def from_update(cls, update):
        return ElasticsearchDocument(update.identifier, update.timestamp, list(update.fields))

    def _deep_equals(self, other):
        return self.identifier == other.identifier and \
               self.timestamp == other.timestamp and \
               self.fields == other.fields

    def _deep_hash(self):
        if self.fields:
            return hash((self.identifier, self.timestamp, self.fields))
        else:
            return hash((self.identifier, self.timestamp))

    def _deep_string(self):
        return {
            "identifier": self._identifier,
            "fields": self._fields,
            "timestamp": self._timestamp
        }
