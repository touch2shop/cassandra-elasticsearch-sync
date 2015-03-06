
class ElasticsearchDocument:

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
