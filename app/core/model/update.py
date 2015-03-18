from app.core.model.document import Document


class Update(Document):

    def __init__(self, identifier=None, is_delete=None, timestamp=None, fields=None):
        super(Update, self).__init__(identifier=identifier, timestamp=timestamp, fields=fields)
        self._is_delete = is_delete

    @property
    def is_delete(self):
        return self._is_delete

    @is_delete.setter
    def is_delete(self, value):
        self._is_delete = value

    def _deep_equals(self, other):
        return self.identifier == other.identifier and \
            self.is_delete == other.is_delete and \
            self.timestamp == other.timestamp and \
            self.fields == other.fields

    def _deep_hash(self):
        return hash((self.identifier, self.is_delete, self.timestamp, frozenset(self.fields)))

    def _deep_string_dictionary(self):
        return {
            "identifier": self.identifier,
            "is_delete": self.is_delete,
            "timestamp": self.timestamp,
            "fields": self.fields
        }

    @classmethod
    def from_document(cls, document, is_delete):
        update = Update()
        update.identifier = document.identifier
        update.timestamp = document.timestamp
        update.fields = document.fields
        update.is_delete = is_delete
        return update
