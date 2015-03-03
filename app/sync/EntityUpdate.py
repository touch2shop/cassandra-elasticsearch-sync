from app.sync.FieldUpdate import FieldUpdate


class EntityUpdate(object):

    def __init__(self, event):
        self._event = event
        self._fields = []

    @property
    def identifier(self):
        return self._event.identifier

    @property
    def is_delete(self):
        return self._event.is_delete

    @property
    def timestamp(self):
        return self._event.timestamp

    def add_field(self, name, value, timestamp):
        self._fields.append(FieldUpdate(name, value, timestamp))

    @property
    def get_fields(self):
        return self._fields
