
class EntityUpdate(object):

    def __init__(self, event):
        self._event = event
        self._fields = {}

    @property
    def identifier(self):
        return self._event.identifier

    @property
    def timestamp(self):
        return self._event.timestamp

    def add_field(self, name, value):
        self._fields[name] = value

    def get_field(self, name):
        return self._fields[name]

    def get_field_names(self):
        return self._fields.keys()

    def has_field(self, name):
        return name in self._fields
