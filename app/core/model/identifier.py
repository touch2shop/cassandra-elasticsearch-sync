from app.core.model.abstract_data_object import AbstractDataObject


class Identifier(AbstractDataObject):

    def __init__(self, namespace, table, key):
        self._namespace = namespace
        self._table = table
        self._key = key

    @property
    def namespace(self):
        return self._namespace

    @property
    def table(self):
        return self._table

    @property
    def key(self):
        return self._key

    def _deep_equals(self, other):
        return self.namespace == other.namespace and self.table == other.table and self.key == other.key

    def __hash__(self):
        return hash((self.namespace, self.table, self.key))

    def __repr__(self):
        return "(%s, %s, %s)" % (self.namespace, self.table, self.key)
