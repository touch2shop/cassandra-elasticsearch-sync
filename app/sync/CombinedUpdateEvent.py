from sortedcontainers import SortedSet


class CombinedUpdateEvent(object):

    def __init__(self, identifier):
        self._updates = SortedSet()
        self._identifier = identifier

    def add_update(self, update):
        self._updates.add(update)

    @property
    def field_names(self):
        field_names = set()
        for update in self._updates:
            for field_name in update.field_names:
                field_names.add(field_name)
        return field_names

    @property
    def is_delete(self):
        return self._get_most_recent_update().is_delete

    @property
    def identifier(self):
        return self._identifier

    @property
    def timestamp(self):
        return self._get_most_recent_update().timestamp

    def _get_most_recent_update(self):
        return self._updates.index(-1)
