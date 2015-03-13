

class SyncState(object):

    def __init__(self, last_sync_timestamp=None):
        self._last_sync_timestamp = last_sync_timestamp

    @property
    def last_sync_timestamp(self):
        return self._last_sync_timestamp

    @last_sync_timestamp.setter
    def last_sync_timestamp(self, value):
        self._last_sync_timestamp = value
