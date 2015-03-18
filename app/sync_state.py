

class SyncState(object):

    def __init__(self, store, last_cassandra_to_elasticsearch_sync=None, last_elasticsearch_to_cassandra_sync=None):
        self._store = store
        self._last_cassandra_to_elasticsearch_sync = last_cassandra_to_elasticsearch_sync
        self._last_elasticsearch_to_cassandra_sync = last_elasticsearch_to_cassandra_sync

    @property
    def last_cassandra_to_elasticsearch_sync(self):
        return self._last_cassandra_to_elasticsearch_sync

    @last_cassandra_to_elasticsearch_sync.setter
    def last_cassandra_to_elasticsearch_sync(self, value):
        self._last_cassandra_to_elasticsearch_sync = value

    @property
    def last_elasticsearch_to_cassandra_sync(self):
        return self._last_elasticsearch_to_cassandra_sync

    @last_elasticsearch_to_cassandra_sync.setter
    def last_elasticsearch_to_cassandra_sync(self, value):
        self._last_elasticsearch_to_cassandra_sync = value

    def save(self):
        self._store.save(self)
