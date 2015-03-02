from time_uuid import TimeUUID
from app.core.AbstractDataObject import AbstractDataObject


class CassandraLogEntry(AbstractDataObject):

    def __init__(self):
        self._logged_keyspace = None
        self._logged_table = None
        self._logged_key = None
        self._time_uuid = None
        self._operation = None
        self._updated_columns = None

    @property
    def time_uuid(self):
        return self._time_uuid

    @time_uuid.setter
    def time_uuid(self, value):
        if value is not None:
            self._time_uuid = TimeUUID.convert(value)
        else:
            self._time_uuid = None

    @property
    def time(self):
        if self.time_uuid is not None:
            return self.time_uuid.get_datetime()
        else:
            return None

    @property
    def timestamp(self):
        if self.time_uuid is not None:
            return self.time_uuid.get_timestamp()
        else:
            return None

    @property
    def logged_keyspace(self):
        return self._logged_keyspace

    @logged_keyspace.setter
    def logged_keyspace(self, value):
        self._logged_keyspace = value
        
    @property
    def logged_table(self):
        return self._logged_table

    @logged_table.setter
    def logged_table(self, value):
        self._logged_table = value

    @property
    def logged_key(self):
        return self._logged_key

    @logged_key.setter
    def logged_key(self, value):
        self._logged_key = value
        
    @property
    def operation(self):
        return self._operation

    @operation.setter
    def operation(self, value):
        self._operation = value

    @property
    def is_delete(self):
        return self._operation.lower() == "delete"

    @property
    def updated_columns(self):
        return self._updated_columns

    @updated_columns.setter
    def updated_columns(self, value):
        if value is not None:
            self._updated_columns = set(value)
        else:
            self._updated_columns = None

    def __cmp__(self, other):
        if self.time_uuid is not None:
            return TimeUUID.__cmp__(self.time_uuid, other.time_uuid)
        else:
            return -1

    def _deep_equals(self, other):
        return self.time_uuid == other.time_uuid and \
            self.logged_keyspace == other.logged_keyspace and \
            self.logged_table == other.logged_table and \
            self.logged_key == other.logged_key and \
            self.operation == other.operation and \
            self.updated_columns == other.updated_columns

    def _deep_hash(self):
        return hash((self.time_uuid, self.logged_keyspace, self.logged_table, self.logged_key,
                     self.operation, frozenset(self.updated_columns)))

    def __repr__(self):
        return repr({
            "time_uuid": repr(self.time_uuid),
            "logged_keyspace": repr(self.logged_keyspace),
            "logged_table": repr(self.logged_table),
            "logged_key": repr(self.logged_key),
            "operation": repr(self.operation),
            "updated_columns": repr(self.updated_columns),
        })