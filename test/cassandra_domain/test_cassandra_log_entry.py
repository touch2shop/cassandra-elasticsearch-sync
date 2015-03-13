from datetime import datetime
from time_uuid import TimeUUID
from app.cassandra_domain.cassandra_log_entry import CassandraLogEntry


# noinspection PyMethodMayBeStatic,PyClassHasNoInit
class TestCassandraLogEntry:

    def test_is_delete(self):
        assert CassandraLogEntry(operation="delete").is_delete is True
        assert CassandraLogEntry(operation="DELETE").is_delete is True
        assert CassandraLogEntry(operation="save").is_delete is False
        assert CassandraLogEntry(operation="bananas").is_delete is False
        assert CassandraLogEntry(operation=None).is_delete is False

    def test_is_save(self):
        assert CassandraLogEntry(operation="save").is_save is True
        assert CassandraLogEntry(operation="SAVE").is_save is True
        assert CassandraLogEntry(operation="bananas").is_save is False
        assert CassandraLogEntry(operation="delete").is_save is False
        assert CassandraLogEntry(operation=None).is_save is False

    def test_get_timestamp(self):
        utc_time = datetime.utcnow()
        log_entry = CassandraLogEntry(time_uuid=TimeUUID.convert(utc_time))

        assert datetime.utcfromtimestamp(log_entry.timestamp) == utc_time

    def test_get_time(self):
        utc_time = datetime.utcnow()
        log_entry = CassandraLogEntry(time_uuid=TimeUUID.convert(utc_time))

        assert log_entry.time == utc_time
