from app.cassandra.CassandraLogEntry import CassandraLogEntry


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
