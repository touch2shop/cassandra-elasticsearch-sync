import random
import uuid

import arrow
import pytest
from time_uuid import TimeUUID
from hamcrest import *

from app.cassandra_domain.cassandra_log_entry import CassandraLogEntry


@pytest.fixture(scope="function", autouse=True)
def setup(cassandra_log_entry_store):
    cassandra_log_entry_store.delete_all()


class TestCassandraLogEntryStore(object):

    def test_create_log_entry(self, cassandra_log_entry_store):
        entry = self.build_log_entry(time_uuid=uuid.uuid1(),
                                     logged_keyspace="test_keyspace",
                                     logged_table="test_table",
                                     logged_key=str(uuid.uuid4()),
                                     operation="save",
                                     updated_columns={"a", "b", "c"})
        cassandra_log_entry_store.create(entry)

        rows = cassandra_log_entry_store.find_by_logged_row(entry.logged_keyspace, entry.logged_table, entry.logged_key)
        log_entries = rows.to_list()
        assert len(log_entries) == 1
        assert log_entries[0] == entry

    def test_find_all(self, cassandra_log_entry_store):
        entries = list()
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-03-05T00:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-03-05T00:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-03-05T15:59:59.999999-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="delete",
                                            updated_columns={"d"}))
        for entry in entries:
            cassandra_log_entry_store.create(entry)

        found = cassandra_log_entry_store.find_all().to_list()
        assert_that(found, has_length(len(entries)))
        assert_that(found, has_items(*entries))

    def test_find_log_entries_filtering_by_minimum_timestamp(self, cassandra_log_entry_store):
        minimum_timestamp = arrow.get("2015-01-02T16:00:00.000000-00:00").float_timestamp
        entries = list()

        # These entries SHOULD NOT be included in the results
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-01T00:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-02T00:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-02T15:59:59.999999-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))

        # These entries SHOULD be included in the results
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-02T16:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-03T00:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-04T00:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-05T00:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-05T15:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))

        shuffled_entries = list(entries)
        random.shuffle(shuffled_entries)
        for entry in shuffled_entries:
            cassandra_log_entry_store.create(entry)

        found_entries = cassandra_log_entry_store.find_by_time_greater_or_equal_than(minimum_timestamp).to_list()

        assert_that(found_entries, has_length(5))
        assert_that(found_entries, is_not(has_items(entries[0], entries[1], entries[2])))
        assert_that(found_entries, has_items(entries[3], entries[4], entries[5], entries[6], entries[7]))

    @staticmethod
    def _create_time_uuid(date_time_string):
        return TimeUUID.convert(arrow.get(date_time_string).datetime)

    @staticmethod
    def build_log_entry(time_uuid, logged_keyspace, logged_table, logged_key, operation, updated_columns):
        log_entry = CassandraLogEntry()
        log_entry.time_uuid = time_uuid
        log_entry.logged_keyspace = logged_keyspace
        log_entry.logged_table = logged_table
        log_entry.logged_key = logged_key
        log_entry.operation = operation
        log_entry.updated_columns = updated_columns
        return log_entry
