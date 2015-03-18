import os
import shutil
from uuid import uuid4
import arrow
import pytest

from app.sync_state_store import SyncStateStore
from app.sync_state_store import DATE_FORMAT


@pytest.fixture(scope="session")
def existing_filename(resources_directory):
    return os.path.join(resources_directory, "state.yaml")


def random_filename():
    return unicode(uuid4()) + ".yaml"


# noinspection PyShadowingNames
def copy_file(tmpdir, existing_filename):
    copied_filename = unicode(tmpdir.join(random_filename()))
    shutil.copy(existing_filename, copied_filename)
    return copied_filename


# noinspection PyClassHasNoInit,PyMethodMayBeStatic,PyShadowingNames
class TestSyncStateStore:

    def test_load_from_existing_file(self, existing_filename):
        store = SyncStateStore(existing_filename)
        state = store.load()
        assert state

        last_elasticsearch_to_cassandra_brazil_time = arrow.get("2015-01-23T15:01:22.654321-03:00", DATE_FORMAT)
        last_elasticsearch_to_cassandra_utc_time = arrow.get("2015-01-23T18:01:22.654321-00:00", DATE_FORMAT)
        assert state.last_elasticsearch_to_cassandra_sync == last_elasticsearch_to_cassandra_brazil_time.float_timestamp
        assert state.last_elasticsearch_to_cassandra_sync == last_elasticsearch_to_cassandra_utc_time.float_timestamp

        last_cassandra_to_elasticsearch_brazil_time = arrow.get("2015-01-23T14:55:33.123456-03:00", DATE_FORMAT)
        last_cassandra_to_elasticsearch_utc_time = arrow.get("2015-01-23T17:55:33.123456-00:00", DATE_FORMAT)
        assert state.last_cassandra_to_elasticsearch_sync == last_cassandra_to_elasticsearch_brazil_time.float_timestamp
        assert state.last_cassandra_to_elasticsearch_sync == last_cassandra_to_elasticsearch_utc_time.float_timestamp

    def test_load_default_if_file_not_exists(self, tmpdir):
        filename = unicode(tmpdir.join("does_not_exist.yaml"))
        store = SyncStateStore(filename)

        state = store.load()
        assert state
        assert state.last_cassandra_to_elasticsearch_sync is None
        assert state.last_elasticsearch_to_cassandra_sync is None

    def test_load_from_existing_file_them_save(self, tmpdir, existing_filename):
        copied_filename = copy_file(tmpdir, existing_filename)
        store = SyncStateStore(copied_filename)
        state = store.load()

        last_cassandra_to_elasticsearch_brazil_time = arrow.get("2015-01-23T14:55:33.123456-03:00", DATE_FORMAT)
        state.last_cassandra_to_elasticsearch_sync = last_cassandra_to_elasticsearch_brazil_time.timestamp
        last_elasticsearch_to_cassandra_brazil_time = arrow.get("2015-01-23T15:01:22.654321-03:00", DATE_FORMAT)
        state.last_elasticsearch_to_cassandra_sync = last_elasticsearch_to_cassandra_brazil_time.timestamp

        state.save()

        state = store.load()
        assert state.last_cassandra_to_elasticsearch_sync == last_cassandra_to_elasticsearch_brazil_time.timestamp
        assert state.last_elasticsearch_to_cassandra_sync == last_elasticsearch_to_cassandra_brazil_time.timestamp

    def test_load_default_from_non_existent_file_them_save(self, tmpdir):
        filename = unicode(tmpdir.join(random_filename()))
        store = SyncStateStore(filename)
        state = store.load()

        last_cassandra_to_elasticsearch_brazil_time = arrow.get("2015-01-23T14:55:33.123456-03:00", DATE_FORMAT)
        state.last_cassandra_to_elasticsearch_sync = last_cassandra_to_elasticsearch_brazil_time.timestamp
        last_elasticsearch_to_cassandra_brazil_time = arrow.get("2015-01-23T15:01:22.654321-03:00", DATE_FORMAT)
        state.last_elasticsearch_to_cassandra_sync = last_elasticsearch_to_cassandra_brazil_time.timestamp

        state.save()

        state = store.load()
        assert state.last_cassandra_to_elasticsearch_sync == last_cassandra_to_elasticsearch_brazil_time.timestamp
        assert state.last_elasticsearch_to_cassandra_sync == last_elasticsearch_to_cassandra_brazil_time.timestamp

    def test_load_from_existing_file_then_save_none(self, tmpdir, existing_filename):
        copied_filename = copy_file(tmpdir, existing_filename)
        store = SyncStateStore(copied_filename)
        state = store.load()

        state.last_cassandra_to_elasticsearch_sync = None
        state.last_elasticsearch_to_cassandra_sync = None

        state.save()

        state = store.load()
        assert state.last_cassandra_to_elasticsearch_sync is None
        assert state.last_elasticsearch_to_cassandra_sync is None
