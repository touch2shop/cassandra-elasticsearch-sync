import os
import shutil
import arrow
import pytest

from app.sync_state_store import SyncStateStore
from app.sync_state_store import DATE_FORMAT


@pytest.fixture(scope="session")
def existing_filename(resources_directory):
    return os.path.join(resources_directory, "state.yaml")


# noinspection PyClassHasNoInit,PyMethodMayBeStatic,PyShadowingNames
class TestSyncStateStore:

    def test_load_from_existing_file(self, existing_filename):
        store = SyncStateStore(existing_filename)
        state = store.load()
        assert state

        brazil_time = arrow.get("2015-01-23T14:55:33.123456-03:00", DATE_FORMAT)
        utc_time = arrow.get("2015-01-23T17:55:33.123456-00:00", DATE_FORMAT)
        assert state.last_sync_timestamp == brazil_time.float_timestamp == utc_time.float_timestamp

    def test_load_default_if_file_not_exists(self, tmpdir):
        filename = str(tmpdir.join("does_not_exist.yaml"))
        store = SyncStateStore(filename)

        state = store.load()
        assert state
        assert state.last_sync_timestamp is None

    def test_load_from_existing_file_them_save(self, tmpdir, existing_filename):
        copied_filename = str(tmpdir.join("copied_state.yaml"))
        shutil.copy(existing_filename, copied_filename)
        store = SyncStateStore(copied_filename)
        state = store.load()

        brazil_time = arrow.get("2015-01-23T14:55:33.123456-03:00", DATE_FORMAT)
        state.last_sync_timestamp = brazil_time.timestamp
        store.save(state)

        state = store.load()
        assert state.last_sync_timestamp == brazil_time.timestamp


    def test_load_default_from_non_existent_file_them_save(self, tmpdir):
        filename = str(tmpdir.join("new_state.yaml"))
        store = SyncStateStore(filename)
        state = store.load()

        brazil_time = arrow.get("2015-01-23T14:55:33.123456-03:00", DATE_FORMAT)
        state.last_sync_timestamp = brazil_time.timestamp
        store.save(state)

        state = store.load()
        assert state.last_sync_timestamp == brazil_time.timestamp
