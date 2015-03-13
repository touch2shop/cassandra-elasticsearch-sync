import os
import pytest
import app.settings
from app.settings import Settings


# noinspection PyShadowingNames
@pytest.fixture(scope="module")
def full_settings_file_name(resources_directory):
    return os.path.join(resources_directory, "full_settings.yaml")


# noinspection PyShadowingNames
@pytest.fixture(scope="module")
def empty_settings_file_name(resources_directory):
    return os.path.join(resources_directory, "empty_settings.yaml")


# noinspection PyMethodMayBeStatic,PyClassHasNoInit,PyShadowingNames
class TestSettings:

    def test_load_full_settings_from_file(self, full_settings_file_name):
        settings = Settings.load_from_file(full_settings_file_name)
        assert settings.cassandra_log_keyspace == "test_logger"
        assert settings.cassandra_log_table == "test_log"
        assert settings.cassandra_id_column_name == "test_id"
        assert settings.cassandra_timestamp_column_name == "test_timestamp"
        assert settings.interval_between_runs == 60

    def test_load_defaults_if_settings_from_file_is_empty(self, empty_settings_file_name):
        settings = Settings.load_from_file(empty_settings_file_name)
        assert settings.cassandra_log_keyspace == app.settings.DEFAULT_CASSANDRA_LOG_KEYSPACE
        assert settings.cassandra_log_table == app.settings.DEFAULT_CASSANDRA_LOG_TABLE
        assert settings.cassandra_id_column_name == app.settings.DEFAULT_CASSANDRA_ID_COLUMN_NAME
        assert settings.cassandra_timestamp_column_name == app.settings.DEFAULT_CASSANDRA_TIMESTAMP_COLUMN_NAME
        assert settings.interval_between_runs == app.settings.DEFAULT_INTERVAL_BETWEEN_RUNS
