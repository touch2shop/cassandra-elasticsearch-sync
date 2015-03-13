import logging
import os
import arrow
import yaml
from app.sync_state import SyncState


DATE_FORMAT = "YYYY-MM-DDTHH:mm:ss.SSSSSSZZ"


class SyncStateStore:

    def __init__(self, filename):
        if not filename:
            raise ValueError("File name can not be empty")
        self._filename = filename
        self._logger = logging.getLogger()

    def load(self):
        if os.path.isfile(self._filename):
            self._logger.info("Loading previous state from file %s", self._filename)
            stream = open(self._filename, 'r')
            data = yaml.load(stream)
            return self._from_yaml(data)
        else:
            self._logger.warn("Previous state file %s not found. Loading initial state.", self._filename)
            return self._default_instance()

    def save(self, sync_state):
        stream = open(self._filename, 'w')
        yaml.dump(self._to_yaml(sync_state), stream)

    @classmethod
    def _to_utc_time_string(cls, timestamp):
        return arrow.get(timestamp).format(DATE_FORMAT)

    @classmethod
    def _to_timestamp(cls, utc_time):
        return arrow.get(utc_time).float_timestamp

    @classmethod
    def _default_instance(cls):
        return SyncState()

    @classmethod
    def _to_yaml(cls, sync_state):
        return {"last_sync_timestamp": cls._to_utc_time_string(sync_state.last_sync_timestamp)}

    @classmethod
    def _from_yaml(cls, data):
        time_string = data.get("last_sync_timestamp", None)
        if time_string:
            last_sync_timestamp = cls._to_timestamp(time_string)
        else:
            last_sync_timestamp = None
        return SyncState(last_sync_timestamp=last_sync_timestamp)
