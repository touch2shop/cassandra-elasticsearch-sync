from abc import ABCMeta


class AbstractDataRiver:

    __metaclass__ = ABCMeta

    def __init__(self, update_fetcher, update_applier):
        self._update_fetcher = update_fetcher
        self._update_applier = update_applier

    def propagate_updates(self, minimum_timestamp=None):
        last_update_timestamp = None

        for update in self._update_fetcher.fetch_updates(minimum_timestamp):
            self._update_applier.apply_update(update)
            if update.timestamp > last_update_timestamp:
                last_update_timestamp = update.timestamp

        return last_update_timestamp
