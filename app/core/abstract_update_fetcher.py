from abc import ABCMeta, abstractmethod


class AbstractUpdateFetcher(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        self.__data_iterator = None

    def fetch_updates(self, minimum_timestamp=None):
        self.__data_iterator = self._fetch_data(minimum_timestamp)
        return self

    def __iter__(self):
        return self

    def __next__(self):
        return next(self)

    def next(self):
        if not self.__data_iterator:
            raise ValueError("There's no data fetched. Did you call fetch_updates first?")

        update = None
        while update is None:
            next_data = next(self.__data_iterator)
            update = self._to_update(next_data)
        return update

    @abstractmethod
    def _fetch_data(self, minimum_timestamp):
        pass

    @abstractmethod
    def _to_update(self, data):
        pass
