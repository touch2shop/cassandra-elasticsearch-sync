from abc import ABCMeta, abstractmethod


class AbstractUpdateFetcher(object):

    def __init__(self):
        pass

    __metaclass__ = ABCMeta

    @abstractmethod
    def fetch_updates(self, minimum_timestamp=None):
        return self

    def __iter__(self):
        return self

    def __next__(self):
        return next(self)

    @abstractmethod
    def next(self):
        raise StopIteration()
