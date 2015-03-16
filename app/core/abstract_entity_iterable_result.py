from abc import abstractmethod, ABCMeta
from collections import Iterable


class AbstractEntityIterableResult:

    __metaclass__ = ABCMeta.__class__

    def __init__(self, result):
        if isinstance(result, list) or isinstance(result, Iterable):
            self._iterator = result.__iter__()
        else:
            self._iterator = result

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        return self._to_entity(next(self._iterator))

    @abstractmethod
    def _to_entity(self, single_result):
        pass

    def to_list(self):
        """
        Gets all entities from the inner result iterator. Use with caution, since there might be thousands or
        millions of results. This is mainly useful for unit testing.
        :return: all entities from the result iterator.
        """
        _all = []
        for single_result in self:
            _all.append(single_result)
        return _all
